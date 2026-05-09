# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

module Pgque
  # Thin wrapper over the pgque SQL functions.
  #
  # Note: Pgque::Client#send mirrors the SQL `pgque.send(queue, payload)`
  # primitive and the Python/TS client surface. That name shadows
  # Ruby's Object#send, so use #__send__ or #public_send when you need
  # to invoke a method on a Pgque::Client instance reflectively.
  class Client
    attr_reader :conn

    def self.connect(dsn)
      conn = PG.connect(dsn)
      new(conn, owns_conn: true)
    rescue PG::ConnectionBad => e
      raise ConnectionError, e.message
    end

    def initialize(conn, owns_conn: false)
      @conn = conn
      @owns_conn = owns_conn
    end

    def close
      return unless @owns_conn
      return if @conn.finished?
      @conn.close
    end

    def send(queue, payload, type: "default")
      if payload.is_a?(Event)
        type = payload.type
        payload = payload.payload
      end
      encoded = encode_payload(payload)
      result =
        if custom_type?(type)
          @conn.exec_params(
            "select pgque.send($1, $2, $3::jsonb)",
            [queue, type, encoded],
          )
        else
          @conn.exec_params(
            "select pgque.send($1, $2::jsonb)",
            [queue, encoded],
          )
        end
      integer_scalar(result)
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    def send_batch(queue, type, payloads)
      encoded = payloads.map { |p| encode_payload(p) }
      array_literal = pg_text_array(encoded)
      result = @conn.exec_params(
        "select unnest(pgque.send_batch($1, $2, $3::jsonb[]))",
        [queue, type, array_literal],
      )
      integer_column(result)
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    def receive(queue, consumer, max_messages = 100)
      result = @conn.exec_params(
        "select * from pgque.receive($1, $2, $3)",
        [queue, consumer, max_messages],
      )
      result.each_row.map { |row| row_to_message(row) }
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    def ack(batch_id)
      result = @conn.exec_params("select pgque.ack($1)", [batch_id])
      integer_scalar(result)
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    def force_next_tick(queue)
      result = @conn.exec_params("select pgque.force_next_tick($1)", [queue])
      v = scalar(result)
      v.nil? || v.empty? ? nil : v.to_i
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    # Experimental: function names, edge-case behavior, and signatures may
    # change before the cooperative API is marked stable.
    def subscribe_subconsumer(queue, consumer, subconsumer)
      result = @conn.exec_params(
        "select pgque.subscribe_subconsumer($1, $2, $3)",
        [queue, consumer, subconsumer],
      )
      integer_scalar(result)
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    def unsubscribe_subconsumer(queue, consumer, subconsumer, batch_handling: 0)
      result = @conn.exec_params(
        "select pgque.unsubscribe_subconsumer($1, $2, $3, $4)",
        [queue, consumer, subconsumer, batch_handling],
      )
      integer_scalar(result)
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    def receive_coop(queue, consumer, subconsumer, max_messages: 100,
                     dead_interval: nil)
      result = @conn.exec_params(
        "select * from pgque.receive_coop($1, $2, $3, $4, $5::interval)",
        [queue, consumer, subconsumer, max_messages, dead_interval],
      )
      result.each_row.map { |row| row_to_message(row) }
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    def touch_subconsumer(queue, consumer, subconsumer)
      result = @conn.exec_params(
        "select pgque.touch_subconsumer($1, $2, $3)",
        [queue, consumer, subconsumer],
      )
      integer_scalar(result)
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    def nack(batch_id, msg, retry_after: 60, reason: nil)
      payload_str = case msg.payload
                    when Hash, Array then JSON.dump(msg.payload)
                    when nil         then "null"
                    else                  msg.payload.to_s
                    end
      created_at_str = format_created_at(msg.created_at)

      @conn.exec_params(
        "select pgque.nack($1, " \
        "ROW($2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11)::pgque.message, " \
        "$12::interval, $13)",
        [
          batch_id, msg.msg_id, msg.batch_id, msg.type, payload_str,
          msg.retry_count, created_at_str,
          msg.extra1, msg.extra2, msg.extra3, msg.extra4,
          "#{retry_after} seconds", reason,
        ],
      )
      nil
    rescue PG::Error => e
      raise_wrapped_sql_error(e)
    end

    private

    # Hash/Array: JSON-encoded.
    # nil: literal "null" so ::jsonb yields JSON null (not SQL NULL).
    # String: passed through verbatim; caller must supply valid JSON text.
    # Anything else (Integer, Float, true, false, Symbol, ...): coerced
    # via #to_s so numerics and booleans round-trip naturally
    # (42 -> "42", true -> "true"). Symbols and other objects whose
    # to_s isn't valid JSON will surface a SQL error from the ::jsonb
    # cast -- callers who care should pre-encode with JSON.dump.
    def encode_payload(payload)
      case payload
      when Hash, Array then JSON.dump(payload)
      when nil         then "null"
      when String      then payload
      else                  payload.to_s
      end
    end

    def pg_text_array(strings)
      escaped = strings.map do |s|
        inner = s.to_s.gsub('\\') { '\\\\' }.gsub('"') { '\\"' }
        "\"#{inner}\""
      end
      "{#{escaped.join(',')}}"
    end

    def row_to_message(row)
      Message.new(
        msg_id: row[0].to_i,
        batch_id: row[1].to_i,
        type: row[2],
        payload: parse_jsonb(row[3]),
        retry_count: row[4].nil? ? nil : row[4].to_i,
        created_at: row[5].nil? ? nil : Time.parse(row[5]),
        extra1: row[6],
        extra2: row[7],
        extra3: row[8],
        extra4: row[9],
      )
    end

    def parse_jsonb(text)
      return nil if text.nil?
      JSON.parse(text)
    rescue JSON::ParserError
      text
    end

    def wrap_sql_error(error)
      msg = error.message.to_s
      low = msg.downcase
      if low.include?("queue not found")
        QueueNotFound.new(msg)
      elsif low.include?("batch not found")
        BatchNotFound.new(msg)
      else
        Error.new(msg)
      end
    end

    def raise_wrapped_sql_error(error)
      wrapped = wrap_sql_error(error)
      wrapped.set_backtrace(error.backtrace) if error.backtrace
      raise wrapped, cause: error
    end

    def scalar(result)
      result.getvalue(0, 0)
    end

    def integer_scalar(result)
      scalar(result).to_i
    end

    def integer_column(result)
      result.column_values(0).map(&:to_i)
    end

    def custom_type?(type)
      !type.to_s.empty? && type != "default"
    end

    def format_created_at(value)
      case value
      when Time then value.iso8601(6)
      else value
      end
    end
  end
end
