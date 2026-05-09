# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

module Pgque
  class Client
    attr_reader :conn

    def self.connect(dsn, autocommit: false)
      conn = PG.connect(dsn)
      new(conn, owns_conn: true, autocommit: autocommit)
    rescue PG::ConnectionBad => e
      raise ConnectionError, e.message
    end

    def initialize(conn, owns_conn: false, autocommit: false)
      @conn = conn
      @owns_conn = owns_conn
      @autocommit = autocommit
    end

    def autocommit?
      @autocommit
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
        if type && type != "" && type != "default"
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
      result.values[0][0].to_i
    end

    def send_batch(queue, type, payloads)
      encoded = payloads.map { |p| encode_payload(p) }
      array_literal = pg_text_array(encoded)
      result = @conn.exec_params(
        "select unnest(pgque.send_batch($1, $2, $3::jsonb[]))",
        [queue, type, array_literal],
      )
      result.values.map { |r| r[0].to_i }
    end

    private

    def encode_payload(payload)
      case payload
      when Hash, Array then JSON.dump(payload)
      when nil         then "null"
      else                  payload
      end
    end

    def pg_text_array(strings)
      escaped = strings.map do |s|
        inner = s.to_s.gsub('\\') { '\\\\' }.gsub('"') { '\\"' }
        "\"#{inner}\""
      end
      "{#{escaped.join(',')}}"
    end
  end
end
