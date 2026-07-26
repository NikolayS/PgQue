# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"
require "logger"
require "stringio"
require "uri"

class TestConsumerResilience < Minitest::Test
  include PgqueTest::Helpers

  def force_tick(conn, queue)
    conn.exec_params("select pgque.force_next_tick($1)", [queue])
    conn.exec_params("select pgque.ticker($1)", [queue])
  end

  def silent_logger
    log = Logger.new(StringIO.new)
    log.level = Logger::FATAL
    log
  end

  def start_in_thread(consumer)
    error = nil
    thread = Thread.new do
      consumer.start
    rescue StandardError => e
      error = e
    end
    [thread, -> { error }]
  end

  def wait_until(timeout: 10)
    deadline = monotonic + timeout
    until yield
      return false if monotonic >= deadline

      sleep 0.05
    end
    true
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def dsn_with_application_name(value)
    uri = URI.parse(dsn)
    unless uri.scheme&.start_with?("postgres")
      return "#{dsn} application_name='#{value}'"
    end

    query = URI.decode_www_form(uri.query.to_s)
    query.reject! { |key, _| key == "application_name" }
    query << ["application_name", value]
    uri.query = URI.encode_www_form(query)
    uri.to_s
  end

  def test_consumer_drains_existing_backlog_without_poll_interval_waits
    with_queue do |queue, consumer_name, conn|
      client = Pgque::Client.new(conn)
      3.times do |i|
        client.send(queue, { "i" => i }, type: "evt.backlog")
        force_tick(conn, queue)
      end

      seen = []
      cons = Pgque::Consumer.new(
        dsn, queue: queue, name: consumer_name,
        poll_interval: 30, logger: silent_logger
      )
      cons.on("evt.backlog") { |msg| seen << msg.payload }

      started = monotonic
      thread, thread_error = start_in_thread(cons)
      begin
        drained = wait_until(timeout: 5) { seen.size == 3 }
        elapsed = monotonic - started

        assert drained,
               "backlog stalled at #{seen.size}/3; consumer waited between batches"
        assert_operator elapsed, :<, 5,
                        "backlog took #{elapsed.round(2)}s to drain"
        assert_nil thread_error.call
      ensure
        cons.stop
        thread.join(3)
      end
    end
  end

  def test_consumer_recovers_from_initial_connect_error
    with_queue do |queue, consumer_name, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "x" => 1 }, type: "evt.connect")
      force_tick(conn, queue)

      original_connect = PG.method(:connect)
      connect_calls = 0
      PG.define_singleton_method(:connect) do |*args, **kwargs|
        connect_calls += 1
        if connect_calls == 1
          raise PG::ConnectionBad, "simulated initial connection failure"
        end

        original_connect.call(*args, **kwargs)
      end

      seen = []
      cons = Pgque::Consumer.new(
        dsn, queue: queue, name: consumer_name,
        poll_interval: 0.1, logger: silent_logger
      )
      cons.on("evt.connect") { |msg| seen << msg.payload }

      thread, thread_error = start_in_thread(cons)
      begin
        assert wait_until(timeout: 5) { seen.size == 1 },
               "consumer did not reconnect after initial failure"
        assert_operator connect_calls, :>=, 2
        assert thread.alive?, "consumer exited after recovering"
        assert_nil thread_error.call
      ensure
        cons.stop
        thread.join(3)
        PG.define_singleton_method(:connect, original_connect)
      end
    end
  end

  def test_consumer_recovers_from_transient_receive_error
    with_queue do |queue, consumer_name, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "x" => 1 }, type: "evt.receive")
      force_tick(conn, queue)

      original_receive = Pgque::Client.instance_method(:receive)
      receive_calls = 0
      Pgque::Client.define_method(:receive) do |*args|
        receive_calls += 1
        if receive_calls == 1
          raise Pgque::Error, "simulated transient receive failure"
        end

        original_receive.bind_call(self, *args)
      end

      seen = []
      cons = Pgque::Consumer.new(
        dsn, queue: queue, name: consumer_name,
        poll_interval: 0.1, logger: silent_logger
      )
      cons.on("evt.receive") { |msg| seen << msg.payload }

      thread, thread_error = start_in_thread(cons)
      begin
        assert wait_until(timeout: 5) { seen.size == 1 },
               "consumer did not retry receive after transient failure"
        assert_operator receive_calls, :>=, 2
        assert thread.alive?, "consumer exited after recovering"
        assert_nil thread_error.call
      ensure
        cons.stop
        thread.join(3)
        Pgque::Client.define_method(:receive, original_receive)
      end
    end
  end

  def test_consumer_reconnects_after_backend_is_terminated
    with_queue do |queue, consumer_name, conn|
      app_name = "pgque_ruby_#{SecureRandom.hex(6)}"
      cons = Pgque::Consumer.new(
        dsn_with_application_name(app_name),
        queue: queue, name: consumer_name,
        poll_interval: 0.2, logger: silent_logger
      )
      seen = []
      cons.on("evt.restart") { |msg| seen << msg.payload }

      thread, thread_error = start_in_thread(cons)
      begin
        old_pid = nil
        connected = wait_until(timeout: 5) do
          result = conn.exec_params(
            "select pid from pg_stat_activity " \
            "where application_name = $1 and pid <> pg_backend_pid()",
            [app_name],
          )
          old_pid = result.ntuples.zero? ? nil : result.getvalue(0, 0).to_i
        end
        assert connected, "consumer backend did not appear"

        killed = conn.exec_params(
          "select pg_terminate_backend($1)", [old_pid]
        ).getvalue(0, 0)
        assert_equal "t", killed

        reconnected = wait_until(timeout: 8) do
          result = conn.exec_params(
            "select pid from pg_stat_activity " \
            "where application_name = $1 and pid <> pg_backend_pid()",
            [app_name],
          )
          result.ntuples.positive? && result.getvalue(0, 0).to_i != old_pid
        end
        assert reconnected, "consumer did not reconnect after backend termination"

        client = Pgque::Client.new(conn)
        client.send(queue, { "r" => 1 }, type: "evt.restart")
        force_tick(conn, queue)
        assert wait_until(timeout: 5) { seen.size == 1 },
               "reconnected consumer did not resume processing"
        assert thread.alive?
        assert_nil thread_error.call
      ensure
        cons.stop
        thread.join(3)
      end
    end
  end

  def test_stop_is_prompt_during_connection_retry_wait
    with_queue do |queue, consumer_name, _conn|
      original_connect = PG.method(:connect)
      connect_calls = 0
      PG.define_singleton_method(:connect) do |*|
        connect_calls += 1
        raise PG::ConnectionBad, "simulated persistent connection failure"
      end

      cons = Pgque::Consumer.new(
        dsn, queue: queue, name: consumer_name,
        poll_interval: 30, logger: silent_logger
      )
      thread, thread_error = start_in_thread(cons)
      begin
        assert wait_until(timeout: 2) { connect_calls.positive? }
        assert thread.alive?, "consumer exited instead of waiting to reconnect"

        started = monotonic
        cons.stop
        thread.join(3)
        elapsed = monotonic - started

        refute thread.alive?, "consumer did not stop during retry wait"
        assert_operator elapsed, :<, 2,
                        "stop took #{elapsed.round(2)}s during retry wait"
        assert_nil thread_error.call
      ensure
        cons.stop
        thread.join(3)
        PG.define_singleton_method(:connect, original_connect)
      end
    end
  end
end
