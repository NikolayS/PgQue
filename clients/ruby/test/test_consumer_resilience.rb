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
    return "#{dsn} application_name='#{value}'" unless dsn.start_with?("postgres://", "postgresql://")

    uri = URI.parse(dsn)

    # libpq treats '+' literally. Preserve existing URI query bytes.
    query = uri.query.to_s.split("&")
    query.reject! do |pair|
      URI::DEFAULT_PARSER.unescape(pair.split("=", 2).first) == "application_name"
    end
    encoded_name = URI.encode_www_form_component(value).gsub("+", "%20")
    query << "application_name=#{encoded_name}"
    uri.query = query.join("&")
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
        poll_interval: 10, logger: silent_logger
      )
      seen = []
      cons.on("evt.restart") { |msg| seen << msg.payload }

      # Observe the actual backend only after its receive returned empty.
      # Run this query on the consumer thread: libpq connections must not be
      # shared concurrently. The production wait remains unchanged.
      idle_waits = Queue.new
      original_wait = cons.method(:wait_for_notify_or_stop)
      cons.define_singleton_method(:wait_for_notify_or_stop) do |consumer_conn|
        channels = consumer_conn.exec(
          "select pg_listening_channels()"
        ).column_values(0)
        idle_waits << {
          pid: consumer_conn.backend_pid, channels: channels,
          started_at: Process.clock_gettime(Process::CLOCK_MONOTONIC),
        }
        original_wait.call(consumer_conn)
      end

      thread, thread_error = start_in_thread(cons)
      begin
        initial_wait = nil
        assert wait_until(timeout: 5) {
          initial_wait = idle_waits.pop unless idle_waits.empty?
        }, "consumer did not reach its initial idle wait"
        channel = "pgque_#{queue}"
        assert_includes initial_wait[:channels], channel

        killed = conn.exec_params(
          "select pg_terminate_backend($1)", [initial_wait[:pid]]
        ).getvalue(0, 0)
        assert_equal "t", killed

        replacement_wait = nil
        reconnected = wait_until(timeout: 15) do
          unless idle_waits.empty?
            observed = idle_waits.pop
            replacement_wait = observed if observed[:pid] != initial_wait[:pid]
          end
          replacement_wait
        end
        assert reconnected, "replacement backend did not reach its idle wait"
        assert_includes replacement_wait[:channels], channel,
                        "replacement backend did not restore LISTEN"

        # Send only after the replacement backend is empty and waiting.
        # Delivery must precede its ten-second polling deadline; an immediate
        # post-connect receive or polling-only reconnect cannot pass.
        client = Pgque::Client.new(conn)
        client.send(queue, { "r" => 1 }, type: "evt.restart")
        force_tick(conn, queue)
        assert wait_until(timeout: 5) { seen.size == 1 },
               "replacement backend did not wake on the notification"
        assert_operator monotonic - replacement_wait[:started_at], :<,
                        cons.poll_interval,
                        "delivery waited for the replacement backend's next poll"
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

# URI query values use percent escapes, not HTML form encoding.
class TestConsumerResilienceDSN < Minitest::Test
  def test_dsn_application_name_preserves_libpq_uri_parameters
    [
      "postgresql://user:pass%20word@localhost/db?password=pass%20word&options=-c%20statement_timeout%3D5000&application_name=old",
      "postgresql://user:pass+word@localhost/db?password=pass+word&options=-c%20statement_timeout%3D5000&application%5Fname=old"
    ].each do |original|
      harness = TestConsumerResilience.new("unused")
      harness.define_singleton_method(:dsn) { original }
      changed = harness.dsn_with_application_name("receiver name+tag")
      before = PG::Connection.conninfo_parse(original).to_h { |item| [item[:keyword], item[:val]] }
      after = PG::Connection.conninfo_parse(changed).to_h { |item| [item[:keyword], item[:val]] }
      assert_equal before.merge("application_name" => "receiver name+tag"), after
    end
  end
end
