# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

# Regression tests for the LISTEN/NOTIFY wait: stop() must take effect
# promptly, and a real NOTIFY must wake the wait well before
# poll_interval expires.

require_relative "test_helper"
require "logger"
require "stringio"

class TestConsumerListenStop < Minitest::Test
  include PgqueTest::Helpers

  def silent_logger
    log = Logger.new(StringIO.new)
    log.level = Logger::FATAL
    log
  end

  def test_stop_is_honored_promptly
    with_queue do |queue, consumer_n, _conn|
      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 10, logger: silent_logger)
      t = Thread.new { cons.start }
      sleep 1.0

      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      cons.stop
      t.join(3.5)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0

      refute t.alive?,
             "consumer thread still alive #{elapsed.round(2)}s after stop()"
      assert_operator elapsed, :<, 3.0,
                      "stop() took #{elapsed.round(2)}s; expected <3s"
    end
  end

  def test_notify_wakes_consumer_before_poll_interval
    with_queue do |queue, consumer_n, _conn|
      seen = []
      handler_called = Mutex.new
      handler_cv = ConditionVariable.new

      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 10, logger: silent_logger)
      cons.on("evt.wake") do |m|
        handler_called.synchronize do
          seen << m.payload
          handler_cv.signal
        end
      end

      t = Thread.new { cons.start }
      begin
        sleep 1.0

        producer = PG.connect(dsn)
        begin
          client = Pgque::Client.new(producer)
          client.send(queue, { "v" => 1 }, type: "evt.wake")
          producer.exec_params("select pgque.force_next_tick($1)", [queue])
          producer.exec_params("select pgque.ticker($1)", [queue])
        ensure
          producer.close
        end

        woke = false
        handler_called.synchronize do
          deadline = Time.now + 3.0
          until seen.any? || Time.now >= deadline
            handler_cv.wait(handler_called, deadline - Time.now)
          end
          woke = seen.any?
        end
        assert woke, "handler not invoked within 3s; NOTIFY did not wake"
        assert_equal 1, seen.size
      ensure
        cons.stop
        t.join(3.0)
      end
    end
  end
end
