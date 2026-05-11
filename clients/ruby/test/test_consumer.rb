# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"
require "logger"
require "stringio"

class TestConsumerUnit < Minitest::Test
  include PgqueTest::Helpers

  class FakeTxConn
    def transaction
      yield self
    end
  end

  class SpyClient
    attr_reader :receive_calls

    def initialize(*)
      @receive_calls = []
    end

    def receive(queue, consumer, max_messages)
      @receive_calls << [queue, consumer, max_messages]
      []
    end

    def ack(_batch_id)
      1
    end

    def nack(*); end
  end

  def test_consumer_default_max_messages_requests_whole_batch
    cons = Pgque::Consumer.new(dsn, queue: "q", name: "c")
    assert_equal 2_147_483_647, cons.max_messages
  end

  def test_consumer_configured_max_messages_is_preserved
    cons = Pgque::Consumer.new(dsn, queue: "q", name: "c", max_messages: 123)
    assert_equal 123, cons.max_messages
  end

  def test_consumer_poll_once_passes_default_max_messages
    cons = Pgque::Consumer.new(dsn, queue: "q", name: "c")
    spy = SpyClient.new
    Pgque::Client.stub :new, ->(*) { spy } do
      cons.poll_once(FakeTxConn.new)
    end
    assert_equal [["q", "c", 2_147_483_647]], spy.receive_calls
  end

  def test_consumer_poll_once_passes_configured_max_messages
    cons = Pgque::Consumer.new(dsn, queue: "q", name: "c", max_messages: 123)
    spy = SpyClient.new
    Pgque::Client.stub :new, ->(*) { spy } do
      cons.poll_once(FakeTxConn.new)
    end
    assert_equal [["q", "c", 123]], spy.receive_calls
  end

  def test_consumer_rejects_invalid_unknown_handler_policy
    skip_dsn_for_this_class!
    assert_raises(ArgumentError) do
      Pgque::Consumer.new("dummy", queue: "q", name: "c",
                          unknown_handler_policy: "bogus")
    end
  end

  def test_consumer_dead_interval_without_subconsumer_raises
    skip_dsn_for_this_class!
    assert_raises(ArgumentError) do
      Pgque::Consumer.new("dummy", queue: "q", name: "c",
                          dead_interval: "5 minutes")
    end
  end

  def test_running_clears_after_start_failure
    skip_dsn_for_this_class!
    # Point at an unreachable port so PG.connect raises immediately
    # inside start, exiting before the poll loop ever runs.
    cons = Pgque::Consumer.new(
      "postgresql://nobody:wrong@localhost:1/nodb",
      queue: "q", name: "c", logger: silent_logger
    )
    refute cons.running?
    assert_raises(PG::Error) { cons.start }
    refute cons.running?,
           "consumer.running? must be false after start raised"
  end

  def silent_logger
    require "logger"
    require "stringio"
    log = Logger.new(StringIO.new)
    log.level = Logger::FATAL
    log
  end

  def test_invalid_pgque_log_level_falls_back_to_fatal
    skip_dsn_for_this_class!
    old = ENV["PGQUE_LOG_LEVEL"]
    ENV["PGQUE_LOG_LEVEL"] = " warning "

    cons = Pgque::Consumer.new("dummy", queue: "q", name: "c")
    assert_equal Logger::FATAL, cons.logger.level
  ensure
    ENV["PGQUE_LOG_LEVEL"] = old
  end

  private

  # Some unit tests don't actually connect; allow them even without DSN.
  def skip_dsn_for_this_class!
    # The setup-level skip already passes when DSN is set; this method is
    # here so the structure is symmetric with the integration tests.
  end
end

class TestConsumerIntegration < Minitest::Test
  include PgqueTest::Helpers

  def run_consumer_for(consumer, seconds)
    t = Thread.new { consumer.start }
    Thread.new do
      sleep seconds
      consumer.stop
    end
    t
  end

  def force_tick(conn, queue)
    conn.exec_params("select pgque.force_next_tick($1)", [queue])
    conn.exec_params("select pgque.ticker($1)", [queue])
  end

  def silent_logger
    log = Logger.new(StringIO.new)
    log.level = Logger::FATAL
    log
  end

  def capturing_logger
    io = StringIO.new
    log = Logger.new(io)
    log.level = Logger::WARN
    [log, io]
  end

  def retry_count_for_msg(conn, queue, msg_id)
    conn.exec_params(
      "select count(*) from pgque.retry_queue rq " \
      "join pgque.queue q on q.queue_id = rq.ev_queue " \
      "where q.queue_name = $1 and rq.ev_id = $2",
      [queue, msg_id],
    ).values[0][0].to_i
  end

  def dlq_count_for_msg(conn, queue, msg_id)
    conn.exec_params(
      "select count(*) from pgque.dead_letter dl " \
      "join pgque.queue q on q.queue_id = dl.dl_queue_id " \
      "where q.queue_name = $1 and dl.ev_id = $2",
      [queue, msg_id],
    ).values[0][0].to_i
  end

  def test_consumer_dispatches_by_event_type
    with_queue do |queue, consumer_n, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "i" => 1 }, type: "evt.a")
      client.send(queue, { "i" => 2 }, type: "evt.b")
      force_tick(conn, queue)

      seen_a = []
      seen_b = []
      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 1, logger: silent_logger)
      cons.on("evt.a") { |m| seen_a << m.payload }
      cons.on("evt.b") { |m| seen_b << m.payload }

      run_consumer_for(cons, 3.0).join(5.0)

      assert_equal 1, seen_a.size
      assert_equal 1, seen_b.size
    end
  end

  def test_consumer_default_handler_catches_unknown
    with_queue do |queue, consumer_n, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "x" => 99 }, type: "never.registered.type")
      force_tick(conn, queue)

      fallback = []
      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 1, logger: silent_logger)
      cons.on("*") { |m| fallback << m }

      run_consumer_for(cons, 3.0).join(5.0)

      assert_equal 1, fallback.size
      assert_equal "never.registered.type", fallback[0].type
    end
  end

  def test_consumer_nacks_on_handler_error
    with_queue do |queue, consumer_n, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "i" => 1 }, type: "evt.fail")
      force_tick(conn, queue)

      n_calls = 0
      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 1, retry_after: 0,
                                 logger: silent_logger)
      cons.on("evt.fail") { |_m| n_calls += 1; raise "simulated failure" }

      run_consumer_for(cons, 3.0).join(5.0)

      assert_operator n_calls, :>=, 1
      cnt = conn.exec_params(
        "select count(*) from pgque.retry_queue rq " \
        "join pgque.queue q on q.queue_id = rq.ev_queue " \
        "where q.queue_name = $1",
        [queue],
      ).values[0][0].to_i
      assert_operator cnt, :>=, 1
    end
  end

  def test_consumer_nacks_unhandled_event_type
    with_queue do |queue, consumer_n, conn|
      client = Pgque::Client.new(conn)
      msg_id = client.send(queue, { "x" => 1 }, type: "totally.unregistered.type")
      force_tick(conn, queue)

      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 1, logger: silent_logger)
      run_consumer_for(cons, 3.0).join(5.0)

      rq = retry_count_for_msg(conn, queue, msg_id)
      dlq = dlq_count_for_msg(conn, queue, msg_id)
      assert_operator rq + dlq, :>=, 1,
                      "unhandled event was not nacked: rq=#{rq} dlq=#{dlq}"

      force_tick(conn, queue)
      follow = client.receive(queue, consumer_n, 10)
      refute(follow.any? { |m| m.msg_id == msg_id },
             "batch did not advance past unhandled msg_id")
      client.ack(follow[0].batch_id) if follow.any?
    end
  end

  def test_consumer_acks_unhandled_event_type_when_opt_in
    with_queue do |queue, consumer_n, conn|
      client = Pgque::Client.new(conn)
      msg_id = client.send(queue, { "x" => 1 }, type: "totally.unregistered.type")
      force_tick(conn, queue)

      log, io = capturing_logger
      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 1,
                                 unknown_handler_policy: "ack",
                                 logger: log)
      run_consumer_for(cons, 3.0).join(5.0)

      assert_equal 0, retry_count_for_msg(conn, queue, msg_id)
      assert_equal 0, dlq_count_for_msg(conn, queue, msg_id)
      assert_includes io.string, "totally.unregistered.type"

      force_tick(conn, queue)
      follow = client.receive(queue, consumer_n, 10)
      refute(follow.any? { |m| m.msg_id == msg_id })
      client.ack(follow[0].batch_id) if follow.any?
    end
  end

  def test_consumer_stop_returns_promptly
    with_queue do |queue, consumer_n, _conn|
      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 10, logger: silent_logger)
      t = Thread.new { cons.start }
      sleep 0.5
      cons.stop
      finished = t.join(15)
      refute_nil finished, "consumer did not stop after stop()"
    end
  end

  def test_consumer_stop_returns_within_2s_while_waiting
    with_queue do |queue, consumer_n, _conn|
      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 30, logger: silent_logger)
      t = Thread.new { cons.start }
      sleep 1.0

      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      cons.stop
      t.join(5.0)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0

      refute t.alive?, "consumer thread did not stop"
      assert_operator elapsed, :<, 2.0,
                      "stop() took #{elapsed.round(2)}s; expected <2s"
    end
  end

  def test_consumer_wakes_on_pg_notify_before_poll_interval
    with_queue do |queue, consumer_n, conn|
      received = []
      received_evt = Mutex.new
      received_cv = ConditionVariable.new

      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 30, logger: silent_logger)
      cons.on("evt.wake") do |m|
        received_evt.synchronize do
          received << m
          received_cv.signal
        end
      end

      t = Thread.new { cons.start }
      sleep 1.5

      t_send = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      producer = PG.connect(dsn)
      begin
        client = Pgque::Client.new(producer)
        client.send(queue, { "i" => 1 }, type: "evt.wake")
        producer.exec_params("select pgque.force_next_tick($1)", [queue])
        producer.exec_params("select pgque.ticker($1)", [queue])
        producer.exec_params("notify pgque_#{queue}, 'go'")
      ensure
        producer.close
      end

      woke = false
      received_evt.synchronize do
        deadline = Time.now + 5
        until received.any? || Time.now >= deadline
          received_cv.wait(received_evt, deadline - Time.now)
        end
        woke = received.any?
      end
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t_send

      cons.stop
      t.join(5.0)

      assert woke, "consumer did not wake on pg_notify within 5s"
      assert_equal 1, received.size
      assert_operator elapsed, :<, 5.0,
                      "consumer woke too slowly (#{elapsed.round(2)}s)"
    end
  end

  def test_consumer_partial_batch_acks_good_messages_only
    with_queue do |queue, consumer_n, conn|
      client = Pgque::Client.new(conn)
      ok1 = client.send(queue, { "i" => 1 }, type: "ok")
      boom = client.send(queue, { "i" => 2 }, type: "boom")
      ok2 = client.send(queue, { "i" => 3 }, type: "ok")
      force_tick(conn, queue)

      seen_ok = []
      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 1, retry_after: 3600,
                                 logger: silent_logger)
      cons.on("ok") { |m| seen_ok << m.msg_id }
      cons.on("boom") { |_| raise "handler boom" }

      run_consumer_for(cons, 3.0).join(5.0)

      assert_includes seen_ok, ok1
      assert_includes seen_ok, ok2

      assert_operator retry_count_for_msg(conn, queue, boom), :>=, 1
      assert_equal 0, retry_count_for_msg(conn, queue, ok1)
      assert_equal 0, retry_count_for_msg(conn, queue, ok2)

      force_tick(conn, queue)
      follow = client.receive(queue, consumer_n, 10)
      ids = follow.map(&:msg_id)
      refute_includes ids, ok1
      refute_includes ids, ok2
      client.ack(follow[0].batch_id) if follow.any?
    end
  end
end
