# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

# Experimental cooperative-consumers API. Function names, edge-case
# behavior, and client API shape may change before this feature is
# marked stable.

require_relative "test_helper"
require "logger"
require "stringio"

module CoopHelpers
  def with_coop_queue
    conn = PG.connect(PGQUE_TEST_DSN)
    q = unique_queue_name
    conn.exec_params("select pgque.create_queue($1)", [q])
    yield q, conn
  ensure
    if conn && !conn.finished?
      # Reset failed transaction state before cleanup, mirroring
      # with_queue. Otherwise a test that leaves the connection in
      # PQTRANS_INERROR will make unsubscribe/drop fail and leak state.
      conn.exec("ROLLBACK") rescue nil
      begin
        rows = conn.exec_params(
          "select c.co_name from pgque.consumer c " \
          "join pgque.subscription s on s.sub_consumer = c.co_id " \
          "join pgque.queue qq on qq.queue_id = s.sub_queue " \
          "where qq.queue_name = $1 and s.sub_role = 'coop_member'",
          [q],
        ).values.map { |r| r[0] }
        rows.each do |co_name|
          parent, sep, sub = co_name.rpartition(".")
          next if sep.empty? || parent.empty? || sub.empty?
          conn.exec_params(
            "select pgque.unsubscribe_subconsumer($1, $2, $3, 1)",
            [q, parent, sub],
          )
        end
        conn.exec_params("select pgque.drop_queue($1, true)", [q])
      rescue PG::Error
        # best-effort cleanup
      end
      conn.close
    end
  end

  def tick(conn, queue)
    conn.exec_params("select pgque.force_next_tick($1)", [queue])
    conn.exec_params("select pgque.ticker($1)", [queue])
  end

  def silent_logger
    log = Logger.new(StringIO.new)
    log.level = Logger::FATAL
    log
  end
end

class TestCoop < Minitest::Test
  include PgqueTest::Helpers
  include CoopHelpers

  def consumer_n
    @consumer_n ||= unique_consumer_name
  end

  def test_subscribe_subconsumer_returns_1_then_0
    with_coop_queue do |q, conn|
      client = Pgque::Client.new(conn)
      first = client.subscribe_subconsumer(q, consumer_n, "worker-1")
      second = client.subscribe_subconsumer(q, consumer_n, "worker-1")
      assert_equal 1, first
      assert_equal 0, second
    end
  end

  def test_receive_coop_returns_messages_and_ack_finishes
    with_coop_queue do |q, conn|
      client = Pgque::Client.new(conn)
      client.subscribe_subconsumer(q, consumer_n, "worker-1")
      client.send(q, { "k" => 1 }, type: "evt.a")
      client.send(q, { "k" => 2 }, type: "evt.a")
      tick(conn, q)

      msgs = client.receive_coop(q, consumer_n, "worker-1", max_messages: 10)
      assert_equal 2, msgs.size
      ks = msgs.map { |m| m.payload["k"] }.sort
      assert_equal [1, 2], ks

      client.ack(msgs[0].batch_id)

      follow = client.receive_coop(q, consumer_n, "worker-1", max_messages: 10)
      assert_equal [], follow
    end
  end

  def test_two_subconsumers_split_batches_no_duplicates
    with_coop_queue do |q, conn|
      Pgque.connect(dsn) do |producer|
        producer.subscribe_subconsumer(q, consumer_n, "worker-1")
        producer.subscribe_subconsumer(q, consumer_n, "worker-2")
        6.times { |i| producer.send(q, { "i" => i }, type: "evt") }
        producer.conn.exec_params("select pgque.force_next_tick($1)", [q])
        producer.conn.exec_params("select pgque.ticker($1)", [q])
      end

      # Ruby pg runs each exec_params as its own implicit transaction, so
      # the FOR UPDATE lock taken by receive_coop drops as soon as the
      # call returns -- no autocommit flag needed (cf. psycopg's
      # autocommit=True in the Python equivalent test).
      Pgque.connect(dsn) do |c1|
        Pgque.connect(dsn) do |c2|
          m1 = c1.receive_coop(q, consumer_n, "worker-1", max_messages: 100)
          m2 = c2.receive_coop(q, consumer_n, "worker-2", max_messages: 100)

          ids1 = m1.map(&:msg_id)
          ids2 = m2.map(&:msg_id)
          assert_empty ids1 & ids2,
                       "member-1 and member-2 saw same msg_ids: #{ids1 & ids2}"
          assert_operator m1.size + m2.size, :>=, 1

          c1.ack(m1[0].batch_id) if m1.any?
          c2.ack(m2[0].batch_id) if m2.any?
        end
      end

      Pgque.connect(dsn) do |cleanup|
        cleanup.unsubscribe_subconsumer(q, consumer_n, "worker-1",
                                        batch_handling: 1)
        cleanup.unsubscribe_subconsumer(q, consumer_n, "worker-2",
                                        batch_handling: 1)
      end
    end
  end

  def test_unsubscribe_subconsumer_with_active_batch_default_raises
    with_coop_queue do |q, conn|
      client = Pgque::Client.new(conn)
      client.subscribe_subconsumer(q, consumer_n, "worker-1")
      client.send(q, { "i" => 1 }, type: "evt")
      tick(conn, q)

      msgs = client.receive_coop(q, consumer_n, "worker-1")
      assert_equal 1, msgs.size

      assert_raises(Pgque::Error) do
        client.unsubscribe_subconsumer(q, consumer_n, "worker-1")
      end
      conn.exec("rollback") rescue nil

      rv = client.unsubscribe_subconsumer(q, consumer_n, "worker-1",
                                          batch_handling: 1)
      assert_equal 1, rv
    end
  end

  def test_unsubscribe_subconsumer_routes_active_messages_through_retry
    with_coop_queue do |q, conn|
      client = Pgque::Client.new(conn)
      client.subscribe_subconsumer(q, consumer_n, "worker-1")
      client.send(q, { "i" => 1 }, type: "evt")
      tick(conn, q)

      msgs = client.receive_coop(q, consumer_n, "worker-1")
      assert_equal 1, msgs.size

      rv = client.unsubscribe_subconsumer(q, consumer_n, "worker-1",
                                          batch_handling: 1)
      assert_equal 1, rv
    end
  end

  def test_touch_subconsumer_returns_1_on_registered_row
    with_coop_queue do |q, conn|
      client = Pgque::Client.new(conn)
      client.subscribe_subconsumer(q, consumer_n, "worker-1")
      rv = client.touch_subconsumer(q, consumer_n, "worker-1")
      assert_equal 1, rv
    end
  end

  def test_consumer_coop_dispatches_and_acks
    with_coop_queue do |q, conn|
      client = Pgque::Client.new(conn)
      client.subscribe_subconsumer(q, consumer_n, "worker-1")
      msg_id = client.send(q, { "x" => 1 }, type: "evt.coop")
      tick(conn, q)

      seen = []
      cons = Pgque::Consumer.new(dsn,
                                 queue: q, name: consumer_n,
                                 subconsumer: "worker-1",
                                 poll_interval: 1,
                                 logger: silent_logger)
      cons.on("evt.coop") { |m| seen << m }

      t = Thread.new { cons.start }
      Thread.new do
        sleep 3.0
        cons.stop
      end
      t.join(5.0)

      assert_equal 1, seen.size
      assert_equal msg_id, seen[0].msg_id

      follow = client.receive_coop(q, consumer_n, "worker-1")
      assert_equal [], follow

      client.unsubscribe_subconsumer(q, consumer_n, "worker-1",
                                     batch_handling: 1)
    end
  end

  def test_consumer_without_subconsumer_unchanged
    with_queue do |queue, c_name, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "v" => 1 }, type: "evt.normal")
      tick(conn, queue)

      seen = []
      cons = Pgque::Consumer.new(dsn,
                                 queue: queue, name: c_name,
                                 poll_interval: 1,
                                 logger: silent_logger)
      cons.on("evt.normal") { |m| seen << m }

      t = Thread.new { cons.start }
      Thread.new do
        sleep 3.0
        cons.stop
      end
      t.join(5.0)

      assert_equal 1, seen.size
    end
  end

  def test_consumer_dead_interval_without_subconsumer_raises
    assert_raises(ArgumentError) do
      Pgque::Consumer.new(dsn, queue: "q", name: "c",
                          dead_interval: "5 minutes")
    end
  end
end
