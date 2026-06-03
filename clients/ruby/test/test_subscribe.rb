# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestSubscribe < Minitest::Test
  include PgqueTest::Helpers

  def test_subscribe_returns_one_for_new_then_zero_for_existing
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      fresh = "#{unique_consumer_name}_sub"
      begin
        first = client.subscribe(queue, fresh)
        assert_equal 1, first, "first subscribe must return 1 for a fresh consumer"
        second = client.subscribe(queue, fresh)
        assert_equal 0, second, "second subscribe must return 0 (already registered)"
      ensure
        conn.exec_params(
          "select pgque.unregister_consumer($1, $2)", [queue, fresh]
        ) rescue nil
      end
    end
  end

  def test_unsubscribe_returns_positive_for_existing_then_zero_for_missing
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      first = client.unsubscribe(queue, consumer)
      assert_operator first, :>=, 1,
                      "first unsubscribe of a registered consumer must return >= 1"
      second = client.unsubscribe(queue, consumer)
      assert_equal 0, second,
                   "second unsubscribe must return 0 (no longer registered)"
    end
  end

  def test_subscribed_consumer_can_receive_messages
    with_queue do |queue, _registered, conn|
      client = Pgque::Client.new(conn)
      fresh = "#{unique_consumer_name}_recv"
      begin
        client.subscribe(queue, fresh)
        client.send(queue, { "x" => 1 }, type: "sub.test")
        client.force_next_tick(queue)
        client.ticker(queue)
        msgs = client.receive(queue, fresh, 10)
        assert_equal 1, msgs.size
        assert_equal "sub.test", msgs[0].type
        client.ack(msgs[0].batch_id)
      ensure
        conn.exec_params(
          "select pgque.unregister_consumer($1, $2)", [queue, fresh]
        ) rescue nil
      end
    end
  end
end

class TestSubscribeSqlForm < Minitest::Test
  # Capture exec_params calls without a real DB.
  class FakeConn
    attr_reader :sql_used, :params_used

    def initialize(scalar:)
      @scalar = scalar
    end

    def exec_params(sql, params)
      @sql_used = sql
      @params_used = params
      FakeResult.new(@scalar)
    end

    class FakeResult
      def initialize(value)
        @value = value
      end

      def getvalue(_row, _col)
        @value
      end
    end
  end

  def test_subscribe_issues_two_arg_sql_and_returns_integer
    conn = FakeConn.new(scalar: "1")
    client = Pgque::Client.new(conn)
    n = client.subscribe("orders", "processor")
    assert_equal 1, n
    assert_includes conn.sql_used, "pgque.subscribe($1, $2)"
    assert_equal ["orders", "processor"], conn.params_used
  end

  def test_subscribe_returns_zero_when_already_registered
    conn = FakeConn.new(scalar: "0")
    client = Pgque::Client.new(conn)
    assert_equal 0, client.subscribe("orders", "processor")
  end

  def test_unsubscribe_issues_two_arg_sql_and_returns_integer
    conn = FakeConn.new(scalar: "1")
    client = Pgque::Client.new(conn)
    n = client.unsubscribe("orders", "processor")
    assert_equal 1, n
    assert_includes conn.sql_used, "pgque.unsubscribe($1, $2)"
    assert_equal ["orders", "processor"], conn.params_used
  end

  def test_unsubscribe_returns_zero_when_not_subscribed
    conn = FakeConn.new(scalar: "0")
    client = Pgque::Client.new(conn)
    assert_equal 0, client.unsubscribe("orders", "processor")
  end
end
