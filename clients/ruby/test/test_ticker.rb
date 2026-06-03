# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestTicker < Minitest::Test
  include PgqueTest::Helpers

  def test_ticker_after_force_next_tick_returns_non_nil_integer
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "x" => 1 }, type: "tick.test")
      client.force_next_tick(queue)
      tick_id = client.ticker(queue)
      refute_nil tick_id, "ticker after force_next_tick must produce a tick"
      assert_kind_of Integer, tick_id
      assert_operator tick_id, :>, 0
    end
  end

  def test_ticker_returns_nil_when_no_new_tick_needed
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      client.force_next_tick(queue)
      client.ticker(queue)
      # Immediately again with no new activity: ticker returns nil.
      assert_nil client.ticker(queue),
                 "second ticker call with no new events must return nil"
    end
  end

  def test_ticker_all_returns_non_negative_integer
    Pgque.connect(dsn) do |client|
      n = client.ticker_all
      assert_kind_of Integer, n
      assert_operator n, :>=, 0
    end
  end
end

class TestTickerSqlForm < Minitest::Test
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

  def test_ticker_issues_single_queue_sql
    conn = FakeConn.new(scalar: "42")
    client = Pgque::Client.new(conn)
    tick_id = client.ticker("orders")
    assert_equal 42, tick_id
    assert_includes conn.sql_used, "pgque.ticker($1)"
    assert_equal ["orders"], conn.params_used
  end

  def test_ticker_returns_nil_for_nil_scalar
    conn = FakeConn.new(scalar: nil)
    client = Pgque::Client.new(conn)
    assert_nil client.ticker("orders")
  end

  def test_ticker_returns_nil_for_empty_scalar
    conn = FakeConn.new(scalar: "")
    client = Pgque::Client.new(conn)
    assert_nil client.ticker("orders")
  end

  def test_ticker_all_issues_zero_arg_sql
    conn = FakeConn.new(scalar: "3")
    client = Pgque::Client.new(conn)
    n = client.ticker_all
    assert_equal 3, n
    assert_includes conn.sql_used, "pgque.ticker()"
    refute_includes conn.sql_used, "$1"
    assert_equal [], conn.params_used
  end
end
