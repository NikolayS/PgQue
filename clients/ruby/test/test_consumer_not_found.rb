# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestConsumerNotFound < Minitest::Test
  include PgqueTest::Helpers

  class RaisingConn
    def initialize(error)
      @error = error
    end

    def exec_params(*)
      raise @error
    end
  end

  def assert_classified_with_cause(message)
    raw = PG::Error.new(message)
    error = assert_raises(Pgque::ConsumerNotFound) do
      yield Pgque::Client.new(RaisingConn.new(raw))
    end
    assert_same raw, error.cause
    assert_equal raw.backtrace, error.backtrace
  end

  def test_receive_classifies_all_missing_consumer_fragments
    [
      "consumer not registered",
      "consumer not found",
      "Not subscriber to queue: orders/worker",
    ].each do |message|
      assert_classified_with_cause(message) do |client|
        client.receive("orders", "worker", 1)
      end
    end
  end

  def test_receive_coop_classifies_missing_main_and_subconsumer
    [
      "cooperative main consumer not found: orders/workers",
      "cooperative subconsumer not found: orders/workers/worker-1",
    ].each do |message|
      assert_classified_with_cause(message) do |client|
        client.receive_coop("orders", "workers", "worker-1")
      end
    end
  end

  def test_receive_from_real_unregistered_consumer_preserves_pg_error
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      error = assert_raises(Pgque::ConsumerNotFound) do
        client.receive(queue, "missing_consumer", 1)
      end

      assert_kind_of PG::Error, error.cause
      assert_includes error.message.downcase, "not subscriber to queue"
      assert_equal error.cause.backtrace, error.backtrace
    end
  end
end
