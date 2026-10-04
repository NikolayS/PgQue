# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestReceiveOverflow < Minitest::Test
  FakeResult = Struct.new(:fields) do
    def error_field(field)
      fields[field]
    end
  end

  FakePgError = Struct.new(:message, :result, :backtrace)

  class FakeConnection
    attr_reader :closed

    def escape_identifier(value) = %Q{"#{value}"}
    def exec(_sql); end
    def transaction = yield
    def finished? = false
    def close = @closed = true
  end

  class OverflowClient
    attr_reader :receive_calls, :ack_calls

    def initialize(error)
      @error = error
      @receive_calls = 0
      @ack_calls = 0
    end

    def receive(*)
      @receive_calls += 1
      raise @error
    end

    def ack(*) = @ack_calls += 1
  end

  def raw_error(message, sqlstate: "54000", hint: "server hint")
    FakePgError.new(
      message,
      FakeResult.new({
        PG::Result::PG_DIAG_SQLSTATE => sqlstate,
        PG::Result::PG_DIAG_MESSAGE_PRIMARY => message,
        PG::Result::PG_DIAG_MESSAGE_HINT => hint,
      }),
      [],
    )
  end

  def wrap(raw, operation: nil, configured_limit: nil)
    Pgque::Client.allocate.__send__(
      :wrap_sql_error, raw,
      operation: operation, configured_limit: configured_limit
    )
  end

  def test_wraps_plain_receive_overflow
    error = wrap(raw_error("pgque.receive: batch exceeds max_return of 17"),
                 operation: "receive", configured_limit: 17)
    assert_kind_of Pgque::ReceiveOverflow, error
    assert_kind_of Pgque::Error, error
    assert_equal "54000", error.sqlstate
    assert_equal "server hint", error.hint
    assert_equal 17, error.configured_limit
    assert_equal "receive", error.operation
  end

  def test_wraps_cooperative_receive_overflow
    error = wrap(raw_error("pgque.receive_coop: batch exceeds max_return of 23"),
                 operation: "receive_coop", configured_limit: 23)
    assert_kind_of Pgque::ReceiveOverflow, error
    assert_equal 23, error.configured_limit
    assert_equal "receive_coop", error.operation
  end

  def test_does_not_misclassify_other_errors
    cases = [
      raw_error("program limit exceeded"),
      raw_error("pgque.send: batch exceeds max_return of 17"),
      raw_error("pgque.receive: batch exceeds max_return of seventeen"),
      raw_error("pgque.receive: batch exceeds max_return of 17", sqlstate: "22023"),
    ]
    cases.each do |raw|
      assert_instance_of Pgque::Error,
                         wrap(raw, operation: "receive", configured_limit: 17)
    end
  end

  def test_matching_server_text_requires_matching_receive_call_context
    raw = raw_error("pgque.receive: batch exceeds max_return of 17")
    contexts = [
      {},
      { operation: "receive_coop", configured_limit: 17 },
      { operation: "receive", configured_limit: 18 },
    ]
    contexts.each do |context|
      assert_instance_of Pgque::Error, wrap(raw, **context)
    end
  end

  def test_consumer_overflow_fails_fast_without_handler_or_ack_and_cleans_up
    overflow = Pgque::ReceiveOverflow.new(
      "pgque.receive: batch exceeds max_return of 3",
      sqlstate: "54000", hint: "increase safely", configured_limit: 3,
      operation: "receive"
    )
    fake_conn = FakeConnection.new
    client = OverflowClient.new(overflow)
    handler_called = false
    consumer = Pgque::Consumer.new("dsn", queue: "q", name: "c", max_messages: 3)
    consumer.on("event") { handler_called = true }

    Pgque::Client.stub(:new, client) do
      PG.stub(:connect, fake_conn) do
        error = assert_raises(Pgque::ReceiveOverflow) { consumer.start }
        assert_same overflow, error
      end
    end

    refute handler_called
    refute consumer.running?
    assert fake_conn.closed
    assert_equal 1, client.receive_calls
    assert_equal 0, client.ack_calls
  end
end
