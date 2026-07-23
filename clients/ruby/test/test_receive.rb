# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestReceive < Minitest::Test
  include PgqueTest::Helpers

  def test_receive_empty_when_no_tick
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "a" => 1 })
      msgs = client.receive(queue, consumer, 10)
      assert_equal [], msgs
    end
  end

  def test_receive_returns_messages_after_tick
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "key" => "value" })
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 10)
      assert_equal 1, msgs.size
      m = msgs[0]
      refute_nil m.batch_id
      refute_nil m.msg_id
      assert_equal "default", m.type
      assert_equal({ "key" => "value" }, m.payload)
    end
  end

  def test_ack_advances_position
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "k" => 1 })
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 10)
      assert_equal 1, msgs.size
      client.ack(msgs[0].batch_id)
      msgs2 = client.receive(queue, consumer, 10)
      assert_equal [], msgs2
    end
  end

  def test_receive_returns_at_most_max_messages
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      5.times { |i| client.send(queue, { "i" => i }) }
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 3)
      assert_equal 3, msgs.size
      client.ack(msgs[0].batch_id)
    end
  end

  def test_receive_preserves_event_type
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      client.send(queue, { "a" => 1 }, type: "evt.alpha")
      client.send(queue, { "b" => 2 }, type: "evt.beta")
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 10)
      types = msgs.map(&:type).sort
      assert_equal ["evt.alpha", "evt.beta"], types
      client.ack(msgs[0].batch_id)
    end
  end

  def test_receive_preserves_null_type_and_payload
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      event_id = conn.exec_params(
        "select pgque.insert_event($1, null, null)", [queue]
      ).getvalue(0, 0).to_i
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])

      msg = client.receive(queue, consumer, 1).fetch(0)
      assert_equal event_id, msg.msg_id
      assert_nil msg.type
      assert_nil msg.payload
      client.ack(msg.batch_id)
    end
  end

  def test_message_timestamp_round_trip
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      before = Time.now.utc - 5
      client.send(queue, { "x" => 1 })
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      after = Time.now.utc + 5
      msgs = client.receive(queue, consumer, 10)
      assert_equal 1, msgs.size
      assert_kind_of Time, msgs[0].created_at
      assert_operator msgs[0].created_at, :>=, before
      assert_operator msgs[0].created_at, :<=, after
      client.ack(msgs[0].batch_id)
    end
  end
end
