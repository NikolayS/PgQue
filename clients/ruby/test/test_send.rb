# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestSend < Minitest::Test
  include PgqueTest::Helpers

  def test_send_returns_int_event_id
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      eid = client.send(queue, { "order_id" => 42 })
      assert_kind_of Integer, eid
      assert_operator eid, :>, 0
    end
  end

  def test_send_with_explicit_type
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      eid = client.send(queue, { "id" => 1 }, type: "order.created")
      assert_kind_of Integer, eid
    end
  end

  def test_send_event_object
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      event = Pgque::Event.new(payload: { "x" => 1 }, type: "custom.t")
      eid = client.send(queue, event)
      assert_kind_of Integer, eid
    end
  end

  def test_send_str_payload_passes_through
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      eid = client.send(queue, '"plain string"')
      assert_kind_of Integer, eid
    end
  end

  def test_send_nil_payload
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      eid = client.send(queue, nil)
      assert_kind_of Integer, eid
    end
  end

  def test_send_batch_returns_ids_in_order
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      ids = client.send_batch(queue, "batch.test", [
        { "n" => 1 }, { "n" => 2 }, { "n" => 3 }, { "n" => 4 }
      ])
      assert_equal 4, ids.size
      assert ids.all? { |i| i.is_a?(Integer) }
      assert_equal ids.sort, ids
    end
  end
end
