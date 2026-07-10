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
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      payload = { "x" => 1 }
      event = Pgque::Event.new(payload: payload, type: "custom.t")
      refute_respond_to event, :extra
      eid = client.send(queue, event)
      assert_kind_of Integer, eid

      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msg = client.receive(queue, consumer, 1).fetch(0)
      assert_equal eid, msg.msg_id
      assert_equal "custom.t", msg.type
      assert_equal payload, msg.payload
      client.ack(msg.batch_id)
    end
  end

  def test_event_rejects_extra_keyword_instead_of_silently_dropping_it
    error = assert_raises(ArgumentError) do
      Pgque::Event.new(
        payload: { "x" => 1 },
        type: "custom.t",
        extra: { "trace_id" => "lost" },
      )
    end
    assert_match(/unknown keyword.*extra/, error.message)
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

  def test_send_numeric_and_boolean_payloads_coerce_via_to_s
    # Non-String/Hash/Array/nil payloads run through to_s so numerics
    # and booleans round-trip naturally as JSON scalars.
    cases = [
      [42,    42],
      [3.14,  3.14],
      [true,  true],
      [false, false],
    ]
    cases.each do |payload, expected|
      with_queue do |queue, consumer, conn|
        client = Pgque::Client.new(conn)
        client.send(queue, payload)
        conn.exec_params("select pgque.force_next_tick($1)", [queue])
        conn.exec_params("select pgque.ticker($1)", [queue])
        msgs = client.receive(queue, consumer, 10)
        assert_equal 1, msgs.size, "no message for #{payload.inspect}"
        assert_equal expected, msgs[0].payload,
                     "#{payload.inspect} did not round-trip"
        client.ack(msgs[0].batch_id)
      end
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

  def test_send_unicode_payload
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      payload = { "text" => "héllo wörld 🎉 — ünicode тест" }
      client.send(queue, payload)
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 10)
      assert_equal 1, msgs.size
      assert_equal payload, msgs[0].payload
      client.ack(msgs[0].batch_id)
    end
  end

  def test_send_large_payload
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      big = { "data" => "x" * 100_000 }
      client.send(queue, big)
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 10)
      assert_equal 1, msgs.size
      assert_equal big, msgs[0].payload
      client.ack(msgs[0].batch_id)
    end
  end

  def test_jsonb_payload_round_trip
    cases = [
      [{ "key" => "val", "n" => 1 }, { "key" => "val", "n" => 1 }],
      [[1, "two", nil],              [1, "two", nil]],
      ['"just a string"',             "just a string"],
      ["42",                          42],
      ["null",                        nil],
    ]
    cases.each do |payload, expected|
      with_queue do |queue, consumer, conn|
        client = Pgque::Client.new(conn)
        client.send(queue, payload)
        conn.exec_params("select pgque.force_next_tick($1)", [queue])
        conn.exec_params("select pgque.ticker($1)", [queue])
        msgs = client.receive(queue, consumer, 10)
        assert_equal 1, msgs.size, "no message for payload=#{payload.inspect}"
        if expected.nil?
          assert_nil msgs[0].payload,
                     "payload=#{payload.inspect} did not round-trip to nil"
        else
          assert_equal expected, msgs[0].payload,
                       "payload=#{payload.inspect} did not round-trip"
        end
        client.ack(msgs[0].batch_id)
      end
    end
  end

  def test_send_batch_mixed_payloads_preserve_order
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      payloads = [{ "a" => 1 }, nil, "42"]
      expected = [{ "a" => 1 }, nil, 42]
      ids = client.send_batch(queue, "batch.mixed", payloads)
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 10)
      assert_equal ids, msgs.map(&:msg_id)
      assert_equal expected, msgs.map(&:payload)
      client.ack(msgs[0].batch_id)
    end
  end

  def test_send_batch_nil_payload_produces_json_null
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      client.send_batch(queue, "default", [nil])
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 10)
      assert_equal 1, msgs.size, "send_batch([nil]) should produce 1 message"
      assert_nil msgs[0].payload, "payload must be JSON null, not SQL NULL"
      client.ack(msgs[0].batch_id)
    end
  end

  def test_send_to_missing_queue_raises
    conn = PG.connect(PGQUE_TEST_DSN)
    begin
      client = Pgque::Client.new(conn)
      assert_raises(Pgque::Error) do
        client.send("does_not_exist_xyz_12345", { "x" => 1 })
      end
    ensure
      conn.close
    end
  end
end

class TestSendSqlForm < Minitest::Test
  # Capture exec_params calls without a real DB.
  class FakeConn
    attr_reader :sql_used, :params_used

    def exec_params(sql, params)
      @sql_used = sql
      @params_used = params
      FakeResult.new
    end

    class FakeResult
      def getvalue(_row, _col)
        "999"
      end
    end
  end

  def test_2arg_form_for_default_type
    [nil, "", "default"].each do |type_val|
      conn = FakeConn.new
      client = Pgque::Client.new(conn)
      eid = client.send("q", { "x" => 1 }, type: type_val)
      assert_equal 999, eid
      assert_includes conn.sql_used, "send($1, $2::jsonb)"
      refute_includes conn.sql_used, "send($1, $2, $3::jsonb)",
                      "type=#{type_val.inspect} should use 2-arg form"
    end
  end

  def test_3arg_form_for_custom_type
    conn = FakeConn.new
    client = Pgque::Client.new(conn)
    eid = client.send("q", { "x" => 1 }, type: "custom")
    assert_equal 999, eid
    assert_includes conn.sql_used, "send($1, $2, $3::jsonb)"
  end
end
