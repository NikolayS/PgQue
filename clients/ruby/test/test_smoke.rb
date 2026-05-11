# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestSmoke < Minitest::Test
  include PgqueTest::Helpers

  def test_smoke_send_receive_ack
    queue = unique_queue_name
    consumer_n = unique_consumer_name
    Pgque.connect(dsn) do |client|
      client.conn.exec_params("select pgque.create_queue($1)", [queue])
      client.conn.exec_params("select pgque.subscribe($1, $2)", [queue, consumer_n])

      begin
        client.send(queue, { "hello" => "world" }, type: "smoke.test")
        client.conn.exec_params("select pgque.force_next_tick($1)", [queue])
        client.conn.exec_params("select pgque.ticker($1)", [queue])

        msgs = client.receive(queue, consumer_n, 10)
        assert_equal 1, msgs.size
        assert_equal "smoke.test", msgs[0].type
        assert_equal({ "hello" => "world" }, msgs[0].payload)

        client.ack(msgs[0].batch_id)
      ensure
        client.conn.exec_params("select pgque.unregister_consumer($1, $2)",
                                [queue, consumer_n]) rescue nil
        client.conn.exec_params("select pgque.drop_queue($1, true)", [queue]) rescue nil
      end
    end
  end
end
