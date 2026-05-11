# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestNack < Minitest::Test
  include PgqueTest::Helpers

  def enqueue_and_receive(client, queue, consumer, payload, conn)
    client.send(queue, payload)
    conn.exec_params("select pgque.force_next_tick($1)", [queue])
    conn.exec_params("select pgque.ticker($1)", [queue])
    msgs = client.receive(queue, consumer, 10)
    assert_equal 1, msgs.size
    msgs[0]
  end

  def test_nack_routes_to_retry_queue
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      msg = enqueue_and_receive(client, queue, consumer, { "k" => "retry" }, conn)
      client.nack(msg.batch_id, msg, retry_after: 0)
      client.ack(msg.batch_id)

      retry_count = conn.exec_params(
        "select count(*) from pgque.retry_queue rq " \
        "join pgque.queue q on q.queue_id = rq.ev_queue " \
        "where q.queue_name = $1",
        [queue],
      ).values[0][0].to_i
      assert_equal 1, retry_count

      dlq_count = conn.exec_params(
        "select count(*) from pgque.dead_letter dl " \
        "join pgque.queue q on q.queue_id = dl.dl_queue_id " \
        "where q.queue_name = $1",
        [queue],
      ).values[0][0].to_i
      assert_equal 0, dlq_count
    end
  end

  def test_nack_routes_to_dlq_at_max_retries
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      conn.exec_params(
        "update pgque.queue set queue_max_retries = 0 where queue_name = $1",
        [queue],
      )

      msg = enqueue_and_receive(client, queue, consumer, { "k" => "doomed" }, conn)
      client.nack(msg.batch_id, msg, retry_after: 0, reason: "poison pill")
      client.ack(msg.batch_id)

      dlq_count = conn.exec_params(
        "select count(*) from pgque.dead_letter dl " \
        "join pgque.queue q on q.queue_id = dl.dl_queue_id " \
        "where q.queue_name = $1",
        [queue],
      ).values[0][0].to_i
      assert_equal 1, dlq_count
    end
  end

  def test_nack_invalid_batch_raises
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      msg = enqueue_and_receive(client, queue, consumer, { "x" => 1 }, conn)
      client.ack(msg.batch_id)

      assert_raises(Pgque::Error) do
        client.nack(msg.batch_id, msg, retry_after: 0)
      end
    end
  end
end
