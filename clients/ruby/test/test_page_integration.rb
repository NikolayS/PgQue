# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
require_relative "test_helper"

class TestPageIntegration < Minitest::Test
  include PgqueTest::Helpers

  def test_live_page_round_trip
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      worker = "live-worker"
      ids = 2.times.map { |i| client.send(queue, {"i" => i}, type: "page.live") }
      max_id = 9_223_372_036_854_775_807
      conn.exec_params(
        "select pgque.event_retry_raw($1,$2,now() - interval '1 second',$3," \
        "now(),0,'page.live','{\"i\":\"max\"}',null,null,null,null)",
        [queue, consumer, max_id],
      )
      conn.exec("select pgque.maint_retry_events()")
      client.force_next_tick(queue)
      client.ticker(queue)
      conn.exec_params(
        "update pgque.tick set tick_event_seq = $1 where tick_id = " \
        "(select max(tick_id) from pgque.tick where tick_queue = " \
        "(select queue_id from pgque.queue where queue_name = $2))",
        [max_id, queue],
      )
      assert_raises(RuntimeError) do
        client.process_page(queue, consumer, worker, page_size: 2) { raise "boom" }
      end
      page = client.receive_page(queue, consumer, worker, page_size: 2)
      assert_equal "page", page.status
      refute page.is_last
      assert_equal ids, page.messages.map(&:msg_id)
      assert_operator client.renew_page(page.page_token, worker), :>=, page.lease_until
      seen = []
      result = client.process_page(queue, consumer, worker, page_size: 2) { |m| seen << m.msg_id }
      assert_equal 2, result.processed_count
      refute result.batch_finished
      assert_equal ids, seen
      assert_equal({status: "already_acked", batch_finished: false}, client.ack_page(page.page_token, worker))
      max_page = client.receive_page(queue, consumer, worker, page_size: 2)
      assert_equal [max_id], max_page.messages.map(&:msg_id)
      failures = [{msg_id: max_id.to_s, retry_after_seconds: 0}]
      assert_equal({status: "acked", batch_finished: true}, client.ack_page(max_page.page_token, worker, failures: failures))
      assert_equal({status: "already_acked", batch_finished: true}, client.ack_page(max_page.page_token, worker, failures: failures))
      retry_ids = conn.exec_params(
        "select ev_id from pgque.retry_queue where ev_queue = " \
        "(select queue_id from pgque.queue where queue_name = $1)", [queue],
      ).map { |row| Integer(row["ev_id"]) }
      assert_includes retry_ids, max_id
      idle = client.receive_page(queue, consumer, worker)
      assert_includes ["idle", "advanced"], idle.status
      assert_empty idle.messages
      assert_nil idle.page_token
    end
  end
end
