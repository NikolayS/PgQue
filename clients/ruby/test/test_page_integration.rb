# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
require_relative "test_helper"

class TestPageIntegration < Minitest::Test
  include PgqueTest::Helpers

  def test_page_mode_adapters
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      coop, member, coop_worker = "#{consumer}_group", "member-one", "coop-process"
      partitioned, partition_worker = "#{consumer}_partitioned", "partition-process"
      conn.exec_params("select pgque.register_subconsumer($1,$2,$3)", [queue, coop, member])
      conn.exec_params("select pgque.subscribe_partitioned($1,$2,3)", [queue, partitioned])
      epoch = Integer(conn.exec_params(
        "select pgque.claim_slot($1,$2,2,$3,interval '3 minutes')",
        [queue, partitioned, partition_worker],
      ).getvalue(0, 0))
      keys = [0, 2].to_h do |slot|
        key = conn.exec_params(
          "select 'adapter-key-' || i from generate_series(1,1000) as i " \
          "where (hashtextextended('adapter-key-' || i,0) % 3 + 3) % 3 = $1 " \
          "order by i limit 1", [slot],
        ).getvalue(0, 0)
        [slot, key]
      end
      payloads = ["target-first", "other-slot", "target-last"]
      event_keys = [keys[2], keys[0], keys[2]]
      ids = payloads.zip(event_keys).map do |payload, key|
        Integer(conn.exec_params(
          "select pgque.send($1,'adapter.mode',$2::text,$3::text)",
          [queue, payload, key],
        ).getvalue(0, 0))
      end
      client.force_next_tick(queue)
      client.ticker(queue)
      check_page = lambda do |page, indexes|
        assert_equal "page", page.status
        refute_nil page.page_token
        refute_empty page.page_token
        refute_nil page.batch_id
        assert_equal 1, page.page_number
        assert page.is_last
        refute_nil page.lease_until
        assert_equal indexes.map { |i| ids[i] }, page.messages.map(&:msg_id)
        assert_equal indexes.map { |i| payloads[i] }, page.messages.map(&:payload)
        assert_equal indexes.map { |i| event_keys[i] }, page.messages.map(&:extra1)
        assert page.messages.all? { |m| m.batch_id == page.batch_id && m.type == "adapter.mode" }
      end
      check_ack = lambda do |page, worker|
        error = assert_raises(Pgque::Error) { client.ack_page(page.page_token, "not-the-owner") }
        assert_match(/wrong worker/, error.message)
        assert_equal({status: "acked", batch_finished: true}, client.ack_page(page.page_token, worker))
        assert_equal({status: "already_acked", batch_finished: true}, client.ack_page(page.page_token, worker))
      end
      coop_page = client.receive_page_coop(
        queue, coop, member, coop_worker, page_size: 3,
        dead_interval: nil, lease: "3 minutes",
      )
      check_page.call(coop_page, [0, 1, 2])
      assert_nil coop_page.fence_epoch
      check_ack.call(coop_page, coop_worker)
      coop_empty = client.receive_page_coop(
        queue, coop, member, coop_worker, page_size: 3,
        dead_interval: "5 minutes", lease: "3 minutes",
      )
      assert_includes ["idle", "advanced"], coop_empty.status
      assert_empty coop_empty.messages
      assert_nil coop_empty.page_token

      partition_page = client.receive_page_partitioned(
        queue, partitioned, 2, 3, partition_worker, page_size: 2,
      )
      check_page.call(partition_page, [0, 2])
      assert_equal epoch, partition_page.fence_epoch
      check_ack.call(partition_page, partition_worker)
      partition_empty = client.receive_page_partitioned(
        queue, partitioned, 2, 3, partition_worker, page_size: 2,
      )
      assert_includes ["idle", "advanced"], partition_empty.status
      assert_empty partition_empty.messages
      assert_nil partition_empty.page_token
    end
  end

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
