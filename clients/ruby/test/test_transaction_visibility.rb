# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

# PgQ snapshot isolation: events committed in transaction T are only
# visible to a batch whose tick was taken after T committed. Collapsing
# send + force_next_tick + receive into one transaction violates that
# contract. These tests document and enforce it.

require_relative "test_helper"
require "logger"
require "stringio"

class TestTransactionVisibility < Minitest::Test
  include PgqueTest::Helpers

  def silent_logger
    log = Logger.new(StringIO.new)
    log.level = Logger::FATAL
    log
  end

  def test_collapsed_transaction_returns_no_messages
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)

      conn.exec("BEGIN")
      begin
        client.send(queue, { "x" => 1 }, type: "collapsed.test")
        conn.exec_params("select pgque.force_next_tick($1)", [queue])
        conn.exec_params("select pgque.ticker($1)", [queue])
        # No commit between send and receive -- one transaction.
        msgs = client.receive(queue, consumer, 10)
        assert_equal 0, msgs.size,
                     "PgQ visibility violation: collapsed transaction " \
                     "returned #{msgs.size} message(s); expected 0. " \
                     "Add a commit between send and force_next_tick."
      ensure
        conn.exec("ROLLBACK")
      end
    end
  end

  def test_unhandled_event_nack_assertion_catches_stale_cursor
    with_queue do |queue, consumer_n, conn|
      client = Pgque::Client.new(conn)
      msg_id = client.send(queue, { "x" => 1 }, type: "totally.unregistered.type")
      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])

      cons = Pgque::Consumer.new(dsn, queue: queue, name: consumer_n,
                                 poll_interval: 1, logger: silent_logger)
      # Simulate a broken consumer that receives but neither acks nor nacks.
      cons.define_singleton_method(:poll_once) { |_c| }

      t = Thread.new { cons.start }
      sleep 2.0
      cons.stop
      t.join(4.0)

      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      follow = client.receive(queue, consumer_n, 10)

      assert(follow.any? { |m| m.msg_id == msg_id },
             "expected the unprocessed message to still be visible " \
             "(cursor did not advance because poll_once was a no-op), but " \
             "re-receive returned no rows. This indicates the batch cursor " \
             "advanced without an explicit ack -- a PgQ visibility violation.")

      client.ack(follow[0].batch_id) if follow.any?
    end
  end
end
