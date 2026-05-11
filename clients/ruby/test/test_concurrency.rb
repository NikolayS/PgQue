# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestConcurrency < Minitest::Test
  include PgqueTest::Helpers

  def test_concurrent_producers_no_id_collisions
    with_queue do |queue, _consumer, _conn|
      n_threads = 4
      per_thread = 25
      seen_ids = []
      seen_lock = Mutex.new

      threads = n_threads.times.map do
        Thread.new do
          ids = []
          Pgque.connect(dsn) do |client|
            per_thread.times do |i|
              ids << client.send(
                queue,
                { "thread" => Thread.current.object_id, "i" => i },
              )
            end
          end
          seen_lock.synchronize { seen_ids.concat(ids) }
        end
      end
      threads.each { |t| refute_nil t.join(30), "producer thread hung" }

      assert_equal n_threads * per_thread, seen_ids.size
      assert_equal seen_ids.size, seen_ids.uniq.size,
                   "duplicate event ids: #{seen_ids - seen_ids.uniq}"
    end
  end
end
