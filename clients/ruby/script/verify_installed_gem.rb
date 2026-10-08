# frozen_string_literal: true

# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Run from a clean GEM_HOME, outside the checkout, against installed candidate SQL.
require "pgque"
require "json"
require "securerandom"

abort "installed gem version differs from release input" unless Pgque::VERSION == ENV.fetch("VERSION")
root = File.realpath(Gem.loaded_specs.fetch("pgque").full_gem_path)
install_root = File.realpath(ENV.fetch("GEM_HOME")) + File::SEPARATOR
abort "pgque loaded outside isolated GEM_HOME" unless root.start_with?(install_root)

client = Pgque.connect(ENV.fetch("PGQUE_TEST_DSN"))
conn = client.conn
check = ->(condition, message) { raise message unless condition }
begin
  [false, true].each do |cooperative|
    [false, true].each do |paged|
      queue = "gem_release_#{SecureRandom.hex(8)}"
      begin
        conn.exec_params("select pgque.create_queue($1)", [queue])
        if cooperative
          client.subscribe_subconsumer(queue, "consumer", "sub")
        else
          client.subscribe(queue, "consumer")
        end
        payloads = 3.times.map { |id| {"id" => id, "text" => "installed gem"} }
        ids = [client.send(queue, payloads.first, type: "release.check")]
        ids.concat(client.send_batch(queue, "release.check", payloads.drop(1)))
        client.force_next_tick(queue)
        client.ticker(queue)
        if paged
          receive = lambda do
            if cooperative
              client.receive_page_coop(queue, "consumer", "sub", "worker", page_size: 2)
            else
              client.receive_page(queue, "consumer", "worker", page_size: 2)
            end
          end
          messages = []
          [2, 1].each_with_index do |size, index|
            page = receive.call
            check.call(page.status == "page" && page.messages.size == size, "wrong page size/status")
            repeated = receive.call
            check.call(repeated.page_token == page.page_token && repeated.messages.map(&:msg_id) == page.messages.map(&:msg_id), "page redelivery differs")
            messages.concat(page.messages)
            result = client.ack_page(page.page_token, "worker")
            check.call(result == {status: "acked", batch_finished: index == 1}, "page ack differs")
          end
          check.call(receive.call.status == "idle", "paged queue did not drain")
        else
          receive = lambda do
            cooperative ? client.receive_coop(queue, "consumer", "sub", max_messages: 3) : client.receive(queue, "consumer", 3)
          end
          messages = receive.call
          repeated = receive.call
          check.call(repeated.map(&:msg_id) == ids && repeated.first.batch_id == messages.first.batch_id, "batch redelivery differs")
          check.call(client.ack(messages.first.batch_id) == 1, "ack did not finish batch")
          check.call(receive.call.empty?, "queue did not drain")
        end
        check.call(messages.map(&:msg_id) == ids, "event IDs differ")
        decoded = messages.map { |message| message.payload.is_a?(String) ? JSON.parse(message.payload) : message.payload }
        check.call(decoded == payloads, "payloads differ")
        check.call(messages.all? { |message| message.type == "release.check" }, "event types differ")
        puts "PASS installed gem: cooperative=#{cooperative}, paged=#{paged}, exact IDs/payloads/redelivery/ack/drain"
      ensure
        conn.exec_params("select pgque.drop_queue($1, true)", [queue])
      end
    end
  end
ensure
  client.close
end
