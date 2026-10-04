# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

module Pgque
  class Page
    attr_reader :status, :batch_id, :page_token, :page_number, :is_last,
                :messages, :lease_until, :fence_epoch

    def initialize(status:, batch_id:, page_token:, page_number:, is_last:,
                   messages:, lease_until:, fence_epoch:)
      @status = status
      @batch_id = batch_id
      @page_token = page_token
      @page_number = page_number
      @is_last = is_last
      @messages = messages
      @lease_until = lease_until
      @fence_epoch = fence_epoch
    end
  end

  PageResult = Struct.new(:status, :processed_count, :batch_finished,
                          keyword_init: true)
end
