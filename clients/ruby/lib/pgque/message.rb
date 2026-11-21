# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

module Pgque
  # A received PgQue event. +type+ and +payload+ are +nil+ when the
  # corresponding SQL +ev_type+ or +ev_data+ value is NULL.
  class Message
    attr_reader :msg_id, :batch_id, :type, :payload, :retry_count,
                :created_at, :extra1, :extra2, :extra3, :extra4

    def initialize(msg_id:, batch_id:, type:, payload:, retry_count:,
                   created_at:, extra1: nil, extra2: nil, extra3: nil,
                   extra4: nil)
      @msg_id = msg_id
      @batch_id = batch_id
      @type = type
      @payload = payload
      @retry_count = retry_count
      @created_at = created_at
      @extra1 = extra1
      @extra2 = extra2
      @extra3 = extra3
      @extra4 = extra4
    end
  end
end
