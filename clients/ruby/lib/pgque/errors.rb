# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

module Pgque
  class Error < StandardError; end

  class ReceiveOverflow < Error
    attr_reader :sqlstate, :hint, :configured_limit, :operation

    def initialize(message, sqlstate:, hint:, configured_limit:, operation:)
      super(message)
      @sqlstate = sqlstate
      @hint = hint
      @configured_limit = configured_limit
      @operation = operation
    end
  end

  class ConnectionError < Error; end

  class QueueNotFound < Error; end

  class BatchNotFound < Error; end

  class ConsumerNotFound < Error; end
end
