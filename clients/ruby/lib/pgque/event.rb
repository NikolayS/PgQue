# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

module Pgque
  class Event
    attr_reader :payload, :type, :extra

    def initialize(payload:, type: "default", extra: {})
      @payload = payload
      @type = type
      @extra = extra
    end
  end
end
