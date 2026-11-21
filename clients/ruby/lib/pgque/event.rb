# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

module Pgque
  class Event
    attr_reader :payload, :type

    def initialize(payload:, type: "default")
      @payload = payload
      @type = type
    end
  end
end
