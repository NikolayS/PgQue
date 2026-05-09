# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

module Pgque
  class Error < StandardError; end

  class ConnectionError < Error; end
end
