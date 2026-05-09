# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

require "json"
require "time"
require "pg"

require "pgque/version"
require "pgque/errors"
require "pgque/event"
require "pgque/message"
require "pgque/client"
require "pgque/consumer"

module Pgque
  def self.connect(dsn, autocommit: false)
    client = Client.connect(dsn, autocommit: autocommit)
    return client unless block_given?

    begin
      yield client
    ensure
      client.close
    end
  end
end
