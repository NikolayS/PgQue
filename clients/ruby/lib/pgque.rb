# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

require "pg"

require "pgque/version"
require "pgque/errors"
require "pgque/client"

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
