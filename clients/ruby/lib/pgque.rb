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
  # Open a connection and return a Pgque::Client.
  #
  # Ruby's pg gem runs each statement in its own implicit transaction
  # by default -- the equivalent of psycopg's autocommit=True. To group
  # statements into one transaction, use conn.transaction { ... } on the
  # underlying PG::Connection (client.conn). There is no autocommit
  # flag because Ruby pg has no per-connection autocommit attribute to
  # toggle; transaction control is per-call via the transaction block.
  def self.connect(dsn)
    client = Client.connect(dsn)
    return client unless block_given?

    begin
      yield client
    ensure
      client.close
    end
  end
end
