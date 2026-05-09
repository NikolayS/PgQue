# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

module Pgque
  class Client
    attr_reader :conn

    def self.connect(dsn, autocommit: false)
      conn = PG.connect(dsn)
      new(conn, owns_conn: true, autocommit: autocommit)
    rescue PG::ConnectionBad => e
      raise ConnectionError, e.message
    end

    def initialize(conn, owns_conn: false, autocommit: false)
      @conn = conn
      @owns_conn = owns_conn
      @autocommit = autocommit
    end

    def autocommit?
      @autocommit
    end

    def close
      return unless @owns_conn
      return if @conn.finished?
      @conn.close
    end
  end
end
