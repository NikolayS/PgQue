# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require "minitest/autorun"
require "securerandom"
require "pgque"

PGQUE_TEST_DSN = ENV["PGQUE_TEST_DSN"]

module PgqueTest
  module Helpers
    def setup
      skip "PGQUE_TEST_DSN not set" unless PGQUE_TEST_DSN
      super if defined?(super)
    end

    def dsn
      PGQUE_TEST_DSN
    end

    def unique_queue_name
      base = name.to_s.gsub(/[^a-z0-9_]/i, "_")
      "rbt_#{base[0, 40]}_#{SecureRandom.hex(4)}"
    end

    def unique_consumer_name
      base = name.to_s.gsub(/[^a-z0-9_]/i, "_")
      "rbt_c_#{base[0, 38]}_#{SecureRandom.hex(4)}"
    end

    def with_queue
      conn = PG.connect(PGQUE_TEST_DSN)
      q = unique_queue_name
      c = unique_consumer_name
      conn.exec_params("select pgque.create_queue($1)", [q])
      conn.exec_params("select pgque.register_consumer($1, $2)", [q, c])
      yield q, c, conn
    ensure
      if conn && !conn.finished?
        # Reset the connection's transaction state before cleaning up.
        # If the test body left the conn in a failed transaction (an
        # in-flight assertion failure after a SQL error, for example)
        # any subsequent query is rejected until the transaction is
        # rolled back -- which would silently break drop_queue and leak
        # the test queue across runs.
        conn.exec("ROLLBACK") rescue nil
        begin
          conn.exec_params("select pgque.unregister_consumer($1, $2)", [q, c]) if q && c
          conn.exec_params("select pgque.drop_queue($1, true)", [q]) if q
        rescue PG::Error
          # cleanup is best-effort
        end
        conn.close
      end
    end
  end
end
