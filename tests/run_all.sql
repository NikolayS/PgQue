-- run_all.sql -- pgque regression test suite
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Usage: psql -d pgque_test -f tests/run_all.sql
-- Requires: pgque.sql already loaded

-- Abort on first error
\set ON_ERROR_STOP on

\echo '=== pgque regression test suite ==='
\echo ''

\echo 'Running: test_pgque_config'
\i tests/test_pgque_config.sql

\echo 'Running: test_pgque_roles'
\i tests/test_pgque_roles.sql

\echo 'Running: test_pgque_additions'
\i tests/test_pgque_additions.sql

\echo 'Running: test_security_definer'
\i tests/test_security_definer.sql

\echo 'Running: test_core_lifecycle'
\i tests/test_core_lifecycle.sql

\echo 'Running: test_core_events'
\i tests/test_core_events.sql

\echo 'Running: test_core_ticker'
\i tests/test_core_ticker.sql

\echo 'Running: test_core_consumer'
\i tests/test_core_consumer.sql

\echo 'Running: test_core_retry'
\i tests/test_core_retry.sql

\echo 'Running: test_core_rotation'
\i tests/test_core_rotation.sql

\echo 'Running: test_pgcron_lifecycle'
\i tests/test_pgcron_lifecycle.sql

\echo 'Running: test_install_idempotency'
\i tests/test_install_idempotency.sql

\echo 'Running: test_status'
\i tests/test_status.sql

\echo 'Running: test_notify'
\i tests/test_notify.sql

\echo 'Running: test_api_send'
\i tests/test_api_send.sql

\echo 'Running: test_api_receive'
\i tests/test_api_receive.sql

\echo 'Running: test_api_dlq'
\i tests/test_api_dlq.sql


\echo ''
\echo '=== ALL TESTS PASSED ==='
