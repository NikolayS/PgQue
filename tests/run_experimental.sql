-- run_experimental.sql -- pgque experimental SQL test suite
-- Requires: pgque.sql already loaded, plus selected experimental SQL loaded explicitly.

\set ON_ERROR_STOP on

\echo '=== pgque experimental test suite ==='
\echo ''

\echo 'Running: test_api_delayed'
\i tests/test_api_delayed.sql

\echo 'Running: test_observability'
\i tests/test_observability.sql

\echo 'Running: test_experimental_config_api'
\i tests/test_experimental_config_api.sql

\echo ''
\echo '=== ALL EXPERIMENTAL TESTS PASSED ==='
