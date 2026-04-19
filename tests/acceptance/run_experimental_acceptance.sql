\set ON_ERROR_STOP on
\echo '=== logres experimental acceptance tests ==='
\echo ''
\echo 'Running: US-3 Retry and DLQ flow'
\i tests/acceptance/us3_retry_dlq.sql
\echo ''
\echo 'Running: US-4 Delayed delivery'
\i tests/acceptance/us4_delayed_delivery.sql
\echo ''
\echo 'Running: US-6 Graceful rotation under consumer lag'
\i tests/acceptance/us6_rotation_lag.sql
\echo ''
\echo 'Running: US-9 Observability and health monitoring'
\i tests/acceptance/us9_observability.sql
\echo ''
\echo '=== ALL EXPERIMENTAL ACCEPTANCE TESTS PASSED ==='
