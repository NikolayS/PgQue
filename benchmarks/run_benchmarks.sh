#!/usr/bin/env bash
set -Eeuo pipefail

DSN="${1:-postgresql://postgres@localhost/pgque_test}"

echo "=== pgque benchmark suite ==="
echo "DSN: ${DSN}"
echo ""

echo "=== Insert throughput ==="
psql "${DSN}" -f benchmarks/insert_bench.sql 2>&1 | grep -E "NOTICE|Timing"

echo ""
echo "=== Consumer read throughput ==="
psql "${DSN}" -f benchmarks/consumer_bench.sql 2>&1 | grep -E "NOTICE|Timing"

echo ""
echo "=== Dead tuple check ==="
psql "${DSN}" -f benchmarks/dead_tuple_check.sql 2>&1

echo ""
echo "=== Benchmark complete ==="
