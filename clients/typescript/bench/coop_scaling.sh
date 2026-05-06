#!/usr/bin/env bash
# pgque -- TypeScript client for PgQue
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
#
# End-to-end driver for the cooperative-consumer scaling benchmark.
#
# Sweeps {1, 2, 4, 8, 16} subconsumers, runs the TypeScript benchmark, and
# renders a PNG via matplotlib.
#
#   PGQUE_TEST_DSN=postgres://nik@localhost/pgque_coop_ts \
#     bash clients/typescript/bench/coop_scaling.sh
#
# Outputs:
#   clients/typescript/bench/coop_scaling.csv  (CSV from the bench)
#   clients/typescript/bench/coop_scaling.png  (rendered chart)
set -Eeuo pipefail

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN not set" >&2
  exit 1
fi

bench_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
events="${PGQUE_BENCH_EVENTS:-5000}"
payload="${PGQUE_BENCH_PAYLOAD:-64}"
runs="${PGQUE_BENCH_RUNS:-3}"
subconsumers="${PGQUE_BENCH_SUBCONSUMERS:-1,2,4,8,16}"
# 0.25ms handler keeps each per-message simulated work small enough that
# the cooperative `FOR UPDATE` allocator stays on the hot path within a
# {1, 2, 4, 8, 16} sweep. With handler work in the millisecond range, 16
# workers are nowhere near saturating the allocator on a developer
# machine — the chart shows pure linear scaling with no plateau, which
# tells half the story. 0.25ms reveals the rise-then-plateau-then-regress
# shape the docs describe.
handler_work_ms="${PGQUE_BENCH_HANDLER_WORK_MS:-0.25}"

csv_out="${bench_dir}/coop_scaling.csv"
png_out="${bench_dir}/coop_scaling.png"

pg_version="$(psql "${PGQUE_TEST_DSN}" -At -c 'select version()' \
  | sed -E 's/ \(.*$//' | sed -E 's/ on .*$//')"
machine="$(uname -srm)"

export PGQUE_BENCH_PG_VERSION="${pg_version}"
export PGQUE_BENCH_MACHINE="${machine}"
export PGQUE_BENCH_EVENTS="${events}"
export PGQUE_BENCH_PAYLOAD="${payload}"
export PGQUE_BENCH_HANDLER_WORK_MS="${handler_work_ms}"

echo "# coop_scaling driver: events=${events} payload=${payload}B runs=${runs} subconsumers=${subconsumers} handler_work_ms=${handler_work_ms}" >&2
echo "# pg_version=${pg_version} machine=${machine}" >&2

bun run "${bench_dir}/coop_scaling.ts" \
  --subconsumers "${subconsumers}" \
  --events "${events}" \
  --payload "${payload}" \
  --runs "${runs}" \
  --handler-work-ms "${handler_work_ms}" \
  | tee "${csv_out}"

python3 "${bench_dir}/plot.py" "${png_out}" < "${csv_out}"
echo "wrote ${png_out}" >&2
