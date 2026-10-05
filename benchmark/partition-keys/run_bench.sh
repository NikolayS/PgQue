#!/usr/bin/env bash
# run_bench.sh -- partition-keys read-amplification benchmark (SPEC section 14
# S4) at a high-volume multi-tenant profile: sustained keyed producer + N slot
# workers driving the real pgque v0.8 lease API, measured at N=16 and N=32,
# plus a stalled-slot phase that exercises the R7 rotation-pinning failure mode.
#
# Assumes pgque is already installed into $PGDATABASE (setup_vm.sh does this).
# Everything lands under $OUT (default /tmp/bench/pk); a phases.csv manifest
# ties each phase directory to its metadata for summarize.py.
#
# Knobs (env): RATE, N_SLOTS, DURATION_MIN drive the whole run; the smoke run
# is just those three plus PHASES=1. See README.md.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLING="${TOOLING:-$SCRIPT_DIR/../tooling}"

# --- connection ------------------------------------------------------------
export PGHOST="${PGHOST:-127.0.0.1}"
export PGUSER="${PGUSER:-postgres}"
export PGDATABASE="${PGDATABASE:-bench}"

# --- profile ---------------------------------------------------------------
RATE="${RATE:-5000}"                 # producer sends/s
N_SLOTS="${N_SLOTS:-16}"             # base slot count (steady-16); steady-32 = 2x
DURATION_MIN="${DURATION_MIN:-30}"   # steady-phase minutes
STALL_MIN="${STALL_MIN:-15}"         # stalled-slot phase minutes
STALL_ON_MIN="${STALL_ON_MIN:-2}"    # minute the target slot is SIGSTOPped
STALL_OFF_MIN="${STALL_OFF_MIN:-10}" # minute the target slot is resumed
STALL_SLOT="${STALL_SLOT:-7}"        # which slot to stall
ROTATION="${ROTATION:-30 seconds}"   # queue rotation period (fast => visible floor)
BATCH="${BATCH:-500}"                # receive_partitioned max
TTL_S="${TTL_S:-30}"                 # lease ttl
PGB_C="${PGB_C:-16}"                 # pgbench clients
PGB_J="${PGB_J:-8}"                  # pgbench threads
DEVICE="${DEVICE:-sda}"              # block device for sys_metrics_sampler
PHASES="${PHASES:-1,2,3}"            # which measured phases to run
OUT="${OUT:-/tmp/bench/pk}"

BUN="${BUN:-bun}"
PYTHON="${PYTHON:-python3}"

N1="$N_SLOTS"
N2=$(( N_SLOTS * 2 ))
DUR_S=$(( DURATION_MIN * 60 ))
STALL_DUR_S=$(( STALL_MIN * 60 ))

mkdir -p "$OUT"
MANIFEST="$OUT/phases.csv"
echo "label,queue,consumer,n,rate,dur_s,start_iso,end_iso,dir" > "$MANIFEST"

log() { echo "[$(date -u +%FT%TZ)] $*"; }

psql_q() { psql -X -q -v ON_ERROR_STOP=1 -c "$1"; }

# Kill background jobs of this run on exit; -CONT any stopped worker first.
CLEAN_PIDS=()
cleanup() {
  for p in "${CLEAN_PIDS[@]:-}"; do
    [[ -n "$p" ]] && kill -CONT "$p" 2>/dev/null || true
    [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
  done
}
trap cleanup EXIT

ensure_partitioned() {
  # ensure_partitioned <queue> <consumer> <n>
  local queue="$1" consumer="$2" n="$3" k
  # Force-drop any pre-existing queue: a leftover subscription (e.g. from a
  # smoke run) has a frozen cursor and pins rotation for the WHOLE queue
  # (SPEC R7), silently distorting every measured phase.
  psql -X -q -c "select pgque.drop_queue('$queue', true)" >/dev/null 2>&1 || true
  psql -X -q -c "select pgque.create_queue('$queue')" >/dev/null 2>&1 || true
  psql_q "update pgque.queue set queue_rotation_period = '$ROTATION'::interval where queue_name = '$queue'"
  for (( k = 0; k < n; k++ )); do
    psql_q "select pgque.subscribe_slot('$queue', '$consumer', $k, $n)" >/dev/null
  done
  log "queue '$queue' ready: consumer '$consumer', $n slots, rotation '$ROTATION'"
}

rest_slots_spec() {
  # rest_slots_spec <stall_slot> <n>  -> prints slot spec excluding stall_slot
  local s="$1" n="$2"
  if (( s == 0 )); then
    echo "1-$(( n - 1 ))"
  elif (( s == n - 1 )); then
    echo "0-$(( n - 2 ))"
  else
    echo "0-$(( s - 1 )),$(( s + 1 ))-$(( n - 1 ))"
  fi
}

run_phase() {
  # run_phase <label> <queue> <consumer> <n> <dur_s> <stall_slot|-1>
  local label="$1" queue="$2" consumer="$3" n="$4" dur="$5" stall="$6"
  local dir="$OUT/$label"
  mkdir -p "$dir"

  # Render the producer script with the live queue name.
  sed "s/@QUEUE@/$queue/g" "$SCRIPT_DIR/producer.sql" > "$dir/producer.sql"

  psql_q "select pg_stat_statements_reset()" >/dev/null
  local start_iso
  start_iso="$(date -u +%FT%TZ)"
  log "PHASE $label start: queue=$queue consumer=$consumer n=$n rate=$RATE dur=${dur}s stall_slot=$stall"

  # --- samplers ------------------------------------------------------------
  QUEUE="$queue" CONSUMER="$consumer" INTERVAL=5 DURATION="$dur" OUTDIR="$dir" \
    bash "$SCRIPT_DIR/slot_status_sampler.sh" >"$dir/slot_status_sampler.log" 2>&1 &
  CLEAN_PIDS+=($!)
  "$PYTHON" "$TOOLING/sys_metrics_sampler.py" --interval 10 --duration "$dur" \
    --device "$DEVICE" --out "$dir/sys_metrics.csv" >"$dir/sys_metrics.log" 2>&1 &
  CLEAN_PIDS+=($!)
  "$PYTHON" "$TOOLING/pg_stat_statements_snapshot.py" --interval 10 --duration "$dur" \
    --dsn "host=$PGHOST dbname=$PGDATABASE user=$PGUSER" \
    --out "$dir/pgss_timeseries.csv" >"$dir/pgss_ts.log" 2>&1 &
  CLEAN_PIDS+=($!)
  DSN="host=$PGHOST dbname=$PGDATABASE user=$PGUSER" \
    "$PYTHON" "$TOOLING/bloat_sampler.py" --system pgque --interval 30 --duration "$dur" \
    >"$dir/bloat.csv" 2>"$dir/bloat.log" &
  CLEAN_PIDS+=($!)

  # --- ticker --------------------------------------------------------------
  RUN_S="$dur" TICK_MS=250 MAINT_S=60 \
    "$PYTHON" "$SCRIPT_DIR/pk_ticker.py" >"$dir/ticker.log" 2>&1 &
  CLEAN_PIDS+=($!)

  # --- producer ------------------------------------------------------------
  pgbench -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -n -f "$dir/producer.sql" \
    -c "$PGB_C" -j "$PGB_J" -R "$RATE" -T "$dur" -P 30 \
    --aggregate-interval=10 --log --log-prefix="$dir/producer_agg" \
    >"$dir/producer.log" 2>&1 &
  local prod_pid=$!
  CLEAN_PIDS+=("$prod_pid")

  # --- consumers -----------------------------------------------------------
  local acks="$dir/acks.log"
  : > "$acks"
  if (( stall < 0 )); then
    QUEUE="$queue" CONSUMER="$consumer" N_SLOTS="$n" SLOTS="0-$(( n - 1 ))" \
      BATCH="$BATCH" TTL_S="$TTL_S" LOG="$acks" RUN_S="$dur" \
      "$BUN" "$SCRIPT_DIR/slot_worker.ts" >"$dir/worker.log" 2>&1 &
    CLEAN_PIDS+=($!)
  else
    # Rest of the slots as one process; the target slot as its own process so
    # it can be SIGSTOPped (a genuine stalled worker -- SPEC R7).
    local rest
    rest="$(rest_slots_spec "$stall" "$n")"
    QUEUE="$queue" CONSUMER="$consumer" N_SLOTS="$n" SLOTS="$rest" \
      BATCH="$BATCH" TTL_S="$TTL_S" LOG="$acks" RUN_S="$dur" \
      "$BUN" "$SCRIPT_DIR/slot_worker.ts" >"$dir/worker_rest.log" 2>&1 &
    CLEAN_PIDS+=($!)
    QUEUE="$queue" CONSUMER="$consumer" N_SLOTS="$n" SLOTS="$stall" \
      BATCH="$BATCH" TTL_S="$TTL_S" LOG="$acks" RUN_S="$dur" \
      "$BUN" "$SCRIPT_DIR/slot_worker.ts" >"$dir/worker_stall.log" 2>&1 &
    local stall_pid=$!
    CLEAN_PIDS+=("$stall_pid")
    # Schedule the stall window: STOP at STALL_ON_MIN, CONT at STALL_OFF_MIN.
    (
      sleep $(( STALL_ON_MIN * 60 ))
      echo "[$(date -u +%FT%TZ)] SIGSTOP slot $stall (pid $stall_pid)" >>"$dir/stall.log"
      kill -STOP "$stall_pid" 2>/dev/null || true
      sleep $(( (STALL_OFF_MIN - STALL_ON_MIN) * 60 ))
      echo "[$(date -u +%FT%TZ)] SIGCONT slot $stall (pid $stall_pid)" >>"$dir/stall.log"
      kill -CONT "$stall_pid" 2>/dev/null || true
    ) &
    CLEAN_PIDS+=($!)
  fi

  # Producer bounds the phase (-T). Consumers/samplers self-terminate at RUN_S.
  wait "$prod_pid" 2>/dev/null || true
  log "PHASE $label producer done; letting consumers/samplers flush"
  # The consumers and samplers self-terminate at RUN_S/--duration; give them a
  # moment to flush their last writes before the boundary snapshot.
  sleep 5

  # --- read-amp boundary snapshot -----------------------------------------
  # Under pg_stat_statements.track=top the event-table scan buffers roll up
  # into the top-level receive_partitioned call, so these normalized rows are
  # the read-amp measurement (no per-batch cursor eviction).
  psql -X -q -c "\copy (
      select left(regexp_replace(query, '\s+', ' ', 'g'), 80) as query_head,
             calls, rows, shared_blks_hit, shared_blks_read,
             round(total_exec_time::numeric, 1) as total_exec_time_ms
        from pg_stat_statements
       where query ~ 'receive_partitioned|ack_partitioned|claim_slot|release_slot|pgque\.ticker|pgque\.send|pgque\.maint'
       order by shared_blks_hit + shared_blks_read desc
    ) to '$dir/pgss_end.csv' csv header" >/dev/null 2>&1 || true

  local end_iso
  end_iso="$(date -u +%FT%TZ)"
  echo "$label,$queue,$consumer,$n,$RATE,$dur,$start_iso,$end_iso,$dir" >> "$MANIFEST"
  log "PHASE $label end"

  # Reap this phase's background jobs before the next phase.
  cleanup
  CLEAN_PIDS=()
  wait 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Phase dispatch
# ---------------------------------------------------------------------------
has_phase() { [[ ",$PHASES," == *",$1,"* ]]; }

log "=== partition-keys bench: RATE=$RATE N1=$N1 N2=$N2 DURATION_MIN=$DURATION_MIN PHASES=$PHASES ==="

if has_phase 1; then
  ensure_partitioned "bench_q" "w$N1" "$N1"
  run_phase "steady-$N1" "bench_q" "w$N1" "$N1" "$DUR_S" -1
fi

if has_phase 2; then
  ensure_partitioned "bench_q32" "w$N2" "$N2"
  run_phase "steady-$N2" "bench_q32" "w$N2" "$N2" "$DUR_S" -1
fi

if has_phase 3; then
  # Reuse (or create) the N1 queue; stall one slot mid-phase.
  ensure_partitioned "bench_q" "w$N1" "$N1"
  run_phase "stalled-$N1" "bench_q" "w$N1" "$N1" "$STALL_DUR_S" "$STALL_SLOT"
fi

log "=== all phases done; summarizing ==="
"$PYTHON" "$SCRIPT_DIR/summarize.py" --out "$OUT" > "$OUT/summary.md" 2>"$OUT/summarize.log" || {
  log "summarize failed; see $OUT/summarize.log"
}
log "summary written to $OUT/summary.md"
cat "$OUT/summary.md" 2>/dev/null || true
