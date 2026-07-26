#!/usr/bin/env bash
# slot_status_sampler.sh -- per-slot lease + lag sampler for the partition-keys
# bench. Every INTERVAL seconds it appends two CSVs:
#
#   $OUTDIR/slot_status.csv   ts,queue,consumer,slot,lease_owner,epoch,pending_events
#       one row per slot of the consumer, from pgque.partition_slot_status --
#       this is the R7 rotation-pinning signal (a stalled slot shows a growing
#       pending_events and, after its lease expires, a null lease_owner).
#
#   $OUTDIR/queue_rate.csv    ts,queue,ev_per_sec,ev_new,last_tick_id,ntables,cur_table
#       queue-level throughput + table count from pgque.get_queue_info(queue).
#       ntables/cur_table are the rotation-floor evidence: a pinned slot keeps
#       the engine from dropping old event_N_M tables, so ntables climbs.
set -Eeuo pipefail

QUEUE="${QUEUE:-bench_q}"
CONSUMER="${CONSUMER:-w16}"
INTERVAL="${INTERVAL:-5}"
DURATION="${DURATION:-3600}"
OUTDIR="${OUTDIR:-/tmp/bench/pk}"
PGHOST="${PGHOST:-127.0.0.1}"
PGDATABASE="${PGDATABASE:-bench}"
PGUSER="${PGUSER:-postgres}"
export PGHOST PGDATABASE PGUSER

mkdir -p "$OUTDIR"
slot_csv="$OUTDIR/slot_status.csv"
rate_csv="$OUTDIR/queue_rate.csv"
[[ -f "$slot_csv" ]] || echo "ts,queue,consumer,slot,lease_owner,epoch,pending_events" > "$slot_csv"
[[ -f "$rate_csv" ]] || echo "ts,queue,ev_per_sec,ev_new,last_tick_id,ntables,cur_table" > "$rate_csv"

end=$(( $(date +%s) + DURATION ))
while [[ $(date +%s) -lt $end ]]; do
  ts=$(date -u +%FT%TZ)

  psql -X -q -At -F',' -d "$PGDATABASE" \
    -c "select '${ts}', queue_name, consumer, slot, coalesce(lease_owner,''), epoch, pending_events
          from pgque.partition_slot_status
         where queue_name = '${QUEUE}' and consumer = '${CONSUMER}'
         order by slot" \
    >> "$slot_csv" 2>/dev/null || true

  psql -X -q -At -F',' -d "$PGDATABASE" \
    -c "select '${ts}', queue_name, round(ev_per_sec::numeric, 1), ev_new, last_tick_id,
               queue_ntables, queue_cur_table
          from pgque.get_queue_info('${QUEUE}')" \
    >> "$rate_csv" 2>/dev/null || true

  sleep "$INTERVAL"
done
