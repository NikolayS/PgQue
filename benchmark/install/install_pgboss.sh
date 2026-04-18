#!/usr/bin/env bash
# pgboss install with covering index for polling query (avoids 7.9 GB disk sort pathology).
set -euo pipefail
echo "=== install pg-boss v12.15.0 + Node worker + covering index ==="

curl -fsSL https://deb.nodesource.com/setup_20.x | sudo bash - >/dev/null 2>&1
sudo apt-get install -y nodejs -qq
sudo npm install -g pg-boss@12.15.0 >/dev/null 2>&1

# Schema + queue creation
sudo NODE_PATH=/usr/lib/node_modules node -e "
const { PgBoss } = require('pg-boss');
const boss = new PgBoss('postgres://postgres@127.0.0.1/bench');
boss.start().then(async () => {
  await boss.createQueue('bench_queue');
  console.log('pgboss schema + bench_queue ready');
  process.exit(0);
}).catch(e => { console.error(e.message); process.exit(1); });
"

# Covering index for the polling query ORDER BY priority DESC, created_on, id WHERE state<'active'
# pgboss.job is PARTITIONED — must target the specific leaf partition for bench_queue.
# The partition name is j<first-40-chars-of-sha1(name)>.
sudo -u postgres psql -d bench > /tmp/pgboss_index.log 2>&1 <<'SQL'
DO $$
DECLARE
  partition_name text;
BEGIN
  -- Find the leaf partition for bench_queue by asking pg_inherits.
  -- bench_queue was created via boss.createQueue; pg-boss 12 creates a named partition per queue.
  SELECT c.relname INTO partition_name
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_class p ON p.oid = i.inhparent
  JOIN pg_namespace np ON np.oid = p.relnamespace
  WHERE np.nspname = 'pgboss' AND p.relname = 'job'
    AND pg_get_expr(c.relpartbound, c.oid) LIKE '%bench_queue%'
  LIMIT 1;

  IF partition_name IS NULL THEN
    -- Fallback: try job_common (shared partition when no dedicated one exists)
    partition_name := 'job_common';
  END IF;

  RAISE NOTICE 'Creating covering index on pgboss.%', partition_name;
  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS idx_bench_polling ON pgboss.%I (name, priority DESC, created_on, id) WHERE state < ''active''',
    partition_name);
END
$$;

-- Verify:
SELECT schemaname, tablename, indexname FROM pg_indexes
WHERE schemaname='pgboss' AND indexname='idx_bench_polling';
SQL
cat /tmp/pgboss_index.log

# Worker script
sudo tee /root/pgboss_worker.js >/dev/null <<'EOF'
const { PgBoss } = require('pg-boss');
const boss = new PgBoss('postgres://postgres@127.0.0.1/bench');
boss.start().then(() => {
  boss.work('bench_queue', { teamSize: 8, pollingIntervalSeconds: 0.5 }, async jobs => Promise.resolve());
  console.log('pgboss worker running');
}).catch(e => { console.error(e); process.exit(1); });
process.on('SIGTERM', () => boss.stop({ graceful: false }).then(() => process.exit(0)));
process.on('SIGINT',  () => boss.stop({ graceful: false }).then(() => process.exit(0)));
EOF

echo "=== pgboss installed + covering index ==="
