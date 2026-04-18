#!/usr/bin/env bash
# Per-system clean reinstall + stats reset.
set -uo pipefail
SYS=${1:?system}

echo "=== [$(date -u +%FT%TZ)] clean reinstall: $SYS ==="

# Kill any leftover processes
sudo pkill -f 'idle_in_tx' 2>/dev/null || true
sudo pkill -f 'bench.py' 2>/dev/null || true
sudo pkill -f 'bloat_sampler' 2>/dev/null || true
sudo pkill -f 'pgq_ticker_daemon' 2>/dev/null || true
sudo pkill -f 'bench_worker' 2>/dev/null || true
sudo pkill -f 'que_worker' 2>/dev/null || true
sudo pkill -f 'pgboss_worker' 2>/dev/null || true
sleep 2

# Terminate any dangling backends
sudo -u postgres psql -d bench -c "
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE datname='bench' AND pid<>pg_backend_pid()
    AND (state='idle in transaction' OR application_name LIKE 'idle_in_tx%' OR application_name LIKE 'pgq_ticker%');
" >/dev/null 2>&1

# Drop queue-specific artifacts
case $SYS in
  pgque)
    sudo -u postgres psql -d bench -c "DROP SCHEMA IF EXISTS pgque CASCADE" >/dev/null
    # Also unschedule any leftover pg_cron jobs named pgque_*
    sudo -u postgres psql -d bench -Atc "SELECT jobname FROM cron.job WHERE jobname LIKE 'pgque%'" | while read j; do
      [ -n "$j" ] && sudo -u postgres psql -d bench -c "SELECT cron.unschedule('$j')" >/dev/null
    done
    ;;
  pgq)
    sudo -u postgres psql -d bench -c "DROP EXTENSION IF EXISTS pgq CASCADE; DROP SCHEMA IF EXISTS pgq CASCADE" >/dev/null
    sudo -u postgres psql -d bench -Atc "SELECT jobname FROM cron.job WHERE jobname LIKE 'pgq%'" | while read j; do
      [ -n "$j" ] && sudo -u postgres psql -d bench -c "SELECT cron.unschedule('$j')" >/dev/null
    done
    ;;
  pgmq)
    sudo -u postgres psql -d bench -c "DROP SCHEMA IF EXISTS pgmq CASCADE" >/dev/null
    ;;
  river)
    sudo -u postgres psql -d bench -c "
      DO \$\$ DECLARE t text;
      BEGIN
        FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'river_%'
        LOOP EXECUTE 'DROP TABLE IF EXISTS '||t||' CASCADE'; END LOOP;
      END \$\$;
    " >/dev/null
    ;;
  que)
    sudo -u postgres psql -d bench -c "
      DO \$\$ DECLARE t text;
      BEGIN
        FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'que_%'
        LOOP EXECUTE 'DROP TABLE IF EXISTS '||t||' CASCADE'; END LOOP;
      END \$\$;
    " >/dev/null
    ;;
  pgboss)
    sudo -u postgres psql -d bench -c "DROP SCHEMA IF EXISTS pgboss CASCADE" >/dev/null
    ;;
esac

# Re-run install
bash /tmp/install.sh

# Full stats reset AFTER reinstall so the reinstall's own writes don't count
sudo -u postgres psql -d bench -f /tmp/full_reset.sql >/dev/null

# ANALYZE for fresh stats
sudo -u postgres psql -d bench -c "ANALYZE" >/dev/null

echo "=== [$(date -u +%FT%TZ)] clean reinstall done: $SYS ==="
