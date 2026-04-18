#!/usr/bin/env bash
# Per-VM microbench: CPU (sysbench), memory (sysbench), disk (fio on NVMe)
set -uo pipefail
HOST=$(hostname)

# Ensure tools installed
sudo apt-get install -q -y sysbench fio >/dev/null 2>&1

echo "=== HOST: $HOST at $(date -u +%FT%TZ) ==="

echo "--- CPU (sysbench cpu --cpu-max-prime=20000 --threads=8 --time=10) ---"
sysbench cpu --cpu-max-prime=20000 --threads=8 --time=10 run 2>/dev/null | grep -E "events per second|total time|avg:|min:|max:"

echo ""
echo "--- Memory (sysbench memory --memory-block-size=1M --memory-total-size=20G --threads=8 --time=10) ---"
sysbench memory --memory-block-size=1M --memory-total-size=20G --threads=8 --time=10 run 2>/dev/null | grep -E "transferred|MiB/sec|total time"

echo ""
echo "--- FIO 4k randwrite iodepth=32 numjobs=4 runtime=15 on NVMe ---"
FIO_FILE=/mnt/pgdata/_microbench_fio_test
sudo fio --name=rand_write --ioengine=libaio --iodepth=32 --rw=randwrite --bs=4k --size=512M \
    --numjobs=4 --time_based --runtime=15 --filename=$FIO_FILE --direct=1 --group_reporting \
    --output-format=normal 2>/dev/null | grep -E "WRITE:|iops|bw" | head -5
sudo rm -f $FIO_FILE

echo ""
echo "--- FIO 1M seq write iodepth=8 runtime=10 on NVMe ---"
sudo fio --name=seq_write --ioengine=libaio --iodepth=8 --rw=write --bs=1M --size=2G \
    --numjobs=1 --time_based --runtime=10 --filename=$FIO_FILE --direct=1 --group_reporting \
    --output-format=normal 2>/dev/null | grep -E "WRITE:|iops|bw" | head -5
sudo rm -f $FIO_FILE

echo "=== DONE: $HOST ==="
