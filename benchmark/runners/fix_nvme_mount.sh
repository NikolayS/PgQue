#!/usr/bin/env bash
# Format + mount NVMe instance store, move PG data dir to NVMe, re-setup symlink.
set -Eeuo pipefail
DEV=/dev/nvme1n1
MNT=/mnt/pgdata
PG_DATA_SRC=/var/lib/postgresql/18/main
PG_DATA_DST=$MNT/postgresql/18/main

echo "=== pre-flight ==="
lsblk | grep nvme1n1
findmnt $MNT 2>&1 | head -1 || echo "$MNT not mounted"

# Stop PG
echo "=== stopping postgresql ==="
sudo systemctl stop postgresql@18-main || true
sleep 2

# Format if needed
if ! sudo blkid $DEV 2>&1 | grep -q xfs; then
  echo "=== formatting $DEV as xfs ==="
  sudo mkfs.xfs -f $DEV
fi

# Mount
echo "=== mounting $DEV → $MNT (noatime,nodiratime) ==="
sudo mkdir -p $MNT
# /mnt/pgdata may currently be a non-empty dir (has the 'bench' symlink target).
# Move contents aside first.
sudo mkdir -p /tmp/_pgdata_old
sudo mv $MNT/* /tmp/_pgdata_old/ 2>/dev/null || true
sudo mount -o noatime,nodiratime $DEV $MNT
# Restore any content (e.g. bench dir)
sudo mv /tmp/_pgdata_old/* $MNT/ 2>/dev/null || true
sudo rmdir /tmp/_pgdata_old 2>/dev/null || true

# Create PG data path on NVMe
sudo mkdir -p $MNT/postgresql/18
sudo chown postgres:postgres $MNT/postgresql $MNT/postgresql/18

# Move current data (if symlink points somewhere, follow)
REAL_SRC=$(sudo readlink -f $PG_DATA_SRC)
echo "=== PG data currently at: $REAL_SRC (mv to $PG_DATA_DST) ==="
if [ -d "$REAL_SRC" ] && [ "$REAL_SRC" != "$PG_DATA_DST" ]; then
  sudo mv "$REAL_SRC" $PG_DATA_DST
fi
# Remove the source (it's either a symlink now or gone)
sudo rm -rf $PG_DATA_SRC
# Make parent
sudo mkdir -p $(dirname $PG_DATA_SRC)
# Symlink
sudo ln -s $PG_DATA_DST $PG_DATA_SRC
sudo chown -h postgres:postgres $PG_DATA_SRC

# Start PG
echo "=== starting postgresql ==="
sudo systemctl start postgresql@18-main
sleep 3
sudo systemctl status postgresql@18-main --no-pager | head -5

# Verify
echo "=== verify ==="
findmnt $MNT
df $MNT
sudo -u postgres psql -tAc "SHOW data_directory"
sudo stat --format='%F %N' $PG_DATA_SRC
