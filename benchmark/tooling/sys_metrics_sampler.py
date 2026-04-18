#!/usr/bin/env python3
"""R7 v2 sys metrics sampler: CPU + RAM + per-device NVMe IOPS / MiB/s / latency.

Columns:
  ts_iso,
  cpu_user_pct, cpu_system_pct, cpu_iowait_pct, cpu_idle_pct,
  mem_total_mb, mem_used_mb, mem_buff_cache_mb, mem_available_mb,
  disk_read_iops, disk_write_iops,
  disk_read_mib_s, disk_write_mib_s,
  disk_read_lat_ms, disk_write_lat_ms

Reads /proc/stat, /proc/meminfo, /proc/diskstats. No external deps.
"""
import argparse, time, os
from datetime import datetime, timezone

def read_proc_stat():
    with open('/proc/stat') as f:
        fields = f.readline().split()
    # fields: cpu user nice system idle iowait irq softirq steal guest guest_nice
    vals = [int(x) for x in fields[1:]]
    return {
        'user': vals[0] + vals[1],       # user + nice
        'system': vals[2] + vals[5] + vals[6],  # system + irq + softirq
        'iowait': vals[4],
        'idle': vals[3],
        'total': sum(vals),
    }

def read_meminfo():
    info = {}
    with open('/proc/meminfo') as f:
        for ln in f:
            key, v = ln.split(':', 1)
            info[key] = int(v.strip().split()[0])  # kB
    total = info['MemTotal'] / 1024
    free  = info['MemFree'] / 1024
    buff  = info.get('Buffers', 0) / 1024
    cache = info.get('Cached', 0) / 1024 + info.get('SReclaimable', 0) / 1024
    avail = info.get('MemAvailable', free) / 1024
    used  = total - avail
    return total, used, buff + cache, avail

def read_diskstat(device):
    """Returns dict for named device, or None."""
    with open('/proc/diskstats') as f:
        for ln in f:
            parts = ln.split()
            if len(parts) < 14: continue
            if parts[2] != device: continue
            return {
                'reads':        int(parts[3]),
                'sectors_read': int(parts[5]),
                'read_ms':      int(parts[6]),
                'writes':       int(parts[7]),
                'sectors_writ': int(parts[9]),
                'write_ms':     int(parts[10]),
            }
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--interval', type=int, default=10)
    ap.add_argument('--duration', type=int, default=5400)
    ap.add_argument('--device', default='nvme1n1')
    ap.add_argument('--out', default='/tmp/bench/sys_metrics.csv')
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    fh = open(args.out, 'w', buffering=1)  # line-buffered
    fh.write('ts_iso,cpu_user_pct,cpu_system_pct,cpu_iowait_pct,cpu_idle_pct,'
             'mem_total_mb,mem_used_mb,mem_buff_cache_mb,mem_available_mb,'
             'disk_read_iops,disk_write_iops,disk_read_mib_s,disk_write_mib_s,'
             'disk_read_lat_ms,disk_write_lat_ms\n')

    prev_cpu = read_proc_stat()
    prev_disk = read_diskstat(args.device) or {}
    t_end = time.monotonic() + args.duration

    while time.monotonic() < t_end:
        time.sleep(args.interval)
        ts = datetime.now(timezone.utc).isoformat()

        # CPU delta
        cur_cpu = read_proc_stat()
        dt_total = cur_cpu['total'] - prev_cpu['total'] or 1
        cpu_user = 100.0 * (cur_cpu['user'] - prev_cpu['user']) / dt_total
        cpu_sys  = 100.0 * (cur_cpu['system'] - prev_cpu['system']) / dt_total
        cpu_iow  = 100.0 * (cur_cpu['iowait'] - prev_cpu['iowait']) / dt_total
        cpu_idle = 100.0 * (cur_cpu['idle'] - prev_cpu['idle']) / dt_total
        prev_cpu = cur_cpu

        mem_total, mem_used, mem_bc, mem_avail = read_meminfo()

        # Disk delta
        cur_disk = read_diskstat(args.device)
        if cur_disk and prev_disk:
            dr = cur_disk['reads'] - prev_disk.get('reads', 0)
            dw = cur_disk['writes'] - prev_disk.get('writes', 0)
            d_sr = cur_disk['sectors_read'] - prev_disk.get('sectors_read', 0)
            d_sw = cur_disk['sectors_writ'] - prev_disk.get('sectors_writ', 0)
            d_rms = cur_disk['read_ms'] - prev_disk.get('read_ms', 0)
            d_wms = cur_disk['write_ms'] - prev_disk.get('write_ms', 0)
            iops_r = dr / args.interval
            iops_w = dw / args.interval
            mib_r = (d_sr * 512) / (args.interval * (1 << 20))
            mib_w = (d_sw * 512) / (args.interval * (1 << 20))
            lat_r = d_rms / dr if dr > 0 else 0
            lat_w = d_wms / dw if dw > 0 else 0
        else:
            iops_r = iops_w = mib_r = mib_w = lat_r = lat_w = 0
        prev_disk = cur_disk or {}

        fh.write(f'{ts},{cpu_user:.2f},{cpu_sys:.2f},{cpu_iow:.2f},{cpu_idle:.2f},'
                 f'{mem_total:.1f},{mem_used:.1f},{mem_bc:.1f},{mem_avail:.1f},'
                 f'{iops_r:.1f},{iops_w:.1f},{mib_r:.3f},{mib_w:.3f},'
                 f'{lat_r:.3f},{lat_w:.3f}\n')
    fh.close()

if __name__ == '__main__':
    main()
