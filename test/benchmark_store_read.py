"""Cold-read latency benchmark for Store.read.

Writes N records, closes the store, reopens with caching disabled, then times
random Store.read calls. Caching is disabled so we measure the disk read path
(seek + read syscalls today; mmap memcpy after the mmap change), not the
Python-level Cache LRU.

Usage:
    python3 test/benchmark_store_read.py [num_records] [num_reads]

Defaults: 50000 records, 200000 reads.
"""
import os
import random
import shutil
import sys
import time

# Ensure we import the in-tree cog package, not any installed copy.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cog import config as cfg_module
from cog.core import Table


BENCH_DIR = "/tmp/cog_bench_store_read"
NAMESPACE = "bench_ns"
TABLE = "bench_t"


def make_value(i):
    # 60-byte payload — represents a typical small record
    return ("v" + str(i)) * 6


def build_store(num_records):
    if os.path.exists(BENCH_DIR):
        shutil.rmtree(BENCH_DIR)
    os.makedirs(os.path.join(BENCH_DIR, NAMESPACE), exist_ok=True)
    cfg_module.CUSTOM_COG_DB_PATH = BENCH_DIR
    t = Table(TABLE, NAMESPACE, "bench0", cfg_module, flush_interval=1000)
    positions = []
    t.store.begin_batch()
    for i in range(num_records):
        from cog.core import Record
        rec = Record(key="k" + str(i), value=make_value(i), value_type="s")
        pos = t.store.save(rec)
        positions.append(pos)
    t.store.end_batch()
    t.close()
    return positions


def time_reads(positions, num_reads, seed=42):
    cfg_module.CUSTOM_COG_DB_PATH = BENCH_DIR
    t = Table(TABLE, NAMESPACE, "bench0", cfg_module, flush_interval=1)
    # Disable Python-level Cache so reads hit the disk path every call.
    t.store.caching_enabled = False

    rng = random.Random(seed)
    pick = [positions[rng.randrange(len(positions))] for _ in range(num_reads)]

    start = time.perf_counter()
    for pos in pick:
        rec = t.store.read(pos)
        if rec is None:
            raise RuntimeError("unexpected None read at " + str(pos))
    elapsed = time.perf_counter() - start

    t.close()
    return elapsed


def main():
    num_records = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
    num_reads = int(sys.argv[2]) if len(sys.argv) > 2 else 200000

    print(f"building store with {num_records} records...")
    positions = build_store(num_records)
    store_path = os.path.join(BENCH_DIR, NAMESPACE,
                              f"{TABLE}{cfg_module.STORE}bench0")
    size_mb = os.path.getsize(store_path) / (1024 * 1024)
    print(f"store size: {size_mb:.2f} MB")

    # Run twice — first run pays a cold cache cost on the first read pass; the
    # second run reflects steady-state with the OS page cache warm. Report both.
    print(f"timing {num_reads} random reads (run 1, OS page cache cold)...")
    t1 = time_reads(positions, num_reads, seed=42)
    print(f"  elapsed: {t1*1000:.1f} ms  ({num_reads/t1:.0f} ops/s)")

    print(f"timing {num_reads} random reads (run 2, OS page cache warm)...")
    t2 = time_reads(positions, num_reads, seed=43)
    print(f"  elapsed: {t2*1000:.1f} ms  ({num_reads/t2:.0f} ops/s)")


if __name__ == "__main__":
    main()
