"""Microbenchmark: cog.spindle_pack vs msgpack for (key, value) payloads.

Run:
    python test/benchmark_spindle_pack.py
    python test/benchmark_spindle_pack.py --iters 2000000

Covers three workload shapes that represent the on-disk format's hot paths:
    - short:  typical graph node/predicate names (~10-20 chars)
    - medium: longer string values (~100 chars)
    - mixed:  realistic triple workload (subj, pred, obj)

Reports packb, unpackb, and round-trip time, plus payload size.
"""
import argparse
import random
import string
import sys
import time

import msgpack

sys.path.insert(0, '.')
from cog import spindle_pack


def _rand_str(n):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))


def make_pairs(n, key_len, val_len):
    random.seed(42)
    return [(_rand_str(key_len), _rand_str(val_len)) for _ in range(n)]


def bench_pack(name, pack_fn, pairs, iters):
    # Warmup
    for k, v in pairs[:100]:
        pack_fn(k, v)

    t0 = time.perf_counter()
    for _ in range(iters // len(pairs)):
        for k, v in pairs:
            pack_fn(k, v)
    t1 = time.perf_counter()
    elapsed = t1 - t0
    ops = (iters // len(pairs)) * len(pairs)
    return elapsed, ops


def bench_unpack(name, unpack_fn, packed, iters):
    # Warmup
    for buf in packed[:100]:
        unpack_fn(buf)

    t0 = time.perf_counter()
    for _ in range(iters // len(packed)):
        for buf in packed:
            unpack_fn(buf)
    t1 = time.perf_counter()
    elapsed = t1 - t0
    ops = (iters // len(packed)) * len(packed)
    return elapsed, ops


def msgpack_pack(k, v):
    return msgpack.packb((k, v), use_bin_type=True)


def msgpack_unpack(buf):
    return msgpack.unpackb(buf, raw=False)


def spindle_pack_pack(k, v):
    return spindle_pack.packb(k, v)


def spindle_pack_unpack(buf):
    return spindle_pack.unpackb(buf)


def run_scenario(name, pairs, iters):
    print(f"\n=== {name}  (sample n={len(pairs)}, iters={iters:,}) ===")

    # Payload sizes
    mp_sample = msgpack_pack(*pairs[0])
    sp_sample = spindle_pack_pack(*pairs[0])
    mp_avg = sum(len(msgpack_pack(k, v)) for k, v in pairs) / len(pairs)
    sp_avg = sum(len(spindle_pack_pack(k, v)) for k, v in pairs) / len(pairs)
    print(f"avg payload: msgpack={mp_avg:.1f}B  spindle_pack={sp_avg:.1f}B  "
          f"(delta={sp_avg - mp_avg:+.1f}B, {(sp_avg/mp_avg - 1)*100:+.1f}%)")
    print(f"sample[0]: msgpack={mp_sample.hex()}")
    print(f"sample[0]: spindle_pack={sp_sample.hex()}")

    # Pack
    mp_t, mp_n = bench_pack("msgpack", msgpack_pack, pairs, iters)
    sp_t, sp_n = bench_pack("spindle_pack", spindle_pack_pack, pairs, iters)
    print(f"packb    msgpack: {mp_n/mp_t:>12,.0f} ops/s   "
          f"spindle_pack: {sp_n/sp_t:>12,.0f} ops/s   "
          f"speedup: {mp_t/sp_t:.2f}x")

    # Build corpus for unpack
    mp_corpus = [msgpack_pack(k, v) for k, v in pairs]
    sp_corpus = [spindle_pack_pack(k, v) for k, v in pairs]

    mp_t, mp_n = bench_unpack("msgpack", msgpack_unpack, mp_corpus, iters)
    sp_t, sp_n = bench_unpack("spindle_pack", spindle_pack_unpack, sp_corpus, iters)
    print(f"unpackb  msgpack: {mp_n/mp_t:>12,.0f} ops/s   "
          f"spindle_pack: {sp_n/sp_t:>12,.0f} ops/s   "
          f"speedup: {mp_t/sp_t:.2f}x")

    # Round-trip
    def rt_mp():
        for k, v in pairs:
            msgpack_unpack(msgpack_pack(k, v))

    def rt_sp():
        for k, v in pairs:
            spindle_pack_unpack(spindle_pack_pack(k, v))

    reps = max(1, iters // len(pairs))
    t0 = time.perf_counter(); [rt_mp() for _ in range(reps)]; mp_t = time.perf_counter() - t0
    t0 = time.perf_counter(); [rt_sp() for _ in range(reps)]; sp_t = time.perf_counter() - t0
    n = reps * len(pairs)
    print(f"rt       msgpack: {n/mp_t:>12,.0f} ops/s   "
          f"spindle_pack: {n/sp_t:>12,.0f} ops/s   "
          f"speedup: {mp_t/sp_t:.2f}x")


def correctness_check():
    """Sanity: round-trip a variety of shapes through spindle_pack."""
    cases = [
        ("", ""),
        ("k", "v"),
        ("hello", "world"),
        ("a" * 127, "b"),              
        ("a" * 128, "b"),              
        ("a" * 65535, "b"),            
        ("a" * 65536, "b"),            
        ("unicode_☃", "snowman_⛄"),
        ("k", 42),                     
        ("k", 42.5),                   
    ]
    for k, v in cases:
        got_k, got_v = spindle_pack.unpackb(spindle_pack.packb(k, v))
        assert got_k == k and got_v == v, f"roundtrip failed: ({k!r}, {v!r}) -> ({got_k!r}, {got_v!r})"
    print("correctness: ok ({} cases)".format(len(cases)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--iters', type=int, default=500_000,
                    help='approximate total ops per scenario')
    args = ap.parse_args()

    correctness_check()

    run_scenario("short str,str  (key=10B, value=20B)",
                 make_pairs(1000, 10, 20), args.iters)
    run_scenario("medium str,str (key=20B, value=100B)",
                 make_pairs(1000, 20, 100), args.iters)
    run_scenario("long str,str   (key=50B, value=1000B)",
                 make_pairs(500, 50, 1000), args.iters)
    run_scenario("triple-like    (key=32B, value=32B)",
                 make_pairs(1000, 32, 32), args.iters)


if __name__ == '__main__':
    main()
