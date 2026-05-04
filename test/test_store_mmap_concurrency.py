"""Concurrency test for Store mmap fast path.

Exercises:
- Reads served by the mapped region (fast path).
- Reads in the unmapped tail (records written but not yet remapped) — these
  must fall through to the fd path and still return correct bytes.
- Reader holding an old mmap snapshot while writer triggers a remap.

The test is correctness-oriented, not perf. It runs N writer threads and M
reader threads concurrently for a fixed wall-clock budget and asserts every
record that was written can still be decoded back to its original (key, value).
"""
import os
import random
import shutil
import threading
import time
import unittest

from cog.core import Table, Record
from cog import config


DIR_NAME = "TestStoreMmapConcurrency"


class TestStoreMmapConcurrency(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = "/tmp/" + DIR_NAME
        if os.path.exists(cls.db_path):
            shutil.rmtree(cls.db_path)
        os.makedirs(cls.db_path + "/test_ns/")
        config.CUSTOM_COG_DB_PATH = cls.db_path

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            shutil.rmtree(cls.db_path)

    def test_concurrent_read_write_across_remap(self):
        # flush_interval=4 so we get many remap events during the run.
        table = Table("mmap_concur", "test_ns", "tid0", config,
                      flush_interval=4)
        store = table.store

        # Shared registry of (position, key, value) tuples, append-only so a
        # reader iterating snapshot indices is safe under concurrent appends.
        written = []
        written_lock = threading.Lock()
        stop = threading.Event()
        errors = []

        def writer(thread_id, count):
            try:
                for i in range(count):
                    key = "k_{}_{}".format(thread_id, i)
                    val = "v_{}_{}_{}".format(thread_id, i, "x" * 32)
                    rec = Record(key=key, value=val, value_type="s")
                    pos = store.save(rec)
                    with written_lock:
                        written.append((pos, key, val))
            except Exception as e:
                errors.append(("writer", thread_id, repr(e)))

        def reader(thread_id, rng):
            try:
                while not stop.is_set():
                    with written_lock:
                        if not written:
                            continue
                        idx = rng.randrange(len(written))
                        pos, key, val = written[idx]
                    rec = store.read(pos)
                    if rec is None:
                        errors.append(("reader", thread_id,
                                       "got None at pos=" + str(pos)))
                        return
                    if rec.key != key or rec.value != val:
                        errors.append(("reader", thread_id,
                                       "mismatch at pos={}: got ({},{}) want ({},{})"
                                       .format(pos, rec.key, rec.value, key, val)))
                        return
            except Exception as e:
                errors.append(("reader", thread_id, repr(e)))

        writers = [threading.Thread(target=writer, args=(i, 500))
                   for i in range(4)]
        readers = [threading.Thread(target=reader, args=(i, random.Random(100 + i)))
                   for i in range(4)]

        for t in writers + readers:
            t.start()

        for t in writers:
            t.join()
        # Let readers chew on the final state for a bit, then stop.
        time.sleep(0.5)
        stop.set()
        for t in readers:
            t.join()

        # Final pass: verify every written record is readable post-flush.
        store.sync()
        for pos, key, val in written:
            rec = store.read(pos)
            self.assertIsNotNone(rec, "missing record at pos=" + str(pos))
            self.assertEqual(rec.key, key)
            self.assertEqual(rec.value, val)

        table.close()
        self.assertEqual(errors, [], "concurrency errors: " + repr(errors[:5]))


if __name__ == "__main__":
    unittest.main()
