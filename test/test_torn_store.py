"""A store truncated by a crash or disk fault must degrade gracefully:
reads return the intact records (or None), never raise, and the scanner
keeps yielding the buckets that are still readable."""

import glob
import os
import shutil
import unittest

from cog.database import Cog
from cog.core import Record
from cog.config import CogConfig

DB_PATH = "/tmp/TestTornStore"
N_KEYS = 200


class TornStoreTest(unittest.TestCase):

    def setUp(self):
        shutil.rmtree(DB_PATH, ignore_errors=True)
        os.makedirs(DB_PATH)
        c = Cog(config=CogConfig(COG_PATH_PREFIX=DB_PATH, COG_HOME="home",
                                 CUSTOM_COG_DB_PATH=None))
        c.create_or_load_namespace("ns")
        c.create_table("t", "ns")
        self.truth = {}
        for i in range(N_KEYS):
            k, v = "key_%05d" % i, "value_%d" % i
            c.put(Record(k, v))
            self.truth[k] = v
        for e in ("a", "b", "c"):
            c.put_list(Record("alist", e))
        c.sync()
        c.close()
        self.store_path = glob.glob(os.path.join(DB_PATH, "home", "ns", "t-store-*"))[0]
        self.full_size = os.path.getsize(self.store_path)

    def tearDown(self):
        shutil.rmtree(DB_PATH, ignore_errors=True)

    def _reopen(self):
        c = Cog(config=CogConfig(COG_PATH_PREFIX=DB_PATH, COG_HOME="home",
                                 CUSTOM_COG_DB_PATH=None))
        c.create_or_load_namespace("ns")
        c.use_table("t")
        return c

    def _check_truncated(self, new_size):
        with open(self.store_path, 'r+b') as f:
            f.truncate(new_size)
        c = self._reopen()
        try:
            served = 0
            for k, expected in self.truth.items():
                rec = c.get(k)  # must not raise
                if rec is not None:
                    self.assertEqual(rec.value, expected)
                    served += 1
            rec = c.get("alist")
            if rec is not None:
                self.assertTrue(set(rec.value).issubset({"a", "b", "c"}))
            scanned = {}
            for r in c.scanner():  # must terminate without raising
                scanned[r.key] = r.value
            for k, v in scanned.items():
                if k in self.truth:
                    self.assertEqual(v, self.truth[k])
            return served, len(scanned)
        finally:
            c.close()

    def test_truncation_points(self):
        for cut in (3, 10, 40):
            with self.subTest(cut=cut):
                self.setUp()
                served, scanned = self._check_truncated(self.full_size - cut)
                # only the tail record(s) may be affected
                self.assertGreaterEqual(served, N_KEYS - 3)
                self.assertGreaterEqual(scanned, N_KEYS - 3)
                self.tearDown()
        # rebuild for the outer tearDown
        self.setUp()

    def test_half_truncated_store_still_serves_intact_records(self):
        served, scanned = self._check_truncated(self.full_size // 2)
        self.assertGreater(served, 0, "no intact record served from half store")
        self.assertGreater(scanned, 0)

    def test_writes_still_work_after_truncation(self):
        with open(self.store_path, 'r+b') as f:
            f.truncate(self.full_size - 10)
        c = self._reopen()
        try:
            c.put(Record("post_crash_key", "pc_value"))
            c.sync()
            rec = c.get("post_crash_key")
            self.assertIsNotNone(rec)
            self.assertEqual(rec.value, "pc_value")
        finally:
            c.close()


class CorruptRecordTest(unittest.TestCase):
    """Content corruption (bit rot, torn sector) inside one record — with all
    length fields still consistent — must degrade to that record reading as
    missing. It must never raise out of get(), and must not blind scanner()
    to the rest of the table."""

    def setUp(self):
        shutil.rmtree(DB_PATH, ignore_errors=True)
        os.makedirs(DB_PATH)
        c = Cog(config=CogConfig(COG_PATH_PREFIX=DB_PATH, COG_HOME="home",
                                 CUSTOM_COG_DB_PATH=None))
        c.create_or_load_namespace("ns")
        c.create_table("t", "ns")
        c.put(Record("good_key", "good_value"))
        self.victim_value = "corruptme_payload_value"
        c.put(Record("victim_key", self.victim_value))
        c.put(Record("after_key", "after_value"))
        c.sync()
        c.close()
        self.store_path = glob.glob(os.path.join(DB_PATH, "home", "ns", "t-store-*"))[0]

    def tearDown(self):
        shutil.rmtree(DB_PATH, ignore_errors=True)

    def test_corrupt_record_degrades_to_miss(self):
        payload = self.victim_value.encode('utf-8')
        data = bytearray(open(self.store_path, 'rb').read())
        at = data.find(payload)
        self.assertGreater(at, 0, "victim payload not found in store file")
        # 0xFF is never valid in UTF-8: the record's lengths stay consistent
        # but its content no longer decodes (UnicodeDecodeError is a
        # ValueError, the error readers treat as "record unreadable").
        data[at] = 0xFF
        with open(self.store_path, 'wb') as f:
            f.write(bytes(data))

        c = Cog(config=CogConfig(COG_PATH_PREFIX=DB_PATH, COG_HOME="home",
                                 CUSTOM_COG_DB_PATH=None))
        c.create_or_load_namespace("ns")
        c.use_table("t")
        try:
            self.assertIsNone(c.get("victim_key"))  # must not raise
            self.assertEqual(c.get("good_key").value, "good_value")
            self.assertEqual(c.get("after_key").value, "after_value")
            scanned = {r.key: r.value for r in c.scanner()}  # must not raise
            self.assertEqual(scanned.get("good_key"), "good_value")
            self.assertEqual(scanned.get("after_key"), "after_value")
            self.assertNotIn("victim_key", scanned)
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
