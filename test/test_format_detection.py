"""Tests for file format detection. On open, a Store picks the codec once and
sticks with it for the lifetime of the file."""

import logging
import os
import shutil
import unittest

from cog import config
from cog.codec import (
    SpindleCodec,
    detect_codec,
    V2_MAGIC,
    V2_VERSION,
)
from cog.core import Record, Table


DIR_NAME = "TestFormatDetect"


class TestDetectCodec(unittest.TestCase):
    """Pure codec.detect_codec unit tests."""

    def setUp(self):
        self.tmp = "/tmp/" + DIR_NAME + "_unit"
        if os.path.exists(self.tmp):
            shutil.rmtree(self.tmp)
        os.makedirs(self.tmp, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_empty_file_is_v2(self):
        path = os.path.join(self.tmp, "empty")
        open(path, "wb").close()
        with open(path, "rb+") as f:
            codec = detect_codec(f, 0)
        self.assertIsInstance(codec, SpindleCodec)
        self.assertIsNone(codec.created_at)

    def test_v2_file_detected_by_magic(self):
        path = os.path.join(self.tmp, "v2")
        codec = SpindleCodec(created_at=1_234_567_890)
        with open(path, "wb+") as f:
            codec.write_header(f)
        size = os.path.getsize(path)
        with open(path, "rb+") as f:
            detected = detect_codec(f, size)
        self.assertIsInstance(detected, SpindleCodec)
        self.assertEqual(detected.created_at, 1_234_567_890)

    def test_unknown_format_rejected(self):
        path = os.path.join(self.tmp, "garbage")
        with open(path, "wb") as f:
            f.write(b"\x00" * 20)
        size = os.path.getsize(path)
        with open(path, "rb+") as f:
            with self.assertRaises(ValueError):
                detect_codec(f, size)


class TestStoreFormatLocked(unittest.TestCase):
    """Opening or creating a Spindle file must keep producing Spindle records."""

    @classmethod
    def setUpClass(cls):
        cls.dir = "/tmp/" + DIR_NAME + "_store"
        if os.path.exists(cls.dir):
            shutil.rmtree(cls.dir)
        os.makedirs(cls.dir, exist_ok=True)
        config.CUSTOM_COG_DB_PATH = cls.dir

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.dir):
            shutil.rmtree(cls.dir)

    def _ensure_namespace(self, table_name):
        os.makedirs(os.path.join(self.dir, table_name), exist_ok=True)

    def test_v2_store_on_fresh_file(self):
        self._ensure_namespace("freshv2_ns")
        logger = logging.getLogger()
        table = Table("test_tbl", "freshv2_ns", "inst_fv2", config, logger)
        self.assertEqual(table.store.codec.VERSION, 2)
        pos = table.store.save(Record("k", "v"))
        rec = table.store.read(pos)
        self.assertEqual(rec.key, "k")
        self.assertIsNotNone(rec.timestamp)
        table.close()

    def test_v2_file_starts_with_magic(self):
        self._ensure_namespace("magic_ns")
        logger = logging.getLogger()
        table = Table("test_tbl", "magic_ns", "inst_magic", config, logger)
        store_path = table.store.store
        table.close()
        with open(store_path, "rb") as f:
            head = f.read(7)
        self.assertEqual(head[0:6], V2_MAGIC)
        self.assertEqual(head[6], V2_VERSION)


if __name__ == "__main__":
    unittest.main()
