"""Tests for file format detection. On open, a Store picks the codec once and
sticks with it for the lifetime of the file. Mixed-format files are impossible."""

import logging
import os
import shutil
import unittest

from cog import config
from cog.codec import (
    LegacyCodec,
    SpindleCodec,
    detect_codec,
    LEGACY_V0_FLAG,
    LEGACY_V1_FLAG,
    V2_MAGIC,
    V2_VERSION,
)
from cog.core import Record, Table


DIR_NAME = "TestFormatDetect"


def _make_legacy_file(path, flag_byte, payload_pairs):
    """Write a synthetic legacy-format store file with the given flag byte
    (0x30 for v0, 0x31 for v1) and a list of (key, value) pairs."""
    codec = LegacyCodec(version_flag=flag_byte)
    with open(path, "wb") as f:
        for k, v in payload_pairs:
            rec = Record(k, v, format_version=chr(flag_byte), value_type="s")
            f.write(codec.encode_record(rec))


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
        # Brand-new file: created_at is unstamped until write_header is called.
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

    def test_v0_file_detected(self):
        path = os.path.join(self.tmp, "v0")
        _make_legacy_file(path, LEGACY_V0_FLAG, [("a", "b")])
        size = os.path.getsize(path)
        with open(path, "rb+") as f:
            detected = detect_codec(f, size)
        self.assertIsInstance(detected, LegacyCodec)
        self.assertEqual(detected.version_flag, LEGACY_V0_FLAG)
        self.assertEqual(detected.VERSION, 0)

    def test_v1_file_detected(self):
        path = os.path.join(self.tmp, "v1")
        _make_legacy_file(path, LEGACY_V1_FLAG, [("a", "b")])
        size = os.path.getsize(path)
        with open(path, "rb+") as f:
            detected = detect_codec(f, size)
        self.assertIsInstance(detected, LegacyCodec)
        self.assertEqual(detected.version_flag, LEGACY_V1_FLAG)
        self.assertEqual(detected.VERSION, 1)

    def test_unknown_flag_rejected(self):
        path = os.path.join(self.tmp, "garbage")
        with open(path, "wb") as f:
            f.write(b"\x00" * 20)  # short file, byte 16 = 0x00 which is not a legacy flag
        size = os.path.getsize(path)
        with open(path, "rb+") as f:
            with self.assertRaises(ValueError):
                detect_codec(f, size)


class TestStoreFormatLocked(unittest.TestCase):
    """Opening a legacy file and writing must keep producing legacy records.
    Opening or creating a Spindle file must keep producing Spindle records."""

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
        """Table's Indexer.load_indexes needs cog_data_dir(namespace) to exist.
        With the Table(name, namespace, ...) signature, 'name' here is the
        namespace passed to Table; the internal cog_data_dir uses that."""
        os.makedirs(os.path.join(self.dir, table_name), exist_ok=True)

    def _write_legacy_store(self, db_namespace, table_name, instance_id, flag_byte):
        """Create a legacy-format file on disk where the Store will look for it."""
        os.makedirs(os.path.join(self.dir, db_namespace), exist_ok=True)
        store_path = config.CUSTOM_COG_DB_PATH + "/" + db_namespace + "/" + \
                     table_name + config.STORE + instance_id
        _make_legacy_file(store_path, flag_byte, [("seed", "value")])
        return store_path

    def test_v2_store_on_fresh_file(self):
        self._ensure_namespace("freshv2_ns")
        logger = logging.getLogger()
        # Table(name, namespace, instance_id, config, ...)
        table = Table("test_tbl", "freshv2_ns", "inst_fv2", config, logger)
        self.assertEqual(table.store.codec.VERSION, 2)
        pos = table.store.save(Record("k", "v"))
        raw = table.store.read(pos)
        rec = table.store.codec.decode_record(raw)
        self.assertEqual(rec.key, "k")
        self.assertIsNotNone(rec.timestamp)
        table.close()

    def test_legacy_v1_store_keeps_legacy_on_write(self):
        ns, name, inst = "legv1_ns", "test_tbl", "inst_lv1"
        self._write_legacy_store(ns, name, inst, LEGACY_V1_FLAG)

        logger = logging.getLogger()
        table = Table(name, ns, inst, config, logger)
        self.assertEqual(table.store.codec.VERSION, 1)
        self.assertIsInstance(table.store.codec, LegacyCodec)
        pos = table.store.save(Record("newkey", "newval"))
        raw = table.store.read(pos)
        self.assertEqual(raw[16], LEGACY_V1_FLAG)
        table.close()

    def test_legacy_v0_store_keeps_legacy_on_write(self):
        ns, name, inst = "legv0_ns", "test_tbl", "inst_lv0"
        self._write_legacy_store(ns, name, inst, LEGACY_V0_FLAG)

        logger = logging.getLogger()
        table = Table(name, ns, inst, config, logger)
        self.assertEqual(table.store.codec.VERSION, 0)
        pos = table.store.save(Record("newkey", "newval"))
        raw = table.store.read(pos)
        self.assertEqual(raw[16], LEGACY_V0_FLAG)
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
