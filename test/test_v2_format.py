"""Tests for the Spindle on-disk format: file header, per-record timestamps, msgpack
payloads, key_link in-place updates, and varint length boundaries."""

import logging
import os
import shutil
import struct
import time
import unittest

from cog import config
from cog.codec import (
    SpindleCodec,
    V2_HEADER_SIZE,
    V2_MAGIC,
    V2_VERSION,
    _encode_varint,
    _decode_varint,
)
from cog.core import Record, Table


DIR_NAME = "TestV2Format"


class TestV2Header(unittest.TestCase):
    """Fresh files get a 23-byte Spindle header with a live created_at timestamp."""

    @classmethod
    def setUpClass(cls):
        cls.dir = "/tmp/" + DIR_NAME + "_header"
        if os.path.exists(cls.dir):
            shutil.rmtree(cls.dir)
        os.makedirs(cls.dir + "/test_table/", exist_ok=True)
        config.CUSTOM_COG_DB_PATH = cls.dir

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.dir):
            shutil.rmtree(cls.dir)

    def test_fresh_file_has_v2_header(self):
        logger = logging.getLogger()
        before = time.time_ns()
        table = Table("hdr", "test_table", "inst_hdr", config, logger)
        after = time.time_ns()
        store_path = table.store.store
        table.close()

        with open(store_path, "rb") as f:
            header = f.read(V2_HEADER_SIZE)
        self.assertEqual(len(header), V2_HEADER_SIZE)
        self.assertEqual(header[0:6], V2_MAGIC)
        self.assertEqual(header[6], V2_VERSION)
        created_at = struct.unpack("<q", header[7:15])[0]
        self.assertGreaterEqual(created_at, before)
        self.assertLessEqual(created_at, after)
        self.assertEqual(header[15:23], b"\x00" * 8)

    def test_created_at_survives_reopen(self):
        logger = logging.getLogger()
        table = Table("persist", "test_table", "inst_persist", config, logger)
        first_created_at = table.store.created_at
        self.assertIsNotNone(first_created_at)
        table.close()

        time.sleep(0.002)
        table2 = Table("persist", "test_table", "inst_persist", config, logger)
        self.assertEqual(table2.store.created_at, first_created_at)
        table2.close()

    def test_data_start_past_header(self):
        logger = logging.getLogger()
        table = Table("ds", "test_table", "inst_ds", config, logger)
        self.assertEqual(table.store.data_start, V2_HEADER_SIZE)
        rec = Record("k", "v")
        pos = table.store.save(rec)
        # First record position must be at or past the header.
        self.assertGreaterEqual(pos, V2_HEADER_SIZE)
        table.close()


class TestSpindleCodecRoundTrip(unittest.TestCase):
    """SpindleCodec.encode_record ↔ SpindleCodec.decode_record for every value_type."""

    def setUp(self):
        self.codec = SpindleCodec(created_at=time.time_ns())

    def test_string_record(self):
        ts = time.time_ns()
        r = Record("rocket", "saturn-v", value_type="s", key_link=42)
        r.timestamp = ts
        raw = self.codec.encode_record(r)
        out = self.codec.decode_record(raw)
        self.assertEqual(out.key, "rocket")
        self.assertEqual(out.value, "saturn-v")
        self.assertEqual(out.value_type, "s")
        self.assertEqual(out.key_link, 42)
        self.assertEqual(out.timestamp, ts)
        self.assertEqual(out.format_version, "2")

    def test_list_record_with_value_link(self):
        r = Record("fruits", "mango", value_type="l", key_link=100, value_link=200)
        r.timestamp = 1
        raw = self.codec.encode_record(r)
        out = self.codec.decode_record(raw)
        self.assertEqual(out.value_type, "l")
        self.assertEqual(out.value_link, 200)

    def test_set_record_with_value_link(self):
        r = Record("edges", "v1", value_type="u", key_link=-1, value_link=-1)
        r.timestamp = 0
        raw = self.codec.encode_record(r)
        out = self.codec.decode_record(raw)
        self.assertEqual(out.value_type, "u")
        self.assertEqual(out.key_link, -1)
        self.assertEqual(out.value_link, -1)

    def test_payload_with_sentinel_bytes_roundtrips(self):
        # Legacy was vulnerable to 0xFD inside float-serialised payloads.
        # Spindle has no separators — any byte sequence is safe.
        for byte_val in (0xAC, 0xFD, 0x00, 0xFF):
            r = Record("k", bytes([byte_val]) * 32)
            r.timestamp = 1
            raw = self.codec.encode_record(r)
            out = self.codec.decode_record(raw)
            self.assertEqual(out.value, bytes([byte_val]) * 32,
                             f"roundtrip failed for byte 0x{byte_val:02x}")


class TestV2UpdateKeyLink(unittest.TestCase):
    """update_key_link must overwrite exactly 8 bytes at the given position
    and leave the rest of the record, including the timestamp, untouched."""

    def test_update_key_link_is_isolated(self):
        codec = SpindleCodec(created_at=time.time_ns())
        r = Record("k", "v", value_type="s", key_link=123)
        r.timestamp = 999_999_999
        raw = codec.encode_record(r)

        # Patch key_link to -1 using the codec's bytes helper.
        new_prefix = codec.key_link_bytes(-1)
        self.assertEqual(len(new_prefix), 8)
        patched = new_prefix + raw[8:]
        self.assertEqual(len(patched), len(raw))

        out = codec.decode_record(patched)
        self.assertEqual(out.key_link, -1)
        self.assertEqual(out.timestamp, 999_999_999)
        self.assertEqual(out.key, "k")
        self.assertEqual(out.value, "v")


class TestVarint(unittest.TestCase):
    """Varint encoder/decoder at boundaries."""

    def test_boundaries(self):
        for length in (0, 1, 127, 128, 255, 256, 65535, 65536, 100_000, 2**32 - 1):
            enc = _encode_varint(length)
            dec_val, dec_size = _decode_varint(enc, 0)
            self.assertEqual(dec_val, length, f"varint roundtrip failed at {length}")
            self.assertEqual(dec_size, len(enc))

    def test_expected_sizes(self):
        self.assertEqual(len(_encode_varint(127)), 1)
        self.assertEqual(len(_encode_varint(128)), 2)
        self.assertEqual(len(_encode_varint(255)), 2)
        self.assertEqual(len(_encode_varint(256)), 3)
        self.assertEqual(len(_encode_varint(65535)), 3)
        self.assertEqual(len(_encode_varint(65536)), 5)


class TestV2StoreTimestamps(unittest.TestCase):
    """End-to-end: saves stamp each record with a fresh time.time_ns()."""

    @classmethod
    def setUpClass(cls):
        cls.dir = "/tmp/" + DIR_NAME + "_ts"
        if os.path.exists(cls.dir):
            shutil.rmtree(cls.dir)
        os.makedirs(cls.dir + "/test_table/", exist_ok=True)
        config.CUSTOM_COG_DB_PATH = cls.dir

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.dir):
            shutil.rmtree(cls.dir)

    def test_per_record_timestamp_monotonic_and_past_created_at(self):
        logger = logging.getLogger()
        table = Table("ts", "test_table", "inst_ts", config, logger)
        store = table.store
        created_at = store.created_at
        index = table.indexer.index_list[0]

        pos1 = store.save(Record("k1", "v1"))
        index.put("k1", pos1, store)
        time.sleep(0.001)
        pos2 = store.save(Record("k2", "v2"))
        index.put("k2", pos2, store)

        r1 = store.read(pos1)
        r2 = store.read(pos2)
        self.assertGreaterEqual(r1.timestamp, created_at)
        self.assertGreaterEqual(r2.timestamp, created_at)
        self.assertGreater(r2.timestamp - r1.timestamp, 1_000_000)

        table.close()


if __name__ == "__main__":
    unittest.main()
