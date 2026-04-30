"""Tests for cog.migrate — legacy 3.x to Spindle 4.x migration."""

import marshal
import os
import random
import shutil
import struct
import unittest

from cog.migrate import migrate, STORE_MARKER, INDEX_MARKER
from cog.codec import SpindleCodec, V2_MAGIC, V2_HEADER_SIZE, detect_codec
from cog.core import Record, Table
from cog import config

DIR_NAME = "TestMigrate"
DB_PATH = "/tmp/" + DIR_NAME

# Legacy format constants (same as in migrate.py)
_RECORD_SEP = b'\xFD'
_UNIT_SEP = b'\xAC'
_LEGACY_KEY_LINK_LEN = 16
_LEGACY_INDEX_BLOCK_LEN = 32


def _legacy_marshal_record(key, value, value_type='s', key_link=-1, value_link=-1):
    """Produce raw bytes for a single legacy record."""
    key_link_bytes = str(key_link).encode().rjust(_LEGACY_KEY_LINK_LEN)
    serialized = marshal.dumps((key, value))
    rec = (
        key_link_bytes
        + b'1'  # format_version
        + value_type.encode()
        + str(len(serialized)).encode()
        + _UNIT_SEP
        + serialized
    )
    if value_type in ('l', 'u'):
        rec += str(value_link).encode()
    rec += _RECORD_SEP
    return rec


def _write_legacy_store(path, records):
    """Write a legacy store file.  *records* is a list of
    (key, value, value_type, key_link, value_link) tuples.
    Returns list of (old_position, key) pairs."""
    positions = []
    with open(path, 'wb') as f:
        for key, value, value_type, key_link, value_link in records:
            pos = f.tell()
            positions.append((pos, key))
            f.write(_legacy_marshal_record(key, value, value_type, key_link, value_link))
    return positions


def _write_legacy_index(path, slot_values, capacity):
    """Write a legacy index file.  *slot_values* is a dict of
    slot_number -> store_position.  Unmentioned slots get the empty sentinel."""
    empty = '-1'.zfill(_LEGACY_INDEX_BLOCK_LEN).encode()
    with open(path, 'wb') as f:
        for i in range(capacity):
            if i in slot_values:
                f.write(str(slot_values[i]).encode().rjust(_LEGACY_INDEX_BLOCK_LEN))
            else:
                f.write(empty)


class TestMigrateStoreOnly(unittest.TestCase):
    """Migrate a hand-crafted legacy store (no index) and verify Spindle output."""

    def setUp(self):
        self.dir = DB_PATH + "_store"
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(os.path.join(self.dir, "ns"), exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def test_string_records_migrate(self):
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst1")
        _write_legacy_store(store_path, [
            ("key1", "val1", "s", -1, -1),
            ("key2", "val2", "s", -1, -1),
            ("key3", "val3", "s", -1, -1),
        ])

        result = migrate(self.dir)
        self.assertEqual(result['stores_migrated'], 1)
        self.assertEqual(result['errors'], [])

        # Verify the new file is Spindle format
        with open(store_path, 'rb') as f:
            head = f.read(6)
        self.assertEqual(head, V2_MAGIC)

        # Verify records are readable with SpindleCodec
        codec = SpindleCodec(created_at=0)
        with open(store_path, 'rb') as f:
            f.seek(0)
            file_size = f.seek(0, 2)
            f.seek(0)
            codec = detect_codec(f, file_size)
            f.seek(V2_HEADER_SIZE)
            for expected_key in ("key1", "key2", "key3"):
                raw = codec.read_record(f)
                self.assertIsNotNone(raw, f"Expected record for {expected_key}")
                rec = codec.decode_record(raw)
                self.assertEqual(rec.key, expected_key)

    def test_list_records_with_value_links(self):
        """List records have value_link chains that must be remapped."""
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst2")

        # Manually compute legacy positions to set up value_link chain:
        # rec0 at pos 0: "fruits" -> "apple", value_link=-1 (tail)
        rec0_bytes = _legacy_marshal_record("fruits", "apple", "l", -1, -1)
        pos0 = 0
        pos1 = len(rec0_bytes)
        # rec1 at pos1: "fruits" -> "banana", value_link=pos0 (points to rec0)
        rec1_bytes = _legacy_marshal_record("fruits", "banana", "l", -1, pos0)

        with open(store_path, 'wb') as f:
            f.write(rec0_bytes)
            f.write(rec1_bytes)

        result = migrate(self.dir)
        self.assertEqual(result['stores_migrated'], 1)

        # Read back and verify value_link was remapped
        with open(store_path, 'rb') as f:
            file_size = f.seek(0, 2)
            f.seek(0)
            codec = detect_codec(f, file_size)
            f.seek(V2_HEADER_SIZE)

            raw0 = codec.read_record(f)
            rec0 = codec.decode_record(raw0)
            new_pos0 = V2_HEADER_SIZE
            self.assertEqual(rec0.key, "fruits")
            self.assertEqual(rec0.value, "apple")
            self.assertEqual(rec0.value_link, -1)

            raw1 = codec.read_record(f)
            rec1 = codec.decode_record(raw1)
            self.assertEqual(rec1.key, "fruits")
            self.assertEqual(rec1.value, "banana")
            self.assertEqual(rec1.value_link, new_pos0)

    def test_key_link_chain_remapped(self):
        """key_link (hash collision chain) must be remapped."""
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst3")

        rec0_bytes = _legacy_marshal_record("k_a", "v_a", "s", -1, -1)
        pos0 = 0
        pos1 = len(rec0_bytes)
        # rec1 has key_link pointing to rec0 (collision chain)
        rec1_bytes = _legacy_marshal_record("k_b", "v_b", "s", pos0, -1)

        with open(store_path, 'wb') as f:
            f.write(rec0_bytes)
            f.write(rec1_bytes)

        result = migrate(self.dir)
        self.assertEqual(result['stores_migrated'], 1)

        with open(store_path, 'rb') as f:
            file_size = f.seek(0, 2)
            f.seek(0)
            codec = detect_codec(f, file_size)
            f.seek(V2_HEADER_SIZE)

            raw0 = codec.read_record(f)
            rec0 = codec.decode_record(raw0)
            self.assertEqual(rec0.key_link, -1)

            raw1 = codec.read_record(f)
            rec1 = codec.decode_record(raw1)
            self.assertEqual(rec1.key_link, V2_HEADER_SIZE)

    def test_backup_files_created(self):
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst4")
        _write_legacy_store(store_path, [("k", "v", "s", -1, -1)])

        migrate(self.dir)
        self.assertTrue(os.path.exists(store_path + '.v3_backup'))

    def test_remove_backups(self):
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst5")
        _write_legacy_store(store_path, [("k", "v", "s", -1, -1)])

        migrate(self.dir, remove_backups=True)
        self.assertFalse(os.path.exists(store_path + '.v3_backup'))

    def test_already_spindle_skipped(self):
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst6")
        codec = SpindleCodec(created_at=1)
        with open(store_path, 'wb') as f:
            codec.write_header(f)

        result = migrate(self.dir)
        self.assertEqual(result['stores_migrated'], 0)
        self.assertEqual(result['skipped'], 1)

    def test_empty_store_skipped(self):
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst7")
        open(store_path, 'wb').close()

        result = migrate(self.dir)
        self.assertEqual(result['skipped'], 1)

    def test_migrated_timestamp_is_zero(self):
        """Migrated records should have timestamp=0."""
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst8")
        _write_legacy_store(store_path, [("k", "v", "s", -1, -1)])

        migrate(self.dir)
        with open(store_path, 'rb') as f:
            file_size = f.seek(0, 2)
            f.seek(0)
            codec = detect_codec(f, file_size)
            f.seek(V2_HEADER_SIZE)
            raw = codec.read_record(f)
            rec = codec.decode_record(raw)
        self.assertEqual(rec.timestamp, 0)


class TestMigrateWithIndex(unittest.TestCase):
    """Migrate store + index together and verify the index is usable."""

    def setUp(self):
        self.dir = DB_PATH + "_idx"
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(os.path.join(self.dir, "ns"), exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def test_index_converted_to_8byte_blocks(self):
        """Index slots change from 32-byte ASCII to 8-byte int64 LE, and the
        record is addressable under the current slot formula after migration."""
        from cog.core import cog_hash
        capacity = 16
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst1")
        index_path = os.path.join(self.dir, "ns", f"tbl{INDEX_MARKER}inst1-0")

        positions = _write_legacy_store(store_path, [
            ("k1", "v1", "s", -1, -1),
        ])
        old_pos = positions[0][0]

        # Put the store position somewhere in the legacy index; migration
        # rehashes under the current slot formula, so the exact legacy slot
        # doesn't have to match the new slot.
        _write_legacy_index(index_path, {3: old_pos}, capacity)

        result = migrate(self.dir)
        self.assertEqual(result['stores_migrated'], 1)
        self.assertEqual(result['indexes_migrated'], 1)

        # Verify new index file size: 8 bytes * capacity.
        self.assertEqual(os.path.getsize(index_path), 8 * capacity)

        # The record must be placed at whatever slot cog_hash('k1', capacity)
        # now chooses, with its store position pointing at the record body
        # right after the Spindle header.
        expected_slot = cog_hash('k1', capacity)
        with open(index_path, 'rb') as f:
            f.seek(expected_slot * 8)
            new_pos = struct.unpack('<q', f.read(8))[0]
        self.assertEqual(new_pos, V2_HEADER_SIZE)

        # A different slot (not the one we rehashed into) must stay zero.
        empty_slot = (expected_slot + 1) % capacity
        with open(index_path, 'rb') as f:
            f.seek(empty_slot * 8)
            self.assertEqual(f.read(8), b'\x00' * 8)

    def test_index_backup_created(self):
        capacity = 4
        store_path = os.path.join(self.dir, "ns", f"tbl{STORE_MARKER}inst2")
        index_path = os.path.join(self.dir, "ns", f"tbl{INDEX_MARKER}inst2-0")
        _write_legacy_store(store_path, [("k", "v", "s", -1, -1)])
        _write_legacy_index(index_path, {}, capacity)

        migrate(self.dir)
        self.assertTrue(os.path.exists(index_path + '.v3_backup'))


class TestMigrateEndToEnd(unittest.TestCase):
    """Create a legacy database, migrate it, then open with v4 Table and verify."""

    def setUp(self):
        self.dir = DB_PATH + "_e2e"
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(os.path.join(self.dir, "ns"), exist_ok=True)
        config.CUSTOM_COG_DB_PATH = self.dir

    def tearDown(self):
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def _build_legacy_db(self, table_name, instance_id, kv_pairs):
        """Build a realistic legacy store+index using the actual legacy encoding
        and hash function, then migrate and return the Table for verification."""
        import xxhash

        capacity = config.INDEX_CAPACITY
        ns = "ns"
        store_path = config.cog_store(ns, table_name, instance_id)
        index_path = config.cog_index(ns, table_name, instance_id, 0)

        empty_block = '-1'.zfill(_LEGACY_INDEX_BLOCK_LEN).encode()

        # Create empty legacy index
        with open(index_path, 'wb') as f:
            f.write(empty_block * capacity)

        # Write records and build index (mimicking legacy Index.put)
        index_slots = {}  # slot_offset -> store_position (as ASCII bytes)

        with open(store_path, 'wb') as sf, open(index_path, 'r+b') as idx:
            import mmap
            idx_mm = mmap.mmap(idx.fileno(), 0)

            for key, value in kv_pairs:
                # Hash to get slot
                num = xxhash.xxh32(key, seed=2).intdigest() % capacity
                slot = max((num % capacity) - 1, 0)
                offset = _LEGACY_INDEX_BLOCK_LEN * slot

                existing = idx_mm[offset:offset + _LEGACY_INDEX_BLOCK_LEN]

                pos = sf.tell()
                if existing == empty_block:
                    sf.write(_legacy_marshal_record(key, value, 's', -1, -1))
                else:
                    head_pos = int(existing)
                    sf.write(_legacy_marshal_record(key, value, 's', head_pos, -1))

                idx_mm[offset:offset + _LEGACY_INDEX_BLOCK_LEN] = \
                    str(pos).encode().rjust(_LEGACY_INDEX_BLOCK_LEN)

            idx_mm.flush()
            idx_mm.close()

    def test_migrate_then_open_and_read(self):
        """Write legacy data, migrate, open with v4 Table, verify all keys."""
        kv_pairs = [(f"key_{i}", f"value_{i}") for i in range(100)]
        self._build_legacy_db("test_tbl", "inst_e2e", kv_pairs)

        result = migrate(self.dir)
        self.assertEqual(result['stores_migrated'], 1)
        self.assertEqual(result['indexes_migrated'], 1)
        self.assertEqual(result['errors'], [])

        import logging
        table = Table("test_tbl", "ns", "inst_e2e", config, logging.getLogger())
        store = table.store
        index = table.indexer.index_list[0]

        for key, expected_val in kv_pairs:
            rec = index.get(key, store)
            self.assertIsNotNone(rec, f"Missing key after migration: {key}")
            self.assertEqual(rec.value, expected_val)

        table.close()

    def test_migrate_with_floats(self):
        """Float values (the original source of the 0xFD bug) survive migration."""
        random.seed(42)
        kv_pairs = [(f"embed_{i}", random.random()) for i in range(50)]
        self._build_legacy_db("float_tbl", "inst_float", kv_pairs)

        result = migrate(self.dir)
        self.assertEqual(result['errors'], [])

        import logging
        table = Table("float_tbl", "ns", "inst_float", config, logging.getLogger())
        store = table.store
        index = table.indexer.index_list[0]

        for key, expected_val in kv_pairs:
            rec = index.get(key, store)
            self.assertIsNotNone(rec, f"Missing key: {key}")
            self.assertAlmostEqual(rec.value, expected_val)

        table.close()

    def test_idempotent(self):
        """Running migrate twice should skip already-migrated files."""
        self._build_legacy_db("idem_tbl", "inst_idem", [("k", "v")])

        r1 = migrate(self.dir)
        self.assertEqual(r1['stores_migrated'], 1)

        r2 = migrate(self.dir)
        self.assertEqual(r2['stores_migrated'], 0)
        self.assertEqual(r2['skipped'], 1)


class TestMigrateEdgeCases(unittest.TestCase):

    def setUp(self):
        self.dir = DB_PATH + "_edge"
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(os.path.join(self.dir, "ns"), exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def test_nonexistent_path_raises(self):
        with self.assertRaises(FileNotFoundError):
            migrate("/tmp/definitely_does_not_exist_xyz")

    def test_sys_and_views_dirs_skipped(self):
        """The sys and views directories should not be scanned."""
        os.makedirs(os.path.join(self.dir, "sys"), exist_ok=True)
        os.makedirs(os.path.join(self.dir, "views"), exist_ok=True)
        result = migrate(self.dir)
        self.assertEqual(result['stores_migrated'], 0)
        self.assertEqual(result['skipped'], 0)

    def test_multiple_namespaces(self):
        """Migration should handle multiple namespace directories."""
        for ns in ("ns_a", "ns_b"):
            ns_dir = os.path.join(self.dir, ns)
            os.makedirs(ns_dir, exist_ok=True)
            store_path = os.path.join(ns_dir, f"tbl{STORE_MARKER}inst1")
            _write_legacy_store(store_path, [("k", "v", "s", -1, -1)])

        result = migrate(self.dir)
        self.assertEqual(result['stores_migrated'], 2)


if __name__ == "__main__":
    unittest.main()
