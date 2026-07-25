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
        config.CUSTOM_COG_DB_PATH = None
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

    def test_graph_edge_keys_rewritten(self):
        """Legacy torque edge keys (string suffix) must become queryable with
        the v4 byte-prefix encoding after migration."""
        from cog.database import out_nodes, in_nodes

        # Legacy torque stored directional edges as string-suffix keys.
        legacy_pairs = [
            ("alice" + "__:out:__", "bob"),
            ("bob" + "__:in:__", "alice"),
        ]
        self._build_legacy_db("edge_tbl", "inst_edge", legacy_pairs)
        # The rewrite only applies to graph namespaces — mark this one by
        # giving it a node-set table, as any real torque namespace has.
        node_set_store = config.cog_store("ns", config.GRAPH_NODE_SET_TABLE_NAME, "inst_edge")
        open(node_set_store, 'wb').close()

        result = migrate(self.dir)
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['stores_migrated'], 1)

        import logging
        table = Table("edge_tbl", "ns", "inst_edge", config, logging.getLogger())
        store = table.store
        try:
            # New code queries with the byte-prefix key; the migrated record
            # must be found under it.
            out_rec = table.indexer.get(out_nodes("alice"), store)
            self.assertIsNotNone(out_rec, "out edge unreachable after migration")
            self.assertEqual(out_rec.value, "bob")

            in_rec = table.indexer.get(in_nodes("bob"), store)
            self.assertIsNotNone(in_rec, "in edge unreachable after migration")
            self.assertEqual(in_rec.value, "alice")

            # The old string key must no longer resolve.
            self.assertIsNone(table.indexer.get("alice" + "__:out:__", store))
        finally:
            table.close()

    def test_kv_keys_with_edge_suffix_not_rewritten(self):
        """In a plain KV namespace (no TOR_NODE_SET table), user keys that
        happen to end with the legacy edge suffixes are data, not edge keys —
        rewriting them would silently lose them."""
        legacy_pairs = [
            ("report__:out:__", "user_value_1"),
            ("__:in:__", "user_value_2"),
            ("normal_key", "user_value_3"),
        ]
        self._build_legacy_db("kv_tbl", "inst_kv", legacy_pairs)

        result = migrate(self.dir)
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['stores_migrated'], 1)

        import logging
        table = Table("kv_tbl", "ns", "inst_kv", config, logging.getLogger())
        try:
            for key, expected_val in legacy_pairs:
                rec = table.indexer.get(key, table.store)
                self.assertIsNotNone(rec, f"KV key {key!r} lost after migration")
                self.assertEqual(rec.value, expected_val)
        finally:
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


class TestMigrateLiveness(unittest.TestCase):
    """Deletions and same-key updates in 3.x live only in the legacy index
    (the store is an append-only log). The rebuilt 4.x index must contain
    exactly the live record for each key: deleted keys stay deleted, updated
    keys resolve to the newest value, and each key appears at most once in a
    bucket chain (the invariant scanner/MemoryView depend on)."""

    def setUp(self):
        self.dir = DB_PATH + "_live"
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(os.path.join(self.dir, "ns"), exist_ok=True)
        config.CUSTOM_COG_DB_PATH = self.dir

    def tearDown(self):
        config.CUSTOM_COG_DB_PATH = None
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def _legacy_slot(self, key, capacity):
        import xxhash
        num = xxhash.xxh32(key, seed=2).intdigest() % capacity
        return max((num % capacity) - 1, 0)

    def _open_table(self, table_name, instance_id):
        import logging
        return Table(table_name, "ns", instance_id, config, logging.getLogger())

    def test_deleted_key_stays_deleted(self):
        """3.x Cog.delete unlinks the index entry but leaves the record in the
        store. The record must NOT be re-indexed by migration."""
        capacity = config.INDEX_CAPACITY
        store_path = config.cog_store("ns", "del_tbl", "inst_del")
        index_path = config.cog_index("ns", "del_tbl", "inst_del", 0)

        positions = _write_legacy_store(store_path, [
            ("dead_key", "temp", "s", -1, -1),   # was deleted in 3.x
            ("alive_key", "ok", "s", -1, -1),
        ])
        alive_pos = positions[1][0]

        slot_alive = self._legacy_slot("alive_key", capacity)
        slot_dead = self._legacy_slot("dead_key", capacity)
        self.assertNotEqual(slot_alive, slot_dead)
        # deleted key: slot back to the empty sentinel (what 3.x delete did)
        _write_legacy_index(index_path, {slot_alive: alive_pos}, capacity)

        result = migrate(self.dir)
        self.assertEqual(result['errors'], [])
        self.assertEqual(result['stores_migrated'], 1)

        table = self._open_table("del_tbl", "inst_del")
        try:
            rec = table.indexer.get("alive_key", table.store)
            self.assertIsNotNone(rec)
            self.assertEqual(rec.value, "ok")
            self.assertIsNone(table.indexer.get("dead_key", table.store),
                              "key deleted in 3.x resurrected after migration")
            scanned = [r.key for r in table.indexer.scanner(table.store)]
            self.assertEqual(scanned, ["alive_key"])
        finally:
            table.close()

    def test_updated_key_indexed_once_with_newest_value(self):
        """A key written N times leaves N records in the legacy store but must
        appear exactly once in the rebuilt index, resolving to the newest
        value (both via get and via scanner, which MemoryView relies on)."""
        capacity = config.INDEX_CAPACITY
        store_path = config.cog_store("ns", "upd_tbl", "inst_upd")
        index_path = config.cog_index("ns", "upd_tbl", "inst_upd", 0)

        # Legacy same-key update: new head takes the old head's key_link (-1);
        # the old record stays in the store, unreachable from the index.
        positions = _write_legacy_store(store_path, [
            ("u_key", "old_value", "s", -1, -1),
            ("u_key", "new_value", "s", -1, -1),
        ])
        newest_pos = positions[1][0]
        slot = self._legacy_slot("u_key", capacity)
        _write_legacy_index(index_path, {slot: newest_pos}, capacity)

        result = migrate(self.dir)
        self.assertEqual(result['errors'], [])

        table = self._open_table("upd_tbl", "inst_upd")
        try:
            rec = table.indexer.get("u_key", table.store)
            self.assertEqual(rec.value, "new_value")
            scanned = [(r.key, r.value) for r in table.indexer.scanner(table.store)]
            self.assertEqual(scanned, [("u_key", "new_value")],
                             "scanner must yield exactly one entry per key")
        finally:
            table.close()

    def test_list_key_indexed_once_with_full_chain(self):
        """put_list appends one store record per element, each indexed over the
        previous. Only the newest head is live; its value chain must
        materialize the full list, and scan must not duplicate the key."""
        capacity = config.INDEX_CAPACITY
        store_path = config.cog_store("ns", "list_tbl", "inst_list")
        index_path = config.cog_index("ns", "list_tbl", "inst_list", 0)

        rec0 = _legacy_marshal_record("fruits", "apple", "l", -1, -1)
        pos0 = 0
        pos1 = len(rec0)
        rec1 = _legacy_marshal_record("fruits", "banana", "l", -1, pos0)
        with open(store_path, 'wb') as f:
            f.write(rec0)
            f.write(rec1)
        slot = self._legacy_slot("fruits", capacity)
        _write_legacy_index(index_path, {slot: pos1}, capacity)

        result = migrate(self.dir)
        self.assertEqual(result['errors'], [])

        table = self._open_table("list_tbl", "inst_list")
        try:
            rec = table.indexer.get("fruits", table.store)
            self.assertEqual(rec.value, ["banana", "apple"])
            scanned = [r.key for r in table.indexer.scanner(table.store)]
            self.assertEqual(scanned, ["fruits"])
        finally:
            table.close()


class TestOpenAfterMigration(unittest.TestCase):
    """The default migrate() leaves .v3_backup files next to the migrated
    files. Opening the database afterwards must work, and one unloadable
    table must not take the whole namespace down."""

    def setUp(self):
        self.dir = DB_PATH + "_open"
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(self.dir, exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def _new_cog(self):
        from cog.database import Cog
        from cog.config import CogConfig
        return Cog(config=CogConfig(COG_PATH_PREFIX=self.dir, COG_HOME="home"))

    def test_namespace_opens_with_backup_and_junk_files(self):
        from cog.core import Record as R
        c = self._new_cog()
        c.create_or_load_namespace("ns")
        c.create_table("t1", "ns")
        c.put(R("k1", "v1"))
        c.sync()
        c.close()

        ns_dir = os.path.join(self.dir, "home", "ns")
        index_file = next(f for f in os.listdir(ns_dir) if INDEX_MARKER in f)
        store_file = next(f for f in os.listdir(ns_dir) if STORE_MARKER in f)
        # what migrate() leaves behind, plus an unparseable stray file
        open(os.path.join(ns_dir, index_file + ".v3_backup"), 'wb').close()
        open(os.path.join(ns_dir, store_file + ".v3_backup"), 'wb').close()
        open(os.path.join(ns_dir, store_file + ".v4_tmp"), 'wb').close()
        open(os.path.join(ns_dir, "stray-index-notanumber"), 'wb').close()

        c2 = self._new_cog()
        c2.create_or_load_namespace("ns")  # raised ValueError in 4.0.0rc1
        c2.use_table("t1")
        rec = c2.get("k1")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.value, "v1")
        c2.close()

    def test_one_legacy_table_does_not_brick_namespace(self):
        from cog.core import Record as R
        c = self._new_cog()
        c.create_or_load_namespace("ns")
        c.create_table("t_good", "ns")
        c.put(R("k_good", "v_good"))
        c.create_table("t_bad", "ns")
        c.put(R("k_bad", "v_bad"))
        c.sync()
        c.close()

        # replace t_bad's store with legacy-format bytes (unmigrated table)
        ns_dir = os.path.join(self.dir, "home", "ns")
        bad_store = next(f for f in os.listdir(ns_dir)
                         if f.startswith("t_bad") and STORE_MARKER in f)
        with open(os.path.join(ns_dir, bad_store), 'wb') as f:
            f.write(_legacy_marshal_record("k_bad", "v_bad"))

        c2 = self._new_cog()
        c2.create_or_load_namespace("ns")  # must not raise
        c2.use_table("t_good")
        rec = c2.get("k_good")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.value, "v_good")
        with self.assertRaises(ValueError):
            c2.use_table("t_bad")  # direct access still fails loudly
        c2.close()


class TestMigratePreflight(unittest.TestCase):
    """If any record cannot be encoded, migrate() must report every problem
    and convert nothing (no half-migrated database)."""

    def setUp(self):
        self.dir = DB_PATH + "_preflight"
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(os.path.join(self.dir, "ns"), exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def test_preflight_failure_reports_all_and_converts_nothing(self):
        import cog.migrate as M
        good = os.path.join(self.dir, "ns", f"good{STORE_MARKER}i1")
        bad = os.path.join(self.dir, "ns", f"bad{STORE_MARKER}i1")
        _write_legacy_store(good, [("k1", "v1", "s", -1, -1)])
        _write_legacy_store(bad, [
            ("k2", "v2", "s", -1, -1),
            ("poison_a", "x", "s", -1, -1),
            ("poison_b", "x", "s", -1, -1),
        ])

        orig = M.spindle_pack.packb

        def poisoned(key, value):
            if isinstance(key, str) and key.startswith("poison"):
                raise ValueError("simulated unencodable value")
            return orig(key, value)

        M.spindle_pack.packb = poisoned
        try:
            stats = migrate(self.dir)
        finally:
            M.spindle_pack.packb = orig

        self.assertEqual(stats['stores_migrated'], 0)
        self.assertEqual(len(stats['errors']), 2)  # BOTH poison records reported
        self.assertTrue(all('bad' in e for e in stats['errors']))
        # nothing on disk was touched
        self.assertEqual(sorted(os.listdir(os.path.join(self.dir, "ns"))),
                         sorted([os.path.basename(good), os.path.basename(bad)]))

        # with the poison gone, the same database migrates cleanly
        stats2 = migrate(self.dir)
        self.assertEqual(stats2['errors'], [])
        self.assertEqual(stats2['stores_migrated'], 2)

    def test_corrupt_record_mid_store_aborts_migration(self):
        """A record the sequential reader cannot parse would make migration
        silently drop everything after it — pre-flight must refuse instead."""
        bad = os.path.join(self.dir, "ns", f"corrupt{STORE_MARKER}i1")
        good_rec = _legacy_marshal_record("k1", "v1")
        with open(bad, 'wb') as f:
            f.write(good_rec)
            # 18-byte pseudo-header with an invalid value_type, then junk the
            # reader can never resynchronize past.
            f.write(b"1".rjust(16) + b"1z" + b"\x00" * 32)
            f.write(good_rec)  # a record that WOULD be lost

        stats = migrate(self.dir)
        self.assertEqual(stats['stores_migrated'], 0)
        self.assertEqual(len(stats['errors']), 1)
        self.assertIn("would be lost", stats['errors'][0])
        # original file untouched
        self.assertEqual(sorted(os.listdir(os.path.join(self.dir, "ns"))),
                         [os.path.basename(bad)])


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
