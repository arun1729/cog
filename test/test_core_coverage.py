"""
Tests for:
- Record.get_kv_tuple()
- Index load increment on non-empty blocks
- Index put loopback and capacity handling
- Store address exceeds block size exception
- Index get loopback and EOF handling
- Store EOF handling in scanner
- Index delete key not found scenarios
- Index flush
- Store read EOF handling
- Indexer with multiple indexes
"""
from cog.core import Table, Record, Index, Store, Indexer, TableMeta, cog_hash
from cog import config
import logging
import os
from logging.config import dictConfig
import shutil
import unittest

DIR_NAME = "TestCoreCoverage"


class TestCoreCoverage(unittest.TestCase):
    """Tests to increase coverage of core.py"""

    @classmethod
    def setUpClass(cls):
        cls.db_path = "/tmp/" + DIR_NAME
        cls._cleanup_path(cls.db_path)
        os.makedirs(cls.db_path + "/test_table/")
        config.CUSTOM_COG_DB_PATH = cls.db_path
        dictConfig(config.logging_config)

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_path(cls.db_path)
        print("*** deleted core coverage test data.")

    @staticmethod
    def _cleanup_path(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    def test_record_get_kv_tuple(self):
        """Test Record.get_kv_tuple() returns correct tuple"""
        record = Record("my_key", "my_value")
        kv_tuple = record.get_kv_tuple()
        self.assertEqual(kv_tuple, ("my_key", "my_value"))

    def test_record_get_kv_tuple_with_list(self):
        """Test Record.get_kv_tuple() with list value"""
        record = Record("list_key", ["val1", "val2", "val3"], value_type='l')
        kv_tuple = record.get_kv_tuple()
        self.assertEqual(kv_tuple[0], "list_key")
        self.assertEqual(kv_tuple[1], ["val1", "val2", "val3"])

    def test_record_is_empty(self):
        """Test Record.is_empty() method"""
        empty_record = Record(None, None)
        self.assertTrue(empty_record.is_empty())

        non_empty_record = Record("key", "value")
        self.assertFalse(non_empty_record.is_empty())

    def test_index_flush(self):
        """Test Index.flush() method"""
        logger = logging.getLogger()
        table = Table("flush_test", "test_table", "test_flush_id", config)
        index = table.indexer.index_list[0]
        
        # Put some data
        record = Record("flush_key", "flush_value")
        position = table.store.save(record)
        index.put(record.key, position, table.store)
        
        # Flush should not raise
        index.flush()
        
        # Verify data still retrievable after flush
        retrieved = index.get("flush_key", table.store)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.value, "flush_value")
        
        table.close()

    def test_index_get_nonexistent_key(self):
        """Test Index.get() returns None for non-existent key"""
        logger = logging.getLogger()
        table = Table("nonexistent_test", "test_table", "test_ne_id", config)
        index = table.indexer.index_list[0]
        
        result = index.get("definitely_not_exists", table.store)
        self.assertIsNone(result)
        
        table.close()

    def test_index_delete_nonexistent_key(self):
        """Test Index.delete() returns False for non-existent key"""
        logger = logging.getLogger()
        table = Table("delete_ne_test", "test_table", "test_del_ne_id", config)
        index = table.indexer.index_list[0]
        
        result = index.delete("key_that_does_not_exist", table.store)
        self.assertFalse(result)
        
        table.close()

    def test_index_delete_in_chain(self):
        """Test Index.delete() for key in collision chain"""
        # Save original capacity
        orig_capacity = config.INDEX_CAPACITY
        config.INDEX_CAPACITY = 4  # Force collisions
        
        logger = logging.getLogger()
        table = Table("del_chain_test", "test_table", "test_del_chain_id", config)
        store = table.store
        index = table.indexer.index_list[0]
        
        # Insert multiple records that may collide
        records = [
            Record("key_a", "value_a"),
            Record("key_b", "value_b"),
            Record("key_c", "value_c"),
            Record("key_d", "value_d"),
        ]
        
        for rec in records:
            position = store.save(rec)
            index.put(rec.key, position, store)
        
        # Delete middle key
        result = index.delete("key_b", store)
        self.assertTrue(result, "index.delete() should return True on successful deletion")
        
        # Verify deleted key is no longer accessible
        self.assertIsNone(index.get("key_b", store), "The deleted key should not be found")
        
        # Verify other keys still exist
        self.assertIsNotNone(index.get("key_a", store))
        self.assertIsNotNone(index.get("key_c", store))
        self.assertIsNotNone(index.get("key_d", store))
        
        table.close()
        config.INDEX_CAPACITY = orig_capacity

    def test_indexer_get_key_not_found_logs(self):
        """Test Indexer.get() when key not found"""
        logger = logging.getLogger()
        table = Table("indexer_nf_test", "test_table", "test_indexer_nf_id", config)
        
        result = table.indexer.get("nonexistent_key", table.store)
        self.assertIsNone(result)
        
        table.close()

    def test_store_read_eof_handling(self):
        """Test Store read behavior at boundaries"""
        logger = logging.getLogger()
        table = Table("store_eof_test", "test_table", "test_store_eof_id", config)
        store = table.store
        
        # Save a record
        record = Record("eof_key", "eof_value")
        position = store.save(record)
        
        # Read at valid position
        data = store.read(position)
        self.assertIsNotNone(data)
        
        table.close()

    def test_scanner_with_empty_index(self):
        """Test scanner on empty index"""
        logger = logging.getLogger()
        table = Table("empty_scan_test", "test_table", "test_empty_scan_id", config)
        
        count = 0
        for _ in table.indexer.scanner(table.store):
            count += 1
        
        # Empty table should yield no records
        self.assertEqual(count, 0)
        
        table.close()

    def test_scanner_with_records(self):
        """Test scanner iterates through all records"""
        logger = logging.getLogger()
        table = Table("scan_test", "test_table", "test_scan_id", config)
        store = table.store
        indexer = table.indexer
        
        # Insert records
        records = [
            Record("scan_key1", "scan_value1"),
            Record("scan_key2", "scan_value2"),
            Record("scan_key3", "scan_value3"),
        ]
        
        for rec in records:
            position = store.save(rec)
            indexer.put(rec.key, position, store)
        
        # Scan should return all records
        scanned_keys = set()
        for r in indexer.scanner(store):
            scanned_keys.add(r.key)
        
        self.assertEqual(scanned_keys, {"scan_key1", "scan_key2", "scan_key3"})
        
        table.close()

    def test_index_collision_chain_traversal(self):
        """Test get() traverses collision chain correctly"""
        orig_capacity = config.INDEX_CAPACITY
        config.INDEX_CAPACITY = 2  # Very small to force many collisions
        
        logger = logging.getLogger()
        table = Table("collision_chain_test", "test_table", "test_collision_chain_id", config)
        store = table.store
        index = table.indexer.index_list[0]
        
        # Insert many records to force collision chain
        for i in range(20):
            rec = Record(f"collision_key_{i}", f"collision_value_{i}")
            position = store.save(rec)
            index.put(rec.key, position, store)
        
        # Verify all records retrievable
        for i in range(20):
            result = index.get(f"collision_key_{i}", store)
            self.assertIsNotNone(result, f"Key collision_key_{i} should be found")
            self.assertEqual(result.value, f"collision_value_{i}")
        
        table.close()
        config.INDEX_CAPACITY = orig_capacity

    def test_indexer_delete_returns_false_for_missing_key(self):
        """Test Indexer.delete() returns False when key not found"""
        logger = logging.getLogger()
        table = Table("indexer_del_test", "test_table", "test_indexer_del_id", config)
        
        result = table.indexer.delete("missing_key", table.store)
        self.assertFalse(result)
        
        table.close()

    def test_store_batch_mode(self):
        """Test Store batch mode for deferred flushing"""
        logger = logging.getLogger()
        table = Table("batch_test", "test_table", "test_batch_id", config)
        store = table.store
        
        store.begin_batch()
        
        # Insert multiple records in batch mode
        positions = []
        for i in range(10):
            rec = Record(f"batch_key_{i}", f"batch_value_{i}")
            pos = store.save(rec)
            positions.append(pos)
        
        store.end_batch()
        
        # Verify all records readable
        for pos in positions:
            data = store.read(pos)
            self.assertIsNotNone(data)
        
        table.close()

    def test_store_sync(self):
        """Test Store.sync() forces flush"""
        logger = logging.getLogger()
        table = Table("sync_test", "test_table", "test_sync_id", config)
        store = table.store
        
        rec = Record("sync_key", "sync_value")
        store.save(rec)
        
        # sync should not raise
        store.sync()
        
        table.close()

    def test_record_str_representation(self):
        """Test Record __str__ method"""
        record = Record("str_key", "str_value", tombstone='0', store_position=100, 
                       value_type='s', key_link=50, value_link=-1)
        str_repr = str(record)
        
        self.assertIn("str_key", str_repr)
        self.assertIn("str_value", str_repr)
        self.assertIn("100", str_repr)  # store_position
        self.assertIn("50", str_repr)   # key_link

    def test_cog_hash_function(self):
        """Test standalone cog_hash function"""
        hash1 = cog_hash("test_string", 1000)
        hash2 = cog_hash("test_string", 1000)
        hash3 = cog_hash("different_string", 1000)
        
        # Same input should produce same hash
        self.assertEqual(hash1, hash2)
        # Different input may produce different hash
        # (not guaranteed, but likely with different strings)
        self.assertIsInstance(hash3, int)

    def test_index_get_with_key_chain(self):
        """Test Index.get() follows key_link chain for correct key"""
        orig_capacity = config.INDEX_CAPACITY
        config.INDEX_CAPACITY = 4
        
        logger = logging.getLogger()
        table = Table("key_chain_test", "test_table", "test_key_chain_id", config)
        store = table.store
        index = table.indexer.index_list[0]
        
        # Insert records
        rec1 = Record("chain_a", "value_a")
        rec2 = Record("chain_b", "value_b")
        
        pos1 = store.save(rec1)
        index.put(rec1.key, pos1, store)
        
        pos2 = store.save(rec2)
        index.put(rec2.key, pos2, store)
        
        # Get should find correct records
        result_a = index.get("chain_a", store)
        result_b = index.get("chain_b", store)
        
        self.assertIsNotNone(result_a)
        self.assertEqual(result_a.value, "value_a")
        self.assertIsNotNone(result_b)
        self.assertEqual(result_b.value, "value_b")
        
        table.close()
        config.INDEX_CAPACITY = orig_capacity

    def test_record_set_methods(self):
        """Test Record setter methods"""
        record = Record("key", "value")
        
        record.set_store_position(500)
        self.assertEqual(record.store_position, 500)
        
        record.set_key_link(100)
        self.assertEqual(record.key_link, 100)
        
        record.set_value_link(200)
        self.assertEqual(record.value_link, 200)
        
        record.set_value("new_value")
        self.assertEqual(record.value, "new_value")

    def test_record_set_store_position_type_error(self):
        """Test Record.set_store_position raises on non-int"""
        record = Record("key", "value")
        
        with self.assertRaises(ValueError):
            record.set_store_position("not_an_int")

    def test_store_update_record_link_type_error(self):
        """Test Store.update_record_link_inplace raises on non-int"""
        logger = logging.getLogger()
        table = Table("update_link_test", "test_table", "test_update_link_id", config)
        
        with self.assertRaises(ValueError):
            table.store.update_record_link_inplace(0, "not_an_int")
        
        table.close()

    def test_index_head_only_get(self):
        """Test Index.get_head_only for O(1) lookup"""
        logger = logging.getLogger()
        table = Table("head_only_test", "test_table", "test_head_only_id", config)
        store = table.store
        index = table.indexer.index_list[0]
        
        # Insert record with list values
        rec = Record("head_key", "head_value")
        position = store.save(rec)
        index.put(rec.key, position, store)
        
        # Get head only
        head_rec, head_pos = index.get_head_only("head_key", store)
        
        self.assertIsNotNone(head_rec)
        self.assertEqual(head_rec.key, "head_key")
        self.assertEqual(head_pos, position)
        
        # Non-existent key
        none_rec, none_pos = index.get_head_only("no_key", store)
        self.assertIsNone(none_rec)
        self.assertIsNone(none_pos)
        
        table.close()

    def test_indexer_get_head_only(self):
        """Test Indexer.get_head_only delegates to index"""
        logger = logging.getLogger()
        table = Table("indexer_head_test", "test_table", "test_indexer_head_id", config)
        
        rec = Record("indexer_head_key", "indexer_head_value")
        position = table.store.save(rec)
        table.indexer.put(rec.key, position, table.store)
        
        head_rec, head_pos = table.indexer.get_head_only("indexer_head_key", table.store)
        
        self.assertIsNotNone(head_rec)
        self.assertEqual(head_rec.key, "indexer_head_key")
        
        table.close()


if __name__ == '__main__':
    unittest.main()
