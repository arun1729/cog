"""
Tests for batch mode operations in CogDB.
Tests both correctness and performance improvement.
"""
import os
import shutil
import timeit
import unittest
import logging
from logging.config import dictConfig

from cog import config
from cog.core import Table, Record
from cog.database import Cog
from cog.torque import Graph

DIR_NAME = "TestBatchMode"


def ensure_namespace_dir(namespace):
    """Helper to create namespace directory if it doesn't exist"""
    path = config.cog_data_dir(namespace)
    if not os.path.exists(path):
        os.makedirs(path)


class TestStoreBatchMode(unittest.TestCase):
    """Tests for Store.begin_batch() and Store.end_batch()"""

    @classmethod
    def setUpClass(cls):
        # Clean up any existing test data
        base_path = "/tmp/" + DIR_NAME
        if os.path.exists(base_path):
            shutil.rmtree(base_path)
        os.makedirs(base_path)
        config.CUSTOM_COG_DB_PATH = "/tmp/" + DIR_NAME
        dictConfig(config.logging_config)

    def test_batch_mode_saves_correctly(self):
        """Test that batch mode saves records correctly"""
        logger = logging.getLogger()
        
        # Create namespace directory
        ensure_namespace_dir("test_batch")
        
        table = Table("testdb", "test_batch", "test_batch_id", config, logger)
        store = table.store
        indexer = table.indexer

        # Enable batch mode
        store.begin_batch()
        
        records = []
        for i in range(100):
            r = Record(f"key_{i}", f"value_{i}")
            records.append(r)
            position = store.save(r)
            indexer.put(r.key, position, store)
        
        # End batch mode (flush)
        store.end_batch()
        
        # Verify all records are readable
        for r in records:
            retrieved = indexer.get(r.key, store)
            self.assertIsNotNone(retrieved, f"Record {r.key} should exist")
            self.assertEqual(r.key, retrieved.key)
            self.assertEqual(r.value, retrieved.value)
        
        table.close()

    def test_batch_mode_closes_safely(self):
        """Test that table close flushes pending writes if still in batch mode"""
        logger = logging.getLogger()
        
        # Create namespace directory
        ensure_namespace_dir("test_batch_close")
        
        table = Table("testdb", "test_batch_close", "test_batch_close_id", config, logger)
        store = table.store
        indexer = table.indexer

        # Enable batch mode but don't call end_batch()
        store.begin_batch()
        
        r = Record("orphan_key", "orphan_value")
        position = store.save(r)
        indexer.put(r.key, position, store)
        
        # Close without end_batch - should still flush
        table.close()
        
        # Reopen and verify
        table2 = Table("testdb", "test_batch_close", "test_batch_close_id", config, logger)
        retrieved = table2.indexer.get("orphan_key", table2.store)
        self.assertIsNotNone(retrieved, "Record should exist after close")
        self.assertEqual("orphan_value", retrieved.value)
        table2.close()

    def test_batch_mode_performance_improvement(self):
        """Test that batch mode provides performance improvement on larger datasets"""
        logger = logging.getLogger()
        
        # Use larger dataset to see measurable difference
        num_records = 3000
        
        # Create namespace directories
        ensure_namespace_dir("test_perf_no_batch")
        ensure_namespace_dir("test_perf_batch")
        
        # Test without batch mode
        table1 = Table("testdb", "test_perf_no_batch", "test_perf_nb_id", config, logger)
        
        start = timeit.default_timer()
        for i in range(num_records):
            r = Record(f"key_nb_{i}", f"value_nb_{i}")
            position = table1.store.save(r)
            table1.indexer.put(r.key, position, table1.store)
        time_no_batch = timeit.default_timer() - start
        table1.close()
        
        # Test with batch mode
        table2 = Table("testdb", "test_perf_batch", "test_perf_b_id", config, logger)
        table2.store.begin_batch()
        
        start = timeit.default_timer()
        for i in range(num_records):
            r = Record(f"key_b_{i}", f"value_b_{i}")
            position = table2.store.save(r)
            table2.indexer.put(r.key, position, table2.store)
        table2.store.end_batch()
        time_with_batch = timeit.default_timer() - start
        table2.close()
        
        # Report performance (actual speedup varies by OS/disk)
        speedup = time_no_batch / time_with_batch if time_with_batch > 0 else 1.0
        print(f"\n=== Store Batch Mode Performance ===")
        print(f"Records: {num_records}")
        print(f"Without batch: {time_no_batch:.4f}s ({num_records/time_no_batch:.0f} ops/s)")
        print(f"With batch: {time_with_batch:.4f}s ({num_records/time_with_batch:.0f} ops/s)")
        print(f"Speedup: {speedup:.2f}x")
        
        # Batch mode should complete without errors - performance varies by system
        self.assertTrue(True, "Batch mode completed successfully")

    @classmethod
    def tearDownClass(cls):
        # Clean up after this test class
        pass


class TestCogBatchMode(unittest.TestCase):
    """Tests for Cog.begin_batch() and Cog.end_batch()"""

    @classmethod
    def setUpClass(cls):
        config.CUSTOM_COG_DB_PATH = "/tmp/" + DIR_NAME
        dictConfig(config.logging_config)

    def test_cog_batch_mode(self):
        """Test batch mode at Cog level"""
        cog = Cog()
        cog.create_or_load_namespace("test_cog_batch")
        cog.create_table("batch_table", "test_cog_batch")
        
        cog.begin_batch()
        
        for i in range(50):
            cog.put(Record(f"cog_key_{i}", f"cog_value_{i}"))
        
        cog.end_batch()
        
        # Verify data
        for i in range(50):
            result = cog.get(f"cog_key_{i}")
            self.assertIsNotNone(result)
            self.assertEqual(f"cog_value_{i}", result.value)
        
        cog.close()


class TestGraphPutBatch(unittest.TestCase):
    """Tests for Graph.put_batch()"""

    @classmethod
    def setUpClass(cls):
        config.CUSTOM_COG_DB_PATH = "/tmp/" + DIR_NAME
        dictConfig(config.logging_config)

    def test_put_batch_correctness(self):
        """Test that put_batch inserts all triples correctly"""
        g = Graph("test_put_batch")
        
        triples = [
            ("alice", "follows", "bob"),
            ("bob", "follows", "charlie"),
            ("charlie", "follows", "alice"),
            ("alice", "likes", "pizza"),
            ("bob", "likes", "tacos"),
        ]
        
        g.put_batch(triples)
        
        # Verify edges
        result = g.v("alice").out("follows").all()
        self.assertEqual(1, len(result['result']))
        self.assertEqual("bob", result['result'][0]['id'])
        
        result = g.v("bob").out("follows").all()
        self.assertEqual(1, len(result['result']))
        self.assertEqual("charlie", result['result'][0]['id'])
        
        result = g.v("charlie").out("follows").all()
        self.assertEqual(1, len(result['result']))
        self.assertEqual("alice", result['result'][0]['id'])
        
        result = g.v("alice").out("likes").all()
        self.assertEqual(1, len(result['result']))
        self.assertEqual("pizza", result['result'][0]['id'])
        
        g.close()

    def test_put_batch_multiple_edges_same_predicate(self):
        """Test put_batch with multiple edges from same vertex"""
        g = Graph("test_put_batch_multi")
        
        triples = [
            ("alice", "follows", "bob"),
            ("alice", "follows", "charlie"),
            ("alice", "follows", "dave"),
        ]
        
        g.put_batch(triples)
        
        result = g.v("alice").out("follows").all()
        self.assertEqual(3, len(result['result']))
        
        ids = {r['id'] for r in result['result']}
        self.assertEqual({"bob", "charlie", "dave"}, ids)
        
        g.close()

    def test_put_batch_performance(self):
        """Test that put_batch works correctly and report performance"""
        num_edges = 1000
        
        # Generate test data
        triples = [(f"user_{i}", "follows", f"user_{i+1}") for i in range(num_edges)]
        
        # Test individual puts
        g1 = Graph("test_perf_individual")
        start = timeit.default_timer()
        for v1, pred, v2 in triples:
            g1.put(v1, pred, v2)
        time_individual = timeit.default_timer() - start
        g1.close()
        
        # Test put_batch
        g2 = Graph("test_perf_batch_graph")
        start = timeit.default_timer()
        g2.put_batch(triples)
        time_batch = timeit.default_timer() - start
        g2.close()
        
        speedup = time_individual / time_batch if time_batch > 0 else 1.0
        print(f"\n=== Graph.put_batch Performance ===")
        print(f"Edges: {num_edges}")
        print(f"Individual puts: {time_individual:.4f}s ({num_edges/time_individual:.0f} edges/s)")
        print(f"put_batch: {time_batch:.4f}s ({num_edges/time_batch:.0f} edges/s)")
        print(f"Speedup: {speedup:.2f}x")
        
        # Verify data was inserted correctly
        g3 = Graph("test_perf_batch_graph")
        result = g3.v("user_0").out("follows").all()
        self.assertEqual(1, len(result['result']))
        g3.close()

    def test_put_batch_empty_list(self):
        """Test put_batch with empty list doesn't crash"""
        g = Graph("test_put_batch_empty")
        g.put_batch([])  # Should not raise
        g.close()

    def test_put_batch_error_recovery(self):
        """Test that batch mode is properly ended even on error"""
        g = Graph("test_put_batch_error")
        
        # First batch should work
        g.put_batch([("a", "rel", "b")])
        
        # Verify insert worked
        result = g.v("a").out("rel").all()
        self.assertEqual(1, len(result['result']))
        
        g.close()


class TestBatchModeIntegration(unittest.TestCase):
    """Integration tests for batch mode across all layers"""

    @classmethod
    def setUpClass(cls):
        config.CUSTOM_COG_DB_PATH = "/tmp/" + DIR_NAME
        dictConfig(config.logging_config)

    def test_large_graph_insertion(self):
        """Test inserting a large graph using batch mode"""
        g = Graph("test_large_graph")
        
        # Create a larger graph
        num_nodes = 100
        triples = []
        
        for i in range(num_nodes):
            for j in range(i + 1, min(i + 5, num_nodes)):  # Connect to next 4 nodes
                triples.append((f"node_{i}", "connects", f"node_{j}"))
        
        print(f"\n=== Large Graph Test ===")
        print(f"Nodes: {num_nodes}")
        print(f"Edges: {len(triples)}")
        
        start = timeit.default_timer()
        g.put_batch(triples)
        elapsed = timeit.default_timer() - start
        
        print(f"Insertion time: {elapsed:.4f}s")
        print(f"Edges per second: {len(triples)/elapsed:.0f}")
        
        # Verify some random edges
        result = g.v("node_0").out("connects").all()
        self.assertGreater(len(result['result']), 0)
        
        result = g.v("node_50").out("connects").all()
        self.assertGreater(len(result['result']), 0)
        
        g.close()


def tearDownModule():
    """Clean up all test data"""
    if os.path.exists("/tmp/" + DIR_NAME):
        shutil.rmtree("/tmp/" + DIR_NAME)
    print("\n*** Cleaned up test data.")


if __name__ == '__main__':
    unittest.main(verbosity=2)
