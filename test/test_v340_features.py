"""
Tests for:
- flush_interval parameter
- sync() method
- get_head_only() optimization
- put_set cache behavior
"""
import os
import shutil
import unittest
from cog.torque import Graph
from cog.database import Cog
from cog.core import Record
from cog import config


class TestFlushInterval(unittest.TestCase):
    """Test flush_interval and sync() functionality"""
    
    def setUp(self):
        self.test_dir = "/tmp/TestFlushInterval"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        config.CUSTOM_COG_DB_PATH = self.test_dir
    
    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_graph_with_default_flush_interval(self):
        """Graph should work with default flush_interval=1"""
        g = Graph("test_graph")
        g.put("a", "rel", "b")
        g.put("b", "rel", "c")
        result = g.v("a").out("rel").all()
        self.assertEqual(result, {'result': [{'id': 'b'}]})
        g.close()
    
    def test_graph_with_high_flush_interval(self):
        """Graph should work with flush_interval > 1"""
        g = Graph("test_graph", flush_interval=100)
        for i in range(50):
            g.put(f"node_{i}", "connects", f"node_{i+1}")
        g.sync()  # Force flush
        
        # Verify data is readable
        result = g.v("node_0").out("connects").all()
        self.assertEqual(result, {'result': [{'id': 'node_1'}]})
        g.close()
    
    def test_graph_sync_method(self):
        """sync() should flush all pending writes"""
        g = Graph("test_graph", flush_interval=1000)  # Very high interval
        g.put("x", "rel", "y")
        g.sync()  # Force flush
        g.close()
        
        # Reopen and verify data persisted
        g2 = Graph("test_graph")
        result = g2.v("x").out("rel").all()
        self.assertEqual(result, {'result': [{'id': 'y'}]})
        g2.close()
    
    def test_cog_flush_interval(self):
        """Cog should accept flush_interval parameter"""
        cog = Cog(flush_interval=50)
        self.assertEqual(cog.flush_interval, 50)
        cog.close()
    
    def test_cog_sync_method(self):
        """Cog.sync() should flush all tables"""
        cog = Cog(flush_interval=100)
        cog.create_or_load_namespace("test_ns")
        cog.create_table("test_table", "test_ns")
        cog.put(Record("key1", "value1"))
        cog.sync()
        cog.close()


class TestGetHeadOnly(unittest.TestCase):
    """Test get_head_only optimization"""
    
    def setUp(self):
        self.test_dir = "/tmp/TestGetHeadOnly"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        config.CUSTOM_COG_DB_PATH = self.test_dir
    
    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_get_head_only_returns_none_for_missing_key(self):
        """get_head_only should return (None, None) for missing keys"""
        cog = Cog()
        cog.create_or_load_namespace("test")
        cog.create_table("t1", "test")
        
        record, pos = cog.current_table.indexer.get_head_only("nonexistent", cog.current_table.store)
        self.assertIsNone(record)
        self.assertIsNone(pos)
        cog.close()
    
    def test_get_head_only_returns_head_record(self):
        """get_head_only should return head record without loading value chain"""
        cog = Cog()
        cog.create_or_load_namespace("test")
        cog.create_table("t1", "test")
        
        # Add a record
        cog.put(Record("mykey", "myvalue"))
        
        # Get head only
        record, pos = cog.current_table.indexer.get_head_only("mykey", cog.current_table.store)
        self.assertIsNotNone(record)
        self.assertEqual(record.key, "mykey")
        self.assertIsNotNone(pos)
        cog.close()
    
    def test_get_head_only_with_multivalue(self):
        """get_head_only should return head even for multi-value keys"""
        cog = Cog()
        cog.create_or_load_namespace("test")
        cog.create_table("t1", "test")
        
        # Add multiple values to same key
        cog.put_list(Record("mykey", "val1"))
        cog.put_list(Record("mykey", "val2"))
        cog.put_list(Record("mykey", "val3"))
        
        # Get head only - should NOT traverse the value chain
        record, pos = cog.current_table.indexer.get_head_only("mykey", cog.current_table.store)
        self.assertIsNotNone(record)
        self.assertEqual(record.key, "mykey")
        # Value should be just the head value, not all values
        self.assertEqual(record.value, "val3")  # Newest value is at head (prepend)
        cog.close()


class TestPutSetOptimization(unittest.TestCase):
    """Test optimized put_set with cache"""
    
    def setUp(self):
        self.test_dir = "/tmp/TestPutSetOpt"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        config.CUSTOM_COG_DB_PATH = self.test_dir
    
    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_put_set_deduplication(self):
        """put_set should deduplicate values"""
        cog = Cog()
        cog.create_or_load_namespace("test")
        cog.create_table("t1", "test")
        
        # Add same value multiple times
        cog.put_set(Record("key1", "val1"))
        cog.put_set(Record("key1", "val1"))
        cog.put_set(Record("key1", "val1"))
        
        # Should only have one value
        record = cog.get("key1")
        self.assertEqual(len(record.value), 1)
        self.assertIn("val1", record.value)
        cog.close()
    
    def test_put_set_multiple_values(self):
        """put_set should store multiple unique values"""
        cog = Cog()
        cog.create_or_load_namespace("test")
        cog.create_table("t1", "test")
        
        cog.put_set(Record("key1", "val1"))
        cog.put_set(Record("key1", "val2"))
        cog.put_set(Record("key1", "val3"))
        
        record = cog.get("key1")
        self.assertEqual(len(record.value), 3)
        self.assertEqual(set(record.value), {"val1", "val2", "val3"})
        cog.close()
    
    def test_put_set_cache_hit(self):
        """put_set should use cache for deduplication"""
        cog = Cog()
        cog.create_or_load_namespace("test")
        cog.create_table("t1", "test")
        
        # Add values - second call should use cache
        cog.put_set(Record("key1", "val1"))
        cache_key = ("t1", "key1")
        self.assertIn(cache_key, cog.cache)
        
        # Add same value again - should hit cache and skip write
        cog.put_set(Record("key1", "val1"))
        
        # Add new value - should update cache
        cog.put_set(Record("key1", "val2"))
        self.assertIn("val2", cog.cache[cache_key].value)
        
        cog.close()


class TestStarGraphPerformance(unittest.TestCase):
    """Test that star graphs (hub nodes) work correctly after optimization"""
    
    def setUp(self):
        self.test_dir = "/tmp/TestStarGraph"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        config.CUSTOM_COG_DB_PATH = self.test_dir
    
    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_star_graph_correctness(self):
        """Star graph should maintain correct adjacency"""
        g = Graph("star_test", flush_interval=100)
        
        # Create star: hub -> [a, b, c, d, e]
        targets = ["a", "b", "c", "d", "e"]
        for target in targets:
            g.put("hub", "connects", target)
        g.sync()
        
        # Verify all edges exist
        result = g.v("hub").out("connects").all()
        result_ids = {r['id'] for r in result['result']}
        self.assertEqual(result_ids, set(targets))
        
        g.close()
    
    def test_star_graph_with_many_edges(self):
        """Star graph with 100 edges should be correct"""
        g = Graph("star_test", flush_interval=100)
        
        num_edges = 100
        for i in range(num_edges):
            g.put("hub", "connects", f"target_{i}")
        g.sync()
        
        result = g.v("hub").out("connects").all()
        self.assertEqual(len(result['result']), num_edges)
        
        g.close()


class TestAsyncFlush(unittest.TestCase):
    """Test async flush behavior (auto-enabled when flush_interval > 1)"""
    
    def setUp(self):
        self.test_dir = "/tmp/TestAsyncFlush"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        config.CUSTOM_COG_DB_PATH = self.test_dir
    
    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_async_flush_data_integrity(self):
        """Data should be intact after async flush"""
        g = Graph("async_test", flush_interval=50)
        
        # Write some data
        for i in range(30):
            g.put(f"n{i}", "rel", f"n{i+1}")
        
        g.sync()
        g.close()
        
        # Reopen and verify
        g2 = Graph("async_test")
        for i in range(30):
            result = g2.v(f"n{i}").out("rel").all()
            self.assertEqual(result, {'result': [{'id': f"n{i+1}"}]})
        g2.close()


if __name__ == "__main__":
    unittest.main()
