"""
Test to verify that predicate tables are only created with hashed names,
not with raw predicate names (regression test for duplicate file bug).
"""
from cog.torque import Graph
from cog.database import hash_predicate
from cog import config as cfg
import unittest
import os
import shutil

DIR_NAME = "TestFileCount"


class TestPredicateFileCreation(unittest.TestCase):
    """
    Ensure Graph.put() and Graph.put_batch() only create tables with hashed
    predicate names, not raw predicate names.
    """

    @classmethod
    def setUpClass(cls):
        cls.base_path = os.path.join(cfg.COG_PATH_PREFIX, DIR_NAME)
        if os.path.exists(cls.base_path):
            shutil.rmtree(cls.base_path)
        os.makedirs(cls.base_path, exist_ok=True)

    def test_put_creates_only_hashed_predicate_files(self):
        """Verify Graph.put() doesn't create files with raw predicate names."""
        graph_name = "test_put_hashed"
        g = Graph(graph_name=graph_name, cog_home=DIR_NAME)
        
        # Use distinctive predicate names that would be obvious if stored raw
        predicates = ["KNOWS", "LIKES", "WORKS_AT"]
        
        g.put("alice", "KNOWS", "bob")
        g.put("alice", "LIKES", "charlie")
        g.put("bob", "WORKS_AT", "acme")
        
        # Get the actual graph directory from config
        graph_dir = g.config.cog_data_dir(graph_name)
        g.close()
        
        files = os.listdir(graph_dir)
        
        # No file should contain raw predicate names
        for predicate in predicates:
            raw_files = [f for f in files if f.startswith(predicate + "-")]
            self.assertEqual(
                len(raw_files), 0,
                f"Found files with raw predicate name '{predicate}': {raw_files}. "
                f"Predicates should be hashed before creating table files."
            )
        
        # Verify hashed predicate files DO exist
        for predicate in predicates:
            hashed = hash_predicate(predicate)
            hashed_files = [f for f in files if f.startswith(hashed + "-")]
            self.assertGreaterEqual(
                len(hashed_files), 1,
                f"No files found for hashed predicate {hashed} (from '{predicate}')"
            )

    def test_put_batch_creates_only_hashed_predicate_files(self):
        """Verify Graph.put_batch() doesn't create files with raw predicate names."""
        graph_name = "test_batch_hashed"
        g = Graph(graph_name=graph_name, cog_home=DIR_NAME)
        
        predicates = ["FOLLOWS", "MENTIONS", "REPLIES_TO"]
        
        g.put_batch([
            ("user1", "FOLLOWS", "user2"),
            ("user1", "MENTIONS", "user3"),
            ("user2", "REPLIES_TO", "user1"),
        ])
        
        # Get the actual graph directory from config
        graph_dir = g.config.cog_data_dir(graph_name)
        g.close()
        
        files = os.listdir(graph_dir)
        
        # No file should contain raw predicate names
        for predicate in predicates:
            raw_files = [f for f in files if f.startswith(predicate + "-")]
            self.assertEqual(
                len(raw_files), 0,
                f"Found files with raw predicate name '{predicate}': {raw_files}. "
                f"put_batch should use hashed predicate names."
            )
        
        # Verify hashed predicate files DO exist
        for predicate in predicates:
            hashed = hash_predicate(predicate)
            hashed_files = [f for f in files if f.startswith(hashed + "-")]
            self.assertGreaterEqual(
                len(hashed_files), 1,
                f"No files found for hashed predicate {hashed} (from '{predicate}')"
            )

    def test_expected_file_count(self):
        """Verify the expected number of files is created (no duplicates)."""
        graph_name = "test_file_count"
        g = Graph(graph_name=graph_name, cog_home=DIR_NAME)
        
        # 3 predicates
        g.put("a", "P1", "b")
        g.put("a", "P2", "c")
        g.put("a", "P3", "d")
        
        # Get the actual graph directory from config
        graph_dir = g.config.cog_data_dir(graph_name)
        g.close()
        
        files = os.listdir(graph_dir)
        
        # Expected: 3 predicate tables + 2 system tables = 5 tables
        # Each table = 2 files (index + store) = 10 files total
        # System tables: TOR_NODE_SET, TOR_EDGE_SET
        self.assertEqual(
            len(files), 10,
            f"Expected 10 files (3 predicates + 2 system tables × 2 files each), "
            f"but found {len(files)}: {sorted(files)}"
        )

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.base_path):
            shutil.rmtree(cls.base_path)


if __name__ == '__main__':
    unittest.main()

