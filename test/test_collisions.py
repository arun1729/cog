#!/usr/bin/env python3
"""
Test hash collision handling in CogDB.

This test uses a very small INDEX_CAPACITY to force collisions, then verifies:
1. All records can be retrieved correctly via get()
2. Scanner returns all records (follows key_link chains)
3. Embeddings with collisions work correctly
"""
import unittest
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cog.torque import Graph
from cog.database import Cog
from cog.core import Record
from cog import config

# Save original config
ORIGINAL_INDEX_CAPACITY = config.INDEX_CAPACITY

# Test directory
TEST_DIR = "/tmp/CogCollisionTest"


class TestHashCollisions(unittest.TestCase):
    """Test that hash collisions are handled correctly."""

    @classmethod
    def setUpClass(cls):
        """Set up test with very small INDEX_CAPACITY to force collisions."""
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)
        os.makedirs(TEST_DIR)
        
        # Use very small capacity to guarantee collisions
        # With capacity=101 and 500 items, average ~5 collisions per bucket
        config.INDEX_CAPACITY = 101  # Small prime number

    @classmethod
    def tearDownClass(cls):
        """Restore original config and clean up."""
        config.INDEX_CAPACITY = ORIGINAL_INDEX_CAPACITY
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)

    def setUp(self):
        """Clean test directory before each test."""
        test_path = os.path.join(TEST_DIR, "test_data")
        if os.path.exists(test_path):
            shutil.rmtree(test_path)

    # =========================================================================
    # Basic key-value collision tests
    # =========================================================================

    def test_basic_collision_handling(self):
        """Test that all records are retrievable despite hash collisions."""
        cog = Cog()
        cog.create_or_load_namespace("collision_test")
        cog.create_table("test_table", "collision_test")

        # Insert 500 records with capacity=101, guaranteeing many collisions
        num_records = 500
        for i in range(num_records):
            key = f"key_{i}"
            value = f"value_{i}"
            cog.put(Record(key, value))

        # Verify all records can be retrieved
        retrieved = 0
        for i in range(num_records):
            key = f"key_{i}"
            expected_value = f"value_{i}"
            record = cog.get(key)
            self.assertIsNotNone(record, f"Record with key '{key}' not found")
            self.assertEqual(record.value, expected_value, f"Wrong value for key '{key}'")
            retrieved += 1

        self.assertEqual(retrieved, num_records, "Not all records were retrieved")
        cog.close()

    def test_scanner_returns_all_collided_records(self):
        """Test that scanner returns all records including those with hash collisions."""
        cog = Cog()
        cog.create_or_load_namespace("scanner_test")
        cog.create_table("test_table", "scanner_test")

        # Insert records
        num_records = 500
        inserted_keys = set()
        for i in range(num_records):
            key = f"key_{i}"
            value = f"value_{i}"
            cog.put(Record(key, value))
            inserted_keys.add(key)

        # Scan and collect all keys
        scanned_keys = set()
        for record in cog.scanner():
            scanned_keys.add(record.key)

        # All inserted keys should be found by scanner
        self.assertEqual(len(scanned_keys), num_records, 
                        f"Scanner returned {len(scanned_keys)} records, expected {num_records}")
        self.assertEqual(inserted_keys, scanned_keys, 
                        "Scanner did not return all inserted keys")

        cog.close()

    def test_update_with_collisions(self):
        """Test that updates work correctly with hash collisions."""
        cog = Cog()
        cog.create_or_load_namespace("update_test")
        cog.create_table("test_table", "update_test")

        # Insert initial records
        num_records = 200
        for i in range(num_records):
            cog.put(Record(f"key_{i}", f"original_{i}"))

        # Update all records
        for i in range(num_records):
            cog.put(Record(f"key_{i}", f"updated_{i}"))

        # Verify updates
        for i in range(num_records):
            record = cog.get(f"key_{i}")
            self.assertIsNotNone(record)
            self.assertEqual(record.value, f"updated_{i}")

        cog.close()

    # =========================================================================
    # Graph collision tests
    # =========================================================================

    def test_graph_collision_handling(self):
        """Test that graph operations work correctly with hash collisions."""
        g = Graph("collision_graph", cog_home="CollisionTest", cog_path_prefix=TEST_DIR)

        # Add many edges to force collisions
        num_nodes = 300
        for i in range(num_nodes):
            g.put(f"node_{i}", "connects", f"node_{(i+1) % num_nodes}")

        # Verify all nodes are queryable
        for i in range(0, num_nodes, 10):  # Sample every 10th node
            result = g.v(f"node_{i}").out("connects").all()
            self.assertEqual(len(result['result']), 1)
            expected = f"node_{(i+1) % num_nodes}"
            self.assertEqual(result['result'][0]['id'], expected)

        # Verify scan returns all nodes (may include edge targets)
        scan_result = g.scan(limit=num_nodes + 100)
        self.assertGreaterEqual(len(scan_result['result']), num_nodes)

        g.close()

    # =========================================================================
    # Embedding collision tests
    # =========================================================================

    def test_embedding_collision_handling(self):
        """Test that embeddings work correctly with hash collisions."""
        g = Graph("embed_collision", cog_home="EmbedCollisionTest", cog_path_prefix=TEST_DIR)

        # Generate embeddings for many words
        num_embeddings = 500
        embedding_dim = 10
        
        # Store embeddings
        for i in range(num_embeddings):
            word = f"word_{i}"
            # Deterministic embedding based on word index
            embedding = [float(i + j) / num_embeddings for j in range(embedding_dim)]
            g.put_embedding(word, embedding)

        # Verify all embeddings can be retrieved
        retrieved = 0
        for i in range(num_embeddings):
            word = f"word_{i}"
            embedding = g.get_embedding(word)
            self.assertIsNotNone(embedding, f"Embedding for '{word}' not found")
            self.assertEqual(len(embedding), embedding_dim)
            # Verify first value matches expected
            expected_first = float(i) / num_embeddings
            self.assertAlmostEqual(embedding[0], expected_first, places=5)
            retrieved += 1

        self.assertEqual(retrieved, num_embeddings)

        # Verify embedding_stats returns correct count
        stats = g.embedding_stats()
        self.assertEqual(stats['count'], num_embeddings)
        self.assertEqual(stats['dimensions'], embedding_dim)

        g.close()

    def test_k_nearest_with_collisions(self):
        """Test that k_nearest works correctly when embeddings have hash collisions."""
        g = Graph("knn_collision", cog_home="KNNCollisionTest", cog_path_prefix=TEST_DIR)

        # Create embeddings in clusters to test similarity search
        # Cluster 1: words 0-99 have similar embeddings
        # Cluster 2: words 100-199 have similar embeddings
        # etc.
        num_embeddings = 300
        embedding_dim = 10

        for i in range(num_embeddings):
            word = f"word_{i}"
            cluster = i // 100
            # Base vector for cluster + small noise
            embedding = [float(cluster) + (i % 100) * 0.001 + j * 0.01 
                        for j in range(embedding_dim)]
            g.put_embedding(word, embedding)

        # Find k-nearest to word_50 (cluster 0)
        # Note: Don't call g.v() first - that returns empty list (no graph vertices)
        # Instead, call k_nearest directly which scans the embedding table
        result = g.k_nearest("word_50", k=5).all()
        
        # All nearest neighbors should be from cluster 0 (words 0-99)
        self.assertEqual(len(result['result']), 5)
        for item in result['result']:
            word_id = item['id']
            word_num = int(word_id.split('_')[1])
            self.assertTrue(0 <= word_num < 100, 
                          f"Expected cluster 0 word, got {word_id}")

        g.close()

    def test_batch_embedding_with_collisions(self):
        """Test that batch embedding insertion works with collisions."""
        g = Graph("batch_collision", cog_home="BatchCollisionTest", cog_path_prefix=TEST_DIR)

        # Prepare batch data
        num_embeddings = 400
        embedding_dim = 10
        batch = []
        
        for i in range(num_embeddings):
            word = f"batchword_{i}"
            embedding = [float(i * j) / 1000 for j in range(embedding_dim)]
            batch.append((word, embedding))

        # Batch insert
        g.put_embeddings_batch(batch)

        # Verify all are retrievable
        stats = g.embedding_stats()
        self.assertEqual(stats['count'], num_embeddings)

        # Spot check some embeddings
        for i in [0, 100, 200, 399]:
            word = f"batchword_{i}"
            embedding = g.get_embedding(word)
            self.assertIsNotNone(embedding, f"Embedding for '{word}' not found")
            self.assertEqual(len(embedding), embedding_dim)

        g.close()


if __name__ == '__main__':
    unittest.main()
