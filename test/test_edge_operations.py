"""
Tests for edge update operations and index deletion with hash collisions.

1. update_edge: Verifies that updating edges preserves unrelated edges
2. Index.delete: Verifies deletion works correctly in hash collision chains
"""

import pytest
import os
import shutil
import uuid
from cog.torque import Graph
from cog.database import Cog, hash_predicate, in_nodes, out_nodes
from cog.core import Table, Record, Index, cog_hash
from cog import config


COG_HOME = "test_edge_operations_home"


@pytest.fixture
def clean_graph():
    """Create a fresh graph for each test with complete isolation."""
    unique_dir = COG_HOME + "_" + str(uuid.uuid4())[:8]
    full_path = "/tmp/" + unique_dir
    
    if os.path.exists(full_path):
        shutil.rmtree(full_path)
    os.makedirs(full_path)
    config.CUSTOM_COG_DB_PATH = full_path
    
    g = Graph("test_graph")
    yield g
    g.close()
    if os.path.exists(full_path):
        shutil.rmtree(full_path)


@pytest.fixture
def clean_cog():
    """Create a fresh Cog instance for low-level tests."""
    unique_dir = COG_HOME + "_" + str(uuid.uuid4())[:8]
    full_path = "/tmp/" + unique_dir
    
    if os.path.exists(full_path):
        shutil.rmtree(full_path)
    os.makedirs(full_path)
    config.CUSTOM_COG_DB_PATH = full_path
    
    cog = Cog()
    yield cog
    cog.close()
    if os.path.exists(full_path):
        shutil.rmtree(full_path)
    if os.path.exists("/tmp/" + COG_HOME):
        shutil.rmtree("/tmp/" + COG_HOME)


class TestUpdateEdge:
    """
    Tests for update_edge behavior.
    
    Verifies that when updating an edge (e.g., changing A -> follows -> B to A -> follows -> C),
    only the specific edge is modified and unrelated edges are preserved.
    
    For example, if D -> follows -> B also exists, it should remain intact.
    """

    def test_update_edge_preserves_other_incoming_edges(self, clean_graph):
        """
        Scenario:
        - A follows B
        - D follows B
        - Update A to follow C instead
        - D's edge to B should be preserved
        """
        g = clean_graph
        
        # Setup: A -> B and D -> B
        g.put("A", "follows", "B")
        g.put("D", "follows", "B")
        
        # Verify setup
        a_follows = g.v("A").out("follows").all()
        assert len(a_follows["result"]) == 1
        assert a_follows["result"][0]["id"] == "B"
        
        d_follows = g.v("D").out("follows").all()
        assert len(d_follows["result"]) == 1
        assert d_follows["result"][0]["id"] == "B"
        
        b_followers = g.v("B").inc("follows").all()
        assert len(b_followers["result"]) == 2
        follower_ids = {r["id"] for r in b_followers["result"]}
        assert follower_ids == {"A", "D"}
        
        # Action: Update A to follow C instead of B
        g.put("A", "follows", "C", update=True)
        
        # Verify: A now follows C
        a_follows_after = g.v("A").out("follows").all()
        assert len(a_follows_after["result"]) == 1
        assert a_follows_after["result"][0]["id"] == "C"
        
        # Verify: D still follows B (THIS IS THE KEY ASSERTION)
        d_follows_after = g.v("D").out("follows").all()
        assert len(d_follows_after["result"]) == 1, "D's edge to B was incorrectly deleted!"
        assert d_follows_after["result"][0]["id"] == "B"
        
        # Verify: B's incoming edges only contain D now (A was removed)
        b_followers_after = g.v("B").inc("follows").all()
        assert len(b_followers_after["result"]) == 1
        assert b_followers_after["result"][0]["id"] == "D"
        
        # Verify: C's incoming edges contain A
        c_followers = g.v("C").inc("follows").all()
        assert len(c_followers["result"]) == 1
        assert c_followers["result"][0]["id"] == "A"

    def test_update_edge_with_multiple_targets(self, clean_graph):
        """
        Scenario:
        - A follows [B, C, D]
        - E follows B
        - Update A to follow X
        - All of A's old edges should be removed
        - E's edge to B should be preserved
        """
        g = clean_graph
        
        # Setup
        g.put("A", "follows", "B")
        g.put("A", "follows", "C")
        g.put("A", "follows", "D")
        g.put("E", "follows", "B")  # Another node pointing to B
        
        # Verify A follows 3 nodes
        a_follows = g.v("A").out("follows").all()
        assert len(a_follows["result"]) == 3
        
        # Update A to only follow X
        g.put("A", "follows", "X", update=True)
        
        # A should now only follow X
        a_follows_after = g.v("A").out("follows").all()
        assert len(a_follows_after["result"]) == 1
        assert a_follows_after["result"][0]["id"] == "X"
        
        # E should still follow B
        e_follows = g.v("E").out("follows").all()
        assert len(e_follows["result"]) == 1, "E's edge to B was incorrectly deleted!"
        assert e_follows["result"][0]["id"] == "B"

    def test_update_edge_single_value(self, clean_graph):
        """Test update when there's only a single outgoing edge."""
        g = clean_graph
        
        g.put("A", "likes", "B")
        g.put("A", "likes", "C", update=True)
        
        result = g.v("A").out("likes").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "C"

    def test_update_edge_no_existing_edge(self, clean_graph):
        """Test update when there's no existing edge (should just create)."""
        g = clean_graph
        
        g.put("A", "likes", "B", update=True)
        
        result = g.v("A").out("likes").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "B"


class TestIndexDelete:
    """
    Tests for Index.delete behavior with hash collision chains.
    
    Verifies that deleting keys works correctly regardless of their position
    in a hash collision chain (head, middle, or tail).
    """

    def test_delete_second_item_in_collision_chain(self, clean_cog):
        """
        Test deleting a key that is the second item in a hash collision chain.
        """
        cog = clean_cog
        
        # We need to find or create keys that hash to the same bucket
        # For testing, we'll insert multiple keys and delete them in various orders
        cog.create_table("collision_test", config.COG_DEFAULT_NAMESPACE)
        
        # Insert several keys - some may collide
        keys = ["key_a", "key_b", "key_c", "key_d", "key_e"]
        for key in keys:
            cog.put(Record(key, f"value_{key}"))
        
        # Verify all keys exist
        for key in keys:
            record = cog.get(key)
            assert record is not None, f"Key {key} should exist"
            assert record.value == f"value_{key}"
        
        # Delete keys in reverse order (more likely to hit collision chain scenarios)
        for key in reversed(keys):
            cog.delete(key)
            # Verify deletion
            record = cog.get(key)
            assert record is None, f"Key {key} should be deleted"
        
        # Verify all keys are gone
        for key in keys:
            assert cog.get(key) is None

    def test_delete_from_middle_of_chain(self, clean_cog):
        """Test deleting from the middle of a collision chain."""
        cog = clean_cog
        cog.create_table("chain_test", config.COG_DEFAULT_NAMESPACE)
        
        # Insert keys
        for i in range(10):
            cog.put(Record(f"item_{i}", f"value_{i}"))
        
        # Delete from middle
        cog.delete("item_5")
        assert cog.get("item_5") is None
        
        # Others should still exist
        for i in range(10):
            if i != 5:
                record = cog.get(f"item_{i}")
                assert record is not None, f"item_{i} should still exist"

    def test_delete_head_of_chain(self, clean_cog):
        """Test deleting the head of a collision chain."""
        cog = clean_cog
        cog.create_table("head_test", config.COG_DEFAULT_NAMESPACE)
        
        # Insert keys
        keys = ["first", "second", "third"]
        for key in keys:
            cog.put(Record(key, f"val_{key}"))
        
        # Delete first inserted (might be head of chain)
        cog.delete("first")
        assert cog.get("first") is None
        
        # Others should exist
        assert cog.get("second") is not None
        assert cog.get("third") is not None

    def test_delete_all_items_in_chain(self, clean_cog):
        """Test deleting all items that might be in a collision chain."""
        cog = clean_cog
        cog.create_table("all_delete_test", config.COG_DEFAULT_NAMESPACE)
        
        # Insert
        for i in range(5):
            cog.put(Record(f"k{i}", f"v{i}"))
        
        # Delete all in forward order
        for i in range(5):
            cog.delete(f"k{i}")
        
        # Verify all gone
        for i in range(5):
            assert cog.get(f"k{i}") is None
        
        # Re-insert should work
        cog.put(Record("k0", "new_value"))
        assert cog.get("k0").value == "new_value"

    def test_delete_nonexistent_key(self, clean_cog):
        """Test deleting a key that doesn't exist."""
        cog = clean_cog
        cog.create_table("nonexistent_test", config.COG_DEFAULT_NAMESPACE)
        
        # Insert one key
        cog.put(Record("exists", "value"))
        
        # Delete non-existent key should not crash
        result = cog.current_table.indexer.delete("does_not_exist", cog.current_table.store)
        # Should return False for not found
        assert result is False


class TestGraphIntegration:
    """Integration tests using the Graph API to verify both fixes work together."""

    def test_complex_graph_operations(self, clean_graph):
        """Test a complex sequence of operations."""
        g = clean_graph
        
        # Build a social network
        g.put("alice", "follows", "bob")
        g.put("alice", "follows", "charlie")
        g.put("bob", "follows", "charlie")
        g.put("dave", "follows", "charlie")
        g.put("eve", "follows", "bob")
        
        # Charlie has 3 followers: alice, bob, dave
        charlie_followers = g.v("charlie").inc("follows").all()
        assert len(charlie_followers["result"]) == 3
        
        # Update alice to only follow dave
        g.put("alice", "follows", "dave", update=True)
        
        # Charlie should now have 2 followers: bob, dave
        charlie_followers_after = g.v("charlie").inc("follows").all()
        assert len(charlie_followers_after["result"]) == 2
        follower_ids = {r["id"] for r in charlie_followers_after["result"]}
        assert follower_ids == {"bob", "dave"}
        
        # Bob should still have eve as follower
        bob_followers = g.v("bob").inc("follows").all()
        assert len(bob_followers["result"]) == 1
        assert bob_followers["result"][0]["id"] == "eve"
        
        # Alice should only follow dave
        alice_follows = g.v("alice").out("follows").all()
        assert len(alice_follows["result"]) == 1
        assert alice_follows["result"][0]["id"] == "dave"

    def test_drop_and_update_combination(self, clean_graph):
        """Test combining drop (delete_edge) and update operations."""
        g = clean_graph
        
        g.put("A", "rel", "B")
        g.put("A", "rel", "C")
        g.put("X", "rel", "B")
        
        # Drop specific edge A -> B
        g.drop("A", "rel", "B")
        
        # A should still have edge to C
        a_rels = g.v("A").out("rel").all()
        assert len(a_rels["result"]) == 1
        assert a_rels["result"][0]["id"] == "C"
        
        # X should still have edge to B
        x_rels = g.v("X").out("rel").all()
        assert len(x_rels["result"]) == 1
        assert x_rels["result"][0]["id"] == "B"
        
        # Now update A to point to Z
        g.put("A", "rel", "Z", update=True)
        
        a_rels_after = g.v("A").out("rel").all()
        assert len(a_rels_after["result"]) == 1
        assert a_rels_after["result"][0]["id"] == "Z"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
