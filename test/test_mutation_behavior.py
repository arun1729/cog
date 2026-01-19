"""
Test cases for Graph mutation behavior.

These tests document and verify how Graph handles state across multiple queries.
The current behavior is MUTABLE - each traversal method modifies the Graph's
internal state (last_visited_vertices).

"""

import unittest
import os
import shutil
from cog.torque import Graph

DIR_NAME = "cog_test_mutation"


class TestGraphMutationBehavior(unittest.TestCase):
    """Test the mutable nature of Graph traversals."""
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)
        os.makedirs("/tmp/" + DIR_NAME, exist_ok=True)
        
        cls.g = Graph("mutation_test", cog_home=DIR_NAME)
        # Create a simple test graph
        cls.g.put("alice", "knows", "bob")
        cls.g.put("alice", "likes", "pizza")
        cls.g.put("bob", "knows", "charlie")
        cls.g.put("bob", "likes", "tacos")
        cls.g.put("charlie", "knows", "diana")
    
    @classmethod
    def tearDownClass(cls):
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)
    
    # === SAFE PATTERNS (one-shot queries) ===
    
    def test_one_shot_query_is_safe(self):
        """Single-expression queries work correctly."""
        result = self.g.v("alice").out("knows").all()
        self.assertEqual(result['result'][0]['id'], 'bob')
    
    def test_sequential_queries_are_safe(self):
        """Sequential one-shot queries work correctly because v() resets state."""
        # First query
        r1 = self.g.v("alice").out("knows").all()
        self.assertEqual(r1['result'][0]['id'], 'bob')
        
        # Second query - v() resets state
        r2 = self.g.v("bob").out("knows").all()
        self.assertEqual(r2['result'][0]['id'], 'charlie')
        
        # Third query - still works
        r3 = self.g.v("alice").out("likes").all()
        self.assertEqual(r3['result'][0]['id'], 'pizza')
    
    def test_v_resets_state(self):
        """Each call to v() creates a fresh starting point."""
        # Build up some state
        self.g.v("alice").out("knows").out("knows")
        
        # v() should reset completely
        result = self.g.v("charlie").out("knows").all()
        self.assertEqual(result['result'][0]['id'], 'diana')
    
    # === MUTATION BEHAVIOR (documenting current behavior) ===
    
    def test_graph_is_mutated_after_traversal(self):
        """After a traversal, Graph's internal state is mutated."""
        # Start traversal
        self.g.v("alice")
        
        # Graph now has state - continuing without v() uses existing state
        result = self.g.out("knows").all()
        self.assertEqual(result['result'][0]['id'], 'bob')
    
    def test_multi_line_traversal_works(self):
        """Multi-line traversal using same graph object works."""
        self.g.v("alice")
        self.g.out("knows")
        self.g.out("knows")
        result = self.g.all()
        self.assertEqual(result['result'][0]['id'], 'charlie')
    
    def test_intermediate_storage_is_mutable(self):
        """Storing intermediate result and continuing mutates the original."""
        base = self.g.v("alice")
        # base and self.g point to the same object
        
        # This mutates both base and self.g
        base.out("knows")
        
        # Continuing from "base" continues from mutated state (bob, not alice)
        result = base.out("knows").all()
        self.assertEqual(result['result'][0]['id'], 'charlie')
    
    def test_traversal_continues_from_current_position(self):
        """
        Traversals continue from the current position in the graph.
        To start from a different vertex, call v() again.
        """
        # Start from alice
        self.g.v("alice")
        
        # First traversal - moves to bob
        r1 = self.g.out("knows").all()
        self.assertEqual(r1['result'][0]['id'], 'bob')
        
        # Continuing traverses from current position (bob)
        r2 = self.g.out("likes").all()
        # Returns bob's likes since we're at bob now
        self.assertEqual(r2['result'][0]['id'], 'tacos')
    
    def test_correct_branching_pattern(self):
        """The correct way to branch is to restart with v()."""
        # Branch 1
        r1 = self.g.v("alice").out("knows").all()
        self.assertEqual(r1['result'][0]['id'], 'bob')
        
        # Branch 2 - restart with v()
        r2 = self.g.v("alice").out("likes").all()
        self.assertEqual(r2['result'][0]['id'], 'pizza')  # Correct!
    
    # === CHAINED v() BEHAVIOR ===
    
    def test_v_in_middle_of_chain_resets(self):
        """Calling v() mid-chain resets the traversal."""
        result = self.g.v("alice").out("knows").v("charlie").out("knows").all()
        # v("charlie") resets, so we get charlie's knows
        self.assertEqual(result['result'][0]['id'], 'diana')
    
    # === RETURN VALUES ===
    
    def test_traversal_methods_return_graph(self):
        """All traversal methods return the graph for chaining."""
        result = self.g.v("alice")
        self.assertIs(result, self.g)
        
        result = self.g.out("knows")
        self.assertIs(result, self.g)
    
    def test_terminal_methods_return_data(self):
        """Terminal methods return data, not the graph."""
        self.g.v("alice").out("knows")
        
        result = self.g.all()
        self.assertIsInstance(result, dict)
        self.assertIn('result', result)
        
        self.g.v("alice").out("knows")
        count = self.g.count()
        self.assertIsInstance(count, int)


class TestRemoteGraphMutationBehavior(unittest.TestCase):
    """Test that RemoteGraph has same mutation behavior as local Graph."""
    
    @classmethod
    def setUpClass(cls):
        import time
        if os.path.exists("/tmp/" + DIR_NAME + "_remote"):
            shutil.rmtree("/tmp/" + DIR_NAME + "_remote")
        os.makedirs("/tmp/" + DIR_NAME + "_remote", exist_ok=True)
        
        cls.g = Graph("mutation_remote", cog_home=DIR_NAME + "_remote")
        cls.g.put("alice", "knows", "bob")
        cls.g.put("alice", "likes", "pizza")
        cls.g.put("bob", "knows", "charlie")
        
        cls.port = 18090
        cls.g.serve(port=cls.port, writable=True)
        time.sleep(0.2)
        
        cls.remote = Graph.connect(f"http://localhost:{cls.port}/mutation_remote")
    
    @classmethod
    def tearDownClass(cls):
        cls.g.stop()
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME + "_remote")
    
    def test_remote_sequential_queries_work(self):
        """RemoteGraph can be reused for sequential queries."""
        r1 = self.remote.v("alice").out("knows").all()
        self.assertEqual(r1['result'][0]['id'], 'bob')
        
        r2 = self.remote.v("bob").out("knows").all()
        self.assertEqual(r2['result'][0]['id'], 'charlie')
    
    def test_remote_query_clears_after_execution(self):
        """RemoteGraph clears query parts after execution."""
        # First query
        self.remote.v("alice").out("knows").all()
        
        # Second query starts fresh
        r = self.remote.v("alice").out("likes").all()
        self.assertEqual(r['result'][0]['id'], 'pizza')


if __name__ == '__main__':
    unittest.main()
