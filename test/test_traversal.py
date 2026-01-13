from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "TraversalTest"


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


class TraversalTest(unittest.TestCase):
    """Tests for BFS and DFS graph traversal methods."""

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)

        # Create a test graph:
        #   alice -> bob -> charlie -> david
        #          \-> eve -> frank
        #   greg -> bob (incoming to bob)
        cls.g = Graph(graph_name="traversal", cog_home=DIR_NAME)
        cls.g.put("alice", "follows", "bob")
        cls.g.put("bob", "follows", "charlie")
        cls.g.put("charlie", "follows", "david")
        cls.g.put("alice", "follows", "eve")
        cls.g.put("eve", "follows", "frank")
        cls.g.put("greg", "follows", "bob")
        cls.g.put("bob", "status", "active")
        print(">>> traversal test setup complete.\n")

    # ==================== BFS Tests ====================

    def test_bfs_basic(self):
        """BFS should find all vertices reachable within max_depth."""
        result = self.g.v("alice").bfs(predicates="follows", max_depth=2).all()
        ids = [r['id'] for r in result['result']]
        # Depth 1: bob, eve. Depth 2: charlie, frank
        self.assertEqual(sorted(ids), sorted(['bob', 'eve', 'charlie', 'frank']))

    def test_bfs_max_depth_1(self):
        """BFS with max_depth=1 should only return immediate neighbors."""
        result = self.g.v("alice").bfs(predicates="follows", max_depth=1).all()
        ids = [r['id'] for r in result['result']]
        self.assertEqual(sorted(ids), sorted(['bob', 'eve']))

    def test_bfs_min_depth(self):
        """BFS with min_depth should filter out shallow vertices."""
        result = self.g.v("alice").bfs(predicates="follows", max_depth=2, min_depth=2).all()
        ids = [r['id'] for r in result['result']]
        # Only depth 2: charlie, frank
        self.assertEqual(sorted(ids), sorted(['charlie', 'frank']))

    def test_bfs_until(self):
        """BFS with until condition should stop when condition is met."""
        result = self.g.v("alice").bfs(predicates="follows", until=lambda v: v == "charlie").all()
        ids = [r['id'] for r in result['result']]
        # Should include bob, eve (explored before charlie) and charlie (where it stopped)
        self.assertIn('charlie', ids)

    def test_bfs_direction_inc(self):
        """BFS with direction='inc' should traverse incoming edges."""
        result = self.g.v("bob").bfs(predicates="follows", direction="inc", max_depth=1).all()
        ids = [r['id'] for r in result['result']]
        # alice and greg both follow bob
        self.assertEqual(sorted(ids), sorted(['alice', 'greg']))

    def test_bfs_direction_both(self):
        """BFS with direction='both' should traverse both directions."""
        result = self.g.v("bob").bfs(predicates="follows", direction="both", max_depth=1).all()
        ids = [r['id'] for r in result['result']]
        # Outgoing: charlie. Incoming: alice, greg
        self.assertEqual(sorted(ids), sorted(['charlie', 'alice', 'greg']))

    def test_bfs_all_predicates(self):
        """BFS with predicates=None should follow all edge types."""
        result = self.g.v("bob").bfs(max_depth=1).all()
        ids = [r['id'] for r in result['result']]
        # follows->charlie, status->active
        self.assertEqual(sorted(ids), sorted(['charlie', 'active']))

    def test_bfs_unique_prevents_cycles(self):
        """BFS with unique=True should not revisit vertices."""
        # Create a cycle for this test in its own directory
        cycle_dir = "CycleTest"
        if os.path.exists("/tmp/" + cycle_dir):
            shutil.rmtree("/tmp/" + cycle_dir)
        g2 = Graph(graph_name="cycle_test", cog_home=cycle_dir)
        g2.put("a", "edge", "b")
        g2.put("b", "edge", "c")
        g2.put("c", "edge", "a")  # cycle back to a
        
        result = g2.v("a").bfs(max_depth=10).all()
        ids = [r['id'] for r in result['result']]
        # Should visit each node only once
        self.assertEqual(sorted(ids), sorted(['b', 'c']))
        g2.close()
        shutil.rmtree("/tmp/" + cycle_dir, ignore_errors=True)

    # ==================== DFS Tests ====================

    def test_dfs_basic(self):
        """DFS should find all vertices reachable within max_depth."""
        result = self.g.v("alice").dfs(predicates="follows", max_depth=2).all()
        ids = [r['id'] for r in result['result']]
        # Same vertices as BFS, different order
        self.assertEqual(sorted(ids), sorted(['bob', 'eve', 'charlie', 'frank']))

    def test_dfs_max_depth_1(self):
        """DFS with max_depth=1 should only return immediate neighbors."""
        result = self.g.v("alice").dfs(predicates="follows", max_depth=1).all()
        ids = [r['id'] for r in result['result']]
        self.assertEqual(sorted(ids), sorted(['bob', 'eve']))

    def test_dfs_min_depth(self):
        """DFS with min_depth should filter out shallow vertices."""
        result = self.g.v("alice").dfs(predicates="follows", max_depth=2, min_depth=2).all()
        ids = [r['id'] for r in result['result']]
        self.assertEqual(sorted(ids), sorted(['charlie', 'frank']))

    def test_dfs_direction_both(self):
        """DFS with direction='both' should traverse both directions."""
        result = self.g.v("bob").dfs(predicates="follows", direction="both", max_depth=1).all()
        ids = [r['id'] for r in result['result']]
        self.assertEqual(sorted(ids), sorted(['charlie', 'alice', 'greg']))

    # ==================== Edge Cases ====================

    def test_bfs_empty_result(self):
        """BFS from vertex with no outgoing edges should return empty."""
        result = self.g.v("david").bfs(predicates="follows", max_depth=1).all()
        self.assertEqual(result['result'], [])

    def test_bfs_nonexistent_vertex(self):
        """BFS from nonexistent vertex should return empty."""
        result = self.g.v("nonexistent").bfs(max_depth=1).all()
        self.assertEqual(result['result'], [])

    def test_dfs_chain_with_filter(self):
        """BFS/DFS should chain with existing filter method."""
        result = self.g.v("alice").bfs(predicates="follows", max_depth=2).filter(lambda v: v.startswith('c')).all()
        ids = [r['id'] for r in result['result']]
        self.assertEqual(ids, ['charlie'])

    def test_bfs_preserves_tags(self):
        """BFS should preserve tags from starting vertex."""
        result = self.g.v("alice").tag("start").bfs(predicates="follows", max_depth=1).all()
        for r in result['result']:
            self.assertEqual(r.get('start'), 'alice')

    @classmethod
    def tearDownClass(cls):
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME, ignore_errors=True)
        print("*** deleted traversal test data.")


if __name__ == '__main__':
    unittest.main()
