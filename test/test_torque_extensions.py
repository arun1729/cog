from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "TorqueExtensionsTest"


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


class TorqueExtensionsTest(unittest.TestCase):
    """
    Tests for new Torque traversal methods:
    - both(): bidirectional traversal
    - is_(): filter to specific nodes
    - unique(): remove duplicates
    - limit(): limit results
    - skip(): skip results
    - back(): return to tagged position
    """
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)
        os.mkdir("/tmp/" + DIR_NAME)

        cls.g = Graph(graph_name="test_graph", cog_home=DIR_NAME)
        # Create a simple test graph:
        # alice -> bob -> charlie -> alice (cycle)
        # alice -> dani
        # bob has status "cool"
        cls.g.put("alice", "follows", "bob")
        cls.g.put("bob", "follows", "charlie")
        cls.g.put("charlie", "follows", "alice")
        cls.g.put("alice", "follows", "dani")
        cls.g.put("bob", "status", "cool")
        cls.g.put("dani", "status", "cool")
        print(">>> TorqueExtensionsTest setup complete.\n")

    # =========== both() tests ===========

    def test_both_follows_from_bob(self):
        """both() should return vertices connected by edges in either direction."""
        result = self.g.v("bob").both("follows").all()
        ids = {r['id'] for r in result['result']}
        # bob follows charlie, alice follows bob
        self.assertIn("charlie", ids)
        self.assertIn("alice", ids)

    def test_both_no_predicate(self):
        """both() with no predicate should follow all edge types."""
        result = self.g.v("bob").both().all()
        ids = {r['id'] for r in result['result']}
        # bob -> charlie (follows), alice -> bob (follows), bob -> cool (status)
        self.assertIn("charlie", ids)
        self.assertIn("alice", ids)
        self.assertIn("cool", ids)

    # =========== is_() tests ===========

    def test_is_single_node(self):
        """is_() should filter to only the specified node."""
        result = self.g.v("alice").out("follows").is_("bob").all()
        self.assertEqual(len(result['result']), 1)
        self.assertEqual(result['result'][0]['id'], "bob")

    def test_is_multiple_nodes(self):
        """is_() should accept multiple nodes."""
        result = self.g.v("alice").out("follows").is_("bob", "dani").all()
        ids = {r['id'] for r in result['result']}
        self.assertEqual(ids, {"bob", "dani"})

    def test_is_no_match(self):
        """is_() should return empty if no nodes match."""
        result = self.g.v("alice").out("follows").is_("nonexistent").all()
        self.assertEqual(result['result'], [])

    def test_is_with_list(self):
        """is_() should accept a list of nodes."""
        result = self.g.v("alice").out("follows").is_(["bob", "dani"]).all()
        ids = {r['id'] for r in result['result']}
        self.assertEqual(ids, {"bob", "dani"})

    # =========== unique() tests ===========

    def test_unique_removes_duplicates(self):
        """unique() should remove duplicate vertices."""
        # Get all followers' statuses - "cool" appears twice (bob and dani)
        result_without_unique = self.g.v("alice").out("follows").out("status").all()
        result_with_unique = self.g.v("alice").out("follows").out("status").unique().all()

        # Without unique, we should have duplicates
        ids_without = [r['id'] for r in result_without_unique['result']]
        self.assertEqual(ids_without.count("cool"), 2)

        # With unique, no duplicates
        ids_with = [r['id'] for r in result_with_unique['result']]
        self.assertEqual(ids_with.count("cool"), 1)

    def test_unique_preserves_order(self):
        """unique() should preserve the order of first occurrence."""
        result = self.g.v().unique().all()
        # Should have vertices in order of first appearance
        self.assertTrue(len(result['result']) > 0)

    # =========== limit() tests ===========

    def test_limit_returns_n_results(self):
        """limit() should return at most N vertices."""
        result = self.g.v().limit(2).all()
        self.assertEqual(len(result['result']), 2)

    def test_limit_more_than_available(self):
        """limit() with N larger than result set should return all."""
        all_result = self.g.v().all()
        limited_result = self.g.v().limit(1000).all()
        self.assertEqual(len(all_result['result']), len(limited_result['result']))

    def test_limit_zero(self):
        """limit(0) should return empty."""
        result = self.g.v().limit(0).all()
        self.assertEqual(result['result'], [])

    # =========== skip() tests ===========

    def test_skip_skips_n_results(self):
        """skip() should skip the first N vertices."""
        all_result = self.g.v().all()
        skipped_result = self.g.v().skip(2).all()
        self.assertEqual(len(skipped_result['result']), len(all_result['result']) - 2)

    def test_skip_more_than_available(self):
        """skip() with N larger than result set should return empty."""
        result = self.g.v().skip(1000).all()
        self.assertEqual(result['result'], [])

    def test_limit_and_skip_pagination(self):
        """limit() and skip() together enable pagination."""
        all_result = self.g.v().all()
        page1 = self.g.v().limit(2).all()
        page2 = self.g.v().skip(2).limit(2).all()

        # Pages should not overlap
        page1_ids = {r['id'] for r in page1['result']}
        page2_ids = {r['id'] for r in page2['result']}
        self.assertEqual(len(page1_ids & page2_ids), 0)

    # =========== back() tests ===========

    def test_back_returns_to_tagged_vertex(self):
        """back() should return to the previously tagged vertex."""
        result = self.g.v("alice").tag("start").out("follows").back("start").all()
        # Should return to alice
        ids = {r['id'] for r in result['result']}
        self.assertEqual(ids, {"alice"})

    def test_back_preserves_tags(self):
        """back() should preserve existing tags."""
        result = self.g.v("alice").tag("origin").out("follows").tag("middle").back("origin").all()
        for r in result['result']:
            self.assertIn("origin", r)
            self.assertIn("middle", r)

    def test_back_with_invalid_tag(self):
        """back() with non-existent tag should return empty."""
        result = self.g.v("alice").out("follows").back("nonexistent").all()
        self.assertEqual(result['result'], [])

    def test_back_after_filter(self):
        """back() should work with filtered results."""
        result = self.g.v("alice").tag("start").out("follows").has("status", "cool").back("start").all()
        # Only bob has status cool, so we should get back to alice (who follows bob)
        ids = {r['id'] for r in result['result']}
        self.assertEqual(ids, {"alice"})

    @classmethod
    def tearDownClass(cls):
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)
        print("*** TorqueExtensionsTest cleanup complete.")


if __name__ == '__main__':
    unittest.main()
