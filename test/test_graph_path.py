from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "GraphPathTest"


class GraphPathTest(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)
        os.mkdir("/tmp/" + DIR_NAME)

        cls.g = Graph(graph_name="pathtest", cog_home=DIR_NAME)
        cls.g.put("bob", "follows", "fred")
        cls.g.put("bob", "status", "cool_person")
        cls.g.put("fred", "follows", "greg")
        cls.g.put("greg", "status", "cool_person")
        cls.g.put("alice", "follows", "bob")

    def test_graph_single_hop(self):
        """Verify graph() returns correct nodes and links for a single hop."""
        result = self.g.v("bob").out("follows").graph()
        node_ids = {n['id'] for n in result['nodes']}
        self.assertEqual(node_ids, {'bob', 'fred'})

        links = result['links']
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]['source'], 'bob')
        self.assertEqual(links[0]['target'], 'fred')
        self.assertEqual(links[0]['label'], 'follows')

    def test_graph_multi_hop(self):
        """Verify graph() returns correct nodes and links through two hops."""
        result = self.g.v("bob").out("follows").out("follows").graph()
        node_ids = {n['id'] for n in result['nodes']}
        self.assertEqual(node_ids, {'bob', 'fred', 'greg'})

        links = result['links']
        self.assertEqual(len(links), 2)
        link_keys = {f"{l['source']}-{l['label']}-{l['target']}" for l in links}
        self.assertIn('bob-follows-fred', link_keys)
        self.assertIn('fred-follows-greg', link_keys)

    def test_graph_multiple_predicates(self):
        """Verify graph() with multiple predicates."""
        result = self.g.v("bob").out(["follows", "status"]).graph()
        node_ids = {n['id'] for n in result['nodes']}
        self.assertIn('bob', node_ids)
        self.assertIn('fred', node_ids)
        self.assertIn('cool_person', node_ids)

        link_labels = {l['label'] for l in result['links']}
        self.assertIn('follows', link_labels)
        self.assertIn('status', link_labels)

    def test_graph_deduplication(self):
        """Verify graph() deduplicates nodes and links from multiple paths."""
        result = self.g.v("bob").out(["follows", "status"]).graph()
        node_ids = [n['id'] for n in result['nodes']]
        # bob should appear only once despite being in two paths
        self.assertEqual(node_ids.count('bob'), 1)

    def test_graph_with_inc(self):
        """Verify graph() works with inc() (reverse traversal)."""
        result = self.g.v("fred").inc("follows").graph()
        node_ids = {n['id'] for n in result['nodes']}
        self.assertIn('fred', node_ids)
        self.assertIn('bob', node_ids)

        links = result['links']
        self.assertTrue(len(links) > 0)
        for link in links:
            self.assertEqual(link['label'], 'follows')

    def test_graph_empty_result(self):
        """Verify graph() returns empty nodes/links for no matches."""
        result = self.g.v("bob").out("nonexistent").graph()
        self.assertEqual(result, {'nodes': [], 'links': []})

    def test_all_does_not_include_path(self):
        """Verify all() does NOT include path data (path is only in graph())."""
        result = self.g.v("bob").out("follows").all()
        for r in result['result']:
            self.assertNotIn('path', r)

    def test_view_uses_graph_format(self):
        """Verify view() generates HTML with graphData containing nodes and links."""
        view = self.g.v("bob").out("follows").view("test_path_view")
        self.assertTrue(view.url.endswith("test_path_view.html"))

    def test_graph_single_value_out(self):
        """Cover __hop 's' branch (out direction) by storing a single-value record."""
        g = Graph(graph_name="sval_out_test", cog_home=DIR_NAME)
        from cog.database import hash_predicate, out_nodes, in_nodes
        from cog.core import Record
        # Store a single-value 's' type record directly (bypassing put_set)
        predicate = "knows"
        pred_hash = hash_predicate(predicate)
        g.cog.use_namespace("sval_out_test")
        g.cog.use_table(g.config.GRAPH_EDGE_SET_TABLE_NAME).put(Record(pred_hash, predicate))
        g.cog.use_table(g.config.GRAPH_NODE_SET_TABLE_NAME).put(Record("x", ""))
        g.cog.use_table(g.config.GRAPH_NODE_SET_TABLE_NAME).put(Record("y", ""))
        g.cog.use_table(pred_hash).put(Record(out_nodes("x"), "y"))
        g.cog.use_table(pred_hash).put(Record(in_nodes("y"), "x"))
        g.all_predicates = g.cog.list_tables()
        g._predicate_reverse_lookup_cache[pred_hash] = predicate

        result = g.v("x").out("knows").graph()
        node_ids = {n['id'] for n in result['nodes']}
        self.assertEqual(node_ids, {'x', 'y'})
        self.assertEqual(len(result['links']), 1)
        self.assertEqual(result['links'][0]['label'], 'knows')
        g.close()

    def test_graph_single_value_inc(self):
        """Cover __hop 's' branch (in direction) by storing a single-value record."""
        g = Graph(graph_name="sval_inc_test", cog_home=DIR_NAME)
        from cog.database import hash_predicate, out_nodes, in_nodes
        from cog.core import Record
        predicate = "knows"
        pred_hash = hash_predicate(predicate)
        g.cog.use_namespace("sval_inc_test")
        g.cog.use_table(g.config.GRAPH_EDGE_SET_TABLE_NAME).put(Record(pred_hash, predicate))
        g.cog.use_table(g.config.GRAPH_NODE_SET_TABLE_NAME).put(Record("x", ""))
        g.cog.use_table(g.config.GRAPH_NODE_SET_TABLE_NAME).put(Record("y", ""))
        g.cog.use_table(pred_hash).put(Record(out_nodes("x"), "y"))
        g.cog.use_table(pred_hash).put(Record(in_nodes("y"), "x"))
        g.all_predicates = g.cog.list_tables()
        g._predicate_reverse_lookup_cache[pred_hash] = predicate

        result = g.v("y").inc("knows").graph()
        node_ids = {n['id'] for n in result['nodes']}
        self.assertEqual(node_ids, {'x', 'y'})
        self.assertEqual(len(result['links']), 1)
        self.assertEqual(result['links'][0]['label'], 'knows')
        g.close()

    def test_graph_bfs_until_path(self):
        """Cover BFS until branch with path propagation."""
        # alice -> bob -> fred -> greg; stop when hitting fred
        result = self.g.v("alice").bfs("follows", until=lambda vid: vid == "fred").graph()
        node_ids = {n['id'] for n in result['nodes']}
        self.assertIn('fred', node_ids)
        self.assertIn('alice', node_ids)
        # fred should be the terminal vertex
        links = result['links']
        self.assertTrue(len(links) > 0)
        link_labels = {l['label'] for l in links}
        self.assertIn('follows', link_labels)

    @classmethod
    def tearDownClass(cls):
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)


if __name__ == '__main__':
    unittest.main()
