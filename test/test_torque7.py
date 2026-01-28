"""
Tests for:
- Vertex.get_dict() and Vertex.__str__()
- Graph with custom cog_path_prefix
- Graph.load_csv with id_column_name validation
- Graph.__adjacent_vertices with direction='in'
- Graph.hasr() method (reverse has)
- Graph.getv() to retrieve views
- View.render() and View.__str__()
"""
from cog.torque import Graph, Vertex, View, BlankNode
from cog import config
import unittest
import os
import shutil
import json


DIR_NAME = "TorqueCoverageTest"


class TestTorqueCoverage(unittest.TestCase):
    """Tests to increase coverage of torque.py"""

    @classmethod
    def setUpClass(cls):
        cls.db_path = "/tmp/" + DIR_NAME
        cls._cleanup_path(cls.db_path)
        os.makedirs(cls.db_path)

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_path(cls.db_path)
        print("*** deleted torque coverage test data.")

    @staticmethod
    def _cleanup_path(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    def test_vertex_get_dict(self):
        """Test Vertex.get_dict() returns the __dict__"""
        vertex = Vertex("test_id")
        vertex.tags["tag1"] = "value1"
        vertex.set_edge("follows")
        
        d = vertex.get_dict()
        
        self.assertIsInstance(d, dict)
        self.assertEqual(d["id"], "test_id")
        self.assertIn("tag1", d["tags"])
        self.assertEqual(d["tags"]["tag1"], "value1")
        self.assertIn("follows", d["edges"])

    def test_vertex_str(self):
        """Test Vertex.__str__() - verifies set serialization raises TypeError.
        
        Known limitation: Vertex.edges is a set which isn't directly JSON serializable.
        This test documents the current behavior where __str__ raises TypeError
        when edges contains values.
        """
        vertex = Vertex("str_test")
        vertex.tags["name"] = "alice"
        vertex.set_edge("follows")  # Add an edge to make edges non-empty
        
        # Vertex.__str__ uses json.dumps which cannot serialize sets
        with self.assertRaises(TypeError):
            str(vertex)

    def test_graph_with_custom_path_prefix(self):
        """Test Graph creation with custom cog_path_prefix"""
        custom_path = "/tmp"
        g = Graph("custom_prefix_graph", cog_home=DIR_NAME, cog_path_prefix=custom_path)
        
        # Should be able to put and get data
        g.put("alice", "follows", "bob")
        result = g.v("alice").out("follows").all()
        
        self.assertEqual(result["result"][0]["id"], "bob")
        g.close()

    def test_load_csv_requires_id_column(self):
        """Test load_csv raises exception when id_column_name is None"""
        g = Graph("csv_test_graph", cog_home=DIR_NAME)
        
        csv_path = os.path.join(os.path.dirname(__file__), 'test-data', 'books.csv')
        
        with self.assertRaises(Exception) as context:
            g.load_csv(csv_path, None)
        
        self.assertIn("id_column_name must not be None", str(context.exception))
        g.close()

    def test_hasr_method(self):
        """Test hasr() - reverse has traversal"""
        g = Graph("hasr_test_graph", cog_home=DIR_NAME)
        
        g.put("alice", "follows", "bob")
        g.put("charlie", "follows", "bob")
        g.put("dani", "follows", "bob")
        
        # hasr should find vertices that have incoming edges from a specific vertex
        # Start from bob, follow "follows" predicate backwards
        result = g.v("bob").hasr("follows", "alice").all()
        
        # bob should be returned because alice follows bob
        self.assertEqual(len(result["result"]), 1)
        self.assertEqual(result["result"][0]["id"], "bob")
        
        g.close()

    def test_adjacent_vertices_in_direction(self):
        """Test __adjacent_vertices with 'in' direction via inc()"""
        g = Graph("adj_in_test_graph", cog_home=DIR_NAME)
        
        g.put("alice", "follows", "bob")
        g.put("charlie", "follows", "bob")
        
        # inc() uses direction='in' internally
        result = g.v("bob").inc("follows").all()
        
        ids = {r["id"] for r in result["result"]}
        self.assertIn("alice", ids)
        self.assertIn("charlie", ids)
        
        g.close()

    def test_view_creation_and_getv(self):
        """Test view creation and getv() to retrieve it"""
        g = Graph("view_test_graph", cog_home=DIR_NAME)
        
        g.put("alice", "follows", "bob")
        g.put("bob", "follows", "charlie")
        
        # Create a view
        view = g.v().tag("from").out("follows").tag("to").view("test_view")
        
        self.assertIsNotNone(view)
        self.assertIsInstance(view, View)
        
        # Use getv to retrieve the view
        retrieved_view = g.getv("test_view")
        
        self.assertIsNotNone(retrieved_view)
        self.assertIsInstance(retrieved_view, View)
        self.assertIn("test_view", retrieved_view.url)
        
        g.close()

    def test_view_str_method(self):
        """Test View.__str__() returns the url"""
        g = Graph("view_str_test_graph", cog_home=DIR_NAME)
        
        g.put("x", "rel", "y")
        view = g.v().tag("from").out("rel").tag("to").view("str_view")
        
        str_repr = str(view)
        
        self.assertIn("str_view", str_repr)
        self.assertTrue(str_repr.endswith(".html"))
        
        g.close()

    def test_lsv_lists_views(self):
        """Test lsv() lists all views"""
        g = Graph("lsv_test_graph", cog_home=DIR_NAME)
        
        g.put("a", "b", "c")
        g.v().tag("from").out("b").tag("to").view("view1")
        g.v().tag("from").out("b").tag("to").view("view2")
        
        views = g.lsv()
        
        self.assertIn("view1", views)
        self.assertIn("view2", views)
        
        g.close()

    def test_getv_nonexistent_raises(self):
        """Test getv() raises assertion for non-existent view"""
        g = Graph("getv_fail_test", cog_home=DIR_NAME)
        
        with self.assertRaises(AssertionError):
            g.getv("nonexistent_view")
        
        g.close()

    def test_blank_node_is_id(self):
        """Test BlankNode.is_id() class method"""
        bn = BlankNode("test123")
        
        self.assertTrue(BlankNode.is_id(str(bn)))
        self.assertFalse(BlankNode.is_id("regular_id"))
        self.assertFalse(BlankNode.is_id("_:other_format"))

    def test_blank_node_str(self):
        """Test BlankNode.__str__()"""
        bn = BlankNode("myid")
        
        str_repr = str(bn)
        
        self.assertTrue(str_repr.startswith("_:"))
        self.assertIn("myid", str_repr)

    def test_inc_with_predicates_list(self):
        """Test inc() with a list of predicates"""
        g = Graph("inc_list_test", cog_home=DIR_NAME)
        
        g.put("alice", "follows", "bob")
        g.put("charlie", "likes", "bob")
        
        result = g.v("bob").inc(["follows", "likes"]).all()
        
        ids = {r["id"] for r in result["result"]}
        self.assertIn("alice", ids)
        self.assertIn("charlie", ids)
        
        g.close()

    def test_view_persist(self):
        """Test View.persist() saves to file"""
        g = Graph("view_persist_test", cog_home=DIR_NAME)
        
        g.put("p", "q", "r")
        view = g.v().tag("from").out("q").tag("to").view("persist_view")
        
        # Check the file exists
        self.assertTrue(os.path.isfile(view.url))
        
        # Read the file content
        with open(view.url, 'r') as f:
            content = f.read()
        
        self.assertIn("vis.DataSet", content)  # vis.js library usage
        
        g.close()


if __name__ == '__main__':
    unittest.main()
