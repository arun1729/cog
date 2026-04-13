from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "TestLsUse"


class TestDefaultGraphName(unittest.TestCase):
    """Test that graph_name defaults to 'default'."""

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)

    def test_default_graph_name(self):
        """Graph() with no name should use 'default'."""
        g = Graph(cog_home=DIR_NAME)
        self.assertEqual(g.graph_name, "default")
        g.put("a", "rel", "b")
        self.assertEqual(g.v("a").out("rel").count(), 1)
        g.close()

    def test_explicit_graph_name_still_works(self):
        """Graph('myname') should still work as before."""
        g = Graph("explicit_test", cog_home=DIR_NAME)
        self.assertEqual(g.graph_name, "explicit_test")
        g.put("x", "rel", "y")
        self.assertEqual(g.v("x").out("rel").count(), 1)
        g.close()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)


class TestLs(unittest.TestCase):
    """Test ls() lists all graphs."""

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)

    def test_ls_single_graph(self):
        """ls() should list the current graph."""
        g = Graph("graph_one", cog_home=DIR_NAME)
        g.put("a", "rel", "b")
        graphs = g.ls()
        self.assertIn("graph_one", graphs)
        g.close()

    def test_ls_multiple_graphs(self):
        """ls() should list all graphs in the cog_home."""
        g1 = Graph("alpha", cog_home=DIR_NAME)
        g1.put("a", "rel", "b")

        g2 = Graph("beta", cog_home=DIR_NAME)
        g2.put("x", "rel", "y")

        g3 = Graph("gamma", cog_home=DIR_NAME)
        g3.put("m", "rel", "n")

        graphs = g1.ls()
        self.assertIn("alpha", graphs)
        self.assertIn("beta", graphs)
        self.assertIn("gamma", graphs)
        # Should be sorted
        self.assertEqual(graphs, sorted(graphs))

        g1.close()
        g2.close()
        g3.close()

    def test_ls_excludes_sys_and_views(self):
        """ls() should not include 'sys' or 'views' directories."""
        g = Graph("real_graph", cog_home=DIR_NAME)
        g.put("a", "rel", "b")
        graphs = g.ls()
        self.assertNotIn("sys", graphs)
        self.assertNotIn("views", graphs)
        self.assertIn("real_graph", graphs)
        g.close()

    def test_ls_returns_sorted(self):
        """ls() should return graph names in sorted order."""
        g = Graph("zebra", cog_home=DIR_NAME)
        g.put("a", "rel", "b")
        Graph("aardvark", cog_home=DIR_NAME).put("x", "rel", "y")

        graphs = g.ls()
        self.assertEqual(graphs, sorted(graphs))
        g.close()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)


class TestUse(unittest.TestCase):
    """Test use() switches between graphs."""

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)

    def test_use_switches_graph(self):
        """use() should switch to a different graph."""
        g = Graph("first", cog_home=DIR_NAME)
        g.put("alice", "knows", "bob")
        self.assertEqual(g.graph_name, "first")
        self.assertEqual(g.v("alice").out("knows").count(), 1)

        # Switch to a new graph
        g.use("second")
        self.assertEqual(g.graph_name, "second")
        # New graph should be empty
        self.assertEqual(g.v().count(), 0)

        # Add data to second graph
        g.put("charlie", "knows", "dave")
        self.assertEqual(g.v("charlie").out("knows").count(), 1)

        # Switch back — first graph should still have its data
        g.use("first")
        self.assertEqual(g.v("alice").out("knows").count(), 1)
        # charlie should not be in first graph
        self.assertEqual(g.v("charlie").out("knows").count(), 0)
        g.close()

    def test_use_returns_self(self):
        """use() should return self for method chaining."""
        g = Graph("chain_test", cog_home=DIR_NAME)
        g.put("a", "rel", "b")
        result = g.use("chain_test")
        self.assertIs(result, g)
        g.close()

    def test_use_chaining(self):
        """use() should support method chaining."""
        g = Graph("chained", cog_home=DIR_NAME)
        g.put("alice", "knows", "bob")

        result = g.use("chained").v("alice").out("knows").all()
        self.assertEqual(result, {"result": [{"id": "bob"}]})
        g.close()

    def test_use_creates_new_graph(self):
        """use() should create the graph if it doesn't exist."""
        g = Graph("starter", cog_home=DIR_NAME)
        g.put("a", "rel", "b")

        g.use("brand_new")
        self.assertEqual(g.graph_name, "brand_new")
        # Should be usable immediately
        g.put("x", "rel", "y")
        self.assertEqual(g.v("x").out("rel").count(), 1)

        # brand_new should now appear in ls()
        self.assertIn("brand_new", g.ls())
        g.close()

    def test_use_with_ls_workflow(self):
        """Full workflow: default graph → ls → use."""
        g = Graph(cog_home=DIR_NAME)
        self.assertEqual(g.graph_name, "default")

        # Create some named graphs via use
        g.use("social")
        g.put("alice", "follows", "bob")
        g.use("products")
        g.put("widget", "category", "tools")

        # List all graphs
        graphs = g.ls()
        self.assertIn("social", graphs)
        self.assertIn("products", graphs)

        # Switch back and verify data isolation
        g.use("social")
        self.assertEqual(g.v("alice").out("follows").count(), 1)
        self.assertEqual(g.v("widget").out("category").count(), 0)

        g.use("products")
        self.assertEqual(g.v("widget").out("category").count(), 1)
        self.assertEqual(g.v("alice").out("follows").count(), 0)
        g.close()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)


if __name__ == '__main__':
    unittest.main()
