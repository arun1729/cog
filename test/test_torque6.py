from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "TorqueTest6"


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


class TorqueTest(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)

        if not os.path.exists("/tmp/" + DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)

    def test_delete_edge_1(self):
        """Test delete() removes a single edge."""
        g = Graph(graph_name="test_delete", cog_home=DIR_NAME)
        expected = {'result': [{'id': 'greg'}]}
        g.put("bob", "friends", "greg")
        actual = g.v("bob").out("friends").all()
        self.assertTrue(expected == actual)

        g.delete("bob", "friends", "greg")
        actual = g.v("bob").out("friends").all()
        self.assertTrue({'result': []} == actual)
        g.close()

    def test_delete_edge_2(self):
        """Test delete() removes one edge while keeping others."""
        g = Graph(graph_name="test2", cog_home=DIR_NAME)
        g.put("bob", "friends", "greg")
        g.put("bob", "friends", "alice")

        expected = {'result': [{'id': 'alice'}, {'id': 'greg'}]}
        actual = g.v("bob").out("friends").all()
        self.assertTrue(expected == actual)

        expected = {'result': [{'id': 'alice'}]}
        g.delete("bob", "friends", "greg")
        actual = g.v("bob").out("friends").all()
        self.assertTrue(expected == actual)

        actual = g.v("greg").inc("friends").all()
        self.assertTrue({'result': []} == actual)
        g.close()

    def test_delete_edge_3(self):
        """Test delete() only removes specified predicate edge."""
        g = Graph(graph_name="test3", cog_home=DIR_NAME)
        g.put("bob", "friends", "greg")
        g.put("bob", "friends", "alice")
        g.put("bob", "neighbour", "alice")

        g.delete("bob", "friends", "alice")

        expected = {'result': [{'id': 'alice'}]}
        actual = g.v("bob").out("neighbour").all()
        self.assertTrue(expected == actual)

        expected = {'result': [{'id': 'bob', 'edges': ['neighbour']}]}
        actual = g.v("alice").inc().all('e')
        self.assertTrue(ordered(expected) == ordered(actual))
        g.close()

    def test_drop_with_args_raises_deprecation(self):
        """Test drop(s, p, o) raises DeprecationWarning."""
        g = Graph(graph_name="test_deprecation", cog_home=DIR_NAME)
        g.put("a", "b", "c")
        
        with self.assertRaises(DeprecationWarning) as context:
            g.drop("a", "b", "c")
        
        self.assertIn("deprecated", str(context.exception).lower())
        self.assertIn("delete", str(context.exception).lower())
        g.close()

    def test_truncate(self):
        """Test truncate() clears all data but keeps graph usable."""
        g = Graph(graph_name="test_truncate", cog_home=DIR_NAME)
        g.put("alice", "knows", "bob")
        g.put("bob", "knows", "charlie")
        g.put("charlie", "knows", "alice")
        
        # Verify data exists
        self.assertEqual(g.v("alice").out("knows").count(), 1)
        
        # Truncate
        g.truncate()
        
        # Verify empty
        self.assertEqual(g.v().count(), 0)
        
        # Verify still usable
        g.put("new", "data", "here")
        self.assertEqual(g.v("new").out("data").count(), 1)
        g.close()

    def test_filter_string(self):
        g = Graph(graph_name="test4", cog_home=DIR_NAME)
        g.put("bob", "friends", "greg")
        g.put("bob", "friends", "alice")
        g.put("bob", "neighbour", "alice")

        expected = {'result': [{'id': 'alice'}]}
        actual = g.v("bob").out("friends").filter(func=lambda x: x == 'alice').all()
        self.assertTrue(expected == actual)
        g.close()

    def test_filter_int(self):
        g = Graph(graph_name="test5", cog_home=DIR_NAME)
        g.put("bob", "friends", "greg")
        g.put("bob", "friends", "alice")
        g.put("bob", "score", "10")
        g.put("alice", "score", "20")
        g.put("greg", "score", "30")

        expected = {'result': [{'id': 'alice'}, {'id': 'greg'}]}
        actual = g.v().out("score").filter(func=lambda x: int(x) > 10).inc().all()
        self.assertTrue(expected == actual)
        g.close()

    def test_filter_multiple_non_matches(self):
        g = Graph(graph_name="test6", cog_home=DIR_NAME)
        traversal = g.v(["greg", "tom", "alice"])
        returned = traversal.filter(func=lambda x: x == 'alice')
        self.assertIs(returned, traversal)
        expected = {'result': [{'id': 'alice'}]}
        self.assertTrue(expected == returned.all())
        g.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/" + DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
