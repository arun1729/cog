from cog.torque import Graph, ASC, DESC
import unittest
import os
import shutil

DIR_NAME = "TorqueTest"


# target api set
# in("predicate"), out("predicate"), all, count, tag
# next release .forEach(function), this can do filter, adding values etc.

# // The working set of this is bob and dani
# g.V("<charlie>").Out("<follows>").All()
# // The working set of this is fred, as alice follows bob and bob follows fred.
# g.V("<alice>").Out("<follows>").Out("<follows>").All()
# // Finds all things dani points at. Result is bob, greg and cool_person
# g.V("<dani>").Out().All()
# // Finds all things dani points at on the status linkage.
# // Result is bob, greg and cool_person
# g.V("<dani>").Out(["<follows>", "<status>"]).All()
# // Finds all things dani points at on the status linkage, given from a separate query path.
# // Result is {"id": "cool_person", "pred": "<status>"}
# g.V("<dani>").Out(g.V("<status>"), "pred").All()

# https://docs.janusgraph.org/latest/gremlin.html
# https://tinkerpop.apache.org/gremlin.html


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

        data_dir = "test/test-data/test.nq"
        # choose appropriate path based on where the test is called from.
        if os.path.exists("test-data/test.nq"):
            data_dir = "test-data/test.nq"

        TorqueTest.g = Graph(graph_name="people", cog_home=DIR_NAME)
        TorqueTest.g.load_triples(data_dir, "people")
        # print TorqueTest.g.v().all()
        print(">>> test setup complete.\n")

    def test_torque_1(self):
        self.assertEqual(1, TorqueTest.g.v("<alice>").out().count())

    def test_torque_2(self):
        expected = {'result': [{'source': 'cool_person', 'id': 'cool_person'}, {'source': '<fred>', 'id': '<fred>'}]}
        actual = TorqueTest.g.v("<bob>").out().tag("source").all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_3(self):
        expected = {"result": [{"source": "<fred>", "id": "<greg>", "target": "<greg>"}]}
        actual = TorqueTest.g.v("<bob>").out().tag("source").out().tag("target").all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_4(self):
        expected = {'result': [{'source': 'cool_person', 'id': '<bob>', 'target': '<bob>'},
                               {'source': 'cool_person', 'id': '<dani>', 'target': '<dani>'},
                               {'source': 'cool_person', 'id': '<greg>', 'target': '<greg>'},
                               {'source': '<fred>', 'id': '<bob>', 'target': '<bob>'},
                               {'source': '<fred>', 'id': '<emily>', 'target': '<emily>'}]}
        actual = TorqueTest.g.v("<bob>").out().tag("source").inc().tag("target").all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_5(self):
        expected = {'result': [{'source': '<greg>', 'id': '<dani>', 'target': '<dani>'},
                               {'source': '<greg>', 'id': '<fred>', 'target': '<fred>'}]}
        actual = TorqueTest.g.v("<fred>").out().tag("source").inc().tag("target").all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_6(self):
        expected = {'result': [{'source': '<greg>', 'id': '<dani>', 'target': '<dani>'},
                               {'source': '<greg>', 'id': '<fred>', 'target': '<fred>'}]}
        actual = TorqueTest.g.v("<fred>").out().tag("source").inc().tag("target").all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_7(self):
        expected = {'result': [{'source': '<greg>', 'id': 'cool_person', 'target': 'cool_person'}]}
        actual = TorqueTest.g.v("<fred>").out().tag("source").out().tag("target").all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_8(self):
        expected = {'result': [{'source': '<greg>', 'id': 'cool_person', 'target': 'cool_person'},
                               {'source': '<greg>', 'id': '<bob>', 'target': '<bob>'},
                               {'source': '<greg>', 'id': '<greg>', 'target': '<greg>'},
                               {'source': '<greg>', 'id': '<greg>', 'target': '<greg>'}]}
        actual = TorqueTest.g.v("<fred>").out().tag("source").inc().out().tag("target").all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_9(self):
        expected = {'result': [{'source': '<fred>', 'id': '<fred>'}]}
        actual = TorqueTest.g.v("<bob>").out(["<follows>"]).tag("source").all()
        self.assertTrue(ordered(expected) == ordered(actual))

    def test_torque_10(self):
        expected = {'result': [{'source': '<fred>', 'id': '<fred>'}, {'source': 'cool_person', 'id': 'cool_person'}]}
        actual = TorqueTest.g.v("<bob>").out(["<follows>", "<status>"]).tag("source").all()
        self.assertEqual(ordered(expected), ordered(actual))

    # bad predicates, should not break test
    def test_torque_11(self):
        expected = {'result': []}
        actual = TorqueTest.g.v("<bob>").out(["<follows>zzz", "<status>zzz"]).tag("source").all()
        self.assertTrue(ordered(expected) == ordered(actual))

    def test_torque_12(self):
        actual = TorqueTest.g.scan(3, 'v')
        print(actual)
        self.assertTrue(3 == len(actual['result']))
        with self.assertRaises(AssertionError):
            TorqueTest.g.scan('v', 3)

    def test_torque_13(self):
        expected = {'result': [{'id': '<greg>'}, {'id': '<dani>'}, {'id': '<bob>'}]}
        actual = TorqueTest.g.v().inc(["<status>"]).all()
        self.assertTrue(expected == actual)

    def test_torque_14(self):
        expected = {
            'result': [{'id': '<fred>'}, {'id': '<dani>'}, {'id': '<charlie>'}, {'id': '<dani>'}, {'id': '<charlie>'},
                       {'id': '<alice>'}]}
        actual = TorqueTest.g.v().inc(["<status>"]).inc("<follows>").all()
        self.assertTrue(expected == actual)

    def test_torque_15(self):
        view = TorqueTest.g.v("<bob>").out().tag("from").inc().tag("to").view("bob_view")
        print(view.url)
        self.assertTrue(view.url.endswith("bob_view.html"))
        self.assertEqual(['bob_view'], TorqueTest.g.lsv())
        view = TorqueTest.g.v("<dani>").tag("from").out().tag("to").view("dani_view")
        self.assertEqual(['bob_view', 'dani_view'], sorted(TorqueTest.g.lsv()))

    def test_torque_16(self):
        expected = {'result': [{'id': '<bob>'}]}
        actual = TorqueTest.g.v("<charlie>").out("<follows>").has("<follows>", "<fred>").all()
        self.assertTrue(expected == actual)

    def test_torque_17(self):
        expected = {'result': [{'id': '<dani>'}, {'id': '<alice>'}, {'id': '<charlie>'}]}
        actual = TorqueTest.g.v().has("<follows>", "<bob>").all()
        self.assertTrue(expected == actual)

    def test_torque_18(self):
        expected = {'result': [{'id': '<bob>'}, {'id': '<dani>'}, {'id': '<greg>'}]}
        actual = TorqueTest.g.v().has("<status>", 'cool_person').all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_19(self):
        expected = {'result': [{'id': '<fred>', 'edges': ['<follows>']}, {'id': 'cool_person', 'edges': ['<status>']}]}
        actual = TorqueTest.g.v("<bob>").out().all('e')
        self.assertEqual(ordered(expected), ordered(actual))

    def test_torque_20(self):
        expected = {'result': [{'id': '<bob>'}, {'id': '<emily>'}]}
        actual = TorqueTest.g.v().has("<follows>", "<fred>").all('e')
        self.assertTrue(expected == actual)

    def test_torque_21(self):
        expected = {'result': [{'id': '<dani>', 'edges': ['<follows>']}, {'id': '<charlie>', 'edges': ['<follows>']},
                               {'id': '<alice>', 'edges': ['<follows>']}]}
        actual = TorqueTest.g.v().has("<follows>", "<fred>").inc().all('e')
        self.assertTrue(expected == actual)

    def test_torque_22(self):
        expected = {'result': [{'id': '<bob>'}]}
        actual = TorqueTest.g.v().hasr("<follows>", "<alice>").all()
        self.assertEqual(expected, actual)

    # ============================================================
    # Multi-tag tests
    # ============================================================

    def test_multi_tag_with_list(self):
        """Test that tag() accepts a list of tag names and tags all of them."""
        expected = {'result': [{'source': 'cool_person', 'target': 'cool_person', 'id': 'cool_person'},
                               {'source': '<fred>', 'target': '<fred>', 'id': '<fred>'}]}
        actual = TorqueTest.g.v("<bob>").out().tag(["source", "target"]).all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_multi_tag_single_string_still_works(self):
        """Test that tag() still works with a single string (backward compatibility)."""
        expected = {'result': [{'source': 'cool_person', 'id': 'cool_person'},
                               {'source': '<fred>', 'id': '<fred>'}]}
        actual = TorqueTest.g.v("<bob>").out().tag("source").all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_multi_tag_empty_list(self):
        """Test that tag() with empty list does not add any tags."""
        expected = {'result': [{'id': 'cool_person'}, {'id': '<fred>'}]}
        actual = TorqueTest.g.v("<bob>").out().tag([]).all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_multi_tag_chained(self):
        """Test chaining: single tag followed by multi-tag."""
        expected = {'result': [{'first': '<fred>', 'second': '<greg>', 'third': '<greg>', 'id': '<greg>'}]}
        actual = TorqueTest.g.v("<bob>").out("<follows>").tag("first").out().tag(["second", "third"]).all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_multi_tag_with_traversal(self):
        """Test multi-tag combined with out() accepting a list of predicates."""
        expected = {'result': [{'both': 'cool_person', 'id': 'cool_person'},
                               {'both': '<fred>', 'id': '<fred>'}]}
        actual = TorqueTest.g.v("<bob>").out(["<follows>", "<status>"]).tag(["both"]).all()
        self.assertEqual(ordered(expected), ordered(actual))

    def test_multi_tag_preserves_previous_tags(self):
        """Test that multi-tagging preserves tags from earlier in the traversal."""
        expected = {'result': [{'start': '<bob>', 'end1': '<greg>', 'end2': '<greg>', 'id': '<greg>'}]}
        actual = TorqueTest.g.v("<bob>").tag("start").out("<follows>").out().tag(["end1", "end2"]).all()
        self.assertEqual(ordered(expected), ordered(actual))

    # ============================================================
    # Order tests
    # ============================================================

    def test_order_default_ascending(self):
        """Test that order() without arguments sorts ascending by default."""
        actual = TorqueTest.g.v("<bob>").out().order().all()
        ids = [r['id'] for r in actual['result']]
        self.assertEqual(ids, sorted(ids))

    def test_order_ascending(self):
        """Test that order(ASC) sorts ascending."""
        actual = TorqueTest.g.v("<bob>").out().order(ASC).all()
        ids = [r['id'] for r in actual['result']]
        self.assertEqual(ids, sorted(ids))

    def test_order_descending(self):
        """Test that order(DESC) sorts descending."""
        actual = TorqueTest.g.v("<bob>").out().order(DESC).all()
        ids = [r['id'] for r in actual['result']]
        self.assertEqual(ids, sorted(ids, reverse=True))

    def test_order_with_string_literals(self):
        """Test that order works with string literals 'asc' and 'desc'."""
        actual_asc = TorqueTest.g.v("<bob>").out().order("asc").all()
        actual_desc = TorqueTest.g.v("<bob>").out().order("desc").all()
        ids_asc = [r['id'] for r in actual_asc['result']]
        ids_desc = [r['id'] for r in actual_desc['result']]
        self.assertEqual(ids_asc, sorted(ids_asc))
        self.assertEqual(ids_desc, sorted(ids_desc, reverse=True))

    def test_order_placed_before_tag(self):
        """Test that order() can be placed before tag() in the query chain."""
        actual = TorqueTest.g.v("<bob>").out().order().tag("source").all()
        ids = [r['id'] for r in actual['result']]
        self.assertEqual(ids, sorted(ids))
        # Verify tags are still applied
        for r in actual['result']:
            self.assertIn('source', r)

    def test_order_on_empty_result(self):
        """Test that order() handles empty result sets gracefully."""
        actual = TorqueTest.g.v("<bob>").out(["<nonexistent>"]).order().all()
        self.assertEqual(actual, {'result': []})

    @classmethod
    def tearDownClass(cls):
        TorqueTest.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
