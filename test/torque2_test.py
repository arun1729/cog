from cog.torque import Graph
from cog.database import Cog
import unittest
import os
import json
import shutil

DIR_NAME = "TorqueTest2"

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

class TorqueTest2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not os.path.exists("/tmp/"+DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)

        data_dir = "test/test-data/dolphins"
        if os.path.exists("test-data/dolphins"):
            data_dir = "test-data/dolphins"

        TorqueTest2.g = Graph(graph_name="social_graph", cog_dir="/tmp/" + DIR_NAME)
        TorqueTest2.g.load_edgelist(data_dir, "social_graph")
        print "test setup done."


    def test_torque_1(self):
        self.assertEqual(2, TorqueTest2.g.v("10").out().count())


    def test_torque_2(self):
        TorqueTest2.g.put("A","letters","B").put("B","letters","C").put("C","letters","D")
        TorqueTest2.g.put("Z","letters","D")
        expected = json.loads(
            r'{"result": [{"id": "C"}, {"id": "Z"}]}')
        actual = json.loads(TorqueTest2.g.v("A").out(["letters"]).out().out().inc().all())
        self.assertTrue(ordered(expected) == ordered(actual))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print "*** deleted test data."


if __name__ == '__main__':
    unittest.main()
