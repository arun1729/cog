from cog.torque import Loader
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

    def test_aaa_before_all_tests(self):
        if not os.path.exists("/tmp/"+DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)

        if os.path.exists("test-data/dolphins"):
            loader = Loader("/tmp/"+DIR_NAME)
            loader.load_edgelist("test-data/dolphins", "social_graph")
        else:
            loader = Loader("/tmp/" + DIR_NAME)
            loader.load_edgelist("test/test-data/dolphins", "social_graph")

        TorqueTest2.cog = Cog("/tmp/"+DIR_NAME)
        TorqueTest2.g = Graph(graph_name="social_graph", cog_dir="/tmp/" + DIR_NAME)


    def test_torque_1(self):
        self.assertEqual(2, TorqueTest2.g.v("10").out().count())


    def test_torque_2(self):
        TorqueTest2.g.put("A","letters","B").put("B","letters","C").put("C","letters","D")
        TorqueTest2.g.put("Z","letters","D")
        expected = json.loads(
            r'{"result": [{"id": "C"}, {"id": "Z"}]}')
        actual = json.loads(TorqueTest2.g.v("A").out(["letters"]).out().out().inc().all())
        self.assertTrue(ordered(expected) == ordered(actual))


    def test_zzz_after_all_tests(self):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print "*** deleted test data."


if __name__ == '__main__':
    unittest.main()
