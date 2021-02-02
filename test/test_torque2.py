from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "TorqueTest2"

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

class TorqueTest2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not os.path.exists("/tmp/"+DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)


    def test_torque_2(self):
        TorqueTest2.g = Graph(graph_name="better_graph", cog_dir="/tmp/" + DIR_NAME)
        TorqueTest2.g.put("A", "is better than", "B")\
            .put("B", "is better than", "C")\
            .put("A", "is better than", "D")\
            .put("Z", "is better than", "D")\
            .put("D", "is smaller than", "F")
        expected = {'result': [{'id': 'B'}, {'id': 'D'}]}
        actual = TorqueTest2.g.v("A").out(["is better than"]).all()
        self.assertTrue(ordered(expected) == ordered(actual))
        self.assertTrue(TorqueTest2.g.v("A").out(["is better than"]).count() == 2)
        self.assertTrue(TorqueTest2.g.v().count() == 6)
        TorqueTest2.g.close()


    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
