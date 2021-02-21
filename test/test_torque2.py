from cog.torque import Graph
from cog.database import Cog
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
        TorqueTest2.g = Graph(graph_name="better_graph", cog_home=DIR_NAME)
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


    def test_graph_db_load(self):
        data_dir = "test/test-data/test.nq"
        # choose appropriate path based on where the test is called from.
        # if os.path.exists("test-data/test.nq"):
        #     data_dir = "test-data/test.nq"
        #
        # g2 = Graph(graph_name="people", cog_home=DIR_NAME)
        #g2.load_triples(data_dir, "people")
        #put() ] indexed <alice> @: 1642200 : store position: 0 : key bit :b'8301954741'
        #g2 = Graph(graph_name="people", cog_home=DIR_NAME)
        # count = g2.v("<alice>").out().count()
        # print(count)
        #g = Graph(graph_name="movies", cog_home='graphtest4')
        #g.v("<name>").out().count()
        #cogdb = Cog('/tmp/'+DIR_NAME)
        #cogdb.load_triples('/Users/arun/Downloads/30kmoviedata.nq', 'movies')


    @classmethod
    def tearDownClass(cls):
        # pass
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
