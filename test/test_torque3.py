from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "TorqueTest3"


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


class TorqueTest3(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not os.path.exists("/tmp/"+DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)

    def test_torque_load_nq(self):
        nq_file = "test/test-data/100movie.nq"
        if os.path.exists("test-data/100movie.nq"):
            nq_file = "test-data/100movie.nq"
        g = Graph(graph_name="movies", cog_path_prefix="/tmp/" + DIR_NAME)
        g.load_triples(nq_file, 'movies')
        g.close()
        # print(g.v("</en/joe_palma>").inc(["</film/performance/actor>"]).count())

    @classmethod
    def tearDownClass(cls):
        # pass
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
