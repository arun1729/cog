from cog.torque import Graph
import unittest
import os
import shutil
import time

DIR_NAME = "TorqueTest3"


class TorqueTest3(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)

        if not os.path.exists("/tmp/" + DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)

    def test_torque_load_nq(self):
        nq_file = "test/test-data/100lines.nq"
        if os.path.exists("test-data/100lines.nq"):
            nq_file = "test-data/100lines.nq"
        g = Graph(graph_name="movies3", cog_path_prefix="/tmp/" + DIR_NAME)
        g.load_triples(nq_file, 'movies3')
        res = g.v("</en/joe_palma>").inc(["</film/performance/actor>"]).count()
        g.close()
        self.assertEqual(res, 7)

        #reload test
        g2 = Graph(graph_name="movies3", cog_path_prefix="/tmp/" + DIR_NAME)
        res2 = g2.v("</en/joe_palma>").inc(["</film/performance/actor>"]).count()
        g2.close()
        self.assertEqual(res2, 7, "reload test failed.")

    @classmethod
    def tearDownClass(cls):
        # pass
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
