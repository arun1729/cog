from cog.torque import Loader
from cog.torque import Graph
from cog.database import Cog
import unittest
import os
import shutil

DIR_NAME = "TorqueTest"


class TorqueTest(unittest.TestCase):

    def test_aaa_before_all_tests(self):
        if not os.path.exists("/tmp/"+DIR_NAME+"/"):
            os.mkdir("/tmp/" + DIR_NAME + "/")

    def test_torque(self):
        loader = Loader("./test-data/test.nq", "people", "/tmp/graph")

        cog = Cog("/tmp/graph")
        cog.create_table("<follows>", "people")
        # scanner = cog.scanner()
        # for r in scanner:
        #     print r

        g = Graph(graph_name="people", cog_dir="/tmp/graph")
        print g.v("<alice>").out().count()
        print g.v("<alice>").out().all()


    def test_zzz_after_all_tests(self):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print "*** deleted test data."


if __name__ == '__main__':
    unittest.main()
