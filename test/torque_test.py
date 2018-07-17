from cog.torque import Loader
from cog.torque import Graph
from cog.database import Cog
import unittest
import os
import shutil

DIR_NAME = "TorqueTest"

#target api set
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

#https://docs.janusgraph.org/latest/gremlin.html

class TorqueTest(unittest.TestCase):

    def test_aaa_before_all_tests(self):
        if not os.path.exists("/tmp/"+DIR_NAME+"/"):
            os.mkdir("/tmp/" + DIR_NAME + "/")

    def test_torque(self):
        #loader = Loader("./test-data/test.nq", "people", "/tmp/graph")

        cog = Cog("/tmp/graph")
        cog.create_table("<follows>", "people")
        # scanner = cog.scanner()
        # for r in scanner:
        #     print r

        g = Graph(graph_name="people", cog_dir="/tmp/graph")
        #print g.v("<alice>").out().count()
        #print g.v("<bob>").out().tag("source").all()
        #print g.v("<bob>").out().tag("source").out().tag("target").all()
        #print g.v("<bob>").out().tag("source").inc().tag("target").all()

        #print g.v("<fred>").out().tag("source").inc().tag("target").all()

        # repeat loops are not included, it seems to be there in cayley db in the following
        # print g.v("<fred>").out().tag("source").out().tag("target").all()
        #print g.v("<fred>").out().tag("source").inc().out().tag("target").all()

        #print g.v("<bob>").out(["<follows>"]).tag("source").all()
        #print g.v("<bob>").out(["<follows>","<status>"]).tag("source").all()

        #bad predicates, should not break test
        print g.v("<bob>").out(["<follows>zzz", "<status>zzz"]).tag("source").all()

    def test_zzz_after_all_tests(self):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print "*** deleted test data."


if __name__ == '__main__':
    unittest.main()
