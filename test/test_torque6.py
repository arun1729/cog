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
        if not os.path.exists("/tmp/" + DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)

        TorqueTest.g = Graph(graph_name="peopletest", cog_home=DIR_NAME)

    def test_torque_drop_1(self):
        expected = {'result': [{'id': 'greg'}]}
        TorqueTest.g.put("bob", "friends", "greg")
        actual = TorqueTest.g.v("bob").out("friends").all()
        self.assertTrue(expected == actual)

        TorqueTest.g.drop("bob", "friends", "greg")
        actual = TorqueTest.g.v("bob").out("friends").all()
        self.assertTrue({'result': []} == actual)

    def test_torque_drop_2(self):
        TorqueTest.g.put("bob", "friends", "greg")
        TorqueTest.g.put("bob", "friends", "alice")

        expected = {'result': [{'id': 'alice'}, {'id': 'greg'}]}
        actual = TorqueTest.g.v("bob").out("friends").all()
        self.assertTrue(expected == actual)

        expected = {'result': [{'id': 'alice'}]}
        TorqueTest.g.drop("bob", "friends", "greg")
        actual = TorqueTest.g.v("bob").out("friends").all()
        self.assertTrue(expected == actual)

        actual = TorqueTest.g.v("greg").inc("friends").all()
        self.assertTrue({'result': []} == actual)

    def test_torque_drop_3(self):
        TorqueTest.g.put("bob", "friends", "greg")
        TorqueTest.g.put("bob", "friends", "alice")
        TorqueTest.g.put("bob", "neighbour", "alice")

        TorqueTest.g.drop("bob", "friends", "alice")

        expected = {'result': [{'id': 'alice'}]}
        actual = TorqueTest.g.v("bob").out("neighbour").all()
        self.assertTrue(expected == actual)

        expected = {'result': [{'id': 'bob', 'edges': ['neighbour']}]}
        actual = TorqueTest.g.v("alice").inc().all('e')
        self.assertTrue(ordered(expected) == ordered(actual))


    @classmethod
    def tearDownClass(cls):
        TorqueTest.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
