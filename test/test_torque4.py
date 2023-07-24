from cog.torque import Graph
import unittest
import os
import shutil

DIR_NAME = "TorqueTest4"

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

        data_dir = "test/test-data/test_func.nq"
        # choose appropriate path based on where the test is called from.
        if os.path.exists("test-data/test_func.nq"):
            data_dir = "test-data/test_func.nq"

        TorqueTest.g = Graph(graph_name="people", cog_home=DIR_NAME)
        TorqueTest.g.load_triples(data_dir, "people")
        print(">>> test setup complete.\n")

    def test_torque_func_out(self):
        expected = {'result': [{'id': 'alice'}, {'id': 'dani'}, {'id': 'greg'}]}
        actual = TorqueTest.g.v().out("score", func=lambda x: int(x) > 5).inc().all()
        self.assertTrue(expected == actual)

    def test_torque_func_out2(self):
        expected = {'result': [{'id': 'toronto'}]}
        actual = TorqueTest.g.v().out("city", func=lambda x: x.startswith("to")).all()
        self.assertTrue(expected == actual)

    def test_torque_func(self):
        expected = {'result': [{'id': 'vancouver'}]}
        actual = TorqueTest.g.v(func=lambda x: x.startswith("van")).all()
        self.assertTrue(expected == actual)

    def test_torque_func_inc(self):
        expected = {'result': [{'id': 'dani'}]}
        actual = TorqueTest.g.v().inc("city", func=lambda x: x.startswith("d")).all()
        self.assertTrue(expected == actual)

    def test_torque_func(self):
        expected = {'result': [{'id': 'edmonton'}, {'id': 'vancouver'}, {'id': 'montreal'}]}
        actual = TorqueTest.g.v().out("score", func=lambda x: int(x) > 5).inc().out("city").all()
        self.assertTrue(expected == actual)

    @classmethod
    def tearDownClass(cls):
        TorqueTest.g.close()
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
