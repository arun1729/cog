from cog.torque import Graph
import unittest
import os
import shutil
import json

DIR_NAME = "TorqueTest5"

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

        data_dir = "test/test-data/test-data-sm.json"
        if os.path.exists("test-data/test-data-sm.json"):
            data_dir = "test-data/test-data-sm.json"

        TorqueTest.g = Graph(graph_name="people", cog_home=DIR_NAME)
        TorqueTest.f = open(data_dir, "r")
        jlist = json.loads(TorqueTest.f.read())
        for obj in jlist:
            TorqueTest.g.putj(obj)
        TorqueTest.f.close()

    def test_torque_json_1(self):
        expected = {'result': [{'id': 'fred'}, {'id': 'joe'}, {'id': 'alice'}]}
        actual = TorqueTest.g.v("bob").inc().out("friends").out().out("name").all()
        self.assertTrue(ordered(expected) == ordered(actual))

    def test_torque_json_3(self):
        expected = {'result': [{'id': '111-222-3333'}, {'id': '555-555-5555'}]}
        actual = TorqueTest.g.v("bob").inc().out("friends").out().out("contact").out("phone").all()
        self.assertTrue(ordered(expected) == ordered(actual))

    def test_torque_json_4(self):
        expected = 2
        actual = TorqueTest.g.v("alice").inc().out("friends").count()
        self.assertTrue(ordered(expected) == ordered(actual))

    def test_torque_json_4(self):
        expected = {'result': [{'id': 'joe'}, {'id': 'alice'}]}
        actual = TorqueTest.g.v().has("city", "montreal").inc().out("name").all()
        self.assertTrue(ordered(expected) == ordered(actual))

    def test_torque_json_update(self):
        expected = {'result': [{'id': 'vancouver'}]}
        actual = TorqueTest.g.v("fred").inc().out("location").out("city").all()
        self.assertTrue(ordered(expected) == ordered(actual))

        # example of mistake that can be made by updating an object with existing _id at a different location in the json.
        TorqueTest.g.updatej('{"_id" : "11", "city" : "edmonton"}')

        expected = {'result': [{'id': 'edmonton'}, {'id': 'vancouver'}]}
        actual = TorqueTest.g.v("fred").inc().out("location").out("city").all()
        self.assertTrue(ordered(expected) == ordered(actual))

        # the correct way to update that json is to update root object, note that location does not have an _id
        expected = {'result': [{'id': 'edmonton'}]}
        TorqueTest.g.updatej('{"_id":  "1", "name" : "fred", "location" : {"city" : "edmonton"}}')
        actual = TorqueTest.g.v("fred").inc().out("location").out("city").all()
        self.assertTrue(ordered(expected) == ordered(actual))

        with self.assertRaises(Exception) as excp:
            TorqueTest.g.updatej('{"_id":  "1", "name" : "fred", "location" : {"_id" : "11", "city" : "edmonton"}}')
            actual = TorqueTest.g.v("fred").inc().out("location").out("city").all()

        self.assertTrue(str(excp.exception), "Updating a sub object or list item with an _id is not supported.")

    def test_torque_json_update_2(self):
        """
        In this test a json array of objects is being updated, notice that 'bob' in friends get updated to alice
        since their ids match, if the ids did not match update will only add more elements into the array.
        """
        TorqueTest.g.putj('{"_id" : "2", "name" : "dani", "friends" : [{ "name":"bob", "contact": {"phone" :"888-888-8888"}}, {"name" : "emily"}]}')
        expected = {'result': [{'id': 'emily'}, {'id': 'bob'}]}
        actual = TorqueTest.g.v().has("name", "dani").out("friends").out().out("name").all()
        self.assertTrue(ordered(expected) == ordered(actual))

        # update the json object
        TorqueTest.g.updatej('{"_id" : "2", "name" : "dani", "friends": [{ "name":"alice", "contact": { '
                             '"phone" : '
                             '"555-555-5555"} }, {"name" : "joe", "contact": { "phone" : "111-222-3333"}}, '
                             '{"name" : "fred"}]}')

        expected = {'result': [{'id': 'fred'}, {'id': 'joe'}, {'id': 'alice'}]}
        actual = TorqueTest.g.v("dani").inc().out("friends").out().out("name").all()
        self.assertTrue(ordered(expected) == ordered(actual))

        actual = TorqueTest.g.v().has("name", "dani").all()
        self.assertTrue({'result': [{'id': '_:_id_2'}]} == actual)

        #update the same object again
        TorqueTest.g.updatej('{"_id" : "2", "name" : "charlie"}')

        actual = TorqueTest.g.v("charlie").inc().out("_id").all()
        self.assertTrue(actual['result'][0]['id'] == '2')

        actual = TorqueTest.g.v("dani").inc().out("_id").all()
        self.assertTrue(len(actual['result']) == 0)

    def test_torque_json_update_fail(self):
        # test if update fails when _id exists in an object in an array.
        with self.assertRaises(Exception) as excp:
            TorqueTest.g.putj(
                '{"_id" : "2", "name" : "dani", "friends" : [{ "_id": "xx", "name":"bob", "contact": {"phone" :"888-888-8888"}}, {"name" : "emily"}]}')


            TorqueTest.g.updatej('{"_id" : "2", "name" : "dani", "friends": [{ "_id": "xx", "name":"alice", "contact": { '
                                 '"phone" : '
                                 '"555-555-5555"} }, {"name" : "joe", "contact": { "phone" : "111-222-3333"}}, '
                                 '{"name" : "fred"}]}')
        self.assertTrue(str(excp.exception), "Updating a sub object or list item with an _id is not supported.")


    @classmethod
    def tearDownClass(cls):
        TorqueTest.g.close()
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
