from cog.core import Record
from cog.database import Cog
from cog import config
import json
import unittest
import os
import shutil


def qfilter(jsn):
    d = json.loads(jsn[1])
    return d["firstname"]


DIR_NAME = "TestDB"


class TestDB(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not os.path.exists("/tmp/"+DIR_NAME+"/"):
            os.mkdir("/tmp/" + DIR_NAME + "/")
            os.mkdir("/tmp/"+DIR_NAME+"/test_table/")

        config.COG_HOME = DIR_NAME

    def test_db(self):
        data = Record('user100','{"firstname":"Hari","lastname":"seldon"}')
        cogdb = Cog()
        cogdb.create_or_load_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(data)
        scanner = cogdb.scanner()
        res = None
        for r in scanner:
            res = r
        print(res)
        self.assertTrue(data.is_equal_val(res))
        cogdb.close()

    def test_list_tables(self):
        cogdb = Cog()
        cogdb.create_or_load_namespace("test_ns")
        cogdb.create_table("table1", "test_ns")
        cogdb.create_table("table2", "test_ns")
        cogdb.create_table("table3", "test_ns")
        self.assertEqual(set(cogdb.list_tables()), {'table2', 'table3', 'table1'})
        cogdb.close()


    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
