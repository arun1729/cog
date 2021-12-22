from cog.core import Record
from cog.database import Cog
import unittest
import os
import shutil

DIR_NAME = "TestLib"


class TestLib(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not os.path.exists("/tmp/"+DIR_NAME+"/"):
            os.mkdir("/tmp/" + DIR_NAME + "/")
            os.mkdir("/tmp/"+DIR_NAME+"/test/")

    def test_db(self):
        data = Record('testKey','testVal')
        cogdb = Cog()
        cogdb.create_or_load_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(data)
        self.assertTrue(cogdb.get("testKey").is_equal_val(Record('testKey', 'testVal')))
        cogdb.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()