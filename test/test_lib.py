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
        data = ('testKey','testVal')
        cogdb = Cog("/tmp/"+DIR_NAME+"/test")
        cogdb.create_namespace("test")
        cogdb.create_or_load_table("db_test", "test")
        cogdb.put(data)
        self.assertEqual(cogdb.get("testKey"), (b'0', ('testKey', 'testVal')))
        cogdb.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()