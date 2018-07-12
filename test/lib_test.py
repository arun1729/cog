from cog.database import Cog
import unittest
import os
import shutil

DIR_NAME = "TestLib"


class TestLib(unittest.TestCase):

    def test_aaa_before_all_tests(self):
        if not os.path.exists("/tmp/"+DIR_NAME+"/"):
            os.mkdir("/tmp/" + DIR_NAME + "/")
            os.mkdir("/tmp/"+DIR_NAME+"/test/")

    def test_db(self):
        data = ('testKey','testVal')
        cogdb = Cog("/tmp/"+DIR_NAME+"/test/")
        cogdb.create_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(data)
        self.assertEqual(cogdb.get("testKey"), ('0', ('testKey', 'testVal')))

    def test_zzz_after_all_tests(self):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print "*** deleted test data."


if __name__ == '__main__':
    unittest.main()