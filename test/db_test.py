
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

    def test_aaa_before_all_tests(self):
        if not os.path.exists("/tmp/"+DIR_NAME+"/"):
            os.mkdir("/tmp/" + DIR_NAME + "/")
            os.mkdir("/tmp/"+DIR_NAME+"/test_table/")

        config.COG_HOME = DIR_NAME

    def test_db(self):
        data = ('user100','{"firstname":"Hari","lastname":"seldon"}')
        cogdb = Cog(config)
        cogdb.create_namespace("test")
        cogdb.create_table("db_test", "test")
        cogdb.put(data)
        scanner = cogdb.scanner()
        for r in scanner:
            print r

    def test_zzz_after_all_tests(self):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print "*** deleted test data."

if __name__ == '__main__':
    unittest.main()
