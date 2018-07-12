from cog.core import Store
from cog.core import Table
from cog.core import Indexer
from cog import config
import logging
import os
import shutil
from logging.config import dictConfig
import string
import random
import unittest

DIR_NAME = "TestIndexer"


class TestIndexer(unittest.TestCase):

    def test_aaa_before_all_tests(self):
        if not os.path.exists("/tmp/"+DIR_NAME+"/"):
            os.mkdir("/tmp/" + DIR_NAME + "/")
            os.mkdir("/tmp/"+DIR_NAME+"/test_table/")

        config.COG_HOME = DIR_NAME

    def test_indexer_put_get(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        table = Table("testdb","test_table","test_xcvzdfsadx")

        store = Store(table,config,logger)
        indexer = Indexer(table,config,logger)

        max_range=100
        for i in range(max_range):
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = (key,value)

            position=store.save(expected_data)
            indexer.put(expected_data[0],position,store)
            returned_data=indexer.get(expected_data[0], store)
            print "indexer retrieved data: "+str(returned_data)
            self.assertEqual(expected_data, returned_data[1])
            print "Test progress: "+str(i*100.0/max_range)

        c = 0
        scanner = indexer.scanner(store)
        for r in scanner:
            # print r
            c += 1
        print "Total records scanned: " + str(c)

    def test_zzz_after_all_tests(self):
        shutil.rmtree("/tmp/"+DIR_NAME+"/")
        print "*** deleted test data: " + "/tmp/"+DIR_NAME


if __name__ == '__main__':
    unittest.main()
