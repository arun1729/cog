from cog.core import Index
from cog.core import Store
from cog.core import Table
from cog import config
import logging
import os
import shutil
from logging.config import dictConfig
import random
import string

import unittest


DIR_NAME = "TestIndex2"


class TestIndex2(unittest.TestCase):

    def setUp(self):
        path = "/tmp/"+DIR_NAME+"/"
        if not os.path.exists(path):
            os.mkdir("/tmp/" + DIR_NAME + "/")
            os.mkdir("/tmp/"+DIR_NAME+"/test_table/")
        config.CUSTOM_COG_DB_PATH = path
        print "***"

    def test_put_get(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = ("new super data","super new old stuff")

        table = Table("testdb","test_table","test_xcvzdfsadx")

        store = Store(table,config,logger)
        index = Index(table,config,logger)

        for i in range(30):
            print "Index load: "+str(index.get_load())
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = (key,value)
            position=store.save(expected_data)
            status=index.put(expected_data[0],position,store)
            if(status != None):
                returned_data=index.get(expected_data[0], store)
                self.assertEqual(expected_data, returned_data[1])
            else:
                print "Index has reached its capacity."
                break

        c = 0
        scanner = index.scanner(store)
        for r in scanner:
            print r
            c += 1
        print "Total records scanned: " + str(c)

    def tearDown(self):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print "*** deleted test data."

if __name__ == '__main__':
    unittest.main()
