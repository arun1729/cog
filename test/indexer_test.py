from context import cog
from cog.core import Index
from cog.core import Store
from cog.core import Table
from cog.core import Indexer
from cog import database
from cog import config
import logging
import os
from logging.config import dictConfig
import string
import random

import unittest

class TestIndexer(unittest.TestCase):
    def test_indexer(self):
        if (not os.path.exists("/tmp/cog-test/test_table/")):
            os.mkdir("/tmp/cog-test/test_table/")

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        table = Table("testdb","test_table","test_xcvzdfsadx")

        store = Store(table,config,logger)
        indexer = Indexer(table,config,logger)

        max_range=1000
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


if __name__ == '__main__':
    unittest.main()
