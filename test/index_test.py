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
import random
import string

import unittest

class TestIndex2(unittest.TestCase):

    def test_put_get(self):
        if (not os.path.exists("/tmp/cog-test/test_table/")):
            os.mkdir("/tmp/cog-test/test_table/")

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

        index.set_itr_store(store)
        c = 0
        for r in index:
            print r
            c += 1
        print "Total records scanned: " + str(c)


if __name__ == '__main__':
    unittest.main()
