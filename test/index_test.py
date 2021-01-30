from cog.core import Index
from cog.core import Store
from cog.core import TableMeta
from cog import config
import logging
import os
import shutil
from logging.config import dictConfig
import random
import string

import unittest


DIR_NAME = "TestIndexTemp"


class TestIndex2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        path = "/tmp/"+DIR_NAME+"/test_table"
        if not os.path.exists(path):
            os.makedirs(path)
        config.CUSTOM_COG_DB_PATH = "/tmp/"+DIR_NAME
        print("*** created: "+path)

    def test_put_get(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = ("new super data","super new old stuff")

        tablemeta = TableMeta("testdb", "test_table", "test_xcvzdfsadx", None)
        store = Store(tablemeta, config, logger)
        index = Index(tablemeta, config, logger, 0)

        for i in range(30):
            print("Index load: "+str(index.get_load()))
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = (key, value)
            position=store.save(expected_data)
            status=index.put(expected_data[0],position,store)
            if(status != None):
                returned_data=index.get(expected_data[0], store)
                self.assertEqual(expected_data, returned_data[1])
            else:
                print("Index has reached its capacity.")
                break

        c = 0
        scanner = index.scanner(store)
        for r in scanner:
            print(r)
            c += 1
        print("Total records scanned: " + str(c))

        index.close()
        store.close()

    @classmethod
    def tearDownClass(cls):
        path = "/tmp/"+DIR_NAME
        shutil.rmtree(path)
        print("*** deleted test data." + path)


if __name__ == '__main__':
    unittest.main()
