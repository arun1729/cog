from cog.core import Record
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
        tablemeta = TableMeta("testdb", "test_table", "test_xcvzdfsadx", None)
        store = Store(tablemeta, config, logger)
        index = Index(tablemeta, config, logger, 0)
        test_size = 30
        for i in range(test_size):
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = Record(key, value)
            position=store.save(expected_data)
            index.put(expected_data.key,position,store)
            returned_data=index.get(expected_data.key, store)
            self.assertTrue(expected_data.is_equal_val(returned_data))

        c = 0
        scanner = index.scanner(store)
        for r in scanner:
            print(r)
            c += 1
        self.assertEqual(test_size, c)

        index.close()
        store.close()

    @classmethod
    def tearDownClass(cls):
        path = "/tmp/"+DIR_NAME
        shutil.rmtree(path)
        print("*** deleted test data." + path)


if __name__ == '__main__':
    unittest.main()
