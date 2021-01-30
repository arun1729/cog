from cog.core import Table
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

    @classmethod
    def setUpClass(cls):
        path = "/tmp/"+DIR_NAME+"/test_table/"
        if not os.path.exists(path):
            os.makedirs(path)
        config.CUSTOM_COG_DB_PATH = "/tmp/"+DIR_NAME
        print("*** " + config.CUSTOM_COG_DB_PATH + "\n")

    def test_indexer_put_get(self):
        if not os.path.exists("/tmp/"+DIR_NAME+"/test_table/"):
            os.makedirs("/tmp/"+DIR_NAME+"/test_table/")

        config.COG_HOME = DIR_NAME
        print("*** " + config.COG_HOME + "\n")

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        table = Table("testdb","test_table","test_xcvzdfsadx", config, logger)

        store = table.store
        indexer = table.indexer.index_list[0]

        max_range=100
        for i in range(max_range):
            key= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            value= ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100))
            expected_data = (key,value)

            position=store.save(expected_data)
            indexer.put(expected_data[0],position,store)
            returned_data=indexer.get(expected_data[0], store)
            print("indexer retrieved data: "+str(returned_data))
            self.assertEqual(expected_data, returned_data[1])
            print("Test progress: "+str(i*100.0/max_range))

        c = 0
        scanner = indexer.scanner(store)
        for r in scanner:
            # print r
            c += 1
        print("Total records scanned: " + str(c))

        indexer.close()
        store.close()
        table.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME+"/")
        print("*** deleted test data: " + "/tmp/"+DIR_NAME)


if __name__ == '__main__':
    unittest.main()
