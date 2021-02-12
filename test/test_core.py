from cog.core import Table
from cog import config
import logging
import os
from logging.config import dictConfig
import shutil

import unittest

DIR_NAME = "TestCore"


class TestCore(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        path = "/tmp/"+DIR_NAME+"/test_table/"
        if not os.path.exists(path):
            os.makedirs(path)
        config.CUSTOM_COG_DB_PATH = "/tmp/"+DIR_NAME

    def test_put_get(self):
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = ("new super data","super new old stuff")

        table = Table("testdb", "test_table", "test_xcvzdfsadx", config, logger)
        print(config.COG_HOME)
        store = table.store
        index = table.indexer.index_list[0]

        position=store.save(expected_data)
        print("stored")

        index.put(expected_data[0],position,store)
        print("indexed")


        returned_data=index.get(expected_data[0], store)
        print("retrieved data: "+str(returned_data))
        self.assertEqual(expected_data, returned_data[1])

        index.close()
        store.close()

    def test_delete(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = ("new super data","super new old stuff")

        table = Table("testdb","test_table","test_xcvzdfsadx", config, logger)

        store = table.store
        index = table.indexer.index_list[0]

        position=store.save(expected_data)
        print("stored")

        index.put(expected_data[0],position,store)
        print("indexed")

        index.delete(expected_data[0],store)

        returned_data=index.get(expected_data[0], store)
        print("retrieved data: "+str(returned_data))
        # self.assertEqual(None, returned_data)

        index.close()
        store.close()

    def test_indexer(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = ("new super data","super new old stuff")

        table = Table("testdb","test_table","test_xcvzdfsadx", config, logger)

        store = table.store
        indexer = table.indexer

        position=store.save(expected_data)
        print("stored")

        indexer.put(expected_data[0],position,store)
        print("indexed by indexer")

        returned_data = indexer.get(expected_data[0], store)
        print("indexer retrieved data: " + str(returned_data))
        self.assertEqual(expected_data, returned_data[1])

        indexer.delete(expected_data[0],store)
        returned_data=indexer.get(expected_data[0], store)
        print("indexer retrieved data after delete: "+str(returned_data))
        self.assertEqual(None, returned_data)

        indexer.close()
        store.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
