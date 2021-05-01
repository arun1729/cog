from cog.core import Table, Record
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


    def test_record(self):
        record = Record("rocket", "saturn-v", tombstone='0', store_position=25, rtype='s',  key_link=5)
        print(record.serialize())
        print(record.marshal())
        Record.unmarshal(b'1\x1f50s20\x1f)\x02\xda\x06rocket\xfa\x08saturn-v\x1e')

    def test_put_get_string(self):
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = Record("new super data","super new old stuff")

        table = Table("testdb", "test_table", "test_xcvzdfsadx", config, logger)
        print(config.COG_HOME)
        store = table.store
        index = table.indexer.index_list[0]

        position=store.save(expected_data)
        print("stored")

        index.put(expected_data.key,position,store)
        print("indexed")


        returned_data=index.get(expected_data.key, store)
        print("retrieved data: "+str(returned_data))
        self.assertTrue(expected_data.is_equal_val(returned_data))

        index.close()
        store.close()

    def test_put_get_list(self):
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        fruits = (["apple", "orange", "banana", "pears", "cherry", "mango"])

        table = Table("testdb2", "test_table", "test_xcvzdfsadx2", config, logger)
        store = table.store
        index = table.indexer.index_list[0]

        for fruit in fruits:
            print("storing :"+fruit)
            r = Record('fruits', fruit)
            print("CHECK IF LIST EXISTS - - - ->")
            record = index.get(r.key, store)
            print("CHECK IF LIST EXISTS FOUND -> prev rec: "+str(record)+" get prev pos: "+str(record.store_position))
            position=store.save(r, record.store_position, 'l')
            print("stored new list value at store pos: "+str(position))

            index.put(r.key, position, store)
            print("indexed")

        returned_data=index.get(r.key, store)
        print("retrieved data: "+str(returned_data))
        self.assertTrue(returned_data.is_equal_val(Record('fruits', ['mango', 'cherry', 'pears', 'banana', 'orange', 'apple'])))

        index.close()
        store.close()

    def test_delete_list(self):
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        fruits = (["apple", "orange", "banana", "pears", "cherry", "mango"])

        table = Table("testdb2", "test_table", "test_xcvzdfsadx2", config, logger)
        print(config.COG_HOME)
        store = table.store
        index = table.indexer.index_list[0]

        for fruit in fruits:
            print("storing :"+fruit)
            r = Record('fruits', fruit)
            print("CHECK IF LIST EXISTS - - - ->")
            record = index.get(r.key, store)
            print("CHECK IF LIST EXISTS FOUND -> prev rec: "+str(record)+" get prev pos: "+str(record.store_position))
            position=store.save(r, record.store_position, 'l')
            print("stored new list value at store pos: "+str(position))

            index.put(r.key, position, store)
            print("indexed")

        index.delete(r.key, store)
        returned_data=index.get(r.key, store)
        print("retrieved data: "+str(returned_data))
        self.assertTrue(returned_data.is_empty())

        index.close()
        store.close()


    def test_delete(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = Record("new super data","super new old stuff")

        table = Table("testdb","test_table","test_xcvzdfsadx", config, logger)

        store = table.store
        index = table.indexer.index_list[0]

        position=store.save(expected_data)
        print("stored")

        index.put(expected_data.key,position,store)
        print("indexed")

        index.delete(expected_data.key, store)

        returned_data=index.get(expected_data.key, store)
        print("retrieved data: "+str(returned_data))
        self.assertTrue(returned_data.is_empty())

        index.close()
        store.close()

    def test_indexer(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = Record("new super data","super new old stuff")

        table = Table("testdb","test_table","test_xcvzdfsadx", config, logger)

        store = table.store
        indexer = table.indexer

        position=store.save(expected_data)
        print("stored")

        indexer.put(expected_data.key,position,store)
        print("indexed by indexer")

        returned_data = indexer.get(expected_data.key, store)
        print("indexer retrieved data: " + str(returned_data))
        self.assertTrue(expected_data.is_equal_val(returned_data))

        indexer.delete(expected_data.key,store)
        returned_data=indexer.get(expected_data.key, store)
        print("indexer retrieved data after delete: "+str(returned_data))
        self.assertTrue(returned_data.is_empty())

        indexer.close()
        store.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
