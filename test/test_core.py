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
        record = Record("rocket", "saturn-v", tombstone='0', store_position=25,  key_link=5, value_type='s', value_link=54378)
        print(record.marshal())
        unmarshalled_record = Record.unmarshal(record.marshal())
        print(unmarshalled_record)
        self.assertTrue(record.is_equal_val(unmarshalled_record))
        self.assertEqual(record.key, unmarshalled_record.key)
        self.assertEqual(record.value, unmarshalled_record.value)
        self.assertEqual(record.tombstone, unmarshalled_record.tombstone)
        self.assertEqual(record.key_link, unmarshalled_record.key_link)
        self.assertEqual(record.value_type, unmarshalled_record.value_type)
        self.assertEqual(Record.RECORD_LINK_NULL, unmarshalled_record.value_link)

    def test_record2(self):
        record = Record("rocket", "saturn-v", tombstone='0', store_position=25,  key_link=5, value_type='l', value_link=54378)
        unmarshalled_record = Record.unmarshal(record.marshal())
        print(unmarshalled_record)
        self.assertTrue(record.is_equal_val(unmarshalled_record))
        self.assertEqual(record.key, unmarshalled_record.key)
        self.assertEqual(record.value, unmarshalled_record.value)
        self.assertEqual(record.tombstone, unmarshalled_record.tombstone)
        self.assertEqual(record.key_link, unmarshalled_record.key_link)
        self.assertEqual(record.value_type, unmarshalled_record.value_type)
        self.assertEqual(record.value_link, unmarshalled_record.value_link)

    def test_record_list(self):
        record = Record("rockets", ["saturn-v","delta","atlas","mercury"], tombstone='0', store_position=25,  key_link=5, value_type='l', value_link=54378)
        unmarshalled_record = Record.unmarshal(record.marshal())
        print(unmarshalled_record)
        self.assertTrue(record.is_equal_val(unmarshalled_record))
        self.assertEqual(record.key, unmarshalled_record.key)
        self.assertEqual(record.value, unmarshalled_record.value)
        self.assertEqual(record.tombstone, unmarshalled_record.tombstone)
        self.assertEqual(record.key_link, unmarshalled_record.key_link)
        self.assertEqual(record.value_type, unmarshalled_record.value_type)
        self.assertEqual(record.value_link, unmarshalled_record.value_link)

    def test_put_get_string(self):
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = Record("rocket", "gemini-titan")

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

    def test_put_get_multiple_string(self):
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data_list= [Record("rocket", "gemini-titan"), Record("rocket2", "saturn V"), Record("rocket0", "V2")]

        table = Table("testdb", "test_table", "test_xcvzdfsadx", config, logger)
        print(config.COG_HOME)
        store = table.store
        index = table.indexer.index_list[0]
        for rec in expected_data_list:
            position = store.save(rec)
            index.put(rec.key, position, store)
            returned_data = index.get(rec.key, store)
            print("retrieved data: " + str(returned_data))
            self.assertTrue(rec.is_equal_val(returned_data))

        index.close()
        store.close()

    def test_put_get_record_update(self):
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data_list= [Record("rocket", "gemini-titan"), Record("rocket", "saturn V"), Record("rocket", "V2")]
        final_expected_data = Record("rocket", "V2")

        table = Table("testdb", "test_table", "test_xcvzdfsadx", config, logger)
        print(config.COG_HOME)
        store = table.store
        index = table.indexer.index_list[0]
        for rec in expected_data_list:
            position = store.save(rec)
            index.put(rec.key, position, store)
            returned_data = index.get(rec.key, store)
            print("retrieved data: " + str(returned_data))

        updated_rec = index.get(final_expected_data.key, store)
        self.assertTrue(updated_rec.is_equal_val(final_expected_data))

        index.close()
        store.close()

    def test_collision(self):
        orig_conf = config.INDEX_CAPACITY
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data_list= [Record("rocket", "gemini-titan"), Record("rocket2", "saturn V"), Record("rocket0", "V2")]

        table = Table("testdb", "test_table", "test_xcvzdfsadx", config, logger)
        config.INDEX_CAPACITY = 4
        print(config.COG_HOME)
        store = table.store
        index = table.indexer.index_list[0]
        for rec in expected_data_list:
            position = store.save(rec)
            index.put(rec.key, position, store)
            returned_data = index.get(rec.key, store)
            print("retrieved data: " + str(returned_data))
            self.assertTrue(rec.is_equal_val(returned_data))

        index.close()
        store.close()

        #set original config back
        config.INDEX_CAPACITY = orig_conf

    def test_collision_large(self):
        orig_conf = config.INDEX_CAPACITY
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        table = Table("testdb", "test_table", "test_xcvzdfsadx", config, logger)
        config.INDEX_CAPACITY = 4
        print(config.COG_HOME)
        store = table.store
        index = table.indexer.index_list[0]

        num_records = 1000  # Increase this as per your requirements
        expected_data_list = [Record(f"rocket{i}", f"rocket_name_{i}") for i in range(num_records)]

        for rec in expected_data_list:
            position = store.save(rec)
            index.put(rec.key, position, store)

        for rec in expected_data_list:
            returned_data = index.get(rec.key, store)
            print(f"retrieved data for key {rec.key}: {str(returned_data)}")
            self.assertTrue(rec.is_equal_val(returned_data))

        index.close()
        store.close()

        # set original config back
        config.INDEX_CAPACITY = orig_conf

    def test_put_get_list(self):
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        fruits = (["apple", "orange", "banana", "pears", "cherry", "mango"])

        table = Table("testdb2", "test_table", "test_xcvzdfsadx2", config, logger)
        store = table.store
        index = table.indexer.index_list[0]

        for fruit in fruits:
            print("storing :"+fruit)
            r = Record('fruits', fruit, value_type='l')
            print("CHECK IF LIST EXISTS - - - ->")
            read_record = index.get(r.key, store)
            # print("CHECK IF LIST EXISTS FOUND -> prev rec: "+str(record)+" get prev pos: "+str(record.store_position))
            if read_record is not None:
                print("prev record store pos: "+str(read_record.store_position))
                r.set_value_link(read_record.store_position)
            position = store.save(r)
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
            if record is not None:
                print("prev record store pos: "+str(record.store_position))
                r.set_value_link(record.store_position)
            position=store.save(r)
            print("stored new list value at store pos: "+str(position))

            index.put(r.key, position, store)
            print("indexed")

        index.delete(r.key, store)
        returned_data=index.get(r.key, store)
        print("retrieved data: "+str(returned_data))
        self.assertTrue(returned_data == None)

        index.close()
        store.close()


    def test_delete(self):

        dictConfig(config.logging_config)
        logger = logging.getLogger()

        expected_data = Record("new super data","super new old stuff")
        expected_data2 = Record("new super data", "updated value")

        table = Table("testdb","test_table","test_xcvzdfsadx", config, logger)

        store = table.store
        index = table.indexer.index_list[0]

        position=store.save(expected_data)
        print("stored")

        index.put(expected_data.key,position,store)
        print("indexed")

        position = store.save(expected_data2)
        print("stored")

        index.put(expected_data2.key, position, store)
        print("indexed")

        index.delete(expected_data.key, store)

        returned_data=index.get(expected_data.key, store)
        print("retrieved data: "+str(returned_data))
        self.assertTrue(returned_data == None)

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
        self.assertTrue(returned_data == None)

        indexer.close()
        store.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree("/tmp/"+DIR_NAME)
        print("*** deleted test data.")


if __name__ == '__main__':
    unittest.main()
