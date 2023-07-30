from cog.core import Record
from cog.database import Cog
import unittest
import shutil
import os
from cog import config


class TestDB2(unittest.TestCase):

    def test_db(self):
        db_path = '/tmp/cogtestdb2'
        try:
            os.makedirs(db_path)
        except OSError:
            if not os.path.isdir(db_path):
                raise
        config.CUSTOM_COG_DB_PATH = db_path

        cogdb = Cog()

        # create a namespace
        cogdb.create_or_load_namespace("my_namespace")

        # create new table
        cogdb.create_table("new_db", "my_namespace")

        # put some data
        cogdb.put(Record('A', 'val'))
        cogdb.put(Record('B', 'val'))
        cogdb.put(Record('key3', 'val'))
        cogdb.put(Record('key3', 'val_updated'))

        self.assertEqual(cogdb.get('key3').value, 'val_updated')

        cogdb.close()

    def test_put_set(self):
        db_path = '/tmp/cogtestdb3'
        try:
            os.makedirs(db_path)
        except OSError:
            if not os.path.isdir(db_path):
                raise
        config.CUSTOM_COG_DB_PATH = db_path
        # config.INDEX_CAPACITY = 2
        cogdb = Cog()
        cogdb.create_or_load_namespace("my_namespace_4")
        cogdb.create_table("new_db", "my_namespace_4")

        cogdb.put_set(Record('key1', 'value1'))
        cogdb.put_set(Record('key1', 'value1'))
        cogdb.put_set(Record('key1', 'value2'))
        cogdb.put_set(Record('key1', 'value3'))
        cogdb.put_set(Record('key1', 'value4'))
        cogdb.put_set(Record('key1', 'value5'))

        record = cogdb.get('key1')
        self.assertEqual(sorted(record.value), sorted(['value1', 'value2', 'value3', 'value4', 'value5']))

        cogdb.close()

    def test_put_same_value_multiple_times(self):
        db_path = '/tmp/cogtestdb4'
        try:
            os.makedirs(db_path)
        except OSError:
            if not os.path.isdir(db_path):
                raise
        config.CUSTOM_COG_DB_PATH = db_path

        cogdb = Cog()
        cogdb.create_or_load_namespace("my_namespace_5")
        cogdb.create_table("new_db", "my_namespace_5")

        cogdb.put_set(Record('key1', 'value1'))
        cogdb.put_set(Record('key1', 'value1'))
        cogdb.put_set(Record('key1', 'value2'))

        record = cogdb.get('key1')
        self.assertEqual(record.value, ['value2', 'value1'])

        cogdb.close()

    def test_zzz_after_all_tests(self):
        shutil.rmtree('/tmp/cogtestdb2')
        shutil.rmtree('/tmp/cogtestdb3')
        shutil.rmtree('/tmp/cogtestdb4')
        print("*** deleted test data.")
