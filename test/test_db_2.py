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


    def test_zzz_after_all_tests(self):
        shutil.rmtree('/tmp/cogtestdb2')
        print("*** deleted test data.")