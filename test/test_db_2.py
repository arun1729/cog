from cog.core import Record
from cog.database import Cog
import unittest
import shutil

class TestDB2(unittest.TestCase):
    def test_db(self):
        cogdb = Cog('/tmp/cogtestdb2')

        # create a namespace
        cogdb.create_namespace("my_namespace")

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