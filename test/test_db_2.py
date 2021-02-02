
from cog.database import Cog
import unittest
import shutil

class TestDB2(unittest.TestCase):
    def test_db(self):
        cogdb = Cog('/tmp/cogtestdb2')

        # create a namespace
        cogdb.create_namespace("my_namespace")

        # create new table
        cogdb.create_or_load_table("new_db", "my_namespace")

        # put some data
        cogdb.put(('A', 'val'))
        cogdb.put(('B', 'val'))
        cogdb.put(('key3', 'val'))
        cogdb.put(('key3', 'val_updated'))

        self.assertEqual(cogdb.get('key3')[1][1], 'val_updated')

        cogdb.close()


    def test_zzz_after_all_tests(self):
        shutil.rmtree('/tmp/cogtestdb2')
        print("*** deleted test data.")