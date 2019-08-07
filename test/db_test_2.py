
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
        cogdb.put(('key', 'val'))

        # retrieve data
        print cogdb.get('key')

    def test_zzz_after_all_tests(self):
        shutil.rmtree('/tmp/cogtestdb2')
        print "*** deleted test data."