"""
Tests for:
- list_tables loading into namespaces on init
- load_namespace method
- load_edgelist method  
- put_list method
"""
from cog.core import Record
from cog.database import Cog
from cog import config
import unittest
import shutil
import os


class TestDatabaseCoverage(unittest.TestCase):
    """Tests to increase coverage of database.py"""

    @classmethod
    def setUpClass(cls):
        cls.db_path = '/tmp/cog_coverage_test'
        cls._cleanup_path(cls.db_path)

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_path(cls.db_path)
        print("*** deleted coverage test data.")

    @staticmethod
    def _cleanup_path(path):
        if os.path.exists(path):
            shutil.rmtree(path)

    def setUp(self):
        """Fresh db path for each test"""
        self._cleanup_path(self.db_path)
        os.makedirs(self.db_path)
        config.CUSTOM_COG_DB_PATH = self.db_path

    def tearDown(self):
        self._cleanup_path(self.db_path)

    def test_put_list_creates_new_list(self):
        """Test put_list creates a new list when key doesn't exist"""
        cogdb = Cog()
        cogdb.create_or_load_namespace("test_ns")
        cogdb.create_table("list_table", "test_ns")

        # Put first item - creates new list
        cogdb.put_list(Record('mylist', 'value1'))
        record = cogdb.get('mylist')
        self.assertIsNotNone(record)
        # put_list always creates list type records
        self.assertEqual(record.value, ['value1'])

        cogdb.close()

    def test_put_list_appends_to_existing(self):
        """Test put_list appends to existing list"""
        cogdb = Cog()
        cogdb.create_or_load_namespace("test_ns")
        cogdb.create_table("list_table", "test_ns")

        # Create list and append
        cogdb.put_list(Record('mylist', 'value1'))
        cogdb.put_list(Record('mylist', 'value2'))
        cogdb.put_list(Record('mylist', 'value3'))

        record = cogdb.get('mylist')
        self.assertIsNotNone(record)
        # Values should be in list form (newest first due to linked list structure)
        self.assertEqual(sorted(record.value), sorted(['value1', 'value2', 'value3']))

        cogdb.close()

    def test_load_edgelist(self):
        """Test loading graph from edgelist file"""
        cogdb = Cog()
        
        # Use dolphins test data file
        edgelist_path = os.path.join(os.path.dirname(__file__), 'test-data', 'dolphins')
        
        cogdb.load_edgelist(edgelist_path, "dolphins_graph", predicate="connected_to")
        
        # Verify the graph was loaded by checking we can access it
        cogdb.use_namespace("dolphins_graph")
        tables = cogdb.list_tables()
        
        # Should have node set table and predicate table
        self.assertIn(config.GRAPH_NODE_SET_TABLE_NAME, tables)
        
        # Verify actual data was loaded - check for nodes from the dolphins file
        # The dolphins file has edges like "11 1", "15 1", etc.
        cogdb.use_table(config.GRAPH_NODE_SET_TABLE_NAME)
        node_11 = cogdb.get("11")
        node_1 = cogdb.get("1")
        self.assertIsNotNone(node_11, "Node '11' from edgelist should exist")
        self.assertIsNotNone(node_1, "Node '1' from edgelist should exist")
        
        cogdb.close()

    def test_load_namespace_with_existing_tables(self):
        """Test that load_namespace properly loads existing tables from disk"""
        # First, create a database with some tables
        cogdb1 = Cog()
        cogdb1.create_or_load_namespace("persist_ns")
        cogdb1.create_table("table_a", "persist_ns")
        cogdb1.put(Record('key1', 'value1'))
        cogdb1.create_table("table_b", "persist_ns")
        cogdb1.put(Record('key2', 'value2'))
        cogdb1.close()

        # Now create a new Cog instance - should load existing namespace
        cogdb2 = Cog()
        cogdb2.create_or_load_namespace("persist_ns")
        
        # Tables should be loadable
        cogdb2.load_table("table_a", "persist_ns")
        record = cogdb2.get('key1')
        self.assertIsNotNone(record)
        self.assertEqual(record.value, 'value1')

        cogdb2.load_table("table_b", "persist_ns")
        record = cogdb2.get('key2')
        self.assertIsNotNone(record)
        self.assertEqual(record.value, 'value2')

        cogdb2.close()

    def test_list_tables_populates_namespaces_on_init(self):
        """Test that Cog init populates namespaces dict from list_tables"""
        # Create initial database with tables in default namespace
        cogdb1 = Cog()
        cogdb1.create_table("init_table1", config.COG_DEFAULT_NAMESPACE)
        cogdb1.put(Record('k1', 'v1'))
        cogdb1.create_table("init_table2", config.COG_DEFAULT_NAMESPACE)
        cogdb1.put(Record('k2', 'v2'))
        cogdb1.close()

        # New instance should have tables listed in namespaces
        cogdb2 = Cog()
        # The tables should be discoverable via list_tables
        tables = cogdb2.list_tables()
        self.assertIn("init_table1", tables)
        self.assertIn("init_table2", tables)
        cogdb2.close()

    def test_load_namespace_creates_namespace_entry(self):
        """Test load_namespace creates entry in namespaces dict if not present"""
        cogdb1 = Cog()
        cogdb1.create_or_load_namespace("new_ns")
        cogdb1.create_table("test_tbl", "new_ns")
        cogdb1.put(Record('x', 'y'))
        cogdb1.close()

        # Create new instance and explicitly load namespace
        cogdb2 = Cog()
        # Force load a namespace that exists on disk
        cogdb2.load_namespace("new_ns")
        
        self.assertIn("new_ns", cogdb2.namespaces)
        self.assertEqual(cogdb2.current_namespace, "new_ns")
        cogdb2.close()

    def test_put_list_with_multiple_keys(self):
        """Test put_list with multiple different keys"""
        cogdb = Cog()
        cogdb.create_or_load_namespace("test_ns")
        cogdb.create_table("multi_list", "test_ns")

        # Multiple lists
        cogdb.put_list(Record('list_a', 'a1'))
        cogdb.put_list(Record('list_a', 'a2'))
        cogdb.put_list(Record('list_b', 'b1'))
        cogdb.put_list(Record('list_b', 'b2'))
        cogdb.put_list(Record('list_b', 'b3'))

        record_a = cogdb.get('list_a')
        record_b = cogdb.get('list_b')

        self.assertEqual(len(record_a.value), 2)
        self.assertEqual(len(record_b.value), 3)

        cogdb.close()

    def test_load_edgelist_creates_namespace(self):
        """Test that load_edgelist creates namespace if it doesn't exist"""
        cogdb = Cog()
        
        edgelist_path = os.path.join(os.path.dirname(__file__), 'test-data', 'dolphins')
        
        # Load into a brand new namespace
        cogdb.load_edgelist(edgelist_path, "brand_new_graph")
        
        # Namespace should exist now
        self.assertTrue(cogdb.is_namespace("brand_new_graph"))
        
        cogdb.close()


if __name__ == '__main__':
    unittest.main()
