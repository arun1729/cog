"""
Regression test for data corruption with float payloads.

The original bug involved marshal-based codecs using separator bytes (0xFD)
that could appear inside serialized floats. The Spindle codec is
length-addressable with no separators, so this class of bug cannot recur.
These tests verify that float records round-trip correctly.
"""

import logging
import os
import random
import shutil
import unittest

from cog.torque import Graph
from cog.core import Table, Record, Store
from cog import config

DIR_NAME = "TestEOFError"


class TestRecordRoundTrip(unittest.TestCase):
    """Verify float records round-trip correctly through marshal/unmarshal."""

    def test_marshal_unmarshal_float(self):
        """Record.unmarshal should survive float payloads."""
        random.seed(42)
        for i in range(20):
            key = f"entity_{i}"
            value = random.random()
            record = Record(key, value, key_link=100, value_type='s')
            record.timestamp = 1
            marshalled = record.marshal()
            unmarshalled = Record.unmarshal(marshalled)
            self.assertEqual(unmarshalled.key, key)
            self.assertAlmostEqual(unmarshalled.value, value)

    def test_marshal_unmarshal_list_type_float(self):
        """Record with value_type='l' should round-trip float payloads."""
        random.seed(42)
        for i in range(20):
            key = f"entity_{i}"
            value = random.random()
            record = Record(key, value, key_link=100, value_type='l', value_link=9999)
            record.timestamp = 1
            marshalled = record.marshal()
            unmarshalled = Record.unmarshal(marshalled)
            self.assertEqual(unmarshalled.key, key)
            self.assertAlmostEqual(unmarshalled.value, value)
            self.assertEqual(unmarshalled.value_link, 9999)


class TestStoreReadWithFloats(unittest.TestCase):
    """Verify Store.read() correctly handles records with float payloads."""

    @classmethod
    def setUpClass(cls):
        path = "/tmp/" + DIR_NAME + "/test_table/"
        if not os.path.exists(path):
            os.makedirs(path)
        config.CUSTOM_COG_DB_PATH = "/tmp/" + DIR_NAME

    @classmethod
    def tearDownClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)

    def test_store_roundtrip_with_floats(self):
        """Write float records, then read them back via Store.read()."""
        logger = logging.getLogger()

        table = Table("testdb_eof", "test_table", "test_eof_instance", config, logger)
        store = table.store
        index = table.indexer.index_list[0]

        random.seed(42)
        records = []
        for i in range(50):
            key = f"node_{i}"
            value = random.random()
            records.append((key, value))

        positions = []
        for key, value in records:
            record = Record(key, value)
            pos = store.save(record)
            index.put(key, pos, store)
            positions.append(pos)

        for (key, value), pos in zip(records, positions):
            recovered = store.read(pos)
            self.assertIsNotNone(recovered, f"Store.read returned None for key={key}")
            self.assertEqual(recovered.key, key)
            self.assertAlmostEqual(recovered.value, value)

        table.close()


class TestGraphReopenWithEmbeddings(unittest.TestCase):
    """End-to-end: create a graph with embeddings (floats), close it,
    reopen it, and verify data survives."""

    @classmethod
    def setUpClass(cls):
        cls.cog_home = "/tmp/" + DIR_NAME + "_graph"
        if os.path.exists(cls.cog_home):
            shutil.rmtree(cls.cog_home)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.cog_home):
            shutil.rmtree(cls.cog_home)

    def test_graph_reopen_with_embeddings(self):
        """Graph with float embeddings should survive close + reopen."""
        g = Graph("EOFTest", cog_home=self.cog_home)

        triples = []
        for i in range(50):
            triples.append((f"entity_{i}", "relates_to", f"target_{i}"))
        g.put_batch(triples)

        random.seed(42)
        entities_with_embeddings = []
        for i in range(50):
            name = f"entity_{i}"
            vec = [random.random() for _ in range(16)]
            g.put_embedding(name, vec)
            entities_with_embeddings.append((name, vec))

        g.close()

        g2 = Graph("EOFTest", cog_home=self.cog_home)

        result = g2.v("entity_0").out().all()
        self.assertIsNotNone(result)
        self.assertIn("result", result)
        ids = [r["id"] for r in result["result"]]
        self.assertIn("target_0", ids)

        nearest = g2.v().k_nearest("entity_0", k=3).all()
        self.assertIsNotNone(nearest)
        self.assertIn("result", nearest)
        self.assertGreater(len(nearest["result"]), 0)

        g2.close()


class TestTruncatedStore(unittest.TestCase):
    """Verify that read_record handles truncated store files gracefully
    (returns None instead of raising)."""

    @classmethod
    def setUpClass(cls):
        cls.dir = "/tmp/TestTruncatedStore"
        if os.path.exists(cls.dir):
            shutil.rmtree(cls.dir)
        os.makedirs(cls.dir + "/test_table/", exist_ok=True)
        config.CUSTOM_COG_DB_PATH = cls.dir

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.dir):
            shutil.rmtree(cls.dir)

    def test_truncated_header_returns_none(self):
        """If the store file is truncated mid record-header,
        Store.read() should return None."""
        logger = logging.getLogger()
        table = Table("trunc_hdr", "test_table", "trunc_inst", config, logger)
        store = table.store
        data_start = store.data_start
        first_record_pos = store.store_file.seek(0, 2)

        rec = Record("key_trunc", "value_trunc")
        pos = store.save(rec)

        store.store_file.flush()
        store_path = store.store
        table.close()

        with open(store_path, 'r+b') as f:
            f.truncate(data_start + 10)

        table2 = Table("trunc_hdr", "test_table", "trunc_inst", config, logger)
        result = table2.store.read(first_record_pos)
        self.assertIsNone(result)
        table2.close()

    def test_truncated_payload_returns_none(self):
        """If the store file is truncated mid-payload,
        Store.read() should return None."""
        logger = logging.getLogger()
        table = Table("trunc_pay", "test_table", "trunc_inst2", config, logger)
        store = table.store
        data_start = store.data_start

        rec = Record("key_payload", "a_longer_value_for_testing")
        pos = store.save(rec)

        store.store_file.flush()
        store_path = store.store
        table.close()

        # Truncate past the record header but inside the payload.
        # Spindle: 17 bytes + varint — +25 lands mid-payload.
        with open(store_path, 'r+b') as f:
            f.truncate(data_start + 25)

        table2 = Table("trunc_pay", "test_table", "trunc_inst2", config, logger)
        result = table2.store.read(data_start)
        self.assertIsNone(result)
        table2.close()


if __name__ == "__main__":
    unittest.main()
