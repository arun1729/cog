"""
Regression test for EOFError: marshal data too short.

The bug: Store.__read_until() used RECORD_SEP (0xFD) as a delimiter to find
record boundaries.  However, marshal.dumps() can produce bytes containing
0xFD — especially when serialising floats (e.g. embeddings).  This caused
the reader to truncate records mid-payload, corrupting them on reload.

"""

import marshal
import os
import random
import shutil
import unittest

from cog.torque import Graph
from cog.core import Table, Record, RECORD_SEP, UNIT_SEP
from cog import config

DIR_NAME = "TestEOFError"


class TestRecordSepInPayload(unittest.TestCase):
    """Verify records whose marshal output contains RECORD_SEP are
    round-tripped correctly through marshal/unmarshal and Store."""

    def test_marshal_unmarshal_with_fd_byte(self):
        """Record.unmarshal should survive payloads containing 0xFD."""
        # Find a key/value pair whose marshal.dumps output contains 0xFD
        random.seed(42)
        found = False
        for i in range(5000):
            key = f"entity_{i}"
            value = random.random()
            serialized = marshal.dumps((key, value))
            if RECORD_SEP in serialized:
                record = Record(key, value, key_link=100, value_type='s')
                marshalled = record.marshal()
                unmarshalled = Record.unmarshal(marshalled)
                self.assertEqual(unmarshalled.key, key)
                self.assertAlmostEqual(unmarshalled.value, value)
                found = True
                break
        self.assertTrue(found, "Could not find a key/value whose marshal output contains 0xFD")

    def test_marshal_unmarshal_list_type_with_fd_byte(self):
        """Record with value_type='l' should also survive 0xFD in payload."""
        random.seed(42)
        found = False
        for i in range(5000):
            key = f"entity_{i}"
            value = random.random()
            serialized = marshal.dumps((key, value))
            if RECORD_SEP in serialized:
                record = Record(key, value, key_link=100, value_type='l', value_link=9999)
                marshalled = record.marshal()
                unmarshalled = Record.unmarshal(marshalled)
                self.assertEqual(unmarshalled.key, key)
                self.assertAlmostEqual(unmarshalled.value, value)
                self.assertEqual(unmarshalled.value_link, 9999)
                found = True
                break
        self.assertTrue(found, "Could not find a key/value whose marshal output contains 0xFD")


class TestStoreReadWithRecordSep(unittest.TestCase):
    """Verify Store.read() correctly handles records with 0xFD in payload."""

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

    def test_store_roundtrip_with_fd_in_payload(self):
        """Write records containing 0xFD in their serialized payload,
        then read them back via Store.read() and verify correctness."""
        import logging
        from logging.config import dictConfig
        dictConfig(config.logging_config)
        logger = logging.getLogger()

        table = Table("testdb_eof", "test_table", "test_eof_instance", config, logger)
        store = table.store
        index = table.indexer.index_list[0]

        # Collect records whose marshal.dumps contains 0xFD
        random.seed(42)
        problematic_records = []
        for i in range(1000):
            key = f"node_{i}"
            value = random.random()
            serialized = marshal.dumps((key, value))
            if RECORD_SEP in serialized:
                problematic_records.append((key, value))
            if len(problematic_records) >= 10:
                break

        self.assertGreater(len(problematic_records), 0,
                           "Need at least one record with 0xFD in payload")

        # Write them through the Store
        positions = []
        for key, value in problematic_records:
            record = Record(key, value)
            pos = store.save(record)
            index.put(key, pos, store)
            positions.append(pos)

        # Read back and verify
        for (key, value), pos in zip(problematic_records, positions):
            raw = store.read(pos)
            self.assertIsNotNone(raw, f"Store.read returned None for key={key}")
            recovered = Record.unmarshal(raw)
            self.assertEqual(recovered.key, key)
            self.assertAlmostEqual(recovered.value, value)

        table.close()


class TestGraphReopenWithEmbeddings(unittest.TestCase):
    """End-to-end: create a graph with embeddings (floats), close it,
    reopen it, and verify data survives — the exact scenario that
    triggered the original EOFError."""

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

        # Insert triples
        triples = []
        for i in range(50):
            triples.append((f"entity_{i}", "relates_to", f"target_{i}"))
        g.put_batch(triples)

        # Insert embeddings (floats are the main source of 0xFD bytes)
        random.seed(42)
        entities_with_embeddings = []
        for i in range(50):
            name = f"entity_{i}"
            vec = [random.random() for _ in range(16)]
            g.put_embedding(name, vec)
            entities_with_embeddings.append((name, vec))

        g.close()

        # Reopen — this is where the old code would crash with
        # EOFError: marshal data too short
        g2 = Graph("EOFTest", cog_home=self.cog_home)

        # Verify graph traversal still works
        result = g2.v("entity_0").out().all()
        self.assertIsNotNone(result)
        self.assertIn("result", result)
        ids = [r["id"] for r in result["result"]]
        self.assertIn("target_0", ids)

        # Verify embeddings still work
        nearest = g2.v().k_nearest("entity_0", k=3).all()
        self.assertIsNotNone(nearest)
        self.assertIn("result", nearest)
        self.assertGreater(len(nearest["result"]), 0)

        g2.close()


if __name__ == "__main__":
    unittest.main()
