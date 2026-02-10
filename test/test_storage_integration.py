"""
Comprehensive integration test for CogDB's storage layer.

Exercises the full read/write/reopen lifecycle across every data path:
  - Raw Store read/write with problematic payloads (0xFD, 0xAC in marshal data)
  - Database-level put/get for strings, lists, sets
  - Graph-level triples, traversals, batch inserts
  - Embeddings (float vectors) — the primary trigger for the RECORD_SEP bug
  - Close + reopen cycles verifying data survives persistence
  - Mutations after reopen (interleaved read/write)
  - Large-scale batch inserts with reopen
  - Delete operations with reopen
  - Mixed workloads combining all of the above
"""

import logging
from logging.config import dictConfig
import marshal
import math
import os
import random
import shutil
import unittest

from cog.core import Table, Record, Store, RECORD_SEP, UNIT_SEP
from cog.database import Cog
from cog.torque import Graph
from cog import config

DIR_NAME = "TestStorageIntegration"
COG_HOME = "/tmp/" + DIR_NAME


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


# ---------------------------------------------------------------------------
# 1. Low-level Store tests
# ---------------------------------------------------------------------------
class TestStoreLayer(unittest.TestCase):
    """Direct Store read/write with payloads that contain sentinel bytes."""

    DB_DIR = COG_HOME + "/store_layer"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.DB_DIR):
            shutil.rmtree(cls.DB_DIR)
        os.makedirs(cls.DB_DIR + "/test_table/", exist_ok=True)
        config.CUSTOM_COG_DB_PATH = cls.DB_DIR
        dictConfig(config.logging_config)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.DB_DIR):
            shutil.rmtree(cls.DB_DIR)

    def _make_table(self, name):
        logger = logging.getLogger()
        return Table(name, "test_table", "integ_store", config, logger)

    # -- records whose marshalled payload contains RECORD_SEP (0xFD) ----------
    def test_store_records_with_0xFD_in_payload(self):
        table = self._make_table("fd_payload")
        store = table.store
        index = table.indexer.index_list[0]

        random.seed(42)
        written = {}
        for i in range(500):
            key = f"k_{i}"
            value = random.random()
            rec = Record(key, value)
            pos = store.save(rec)
            index.put(key, pos, store)
            written[key] = value

        # Confirm some records actually contain 0xFD
        fd_count = sum(
            1 for k, v in written.items()
            if RECORD_SEP in marshal.dumps((k, v))
        )
        self.assertGreater(fd_count, 0, "Need records with 0xFD in payload")

        # Read back every record
        for key, expected_val in written.items():
            rec = index.get(key, store)
            self.assertIsNotNone(rec, f"Missing key: {key}")
            self.assertEqual(rec.key, key)
            self.assertAlmostEqual(rec.value, expected_val)

        table.close()

    # -- records whose marshalled payload contains UNIT_SEP (0xAC) -----------
    def test_store_records_with_0xAC_in_payload(self):
        table = self._make_table("ac_payload")
        store = table.store
        index = table.indexer.index_list[0]

        random.seed(99)
        written = {}
        for i in range(500):
            key = f"u_{i}"
            value = random.random()
            rec = Record(key, value)
            pos = store.save(rec)
            index.put(key, pos, store)
            written[key] = value

        ac_count = sum(
            1 for k, v in written.items()
            if UNIT_SEP in marshal.dumps((k, v))
        )
        self.assertGreater(ac_count, 0, "Need records with 0xAC in payload")

        for key, expected_val in written.items():
            rec = index.get(key, store)
            self.assertIsNotNone(rec, f"Missing key: {key}")
            self.assertAlmostEqual(rec.value, expected_val)

        table.close()

    # -- records with both sentinels in payload -------------------------------
    def test_store_records_with_both_sentinels(self):
        table = self._make_table("both_sentinel")
        store = table.store
        index = table.indexer.index_list[0]

        random.seed(7)
        written = {}
        for i in range(2000):
            key = f"b_{i}"
            value = random.random()
            serialized = marshal.dumps((key, value))
            if RECORD_SEP in serialized and UNIT_SEP in serialized:
                rec = Record(key, value)
                pos = store.save(rec)
                index.put(key, pos, store)
                written[key] = value
            if len(written) >= 20:
                break

        self.assertGreater(len(written), 0, "Need records with both 0xFD and 0xAC")

        for key, expected_val in written.items():
            rec = index.get(key, store)
            self.assertIsNotNone(rec, f"Missing key: {key}")
            self.assertAlmostEqual(rec.value, expected_val)

        table.close()


# ---------------------------------------------------------------------------
# 2. Database layer: put/get strings, lists, sets + reopen
# ---------------------------------------------------------------------------
class TestDatabaseLayer(unittest.TestCase):
    """Cog database operations with close/reopen cycles."""

    DB_DIR = COG_HOME + "/db_layer"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.DB_DIR):
            shutil.rmtree(cls.DB_DIR)
        os.makedirs(cls.DB_DIR, exist_ok=True)
        config.CUSTOM_COG_DB_PATH = cls.DB_DIR

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.DB_DIR):
            shutil.rmtree(cls.DB_DIR)

    def test_string_put_get_reopen(self):
        """Write string records, close, reopen, verify."""
        db = Cog()
        db.create_or_load_namespace("ns_str")
        db.create_table("tbl_str", "ns_str")

        records = {f"key_{i}": f"value_{i}" for i in range(100)}
        for k, v in records.items():
            db.put(Record(k, v))

        # Verify before close
        for k, v in records.items():
            rec = db.get(k)
            self.assertIsNotNone(rec, f"Pre-close: missing {k}")
            self.assertEqual(rec.value, v)
        db.close()

        # Reopen and verify
        db2 = Cog()
        db2.create_or_load_namespace("ns_str")
        db2.create_table("tbl_str", "ns_str")
        for k, v in records.items():
            rec = db2.get(k)
            self.assertIsNotNone(rec, f"Post-reopen: missing {k}")
            self.assertEqual(rec.value, v)
        db2.close()

    def test_list_put_get_reopen(self):
        """Write list records, close, reopen, verify."""
        db = Cog()
        db.create_or_load_namespace("ns_list")
        db.create_table("tbl_list", "ns_list")

        # Build lists: key -> [val_0, val_1, val_2]
        expected = {}
        for i in range(30):
            key = f"list_key_{i}"
            vals = [f"item_{i}_{j}" for j in range(3)]
            expected[key] = vals
            for v in vals:
                db.put_list(Record(key, v))

        for k, vals in expected.items():
            rec = db.get(k)
            self.assertIsNotNone(rec)
            self.assertEqual(sorted(rec.value), sorted(vals))
        db.close()

        # Reopen
        db2 = Cog()
        db2.create_or_load_namespace("ns_list")
        db2.create_table("tbl_list", "ns_list")
        for k, vals in expected.items():
            rec = db2.get(k)
            self.assertIsNotNone(rec, f"Post-reopen: missing {k}")
            self.assertEqual(sorted(rec.value), sorted(vals))
        db2.close()

    def test_set_put_get_reopen(self):
        """Write set records (with dedup), close, reopen, verify."""
        db = Cog()
        db.create_or_load_namespace("ns_set")
        db.create_table("tbl_set", "ns_set")

        expected = {}
        for i in range(20):
            key = f"set_key_{i}"
            vals = {f"member_{i}_{j}" for j in range(4)}
            expected[key] = vals
            for v in vals:
                db.put_set(Record(key, v))
            # Insert duplicates — should be ignored
            for v in vals:
                db.put_set(Record(key, v))

        for k, vals in expected.items():
            rec = db.get(k)
            self.assertIsNotNone(rec)
            self.assertEqual(set(rec.value), vals)
        db.close()

        db2 = Cog()
        db2.create_or_load_namespace("ns_set")
        db2.create_table("tbl_set", "ns_set")
        for k, vals in expected.items():
            rec = db2.get(k)
            self.assertIsNotNone(rec, f"Post-reopen: missing {k}")
            self.assertEqual(set(rec.value), vals)
        db2.close()

    def test_delete_and_reopen(self):
        """Delete records, close, reopen, verify deletions persisted."""
        db = Cog()
        db.create_or_load_namespace("ns_del")
        db.create_table("tbl_del", "ns_del")

        for i in range(50):
            db.put(Record(f"dk_{i}", f"dv_{i}"))

        # Delete even keys
        for i in range(0, 50, 2):
            db.delete(f"dk_{i}")

        db.close()

        db2 = Cog()
        db2.create_or_load_namespace("ns_del")
        db2.create_table("tbl_del", "ns_del")
        for i in range(50):
            rec = db2.get(f"dk_{i}")
            if i % 2 == 0:
                self.assertIsNone(rec, f"dk_{i} should be deleted")
            else:
                self.assertIsNotNone(rec, f"dk_{i} should exist")
                self.assertEqual(rec.value, f"dv_{i}")
        db2.close()

    def test_write_after_reopen(self):
        """Reopen a database and continue writing without corruption."""
        db = Cog()
        db.create_or_load_namespace("ns_wr")
        db.create_table("tbl_wr", "ns_wr")
        for i in range(50):
            db.put(Record(f"phase1_{i}", f"v1_{i}"))
        db.close()

        # Reopen and add more data
        db2 = Cog()
        db2.create_or_load_namespace("ns_wr")
        db2.create_table("tbl_wr", "ns_wr")
        for i in range(50):
            db2.put(Record(f"phase2_{i}", f"v2_{i}"))
        db2.close()

        # Reopen and verify both phases
        db3 = Cog()
        db3.create_or_load_namespace("ns_wr")
        db3.create_table("tbl_wr", "ns_wr")
        for i in range(50):
            r1 = db3.get(f"phase1_{i}")
            self.assertIsNotNone(r1)
            self.assertEqual(r1.value, f"v1_{i}")
            r2 = db3.get(f"phase2_{i}")
            self.assertIsNotNone(r2)
            self.assertEqual(r2.value, f"v2_{i}")
        db3.close()

    def test_scanner_after_reopen(self):
        """Scanner should see all records after reopen."""
        db = Cog()
        db.create_or_load_namespace("ns_scan")
        db.create_table("tbl_scan", "ns_scan")
        keys = set()
        for i in range(80):
            k = f"scan_{i}"
            db.put(Record(k, f"sv_{i}"))
            keys.add(k)
        db.close()

        db2 = Cog()
        db2.create_or_load_namespace("ns_scan")
        db2.create_table("tbl_scan", "ns_scan")
        scanned_keys = set()
        for r in db2.scanner():
            scanned_keys.add(r.key)
        self.assertEqual(scanned_keys, keys)
        db2.close()


# ---------------------------------------------------------------------------
# 3. Graph layer: triples + embeddings + reopen
# ---------------------------------------------------------------------------
class TestGraphLayer(unittest.TestCase):
    """Graph-level operations with close/reopen cycles."""

    GRAPH_HOME = COG_HOME + "/graph_layer"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.GRAPH_HOME):
            shutil.rmtree(cls.GRAPH_HOME)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.GRAPH_HOME):
            shutil.rmtree(cls.GRAPH_HOME)

    def test_triples_reopen(self):
        """Insert triples, close, reopen, verify traversals."""
        g = Graph("triple_rw", cog_home=self.GRAPH_HOME)
        g.put("alice", "follows", "bob")
        g.put("bob", "follows", "charlie")
        g.put("charlie", "follows", "alice")
        g.put("alice", "likes", "pizza")
        g.put("bob", "likes", "sushi")
        g.close()

        g2 = Graph("triple_rw", cog_home=self.GRAPH_HOME)

        # Forward traversal
        result = g2.v("alice").out("follows").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("bob", ids)

        # Two-hop
        result = g2.v("alice").out("follows").out("follows").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("charlie", ids)

        # Reverse traversal
        result = g2.v("bob").inc("follows").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("alice", ids)

        g2.close()

    def test_batch_triples_reopen(self):
        """Batch insert triples, close, reopen, verify."""
        g = Graph("batch_rw", cog_home=self.GRAPH_HOME)
        triples = [(f"node_{i}", "connects", f"node_{i+1}") for i in range(200)]
        g.put_batch(triples)
        g.close()

        g2 = Graph("batch_rw", cog_home=self.GRAPH_HOME)
        for i in range(200):
            result = g2.v(f"node_{i}").out("connects").all()
            self.assertIsNotNone(result)
            ids = [r["id"] for r in result["result"]]
            self.assertIn(f"node_{i+1}", ids)
        g2.close()

    def test_embeddings_reopen(self):
        """Insert float embeddings, close, reopen, verify vectors and similarity."""
        g = Graph("embed_rw", cog_home=self.GRAPH_HOME)

        # Add some triples so the graph has vertex context
        triples = [(f"entity_{i}", "related_to", f"entity_{(i+1) % 100}") for i in range(100)]
        g.put_batch(triples)

        random.seed(42)
        embeddings = {}
        for i in range(100):
            name = f"entity_{i}"
            vec = [random.random() for _ in range(32)]
            g.put_embedding(name, vec)
            embeddings[name] = vec
        g.close()

        g2 = Graph("embed_rw", cog_home=self.GRAPH_HOME)

        # Verify exact vectors survive round-trip
        for name, vec in embeddings.items():
            stored = g2.get_embedding(name)
            self.assertIsNotNone(stored, f"Missing embedding: {name}")
            self.assertEqual(len(stored), len(vec))
            for a, b in zip(stored, vec):
                self.assertAlmostEqual(a, b, places=10)

        # Verify k-nearest still works
        nearest = g2.v().k_nearest("entity_0", k=5).all()
        self.assertIn("result", nearest)
        self.assertEqual(len(nearest["result"]), 5)

        g2.close()

    def test_embeddings_batch_reopen(self):
        """Batch-insert embeddings, close, reopen, verify."""
        g = Graph("embed_batch_rw", cog_home=self.GRAPH_HOME)

        random.seed(123)
        pairs = [(f"vec_{i}", [random.random() for _ in range(16)]) for i in range(100)]
        g.put_embeddings_batch(pairs)
        g.close()

        g2 = Graph("embed_batch_rw", cog_home=self.GRAPH_HOME)
        for name, vec in pairs:
            stored = g2.get_embedding(name)
            self.assertIsNotNone(stored, f"Missing: {name}")
            for a, b in zip(stored, vec):
                self.assertAlmostEqual(a, b, places=10)
        g2.close()

    def test_drop_edge_and_reopen(self):
        """Drop edges, close, reopen, verify removals persisted."""
        g = Graph("drop_rw", cog_home=self.GRAPH_HOME)
        g.put("x", "rel", "a")
        g.put("x", "rel", "b")
        g.put("x", "rel", "c")
        g.drop("x", "rel", "b")
        g.close()

        g2 = Graph("drop_rw", cog_home=self.GRAPH_HOME)
        result = g2.v("x").out("rel").all()
        ids = sorted([r["id"] for r in result["result"]])
        self.assertIn("a", ids)
        self.assertNotIn("b", ids)
        self.assertIn("c", ids)
        g2.close()

    def test_delete_embedding_and_reopen(self):
        """Delete embeddings, close, reopen, verify deletions."""
        g = Graph("del_embed_rw", cog_home=self.GRAPH_HOME)
        g.put_embedding("keep_me", [0.1, 0.2, 0.3])
        g.put_embedding("delete_me", [0.4, 0.5, 0.6])
        g.delete_embedding("delete_me")
        g.close()

        g2 = Graph("del_embed_rw", cog_home=self.GRAPH_HOME)
        self.assertIsNotNone(g2.get_embedding("keep_me"))
        self.assertIsNone(g2.get_embedding("delete_me"))
        g2.close()


# ---------------------------------------------------------------------------
# 4. Mixed workload: triples + embeddings + mutations + multi-reopen
# ---------------------------------------------------------------------------
class TestMixedWorkload(unittest.TestCase):
    """Combines triples, embeddings, deletes, and multiple reopen cycles."""

    GRAPH_HOME = COG_HOME + "/mixed"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.GRAPH_HOME):
            shutil.rmtree(cls.GRAPH_HOME)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.GRAPH_HOME):
            shutil.rmtree(cls.GRAPH_HOME)

    def test_full_lifecycle(self):
        """
        Phase 1: Create graph with triples + embeddings.
        Phase 2: Reopen, verify, add more data, delete some.
        Phase 3: Reopen again, verify everything.
        """
        random.seed(2026)

        # ---- Phase 1: initial load ----
        g = Graph("lifecycle", cog_home=self.GRAPH_HOME)

        triples_p1 = [
            ("einstein", "developed", "general_relativity"),
            ("einstein", "worked_at", "princeton"),
            ("einstein", "born_in", "germany"),
            ("python", "created_by", "guido"),
            ("python", "used_for", "machine_learning"),
            ("django", "written_in", "python"),
            ("pytorch", "written_in", "python"),
        ]
        g.put_batch(triples_p1)

        embeddings_p1 = {}
        for entity in ["einstein", "general_relativity", "princeton", "germany",
                        "python", "guido", "machine_learning", "django", "pytorch"]:
            vec = [random.random() for _ in range(64)]
            g.put_embedding(entity, vec)
            embeddings_p1[entity] = vec

        g.close()

        # ---- Phase 2: reopen, verify, mutate ----
        g = Graph("lifecycle", cog_home=self.GRAPH_HOME)

        # Verify phase 1 triples
        result = g.v("einstein").out("developed").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("general_relativity", ids)

        result = g.v("python").inc("written_in").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("django", ids)
        self.assertIn("pytorch", ids)

        # Verify phase 1 embeddings
        for name, vec in embeddings_p1.items():
            stored = g.get_embedding(name)
            self.assertIsNotNone(stored, f"Phase 2 check: missing {name}")
            for a, b in zip(stored, vec):
                self.assertAlmostEqual(a, b, places=10)

        # Add new triples
        triples_p2 = [
            ("einstein", "received", "nobel_prize"),
            ("newton", "developed", "classical_mechanics"),
            ("newton", "born_in", "england"),
        ]
        g.put_batch(triples_p2)

        # Add new embeddings
        embeddings_p2 = {}
        for entity in ["nobel_prize", "newton", "classical_mechanics", "england"]:
            vec = [random.random() for _ in range(64)]
            g.put_embedding(entity, vec)
            embeddings_p2[entity] = vec

        # Drop an edge
        g.drop("einstein", "born_in", "germany")

        # Delete an embedding
        g.delete_embedding("germany")

        g.close()

        # ---- Phase 3: reopen and verify everything ----
        g = Graph("lifecycle", cog_home=self.GRAPH_HOME)

        # Phase 1 triples still intact (except dropped edge)
        result = g.v("einstein").out("developed").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("general_relativity", ids)

        result = g.v("einstein").out("worked_at").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("princeton", ids)

        # Dropped edge should be gone
        result = g.v("einstein").out("born_in").all()
        if result and "result" in result and result["result"]:
            ids = [r["id"] for r in result["result"]]
            self.assertNotIn("germany", ids)

        # Phase 2 triples
        result = g.v("einstein").out("received").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("nobel_prize", ids)

        result = g.v("newton").out("developed").all()
        ids = [r["id"] for r in result["result"]]
        self.assertIn("classical_mechanics", ids)

        # Phase 1 embeddings still intact (except deleted)
        for name, vec in embeddings_p1.items():
            if name == "germany":
                self.assertIsNone(g.get_embedding(name))
                continue
            stored = g.get_embedding(name)
            self.assertIsNotNone(stored, f"Phase 3: missing {name}")
            for a, b in zip(stored, vec):
                self.assertAlmostEqual(a, b, places=10)

        # Phase 2 embeddings
        for name, vec in embeddings_p2.items():
            stored = g.get_embedding(name)
            self.assertIsNotNone(stored, f"Phase 3: missing {name}")
            for a, b in zip(stored, vec):
                self.assertAlmostEqual(a, b, places=10)

        # Similarity search still works across both phases
        nearest = g.v().k_nearest("einstein", k=5).all()
        self.assertIn("result", nearest)
        self.assertGreater(len(nearest["result"]), 0)

        g.close()


# ---------------------------------------------------------------------------
# 5. Stress: large batch + reopen
# ---------------------------------------------------------------------------
class TestLargeScale(unittest.TestCase):
    """Larger-scale batch to stress the storage layer."""

    GRAPH_HOME = COG_HOME + "/large_scale"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.GRAPH_HOME):
            shutil.rmtree(cls.GRAPH_HOME)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.GRAPH_HOME):
            shutil.rmtree(cls.GRAPH_HOME)

    def test_large_batch_triples_and_embeddings_reopen(self):
        """Insert 1000 triples + 500 embeddings, close, reopen, spot-check."""
        random.seed(777)

        g = Graph("large", cog_home=self.GRAPH_HOME)

        # 1000 triples
        triples = [(f"src_{i}", "links_to", f"dst_{i}") for i in range(1000)]
        g.put_batch(triples)

        # 500 embeddings (high dimension — more likely to contain 0xFD)
        embeddings = {}
        pairs = []
        for i in range(500):
            name = f"src_{i}"
            vec = [random.random() for _ in range(128)]
            pairs.append((name, vec))
            embeddings[name] = vec
        g.put_embeddings_batch(pairs)

        g.close()

        # Reopen
        g2 = Graph("large", cog_home=self.GRAPH_HOME)

        # Spot-check triples (every 50th)
        for i in range(0, 1000, 50):
            result = g2.v(f"src_{i}").out("links_to").all()
            self.assertIsNotNone(result)
            ids = [r["id"] for r in result["result"]]
            self.assertIn(f"dst_{i}", ids)

        # Spot-check embeddings (every 25th)
        for i in range(0, 500, 25):
            name = f"src_{i}"
            stored = g2.get_embedding(name)
            self.assertIsNotNone(stored, f"Missing embedding: {name}")
            self.assertEqual(len(stored), 128)
            for a, b in zip(stored, embeddings[name]):
                self.assertAlmostEqual(a, b, places=10)

        # k-nearest should still function
        nearest = g2.v().k_nearest("src_0", k=10).all()
        self.assertIn("result", nearest)
        self.assertEqual(len(nearest["result"]), 10)

        g2.close()


# ---------------------------------------------------------------------------
# 6. Edge cases
# ---------------------------------------------------------------------------
class TestEdgeCases(unittest.TestCase):
    """Unusual data patterns that could trip up the storage layer."""

    GRAPH_HOME = COG_HOME + "/edge_cases"

    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.GRAPH_HOME):
            shutil.rmtree(cls.GRAPH_HOME)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.GRAPH_HOME):
            shutil.rmtree(cls.GRAPH_HOME)

    def test_empty_string_values(self):
        """Empty string keys/values should round-trip."""
        db = Cog()
        config.CUSTOM_COG_DB_PATH = self.GRAPH_HOME
        os.makedirs(self.GRAPH_HOME, exist_ok=True)
        db.create_or_load_namespace("ns_edge")
        db.create_table("tbl_edge", "ns_edge")
        db.put(Record("empty_val", ""))
        rec = db.get("empty_val")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.value, "")
        db.close()

    def test_long_keys_and_values(self):
        """Very long strings should survive storage."""
        db = Cog()
        config.CUSTOM_COG_DB_PATH = self.GRAPH_HOME
        os.makedirs(self.GRAPH_HOME, exist_ok=True)
        db.create_or_load_namespace("ns_long")
        db.create_table("tbl_long", "ns_long")

        long_key = "k" * 1000
        long_val = "v" * 10000
        db.put(Record(long_key, long_val))
        db.close()

        db2 = Cog()
        db2.create_or_load_namespace("ns_long")
        db2.create_table("tbl_long", "ns_long")
        rec = db2.get(long_key)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.value, long_val)
        db2.close()

    def test_unicode_keys_and_values(self):
        """Unicode data should survive storage."""
        db = Cog()
        config.CUSTOM_COG_DB_PATH = self.GRAPH_HOME
        os.makedirs(self.GRAPH_HOME, exist_ok=True)
        db.create_or_load_namespace("ns_uni")
        db.create_table("tbl_uni", "ns_uni")

        pairs = {
            "emoji_key": "rocket: 🚀 star: ⭐",
            "chinese": "你好世界",
            "arabic": "مرحبا بالعالم",
            "mixed": "hello 世界 🌍 مرحبا",
        }
        for k, v in pairs.items():
            db.put(Record(k, v))
        db.close()

        db2 = Cog()
        db2.create_or_load_namespace("ns_uni")
        db2.create_table("tbl_uni", "ns_uni")
        for k, v in pairs.items():
            rec = db2.get(k)
            self.assertIsNotNone(rec, f"Missing: {k}")
            self.assertEqual(rec.value, v)
        db2.close()

    def test_special_float_embeddings(self):
        """Embeddings with extreme float values should survive."""
        g = Graph("special_floats", cog_home=self.GRAPH_HOME)

        special_vecs = {
            "tiny": [1e-300, -1e-300, 0.0, 1e-150],
            "large": [1e100, -1e100, 1e50, -1e50],
            "mixed": [0.0, 1.0, -1.0, float('inf') * 0],  # 0.0, 1.0, -1.0, nan→0
        }
        # Replace nan with 0 for comparison
        special_vecs["mixed"] = [0.0 if math.isnan(x) else x for x in special_vecs["mixed"]]

        for name, vec in special_vecs.items():
            g.put_embedding(name, vec)
        g.close()

        g2 = Graph("special_floats", cog_home=self.GRAPH_HOME)
        for name, vec in special_vecs.items():
            stored = g2.get_embedding(name)
            self.assertIsNotNone(stored, f"Missing: {name}")
            for a, b in zip(stored, vec):
                self.assertAlmostEqual(a, b, places=10)
        g2.close()

    def test_overwrite_same_key(self):
        """Overwriting the same key multiple times, then reopen."""
        db = Cog()
        config.CUSTOM_COG_DB_PATH = self.GRAPH_HOME
        os.makedirs(self.GRAPH_HOME, exist_ok=True)
        db.create_or_load_namespace("ns_overwrite")
        db.create_table("tbl_overwrite", "ns_overwrite")

        for i in range(50):
            db.put(Record("same_key", f"version_{i}"))
        db.close()

        db2 = Cog()
        db2.create_or_load_namespace("ns_overwrite")
        db2.create_table("tbl_overwrite", "ns_overwrite")
        rec = db2.get("same_key")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.value, "version_49")
        db2.close()


if __name__ == "__main__":
    unittest.main()
