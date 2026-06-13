"""Unit tests for cog.memory_view.MemoryView — the in-memory adjacency cache
that backs traversals by default (Graph(use_memory_view=True)). It was only
covered indirectly through torque; this exercises its mechanics directly:
byte-key ingest, demand paging, the negative cache, and inline mutations."""

import os
import shutil
import unittest

from cog.torque import Graph
from cog.database import hash_predicate
from cog.memory_view import MemoryView

DIR_NAME = "TestMemoryView"


def _targets(d):
    """MemoryView returns an insertion-ordered dict (or None); normalize to a
    set of neighbor ids for comparison."""
    return set(d) if d else set()


class TestMemoryView(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.home = "/tmp/" + DIR_NAME
        if os.path.exists(cls.home):
            shutil.rmtree(cls.home)
        os.makedirs(cls.home)
        # use_memory_view=False so put() only writes to disk and doesn't keep a
        # live view we'd have to reason about — each test builds its own view.
        cls.g = Graph(graph_name="mv", cog_home=DIR_NAME, use_memory_view=False)
        edges = [
            ("alice", "bob"), ("alice", "carol"),
            ("bob", "carol"), ("carol", "dave"),
        ]
        for s, t in edges:
            cls.g.put(s, "knows", t)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.home):
            shutil.rmtree(cls.home)

    def _view(self, page_size=None):
        table = self.g.cog.get_table(hash_predicate("knows"), "mv")
        return MemoryView(table, page_size=page_size)

    # --- ingest / basic lookups -------------------------------------------

    def test_out_neighbors(self):
        mv = self._view()
        self.assertEqual(_targets(mv.get_out("alice")), {"bob", "carol"})
        self.assertEqual(_targets(mv.get_out("carol")), {"dave"})

    def test_in_neighbors(self):
        mv = self._view()
        self.assertEqual(_targets(mv.get_in("carol")), {"alice", "bob"})
        self.assertEqual(_targets(mv.get_in("bob")), {"alice"})

    def test_keys_decoded_from_byte_prefix(self):
        # Edge keys are stored as b'\x00'/b'\x01' + utf-8; the view must expose
        # plain string node ids, not raw bytes.
        mv = self._view()
        for node in mv.get_out("alice"):
            self.assertIsInstance(node, str)

    # --- negative cache ----------------------------------------------------

    def test_missing_node_returns_none_idempotently(self):
        mv = self._view()
        self.assertIsNone(mv.get_out("ghost"))
        # Second call must also be None (and not raise) — the miss is cached.
        self.assertIsNone(mv.get_out("ghost"))
        self.assertIsNone(mv.get_in("ghost"))

    def test_leaf_has_no_out_edges(self):
        mv = self._view()
        self.assertIsNone(mv.get_out("dave"))

    # --- demand paging -----------------------------------------------------

    def test_demand_load_on_small_page(self):
        # A tiny page means the scanner has not loaded everything; lookups must
        # still resolve via the single-key disk fallback.
        mv = self._view(page_size=1)
        self.assertFalse(mv.fully_loaded)
        self.assertEqual(_targets(mv.get_out("carol")), {"dave"})
        self.assertEqual(_targets(mv.get_in("carol")), {"alice", "bob"})

    def test_load_more_reaches_fully_loaded(self):
        mv = self._view(page_size=1)
        for _ in range(50):
            if mv.fully_loaded:
                break
            mv.load_more()
        self.assertTrue(mv.fully_loaded)
        self.assertEqual(_targets(mv.get_out("alice")), {"bob", "carol"})

    # --- inline mutations --------------------------------------------------

    def test_add_edge_updates_both_directions(self):
        mv = self._view()
        mv.add_edge("alice", "erin")
        self.assertIn("erin", mv.get_out("alice"))
        self.assertIn("alice", mv.get_in("erin"))

    def test_remove_edge(self):
        mv = self._view()
        mv.remove_edge("alice", "bob")
        self.assertNotIn("bob", _targets(mv.get_out("alice")))
        # bob's only in-edge was from alice → now empty → None.
        self.assertIsNone(mv.get_in("bob"))

    def test_replace_out(self):
        mv = self._view()  # default page size → fully loaded
        self.assertTrue(mv.fully_loaded)
        mv.replace_out("alice", "zoe")
        self.assertEqual(_targets(mv.get_out("alice")), {"zoe"})
        # Old targets must drop alice from their in-edges; new target gains it.
        self.assertEqual(_targets(mv.get_in("carol")), {"bob"})
        self.assertEqual(_targets(mv.get_in("zoe")), {"alice"})

    def test_clear_resets_view(self):
        mv = self._view()
        mv.get_out("alice")
        mv.clear()
        # After clear the view re-scans lazily and still resolves edges.
        self.assertEqual(_targets(mv.get_out("alice")), {"bob", "carol"})


class TestMemoryViewShared(unittest.TestCase):
    """A view backed by shared out/in dicts is treated as fully loaded and does
    not run its own scanner."""

    @classmethod
    def setUpClass(cls):
        cls.home = "/tmp/" + DIR_NAME + "_shared"
        if os.path.exists(cls.home):
            shutil.rmtree(cls.home)
        os.makedirs(cls.home)
        cls.g = Graph(graph_name="mv", cog_home=DIR_NAME + "_shared",
                      use_memory_view=False)
        cls.g.put("a", "knows", "b")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.home):
            shutil.rmtree(cls.home)

    def test_shared_dicts_are_authoritative(self):
        table = self.g.cog.get_table(hash_predicate("knows"), "mv")
        shared_out = {"x": {"y": True}}
        shared_in = {"y": {"x": True}}
        mv = MemoryView(table, shared_out=shared_out, shared_in=shared_in)
        self.assertTrue(mv.fully_loaded)
        self.assertEqual(_targets(mv.get_out("x")), {"y"})
        # Disk edge a->b is NOT visible: shared view is authoritative.
        self.assertIsNone(mv.get_out("a"))


if __name__ == "__main__":
    unittest.main()
