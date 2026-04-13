"""
Cloud ↔ Local Parity Tests
===========================

Runs the **same graph operations** against both a local Graph and a cloud
Graph, then asserts return types, response shapes, and values match.

This ensures the cloud response-normalisation layer in torque.py keeps the
two backends perfectly aligned.

Usage
-----
    # Locally (macOS may need SSL_CERT_FILE):
    SSL_CERT_FILE=$(python3 -c "import certifi; print(certifi.where())") \\
        COGDB_API_KEY=cog_xxx python3 -m pytest test/test_cloud_parity.py -v

    # Without a key the whole suite is auto-skipped:
    python3 -m pytest test/test_cloud_parity.py -v

CI
--
    Add COGDB_API_KEY as a GitHub Actions secret, then the workflow step
    passes it via env. See .github/workflows/python-tests.yml.
"""

import os
import shutil
import time
import unittest
from unittest.mock import patch

import pytest

CLOUD_API_KEY = os.environ.get("COGDB_API_KEY")
HAS_CLOUD = bool(CLOUD_API_KEY)

from cog.torque import Graph

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

DIR_NAME = "CloudParityTest"


def ordered(obj):
    """Recursively sort dicts/lists for deterministic comparison."""
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    return obj


# --------------------------------------------------------------------------- #
# Test class
# --------------------------------------------------------------------------- #

@pytest.mark.cloud
@unittest.skipUnless(HAS_CLOUD, "COGDB_API_KEY not set — skipping cloud parity tests")
class TestCloudLocalParity(unittest.TestCase):
    """
    Seeds identical data into a local graph and a cloud graph, runs the
    same operations on both, and asserts the responses are identical.
    """

    maxDiff = None

    # ── fixtures ───────────────────────────────────────────────────────────── #

    @classmethod
    def setUpClass(cls):
        # Clean local directory
        local_path = "/tmp/" + DIR_NAME
        if os.path.exists(local_path):
            shutil.rmtree(local_path)
        os.makedirs(local_path, exist_ok=True)

        # Unique graph name per run so cloud data doesn't collide
        cls.graph_name = f"parity_{int(time.time())}"

        # Local graph — must NOT pick up COGDB_API_KEY from env
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("COGDB_API_KEY", None)
            cls.local = Graph(graph_name=cls.graph_name, cog_home=DIR_NAME)
        assert not cls.local._cloud, "Local graph unexpectedly in cloud mode"

        # Cloud graph
        cls.cloud = Graph(graph_name=cls.graph_name, api_key=CLOUD_API_KEY)
        assert cls.cloud._cloud, "Cloud graph not in cloud mode"

        # Seed identical data on both
        cls.triples = [
            ("alice", "knows", "bob"),
            ("bob", "knows", "charlie"),
            ("charlie", "knows", "alice"),
            ("alice", "works_at", "acme"),
            ("bob", "works_at", "globex"),
            ("charlie", "works_at", "acme"),
            ("alice", "age", "30"),
            ("bob", "age", "25"),
            ("charlie", "age", "35"),
        ]
        for s, p, o in cls.triples:
            cls.local.put(s, p, o)
            cls.cloud.put(s, p, o)

        # Flush and wait for cloud backend to index before tests run
        cls.cloud.sync()
        time.sleep(2)

    @classmethod
    def tearDownClass(cls):
        cls.local.close()
        shutil.rmtree("/tmp/" + DIR_NAME, ignore_errors=True)

    # ── assertion helpers ──────────────────────────────────────────────────── #

    def assert_same_type(self, local_result, cloud_result, ctx=""):
        self.assertEqual(
            type(local_result), type(cloud_result),
            f"Type mismatch ({ctx}): "
            f"local={type(local_result).__name__}, "
            f"cloud={type(cloud_result).__name__}"
        )

    def assert_same_keys(self, local_result, cloud_result, ctx=""):
        if isinstance(local_result, dict) and isinstance(cloud_result, dict):
            self.assertEqual(
                set(local_result.keys()), set(cloud_result.keys()),
                f"Key mismatch ({ctx}): "
                f"local={set(local_result.keys())}, "
                f"cloud={set(cloud_result.keys())}"
            )

    def assert_same_shape(self, local_result, cloud_result, ctx=""):
        self.assert_same_type(local_result, cloud_result, ctx)
        self.assert_same_keys(local_result, cloud_result, ctx)

    def assert_same_result_set(self, local_result, cloud_result, ctx=""):
        self.assert_same_shape(local_result, cloud_result, ctx)
        self.assertEqual(
            ordered(local_result), ordered(cloud_result),
            f"Value mismatch ({ctx})"
        )

    # ====================================================================== #
    # Mutations — return types                                                #
    # ====================================================================== #

    def test_put_returns_self(self):
        """put() returns the Graph object for method chaining."""
        lr = self.local.put("_tmp", "r", "v")
        cr = self.cloud.put("_tmp", "r", "v")
        self.assertIsInstance(lr, Graph)
        self.assertIsInstance(cr, Graph)
        self.assertIs(lr, self.local)
        self.assertIs(cr, self.cloud)
        self.local.delete("_tmp", "r", "v")
        self.cloud.delete("_tmp", "r", "v")

    def test_delete_returns_self(self):
        """delete() returns the Graph object for method chaining."""
        self.local.put("_d", "r", "v")
        self.cloud.put("_d", "r", "v")
        lr = self.local.delete("_d", "r", "v")
        cr = self.cloud.delete("_d", "r", "v")
        self.assertIsInstance(lr, Graph)
        self.assertIsInstance(cr, Graph)

    def test_method_chaining_put(self):
        """g.put(...).put(...) works on both backends."""
        lr = self.local.put("_c1", "r", "v1").put("_c2", "r", "v2")
        cr = self.cloud.put("_c1", "r", "v1").put("_c2", "r", "v2")
        self.assertIsInstance(lr, Graph)
        self.assertIsInstance(cr, Graph)
        for s, o in [("_c1", "v1"), ("_c2", "v2")]:
            self.local.delete(s, "r", o)
            self.cloud.delete(s, "r", o)

    # ====================================================================== #
    # Traversals — v(), out(), inc(), has(), hasr()                           #
    # ====================================================================== #

    def test_v_all(self):
        """g.v().all() has {'result': [...]} with no extra keys."""
        lr = self.local.v().all()
        cr = self.cloud.v().all()
        self.assert_same_shape(lr, cr, "v().all()")
        self.assertEqual(set(lr.keys()), {"result"})
        self.assertEqual(set(cr.keys()), {"result"})
        for item in cr["result"]:
            self.assertIn("id", item)

    def test_v_vertex_out_all(self):
        """g.v('alice').out('knows').all()"""
        lr = self.local.v("alice").out("knows").all()
        cr = self.cloud.v("alice").out("knows").all()
        self.assert_same_result_set(lr, cr, "v('alice').out('knows').all()")

    def test_v_inc_all(self):
        """g.v('bob').inc('knows').all()"""
        lr = self.local.v("bob").inc("knows").all()
        cr = self.cloud.v("bob").inc("knows").all()
        self.assert_same_result_set(lr, cr, "v('bob').inc('knows').all()")

    def test_has_filter(self):
        """g.v().has('works_at', 'acme').all()"""
        lr = self.local.v().has("works_at", "acme").all()
        cr = self.cloud.v().has("works_at", "acme").all()
        self.assert_same_result_set(lr, cr, "has('works_at','acme')")

    def test_hasr(self):
        """g.v().hasr('knows', 'alice').all()"""
        lr = self.local.v().hasr("knows", "alice").all()
        cr = self.cloud.v().hasr("knows", "alice").all()
        self.assert_same_result_set(lr, cr, "hasr('knows','alice')")

    def test_chained_out(self):
        """g.v('alice').out('knows').out('knows').all()"""
        lr = self.local.v("alice").out("knows").out("knows").all()
        cr = self.cloud.v("alice").out("knows").out("knows").all()
        self.assert_same_result_set(lr, cr, "chained out().out()")

    def test_v_list(self):
        """g.v(['alice', 'bob']).all()"""
        lr = self.local.v(["alice", "bob"]).all()
        cr = self.cloud.v(["alice", "bob"]).all()
        self.assert_same_result_set(lr, cr, "v([list]).all()")

    # ====================================================================== #
    # count()                                                                 #
    # ====================================================================== #

    def test_count(self):
        """g.v('alice').out('knows').count() returns same int."""
        lr = self.local.v("alice").out("knows").count()
        cr = self.cloud.v("alice").out("knows").count()
        self.assertIsInstance(lr, int)
        self.assertIsInstance(cr, int)
        self.assertEqual(lr, cr)

    def test_v_count_all(self):
        """g.v().count() returns same int."""
        lr = self.local.v().count()
        cr = self.cloud.v().count()
        self.assertIsInstance(lr, int)
        self.assertIsInstance(cr, int)
        self.assertEqual(lr, cr)

    def test_count_empty(self):
        """count() on empty result returns 0 on both."""
        lr = self.local.v("nonexistent_xyz").out("knows").count()
        cr = self.cloud.v("nonexistent_xyz").out("knows").count()
        self.assertEqual(lr, 0)
        self.assertEqual(cr, 0)

    # ====================================================================== #
    # scan()                                                                  #
    # ====================================================================== #

    def test_scan_shape(self):
        """g.scan() returns {'result': [...]} with no extra keys."""
        lr = self.local.scan(limit=5)
        cr = self.cloud.scan(limit=5)
        self.assert_same_shape(lr, cr, "scan()")
        self.assertEqual(set(lr.keys()), {"result"})
        self.assertEqual(set(cr.keys()), {"result"})
        self.assertEqual(len(lr["result"]), len(cr["result"]))

    def test_scan_items_have_id(self):
        """Each scan result item has an 'id' key."""
        for item in self.local.scan()["result"]:
            self.assertIn("id", item)
        for item in self.cloud.scan()["result"]:
            self.assertIn("id", item)

    # ====================================================================== #
    # Edge cases                                                              #
    # ====================================================================== #

    def test_empty_result(self):
        """Query with no matches returns same empty structure."""
        lr = self.local.v("nonexistent_xyz").out("knows").all()
        cr = self.cloud.v("nonexistent_xyz").out("knows").all()
        self.assert_same_shape(lr, cr, "empty result")
        self.assertEqual(lr["result"], [])
        self.assertEqual(cr["result"], [])

    # ====================================================================== #
    # Lifecycle no-ops                                                        #
    # ====================================================================== #

    def test_sync_noop(self):
        """sync() does not raise on either backend."""
        self.local.sync()
        self.cloud.sync()

    def test_refresh_noop(self):
        """refresh() does not raise on either backend."""
        self.local.refresh()
        self.cloud.refresh()

    def test_close_safe(self):
        """close() does not raise on cloud."""
        tmp = Graph(graph_name="parity_close_test", api_key=CLOUD_API_KEY)
        tmp.close()

    # ====================================================================== #
    # Deep / complex tests                                                    #
    # ====================================================================== #

    # ── tag() + back() round-trip ─────────────────────────────────────────── #

    def test_tag_appears_in_all_results(self):
        """tag('x') labels propagate identically into all() dicts."""
        lr = self.local.v("alice").tag("origin").out("knows").all()
        cr = self.cloud.v("alice").tag("origin").out("knows").all()
        self.assert_same_shape(lr, cr, "tag in all()")
        # Every result should carry the 'origin' tag
        for item in lr["result"]:
            self.assertIn("origin", item)
        for item in cr["result"]:
            self.assertIn("origin", item)
        self.assert_same_result_set(lr, cr, "tag values")

    def test_tag_back_returns_to_origin(self):
        """v().tag('start').out().back('start') returns the starting vertices."""
        lr = self.local.v("alice").tag("start").out("knows").back("start").all()
        cr = self.cloud.v("alice").tag("start").out("knows").back("start").all()
        self.assert_same_result_set(lr, cr, "tag/back round-trip")
        # Should return alice (the tagged vertex), not the traversed neighbours
        local_ids = {item["id"] for item in lr["result"]}
        cloud_ids = {item["id"] for item in cr["result"]}
        self.assertEqual(local_ids, cloud_ids)
        self.assertIn("alice", local_ids)

    def test_multi_tag_back(self):
        """Two tags at different depths, back() to first."""
        lr = (self.local.v("alice").tag("t1")
              .out("knows").tag("t2")
              .out("works_at")
              .back("t1").all())
        cr = (self.cloud.v("alice").tag("t1")
              .out("knows").tag("t2")
              .out("works_at")
              .back("t1").all())
        self.assert_same_result_set(lr, cr, "multi-tag back(t1)")

    # ── order(), limit(), skip() ──────────────────────────────────────────── #

    def test_order_asc(self):
        """v().order('asc').all() returns vertices sorted ascending."""
        lr = self.local.v().order("asc").all()
        cr = self.cloud.v().order("asc").all()
        self.assert_same_shape(lr, cr, "order asc")
        local_ids = [item["id"] for item in lr["result"]]
        cloud_ids = [item["id"] for item in cr["result"]]
        self.assertEqual(local_ids, sorted(local_ids))
        self.assertEqual(cloud_ids, sorted(cloud_ids))
        self.assertEqual(local_ids, cloud_ids)

    def test_order_desc(self):
        """v().order('desc').all() returns vertices sorted descending."""
        lr = self.local.v().order("desc").all()
        cr = self.cloud.v().order("desc").all()
        local_ids = [item["id"] for item in lr["result"]]
        cloud_ids = [item["id"] for item in cr["result"]]
        self.assertEqual(local_ids, sorted(local_ids, reverse=True))
        self.assertEqual(local_ids, cloud_ids)

    def test_limit(self):
        """v().order('asc').limit(2).all() returns exactly 2 items."""
        lr = self.local.v().order("asc").limit(2).all()
        cr = self.cloud.v().order("asc").limit(2).all()
        self.assertEqual(len(lr["result"]), 2)
        self.assertEqual(len(cr["result"]), 2)
        self.assert_same_result_set(lr, cr, "limit(2)")

    def test_skip(self):
        """v().order('asc').skip(2).all() skips first 2 items."""
        full_lr = self.local.v().order("asc").all()
        lr = self.local.v().order("asc").skip(2).all()
        cr = self.cloud.v().order("asc").skip(2).all()
        expected_count = len(full_lr["result"]) - 2
        self.assertEqual(len(lr["result"]), expected_count)
        self.assertEqual(len(cr["result"]), expected_count)
        self.assert_same_result_set(lr, cr, "skip(2)")

    def test_limit_skip_pagination(self):
        """order + skip + limit simulates pagination identically."""
        lr_page1 = self.local.v().order("asc").limit(3).all()
        cr_page1 = self.cloud.v().order("asc").limit(3).all()
        lr_page2 = self.local.v().order("asc").skip(3).limit(3).all()
        cr_page2 = self.cloud.v().order("asc").skip(3).limit(3).all()
        self.assert_same_result_set(lr_page1, cr_page1, "page1")
        self.assert_same_result_set(lr_page2, cr_page2, "page2")
        # Pages must not overlap
        ids_p1 = {item["id"] for item in lr_page1["result"]}
        ids_p2 = {item["id"] for item in lr_page2["result"]}
        self.assertTrue(ids_p1.isdisjoint(ids_p2), "Pages overlap!")

    # ── both() traversal ──────────────────────────────────────────────────── #

    def test_both(self):
        """v('bob').both('knows').all() follows edges in both directions."""
        lr = self.local.v("bob").both("knows").all()
        cr = self.cloud.v("bob").both("knows").all()
        self.assert_same_result_set(lr, cr, "both('knows')")
        # bob knows charlie, and alice knows bob → both should appear
        ids = {item["id"] for item in lr["result"]}
        self.assertIn("charlie", ids)
        self.assertIn("alice", ids)

    # ── is_() filtering ───────────────────────────────────────────────────── #

    def test_is_filter(self):
        """v('alice').out('knows').is_('bob').all() filters to bob only."""
        lr = self.local.v("alice").out("knows").is_("bob").all()
        cr = self.cloud.v("alice").out("knows").is_("bob").all()
        self.assert_same_result_set(lr, cr, "is_('bob')")
        self.assertEqual(len(lr["result"]), 1)
        self.assertEqual(lr["result"][0]["id"], "bob")

    def test_is_multiple(self):
        """is_() with multiple args."""
        lr = self.local.v().is_("alice", "charlie").all()
        cr = self.cloud.v().is_("alice", "charlie").all()
        self.assert_same_result_set(lr, cr, "is_ multi")
        ids = {item["id"] for item in lr["result"]}
        self.assertEqual(ids, {"alice", "charlie"})

    # ── unique() deduplication ────────────────────────────────────────────── #

    def test_unique(self):
        """unique() removes duplicate vertices from multi-path results."""
        # alice and bob both work_at acme / globex; traversing inc may yield dupes
        lr = self.local.v("acme").inc("works_at").unique().all()
        cr = self.cloud.v("acme").inc("works_at").unique().all()
        self.assert_same_result_set(lr, cr, "unique()")
        local_ids = [item["id"] for item in lr["result"]]
        self.assertEqual(len(local_ids), len(set(local_ids)), "Duplicates in local")

    # ── BFS / DFS traversals ──────────────────────────────────────────────── #

    def test_bfs_basic(self):
        """BFS from alice over 'knows' edges with max_depth=2."""
        lr = self.local.v("alice").bfs(predicates="knows", max_depth=2).all()
        cr = self.cloud.v("alice").bfs(predicates="knows", max_depth=2).all()
        self.assert_same_result_set(lr, cr, "bfs depth=2")

    def test_bfs_min_depth(self):
        """BFS with min_depth=2 skips depth-1 neighbours."""
        lr = self.local.v("alice").bfs(predicates="knows", max_depth=2, min_depth=2).all()
        cr = self.cloud.v("alice").bfs(predicates="knows", max_depth=2, min_depth=2).all()
        self.assert_same_result_set(lr, cr, "bfs min_depth=2")
        # depth-1 neighbour (bob) should not appear
        ids = {item["id"] for item in lr["result"]}
        self.assertNotIn("bob", ids)

    def test_dfs_basic(self):
        """DFS from alice over 'knows' edges with max_depth=2."""
        lr = self.local.v("alice").dfs(predicates="knows", max_depth=2).all()
        cr = self.cloud.v("alice").dfs(predicates="knows", max_depth=2).all()
        self.assert_same_result_set(lr, cr, "dfs depth=2")

    def test_bfs_both_direction(self):
        """BFS with direction='both' follows edges in both directions."""
        lr = self.local.v("bob").bfs(predicates="knows", max_depth=1, direction="both").all()
        cr = self.cloud.v("bob").bfs(predicates="knows", max_depth=1, direction="both").all()
        self.assert_same_result_set(lr, cr, "bfs both")

    # ── graph() terminal ──────────────────────────────────────────────────── #

    def test_graph_structure(self):
        """graph() returns {nodes, links} with matching sets."""
        lr = self.local.v("alice").out("knows").graph()
        cr = self.cloud.v("alice").out("knows").graph()
        self.assert_same_shape(lr, cr, "graph()")
        self.assertIn("nodes", lr)
        self.assertIn("links", lr)
        self.assertIn("nodes", cr)
        self.assertIn("links", cr)
        # Compare node id sets
        local_node_ids = {n["id"] for n in lr["nodes"]}
        cloud_node_ids = {n["id"] for n in cr["nodes"]}
        self.assertEqual(local_node_ids, cloud_node_ids)

    # ── triples() terminal ────────────────────────────────────────────────── #

    def test_triples(self):
        """triples() returns the same set of (s, p, o) tuples."""
        lr = self.local.triples()
        cr = self.cloud.triples()
        self.assertIsInstance(lr, list)
        self.assertIsInstance(cr, list)
        self.assertEqual(sorted(lr), sorted(cr))

    # ── put_batch + query verification ────────────────────────────────────── #

    def test_put_batch_and_query(self):
        """put_batch() inserts are query-visible on both backends."""
        batch = [
            ("_pb_x", "rel", "_pb_y"),
            ("_pb_y", "rel", "_pb_z"),
            ("_pb_x", "rel", "_pb_z"),
        ]
        self.local.put_batch(batch)
        self.cloud.put_batch(batch)
        self.cloud.sync()
        time.sleep(1)

        lr = self.local.v("_pb_x").out("rel").all()
        cr = self.cloud.v("_pb_x").out("rel").all()
        self.assert_same_result_set(lr, cr, "put_batch query")
        self.assertEqual(len(lr["result"]), 2)

        # Clean up
        for s, p, o in batch:
            self.local.delete(s, p, o)
            self.cloud.delete(s, p, o)

    # ── delete + verify ───────────────────────────────────────────────────── #

    def test_delete_removes_triple(self):
        """Deleting a triple makes it invisible on both backends."""
        self.local.put("_del_a", "link", "_del_b")
        self.cloud.put("_del_a", "link", "_del_b")
        self.cloud.sync()
        time.sleep(1)

        # Verify it exists
        lr = self.local.v("_del_a").out("link").all()
        cr = self.cloud.v("_del_a").out("link").all()
        self.assertEqual(len(lr["result"]), 1)
        self.assertEqual(len(cr["result"]), 1)

        # Delete and verify gone
        self.local.delete("_del_a", "link", "_del_b")
        self.cloud.delete("_del_a", "link", "_del_b")
        self.cloud.sync()
        time.sleep(1)

        lr = self.local.v("_del_a").out("link").all()
        cr = self.cloud.v("_del_a").out("link").all()
        self.assertEqual(lr["result"], [])
        self.assertEqual(cr["result"], [])

    # ── Complex multi-step traversals ─────────────────────────────────────── #

    def test_out_then_has(self):
        """v('alice').out('knows').has('works_at', 'acme') — traverse then filter."""
        lr = self.local.v("alice").out("knows").has("works_at", "acme").all()
        cr = self.cloud.v("alice").out("knows").has("works_at", "acme").all()
        self.assert_same_result_set(lr, cr, "out+has")

    def test_inc_then_out(self):
        """Reverse then forward: inc('works_at').out('knows')."""
        lr = self.local.v("acme").inc("works_at").out("knows").all()
        cr = self.cloud.v("acme").inc("works_at").out("knows").all()
        self.assert_same_result_set(lr, cr, "inc+out chain")

    def test_deep_chain_tag_is(self):
        """Deep chain: v → tag → out → out → is_ → all with tag in results."""
        lr = (self.local.v("alice").tag("root")
              .out("knows").out("works_at").is_("acme").all())
        cr = (self.cloud.v("alice").tag("root")
              .out("knows").out("works_at").is_("acme").all())
        self.assert_same_result_set(lr, cr, "deep chain tag+is_")
        for item in lr["result"]:
            self.assertEqual(item.get("root"), "alice")
        for item in cr["result"]:
            self.assertEqual(item.get("root"), "alice")

    def test_v_list_out_order_limit(self):
        """v([list]).out().order().limit() — combined pipeline."""
        lr = (self.local.v(["alice", "bob"]).out("knows")
              .order("asc").limit(2).all())
        cr = (self.cloud.v(["alice", "bob"]).out("knows")
              .order("asc").limit(2).all())
        self.assert_same_result_set(lr, cr, "v-list+out+order+limit")
        self.assertEqual(len(lr["result"]), 2)

    def test_count_after_complex_traversal(self):
        """count() at end of multi-step chain."""
        lr = self.local.v("alice").out("knows").out("works_at").count()
        cr = self.cloud.v("alice").out("knows").out("works_at").count()
        self.assertIsInstance(lr, int)
        self.assertIsInstance(cr, int)
        self.assertEqual(lr, cr)

    # ── ls() graph listing ────────────────────────────────────────────────── #

    def test_ls_contains_graph(self):
        """ls() includes the current graph name on both backends."""
        lr = self.local.ls()
        cr = self.cloud.ls()
        self.assertIsInstance(lr, list)
        self.assertIsInstance(cr, list)
        self.assertIn(self.graph_name, cr)

    # ── scan with limit and type ──────────────────────────────────────────── #

    def test_scan_limit_respected(self):
        """scan(limit=3) returns at most 3 items on both."""
        lr = self.local.scan(limit=3)
        cr = self.cloud.scan(limit=3)
        self.assertLessEqual(len(lr["result"]), 3)
        self.assertLessEqual(len(cr["result"]), 3)
        self.assert_same_shape(lr, cr, "scan limit=3")

    def test_scan_edges(self):
        """scan(scan_type='e') returns edge results on both."""
        lr = self.local.scan(limit=5, scan_type="e")
        cr = self.cloud.scan(limit=5, scan_type="e")
        self.assert_same_shape(lr, cr, "scan edges")
        self.assertEqual(set(lr.keys()), {"result"})
        self.assertEqual(set(cr.keys()), {"result"})


if __name__ == "__main__":
    unittest.main()
