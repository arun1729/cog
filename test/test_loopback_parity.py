"""
Loopback Parity Tests

Tests the full code path in torque.py (chain accumulation,
CloudClient serialization, response normalization) by replacing the HTTP
transport with a loopback that executes queries on a second local Graph.
"""

import os
import re
import shutil
import unittest

from cog.torque import Graph

# Whitelist mirroring cog/server.py _execute_query
_ALLOWED_METHODS = {
    'v', 'out', 'inc', 'both', 'has', 'hasr', 'tag', 'back',
    'all', 'count', 'first', 'one', 'scan', 'filter', 'unique', 'limit', 'skip',
    'is_', 'bfs', 'dfs', 'sim', 'k_nearest', 'order',
}

_METHOD_RE = re.compile(r'\.?([a-zA-Z_][a-zA-Z0-9_]*)\s*\(')


def _execute_query(graph, query_str):
    """Safely eval a Torque query string — mirrors server._execute_query."""
    query_str = query_str.strip()

    allowed_starts = ('v(', 'scan(')
    if not any(query_str.startswith(s) for s in allowed_starts):
        raise ValueError(f"Query must start with one of: {list(allowed_starts)}")

    if '__' in query_str:
        raise ValueError("Query contains forbidden pattern '__'")

    methods_used = set(_METHOD_RE.findall(query_str))
    invalid = methods_used - _ALLOWED_METHODS
    if invalid:
        raise ValueError(f"Disallowed methods: {invalid}")

    full_query = f"graph.{query_str}"
    compile(full_query, '<query>', 'eval')
    result = eval(full_query, {"__builtins__": {}}, {"graph": graph})  # noqa: S307

    if isinstance(result, dict):
        return result
    if isinstance(result, int):
        return {"result": result}
    return {"result": []}


class LoopbackTransport:
    """
    Routes cloud HTTP calls to a second *local* Graph so that the full
    serialization ↔ deserialization round-trip is exercised without
    touching the network.
    """

    def __init__(self, backing_graph):
        self.g = backing_graph

    def __call__(self, method, path, body=None):
        if path == "/mutate_batch":
            return self._mutate_batch(body)
        if path == "/query":
            return self._query(body)
        raise ValueError(f"LoopbackTransport: unhandled path {path}")

    # ── mutations ────────────────────────────────────────────────────

    def _mutate_batch(self, body):
        mutations = body.get("mutations", [])
        for m in mutations:
            self._apply_one(m)
        return {"ok": True, "count": len(mutations)}

    def _apply_one(self, m):
        op = m.get("op", "")
        if op == "PUT":
            self.g.put(
                m["s"], m["p"], m["o"],
                update=m.get("update", False),
                create_new_edge=m.get("create_new_edge", False),
            )
        elif op == "DELETE":
            self.g.delete(m["s"], m["p"], m["o"])
        elif op == "DROP":
            self.g.drop()
        elif op == "TRUNCATE":
            self.g.truncate()
        elif op == "PUT_EMBEDDING":
            self.g.put_embedding(m["word"], m["embedding"])
        elif op == "DELETE_EMBEDDING":
            self.g.delete_embedding(m["word"])
        else:
            raise ValueError(f"LoopbackTransport: unknown mutation op={op}")

    # ── queries ──────────────────────────────────────────────────────

    def _query(self, body):
        q = body["q"]

        # Embedding helpers — not standard traversals
        if q == "embedding_stats()":
            result = self.g.embedding_stats()
            return {"ok": True, **result}

        m = re.match(r'^get_embedding\("(.+)"\)$', q)
        if m:
            emb = self.g.get_embedding(m.group(1))
            return {"ok": True, "embedding": emb}

        m = re.match(r'^scan_embeddings\((\d+)\)$', q)
        if m:
            result = self.g.scan_embeddings(limit=int(m.group(1)))
            return {"ok": True, **result}

        # Standard traversal queries (v(...), scan(...))
        result = _execute_query(self.g, q)
        # Mirror the server handler: {"ok": True, "result": <inner>}
        return {"ok": True, "result": result.get("result", result)}


# ─────────────────────────────────────────────────────────────────────────── #
# Helpers                                                                      #
# ─────────────────────────────────────────────────────────────────────────── #

LOCAL_DIR = "/tmp/LoopbackParityLocal"
CLOUD_DIR = "/tmp/LoopbackParityCloud"


def ordered(obj):
    """Recursively sort dicts/lists for deterministic comparison."""
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    return obj


def _make_cloud_graph(graph_name, backing_graph):
    """Create a Graph in cloud mode whose CloudClient routes to *backing_graph*."""
    g = Graph(graph_name=graph_name, api_key="loopback-key")
    # Patch the transport layer
    g._cloud_client._request = LoopbackTransport(backing_graph)
    return g


# ─────────────────────────────────────────────────────────────────────────── #
# Test class                                                                   #
# ─────────────────────────────────────────────────────────────────────────── #

class TestLoopbackParity(unittest.TestCase):
    """
    Seeds identical data into a local graph and a loopback-cloud graph,
    runs the same operations, and asserts the responses are identical.
    """

    maxDiff = None

    # ── fixtures ─────────────────────────────────────────────────────

    @classmethod
    def setUpClass(cls):
        for d in (LOCAL_DIR, CLOUD_DIR):
            if os.path.exists(d):
                shutil.rmtree(d)
            os.makedirs(d, exist_ok=True)

        graph_name = "loopback_parity"

        # Reference local graph
        cls.local = Graph(graph_name=graph_name, cog_home="LoopbackParityLocal")
        assert not cls.local._cloud

        # Backing graph for the loopback transport (separate storage)
        cls._backing = Graph(graph_name=graph_name, cog_home="LoopbackParityCloud")
        assert not cls._backing._cloud

        # Cloud graph wired to the backing local graph
        cls.cloud = _make_cloud_graph(graph_name, cls._backing)
        assert cls.cloud._cloud

        # Seed identical data
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

    @classmethod
    def tearDownClass(cls):
        cls.local.close()
        cls._backing.close()
        for d in (LOCAL_DIR, CLOUD_DIR):
            shutil.rmtree(d, ignore_errors=True)

    # ── assertion helpers ────────────────────────────────────────────

    def assert_same_type(self, local_result, cloud_result, ctx=""):
        self.assertEqual(
            type(local_result), type(cloud_result),
            f"Type mismatch ({ctx}): local={type(local_result).__name__}, "
            f"cloud={type(cloud_result).__name__}",
        )

    def assert_same_shape(self, local_result, cloud_result, ctx=""):
        self.assert_same_type(local_result, cloud_result, ctx)
        if isinstance(local_result, dict) and isinstance(cloud_result, dict):
            self.assertEqual(set(local_result.keys()), set(cloud_result.keys()),
                             f"Key mismatch ({ctx})")

    def assert_same_result_set(self, local_result, cloud_result, ctx=""):
        self.assert_same_shape(local_result, cloud_result, ctx)
        self.assertEqual(ordered(local_result), ordered(cloud_result),
                         f"Value mismatch ({ctx})")

    # ================================================================ #
    # Mutations — return types                                          #
    # ================================================================ #

    def test_put_returns_self(self):
        lr = self.local.put("_tmp", "r", "v")
        cr = self.cloud.put("_tmp", "r", "v")
        self.assertIsInstance(lr, Graph)
        self.assertIsInstance(cr, Graph)
        self.assertIs(lr, self.local)
        self.assertIs(cr, self.cloud)
        self.local.delete("_tmp", "r", "v")
        self.cloud.delete("_tmp", "r", "v")

    def test_delete_returns_self(self):
        self.local.put("_d", "r", "v")
        self.cloud.put("_d", "r", "v")
        lr = self.local.delete("_d", "r", "v")
        cr = self.cloud.delete("_d", "r", "v")
        self.assertIsInstance(lr, Graph)
        self.assertIsInstance(cr, Graph)

    def test_method_chaining_put(self):
        lr = self.local.put("_c1", "r", "v1").put("_c2", "r", "v2")
        cr = self.cloud.put("_c1", "r", "v1").put("_c2", "r", "v2")
        self.assertIsInstance(lr, Graph)
        self.assertIsInstance(cr, Graph)
        for s, o in [("_c1", "v1"), ("_c2", "v2")]:
            self.local.delete(s, "r", o)
            self.cloud.delete(s, "r", o)

    def test_put_batch(self):
        batch = [("_b1", "r", "x"), ("_b2", "r", "y")]
        lr = self.local.put_batch(batch)
        cr = self.cloud.put_batch(batch)
        self.assertIsInstance(lr, Graph)
        self.assertIsInstance(cr, Graph)
        # Verify data landed
        lr_data = self.local.v("_b1").out("r").all()
        cr_data = self.cloud.v("_b1").out("r").all()
        self.assert_same_result_set(lr_data, cr_data, "put_batch verify")
        for s, _, o in batch:
            self.local.delete(s, "r", o)
            self.cloud.delete(s, "r", o)

    # ================================================================ #
    # Traversals                                                        #
    # ================================================================ #

    def test_v_all(self):
        lr = self.local.v().all()
        cr = self.cloud.v().all()
        self.assert_same_shape(lr, cr, "v().all()")
        self.assertEqual(set(lr.keys()), {"result"})
        self.assertEqual(set(cr.keys()), {"result"})

    def test_v_vertex_out_all(self):
        lr = self.local.v("alice").out("knows").all()
        cr = self.cloud.v("alice").out("knows").all()
        self.assert_same_result_set(lr, cr, "v('alice').out('knows').all()")

    def test_v_inc_all(self):
        lr = self.local.v("bob").inc("knows").all()
        cr = self.cloud.v("bob").inc("knows").all()
        self.assert_same_result_set(lr, cr, "v('bob').inc('knows').all()")

    def test_has_filter(self):
        lr = self.local.v().has("works_at", "acme").all()
        cr = self.cloud.v().has("works_at", "acme").all()
        self.assert_same_result_set(lr, cr, "has('works_at','acme')")

    def test_hasr(self):
        lr = self.local.v().hasr("knows", "alice").all()
        cr = self.cloud.v().hasr("knows", "alice").all()
        self.assert_same_result_set(lr, cr, "hasr('knows','alice')")

    def test_both(self):
        lr = self.local.v("bob").both("knows").all()
        cr = self.cloud.v("bob").both("knows").all()
        self.assert_same_result_set(lr, cr, "both('knows')")

    def test_chained_out(self):
        lr = self.local.v("alice").out("knows").out("knows").all()
        cr = self.cloud.v("alice").out("knows").out("knows").all()
        self.assert_same_result_set(lr, cr, "chained out()")

    def test_v_list(self):
        lr = self.local.v(["alice", "bob"]).all()
        cr = self.cloud.v(["alice", "bob"]).all()
        self.assert_same_result_set(lr, cr, "v([list]).all()")

    def test_v_all_vertices(self):
        """v() with no args returns all vertices."""
        lr = self.local.v().all()
        cr = self.cloud.v().all()
        self.assertEqual(len(lr["result"]), len(cr["result"]))

    # ================================================================ #
    # Intermediate ops — unique, limit, skip, order, is_, tag, back     #
    # ================================================================ #

    def test_unique(self):
        lr = self.local.v("alice").out("knows").out("knows").unique().all()
        cr = self.cloud.v("alice").out("knows").out("knows").unique().all()
        self.assert_same_result_set(lr, cr, "unique()")

    def test_limit(self):
        lr = self.local.v().limit(2).all()
        cr = self.cloud.v().limit(2).all()
        self.assert_same_shape(lr, cr, "limit()")
        self.assertEqual(len(lr["result"]), len(cr["result"]))

    def test_skip(self):
        lr = self.local.v().skip(2).all()
        cr = self.cloud.v().skip(2).all()
        self.assert_same_shape(lr, cr, "skip()")

    def test_order_asc(self):
        lr = self.local.v().order("asc").all()
        cr = self.cloud.v().order("asc").all()
        # Order should match exactly
        self.assertEqual(
            [item["id"] for item in lr["result"]],
            [item["id"] for item in cr["result"]],
            "order(asc) mismatch",
        )

    def test_order_desc(self):
        lr = self.local.v().order("desc").all()
        cr = self.cloud.v().order("desc").all()
        self.assertEqual(
            [item["id"] for item in lr["result"]],
            [item["id"] for item in cr["result"]],
            "order(desc) mismatch",
        )

    def test_is_(self):
        lr = self.local.v().out("knows").is_("bob").all()
        cr = self.cloud.v().out("knows").is_("bob").all()
        self.assert_same_result_set(lr, cr, "is_('bob')")

    def test_tag_all(self):
        lr = self.local.v("alice").tag("start").out("knows").all()
        cr = self.cloud.v("alice").tag("start").out("knows").all()
        self.assert_same_result_set(lr, cr, "tag('start').out().all()")

    def test_back(self):
        lr = (self.local.v("alice").tag("origin")
              .out("knows").out("works_at").back("origin").all())
        cr = (self.cloud.v("alice").tag("origin")
              .out("knows").out("works_at").back("origin").all())
        self.assert_same_result_set(lr, cr, "back('origin')")

    # ================================================================ #
    # count()                                                           #
    # ================================================================ #

    def test_count(self):
        lr = self.local.v("alice").out("knows").count()
        cr = self.cloud.v("alice").out("knows").count()
        self.assertIsInstance(lr, int)
        self.assertIsInstance(cr, int)
        self.assertEqual(lr, cr)

    def test_v_count(self):
        lr = self.local.v().count()
        cr = self.cloud.v().count()
        self.assertIsInstance(lr, int)
        self.assertIsInstance(cr, int)
        self.assertEqual(lr, cr)

    def test_count_empty(self):
        lr = self.local.v("nonexistent").out("knows").count()
        cr = self.cloud.v("nonexistent").out("knows").count()
        self.assertEqual(lr, 0)
        self.assertEqual(cr, 0)

    # ================================================================ #
    # scan()                                                            #
    # ================================================================ #

    def test_scan_shape(self):
        lr = self.local.scan(limit=5)
        cr = self.cloud.scan(limit=5)
        self.assert_same_shape(lr, cr, "scan()")
        self.assertEqual(set(lr.keys()), {"result"})
        self.assertEqual(len(lr["result"]), len(cr["result"]))

    def test_scan_items_have_id(self):
        for item in self.local.scan()["result"]:
            self.assertIn("id", item)
        for item in self.cloud.scan()["result"]:
            self.assertIn("id", item)

    # ================================================================ #
    # Edge cases                                                        #
    # ================================================================ #

    def test_empty_result(self):
        lr = self.local.v("nonexistent").out("knows").all()
        cr = self.cloud.v("nonexistent").out("knows").all()
        self.assert_same_shape(lr, cr, "empty result")
        self.assertEqual(lr["result"], [])
        self.assertEqual(cr["result"], [])

    def test_empty_v_all(self):
        """v() on an empty graph would return result list."""
        lr = self.local.v("nobody_here").all()
        cr = self.cloud.v("nobody_here").all()
        self.assert_same_shape(lr, cr, "v(nonexistent).all()")

    # ================================================================ #
    # BFS / DFS                                                         #
    # ================================================================ #

    def test_bfs(self):
        lr = self.local.v("alice").bfs("knows", max_depth=2).all()
        cr = self.cloud.v("alice").bfs("knows", max_depth=2).all()
        self.assert_same_result_set(lr, cr, "bfs(knows, 2)")

    def test_dfs(self):
        lr = self.local.v("alice").dfs("knows", max_depth=2).all()
        cr = self.cloud.v("alice").dfs("knows", max_depth=2).all()
        self.assert_same_result_set(lr, cr, "dfs(knows, 2)")

    # ================================================================ #
    # Embeddings                                                        #
    # ================================================================ #

    def test_put_get_embedding(self):
        emb = [0.1, 0.2, 0.3, 0.4]
        self.local.put_embedding("emb_word", emb)
        self.cloud.put_embedding("emb_word", emb)

        lr = self.local.get_embedding("emb_word")
        cr = self.cloud.get_embedding("emb_word")
        self.assertEqual(lr, cr)

        self.local.delete_embedding("emb_word")
        self.cloud.delete_embedding("emb_word")

    def test_embedding_stats(self):
        self.local.put_embedding("stat_w", [1.0, 2.0])
        self.cloud.put_embedding("stat_w", [1.0, 2.0])

        lr = self.local.embedding_stats()
        cr = self.cloud.embedding_stats()
        self.assert_same_shape(lr, cr, "embedding_stats()")
        self.assertEqual(lr["count"], cr["count"])

        self.local.delete_embedding("stat_w")
        self.cloud.delete_embedding("stat_w")

    # ================================================================ #
    # Lifecycle no-ops                                                  #
    # ================================================================ #

    def test_sync_noop(self):
        self.local.sync()
        self.cloud.sync()

    def test_refresh_noop(self):
        self.local.refresh()
        self.cloud.refresh()

    def test_close_safe(self):
        """close() on cloud graph is a no-op and doesn't raise."""
        tmp = _make_cloud_graph("close_test", self._backing)
        tmp.close()

    # ================================================================ #
    # Truncate                                                          #
    # ================================================================ #

    def test_truncate(self):
        """truncate() empties the graph; both backends return 0 after."""
        # Separate graphs so we don't affect other tests
        ld = "/tmp/LoopbackTruncLocal"
        cd = "/tmp/LoopbackTruncCloud"
        for d in (ld, cd):
            if os.path.exists(d):
                shutil.rmtree(d)
            os.makedirs(d, exist_ok=True)

        lg = Graph(graph_name="trunc_test", cog_home="LoopbackTruncLocal")
        bg = Graph(graph_name="trunc_test", cog_home="LoopbackTruncCloud")
        cg = _make_cloud_graph("trunc_test", bg)

        lg.put("x", "r", "y")
        cg.put("x", "r", "y")

        lr = lg.truncate()
        cr = cg.truncate()
        self.assertIsInstance(lr, Graph)
        self.assertIsInstance(cr, Graph)

        self.assertEqual(lg.v().count(), 0)
        self.assertEqual(cg.v().count(), 0)

        lg.close()
        bg.close()
        for d in (ld, cd):
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
