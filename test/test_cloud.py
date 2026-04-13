from cog.torque import Graph
from cog import config as cfg
import unittest
import os
import shutil
import json
from unittest.mock import patch, MagicMock
from io import BytesIO
import urllib.error


def _mock_response(body_dict, status=200):
    """Create a mock HTTP response that works as a context manager."""
    resp = MagicMock()
    resp.read.return_value = json.dumps(body_dict).encode("utf-8")
    resp.status = status
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestCloudModeActivation(unittest.TestCase):
    """Test cloud mode activation via api_key param and env var."""

    def test_explicit_api_key_activates_cloud_mode(self):
        g = Graph("test-graph", api_key="cog_test123")
        self.assertTrue(g._cloud)
        self.assertEqual(g._api_key, "cog_test123")
        self.assertEqual(g.graph_name, "test-graph")

    def test_env_var_activates_cloud_mode(self):
        with patch.dict(os.environ, {"COGDB_API_KEY": "cog_env_key"}):
            g = Graph("test-graph")
            self.assertTrue(g._cloud)
            self.assertEqual(g._api_key, "cog_env_key")

    def test_explicit_key_overrides_env_var(self):
        with patch.dict(os.environ, {"COGDB_API_KEY": "cog_env_key"}):
            g = Graph("test-graph", api_key="cog_explicit")
            self.assertTrue(g._cloud)
            self.assertEqual(g._api_key, "cog_explicit")

    def test_no_key_stays_local(self):
        with patch.dict(os.environ, {}, clear=True):
            # Remove COGDB_API_KEY if present
            os.environ.pop("COGDB_API_KEY", None)
            g = Graph("test-graph", cog_path_prefix="/tmp")
            self.assertFalse(g._cloud)
            self.assertIsNone(g._api_key)
            g.close()
            shutil.rmtree("/tmp/cog_home", ignore_errors=True)

    def test_cloud_mode_does_not_create_local_files(self):
        g = Graph("cloud-test-no-files", api_key="cog_test123")
        self.assertFalse(hasattr(g, 'cog'))


class TestCloudServeBlocked(unittest.TestCase):
    """Test that serve() raises in cloud mode."""

    def test_serve_raises_runtime_error(self):
        g = Graph("test-graph", api_key="cog_test123")
        with self.assertRaises(RuntimeError) as ctx:
            g.serve()
        self.assertIn("cloud mode", str(ctx.exception))


class TestCloudWriteMethods(unittest.TestCase):
    """Test that write methods send correct HTTP requests."""

    @patch("urllib.request.urlopen")
    def test_put_sends_mutate_request(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 1})

        g = Graph("my-graph", api_key="cog_key123")
        result = g.put("alice", "knows", "bob")

        # put() returns self for method chaining
        self.assertIs(result, g)

        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.method, "POST")
        self.assertIn(f"{cfg.CLOUD_API_PREFIX}/my-graph/mutate_batch", req.full_url)
        self.assertEqual(req.get_header("Authorization"), "cog_key123")
        self.assertEqual(req.get_header("Content-type"), "application/json")

        body = json.loads(req.data.decode("utf-8"))
        self.assertIn("mutations", body)
        self.assertEqual(len(body["mutations"]), 1)
        m = body["mutations"][0]
        self.assertEqual(m["op"], "PUT")
        self.assertEqual(m["s"], "alice")
        self.assertEqual(m["p"], "knows")
        self.assertEqual(m["o"], "bob")

    @patch("urllib.request.urlopen")
    def test_delete_sends_mutate_request(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 1})

        g = Graph("my-graph", api_key="cog_key123")
        result = g.delete("alice", "knows", "bob")

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(body["mutations"][0]["op"], "DELETE")
        # Should return self for chaining
        self.assertIs(result, g)

    @patch("urllib.request.urlopen")
    def test_put_batch_sends_batch(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 2})

        g = Graph("my-graph", api_key="cog_key123")
        g.put_batch([
            ("alice", "knows", "bob"),
            ("bob", "knows", "charlie"),
        ])

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertIn("mutations", body)
        self.assertEqual(len(body["mutations"]), 2)
        self.assertTrue(all(m["op"] == "PUT" for m in body["mutations"]))


class TestCloudTraversalChain(unittest.TestCase):
    """Test that traversal chain accumulates and sends at terminal method."""

    @patch("urllib.request.urlopen")
    def test_v_out_all_sends_chain(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({
            "ok": True, "result": [{"id": "bob"}]
        })

        g = Graph("my-graph", api_key="cog_key123")
        result = g.v("alice").out("knows").all()

        # Should have made exactly one HTTP call (at all())
        mock_urlopen.assert_called_once()
        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))

        # Cloud client now sends a query string, not a raw chain
        self.assertIn("q", body)
        self.assertEqual(body["q"], 'v("alice").out("knows").all()')

        # Response should have 'ok' stripped
        self.assertEqual(result, {"result": [{"id": "bob"}]})
        self.assertNotIn("ok", result)

    @patch("urllib.request.urlopen")
    def test_v_out_tag_all_sends_chain(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({
            "ok": True, "result": [{"id": "bob", "source": ":(bob)"}]
        })

        g = Graph("my-graph", api_key="cog_key123")
        g.v("alice").out("knows").tag("source").all()

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(body["q"], 'v("alice").out("knows").tag("source").all()')

    @patch("urllib.request.urlopen")
    def test_count_sends_chain(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({"ok": True, "result": 42})

        g = Graph("my-graph", api_key="cog_key123")
        result = g.v("alice").out("knows").count()

        self.assertEqual(result, 42)

    @patch("urllib.request.urlopen")
    def test_v_all_vertices(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({
            "ok": True, "result": [{"id": "alice"}, {"id": "bob"}]
        })

        g = Graph("my-graph", api_key="cog_key123")
        result = g.v().all()

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(body["q"], 'v().all()')
        self.assertEqual(result["result"], [{"id": "alice"}, {"id": "bob"}])
        self.assertNotIn("ok", result)

    @patch("urllib.request.urlopen")
    def test_chain_resets_between_queries(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({"ok": True, "result": []})

        g = Graph("my-graph", api_key="cog_key123")
        g.v("alice").out("knows").all()  # First query

        # Start second query - chain should reset
        mock_urlopen.return_value = _mock_response({"ok": True, "result": [{"id": "charlie"}]})
        g.v("bob").all()

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        # Second query should only contain v("bob").all(), not the previous chain
        self.assertEqual(body["q"], 'v("bob").all()')

    @patch("urllib.request.urlopen")
    def test_bfs_in_chain(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({"ok": True, "result": [{"id": "charlie"}]})

        g = Graph("my-graph", api_key="cog_key123")
        g.v("alice").bfs(predicates="follows", max_depth=2).all()

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(body["q"], 'v("alice").bfs("follows", 2).all()')

    @patch("urllib.request.urlopen")
    def test_graph_terminal(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({
            "ok": True, "nodes": [{"id": "alice"}],
            "links": []
        })

        g = Graph("my-graph", api_key="cog_key123")
        result = g.v("alice").out("knows").tag("from").graph()

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertIn('graph()', body["q"])


class TestCloudTriples(unittest.TestCase):
    """Test triples() in cloud mode."""

    @patch("urllib.request.urlopen")
    def test_triples_returns_tuples(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({
            "triples": [["alice", "knows", "bob"], ["bob", "knows", "charlie"]]
        })

        g = Graph("my-graph", api_key="cog_key123")
        result = g.triples()

        self.assertEqual(result, [
            ("alice", "knows", "bob"),
            ("bob", "knows", "charlie"),
        ])


class TestCloudErrorHandling(unittest.TestCase):
    """Test HTTP error mapping."""

    def _make_http_error(self, code, body=None):
        if body is None:
            body = {"detail": "test error"}
        return urllib.error.HTTPError(
            url="https://api.cogdb.io/test",
            code=code,
            msg="Error",
            hdrs={},
            fp=BytesIO(json.dumps(body).encode("utf-8"))
        )

    @patch("urllib.request.urlopen")
    def test_401_raises_permission_error(self, mock_urlopen):
        mock_urlopen.side_effect = self._make_http_error(401)
        g = Graph("my-graph", api_key="cog_bad_key")
        with self.assertRaises(PermissionError) as ctx:
            g.put("a", "b", "c")
        self.assertIn("Invalid API key", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_403_raises_permission_error(self, mock_urlopen):
        mock_urlopen.side_effect = self._make_http_error(403)
        g = Graph("my-graph", api_key="cog_bad_key")
        with self.assertRaises(PermissionError):
            g.put("a", "b", "c")

    @patch("urllib.request.urlopen")
    def test_400_raises_value_error(self, mock_urlopen):
        mock_urlopen.side_effect = self._make_http_error(400, {"detail": "missing field"})
        g = Graph("my-graph", api_key="cog_key")
        with self.assertRaises(ValueError) as ctx:
            g.put("a", "b", "c")
        self.assertIn("missing field", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_500_raises_runtime_error(self, mock_urlopen):
        mock_urlopen.side_effect = self._make_http_error(500)
        g = Graph("my-graph", api_key="cog_key")
        with self.assertRaises(RuntimeError) as ctx:
            g.put("a", "b", "c")
        self.assertIn("500", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_connection_error_raises(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        g = Graph("my-graph", api_key="cog_key")
        with self.assertRaises(ConnectionError) as ctx:
            g.put("a", "b", "c")
        self.assertIn("Cannot reach CogDB Cloud", str(ctx.exception))


class TestCloudNoOps(unittest.TestCase):
    """Test that lifecycle methods are safe no-ops in cloud mode."""

    def test_close_is_noop(self):
        g = Graph("my-graph", api_key="cog_key")
        g.close()  # Should not raise

    def test_sync_without_pending_is_safe(self):
        g = Graph("my-graph", api_key="cog_key")
        g.sync()  # Should not raise with no pending mutations

    def test_refresh_is_noop(self):
        g = Graph("my-graph", api_key="cog_key")
        g.refresh()  # Should not raise

    def test_filter_raises_clear_error(self):
        g = Graph("my-graph", api_key="cog_key")
        g.v("alice")  # start a chain
        with self.assertRaises(RuntimeError) as ctx:
            g.filter(lambda x: True)
        self.assertIn("cloud mode", str(ctx.exception))


class TestCloudDropTruncate(unittest.TestCase):
    """Test drop() and truncate() in cloud mode."""

    @patch("urllib.request.urlopen")
    def test_drop_sends_mutate(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 1})
        g = Graph("my-graph", api_key="cog_key")
        g.drop()

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(body["mutations"][0]["op"], "DROP")

    @patch("urllib.request.urlopen")
    def test_truncate_sends_mutate(self, mock_urlopen):
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 1})
        g = Graph("my-graph", api_key="cog_key")
        result = g.truncate()

        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(body["mutations"][0]["op"], "TRUNCATE")
        self.assertIs(result, g)  # returns self for chaining


class TestCloudUrl(unittest.TestCase):
    """Test that cloud URL is set from config."""

    def test_cloud_url_from_config(self):
        g = Graph("my-graph", api_key="cog_key")
        self.assertIn("https://api.cogdb.io", g._cloud_client._base_url)


class TestCloudFlushInterval(unittest.TestCase):
    """Test that flush_interval controls write batching in cloud mode."""

    @patch("urllib.request.urlopen")
    def test_default_flush_interval_sends_immediately(self, mock_urlopen):
        """With flush_interval=1 (default), each put() sends immediately."""
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 1})
        g = Graph("my-graph", api_key="cog_key")
        g.put("alice", "knows", "bob")
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_high_flush_interval_buffers_writes(self, mock_urlopen):
        """With flush_interval > count, writes are buffered until sync()."""
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 3})
        g = Graph("my-graph", api_key="cog_key", flush_interval=10)
        g.put("alice", "knows", "bob")
        g.put("bob", "knows", "charlie")
        g.put("charlie", "knows", "alice")
        mock_urlopen.assert_not_called()
        # sync() flushes all pending
        g.sync()
        mock_urlopen.assert_called_once()
        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(len(body["mutations"]), 3)

    @patch("urllib.request.urlopen")
    def test_auto_flush_on_threshold(self, mock_urlopen):
        """Buffer auto-flushes when flush_interval threshold is reached."""
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 2})
        g = Graph("my-graph", api_key="cog_key", flush_interval=2)
        g.put("alice", "knows", "bob")
        mock_urlopen.assert_not_called()
        g.put("bob", "knows", "charlie")  # triggers auto-flush
        mock_urlopen.assert_called_once()
        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(len(body["mutations"]), 2)

    @patch("urllib.request.urlopen")
    def test_close_flushes_pending(self, mock_urlopen):
        """close() flushes any pending mutations."""
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 1})
        g = Graph("my-graph", api_key="cog_key", flush_interval=10)
        g.put("alice", "knows", "bob")
        mock_urlopen.assert_not_called()
        g.close()
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_query_flushes_pending(self, mock_urlopen):
        """Queries flush pending mutations for read-your-writes consistency."""
        flush_resp = _mock_response({"ok": True, "count": 1})
        query_resp = _mock_response({"ok": True, "result": [{"id": "bob"}]})
        mock_urlopen.side_effect = [flush_resp, query_resp]
        g = Graph("my-graph", api_key="cog_key", flush_interval=10)
        g.put("alice", "knows", "bob")
        mock_urlopen.assert_not_called()
        g.v("alice").out("knows").all()
        self.assertEqual(mock_urlopen.call_count, 2)

    @patch("urllib.request.urlopen")
    def test_flush_interval_zero_manual_only(self, mock_urlopen):
        """With flush_interval=0, writes only send on explicit sync()."""
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 3})
        g = Graph("my-graph", api_key="cog_key", flush_interval=0)
        g.put("a", "b", "c")
        g.put("d", "e", "f")
        g.put("g", "h", "i")
        mock_urlopen.assert_not_called()
        g.sync()
        mock_urlopen.assert_called_once()
        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(len(body["mutations"]), 3)

    @patch("urllib.request.urlopen")
    def test_delete_uses_buffer(self, mock_urlopen):
        """delete() also goes through the buffer."""
        mock_urlopen.return_value = _mock_response({"ok": True, "count": 2})
        g = Graph("my-graph", api_key="cog_key", flush_interval=10)
        g.put("alice", "knows", "bob")
        g.delete("alice", "knows", "bob")
        mock_urlopen.assert_not_called()
        g.sync()
        mock_urlopen.assert_called_once()
        body = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
        self.assertEqual(len(body["mutations"]), 2)
        self.assertEqual(body["mutations"][0]["op"], "PUT")
        self.assertEqual(body["mutations"][1]["op"], "DELETE")


if __name__ == '__main__':
    unittest.main()
