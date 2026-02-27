"""Tests for g.vectorize() auto-embedding feature."""
from cog.torque import Graph
from cog.embedding_providers import _provider_cogdb, _provider_openai, _provider_custom
import unittest
import os
import shutil
import json
from unittest.mock import patch, MagicMock
from io import BytesIO

DIR_NAME = "TestVectorize"


def _make_mock_cogdb_response(texts):
    """Build a CogDB-format response for given texts."""
    embeddings = [{"text": t, "vector": [0.1 * (i + 1)] * 8} for i, t in enumerate(texts)]
    body = json.dumps({"embeddings": embeddings}).encode("utf-8")
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def _make_mock_openai_response(texts):
    """Build an OpenAI-format response for given texts."""
    data = [{"index": i, "embedding": [0.2 * (i + 1)] * 8} for i in range(len(texts))]
    body = json.dumps({"data": data, "model": "text-embedding-3-small"}).encode("utf-8")
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


class TestVectorize(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not os.path.exists("/tmp/" + DIR_NAME):
            os.mkdir("/tmp/" + DIR_NAME)
        cls.g = Graph(graph_name="vec_test", cog_home=DIR_NAME)
        cls.g.put("alice", "knows", "bob")
        cls.g.put("bob", "knows", "charlie")
        cls.g.put("charlie", "knows", "dave")

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_default_provider(self, mock_urlopen):
        """vectorize() with default CogDB provider embeds all nodes."""
        # Capture what texts are sent and return mock response
        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        result = self.g.vectorize()

        self.assertIn("vectorized", result)
        self.assertIn("skipped", result)
        self.assertIn("total", result)
        self.assertEqual(result["total"], result["vectorized"] + result["skipped"])
        self.assertGreater(result["vectorized"], 0)

        # Verify embeddings were actually stored
        for node in ["alice", "bob", "charlie", "dave"]:
            emb = self.g.get_embedding(node)
            self.assertIsNotNone(emb, f"{node} should have an embedding")

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_skips_existing(self, mock_urlopen):
        """vectorize() skips nodes that already have embeddings."""
        def side_effect(req):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        # All nodes already have embeddings from the previous test
        result = self.g.vectorize()

        self.assertEqual(result["vectorized"], 0)
        self.assertEqual(result["skipped"], result["total"])
        # urlopen should NOT have been called since everything was skipped
        mock_urlopen.assert_not_called()

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_openai_provider(self, mock_urlopen):
        """vectorize() with OpenAI provider maps by index correctly."""
        # Use a fresh graph so no pre-existing embeddings
        g2 = Graph(graph_name="vec_openai", cog_home=DIR_NAME)
        g2.put("earth", "orbits", "sun")
        g2.put("mars", "orbits", "sun")

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_openai_response(body["input"])

        mock_urlopen.side_effect = side_effect

        result = g2.vectorize(provider="openai", api_key="sk-test-key")

        self.assertGreater(result["vectorized"], 0)
        for node in ["earth", "mars", "sun"]:
            self.assertIsNotNone(g2.get_embedding(node))

        # Verify Authorization header was set and URL is always api.openai.com
        call_args = mock_urlopen.call_args_list[0]
        req_obj = call_args[0][0]
        self.assertIn("Bearer sk-test-key", req_obj.get_header("Authorization"))
        self.assertEqual(req_obj.full_url, "https://api.openai.com/v1/embeddings")

        g2.close()

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_custom_provider(self, mock_urlopen):
        """vectorize() with custom provider uses the given URL."""
        g3 = Graph(graph_name="vec_custom", cog_home=DIR_NAME)
        g3.put("node_a", "rel", "node_b")

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        result = g3.vectorize(provider="custom", url="https://my-endpoint.com/embed")

        self.assertGreater(result["vectorized"], 0)

        # Verify the custom URL was used
        call_args = mock_urlopen.call_args_list[0]
        req_obj = call_args[0][0]
        self.assertEqual(req_obj.full_url, "https://my-endpoint.com/embed")

        g3.close()

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_batching(self, mock_urlopen):
        """vectorize() chunks large node sets into batches."""
        g4 = Graph(graph_name="vec_batch", cog_home=DIR_NAME)
        # Create 15 nodes
        for i in range(15):
            g4.put("n{}".format(i), "rel", "n{}".format(i + 1))

        call_count = [0]

        def side_effect(req, **kwargs):
            call_count[0] += 1
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        # Use batch_size=5 so 16 unique nodes / 5 = 4 batches (3 full + 1 partial)
        result = g4.vectorize(batch_size=5)

        self.assertGreater(result["vectorized"], 0)
        # Should have made multiple calls
        self.assertGreater(call_count[0], 1)

        g4.close()

    def test_vectorize_empty_graph(self):
        """vectorize() on empty graph returns zeros without calling provider."""
        g5 = Graph(graph_name="vec_empty", cog_home=DIR_NAME)

        result = g5.vectorize()

        self.assertEqual(result, {"vectorized": 0, "skipped": 0, "total": 0})
        g5.close()

    def test_vectorize_unknown_provider(self):
        """vectorize() raises ValueError for unknown provider."""
        with self.assertRaises(ValueError) as ctx:
            self.g.vectorize(provider="nonexistent")
        self.assertIn("nonexistent", str(ctx.exception))

    def test_openai_provider_requires_api_key(self):
        """OpenAI provider raises ValueError without api_key."""
        with self.assertRaises(ValueError):
            _provider_openai(["test"])

    def test_custom_provider_requires_url(self):
        """Custom provider raises ValueError without url."""
        with self.assertRaises(ValueError):
            _provider_custom(["test"])

    def test_vectorize_invalid_batch_size(self):
        """vectorize() raises ValueError for non-positive batch_size."""
        with self.assertRaises(ValueError):
            self.g.vectorize(batch_size=0)
        with self.assertRaises(ValueError):
            self.g.vectorize(batch_size=-1)

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_error_handling(self, mock_urlopen):
        """vectorize() continues on batch failure and reports errors."""
        g6 = Graph(graph_name="vec_errors", cog_home=DIR_NAME)
        g6.put("x", "rel", "y")
        g6.put("y", "rel", "z")

        mock_urlopen.side_effect = Exception("connection refused")

        result = g6.vectorize(batch_size=1)

        self.assertEqual(result["vectorized"], 0)
        self.assertIn("errors", result)
        self.assertGreater(len(result["errors"]), 0)

        g6.close()

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_single_word(self, mock_urlopen):
        """vectorize('word') embeds just that word."""
        g7 = Graph(graph_name="vec_word", cog_home=DIR_NAME)
        g7.put("alpha", "rel", "beta")

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        result = g7.vectorize("alpha")

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["vectorized"], 1)
        self.assertIsNotNone(g7.get_embedding("alpha"))
        self.assertIsNone(g7.get_embedding("beta"))

        g7.close()

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_word_list(self, mock_urlopen):
        """vectorize(['a', 'b']) embeds just those words."""
        g8 = Graph(graph_name="vec_list", cog_home=DIR_NAME)
        g8.put("one", "rel", "two")
        g8.put("two", "rel", "three")

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        result = g8.vectorize(["one", "two"])

        self.assertEqual(result["total"], 2)
        self.assertEqual(result["vectorized"], 2)
        self.assertIsNotNone(g8.get_embedding("one"))
        self.assertIsNotNone(g8.get_embedding("two"))
        self.assertIsNone(g8.get_embedding("three"))

        g8.close()

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_arbitrary_word(self, mock_urlopen):
        """vectorize('word') works for words not in the graph."""
        g9 = Graph(graph_name="vec_arb", cog_home=DIR_NAME)

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        result = g9.vectorize("new_concept")

        self.assertEqual(result["vectorized"], 1)
        self.assertIsNotNone(g9.get_embedding("new_concept"))

        g9.close()

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_auto_embed_in_k_nearest(self, mock_urlopen):
        """k_nearest auto-embeds the query word if missing."""
        g10 = Graph(graph_name="vec_auto", cog_home=DIR_NAME)
        g10.put("cat", "isa", "animal")
        g10.put("dog", "isa", "animal")

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        # First vectorize the graph nodes
        g10.vectorize()

        # Reset mock to track auto-embed call
        mock_urlopen.reset_mock()
        mock_urlopen.side_effect = side_effect

        # k_nearest with a word not in graph — should auto-embed "pet"
        result = g10.v().k_nearest("pet", k=2).all()

        # urlopen should have been called for auto-embed
        self.assertTrue(mock_urlopen.called)
        # "pet" should now have an embedding
        self.assertIsNotNone(g10.get_embedding("pet"))

        g10.close()

    @patch("cog.embedding_providers.urllib.request.urlopen")
    def test_vectorize_stores_provider_config(self, mock_urlopen):
        """vectorize() stores provider config for auto-embed."""
        g11 = Graph(graph_name="vec_cfg", cog_home=DIR_NAME)

        def side_effect(req, **kwargs):
            body = json.loads(req.data.decode("utf-8"))
            return _make_mock_cogdb_response(body["texts"])

        mock_urlopen.side_effect = side_effect

        g11.vectorize(provider="cogdb")
        self.assertEqual(g11._default_provider, "cogdb")

        g11.close()

    @classmethod
    def tearDownClass(cls):
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)
        print("*** deleted test data.")


if __name__ == "__main__":
    unittest.main()
