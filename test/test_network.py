"""
Tests for CogDB Network Feature

Tests server endpoints and RemoteGraph round-trip.
Now with multi-graph support on same port.
"""

import unittest
import os
import shutil
import time
import json
import urllib.request
import urllib.error
from cog.torque import Graph


DIR_NAME = "NetworkTest"


class TestCogDBServer(unittest.TestCase):
    """Test server endpoints directly via urllib."""
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)
        os.makedirs("/tmp/" + DIR_NAME, exist_ok=True)
        
        cls.g = Graph(graph_name="test_network", cog_home=DIR_NAME)
        cls.g.put("alice", "knows", "bob")
        cls.g.put("bob", "knows", "charlie")
        cls.g.put("charlie", "knows", "alice")
        cls.g.put("alice", "likes", "pizza")
        
        # Start server on a random high port to avoid conflicts
        cls.port = 18080
        cls.g.serve(port=cls.port, writable=True)
        time.sleep(0.2)  # Give server time to start
    
    @classmethod
    def tearDownClass(cls):
        cls.g.stop()
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)
    
    def _request(self, path, data=None, method='GET'):
        """Helper to make HTTP requests."""
        url = f"http://localhost:{self.port}{path}"
        if data:
            data = json.dumps(data).encode('utf-8')
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header('Content-Type', 'application/json')
        else:
            req = urllib.request.Request(url, method=method)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.read().decode('utf-8')
    
    def test_index_page(self):
        """GET / returns index page listing graphs."""
        response = self._request('/')
        self.assertIn('CogDB', response)
        self.assertIn('ONLINE', response)
        self.assertIn('test_network', response)
        self.assertIn('Available Graphs', response)
    
    def test_graph_status_page(self):
        """GET /{graph}/ returns status page for that graph."""
        response = self._request('/test_network/')
        self.assertIn('CogDB', response)
        self.assertIn('test_network', response)
        self.assertIn('Nodes', response)
    
    def test_stats_endpoint(self):
        """GET /{graph}/stats returns JSON statistics."""
        response = json.loads(self._request('/test_network/stats'))
        self.assertEqual(response['graph_name'], 'test_network')
        self.assertIn('nodes', response)
        self.assertIn('uptime_seconds', response)
        self.assertTrue(response['writable'])
    
    def test_query_simple(self):
        """POST /{graph}/query executes simple queries."""
        response = json.loads(self._request('/test_network/query', {'q': "v('alice').out('knows').all()"}, 'POST'))
        self.assertTrue(response['ok'])
        self.assertEqual(len(response['result']), 1)
        self.assertEqual(response['result'][0]['id'], 'bob')
    
    def test_query_count(self):
        """POST /{graph}/query handles count() queries."""
        response = json.loads(self._request('/test_network/query', {'q': "v().count()"}, 'POST'))
        self.assertTrue(response['ok'])
        # Should have at least alice, bob, charlie, pizza = 4 nodes
        self.assertGreaterEqual(response['result'], 4)
    
    def test_query_chained(self):
        """POST /{graph}/query handles chained traversals."""
        response = json.loads(self._request('/test_network/query', {'q': "v('alice').out('knows').out('knows').all()"}, 'POST'))
        self.assertTrue(response['ok'])
        self.assertEqual(len(response['result']), 1)
        self.assertEqual(response['result'][0]['id'], 'charlie')
    
    def test_query_invalid(self):
        """POST /{graph}/query rejects invalid queries."""
        try:
            self._request('/test_network/query', {'q': "import os"}, 'POST')
            self.fail("Should have raised an error")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)
    
    def test_mutate_put(self):
        """POST /{graph}/mutate can insert triples."""
        response = json.loads(self._request('/test_network/mutate', {
            'op': 'put',
            'args': ['david', 'knows', 'eve']
        }, 'POST'))
        self.assertTrue(response['ok'])
        self.assertEqual(response['affected'], 1)
        
        # Verify it was inserted
        verify = json.loads(self._request('/test_network/query', {'q': "v('david').out('knows').all()"}, 'POST'))
        self.assertEqual(verify['result'][0]['id'], 'eve')
    
    def test_mutate_delete(self):
        """POST /{graph}/mutate can delete triples."""
        # First insert
        self._request('/test_network/mutate', {'op': 'put', 'args': ['temp', 'edge', 'node']}, 'POST')
        
        # Then delete
        response = json.loads(self._request('/test_network/mutate', {
            'op': 'delete',
            'args': ['temp', 'edge', 'node']
        }, 'POST'))
        self.assertTrue(response['ok'])
    
    def test_graph_not_found(self):
        """Requests to nonexistent graph return 404."""
        try:
            self._request('/nonexistent/stats')
            self.fail("Should have raised 404")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 404)


class TestMultiGraph(unittest.TestCase):
    """Test multiple graphs on same port."""
    
    @classmethod
    def setUpClass(cls):
        for suffix in ['4a', '4b']:
            path = f"/tmp/{DIR_NAME}{suffix}"
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path, exist_ok=True)
        
        cls.g1 = Graph(graph_name="social", cog_home=DIR_NAME + "4a")
        cls.g1.put("alice", "knows", "bob")
        
        cls.g2 = Graph(graph_name="products", cog_home=DIR_NAME + "4b")
        cls.g2.put("laptop", "category", "electronics")
        
        cls.port = 18083
        cls.g1.serve(port=cls.port, writable=True)
        cls.g2.serve(port=cls.port, writable=True)  # Same port!
        time.sleep(0.2)
    
    @classmethod
    def tearDownClass(cls):
        cls.g1.stop()
        cls.g2.stop()
        cls.g1.close()
        cls.g2.close()
        for suffix in ['4a', '4b']:
            shutil.rmtree(f"/tmp/{DIR_NAME}{suffix}")
    
    def _request(self, path, data=None, method='GET'):
        """Helper to make HTTP requests."""
        url = f"http://localhost:{self.port}{path}"
        if data:
            data = json.dumps(data).encode('utf-8')
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header('Content-Type', 'application/json')
        else:
            req = urllib.request.Request(url, method=method)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.read().decode('utf-8')
    
    def test_index_lists_both_graphs(self):
        """Index page shows both graphs."""
        response = self._request('/')
        self.assertIn('social', response)
        self.assertIn('products', response)
    
    def test_query_different_graphs(self):
        """Can query each graph separately."""
        # Query social graph
        r1 = json.loads(self._request('/social/query', {'q': "v('alice').out('knows').all()"}, 'POST'))
        self.assertTrue(r1['ok'])
        self.assertEqual(r1['result'][0]['id'], 'bob')
        
        # Query products graph
        r2 = json.loads(self._request('/products/query', {'q': "v('laptop').out('category').all()"}, 'POST'))
        self.assertTrue(r2['ok'])
        self.assertEqual(r2['result'][0]['id'], 'electronics')
    
    def test_stats_different_graphs(self):
        """Stats endpoint works for each graph."""
        s1 = json.loads(self._request('/social/stats'))
        s2 = json.loads(self._request('/products/stats'))
        
        self.assertEqual(s1['graph_name'], 'social')
        self.assertEqual(s2['graph_name'], 'products')


class TestRemoteGraph(unittest.TestCase):
    """Test RemoteGraph proxy connects and queries correctly."""
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME + "2"):
            shutil.rmtree("/tmp/" + DIR_NAME + "2")
        os.makedirs("/tmp/" + DIR_NAME + "2", exist_ok=True)
        
        cls.g = Graph(graph_name="remote_test", cog_home=DIR_NAME + "2")
        cls.g.put("alice", "knows", "bob")
        cls.g.put("bob", "knows", "charlie")
        cls.g.put("charlie", "knows", "alice")
        
        cls.port = 18081
        cls.g.serve(port=cls.port, writable=True)
        time.sleep(0.2)
        
        # Connect via Graph.connect() - now requires graph name in path
        cls.remote = Graph.connect(f"http://localhost:{cls.port}/remote_test")
    
    @classmethod
    def tearDownClass(cls):
        cls.g.stop()
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME + "2")
    
    def test_connect_and_query(self):
        """Graph.connect() returns working RemoteGraph."""
        result = self.remote.v("alice").out("knows").all()
        self.assertIn('result', result)
        self.assertEqual(len(result['result']), 1)
        self.assertEqual(result['result'][0]['id'], 'bob')
    
    def test_count(self):
        """RemoteGraph count() works."""
        count = self.remote.v().count()
        self.assertEqual(count, 3)  # alice, bob, charlie
    
    def test_chained_traversal(self):
        """RemoteGraph handles chained traversals."""
        result = self.remote.v("alice").out("knows").out("knows").all()
        self.assertEqual(result['result'][0]['id'], 'charlie')
    
    def test_inc_traversal(self):
        """RemoteGraph inc() works."""
        result = self.remote.v("bob").inc("knows").all()
        self.assertEqual(result['result'][0]['id'], 'alice')
    
    def test_remote_write(self):
        """RemoteGraph can write when server is writable."""
        self.remote.put("new_node", "edge", "target")
        
        # Verify via local graph
        result = self.g.v("new_node").out("edge").all()
        self.assertEqual(result['result'][0]['id'], 'target')
    
    def test_stats(self):
        """RemoteGraph stats() returns server info."""
        stats = self.remote.stats()
        self.assertEqual(stats['graph_name'], 'remote_test')
        self.assertIn('nodes', stats)
    
    def test_connect_without_graph_name_fails(self):
        """Graph.connect() without graph name in path raises error."""
        with self.assertRaises(ValueError):
            Graph.connect(f"http://localhost:{self.port}")
    
    def test_limit(self):
        """RemoteGraph limit() works."""
        result = self.remote.v().limit(2).all()
        self.assertIn('result', result)
        self.assertLessEqual(len(result['result']), 2)
    
    def test_skip(self):
        """RemoteGraph skip() works."""
        # Get all results first
        all_results = self.remote.v().all()
        # Skip some
        skipped = self.remote.v().skip(1).all()
        self.assertLess(len(skipped['result']), len(all_results['result']))
    
    def test_unique(self):
        """RemoteGraph unique() works."""
        result = self.remote.v().unique().all()
        self.assertIn('result', result)
        # Check all IDs are unique
        ids = [r['id'] for r in result['result']]
        self.assertEqual(len(ids), len(set(ids)))
    
    def test_has(self):
        """RemoteGraph has() works."""
        result = self.remote.v("alice").has("knows", "bob").all()
        self.assertIn('result', result)
    
    def test_both(self):
        """RemoteGraph both() works."""
        result = self.remote.v("bob").both("knows").all()
        self.assertIn('result', result)
        # Bob should have both incoming and outgoing 'knows' edges
    
    def test_bfs(self):
        """RemoteGraph bfs() works."""
        result = self.remote.v("alice").bfs("knows", max_depth=2).all()
        self.assertIn('result', result)
    
    def test_dfs(self):
        """RemoteGraph dfs() works."""
        result = self.remote.v("alice").dfs("knows", max_depth=2).all()
        self.assertIn('result', result)
    
    def test_delete(self):
        """RemoteGraph delete() works."""
        # Add a node we can delete
        self.remote.put("del_test", "temp", "del_target")
        # Delete it using delete()
        self.remote.delete("del_test", "temp", "del_target")
        # Verify via local graph - should not find the edge
    
    def test_scan(self):
        """RemoteGraph scan() works."""
        result = self.remote.scan()
        self.assertIn('result', result)
    
    def test_remote_drop_raises_not_implemented(self):
        """RemoteGraph drop() with no args raises NotImplementedError."""
        with self.assertRaises(NotImplementedError) as ctx:
            self.remote.drop()
        self.assertIn("not supported", str(ctx.exception).lower())
    
    def test_remote_drop_with_args_raises_deprecation(self):
        """RemoteGraph drop(s, p, o) raises DeprecationWarning."""
        with self.assertRaises(DeprecationWarning) as ctx:
            self.remote.drop("a", "b", "c")
        self.assertIn("deprecated", str(ctx.exception).lower())
    
    def test_remote_truncate(self):
        """RemoteGraph truncate() clears all data."""
        # Insert some data first
        self.remote.put("trunc_a", "rel", "trunc_b")
        
        # Verify it exists via local graph
        result = self.g.v("trunc_a").out("rel").all()
        self.assertEqual(len(result['result']), 1)
        
        # Truncate via remote
        ret = self.remote.truncate()
        self.assertIs(ret, self.remote)  # Returns self for chaining
        
        # Verify data is gone via local graph (re-check)
        result = self.g.v("trunc_a").out("rel").all()
        self.assertEqual(len(result['result']), 0)
    
    def test_remote_put_batch(self):
        """RemoteGraph put_batch() inserts multiple triples."""
        triples = [
            ["batch_a", "rel", "batch_b"],
            ["batch_c", "rel", "batch_d"],
        ]
        ret = self.remote.put_batch(triples)
        self.assertIs(ret, self.remote)
        
        # Verify via local graph
        result = self.g.v("batch_a").out("rel").all()
        self.assertEqual(result['result'][0]['id'], 'batch_b')
        result = self.g.v("batch_c").out("rel").all()
        self.assertEqual(result['result'][0]['id'], 'batch_d')


class TestServerMutationEdgeCases(unittest.TestCase):
    """Test server mutation handler edge cases."""
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME + "5"):
            shutil.rmtree("/tmp/" + DIR_NAME + "5")
        os.makedirs("/tmp/" + DIR_NAME + "5", exist_ok=True)
        
        cls.g = Graph(graph_name="mutate_test", cog_home=DIR_NAME + "5")
        cls.g.put("x", "y", "z")
        
        cls.port = 18084
        cls.g.serve(port=cls.port, writable=True)
        time.sleep(0.2)
    
    @classmethod
    def tearDownClass(cls):
        cls.g.stop()
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME + "5")
    
    def _request(self, path, data=None, method='GET'):
        url = f"http://localhost:{self.port}{path}"
        if data:
            data = json.dumps(data).encode('utf-8')
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header('Content-Type', 'application/json')
        else:
            req = urllib.request.Request(url, method=method)
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.read().decode('utf-8')
    
    def test_mutate_truncate_via_http(self):
        """POST mutate with op=truncate clears the graph."""
        # Insert data
        self._request('/mutate_test/mutate', {'op': 'put', 'args': ['t1', 'r', 't2']}, 'POST')
        
        # Truncate
        response = json.loads(self._request('/mutate_test/mutate', {'op': 'truncate', 'args': []}, 'POST'))
        self.assertTrue(response['ok'])
        self.assertIn('truncated', response.get('message', '').lower())
    
    def test_mutate_unknown_op_returns_400(self):
        """POST mutate with unknown op returns error."""
        try:
            self._request('/mutate_test/mutate', {'op': 'explode', 'args': []}, 'POST')
            self.fail("Should have raised 400")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)
            body = json.loads(e.read().decode('utf-8'))
            self.assertIn('Unknown operation', body.get('error', ''))
    
    def test_mutate_delete_validation(self):
        """POST mutate delete with wrong number of args returns error."""
        try:
            self._request('/mutate_test/mutate', {'op': 'delete', 'args': ['only_one']}, 'POST')
            self.fail("Should have raised 400")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)


class TestReadOnlyServer(unittest.TestCase):
    """Test server in read-only mode rejects writes."""
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME + "3"):
            shutil.rmtree("/tmp/" + DIR_NAME + "3")
        os.makedirs("/tmp/" + DIR_NAME + "3", exist_ok=True)
        
        cls.g = Graph(graph_name="readonly_test", cog_home=DIR_NAME + "3")
        cls.g.put("test", "edge", "node")
        
        cls.port = 18082
        cls.g.serve(port=cls.port, writable=False)  # Read-only
        time.sleep(0.2)
    
    @classmethod
    def tearDownClass(cls):
        cls.g.stop()
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME + "3")
    
    def test_mutate_rejected(self):
        """Read-only server rejects write operations."""
        try:
            url = f"http://localhost:{self.port}/readonly_test/mutate"
            data = json.dumps({'op': 'put', 'args': ['a', 'b', 'c']}).encode('utf-8')
            req = urllib.request.Request(url, data=data, method='POST')
            req.add_header('Content-Type', 'application/json')
            urllib.request.urlopen(req, timeout=5)
            self.fail("Should have raised 403")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 403)


class TestRemoteGraphErrorHandling(unittest.TestCase):
    """Test RemoteGraph error handling and edge cases."""
    
    def test_connection_error_unreachable_host(self):
        """RemoteGraph raises ConnectionError when server is unreachable."""
        from cog.remote import RemoteGraph
        
        # Connect to a port that should not be running
        remote = RemoteGraph("http://localhost:59999/nonexistent", timeout=1)
        
        with self.assertRaises(ConnectionError) as ctx:
            remote.v().all()
        
        # Check error message contains connection info
        self.assertIn("Failed to connect", str(ctx.exception))
        self.assertIn("localhost", str(ctx.exception))
    
    def test_http_error_with_json_body(self):
        """RemoteGraph extracts error message from JSON error response."""
        import urllib.request
        import urllib.error
        from unittest.mock import patch, MagicMock
        from cog.remote import RemoteGraph
        
        remote = RemoteGraph("http://localhost:8080/test_graph")
        
        # Mock an HTTPError with JSON error body
        mock_error = urllib.error.HTTPError(
            url="http://localhost:8080/test_graph/query",
            code=400,
            msg="Bad Request",
            hdrs={},
            fp=None
        )
        mock_error.read = MagicMock(return_value=b'{"error": "Custom error message from server"}')
        
        with patch('urllib.request.urlopen', side_effect=mock_error):
            with self.assertRaises(RuntimeError) as ctx:
                remote.v().all()
            
            # Should extract error message from JSON
            self.assertEqual(str(ctx.exception), "Custom error message from server")
    
    def test_http_error_with_non_json_body(self):
        """RemoteGraph handles non-JSON error responses."""
        import urllib.request
        import urllib.error
        from unittest.mock import patch, MagicMock
        from cog.remote import RemoteGraph
        
        remote = RemoteGraph("http://localhost:8080/test_graph")
        
        # Mock an HTTPError with non-JSON body
        mock_error = urllib.error.HTTPError(
            url="http://localhost:8080/test_graph/query",
            code=500,
            msg="Internal Server Error",
            hdrs={},
            fp=None
        )
        mock_error.read = MagicMock(return_value=b'This is not JSON')
        
        with patch('urllib.request.urlopen', side_effect=mock_error):
            with self.assertRaises(RuntimeError) as ctx:
                remote.v().all()
            
            # Should fall back to HTTPError string representation
            self.assertIn("500", str(ctx.exception))


class TestRemoteGraphFormatArg(unittest.TestCase):
    """Test RemoteGraph._format_arg method for various types."""
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME + "4"):
            shutil.rmtree("/tmp/" + DIR_NAME + "4")
        os.makedirs("/tmp/" + DIR_NAME + "4", exist_ok=True)
        
        cls.g = Graph(graph_name="format_test", cog_home=DIR_NAME + "4")
        cls.g.put("alice", "knows", "bob")
        cls.g.put("bob", "knows", "charlie")
        cls.g.put("alice", "age", "30")
        
        cls.port = 18083
        cls.g.serve(port=cls.port, writable=True)
        time.sleep(0.2)
        
        cls.remote = Graph.connect(f"http://localhost:{cls.port}/format_test")
    
    @classmethod
    def tearDownClass(cls):
        cls.g.stop()
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME + "4")
    
    def test_format_arg_boolean_true(self):
        """_format_arg correctly formats True boolean."""
        from cog.remote import RemoteGraph
        remote = RemoteGraph("http://localhost:8080/test")
        
        result = remote._format_arg(True)
        self.assertEqual(result, "True")
    
    def test_format_arg_boolean_false(self):
        """_format_arg correctly formats False boolean."""
        from cog.remote import RemoteGraph
        remote = RemoteGraph("http://localhost:8080/test")
        
        result = remote._format_arg(False)
        self.assertEqual(result, "False")
    
    def test_format_arg_list(self):
        """_format_arg correctly formats lists."""
        from cog.remote import RemoteGraph
        remote = RemoteGraph("http://localhost:8080/test")
        
        result = remote._format_arg(["a", "b", "c"])
        self.assertEqual(result, '["a", "b", "c"]')
    
    def test_format_arg_list_mixed_types(self):
        """_format_arg correctly formats lists with mixed types."""
        from cog.remote import RemoteGraph
        remote = RemoteGraph("http://localhost:8080/test")
        
        result = remote._format_arg(["text", 42, True])
        self.assertEqual(result, '["text", 42, True]')
    
    def test_format_arg_dict_fallback(self):
        """_format_arg uses json.dumps fallback for dicts."""
        from cog.remote import RemoteGraph
        remote = RemoteGraph("http://localhost:8080/test")
        
        result = remote._format_arg({"key": "value"})
        # Should use json.dumps for dicts
        self.assertEqual(result, '{"key": "value"}')
    
    def test_query_with_boolean_param(self):
        """RemoteGraph handles boolean parameters in queries."""
        # Use unique=False to trigger boolean formatting
        result = self.remote.v("alice").out("knows").dfs(unique=False).all()
        self.assertIn('result', result)
    
    def test_query_with_list_param(self):
        """RemoteGraph handles list parameters in queries."""
        # Use is_() with list of nodes to filter
        result = self.remote.v().is_("alice", "bob").all()
        self.assertIn('result', result)


if __name__ == '__main__':
    unittest.main()
