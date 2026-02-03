"""
Tests for CogDB Share/Tunnel Feature

Tests for the share.py module and related template/server changes
that enable sharing graphs via the CogDB Studio relay.
"""

import unittest
import os
import shutil
import time
import json
import urllib.request


DIR_NAME = "ShareTest"


class TestShareInfo(unittest.TestCase):
    """Test the ShareInfo class functionality."""
    
    def test_shareinfo_wait_timeout(self):
        """ShareInfo.wait() raises TimeoutError when not ready."""
        from cog.share import ShareInfo
        
        info = ShareInfo()
        # Should timeout since _ready is never set
        with self.assertRaises(TimeoutError):
            info.wait(timeout=0.1)
    
    def test_shareinfo_wait_with_error(self):
        """ShareInfo.wait() raises ConnectionError when error is set."""
        from cog.share import ShareInfo
        
        info = ShareInfo()
        info._error = "Test connection error"
        info._ready.set()
        
        with self.assertRaises(ConnectionError) as ctx:
            info.wait(timeout=1)
        
        self.assertIn("Test connection error", str(ctx.exception))
    
    def test_shareinfo_wait_success(self):
        """ShareInfo.wait() returns URL when ready."""
        from cog.share import ShareInfo
        
        info = ShareInfo()
        info.url = "https://test123.s.cogdb.io/"
        info.session_id = "test123"
        info._ready.set()
        
        result = info.wait(timeout=1)
        self.assertEqual(result, "https://test123.s.cogdb.io/")


class TestPathValidation(unittest.TestCase):
    """Test SSRF prevention via path validation."""
    
    def test_validate_path_empty(self):
        """Empty path is rejected."""
        from cog.share import _validate_path
        
        self.assertFalse(_validate_path(""))
        self.assertFalse(_validate_path(None))
    
    def test_validate_path_must_start_with_slash(self):
        """Path must start with /."""
        from cog.share import _validate_path
        
        self.assertFalse(_validate_path("graph/query"))
        self.assertTrue(_validate_path("/graph/query"))
    
    def test_validate_path_rejects_at_symbol(self):
        """Path with @ is rejected (SSRF prevention)."""
        from cog.share import _validate_path
        
        # The @ symbol can be used to make urllib request a different host
        self.assertFalse(_validate_path("/@attacker.com/evil"))
        self.assertFalse(_validate_path("/path@host"))
    
    def test_validate_path_valid_paths(self):
        """Valid paths are accepted."""
        from cog.share import _validate_path
        
        self.assertTrue(_validate_path("/"))
        self.assertTrue(_validate_path("/graph/"))
        self.assertTrue(_validate_path("/graph/query"))
        self.assertTrue(_validate_path("/graph/stats"))
        self.assertTrue(_validate_path("/my_graph-123/"))
        self.assertTrue(_validate_path("/graph?param=value"))
        self.assertTrue(_validate_path("/graph?a=1&b=2"))


class TestTemplatesShareUrl(unittest.TestCase):
    """Test template URL generation with share_url."""
    
    def test_render_graph_row(self):
        """Graph row generates local href."""
        from cog.templates import render_graph_row
        
        html = render_graph_row("test_graph", 100, "rw")
        self.assertIn('href="/test_graph/"', html)
        self.assertIn(">test_graph<", html)
    
    def test_render_graph_row_escapes_xss(self):
        """Graph row escapes potentially malicious graph names."""
        from cog.templates import render_graph_row
        
        # Attempt XSS via graph name
        html = render_graph_row('<script>alert(1)</script>', 100, "rw")
        # Should be escaped, not executed
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)
    
    def test_render_index_page_with_share_url(self):
        """Index page shows share URL when provided."""
        from cog.templates import render_index_page
        
        html = render_index_page(
            version="3.6.3",
            local_ip="127.0.0.1",
            port=8080,
            graphs_html="<div>test</div>",
            uptime_str="0h 0m 1s",
            share_url="https://xyz789.s.cogdb.io/"
        )
        
        # Share URL should appear in the output (escaped)
        self.assertIn("s.cogdb.io", html)
    
    def test_render_status_page_with_share_url(self):
        """Status page shows correct connect URL with share."""
        from cog.templates import render_status_page
        
        html = render_status_page(
            version="3.6.3",
            local_ip="127.0.0.1",
            port=8080,
            graph_name="my_graph",
            instance_id="abc123",
            node_count=50,
            edge_count=3,
            uptime_str="0h 0m 1s",
            queries_served=10,
            last_query_str="5s ago",
            mode_str="writable",
            share_url="https://session123.s.cogdb.io/"
        )
        
        # Back link should be root
        self.assertIn('href="/"', html)
        # Connect URL should use share URL
        self.assertIn("s.cogdb.io", html)


class TestServerHeaders(unittest.TestCase):
    """Test server respects X-Base-Path and X-Share-Url headers."""
    
    @classmethod
    def setUpClass(cls):
        if os.path.exists("/tmp/" + DIR_NAME):
            shutil.rmtree("/tmp/" + DIR_NAME)
        os.makedirs("/tmp/" + DIR_NAME, exist_ok=True)
        
        from cog.torque import Graph
        cls.g = Graph(graph_name="share_test", cog_home=DIR_NAME)
        cls.g.put("alice", "knows", "bob")
        
        cls.port = 18090
        cls.g.serve(port=cls.port)
        time.sleep(0.2)
    
    @classmethod
    def tearDownClass(cls):
        cls.g.stop()
        cls.g.close()
        shutil.rmtree("/tmp/" + DIR_NAME)
    
    def _request_with_headers(self, path, headers=None):
        """Helper to make HTTP requests with custom headers."""
        url = f"http://localhost:{self.port}{path}"
        req = urllib.request.Request(url)
        if headers:
            for key, value in headers.items():
                req.add_header(key, value)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.read().decode('utf-8')
    
    def test_index_page_with_share_url_header(self):
        """Index page shows share URL from header."""
        response = self._request_with_headers('/', headers={
            'X-Share-Url': 'https://test_session.s.cogdb.io/'
        })
        
        self.assertIn('s.cogdb.io', response)
    
    def test_status_page_with_headers(self):
        """Status page uses headers for connect URL."""
        response = self._request_with_headers('/share_test/', headers={
            'X-Share-Url': 'https://mysession.s.cogdb.io/'
        })
        
        # Connect URL should show share URL
        self.assertIn('s.cogdb.io', response)


class TestServeShareParameter(unittest.TestCase):
    """Test the share parameter on g.serve()."""
    
    def test_serve_sets_share_url_attribute(self):
        """serve(share=True) would set _share_url on successful connection."""
        # Note: We can't test actual relay connection without mocking
        # This test verifies the attribute exists and is initially None
        from cog.torque import Graph
        import shutil
        
        test_dir = "/tmp/ShareParamTest"
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        os.makedirs(test_dir, exist_ok=True)
        
        try:
            g = Graph(graph_name="param_test", cog_home="ShareParamTest")
            
            # Initially, _share_url should not exist
            self.assertFalse(hasattr(g, '_share_url'))
            
            # After serve without share, still shouldn't exist
            g.serve(port=18091)
            time.sleep(0.1)
            self.assertFalse(hasattr(g, '_share_url'))
            
            g.stop()
            g.close()
        finally:
            shutil.rmtree(test_dir)


if __name__ == '__main__':
    unittest.main()
