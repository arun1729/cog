"""
CogDB Remote Graph Client
=========================
Enables connecting to a remote CogDB server and querying it as if it were local.

Uses stdlib urllib.request for zero dependencies.
"""

import urllib.request
import urllib.error
import json
import ssl
from urllib.parse import urlparse

# Create SSL context for HTTPS support
# Use certifi if available (for proper HTTPS certificate verification)
try:
    import certifi
    _SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    # Fall back to system certificates
    _SSL_CONTEXT = ssl.create_default_context()


class RemoteGraph:
    """
    A proxy for a remote CogDB graph server.
    
    Provides the same traversal API as the local Graph class,
    but executes queries on a remote server via HTTP.
    
    Usage:
        remote = RemoteGraph("http://localhost:8080/my_graph")
        result = remote.v("alice").out("knows").all()
    """
    
    def __init__(self, url, timeout=30):
        """
        Initialize connection to a remote CogDB server.
        
        Args:
            url: URL including graph name path 
                 (e.g., "http://localhost:8080/my_graph")
                 or share URL (e.g., "https://abc123.s.cogdb.io/my_graph")
            timeout: Request timeout in seconds
        """
        parsed = urlparse(url)
        
        # Preserve the path for graph name extraction
        # e.g., /my_graph -> graph is my_graph
        path = parsed.path.rstrip('/')
        if not path:
            raise ValueError("URL must include graph name in path (e.g., http://localhost:8080/my_graph)")
        
        # Store the full path for building request URLs
        self._base_path = path
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"
        self.timeout = timeout
        self._query_parts = []
        
        # Extract graph name from the last path segment (for display/reference)
        self.graph_name = path.split('/')[-1]
    
    def _request(self, endpoint, data=None, method='GET'):
        """Make an HTTP request to the server."""
        # Use full base path + endpoint
        url = f"{self.base_url}{self._base_path}{endpoint}"
        
        if data is not None:
            data = json.dumps(data).encode('utf-8')
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header('Content-Type', 'application/json')
        else:
            req = urllib.request.Request(url, method=method)
        
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=_SSL_CONTEXT) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            try:
                error_data = json.loads(error_body)
                raise RuntimeError(error_data.get('error', str(e)))
            except json.JSONDecodeError:
                raise RuntimeError(str(e))
        except urllib.error.URLError as e:
            raise ConnectionError(f"Failed to connect to {self.base_url}/{self.graph_name}: {e.reason}")
    
    def _format_arg(self, arg):
        """Format an argument for the query string.
        
        Uses proper escaping to prevent injection attacks.
        """
        if arg is None:
            return 'None'
        elif isinstance(arg, str):
            # Use json.dumps for its string escaping (handles backslashes, quotes, etc.)
            # Not expecting JSON input - just leveraging its robust escape logic
            return json.dumps(arg)
        elif isinstance(arg, bool):
            return 'True' if arg else 'False'
        elif isinstance(arg, (int, float)):
            return str(arg)
        elif isinstance(arg, list):
            formatted = ', '.join(self._format_arg(a) for a in arg)
            return f"[{formatted}]"
        else:
            # Use json.dumps as a safe fallback
            return json.dumps(arg)
    
    def _add_method(self, method_name, *args, **kwargs):
        """Add a method call to the query chain (mutates self, like local Graph)."""
        # Format arguments
        arg_parts = [self._format_arg(a) for a in args]
        for k, v in kwargs.items():
            arg_parts.append(f"{k}={self._format_arg(v)}")
        
        arg_str = ', '.join(arg_parts)
        self._query_parts.append(f"{method_name}({arg_str})")
        return self
    
    def _execute(self):
        """Execute the current query chain and return results."""
        query = '.'.join(self._query_parts)
        
        # Clear query parts for reuse (like local Graph)
        self._query_parts = []
        
        response = self._request('/query', {'q': query}, method='POST')
        
        if not response.get('ok'):
            raise RuntimeError(response.get('error', 'Unknown error'))
        
        return {'result': response.get('result', [])}
    
    # === Vertex selection ===
    
    def v(self, vertex=None):
        """Select vertex/vertices to start traversal."""
        if vertex is None:
            return self._add_method('v')
        return self._add_method('v', vertex)
    
    # === Traversal methods ===
    
    def out(self, predicates=None):
        """Traverse outgoing edges."""
        if predicates is None:
            return self._add_method('out')
        return self._add_method('out', predicates)
    
    def inc(self, predicates=None):
        """Traverse incoming edges."""
        if predicates is None:
            return self._add_method('inc')
        return self._add_method('inc', predicates)
    
    def both(self, predicates=None):
        """Traverse edges in both directions."""
        if predicates is None:
            return self._add_method('both')
        return self._add_method('both', predicates)
    
    def has(self, predicates, vertex):
        """Filter vertices with outgoing edge to vertex."""
        return self._add_method('has', predicates, vertex)
    
    def hasr(self, predicates, vertex):
        """Filter vertices with incoming edge from vertex."""
        return self._add_method('hasr', predicates, vertex)
    
    # === Tagging and navigation ===
    
    def tag(self, tag_names):
        """Tag current vertices with one or more tag names."""
        return self._add_method('tag', tag_names)
    
    def back(self, tag):
        """Return to tagged vertices."""
        return self._add_method('back', tag)
    
    # === Filtering ===
    
    def is_(self, *nodes):
        """Filter to only specified nodes."""
        return self._add_method('is_', *nodes)
    
    def unique(self):
        """Remove duplicate vertices."""
        return self._add_method('unique')
    
    def limit(self, n):
        """Limit results to first N."""
        return self._add_method('limit', n)
    
    def skip(self, n):
        """Skip first N vertices."""
        return self._add_method('skip', n)
    
    def filter(self, func_str):
        """Filter vertices by a function string.
        Note: For remote execution, pass the function as a string."""
        return self._add_method('filter', func_str)
    
    # === Graph algorithms ===
    
    def bfs(self, predicates=None, max_depth=None, min_depth=0,
            direction="out", until=None, unique=True):
        """Breadth-first search traversal."""
        kwargs = {}
        if predicates is not None:
            kwargs['predicates'] = predicates
        if max_depth is not None:
            kwargs['max_depth'] = max_depth
        if min_depth != 0:
            kwargs['min_depth'] = min_depth
        if direction != "out":
            kwargs['direction'] = direction
        if unique != True:
            kwargs['unique'] = unique
        # Note: 'until' lambdas cannot be serialized, skip for remote
        return self._add_method('bfs', **kwargs)
    
    def dfs(self, predicates=None, max_depth=None, min_depth=0,
            direction="out", until=None, unique=True):
        """Depth-first search traversal."""
        kwargs = {}
        if predicates is not None:
            kwargs['predicates'] = predicates
        if max_depth is not None:
            kwargs['max_depth'] = max_depth
        if min_depth != 0:
            kwargs['min_depth'] = min_depth
        if direction != "out":
            kwargs['direction'] = direction
        if unique != True:
            kwargs['unique'] = unique
        return self._add_method('dfs', **kwargs)
    
    # === Terminal methods ===
    
    def all(self, options=None):
        """Execute query and return all results."""
        if options:
            self._query_parts.append(f"all('{options}')")
        else:
            self._query_parts.append("all()")
        return self._execute()
    
    def count(self):
        """Execute query and return count."""
        self._query_parts.append("count()")
        result = self._execute()
        # count() returns {'result': N} or just N
        r = result.get('result', 0)
        return r if isinstance(r, int) else 0
    
    def first(self):
        """Execute query and return first result."""
        self._query_parts.append("first()")
        return self._execute()
    
    def one(self):
        """Execute query and return exactly one result."""
        self._query_parts.append("one()")
        return self._execute()
    
    def scan(self, limit=10, scan_type='v'):
        """Scan vertices or edges."""
        return self._add_method('scan', limit, scan_type)._execute()
    
    # === Write operations ===
    
    def put(self, subject, predicate, obj):
        """Insert a triple (requires writable server)."""
        response = self._request('/mutate', {
            'op': 'put',
            'args': [subject, predicate, obj]
        }, method='POST')
        
        if not response.get('ok'):
            raise RuntimeError(response.get('error', 'Write failed'))
        return self
    
    def put_batch(self, triples):
        """Insert multiple triples (requires writable server)."""
        response = self._request('/mutate', {
            'op': 'put_batch',
            'args': triples
        }, method='POST')
        
        if not response.get('ok'):
            raise RuntimeError(response.get('error', 'Write failed'))
        return self
    
    def drop(self, subject, predicate, obj):
        """Delete a triple (requires writable server)."""
        response = self._request('/mutate', {
            'op': 'delete',
            'args': [subject, predicate, obj]
        }, method='POST')
        
        if not response.get('ok'):
            raise RuntimeError(response.get('error', 'Delete failed'))
        return self
    
    # === Stats ===
    
    def stats(self):
        """Get server statistics."""
        return self._request('/stats')
