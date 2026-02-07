"""
CogDB HTTP Server
=================
Enables serving Graph instances over HTTP for remote querying.
Supports multiple graphs on the same port with path-based routing.

Uses stdlib http.server for zero dependencies.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import json
import threading
import time
import re
import socket
from urllib.parse import urlparse

from cog.templates import render_index_page, render_graph_row, render_status_page


# Version pulled dynamically when available
try:
    from importlib.metadata import version as pkg_version
    COGDB_VERSION = pkg_version('cogdb')
except Exception:
    COGDB_VERSION = "dev"


# Global registry of servers by port
_server_registry = {}  # port -> CogDBServer
_registry_lock = threading.Lock()


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Thread-per-request HTTP server."""
    daemon_threads = True
    allow_reuse_address = True


class CogDBRequestHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler for CogDB server.
    Supports path-based routing for multiple graphs.
    """
    
    # Suppress default logging
    def log_message(self, format, *args):
        pass
    
    def _send_json(self, data, status=200):
        """Send a JSON response."""
        body = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)
    
    def _send_html(self, html, status=200):
        """Send an HTML response."""
        body = html.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)
    
    def _read_json_body(self):
        """Read and parse JSON request body."""
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length == 0:
            return None
        body = self.rfile.read(content_length)
        return json.loads(body.decode('utf-8'))
    
    def _get_local_ip(self):
        """Get the local IP address of this machine."""
        try:
            # Create a socket to determine local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return '127.0.0.1'
    
    def _parse_path(self):
        """Parse the path to extract graph name and action."""
        path = self.path.rstrip('/')
        if path == '' or path == '/':
            return None, 'index'
        
        parts = path.split('/')
        # /graph_name or /graph_name/action
        if len(parts) >= 2:
            graph_name = parts[1]
            action = parts[2] if len(parts) >= 3 else 'status'
            return graph_name, action
        
        return None, 'index'
    
    def _get_share_url(self):
        """Get the share URL from X-Share-Url header (set by relay for remote access)."""
        return self.headers.get('X-Share-Url', '')
    
    def _get_graph_state(self, graph_name):
        """Get the state for a specific graph."""
        graphs = self.server.cog_graphs
        if graph_name not in graphs:
            return None
        return graphs[graph_name]
    
    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_GET(self):
        """Handle GET requests."""
        graph_name, action = self._parse_path()
        
        if action == 'index':
            self._handle_index_page()
        elif graph_name:
            state = self._get_graph_state(graph_name)
            if not state:
                self._send_json({'ok': False, 'error': f'Graph not found: {graph_name}'}, 404)
                return
            
            if action == 'status':
                self._handle_status_page(graph_name, state)
            elif action == 'stats':
                self._handle_stats(graph_name, state)
            else:
                self._send_json({'ok': False, 'error': 'Not found'}, 404)
        else:
            self._send_json({'ok': False, 'error': 'Not found'}, 404)
    
    def do_POST(self):
        """Handle POST requests."""
        graph_name, action = self._parse_path()
        
        if not graph_name:
            self._send_json({'ok': False, 'error': 'Graph name required in path'}, 400)
            return
        
        state = self._get_graph_state(graph_name)
        if not state:
            self._send_json({'ok': False, 'error': f'Graph not found: {graph_name}'}, 404)
            return
        
        if action == 'query':
            self._handle_query(graph_name, state)
        elif action == 'mutate':
            self._handle_mutate(graph_name, state)
        else:
            self._send_json({'ok': False, 'error': 'Not found'}, 404)
    
    def _handle_index_page(self):
        """Render index page listing all available graphs."""
        graphs = self.server.cog_graphs
        start_time = self.server.cog_start_time
        
        # Calculate uptime
        uptime_secs = int(time.time() - start_time)
        hours, remainder = divmod(uptime_secs, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{hours}h {minutes}m {seconds}s"
        
        # Build graph list HTML
        graph_rows = []
        for name, state in graphs.items():
            try:
                node_count = state['graph'].v().count()
            except Exception:
                node_count = 0
            mode = "rw" if state['writable'] else "ro"
            graph_rows.append(render_graph_row(name, node_count, mode))
        
        graphs_html = ''.join(graph_rows) if graph_rows else '<div style="color: #666;">No graphs registered</div>'
        
        html = render_index_page(
            version=COGDB_VERSION,
            local_ip=self._get_local_ip(),
            port=self.server.server_address[1],
            graphs_html=graphs_html,
            uptime_str=uptime_str,
            share_url=self._get_share_url()
        )
        self._send_html(html)
    
    def _handle_status_page(self, graph_name, state):
        """Render terminal-style status page for a specific graph."""
        graph = state['graph']
        start_time = state['start_time']
        queries_served = state['queries_served']
        last_query_time = state['last_query_time']
        writable = state['writable']
        
        # Calculate uptime
        uptime_secs = int(time.time() - start_time)
        hours, remainder = divmod(uptime_secs, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{hours}h {minutes}m {seconds}s"
        
        # Get node/edge counts
        try:
            node_count = graph.v().count()
            edge_count = len(graph.all_predicates) if hasattr(graph, 'all_predicates') else 0
        except Exception:
            node_count = 0
            edge_count = 0
        
        # Last query ago
        if last_query_time:
            ago = int(time.time() - last_query_time)
            last_query_str = f"{ago}s ago"
        else:
            last_query_str = "never"
        
        mode_str = "writable" if writable else "read-only"
        
        # Get instance ID
        try:
            instance_id = graph.cog.instance_id if hasattr(graph, 'cog') else 'N/A'
        except Exception:
            instance_id = 'N/A'
        
        html = render_status_page(
            version=COGDB_VERSION,
            local_ip=self._get_local_ip(),
            port=self.server.server_address[1],
            graph_name=graph_name,
            instance_id=instance_id,
            node_count=node_count,
            edge_count=edge_count,
            uptime_str=uptime_str,
            queries_served=queries_served,
            last_query_str=last_query_str,
            mode_str=mode_str,
            share_url=self._get_share_url()
        )
        self._send_html(html)
    
    def _handle_stats(self, graph_name, state):
        """Return JSON statistics for a graph."""
        graph = state['graph']
        start_time = state['start_time']
        
        try:
            node_count = graph.v().count()
        except Exception:
            node_count = 0
        
        edge_count = len(graph.all_predicates) if hasattr(graph, 'all_predicates') else 0
        
        stats = {
            'version': COGDB_VERSION,
            'graph_name': graph_name,
            'nodes': node_count,
            'edges': edge_count,
            'uptime_seconds': int(time.time() - start_time),
            'queries_served': state['queries_served'],
            'writable': state['writable']
        }
        self._send_json(stats)
    
    def _handle_query(self, graph_name, state):
        """Execute a Torque query on a specific graph."""
        try:
            body = self._read_json_body()
            if not body or 'q' not in body:
                self._send_json({'ok': False, 'error': 'Missing query parameter "q"'}, 400)
                return
            
            query_str = body['q']
            graph = state['graph']
            query_lock = state.get('query_lock')
            
            # Execute the query safely with lock for thread safety
            if query_lock:
                with query_lock:
                    result = self._execute_query(graph, query_str)
            else:
                result = self._execute_query(graph, query_str)
            
            # Update stats
            state['queries_served'] += 1
            state['last_query_time'] = time.time()
            
            self._send_json({'ok': True, 'result': result.get('result', result)})
            
        except Exception as e:
            self._send_json({'ok': False, 'error': str(e)}, 400)
    
    def _execute_query(self, graph, query_str):
        """Safely execute a Torque query string."""
        # Validate query structure - must start with allowed methods
        allowed_starts = ['v(', 'scan(']
        query_stripped = query_str.strip()
        
        if not any(query_stripped.startswith(s) for s in allowed_starts):
            raise ValueError(f"Query must start with one of: {allowed_starts}")
        
        # SECURITY: Block dunder attributes (prevents __globals__, __init__ RCE attacks)
        if '__' in query_str:
            raise ValueError("Query contains forbidden pattern '__'")
        
        # Validate method chain - only allow known traversal methods
        allowed_methods = {
            'v', 'out', 'inc', 'both', 'has', 'hasr', 'tag', 'back',
            'all', 'count', 'first', 'one', 'scan', 'filter', 'unique', 'limit', 'skip',
            'is_', 'bfs', 'dfs', 'sim', 'k_nearest'
        }
        
        # Extract method names from the query
        method_pattern = r'\.?([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
        methods_used = set(re.findall(method_pattern, query_str))
        
        invalid_methods = methods_used - allowed_methods
        if invalid_methods:
            raise ValueError(f"Disallowed methods: {invalid_methods}")
        
        # Build and execute - use compile to check syntax first
        full_query = f"graph.{query_str}"
        try:
            compile(full_query, '<query>', 'eval')
        except SyntaxError as e:
            raise ValueError(f"Invalid query syntax: {e}")
        
        # Execute in restricted namespace
        result = eval(full_query, {"__builtins__": {}}, {"graph": graph})
        
        # Handle different return types
        if isinstance(result, dict):
            return result
        elif isinstance(result, int):
            return {'result': result}
        else:
            return {'result': []}
    
    def _handle_mutate(self, graph_name, state):
        """Handle write operations on a specific graph."""
        if not state['writable']:
            self._send_json({
                'ok': False, 
                'error': 'Write operations disabled. Start server with writable=True'
            }, 403)
            return
        
        try:
            body = self._read_json_body()
            if not body or 'op' not in body:
                self._send_json({'ok': False, 'error': 'Missing "op" parameter'}, 400)
                return
            
            op = body['op']
            args = body.get('args', [])
            graph = state['graph']
            
            if op == 'put':
                if len(args) != 3:
                    self._send_json({'ok': False, 'error': 'put requires [subject, predicate, object]'}, 400)
                    return
                graph.put(args[0], args[1], args[2])
                self._send_json({'ok': True, 'affected': 1})
                
            elif op == 'put_batch':
                if not isinstance(args, list):
                    self._send_json({'ok': False, 'error': 'put_batch requires list of [s,p,o] triples'}, 400)
                    return
                triples = [tuple(t) for t in args]
                graph.put_batch(triples)
                self._send_json({'ok': True, 'affected': len(triples)})
                
            elif op == 'delete':
                if len(args) != 3:
                    self._send_json({'ok': False, 'error': 'delete requires [subject, predicate, object]'}, 400)
                    return
                graph.delete(args[0], args[1], args[2])
                self._send_json({'ok': True, 'affected': 1})
            
            elif op == 'truncate':
                graph.truncate()
                self._send_json({'ok': True, 'message': 'Graph truncated'})
                
            else:
                self._send_json({'ok': False, 'error': f'Unknown operation: {op}'}, 400)
                
        except Exception as e:
            self._send_json({'ok': False, 'error': str(e)}, 400)


class CogDBServer:
    """
    HTTP server wrapper for CogDB Graphs.
    Supports multiple graphs on the same port.
    
    Usage:
        # Graphs register themselves via Graph.serve()
        # This class is used internally
    """
    
    def __init__(self, port=8080, host='0.0.0.0'):
        self.port = port
        self.host = host
        self.server = None
        self.thread = None
        self._running = False
        self._graphs = {}  # graph_name -> state
        self._lock = threading.Lock()
    
    def register_graph(self, graph, writable=False):
        """Register a graph to be served."""
        with self._lock:
            self._graphs[graph.graph_name] = {
                'graph': graph,
                'start_time': time.time(),
                'queries_served': 0,
                'last_query_time': None,
                'writable': writable,
                'query_lock': threading.Lock()  # Per-graph lock for thread-safe queries
            }
            # Update server's graph reference
            if self.server:
                self.server.cog_graphs = self._graphs
    
    def unregister_graph(self, graph_name):
        """Unregister a graph. Returns True if server should shutdown."""
        with self._lock:
            if graph_name in self._graphs:
                del self._graphs[graph_name]
                if self.server:
                    self.server.cog_graphs = self._graphs
            return len(self._graphs) == 0
    
    def has_graph(self, graph_name):
        """Check if a graph is registered."""
        return graph_name in self._graphs
    
    def start(self, blocking=False):
        """Start the HTTP server."""
        if self._running:
            return
        
        self.server = ThreadingHTTPServer((self.host, self.port), CogDBRequestHandler)
        self.server.cog_graphs = self._graphs
        self.server.cog_start_time = time.time()
        
        self._running = True
        
        if blocking:
            try:
                self.server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                self.stop()
        else:
            self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
            self.thread.start()
    
    def stop(self):
        """Stop the HTTP server and release the port."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.server = None
        self._running = False
        self._graphs.clear()
    
    @property
    def url(self):
        """Get the server URL."""
        return f"http://{self.host}:{self.port}"
    
    @property
    def is_running(self):
        return self._running


def get_or_create_server(port, host='0.0.0.0'):
    """Get existing server on port or create a new one."""
    with _registry_lock:
        if port in _server_registry:
            server = _server_registry[port]
            if server.is_running:
                return server, False  # existing server
        
        # Create new server
        server = CogDBServer(port=port, host=host)
        _server_registry[port] = server
        return server, True  # new server


def unregister_from_server(port, graph_name):
    """Unregister a graph from a server. Shuts down server if empty."""
    with _registry_lock:
        if port not in _server_registry:
            return
        
        server = _server_registry[port]
        should_shutdown = server.unregister_graph(graph_name)
        
        if should_shutdown:
            server.stop()
            del _server_registry[port]


def stop_server(port=8080):
    """
    Stop the entire server on a port, unregistering all graphs.
    
    Args:
        port: Port to stop server on (default 8080)
    
    Example:
        from cog.server import stop_server
        stop_server(8080)  # Stops server and all graphs on port 8080
    """
    with _registry_lock:
        if port not in _server_registry:
            return
        
        server = _server_registry[port]
        
        # Clear _server_port on each graph so they can serve again
        for graph_name, state in list(server._graphs.items()):
            graph = state.get('graph')
            if graph and hasattr(graph, '_server_port'):
                graph._server_port = None
        
        server.stop()
        del _server_registry[port]
