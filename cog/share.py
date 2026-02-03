# share.py - WebSocket share client for CogDB Studio
"""
Share client that connects a local g.serve() instance to the CogDB relay,
"""
import json
import logging
import re
import threading
import urllib.request
import urllib.error

logger = logging.getLogger("cog.share")


def _validate_path(path):
    """
    Validate that path is safe to forward to local server.
    """
    if not path:
        return False
    # Path must start with /
    if not path.startswith('/'):
        return False
    # Path must not contain @ (urllib can interpret this as host)
    if '@' in path:
        return False
    # Path must be a valid URL path (alphanumeric, /, -, _, ., ?, &, =, etc.)
    if not re.match(r'^[a-zA-Z0-9/_\-\.\?&=%]+$', path):
        return False
    return True


def _http_request(method, url, body=None, timeout=25, extra_headers=None):
    data = body.encode('utf-8') if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header('Content-Type', 'application/json')
    if extra_headers:
        for key, value in extra_headers.items():
            req.add_header(key, value)
    
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode('utf-8'), resp.headers.get('Content-Type', 'application/json')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8'), e.headers.get('Content-Type', 'application/json')
    except Exception as e:
        return 502, str(e), 'text/plain'


class ShareInfo:
    """Holds share connection information."""
    def __init__(self):
        self.url = None
        self.session_id = None
        self._ready = threading.Event()
        self._error = None
    
    def wait(self, timeout=10):
        """Wait for share URL to be available. Returns URL or raises error."""
        if self._ready.wait(timeout):
            if self._error:
                raise ConnectionError(self._error)
            return self.url
        raise TimeoutError("Timed out waiting for share connection")


def start_share(local_port, local_host="127.0.0.1", relay_url=None):
    """
    Connect to CogDB relay and share requests to local g.serve().
    
    Args:
        local_port: Local HTTP port where g.serve() is running
        local_host: Local host where g.serve() is bound (default 127.0.0.1)
        relay_url: WebSocket URL for the relay server (uses config.RELAY_URL if None)
    
    Returns:
        ShareInfo: Object with .url property and .wait() method
    
    Raises:
        ImportError: If websocket-client is not installed
        RuntimeError: If sharing is disabled (RELAY_URL is None in config)
    """
    try:
        import websocket
    except ImportError:
        raise ImportError(
            "Share requires 'websocket-client' package. "
            "Install with: pip install websocket-client"
        )
    
    # Get relay URL from config if not provided
    if relay_url is None:
        from cog import config as cfg
        relay_url = cfg.RELAY_URL
    
    # Check if sharing is disabled
    if relay_url is None:
        raise RuntimeError(
            "CogDB sharing is disabled. Set RELAY_URL in cog/config.py to enable sharing."
        )
    
    import certifi
    sslopt = {"ca_certs": certifi.where()}
    
    # Normalize host
    forward_host = "127.0.0.1" if local_host == "0.0.0.0" else local_host
    
    info = ShareInfo()
    
    def run():
        try:
            ws = websocket.WebSocket(sslopt=sslopt)
            ws.connect(relay_url)
            
            # Wait for session message
            msg = json.loads(ws.recv())
            if msg.get("type") != "conn.session":
                info._error = "Expected conn.session message from relay"
                info._ready.set()
                return
            
            # Extract share URL from session
            info.session_id = msg.get("session")
            info.url = msg.get("share_url")  # The relay provides the public URL
            logger.debug(f"Relay session established: session={info.session_id}, url={info.url}")
            info._ready.set()
            
            # Forward requests from relay to local server
            while True:
                msg = {}
                try:
                    msg = json.loads(ws.recv())
                    msg_type = msg.get("type")
                    
                    if msg_type == "share.request":
                        path = msg.get("path", "")
                        
                        if not _validate_path(path):
                            logger.warning(f"Rejected invalid path from relay: {path}")
                            ws.send(json.dumps({
                                "type": "share.response",
                                "req_id": msg["req_id"],
                                "status": 400,
                                "body": "Invalid path",
                                "content_type": "text/plain"
                            }))
                            continue
                        
                        # Pass headers so server generates correct links for remote access
                        extra_headers = {
                            "X-Share-Url": info.url or ""
                        } if info.url else None
                        status, body, content_type = _http_request(
                            method=msg["method"],
                            url=f"http://{forward_host}:{local_port}{path}",
                            body=msg.get("body"),
                            extra_headers=extra_headers
                        )
                        ws.send(json.dumps({
                            "type": "share.response",
                            "req_id": msg["req_id"],
                            "status": status,
                            "body": body,
                            "content_type": content_type
                        }))
                    
                    elif msg_type == "conn.ping":
                        ws.send(json.dumps({"type": "conn.pong"}))
                    
                except Exception as e:
                    if msg.get("req_id"):
                        try:
                            ws.send(json.dumps({
                                "type": "share.response",
                                "req_id": msg["req_id"],
                                "status": 502,
                                "body": f"Share error: {str(e)}",
                                "content_type": "text/plain"
                            }))
                        except Exception:
                            break
        except Exception as e:
            info._error = str(e)
            info._ready.set()
    
    t = threading.Thread(target=run, daemon=True)
    t.start()
    return info
