# tunnel.py - WebSocket tunnel client for CogDB Studio
"""
Tunnel client that connects a local g.serve() instance to the Axle relay,
enabling access via the CogDB Studio playground at https://cogdb.io/playground.
"""
import json
import threading
import urllib.request
import urllib.error


def _http_request(method, url, body=None, timeout=25):
    """Make HTTP request using standard library."""
    data = body.encode('utf-8') if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header('Content-Type', 'application/json')
    
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode('utf-8'), resp.headers.get('Content-Type', 'application/json')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8'), e.headers.get('Content-Type', 'application/json')
    except Exception as e:
        return 502, str(e), 'text/plain'


def start_tunnel(local_port, relay_url="wss://axle.cogdb.io/register"):
    """
    Connect to Axle relay and tunnel requests to local g.serve().
    
    Args:
        local_port: Local HTTP port where g.serve() is running
        relay_url: WebSocket URL for the Axle relay server
    
    Returns:
        Thread: The daemon thread running the tunnel
    
    Raises:
        ImportError: If websocket-client is not installed
    """
    try:
        import websocket
    except ImportError:
        raise ImportError(
            "Tunnel requires 'websocket-client' package. "
            "Install with: pip install websocket-client"
        )
    
    def run():
        ws = websocket.WebSocket()
        ws.connect(relay_url)
        
        # Wait for session message
        msg = json.loads(ws.recv())
        if msg.get("type") != "conn.session":
            raise ConnectionError("Expected conn.session message from relay")
        
        # Forward requests from relay to local server
        while True:
            try:
                msg = json.loads(ws.recv())
                msg_type = msg.get("type")
                
                if msg_type == "tunnel.request":
                    status, body, content_type = _http_request(
                        method=msg["method"],
                        url=f"http://localhost:{local_port}{msg['path']}",
                        body=msg.get("body"),
                    )
                    ws.send(json.dumps({
                        "type": "tunnel.response",
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
                            "type": "tunnel.response",
                            "req_id": msg["req_id"],
                            "status": 502,
                            "body": f"Tunnel error: {str(e)}",
                            "content_type": "text/plain"
                        }))
                    except Exception:
                        break
    
    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t
