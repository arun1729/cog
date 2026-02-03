"""
CogDB Server HTML Templates
============================
HTML templates for the CogDB HTTP server admin pages.
"""

import html


def _escape(s):
    """Escape string for safe HTML output."""
    if not s:
        return s
    return html.escape(str(s), quote=True)

# ASCII logo used in both pages
ASCII_LOGO = """ ██████╗ ██████╗  ██████╗ ██████╗ ██████╗ 
██╔════╝██╔═══██╗██╔════╝ ██╔══██╗██╔══██╗
██║     ██║   ██║██║  ███╗██║  ██║██████╔╝
██║     ██║   ██║██║   ██║██║  ██║██╔══██╗
╚██████╗╚██████╔╝╚██████╔╝██████╔╝██████╔╝
 ╚═════╝ ╚═════╝  ╚═════╝ ╚═════╝ ╚═════╝ """

# Common styles used across pages
COMMON_STYLES = """
* { margin: 0; padding: 0; box-sizing: border-box; }
html, body {
    background: #000000;
    color: #00ff00;
    font-family: 'Courier New', Courier, monospace;
    min-height: 100vh;
    width: 100%;
}
.container {
    padding: 40px;
    max-width: 900px;
    margin: 0 auto;
}
.ascii-logo {
    color: #00ff00;
    font-size: 10px;
    line-height: 1.1;
    white-space: pre;
    margin-bottom: 30px;
    text-align: center;
}
.status-line {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    margin-bottom: 40px;
    font-size: 14px;
}
.status-dot {
    width: 10px;
    height: 10px;
    background: #00ff00;
    border-radius: 50%;
    box-shadow: 0 0 10px #00ff00;
    animation: pulse 2s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; box-shadow: 0 0 10px #00ff00; }
    50% { opacity: 0.6; box-shadow: 0 0 5px #00ff00; }
}
.info-block {
    border: 1px solid #333;
    padding: 20px;
    margin-bottom: 20px;
    background: #0a0a0a;
}
.info-row {
    display: flex;
    padding: 8px 0;
    border-bottom: 1px solid #1a1a1a;
}
.info-row:last-child {
    border-bottom: none;
}
.info-label {
    width: 150px;
    color: #666;
}
.info-value {
    color: #00ff00;
}
.section-title {
    color: #ffb000;
    margin-bottom: 15px;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 2px;
}
"""


def render_index_page(version, local_ip, port, graphs_html, uptime_str, share_url=""):
    """
    Render the index page listing all available graphs.
    
    Args:
        version: CogDB version string
        local_ip: Local IP address
        port: Server port
        graphs_html: Pre-rendered HTML for graph rows
        uptime_str: Formatted uptime string
        share_url: Optional share URL for remote access (e.g., 'https://abc123.s.cogdb.io/')
    
    Returns:
        Complete HTML string for the index page
    """
    # Use share URL if available, otherwise local address
    # Escape user-controlled values to prevent XSS
    if share_url:
        address_display = _escape(share_url.rstrip('/'))
        connect_url = f"{_escape(share_url.rstrip('/'))}/&lt;graph&gt;"
    else:
        address_display = f"{_escape(local_ip)}:{port}"
        connect_url = f"http://{_escape(local_ip)}:{port}/&lt;graph&gt;"
    
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CogDB Server</title>
    <style>
        {COMMON_STYLES}
        .graph-row {{
            display: flex;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid #1a1a1a;
        }}
        .graph-row:last-child {{
            border-bottom: none;
        }}
        .graph-link {{
            color: #00ff00;
            text-decoration: none;
            font-size: 16px;
            flex: 1;
        }}
        .graph-link:hover {{
            text-decoration: underline;
        }}
        .graph-info {{
            color: #666;
            margin-right: 20px;
        }}
        .graph-mode {{
            color: #ffb000;
            font-size: 11px;
            padding: 2px 6px;
            border: 1px solid #ffb000;
            border-radius: 3px;
        }}
        .uptime {{
            color: #666;
            text-align: center;
            margin-top: 30px;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="ascii-logo">{ASCII_LOGO}</div>
        
        <div class="status-line">
            <div class="status-dot"></div>
            <span>ONLINE</span>
            <span style="color: #666;">|</span>
            <span style="color: #666;">v{version}</span>
        </div>
        
        <div class="info-block">
            <div class="section-title">Server</div>
            <div class="info-row"><span class="info-label">Address:</span><span class="info-value">{address_display}</span></div>
            <div class="info-row"><span class="info-label">Connect URL:</span><span class="info-value">{connect_url}</span></div>
        </div>
        
        <div class="info-block">
            <div class="section-title">Available Graphs</div>
            {graphs_html}
        </div>
        
        <div class="uptime">Server uptime: {uptime_str}</div>
        <div class="uptime" style="margin-top: 10px;"><a href="https://cogdb.io" target="_blank" style="color: #666; text-decoration: none;">cogdb.io</a></div>
    </div>
</body>
</html>'''


def render_graph_row(name, node_count, mode):
    """
    Render a single graph row for the index page.
    
    Args:
        name: Graph name
        node_count: Number of nodes in the graph
        mode: "rw" or "ro" for read-write or read-only
    
    Returns:
        HTML string for the graph row
    """
    safe_name = _escape(name)
    return f'''
            <div class="graph-row">
                <a href="/{safe_name}/" class="graph-link">{safe_name}</a>
                <span class="graph-info">{node_count:,} nodes</span>
                <span class="graph-mode">{_escape(mode)}</span>
            </div>'''


def render_status_page(version, local_ip, port, graph_name, instance_id, 
                       node_count, edge_count, uptime_str, queries_served, 
                       last_query_str, mode_str, share_url=""):
    """
    Render the status page for a specific graph.
    
    Args:
        version: CogDB version string
        local_ip: Local IP address
        port: Server port
        graph_name: Name of the graph
        instance_id: Graph instance ID
        node_count: Number of nodes
        edge_count: Number of edges
        uptime_str: Formatted uptime string
        queries_served: Number of queries served
        last_query_str: Last query time string
        mode_str: "writable" or "read-only"
        share_url: Optional share URL for remote access (e.g., 'https://abc123.s.cogdb.io/')
    
    Returns:
        Complete HTML string for the status page
    """
    safe_graph_name = _escape(graph_name)
    # Use share URL if available, otherwise local address
    if share_url:
        connect_url = f"{_escape(share_url.rstrip('/'))}/{safe_graph_name}"
    else:
        connect_url = f"http://{_escape(local_ip)}:{port}/{safe_graph_name}"
    
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CogDB - {graph_name}</title>
    <style>
        {COMMON_STYLES}
        .back-link {{
            color: #666;
            text-decoration: none;
            font-size: 12px;
        }}
        .back-link:hover {{
            color: #00ff00;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div style="margin-bottom: 20px;"><a href="/" class="back-link">← All Graphs</a></div>
        
        <div class="ascii-logo">{ASCII_LOGO}</div>
        
        <div class="status-line">
            <div class="status-dot"></div>
            <span>ONLINE</span>
            <span style="color: #666;">|</span>
            <span style="color: #666;">v{version}</span>
        </div>
        
        <div class="info-block">
            <div class="section-title">Graph Status</div>
            <div class="info-row"><span class="info-label">Graph:</span><span class="info-value">{safe_graph_name}</span></div>
            <div class="info-row"><span class="info-label">Instance ID:</span><span class="info-value">{_escape(instance_id)}</span></div>
            <div class="info-row"><span class="info-label">Connect URL:</span><span class="info-value">{connect_url}</span></div>
            <div class="info-row"><span class="info-label">Nodes:</span><span class="info-value">{node_count:,}</span></div>
            <div class="info-row"><span class="info-label">Edges:</span><span class="info-value">{edge_count:,}</span></div>
            <div class="info-row"><span class="info-label">Uptime:</span><span class="info-value">{uptime_str}</span></div>
        </div>
        
        <div class="info-block">
            <div class="section-title">Server Stats</div>
            <div class="info-row"><span class="info-label">Queries served:</span><span class="info-value">{queries_served:,}</span></div>
            <div class="info-row"><span class="info-label">Last query:</span><span class="info-value">{last_query_str}</span></div>
            <div class="info-row"><span class="info-label">Mode:</span><span class="info-value">{mode_str}</span></div>
        </div>
        
        <div style="text-align: center; margin-top: 30px;"><a href="https://cogdb.io" target="_blank" style="color: #666; text-decoration: none; font-size: 12px;">cogdb.io</a></div>
    </div>
</body>
</html>'''
