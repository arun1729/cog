#!/usr/bin/env python3
"""
Build CogDB wheel and serve locally with CORS support.

Usage:
    python3 scripts/local_wheel_server.py
"""
import os
import subprocess
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path


class CORSHandler(SimpleHTTPRequestHandler):
    """HTTP handler with CORS headers for Pyodide."""
    
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()


def main():
    # Find project root (where setup.py is)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    dist_dir = project_root / "dist"
    
    os.chdir(project_root)
    
    # Build the wheel using pip wheel
    print("Building wheel...")
    dist_dir.mkdir(exist_ok=True)
    result = subprocess.run(
        [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", "dist", "."],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Build failed: {result.stderr}")
        sys.exit(1)
    
    # Find the wheel file
    wheels = list(dist_dir.glob("cogdb-*.whl"))
    if not wheels:
        print("No wheel found in dist/")
        sys.exit(1)
    
    wheel_name = wheels[-1].name
    print(f"Built: {wheel_name}")
    
    # Start CORS-enabled server
    os.chdir(dist_dir)
    port = 8888
    
    print(f"\nServing at http://localhost:{port}/")
    print("\nPress Ctrl+C to stop the server.")
    
    httpd = HTTPServer(('', port), CORSHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
