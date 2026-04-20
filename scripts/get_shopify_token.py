"""
get_shopify_token.py
====================
One-time script to get a Shopify Admin API access token for the
gp_metadata_fill app and save it to config.py.

Run this once per store. It will:
1. Open a browser to authorize the app
2. Catch the OAuth callback on localhost:3000
3. Exchange the code for a permanent access token
4. Save it to config.py automatically

Usage:
    py get_shopify_token.py --store zumbrotadrivetrain
"""

import http.server
import threading
import webbrowser
import urllib.parse
import hashlib
import os
import sys
import json
import argparse
import re

try:
    import requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "--quiet"])
    import requests

# ── App credentials (from Dev Dashboard → gp_metadata_fill → Settings) ────────
CLIENT_ID     = "67136df4fcf90bc002468bb25ae8017f"
CLIENT_SECRET = "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"
REDIRECT_URI  = "http://localhost:3000/callback"
SCOPES        = "read_orders,write_orders"

# ── Globals to capture the OAuth callback ─────────────────────────────────────
_auth_code = None
_callback_received = threading.Event()


class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global _auth_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            _auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"""
                <html><body style='font-family:sans-serif;text-align:center;padding:60px'>
                <h2>&#10003; Authorization successful!</h2>
                <p>You can close this tab and return to the terminal.</p>
                </body></html>
            """)
            _callback_received.set()
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing authorization code.")

    def log_message(self, format, *args):
        pass  # Suppress default HTTP server logs


def start_callback_server():
    server = http.server.HTTPServer(("localhost", 3000), CallbackHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server


def get_access_token(shop_domain, code):
    url = f"https://{shop_domain}/admin/oauth/access_token"
    resp = requests.post(url, json={
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code":          code,
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def save_token_to_config(shop_domain, token):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "config.py")

    with open(config_path, "r", encoding="utf-8") as f:
        content = f.read()

    store_key = shop_domain.replace(".myshopify.com", "").replace(".", "_").upper()

    shopify_config = f"""
# ── Shopify API ────────────────────────────────────────────────────────────────
SHOPIFY_CONFIG = {{
    "{shop_domain.replace('.myshopify.com', '')}": {{
        "shop":  "{shop_domain}",
        "token": "{token}",
    }},
}}
"""

    # Replace existing SHOPIFY_CONFIG block if present, else append
    if "SHOPIFY_CONFIG" in content:
        content = re.sub(
            r"\n# ── Shopify API.*?(?=\n# ──|\Z)",
            shopify_config,
            content,
            flags=re.DOTALL
        )
    else:
        content += shopify_config

    with open(config_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"\n  ✅ Token saved to config.py under SHOPIFY_CONFIG['{shop_domain.replace('.myshopify.com', '')}']")


def run(store):
    # Normalize store domain
    if not store.endswith(".myshopify.com"):
        shop_domain = f"{store}.myshopify.com"
    else:
        shop_domain = store

    print(f"\n{'='*60}")
    print(f"  Shopify Token Setup — {shop_domain}")
    print(f"{'='*60}\n")

    # Start local callback server
    print("  Starting local callback server on http://localhost:3000 ...")
    server = start_callback_server()

    # Build authorization URL
    state = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
    auth_url = (
        f"https://{shop_domain}/admin/oauth/authorize"
        f"?client_id={CLIENT_ID}"
        f"&scope={SCOPES}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI, safe='')}"
        f"&state={state}"
    )

    print(f"  Opening browser to authorize app...")
    print(f"  URL: {auth_url}\n")
    webbrowser.open(auth_url)

    print("  Waiting for authorization (browser should have opened)...")
    _callback_received.wait(timeout=120)
    server.shutdown()

    if not _auth_code:
        print("\n  ❌ Timed out waiting for authorization. Please try again.")
        sys.exit(1)

    print("  Authorization code received. Exchanging for access token...")
    try:
        token = get_access_token(shop_domain, _auth_code)
    except Exception as e:
        print(f"\n  ❌ Failed to get access token: {e}")
        sys.exit(1)

    print(f"  Access token obtained: {token[:12]}...")
    save_token_to_config(shop_domain, token)

    print(f"\n  Done! You can now run shopify_update.py to update orders directly.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get Shopify access token for gp_metadata_fill app.")
    parser.add_argument("--store", default="zumbrotadrivetrain",
                        help="Shopify store name (e.g. 'zumbrotadrivetrain')")
    args = parser.parse_args()
    run(args.store)
