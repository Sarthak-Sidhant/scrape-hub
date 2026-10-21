#!/usr/bin/env python3
"""Upload worker.js as the Cloudflare Worker `b2-relay` and turn on its workers.dev address. Uses CF_TOKEN_SARTHAK and CF_ACCOUNT_ID_SARTHAK
from ~/.config/claude-env (never printed). Prints the relay URL to put in the bundler's B2_RELAY_URL.

  python3 deploy/b2-relay/deploy.py [--bucket gepnic-scrape]
"""
import argparse
import json
import os
import urllib.error
import urllib.request
import uuid

ap = argparse.ArgumentParser()
ap.add_argument("--bucket", default="gepnic-scrape")
ap.add_argument("--name", default="b2-relay")
args = ap.parse_args()
env = {}
for line in open(os.path.expanduser("~/.config/claude-env")):
    line = line.strip().removeprefix("export ")
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        env[k] = v.strip().strip("'\"")
TOKEN, ACC = env["CF_TOKEN_SARTHAK"], env["CF_ACCOUNT_ID_SARTHAK"]
API = f"https://api.cloudflare.com/client/v4/accounts/{ACC}/workers"


def call(method, path, body=None, ctype="application/json"):
    req = urllib.request.Request(API + path, data=body, method=method, headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": ctype})
    try:
        return json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path}: HTTP {e.code} {e.read()[:400].decode()}")


code = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.js"), "rb").read()
meta = {"main_module": "worker.js", "compatibility_date": "2026-10-01",
        "bindings": [{"type": "plain_text", "name": "BUCKET", "text": args.bucket}]}
b = uuid.uuid4().hex
body = (f'--{b}\r\nContent-Disposition: form-data; name="metadata"; filename="metadata.json"\r\nContent-Type: application/json\r\n\r\n'.encode()
        + json.dumps(meta).encode() + b"\r\n"
        + f'--{b}\r\nContent-Disposition: form-data; name="worker.js"; filename="worker.js"\r\nContent-Type: application/javascript+module\r\n\r\n'.encode()
        + code + b"\r\n" + f"--{b}--\r\n".encode())
print("uploaded:", call("PUT", f"/scripts/{args.name}", body, f"multipart/form-data; boundary={b}")["success"])
call("POST", f"/scripts/{args.name}/subdomain", json.dumps({"enabled": True, "previews_enabled": False}).encode())
sub = call("GET", "/subdomain")["result"]["subdomain"]
print(f"B2_RELAY_URL=https://{args.name}.{sub}.workers.dev")
