#!/usr/bin/env python3
"""Run the hub locally with an embedded Postgres (pip install pgserver). Dev/test only.

Dashboard password: dev-password-123. Legacy runner token: dev-token.
"""
import hashlib
import os
import sys

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root)
data = os.path.join(root, ".devdata")
os.makedirs(data, exist_ok=True)

import pgserver  # noqa: E402

os.environ["DATABASE_URL"] = pgserver.get_server(os.path.join(data, "pg")).get_uri()
pwf = os.path.join(data, "password")
salt = bytes(range(16))
open(pwf, "w").write("scrypt$%s$%s\n" % (salt.hex(), hashlib.scrypt(b"dev-password-123", salt=salt, n=2 ** 14, r=8, p=1, dklen=32).hex()))
os.environ.update(STATUS_PASSWORD_FILE=pwf, SCRAPE_LEGACY_TOKEN="dev-token")

import uvicorn  # noqa: E402

uvicorn.run("scrape_hub.app:app", host="127.0.0.1", port=int(os.environ.get("PORT", "8787")))
