#!/usr/bin/env python3
"""Build queued tender bundles (scrape_hub/bundles.py), one at a time, and upload each zip to R2 as bundles/<slug>.zip. Its own service
(deploy/scrape-hub-bundler.service), so a big bundle never ties up a hub worker. The zip is written to BUNDLE_WORKDIR first and removed
after the upload.

  python scripts/bundler.py            # run forever
  python scripts/bundler.py --once     # build what is queued now, then exit
"""
import argparse
import hashlib
import os
import shutil
import sys
import time
import traceback

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrape_hub import app, bundles, config  # noqa: E402

WORKDIR = os.environ.get("BUNDLE_WORKDIR") or os.environ.get("CACHE_DIRECTORY") or "/tmp"
MIN_FREE = int(os.environ.get("BUNDLE_MIN_FREE_GB", "5")) * 10 ** 9      # leave the VPS this much disk, whatever the bundle


def connect():
    return psycopg.connect(config.DATABASE_URL, autocommit=True, row_factory=dict_row)


def claim(c):
    return c.execute(f"""UPDATE bundles SET status='building', error='', updated_at=now()
                         WHERE slug = (SELECT slug FROM bundles WHERE status='queued' ORDER BY updated_at FOR UPDATE SKIP LOCKED LIMIT 1)
                         RETURNING {bundles.COLUMNS}""").fetchone()


def build_one(c, b, storage):
    slug, t0 = b["slug"], time.time()
    est = bundles.preview(c, b["tenders"])["doc_bytes"] if b["include"].get("documents") else 0
    free = shutil.disk_usage(WORKDIR).free
    if free - est * 1.1 < MIN_FREE:
        raise RuntimeError(f"not enough disk: the documents are about {est / 1e9:.1f} GB and {free / 1e9:.1f} GB is free")
    path = os.path.join(WORKDIR, f"{slug}.{os.getpid()}.zip.part")
    state = {"checked": 0.0, "stop": False}

    def progress(**kw):
        c.execute("UPDATE bundles SET progress=%s WHERE slug=%s AND status='building'", (Jsonb(kw), slug))

    def cancelled():
        if time.time() - state["checked"] > 2:
            state["checked"] = time.time()
            row = c.execute("SELECT status FROM bundles WHERE slug=%s", (slug,)).fetchone()
            state["stop"] = not row or row["status"] != "building"
        return state["stop"]

    try:
        summary = bundles.build(c, b, storage, path, progress, cancelled)
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        size = os.path.getsize(path)
        client, bucket = app.s3_client("r2")
        key = bundles.OBJECT_PREFIX + slug + ".zip"
        client.upload_file(path, bucket, key, ExtraArgs={"ContentType": "application/zip"})
        summary.update(seconds=round(time.time() - t0, 1), via_relay=storage.via_relay, direct=storage.direct)
        row = c.execute("""UPDATE bundles SET status='ready', progress=%s, object_key=%s, bytes=%s, sha256=%s, built_at=now(), updated_at=now()
                           WHERE slug=%s AND status='building' RETURNING slug""", (Jsonb(summary), key, size, h.hexdigest(), slug)).fetchone()
        if not row:      # edited while uploading: it is queued again (the zip just uploaded is served until the next one replaces it)
            c.execute("UPDATE bundles SET object_key=%s, bytes=%s, sha256=%s, built_at=now() WHERE slug=%s", (key, size, h.hexdigest(), slug))
        print(f"{slug}: {summary['done']} tenders, {summary['files']} files, {size / 1e6:.1f} MB in {summary['seconds']} s "
              f"(B2 via relay {storage.via_relay}, direct {storage.direct})", flush=True)
    finally:
        if os.path.exists(path):
            os.remove(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    os.makedirs(WORKDIR, exist_ok=True)
    storage = bundles.Storage(app.s3_client)
    with connect() as c:
        n = c.execute("UPDATE bundles SET status='queued' WHERE status='building'").rowcount      # a build cut short by a restart
        if n:
            print(f"re-queued {n} interrupted bundle(s)", flush=True)
    print(f"bundler up; B2 reads {'through ' + bundles.RELAY if bundles.RELAY else 'direct'}; workdir {WORKDIR}", flush=True)
    while True:
        try:
            with connect() as c:
                while (b := claim(c)):
                    storage.via_relay = storage.direct = 0
                    try:
                        build_one(c, b, storage)
                    except InterruptedError as e:
                        print(f"{b['slug']}: {e}", flush=True)
                    except Exception as e:
                        traceback.print_exc()
                        c.execute("UPDATE bundles SET status='failed', error=%s, updated_at=now() WHERE slug=%s AND status='building'",
                                  (f"{type(e).__name__}: {e}"[:500], b["slug"]))
        except psycopg.OperationalError as e:
            print("database:", e, flush=True)
        if args.once:
            return
        time.sleep(3)


if __name__ == "__main__":
    main()
