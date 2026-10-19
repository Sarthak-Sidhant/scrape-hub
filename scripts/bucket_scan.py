#!/usr/bin/env python3
"""List every object in the hub's buckets (B2 and R2) and save the totals per top-level prefix in bucket_usage, the ground truth for the
overview's storage section (neither B2 nor R2 reports a bucket's size through its API). Run daily by scrape-hub-bucket-scan.timer.
A listing returns 1,000 objects a call; B2 charges about $0.004 per 1,000 such calls, so a scan of 20 million objects costs about 8 cents.

  python scripts/bucket_scan.py [--store b2|r2]
"""
import argparse
import datetime
import os
import sys
import time

import psycopg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrape_hub import app, config  # noqa: E402


def scan(store):
    client, bucket = app.s3_client(store)
    by, n, t = {}, 0, time.time()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, PaginationConfig={"PageSize": 1000}):
        for o in page.get("Contents", []):
            prefix = o["Key"].split("/", 1)[0] + "/" if "/" in o["Key"] else "(top level)"
            b = by.setdefault(prefix, [0, 0])
            b[0] += 1
            b[1] += o["Size"]
            n += 1
        if n and n % 200000 < 1000:
            print(f"{store}: {n:,} objects listed ({time.time() - t:.0f} s)", flush=True)
    return bucket, by


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", choices=["b2", "r2"], action="append")
    a = ap.parse_args()
    stores = a.store or [s for s, b in (("b2", config.B2_BUCKET), ("r2", config.R2_BUCKET)) if b]
    for store in stores:
        at = datetime.datetime.now(datetime.timezone.utc)
        bucket, by = scan(store)
        with psycopg.connect(os.environ["DATABASE_URL"]) as c:
            c.cursor().executemany("INSERT INTO bucket_usage (at, store, bucket, prefix, objects, bytes) VALUES (%s, %s, %s, %s, %s, %s)",
                                   [(at, store, bucket, p, v[0], v[1]) for p, v in by.items()])
        print(f"{store} {bucket}: {sum(v[0] for v in by.values()):,} objects, {sum(v[1] for v in by.values()) / 1e9:,.1f} GB in {len(by)} prefixes")


if __name__ == "__main__":
    main()
