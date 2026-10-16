#!/usr/bin/env python3
"""Dump the hub's Postgres database and keep the last N dumps in object storage (R2) under backups/.

Run by the scrape-hub-backup systemd timer using the hub's own environment (DATABASE_URL, R2_*).
Custom-format dumps are compressed and restorable with pg_restore, selectively if needed.
"""
import datetime
import os
import shutil
import subprocess
import sys
import tempfile

import boto3
from botocore.config import Config

KEEP = int(os.environ.get("BACKUP_KEEP", "14"))
PREFIX = "backups/"


def main():
    s3 = boto3.client("s3", endpoint_url=os.environ["R2_ENDPOINT"], aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto", config=Config(signature_version="s3v4"))
    bucket = os.environ["R2_BUCKET"]
    work = tempfile.mkdtemp(dir="/var/tmp", prefix="scrape-hub-backup-")
    try:
        name = "scrape_hub-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S") + ".dump"
        path = os.path.join(work, name)
        subprocess.run(["pg_dump", "--format=custom", "--no-owner", "--file", path, os.environ["DATABASE_URL"]], check=True)
        size = os.path.getsize(path)
        s3.upload_file(path, bucket, PREFIX + name)
        got = s3.head_object(Bucket=bucket, Key=PREFIX + name)["ContentLength"]
        if got != size:
            sys.exit(f"upload mismatch: wrote {size} bytes, storage has {got}")
        dumps = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=PREFIX).get("Contents", []) if o["Key"].endswith(".dump"))
        for old in dumps[:-KEEP]:
            s3.delete_object(Bucket=bucket, Key=old)
        print(f"backed up {PREFIX}{name} ({size} bytes); keeping {min(len(dumps), KEEP)} dumps")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
