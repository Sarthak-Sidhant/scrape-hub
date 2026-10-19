#!/usr/bin/env python3
"""Keep every telemetry data point forever without growing the database: move old rows of the high-volume tables to object storage.

For each table below, every whole UTC day older than its window is written as one gzipped CSV (with a header row) to
R2 telemetry/<table>/<YYYY>/<YYYY-MM-DD>__<exported at>.csv.gz, the upload's size is checked, and only then are those rows deleted.
A day exported twice (rows that arrived late) gets a second file, never an overwrite, so nothing is lost. Capacity ramps are small
and stay in the database for good.

Run nightly by scrape-hub-archive.timer with the hub's environment (DATABASE_URL, R2_*).
  python scripts/archive_telemetry.py [--dry-run]
"""
import argparse
import datetime
import gzip
import io
import os
import sys

import psycopg

TABLES = {            # table -> days kept in the database (the dashboard's charts read these); older days move to object storage
    "probes": 30,     # one check a minute per portal
    "metrics": 2,     # runner counters: about 2 GB a day during the 44-portal crawl, so only the last 2 days stay in the database
    "batches": 3,     # records per upload, errors, blocked
}
PREFIX = "telemetry/"


def s3():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=os.environ["R2_ENDPOINT"], aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto", config=Config(signature_version="s3v4"))


def days_to_move(c, table, keep):
    rows = c.execute(f"""SELECT DISTINCT (at AT TIME ZONE 'UTC')::date AS d FROM {table}
                         WHERE at < (now() AT TIME ZONE 'UTC')::date - make_interval(days => %s) ORDER BY 1""", (keep,)).fetchall()
    return [r[0] for r in rows]


def export_day(c, table, day):
    """(gzipped CSV bytes, row count) for one UTC day of a table."""
    buf = io.BytesIO()
    n = 0
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz:
        with c.cursor().copy(f"""COPY (SELECT * FROM {table} WHERE at >= %s::date AND at < %s::date + 1 ORDER BY at)
                                 TO STDOUT WITH (FORMAT csv, HEADER true)""", (day.isoformat(), day.isoformat())) as cp:
            for chunk in cp:
                gz.write(chunk)
                n += bytes(chunk).count(b"\n")
    return buf.getvalue(), max(0, n - 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    client, bucket = (None, None) if a.dry_run else (s3(), os.environ["R2_BUCKET"])
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        c.execute("SET TIME ZONE 'UTC'")      # day boundaries are UTC days
        for table, keep in TABLES.items():
            for day in days_to_move(c, table, keep):
                data, n = export_day(c, table, day)
                key = f"{PREFIX}{table}/{day.year}/{day.isoformat()}__{stamp}.csv.gz"
                if a.dry_run:
                    print(f"would move {n} {table} rows of {day} ({len(data)} bytes gzipped) to {key}")
                    continue
                client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/gzip")
                if client.head_object(Bucket=bucket, Key=key)["ContentLength"] != len(data):
                    sys.exit(f"upload of {key} does not match; nothing deleted")
                with c.transaction():
                    gone = c.execute(f"DELETE FROM {table} WHERE at >= %s::date AND at < %s::date + 1", (day.isoformat(), day.isoformat())).rowcount
                if gone != n:      # rows that arrived during the export stay for the next night's file
                    print(f"note: {table} {day}: exported {n}, deleted {gone}")
                print(f"moved {n} {table} rows of {day} to {key} ({len(data)} bytes)")


if __name__ == "__main__":
    main()
