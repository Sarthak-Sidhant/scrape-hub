#!/usr/bin/env python3
"""Seed the work queue for project `eprocure-fy2526`: one item per organisation with tenders in the window.

Usage: DATABASE_URL=... python scripts/seed_fy2526.py [--only ID,ID] [--reset]
Largest organisations are spread across partitions first so no runner gets all the big ones.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrape_hub import db  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "crawler"))
import orgs  # noqa: E402

PROJECT = "eprocure-fy2526"
COUNTS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "crawler", "org_counts_fy2025_2026.json")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="comma-separated org ids (for tests)")
    ap.add_argument("--reset", action="store_true", help="delete queue rows that are not done first (done rows are kept)")
    ap.add_argument("--offline", action="store_true", help="skip remapping ids against the live portal dropdown (tests only)")
    a = ap.parse_args()
    only = {x for x in a.only.split(",") if x}
    counts = json.load(open(COUNTS))
    if not a.offline:
        # The counts file is keyed by the portal's September ids. Re-key by name against today's dropdown.
        live = orgs.live_options()
        by_name = {}
        for oid, label in live.items():
            by_name.setdefault(orgs.norm(label), []).append(oid)
        remapped, moved, skipped = {}, 0, []
        for old, v in counts.items():
            if v["tenders_count"] <= 0:
                continue
            ids = by_name.get(orgs.norm(v["name"]), [])
            if len(ids) != 1:
                skipped.append((old, v["name"], v["tenders_count"], len(ids)))
                continue
            moved += ids[0] != old
            remapped[ids[0]] = v
        counts = remapped
        print(f"remapped against live dropdown: {len(counts)} orgs, {moved} ids changed, {len(skipped)} unmatched or ambiguous")
        for sk in skipped:
            print("  SKIPPED", sk)
    selected = [(k, v) for k, v in counts.items() if v["tenders_count"] > 0 and (not only or k in only)]
    selected.sort(key=lambda kv: -kv[1]["tenders_count"])
    orgs_list = selected
    db.init_schema()
    with db.conn() as c:
        if a.reset:
            c.execute("DELETE FROM queue WHERE project=%s AND status<>'done'", (PROJECT,))
        with c.cursor() as cur:
            cur.executemany(
                """INSERT INTO queue (project, key, partition_id, payload) VALUES (%s,%s,%s,%s::jsonb)
                   ON CONFLICT (project, key) DO NOTHING""",
                [(PROJECT, k, i, json.dumps({"org_id": k, "org_name": v["name"], "expected": v["tenders_count"]}))
                 for i, (k, v) in enumerate(orgs_list)])
    print(f"seeded {len(orgs_list)} organisations, {sum(v['tenders_count'] for _, v in orgs_list)} tenders expected")


if __name__ == "__main__":
    main()
