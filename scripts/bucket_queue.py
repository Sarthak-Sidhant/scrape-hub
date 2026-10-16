#!/usr/bin/env python3
"""Balance a project's pending queue items into N buckets, one per runner slice, so all runners finish together.

Runners claim items where partition_id % total_jobs == job_index, lowest partition_id first. This rewrites partition_id of the
project's pending items so that:  partition_id % N = bucket,  and within a bucket the biggest items come first.
Weight of an item = per_item + per_record * payload.expected (seconds); defaults are fitted from real runs (13.1 s per
window + 0.331 s per tender at 4 threads). Greedy longest-first assignment (LPT). Only pending items are touched.

Usage: DATABASE_URL=... python scripts/bucket_queue.py PROJECT [--buckets 20] [--per-item 13.1] [--per-record 0.331] [--apply]
Without --apply it only prints what it would do.
"""
import argparse
import heapq
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrape_hub import db  # noqa: E402


def lpt(weights, n):
    """weights: {key: seconds}. Returns ({key: (bucket, rank)}, [bucket_total_seconds])."""
    heap = [(0.0, b) for b in range(n)]
    heapq.heapify(heap)
    members = {b: [] for b in range(n)}
    for key, w in sorted(weights.items(), key=lambda kv: (-kv[1], kv[0])):
        total, b = heapq.heappop(heap)
        members[b].append(key)
        heapq.heappush(heap, (total + w, b))
    out, totals = {}, [0.0] * n
    for b, keys in members.items():
        for rank, key in enumerate(keys):          # keys were added biggest first
            out[key] = (b, rank)
            totals[b] += weights[key]
    return out, totals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("project")
    ap.add_argument("--buckets", type=int, default=20)
    ap.add_argument("--per-item", type=float, default=13.1)
    ap.add_argument("--per-record", type=float, default=0.331)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    with db.conn() as c:
        rows = c.execute("SELECT key, coalesce((payload->>'expected')::int, 0) AS n FROM queue WHERE project=%s AND status='pending'",
                         (a.project,)).fetchall()
        if not rows:
            sys.exit("no pending items")
        n_by = {r["key"]: r["n"] for r in rows}
        weights = {k: a.per_item + a.per_record * n for k, n in n_by.items()}
        assign, totals = lpt(weights, a.buckets)
        counts = [0] * a.buckets
        tenders = [0] * a.buckets
        for k, (b, _) in assign.items():
            counts[b] += 1
            tenders[b] += n_by[k]
        print(f"{len(rows)} pending items, {sum(n_by.values())} tenders, {a.buckets} buckets")
        print(f"estimated seconds per bucket: min {min(totals):.0f}, max {max(totals):.0f}, spread {(max(totals) - min(totals)) / max(totals) * 100:.1f}%")
        for b in range(a.buckets):
            print(f"  bucket {b + 1:2d}: {counts[b]:4d} items  {tenders[b]:6d} tenders  ~{totals[b] / 3600:.2f} h")
        if a.apply:
            with c.cursor() as cur:
                cur.executemany("UPDATE queue SET partition_id=%s WHERE project=%s AND key=%s AND status='pending'",
                                [(b + a.buckets * rank, a.project, k) for k, (b, rank) in assign.items()])
            print("applied: partition_id = bucket + buckets * rank (bucket = partition_id % buckets, biggest first within a bucket)")
        else:
            print("dry run; add --apply to write it")


if __name__ == "__main__":
    main()
