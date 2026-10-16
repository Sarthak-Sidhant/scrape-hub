#!/usr/bin/env python3
"""FY 2025-26 eprocure crawler for GitHub Actions, reporting to scrape-hub.

Each runner claims its slice of organisations from the hub queue, lists every tender in the date window,
fetches the status/details/summary pages for each, and uploads one record per tender. Documents (PDF/ZIP)
are not downloaded here; the metadata and links are stored in the record.
"""
import argparse
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "reporter"))

import requests  # noqa: E402
from requests.adapters import HTTPAdapter  # noqa: E402

import scrape_organization as so  # noqa: E402
from hubclient import HubClient  # noqa: E402

PROJECT = "eprocure-fy2526"
FROM_DATE, TO_DATE = "01/04/2025", "31/03/2026"


def list_tenders(org_id, out_dir, max_tenders, retries):
    """All tender rows for the organisation in the window (paged search results)."""
    s = requests.Session()
    s.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=3))
    s.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    s.headers.update({"User-Agent": so.USER_AGENT, "Accept-Language": "en-US,en;q=0.5"})
    html = so.fetch_search_page(s, org_id=org_id, max_retries=retries, output_dir=out_dir, from_date=FROM_DATE, to_date=TO_DATE)
    out = []
    while True:
        rows, nxt = so.parse_results_page(html)
        out.extend(rows)
        if not nxt or (max_tenders and len(out) >= max_tenders):
            break
        html = so.safe_request(s, "GET", nxt).text
        time.sleep(0.4)
    return out[:max_tenders] if max_tenders else out


def crawl_org(hub, item, args, deadline):
    org_id, name = item["payload"]["org_id"], item["payload"]["org_name"]
    out_dir = tempfile.mkdtemp(prefix=f"org{org_id}_")
    expected = int(item["payload"].get("expected") or 0)
    tenders = []
    for attempt in range(2):  # an empty result for a populated org can be a transient block; retry on a fresh session
        try:
            tenders = list_tenders(org_id, out_dir, args.max_tenders, args.retries)
        except Exception as e:
            hub.block()
            hub.note(f"org {org_id}: search failed ({type(e).__name__})")
            return "failed"
        if tenders or not expected:
            break
    if not args.max_tenders and expected and len(tenders) < expected * 0.9:
        hub.block()
        hub.note(f"org {org_id} {name[:40]}: listed {len(tenders)} of {expected} expected")
        if not tenders:
            return "failed"  # nothing to fetch; do not mark done
    hub.note(f"org {org_id} {name[:60]}: {len(tenders)} tenders")
    state = {"failed": 0}

    def one(t):
        if time.time() > deadline:
            state["failed"] += 1
            return
        try:
            details, *_ = so.fetch_tender_details(t.get("view_link"), t["tender_id"], org_id=org_id,
                                                  output_dir=out_dir, from_date=FROM_DATE, to_date=TO_DATE)
            if not details:
                raise RuntimeError("no details parsed")
            hub.record(t["tender_id"], {"org_id": org_id, "org_name": name, "list": t, "details": details})
        except Exception:
            state["failed"] += 1
            hub.error()

    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        list(ex.map(one, tenders))
    hub.flush()
    # An org with any failed tenders goes back to pending so a later session retries it; records are upserts.
    return "pending" if state["failed"] else "done"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job-index", type=int, default=0)
    ap.add_argument("--total-jobs", type=int, default=1)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--retries", type=int, default=10)
    ap.add_argument("--max-tenders", type=int, default=0, help="cap tenders per org (smoke tests)")
    ap.add_argument("--max-orgs", type=int, default=0, help="stop after this many orgs (smoke tests)")
    ap.add_argument("--max-hours", type=float, default=5.5)
    args = ap.parse_args()

    deadline = time.time() + args.max_hours * 3600
    hub = HubClient(PROJECT, args.job_index, args.total_jobs, note="fy2025-26 crawl")
    done = 0
    try:
        while time.time() < deadline and not (args.max_orgs and done >= args.max_orgs):
            items = hub.claim(1)
            if not items:
                break
            item = items[0]
            status = crawl_org(hub, item, args, deadline)
            hub.complete([item["key"]], status)
            done += 1
        hub.close("finished", f"{done} orgs")
    except BaseException as e:
        hub.close("failed", f"{type(e).__name__}: {e}"[:200])
        raise


if __name__ == "__main__":
    main()
