#!/usr/bin/env python3
"""Check that the sites we scrape answer, and record it for the dashboard and for the runners' auto-pause.

Run once a minute by the scrape-hub-probe systemd timer, with the hub's environment. Two kinds of target:
- PROBE_TARGETS, "name=url" pairs separated by spaces (default: eprocure's search page). A project's `settings.probe` names one of these,
  and runners hold while it is down. A target counts as up when the page answers 200 within the timeout and has a form (the real
  application, not just the static front page that still answers when the app is stuck).
- Every portal in data/gepnic_portals.json, as target "gepnic:<portal>", for the /portals page (set PROBE_FLEET=0 to skip). They are checked
  in parallel so the whole fleet takes about as long as the slowest portal.
Old checks are never deleted here: scripts/archive_telemetry.py moves them to object storage after 30 days.
"""
import concurrent.futures as cf
import os
import sys
import time

import psycopg
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrape_hub import fleet  # noqa: E402

TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "20"))
DEFAULT = "eprocure=https://eprocure.gov.in/eprocure/app?page=WebTenderStatusLists&service=page"


def check(url):
    t = time.perf_counter()
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": fleet.UA})
        ms = int((time.perf_counter() - t) * 1000)
        if r.status_code != 200:
            return False, ms, f"HTTP {r.status_code}"
        if "<form" not in r.text.lower():
            return False, ms, "answered but the page has no form"
        return True, ms, ""
    except Exception as e:
        return False, int((time.perf_counter() - t) * 1000), f"{type(e).__name__}: {str(e)[:120]}"


def main():
    targets = dict(p.split("=", 1) for p in os.environ.get("PROBE_TARGETS", DEFAULT).split() if "=" in p)
    results = []
    with cf.ThreadPoolExecutor(24) as ex:
        jobs = {ex.submit(check, url): name for name, url in targets.items()}
        if os.environ.get("PROBE_FLEET", "1") != "0":
            jobs.update({ex.submit(fleet.check, p, None, TIMEOUT): fleet.TARGET_PREFIX + p["portal"] for p in fleet.load()})
        for f in cf.as_completed(jobs):
            results.append((jobs[f], *f.result()))
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as c:
        with c.cursor() as cur:
            cur.executemany("INSERT INTO probes (target, ok, ms, detail) VALUES (%s, %s, %s, %s)", results)
    down = sorted(n for n, ok, _, _ in results if not ok)
    print(f"{len(results) - len(down)}/{len(results)} up" + (f"; down: {', '.join(down)}" if down else ""))


if __name__ == "__main__":
    main()
