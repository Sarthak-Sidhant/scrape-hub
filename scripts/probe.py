#!/usr/bin/env python3
"""Check that the sites we scrape answer, and record it for the dashboard and for the runners' auto-pause.

Run once a minute by the scrape-hub-probe systemd timer, with the hub's environment. Targets come from PROBE_TARGETS,
"name=url" pairs separated by spaces. A target counts as up when the page answers 200 within the timeout and has a form
(for the eprocure search page: the real application, not just the static front page that still answers when the app is stuck).
"""
import os
import time

import psycopg
import requests

TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "20"))
DEFAULT = "eprocure=https://eprocure.gov.in/eprocure/app?page=WebTenderStatusLists&service=page"


def check(url):
    t = time.perf_counter()
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (scrape-hub uptime check)"})
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
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as c:
        for name, url in targets.items():
            ok, ms, detail = check(url)
            c.execute("INSERT INTO probes (target, ok, ms, detail) VALUES (%s, %s, %s, %s)", (name, ok, ms, detail))
            print(f"{name}: {'up' if ok else 'DOWN'} {ms} ms {detail}")
        c.execute("DELETE FROM probes WHERE at < now() - interval '14 days'")


if __name__ == "__main__":
    main()
