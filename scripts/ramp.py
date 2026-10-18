#!/usr/bin/env python3
"""Measure how much load each GePNIC portal server takes right now, and save it for the /portals page (table `capacity`).

For every server in data/gepnic_portals.json: run 1, 4, 8, then 16 parallel workers for STEP seconds each. Each worker keeps one session
and keeps loading the portal's check page (the real search page). The ramp for a server stops at the first step that slows it down
(see fleet.step_verdict); `safe` is the last step that did not. Servers are measured a few at a time, and a server our own runners are
scraping is skipped so the two do not distort each other. Run twice a day by scrape-hub-ramp.timer (Indian night and afternoon).

  python scripts/ramp.py [--step 15] [--parallel 6] [--only 164.100.78.245]
"""
import argparse
import concurrent.futures as cf
import json
import os
import sys
import threading
import time

import psycopg
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrape_hub import fleet  # noqa: E402


def busy_servers(c):
    """Servers our own runners are working on now: a running run whose project's probe target names one of their portals."""
    rows = c.execute("""SELECT DISTINCT p.settings->>'probe' AS probe FROM runs r JOIN projects p ON p.name = r.project
                        WHERE r.state = 'running' AND r.last_seen > now() - interval '5 minutes'""").fetchall()
    probes = {r[0] for r in rows if r[0]}
    busy = set()
    for p in fleet.load():
        if "eprocure" in probes and p["portal"] in ("eprocure.gov.in", "eprocure.gov.in/epublish"):
            busy.add(p["server_ip"])
        if fleet.TARGET_PREFIX + p["portal"] in probes:
            busy.add(p["server_ip"])
    return busy


def one_step(portals, workers, seconds):
    out, stop = [], threading.Event()

    def work(k):
        s = requests.Session()
        s.headers["User-Agent"] = fleet.UA
        i = k
        while not stop.is_set():
            p = portals[i % len(portals)]
            i += 1
            t = time.perf_counter()
            try:
                r = s.get(p["check_url"], timeout=30)
                ok, _ = fleet.page_ok(p, r.status_code, r.text)
            except requests.RequestException:
                ok = False
            out.append((time.perf_counter() - t, ok))

    threads = [threading.Thread(target=work, args=(k,), daemon=True) for k in range(workers)]
    for t in threads:
        t.start()
    time.sleep(seconds)
    stop.set()
    for t in threads:
        t.join(timeout=35)
    return list(out)


def ramp(ip, portals, seconds):
    steps, base, safe = [], None, 0
    for w in fleet.RAMP_STEPS:
        summary, stop = fleet.step_verdict(one_step(portals, w, seconds), base)
        summary.update(workers=w, rps=round(summary["n"] / seconds, 1), stopped=stop)
        steps.append(summary)
        if base is None:
            base = summary["med_ms"]
        if stop:
            break
        safe = w
    return {"server_ip": ip, "portals": [p["portal"] for p in portals], "safe": safe, "tested_max": steps[-1]["workers"],
            "base_ms": steps[0]["med_ms"], "steps": steps}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", type=float, default=15, help="seconds per step")
    ap.add_argument("--parallel", type=int, default=6, help="servers measured at once")
    ap.add_argument("--only", default="", help="comma-separated server IPs")
    a = ap.parse_args()
    groups = fleet.servers()
    if a.only:
        groups = {ip: ps for ip, ps in groups.items() if ip in a.only.split(",")}
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as c:
        busy = busy_servers(c)
        todo = {ip: ps for ip, ps in groups.items() if ip not in busy}
        with cf.ThreadPoolExecutor(a.parallel) as ex:
            results = list(ex.map(lambda kv: ramp(kv[0], kv[1], a.step), todo.items()))
        with c.cursor() as cur:
            cur.executemany("INSERT INTO capacity (server_ip, portals, safe, tested_max, base_ms, steps, note) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s)",
                            [(r["server_ip"], r["portals"], r["safe"], r["tested_max"], r["base_ms"], json.dumps(r["steps"]), "") for r in results] +
                            [(ip, [p["portal"] for p in groups[ip]], 0, 0, None, "[]", "skipped: our runners are scraping it") for ip in busy if ip in groups])
    for r in sorted(results, key=lambda r: r["portals"][0]):
        print(f"{r['server_ip']:16} safe={r['safe']:>2} base={r['base_ms']}ms  {', '.join(r['portals'])[:60]}")
    if busy:
        print("skipped (our runners active):", ", ".join(sorted(busy)))


if __name__ == "__main__":
    main()
