"""The GePNIC portal fleet: which portals exist, which server each runs on, how to check one, and how a capacity ramp decides
that a server has had enough. Used by scripts/probe.py (every minute), scripts/ramp.py (twice a day) and the /portals page."""
import json
import statistics
import time
from collections import OrderedDict
from pathlib import Path

REGISTRY = Path(__file__).resolve().parent.parent / "data" / "gepnic_portals.json"
TARGET_PREFIX = "gepnic:"          # probe target name for a portal: gepnic:<portal>
UA = "Mozilla/5.0 (scrape-hub uptime check)"


def load():
    """The registry as a list of portal dicts (portal, title, kind, app_url, check_url, ui, server_ip, tomcat, tenders_june2026, organisations)."""
    doc = json.loads(REGISTRY.read_text())
    return doc["portals"]


def source_note():
    return json.loads(REGISTRY.read_text()).get("source", "")


def servers(portals=None):
    """Portals grouped by server IP, biggest server (by tenders) first: OrderedDict ip -> [portal, ...]."""
    groups = {}
    for p in portals if portals is not None else load():
        groups.setdefault(p["server_ip"], []).append(p)
    for ps in groups.values():
        ps.sort(key=lambda p: -p["tenders_june2026"])
    return OrderedDict(sorted(groups.items(), key=lambda kv: -sum(p["tenders_june2026"] for p in kv[1])))


def page_ok(portal, status, text):
    """Is this answer the live application? GePNIC: the search page with its form (the static front page keeps answering when the
    application is stuck). GePNIC 2.0 (Manipur): the API's own health answer."""
    if status != 200:
        return False, f"HTTP {status}"
    if portal.get("ui") == "gepnic2":
        return ("Server is up" in text), ("" if "Server is up" in text else "API did not say it is up")
    return ("<form" in text.lower()), ("" if "<form" in text.lower() else "answered but the page has no form")


def check(portal, session=None, timeout=20):
    """One reachability check: (ok, ms, detail)."""
    import requests
    s = session or requests
    t = time.perf_counter()
    try:
        r = s.get(portal["check_url"], timeout=timeout, headers={"User-Agent": UA})
        ms = int((time.perf_counter() - t) * 1000)
        ok, why = page_ok(portal, r.status_code, r.text)
        return ok, ms, why
    except Exception as e:  # noqa: BLE001 - any failure is a failed check
        return False, int((time.perf_counter() - t) * 1000), f"{type(e).__name__}: {str(e)[:120]}"


RAMP_STEPS = (1, 4, 8, 16)
SLOW_MS = 5000


def step_verdict(samples, base_ms):
    """samples: [(seconds, ok)] from one ramp step. Returns (summary dict, stop) where stop means this step overloaded the server:
    nothing answered, 5% or more failed, the median answer time reached SLOW_MS (a server that slow is struggling whatever we send), or
    it at least doubled from the one-worker step and grew by 500 ms or more."""
    good = sorted(s for s, ok in samples if ok)
    n, fails = len(samples), sum(1 for _, ok in samples if not ok)
    med = statistics.median(good) * 1000 if good else None
    p90 = good[max(0, int(len(good) * 0.9) - 1)] * 1000 if len(good) >= 4 else None
    stop = (not good) or fails / max(n, 1) >= 0.05 or med >= SLOW_MS or (base_ms is not None and med is not None and med >= 2 * base_ms and med - base_ms >= 500)
    return {"n": n, "fail": fails, "med_ms": int(med) if med is not None else None, "p90_ms": int(p90) if p90 is not None else None}, stop
