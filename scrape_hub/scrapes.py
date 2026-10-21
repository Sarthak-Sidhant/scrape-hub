"""Every scrape on the hub as families of sources, for the overview page.

A family is one scraping job over many sites (GePNIC: 46 portals); a source is one site in it (wbtenders.gov.in); a stage is one queue
of work on that source (count, crawl). Projects say where they belong in settings.family / settings.source / settings.stage; without
that the name decides:
  gepnic-<slug>        -> family gepnic,  source <slug>, stage crawl
  gepnic-<slug>-plan   -> family gepnic,  source <slug>, stage count
  <family>-<rest>      -> family <family>, source <family>, stage <rest>   (eprocure-tenders, eprocure-plan, ...)
  <name>               -> family <name>,   source <name>,  stage main
So a new scrape shows up on the overview by itself, as its own family.
"""
import os
import re
import time

from . import fleet

# how each known family is presented; anything else gets its own name and generic words
FAMILIES = {
    "gepnic": {"title": "GePNIC portals", "unit": "tender", "main_stage": "crawl",
               "about": "Every state and PSU portal on NIC's GePNIC software, one project per portal: counted, then crawled with documents."},
    "eprocure": {"title": "CPPP eprocure", "unit": "tender", "main_stage": "tenders",
                 "about": "The central portal eprocure.gov.in, scraped Sep-Oct 2026 (the first scrape on this hub)."},
}
# families whose early records were saved without documents (being redone); their progress counts only records that have them
DOCS_SINCE = {"gepnic": "2026-10-19 20:31:00+00"}
CACHE_S = 15
SLOW_COUNTS_S = 120
_cache = {"at": 0.0, "data": None}
_slow = {"at": 0.0, "records": {}, "missing_docs": {}}


def place(name, settings):
    """(family, source, stage) of a project."""
    s = settings or {}
    if s.get("family"):
        return s["family"], s.get("source") or s["family"], s.get("stage") or "main"
    if name.startswith("gepnic-"):
        rest = name[len("gepnic-"):]
        return ("gepnic", rest[:-len("-plan")], "count") if rest.endswith("-plan") else ("gepnic", rest, "crawl")
    if "-" in name:
        fam, rest = name.split("-", 1)
        return fam, fam, rest
    return name, name, "main"


def host():
    """The hub machine itself: the crawl's real limits are its disk, CPU and memory."""
    st = os.statvfs("/")
    mem = {}
    try:
        for line in open("/proc/meminfo"):
            k, v = line.split(":", 1)
            mem[k] = int(v.split()[0]) * 1024
    except OSError:
        pass
    return {"disk_free": st.f_bavail * st.f_frsize, "disk_total": st.f_blocks * st.f_frsize, "load1": os.getloadavg()[0],
            "cpus": os.cpu_count(), "mem_avail": mem.get("MemAvailable"), "mem_total": mem.get("MemTotal")}


MISSING_DOCS = """coalesce(data->'details'->'documents', '[]'::jsonb) @> '[{"status": "not_stored"}]'::jsonb"""


def count_records(c):
    """({project: records}, {project: records still waiting for their documents} or None). Slow: reads every record (20 s or more)."""
    records = {r["project"]: r["n"] for r in c.execute("SELECT project, count(*)::bigint AS n FROM records GROUP BY 1").fetchall()}
    # only through the partial index: without it this reads every record's JSON (gigabytes). Until it exists (it is built with
    # CREATE INDEX CONCURRENTLY, which takes a while on a live table), progress counts every stored record.
    ready = c.execute("""SELECT i.indisvalid AS ok FROM pg_index i JOIN pg_class x ON x.oid = i.indexrelid
                         WHERE x.relname = 'records_missing_docs'""").fetchone()
    missing = {r["project"]: r["n"] for r in c.execute(
        f"SELECT project, count(*)::bigint AS n FROM records WHERE {MISSING_DOCS} GROUP BY 1").fetchall()} if ready and ready["ok"] else None
    return records, missing


def refresh_counts(c):
    """Recount into record_counts, from one worker at a time and at most every SLOW_COUNTS_S (run from a background thread)."""
    if not c.execute("SELECT pg_try_advisory_xact_lock(748214) AS ok").fetchone()["ok"]:
        return
    if c.execute("SELECT 1 FROM record_counts WHERE at > now() - make_interval(secs => %s) LIMIT 1", (SLOW_COUNTS_S,)).fetchone():
        return
    c.execute("SET LOCAL statement_timeout = '10min'")
    records, missing = count_records(c)
    c.execute("DELETE FROM record_counts")
    for project, n in records.items():
        c.execute("INSERT INTO record_counts (project, n, missing_docs) VALUES (%s, %s, %s)",
                  (project, n, (missing or {}).get(project, 0) if missing is not None else None))


def slow_counts(c):
    """Records per project, and per project the records still waiting for their documents (saved by a run that did not fetch them;
    partial index records_missing_docs). Read from record_counts, which a background thread keeps fresh (every SLOW_COUNTS_S), so no
    request waits for the count; counted here only before that table has ever been filled."""
    if time.time() - _slow["at"] > 20:
        rows = c.execute("SELECT project, n, missing_docs FROM record_counts").fetchall()
        if rows:
            _slow["records"] = {r["project"]: r["n"] for r in rows}
            _slow["missing_docs"] = None if any(r["missing_docs"] is None for r in rows) else {r["project"]: r["missing_docs"] for r in rows}
        else:
            _slow["records"], _slow["missing_docs"] = count_records(c)
        _slow["at"] = time.time()
    return _slow["records"], _slow["missing_docs"]


def build(c):
    projects = c.execute("""SELECT name, title, settings, coalesce((settings->>'archived')::boolean, false) AS archived
                            FROM projects ORDER BY created_at, name""").fetchall()
    queue = {}
    for r in c.execute("""SELECT project, status, count(*)::int AS n, coalesce(sum((payload->>'expected')::bigint), 0)::bigint AS expected
                          FROM queue GROUP BY 1, 2""").fetchall():
        queue.setdefault(r["project"], {})[r["status"]] = {"n": r["n"], "expected": r["expected"]}
    rate10 = {r["project"]: float(r["n"]) / 10 for r in c.execute(
        "SELECT project, sum(records) AS n FROM batches WHERE at > now() - interval '10 minutes' GROUP BY 1").fetchall()}
    rate60 = {r["project"]: float(r["n"]) / 60 for r in c.execute(
        "SELECT project, sum(records) AS n FROM batches WHERE at > now() - interval '60 minutes' GROUP BY 1").fetchall()}
    # windows finished per minute over the last hour: a tail's retries re-list tenders already stored, so tenders per minute
    # undercounts its progress; the queue draining does not (but it is noisy where windows are big and finish rarely)
    drain = {r["project"]: float(r["n"]) / 60 for r in c.execute(
        "SELECT project, count(*) AS n FROM queue WHERE status='done' AND done_at > now() - interval '60 minutes' GROUP BY 1").fetchall()}
    spark = {}
    for r in c.execute("""SELECT project, (floor(extract(epoch FROM at) / 600) * 600)::bigint AS t, sum(records)::bigint AS n
                          FROM batches WHERE at > now() - interval '6 hours' GROUP BY 1, 2""").fetchall():
        spark.setdefault(r["project"], {})[r["t"]] = r["n"]
    runs = {}
    for r in c.execute("""SELECT project, split_part(repository, '/', 1) AS acct, count(*)::int AS n FROM runs
                          WHERE state='running' AND last_seen > now() - interval '5 minutes' GROUP BY 1, 2""").fetchall():
        runs.setdefault(r["project"], {})[r["acct"]] = r["n"]
    load = {}
    for r in c.execute("""SELECT project, sum(value) FILTER (WHERE name='n_http') AS n, sum(value) FILTER (WHERE name='t_http_s') AS t,
                                 sum(value) FILTER (WHERE name='http_error') AS e
                          FROM metrics WHERE name IN ('n_http', 't_http_s', 'http_error') AND at > now() - interval '10 minutes' GROUP BY 1""").fetchall():
        if r["n"]:
            load[r["project"]] = {"s_per_req": (r["t"] or 0) / r["n"], "req_per_min": r["n"] / 10, "dropped": (r["e"] or 0) / r["n"]}
    last_adj = {r["project"]: {"at": r["at"], "reason": r["reason"]} for r in c.execute(
        """SELECT DISTINCT ON (project) project, at, reason FROM adjustments WHERE at > now() - interval '1 day' ORDER BY project, at DESC""").fetchall()}
    records, missing_docs = slow_counts(c)

    # reachability: the newest check and the last 24 hours per probe target
    probe_names = {(p["settings"] or {}).get("probe") for p in projects} - {None, ""}
    probe_last = {r["target"]: r for r in c.execute(
        """SELECT DISTINCT ON (target) target, ok, ms, detail, at FROM probes WHERE target = ANY(%s) AND at > now() - interval '30 minutes'
           ORDER BY target, at DESC""", (list(probe_names),)).fetchall()}
    probe_24 = {r["target"]: r for r in c.execute(
        """SELECT target, count(*)::int AS n, count(*) FILTER (WHERE NOT ok)::int AS failed FROM probes
           WHERE target = ANY(%s) AND at > now() - interval '24 hours' GROUP BY 1""", (list(probe_names),)).fetchall()}
    probe_hours = {}      # the last 24 hours, one cell an hour: [hour epoch, checks, failed, median ms of the answered ones]
    for r in c.execute("""SELECT target, extract(epoch FROM date_trunc('hour', at))::bigint AS h, count(*)::int AS n,
                                 count(*) FILTER (WHERE NOT ok)::int AS failed, (percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) FILTER (WHERE ok))::int AS ms
                          FROM probes WHERE target = ANY(%s) AND at > date_trunc('hour', now()) - interval '23 hours' GROUP BY 1, 2 ORDER BY 2""",
                       (list(probe_names),)).fetchall():
        probe_hours.setdefault(r["target"], []).append([r["h"], r["n"], r["failed"], r["ms"]])
    probe_5 = {}
    for r in c.execute("""SELECT target, ok FROM (SELECT target, ok, row_number() OVER (PARTITION BY target ORDER BY at DESC) AS k FROM probes
                          WHERE target = ANY(%s) AND at > now() - interval '10 minutes') x WHERE k <= 5""", (list(probe_names),)).fetchall():
        probe_5.setdefault(r["target"], []).append(r["ok"])

    titles = {fleet.TARGET_PREFIX + p["portal"]: p for p in fleet.load()}
    now10 = int(time.time() // 600 * 600)
    sources = {}
    for p in projects:
        fam, src, stage = place(p["name"], p["settings"])
        s = sources.setdefault((fam, src), {"family": fam, "source": src, "stages": {}, "probe": None})
        st = p["settings"] or {}
        if st.get("probe"):
            s["probe"] = s["probe"] or st["probe"]
        q = queue.get(p["name"], {})
        s["stages"][stage] = {
            "project": p["name"], "title": p["title"], "archived": bool(p.get("archived")),
            "windows": {k: v["n"] for k, v in q.items()}, "expected": {k: v["expected"] for k, v in q.items()},
            "records": records.get(p["name"], 0), "missing_docs": missing_docs.get(p["name"], 0) if fam in DOCS_SINCE and missing_docs is not None else None,
            "rate_10m": rate10.get(p["name"], 0.0), "rate_60m": rate60.get(p["name"], 0.0), "runners": runs.get(p["name"], {}),
            "drain_per_min": drain.get(p["name"], 0.0),
            "load": load.get(p["name"]), "last_change": last_adj.get(p["name"]),
            "throttle": {"threads": st.get("threads"), "windows": st.get("max_claimed"), "cap": (st.get("adaptive") or {}).get("max_windows"),
                         "paused": bool(st.get("paused"))} if st.get("adaptive") or st.get("threads") else None,
            "spark": [spark.get(p["name"], {}).get(now10 - (35 - i) * 600, 0) for i in range(36)]}

    out_sources, fams = [], {}
    for (fam, src), s in sources.items():
        meta = FAMILIES.get(fam, {})
        main = s["stages"].get(meta.get("main_stage")) or next(iter(s["stages"].values()))
        portal = titles.get(s["probe"] or "") or titles.get(fleet.TARGET_PREFIX + src)
        if fam == "gepnic" and not portal:
            portal = next((v for v in titles.values() if fleet_slug(v["portal"]) == "gepnic-" + src), None)
        w, e = main["windows"], main["expected"]
        expected_total = sum(e.values())
        parked = e.get("parked", 0)
        open_w = w.get("pending", 0) + w.get("claimed", 0)
        # for a family that switched documents on part-way, a tender counts once it is stored with them (or has none to fetch)
        saved = main["records"] - main["missing_docs"] if main["missing_docs"] is not None else main["records"]
        target = max(0, expected_total - parked)
        rate = sum(st["rate_10m"] for st in s["stages"].values())
        pr = probe_last.get(s["probe"]) if s["probe"] else None
        last5 = probe_5.get(s["probe"], [])
        down = bool(s["probe"]) and not (len(last5) >= 2 and last5[0] and last5[1]) and sum(1 for x in last5 if not x) >= 3
        if w and not open_w and not w.get("failed") and w.get("parked"):
            status = "parked"
        elif w and not open_w and not w.get("failed") and not w.get("parked"):
            status = "done"
        elif down:
            status = "down"
        elif main["archived"] and not rate and not open_w and not w.get("failed"):
            status = "done" if w.get("done") else "idle"     # archived only hides a project; the queue decides whether it is done
        elif rate > 0 or any(st["runners"] for st in s["stages"].values()):
            ld = main["load"]
            status = "slow" if (ld and ld["s_per_req"] >= 3) or (pr and pr["ms"] and pr["ms"] >= 3000) else "running"
        else:
            status = "idle"
        left = max(0, target - saved) if fam in DOCS_SINCE else e.get("pending", 0) + e.get("claimed", 0) + e.get("failed", 0)
        p24 = probe_24.get(s["probe"]) if s["probe"] else None
        row = {
            "family": fam, "source": src, "title": portal["title"] if portal else (main["title"] or src),
            "site": portal["portal"] if portal else None, "url": portal["app_url"] if portal else None, "status": status,
            "probe": {"ok": pr["ok"], "ms": pr["ms"], "detail": (pr["detail"] or "")[:160], "at": pr["at"]} if pr else None,
            "uptime_24h": (1 - p24["failed"] / p24["n"]) if p24 and p24["n"] else None, "probe_target": s["probe"],
            "hours": probe_hours.get(s["probe"], []) if s["probe"] else [],
            "expected": expected_total, "parked": parked, "target": target, "saved": saved, "records": main["records"], "left": left,
            "rate_10m": rate, "rate_60m": sum(st["rate_60m"] for st in s["stages"].values()),
            # over the last hour, so a portal that is down for a few minutes does not show a two-year ETA
            # the sooner of two clocks: tenders left over tenders per minute (too slow in a tail, whose retries re-list stored tenders)
            # and windows left over windows finished per minute (noisy where windows are big); both over the last hour
            "eta_s": min([x for x in (eta(left, sum(st["rate_60m"] for st in s["stages"].values()) or rate),
                                      eta(open_w + w.get("failed", 0), main["drain_per_min"])) if x] or [None])
                     if status not in ("parked", "done") else None,
            "windows": w, "throttle": main["throttle"], "load": main["load"], "last_change": main["last_change"],
            "runners": {k: v for st in s["stages"].values() for k, v in st["runners"].items()},
            "spark": [sum(x) for x in zip(*[st["spark"] for st in s["stages"].values()])],
            "stages": {k: {kk: v[kk] for kk in ("project", "windows", "records", "rate_10m")} for k, v in s["stages"].items()}}
        out_sources.append(row)
        f = fams.setdefault(fam, {"family": fam, "title": meta.get("title", fam), "unit": meta.get("unit", "record"), "about": meta.get("about", ""),
                                  "sources": 0, "status": {}, "expected": 0, "parked": 0, "target": 0, "saved": 0, "records": 0, "left": 0,
                                  "rate_10m": 0.0, "runners": {}, "tail_eta_s": None,
                                  "docs_since": DOCS_SINCE.get(fam) if missing_docs is not None else None})
        f["sources"] += 1
        f["status"][status] = f["status"].get(status, 0) + 1
        for k in ("expected", "parked", "target", "saved", "records", "left", "rate_10m"):
            f[k] += row[k]
        for k, v in row["runners"].items():
            f["runners"][k] = f["runners"].get(k, 0) + v
        if row["eta_s"] and status not in ("parked", "done"):
            f["tail_eta_s"] = max(f["tail_eta_s"] or 0, row["eta_s"])
    for f in fams.values():
        f["eta_s"] = eta(f["left"], f["rate_10m"])
    order = sorted(fams.values(), key=lambda f: (-f["rate_10m"], -f["records"]))
    db_bytes = c.execute("SELECT pg_database_size(current_database()) AS n").fetchone()["n"]
    totals = {"records": sum(records.values()), "rate_10m": sum(rate10.values()),
              "rate_5m": float(c.execute("SELECT coalesce(sum(records), 0) AS n FROM batches WHERE at > now() - interval '5 minutes'").fetchone()["n"]) / 5,
              "runners": {}, "sources": len(out_sources), "sources_up": sum(1 for s in out_sources if s["probe"] and s["probe"]["ok"]),
              "sources_checked": sum(1 for s in out_sources if s["probe"])}
    for s in out_sources:
        for k, v in s["runners"].items():
            totals["runners"][k] = totals["runners"].get(k, 0) + v
    return {"families": order, "sources": out_sources, "totals": totals, "host": {**host(), "db_bytes": db_bytes}, "generated": time.time()}


def eta(left, per_min):
    return int(left / per_min * 60) if per_min and per_min > 0 and left else None


def fleet_slug(portal):
    """wbtenders.gov.in -> gepnic-wbtenders (the project naming of the GePNIC scraper)."""
    from .app import project_slug
    return project_slug(portal)


def cached(c):
    if not _cache["data"] or time.time() - _cache["at"] > CACHE_S:
        _cache["data"], _cache["at"] = build(c), time.time()
    return _cache["data"]


def adjustments(c, limit=60, caps_only=False):
    """The throttle's latest decisions across every project (newest first), with the project's family and source."""
    rows = c.execute(f"""SELECT a.at, a.project, a.reason, a.threads_from, a.threads_to, a.claimed_from, a.claimed_to, a.s_per_req, p.settings
                         FROM adjustments a LEFT JOIN projects p ON p.name = a.project
                         {"WHERE a.reason ~ 'window cap [0-9]+ -> [0-9]+|restored'" if caps_only else ""}
                         ORDER BY a.at DESC LIMIT %s""", (limit,)).fetchall()
    out = []
    for r in rows:
        fam, src, stage = place(r["project"], r["settings"])
        out.append({"at": r["at"], "family": fam, "source": src, "stage": stage, "reason": r["reason"], "threads": [r["threads_from"], r["threads_to"]],
                    "windows": [r["claimed_from"], r["claimed_to"]], "s_per_req": r["s_per_req"]})
    return out


TRAFFIC_NAMES = ("n_http", "t_http_s", "http_error", "http_503", "http_429", "http_403", "doc_stored", "doc_refused", "doc_failed",
                 "sessions_new", "sessions_renewed", "captcha_ok", "captcha_fail", "uploaded_bytes", "uploaded_files", "n_tenders")
_traffic = {"at": 0.0, "data": None}


def traffic(c):
    """What our runners send to the sites and pull from them, from the counters they report (cached a minute): per minute over the last
    10 minutes in all and per project, and a 2-hour series in 5-minute buckets. Page requests (n_http) and document downloads are counted
    separately; a login is a search page, a captcha and a submit."""
    if _traffic["data"] and time.time() - _traffic["at"] < 60:
        return _traffic["data"]
    names = list(TRAFFIC_NAMES)
    per = {}
    for r in c.execute("""SELECT project, name, sum(value) AS v FROM metrics WHERE at > now() - interval '10 minutes' AND name = ANY(%s)
                          GROUP BY 1, 2""", (names,)).fetchall():
        per.setdefault(r["project"], {})[r["name"]] = float(r["v"]) / 10
    series = {}
    for r in c.execute("""SELECT (floor(extract(epoch FROM at) / 300) * 300)::bigint AS t, name, sum(value) AS v FROM metrics
                          WHERE at > now() - interval '2 hours' AND name = ANY(%s) GROUP BY 1, 2 ORDER BY 1""", (names,)).fetchall():
        series.setdefault(r["t"], {})[r["name"]] = float(r["v"]) / 5

    def shape(m):
        n = m.get("n_http", 0)
        docs = m.get("doc_stored", 0) + m.get("doc_refused", 0) + m.get("doc_failed", 0)
        caps = m.get("captcha_ok", 0) + m.get("captcha_fail", 0)
        return {"pages": n, "docs": docs, "docs_refused": m.get("doc_refused", 0), "logins": m.get("sessions_new", 0),
                "renewed": m.get("sessions_renewed", 0), "captchas": caps, "captcha_ok": (m.get("captcha_ok", 0) / caps) if caps else None,
                "s_per_req": (m.get("t_http_s", 0) / n) if n else None, "dropped": m.get("http_error", 0),
                "throttled": m.get("http_503", 0) + m.get("http_429", 0) + m.get("http_403", 0),
                "bytes": m.get("uploaded_bytes", 0), "tenders": m.get("n_tenders", 0),
                "requests": n + docs + 3 * m.get("sessions_new", 0) + caps}
    total = {}
    for m in per.values():
        for k, v in m.items():
            total[k] = total.get(k, 0) + v
    by_source = {}
    for proj, m in per.items():
        fam, src, stage = place(proj, None)
        b = by_source.setdefault(f"{fam}/{src}", {})
        for k, v in m.items():
            b[k] = b.get(k, 0) + v
    data = {"per_min": shape(total), "sources": {k: shape(v) for k, v in by_source.items()},
            "series": [{"t": t, **shape(m)} for t, m in sorted(series.items())], "generated": time.time()}
    _traffic["data"], _traffic["at"] = data, time.time()
    return data


TRAFFIC_COLS = {"pages": ("n_http",), "docs": ("doc_stored", "doc_refused", "doc_failed"), "docs_refused": ("doc_refused",),
                "logins": ("sessions_new",), "captcha_ok": ("captcha_ok",), "captcha_fail": ("captcha_fail",), "bytes": ("uploaded_bytes",),
                "dropped": ("http_error",), "throttled": ("http_503", "http_429", "http_403"), "t_http": ("t_http_s",), "tenders": ("n_tenders",)}
_trange = {}


def traffic_range(c, minutes):
    """Traffic per source over the last `minutes` (60, 360 or 1440) in buckets of 1, 5 or 15 minutes, every value per minute:
    {"t": [bucket starts], "sources": {family/source: {col: [values]}}}. Cached for one bucket (at least a minute)."""
    minutes = 60 if minutes <= 60 else 360 if minutes <= 360 else 1440
    bucket = {60: 1, 360: 5, 1440: 15}[minutes]
    hit = _trange.get(minutes)
    if hit and time.time() - hit[0] < max(60, bucket * 60):
        return hit[1]
    names = sorted({n for cols in TRAFFIC_COLS.values() for n in cols})
    end = int(time.time() // (bucket * 60) * (bucket * 60)) - bucket * 60      # the newest whole bucket (the current one is still filling)
    ts = [end - (minutes // bucket - 1 - i) * bucket * 60 for i in range(minutes // bucket)]
    idx = {t: i for i, t in enumerate(ts)}
    src = {}
    for r in c.execute("""SELECT project, (floor(extract(epoch FROM at) / %(b)s) * %(b)s)::bigint AS t, name, sum(value) AS v FROM metrics
                          WHERE at >= to_timestamp(%(start)s) AND name = ANY(%(names)s) GROUP BY 1, 2, 3""",
                       {"b": bucket * 60, "start": ts[0], "names": names}).fetchall():
        i = idx.get(r["t"])
        if i is None:
            continue
        fam, s, _ = place(r["project"], None)
        cols = src.setdefault(f"{fam}/{s}", {k: [0.0] * len(ts) for k in TRAFFIC_COLS})
        for col, members in TRAFFIC_COLS.items():
            if r["name"] in members:
                cols[col][i] += float(r["v"]) / bucket
    for cols in src.values():
        for k in cols:
            cols[k] = [round(v, 2) for v in cols[k]]
    data = {"minutes": minutes, "bucket": bucket, "t": ts, "sources": src, "generated": time.time()}
    _trange[minutes] = (time.time(), data)
    return data


def _kind(reason):
    r = reason or ""
    if "went down at" in r:
        return "down"
    if r.startswith("choked at"):
        return "choke"
    if r.startswith("10 healthy minutes"):
        return "raise"
    if "restored to" in r:
        return "restore"
    if "is down: everything holds" in r:
        return "hold"
    if r.endswith(": halve"):
        return "halve"
    if r.endswith("ease off"):
        return "ease"
    if r.endswith(": more"):
        return "more"
    return "other"


_thist = {}


def throttle_history(c, hours=24):
    """Everything the throttle decided per source over the last `hours`, for the throttle page (cached a minute):
    per source the windows it allowed and the threads per process after every decision, the window cap after every cap change, the
    outages of its portal (from the hub's checks) and its settings now."""
    hours = max(1, min(int(hours), 168))
    hit = _thist.get(hours)
    if hit and time.time() - hit[0] < 60:
        return hit[1]
    from .app import find_outages
    since = time.time() - hours * 3600
    projects = {r["name"]: r["settings"] or {} for r in c.execute("SELECT name, settings FROM projects WHERE settings ? 'adaptive'").fetchall()}
    rows = c.execute("""SELECT project, extract(epoch FROM at)::float8 AS t, threads_from, threads_to, claimed_from, claimed_to, s_per_req, reason
                        FROM adjustments WHERE at > to_timestamp(%s) ORDER BY at""", (since,)).fetchall()
    first_before = {r["project"]: r for r in c.execute(
        """SELECT DISTINCT ON (project) project, threads_to, claimed_to, reason FROM adjustments WHERE at <= to_timestamp(%s)
           ORDER BY project, at DESC""", (since,)).fetchall()}
    out = {}
    for name, st in projects.items():
        fam, src, stage = place(name, st)
        if stage == "count":
            continue
        out[name] = {"family": fam, "source": src, "project": name, "steps": [], "caps": [], "events": [], "outages": [],
                     "now": {"threads": st.get("threads"), "windows": st.get("max_claimed"), "cap": (st.get("adaptive") or {}).get("max_windows"),
                             "fell_at": (st.get("adaptive") or {}).get("fell_at")}, "probe": st.get("probe")}
        b = first_before.get(name)
        if b:
            out[name]["start"] = {"threads": b["threads_to"], "windows": b["claimed_to"]}
    for r in rows:
        s = out.get(r["project"])
        if not s:
            continue
        k = _kind(r["reason"])
        m = re.search(r"window cap (\d+) -> (\d+)", r["reason"] or "") or re.search(r"restored to (\d+)", r["reason"] or "")
        if m:
            if not s["caps"] and m.lastindex == 2:
                s["caps"].append([since, int(m.group(1))])
            s["caps"].append([r["t"], int(m.group(m.lastindex))])
        if r["claimed_to"] is not None and k not in ("down", "choke", "raise", "restore"):
            s["steps"].append([r["t"], r["claimed_to"], r["threads_to"], k, round(r["s_per_req"], 2) if r["s_per_req"] is not None else None])
        if k in ("down", "choke", "raise", "restore", "halve", "hold"):
            s["events"].append([r["t"], k, (r["reason"] or "")[:160]])
    targets = [s["probe"] for s in out.values() if s.get("probe")]
    checks = {}
    for r in c.execute("""SELECT target, extract(epoch FROM at)::float8 AS t, ok FROM probes WHERE target = ANY(%s) AND at > to_timestamp(%s)
                          ORDER BY at""", (targets, since)).fetchall():
        checks.setdefault(r["target"], []).append((r["t"], r["ok"]))
    for s in out.values():
        if s.get("probe") and s["probe"] in checks:
            s["outages"] = [[a, b, g] for a, b, g in find_outages(checks[s["probe"]])]
    data = {"hours": hours, "since": since, "now": time.time(), "sources": list(out.values())}
    _thist[hours] = (time.time(), data)
    return data
