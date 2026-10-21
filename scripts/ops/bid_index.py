"""Bid index: who bid on which tender, and who won, across every tender project, so "all tenders where X bid or won" is a lookup
instead of a read of the whole `records` table (79 GB of JSON in Oct 2026).

Tables (schema eprocure, owned by postgres, readable by scrape_hub):
  bidders       one row per distinct bidder: `key` (lowercase, letters and digits only, "M/s" and trailing Pvt/Ltd/LLP dropped, so
                "M/S eMudhra Limited" and "EMUDHRA LTD." are one bidder), `name` (the spelling first seen)
  bid_index     one row per bid (pid, tender_id, bidder_id, bidder_name as written, status_id, flags, awarded_value, bid_number);
                flags: 1 = in the bid list, 2 = winner (winners / AOC award list / bid status Accepted-AOC), 4 = L1. A winner missing from the bid list gets a row with flag 2
  bidder_stats  per bidder: tenders, bids, wins, L1s, first and last year (rebuilt after each build or refresh)
  bid_projects, bid_statuses   small lookups for pid and status_id
  bid_index_meta               build cursor and the refresh watermark

Gentle on the database: reads `records` in primary-key batches (ids by index, then the batch's rows), one short transaction per batch,
pausing between batches; the rows of a batch and the cursor are committed together, so an interrupted build resumes exactly.
Refresh re-reads only tenders whose scraped_at moved past the watermark (index records_scraped_at, created by `init`).

Run as postgres from /opt/scrape-hub (the hub venv has psycopg):
  sudo -u postgres .venv/bin/python scripts/ops/bid_index.py init        # tables + CREATE INDEX CONCURRENTLY records_scraped_at
  sudo -u postgres .venv/bin/python scripts/ops/bid_index.py build       # full pass (hours); `build --resume` continues an interrupted one
  sudo -u postgres .venv/bin/python scripts/ops/bid_index.py refresh     # tenders scraped since the last build/refresh (nightly timer)
  sudo -u postgres .venv/bin/python scripts/ops/bid_index.py search emudhra [--json]
"""
import json
import re
import sys
import time

import psycopg

BATCH, PAUSE = 2000, 0.25
DSN = "dbname=scrape_hub"

DDL = """
CREATE SCHEMA IF NOT EXISTS eprocure;
CREATE TABLE IF NOT EXISTS eprocure.bid_projects (pid smallint PRIMARY KEY, project text UNIQUE NOT NULL);
CREATE TABLE IF NOT EXISTS eprocure.bid_statuses (status_id smallint PRIMARY KEY, status text UNIQUE NOT NULL);
CREATE TABLE IF NOT EXISTS eprocure.bidders (bidder_id integer PRIMARY KEY, key text UNIQUE NOT NULL, name text NOT NULL);
CREATE TABLE IF NOT EXISTS eprocure.bid_index (pid smallint NOT NULL, tender_id text NOT NULL, bidder_id integer NOT NULL, bidder_name text,
    status_id smallint, flags smallint NOT NULL, awarded_value numeric, bid_number text);
CREATE TABLE IF NOT EXISTS eprocure.bid_index_meta (k text PRIMARY KEY, v text);
GRANT USAGE ON SCHEMA eprocure TO scrape_hub;
GRANT SELECT ON eprocure.bid_projects, eprocure.bid_statuses, eprocure.bidders, eprocure.bid_index, eprocure.bid_index_meta TO scrape_hub;
"""

SUFFIX = {"pvt", "private", "ltd", "limited", "llp", "inc", "co", "company", "corp", "corporation"}


def key_of(name: str) -> str:
    s = (name or "").lower().replace("&", " and ")
    s = re.sub(r"\bm\s*/\s*s\b\.?|\bmessrs\b\.?", " ", s)
    words = re.findall(r"[a-z0-9]+", s)
    while len(words) > 1 and (words[-1] in SUFFIX or words[-1] == "and"):
        words.pop()
    return "".join(words)[:200]                      # a few portals put whole paragraphs in the name field; a btree key must stay small


def num(v):
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return v
    m = re.search(r"\d[\d,]*(?:\.\d+)?", str(v or ""))
    return float(m.group().replace(",", "")) if m else None


def connect():
    c = psycopg.connect(DSN, autocommit=True)
    c.execute("SET application_name='bid-index'")
    c.execute("SET statement_timeout='120s'")
    c.execute("SET work_mem='16MB'")
    return c


def meta(c, k, v=None):
    if v is None:
        r = c.execute("SELECT v FROM eprocure.bid_index_meta WHERE k=%s", (k,)).fetchone()
        return r[0] if r else None
    c.execute("INSERT INTO eprocure.bid_index_meta VALUES (%s,%s) ON CONFLICT (k) DO UPDATE SET v=excluded.v", (k, str(v)))


def projects(c):
    return [r[0] for r in c.execute("""SELECT name FROM projects WHERE name='eprocure-tenders' OR (name LIKE 'gepnic-%%' AND name NOT LIKE '%%-plan')
                                       ORDER BY name""")]


class Ids:
    """name -> small id maps for projects, statuses and bidders, loaded from the tables; new ones are written in the caller's transaction."""

    def __init__(self, c):
        self.pid = dict((p, i) for i, p in c.execute("SELECT pid, project FROM eprocure.bid_projects"))
        self.status = dict((s, i) for i, s in c.execute("SELECT status_id, status FROM eprocure.bid_statuses"))
        self.bidder = {}
        with c.cursor(name="bidders") if not c.autocommit else c.cursor() as cur:
            for i, k in cur.execute("SELECT bidder_id, key FROM eprocure.bidders"):
                self.bidder[k] = i
        self.next_bidder = max(self.bidder.values(), default=0) + 1
        self.new_bidders = []

    def project(self, w, p):
        if p not in self.pid:
            self.pid[p] = max(self.pid.values(), default=0) + 1
            w.execute("INSERT INTO eprocure.bid_projects VALUES (%s,%s)", (self.pid[p], p))
        return self.pid[p]

    def status_id(self, w, s):
        s = " ".join((s or "").split())
        if not s:
            return None
        if s not in self.status:
            self.status[s] = max(self.status.values(), default=0) + 1
            w.execute("INSERT INTO eprocure.bid_statuses VALUES (%s,%s) ON CONFLICT DO NOTHING", (self.status[s], s))
        return self.status[s]

    def bidder_id(self, name):
        k = key_of(name)
        if not k:
            return None
        if k not in self.bidder:
            self.bidder[k] = self.next_bidder
            self.new_bidders.append((self.next_bidder, k, " ".join(name.split())[:500]))
            self.next_bidder += 1
        return self.bidder[k]


READ = """SELECT record_id, d->'bids', d->'fields'->'winners', d->'fields'->>'winner_bidder_name', d->'fields'->>'l1_bidder_name',
                 d->'outcomes'->'awarded_list'
          FROM (SELECT record_id, data->'details' AS d FROM records WHERE project=%s AND record_id BETWEEN %s AND %s) x"""


def rows_for(w, ids, pid, rid, bids, winners, winner, l1, awarded):
    wins = {key_of(n): None for n in (winners or []) + [winner] if n}
    for a in awarded or []:
        if a.get("bidder"):
            wins[key_of(a["bidder"])] = num(a.get("value"))
    wins.pop("", None)
    l1k = key_of(l1) if l1 else ""
    out, seen = [], set()
    for b in bids or []:
        name = b.get("bidder_name") or ""
        bid_ = ids.bidder_id(name)
        if bid_ is None:
            continue
        k = key_of(name)
        seen.add(k)
        won = k in wins or (b.get("status") or "").replace(" ", "").lower() == "accepted-aoc"     # the central portal shows its winner only this way
        flags = 1 | (2 if won else 0) | (4 if l1k and k == l1k else 0)
        out.append((pid, rid, bid_, " ".join(name.split()), ids.status_id(w, b.get("status")), flags, wins.get(k), b.get("bid_number")))
    extra = [(n, 2) for n in (winners or []) + [winner] + [a.get("bidder") for a in awarded or []] if n]
    if l1:
        extra.append((l1, 4))
    for name, flag in extra:                          # a winner / L1 the bid list does not show (no bid list, or a differently written name)
        k = key_of(name)
        if not k or k in seen:
            continue
        seen.add(k)
        flags = (2 if k in wins else 0) | (4 if l1k and k == l1k else 0) or flag
        out.append((pid, rid, ids.bidder_id(name), " ".join(name.split()), None, flags, wins.get(k), None))
    return out


def write(w, ids, rows, cursor_kv=None):
    with w.transaction():
        if ids.new_bidders:
            with w.cursor().copy("COPY eprocure.bidders (bidder_id, key, name) FROM STDIN") as cp:
                for r in ids.new_bidders:
                    cp.write_row(r)
            ids.new_bidders = []
        if rows:
            with w.cursor().copy("COPY eprocure.bid_index FROM STDIN") as cp:
                for r in rows:
                    cp.write_row(r)
        for k, v in (cursor_kv or {}).items():
            meta(w, k, v)


def index_tenders(c, w, ids, project, lo, hi):
    pid = ids.project(w, project)
    rows = []
    for rid, bids, winners, winner, l1, awarded in c.execute(READ, (project, lo, hi)):
        rows += rows_for(w, ids, pid, rid, bids, winners, winner, l1, awarded)
    return rows


def finish(w):
    """Indexes (if missing) and the per-bidder numbers."""
    w.execute("SET statement_timeout=0")
    w.execute("SET maintenance_work_mem='256MB'")
    w.execute("CREATE INDEX IF NOT EXISTS bid_index_tender ON eprocure.bid_index (pid, tender_id)")
    w.execute("CREATE INDEX IF NOT EXISTS bid_index_bidder ON eprocure.bid_index (bidder_id)")
    with w.transaction():
        w.execute("DROP TABLE IF EXISTS eprocure.bidder_stats_new")
        w.execute("""CREATE TABLE eprocure.bidder_stats_new AS
                     SELECT bidder_id, count(DISTINCT (pid, tender_id))::int AS tenders, count(*) FILTER (WHERE flags & 1 > 0)::int AS bids,
                            count(*) FILTER (WHERE flags & 2 > 0)::int AS wins, count(*) FILTER (WHERE flags & 4 > 0)::int AS l1s,
                            min(left(tender_id, 4))::int AS first_year, max(left(tender_id, 4))::int AS last_year
                     FROM eprocure.bid_index WHERE tender_id ~ '^\\d{4}_' GROUP BY bidder_id""")
        w.execute("ALTER TABLE eprocure.bidder_stats_new ADD PRIMARY KEY (bidder_id)")
        w.execute("DROP TABLE IF EXISTS eprocure.bidder_stats")
        w.execute("ALTER TABLE eprocure.bidder_stats_new RENAME TO bidder_stats")
        w.execute("ALTER INDEX IF EXISTS eprocure.bidder_stats_new_pkey RENAME TO bidder_stats_pkey")
        w.execute("GRANT SELECT ON eprocure.bidder_stats TO scrape_hub")
    w.execute("ANALYZE eprocure.bid_index")
    w.execute("ANALYZE eprocure.bidders")


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, file=sys.stderr, flush=True)


def init():
    c = connect()
    c.execute(DDL)
    log("tables ready; building records_scraped_at (concurrently)")
    c.execute("SET statement_timeout=0")
    c.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS records_scraped_at ON records (scraped_at)")
    log("done")


def build(resume=False):
    c, w = connect(), connect()
    c.execute(DDL)
    if resume:
        start_p, start_id = meta(c, "build_project") or "", meta(c, "build_after") or ""
        log(f"resuming after {start_p} {start_id}")
    else:
        started = c.execute("SELECT now()").fetchone()[0].isoformat()
        with w.transaction():
            w.execute("TRUNCATE eprocure.bid_index, eprocure.bidders")
            w.execute("DROP INDEX IF EXISTS eprocure.bid_index_tender")
            w.execute("DROP INDEX IF EXISTS eprocure.bid_index_bidder")
            for k, v in (("state", "building"), ("build_started", started), ("build_project", ""), ("build_after", "")):
                meta(w, k, v)
        start_p, start_id = "", ""
    ids = Ids(c)
    total = 0
    for p in projects(c):
        if p < start_p:
            continue
        last, n, t0 = (start_id if p == start_p else ""), 0, time.time()
        while True:
            keys = c.execute("SELECT record_id FROM records WHERE project=%s AND record_id > %s ORDER BY record_id LIMIT %s",
                             (p, last, BATCH)).fetchall()
            if not keys:
                break
            lo, hi = keys[0][0], keys[-1][0]
            rows = index_tenders(c, w, ids, p, lo, hi)
            write(w, ids, rows, {"build_project": p, "build_after": hi})
            last, n, total = hi, n + len(keys), total + len(rows)
            if n % 100000 < BATCH:
                log(f"  {p} at {hi}: {n} tenders")
            time.sleep(PAUSE)
        log(f"{p}: {n} tenders in {time.time() - t0:.0f}s, {total} rows so far, {len(ids.bidder)} bidders")
    log("indexes and bidder_stats")
    finish(w)
    meta(w, "watermark", meta(c, "build_started"))
    meta(w, "state", "ready")
    log("build done")


def refresh():
    c, w = connect(), connect()
    if meta(c, "state") != "ready":
        log("not built yet (or a build is running): nothing to refresh")
        return
    mark = meta(c, "watermark")
    new_mark = c.execute("SELECT now()").fetchone()[0].isoformat()
    ids = Ids(c)
    want = set(projects(c))
    changed = [(p, r) for p, r in c.execute("""SELECT project, record_id FROM records WHERE scraped_at > %s::timestamptz - interval '10 minutes'
                                                ORDER BY project, record_id""", (mark,)) if p in want]
    log(f"{len(changed)} tenders scraped since {mark}")
    for i in range(0, len(changed), BATCH // 2):
        part = changed[i:i + BATCH // 2]
        rows = []
        by_p = {}
        for p, r in part:
            by_p.setdefault(p, []).append(r)
        with w.transaction():
            for p, rids in by_p.items():
                pid = ids.project(w, p)
                w.execute("DELETE FROM eprocure.bid_index WHERE pid=%s AND tender_id = ANY(%s)", (pid, rids))
                for rid, bids, winners, winner, l1, awarded in c.execute(READ.replace("record_id BETWEEN %s AND %s", "record_id = ANY(%s)"), (p, rids)):
                    rows += rows_for(w, ids, pid, rid, bids, winners, winner, l1, awarded)
            write(w, ids, rows)
        time.sleep(PAUSE)
    finish(w)
    meta(w, "watermark", new_mark)
    log("refresh done")


SEARCH = """SELECT p.project, i.tender_id, i.bidder_name, s.status, i.flags, i.awarded_value,
                   r.data->>'org_name', coalesce(r.data->'details'->'fields'->>'tender_title', r.data->'list'->>'title'),
                   r.data->'details'->'fields'->>'tender_value', r.data->'details'->'fields'->>'published_date',
                   coalesce(r.data->'details'->'fields'->>'stage', r.data->'list'->>'stage'),
                   r.data->'details'->'fields'->'winners', r.data->'details'->'fields'->>'bids_count'
            FROM eprocure.bid_index i JOIN eprocure.bid_projects p USING (pid) LEFT JOIN eprocure.bid_statuses s USING (status_id)
            JOIN records r ON r.project = p.project AND r.record_id = i.tender_id
            WHERE i.bidder_id = ANY(%s) ORDER BY i.tender_id"""


def search(text, as_json=False):
    c = connect()
    k = key_of(text)
    who = c.execute("""SELECT b.bidder_id, b.name, st.tenders, st.wins FROM eprocure.bidders b LEFT JOIN eprocure.bidder_stats st USING (bidder_id)
                       WHERE b.key LIKE %s ORDER BY st.tenders DESC NULLS LAST""", (f"%{k}%",)).fetchall()
    out = [dict(zip(("project", "tender_id", "bidder_name", "status", "flags", "awarded_value", "organisation", "title", "tender_value",
                     "published", "stage", "winners", "bids"), r)) for r in c.execute(SEARCH, ([w[0] for w in who],))] if who else []
    if as_json:
        print(json.dumps({"bidders": [dict(zip(("bidder_id", "name", "tenders", "wins"), w)) for w in who], "bids": out}, default=str))
        return
    for w in who:
        print(f"bidder {w[0]:>9}  {w[1][:60]:<60} tenders {w[2]}  wins {w[3]}")
    for r in out:
        tag = "WON" if r["flags"] & 2 else "L1 " if r["flags"] & 4 else "bid"
        print(f"{r['tender_id']:<26} {r['project'].removeprefix('gepnic-'):<20} {tag} {(r['status'] or '')[:22]:<22} {(r['organisation'] or '')[:28]:<28} {' '.join((r['title'] or '').split())[:70]}")
    print(f"{len(who)} bidder names, {len({(r['project'], r['tender_id']) for r in out})} tenders, {sum(1 for r in out if r['flags'] & 2)} won")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "init":
        init()
    elif cmd == "build":
        build(resume="--resume" in sys.argv)
    elif cmd == "refresh":
        refresh()
    elif cmd == "search" and len(sys.argv) > 2:
        search(" ".join(a for a in sys.argv[2:] if a != "--json"), "--json" in sys.argv)
    else:
        print(__doc__)
