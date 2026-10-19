"""How big everything is and is getting: the database on the VPS disk, and the object storage buckets with what they cost.

- sizes: every 10 minutes the hub notes the database, its biggest parts and the free disk (sample()).
- upload_ledger: bytes and files the runners reported uploading, rolled up per hour and project for good (rollup()); the metrics they
  come from are archived after two days.
- bucket_usage: a daily full listing of each bucket (scripts/bucket_scan.py), the ground truth; between listings the ledger adds what
  was uploaded since.
"""
import os
import time

from . import config

# list prices, per TB (10^12 bytes) and month; set them in the hub's environment if your plan differs. Both have 10 GB free.
PRICE_TB_MONTH = {"b2": float(os.environ.get("B2_PRICE_TB_MONTH", "6.95")), "r2": float(os.environ.get("R2_PRICE_TB_MONTH", "15"))}
FREE_BYTES = 10e9
SAMPLE_EVERY_S = 600


def sample(c):
    """Note the sizes now, at most once per SAMPLE_EVERY_S across all hub workers."""
    if not c.execute("SELECT pg_try_advisory_xact_lock(748212) AS ok").fetchone()["ok"]:
        return
    if c.execute("SELECT 1 FROM sizes WHERE at > now() - make_interval(secs => %s) LIMIT 1", (SAMPLE_EVERY_S - 30,)).fetchone():
        return
    st = os.statvfs("/")
    rows = [("disk_free", st.f_bavail * st.f_frsize), ("disk_total", st.f_blocks * st.f_frsize)]
    rows += [(r["k"], r["v"]) for r in c.execute("""
        SELECT 'db' AS k, pg_database_size(current_database()) AS v
        UNION ALL SELECT 'records_heap', pg_relation_size('records')
        UNION ALL SELECT 'records_toast', coalesce((SELECT pg_total_relation_size(reltoastrelid) FROM pg_class WHERE relname='records'), 0)
        UNION ALL SELECT 'records_indexes', pg_indexes_size('records')
        UNION ALL SELECT 'records_rows', (SELECT reltuples::bigint FROM pg_class WHERE relname='records')""").fetchall()]
    with c.cursor() as cur:
        cur.executemany("INSERT INTO sizes (what, bytes) VALUES (%s, %s)", rows)


def rollup(c):
    """Fold the runners' upload counters into the hourly ledger; the last three hours are recounted so late metrics are not lost."""
    if not c.execute("SELECT pg_try_advisory_xact_lock(748213) AS ok").fetchone()["ok"]:
        return
    since = "now() - interval '3 hours'" if c.execute("SELECT 1 FROM upload_ledger LIMIT 1").fetchone() else "'-infinity'::timestamptz"
    c.execute(f"""INSERT INTO upload_ledger (hour, project, store, bytes, files)
                  SELECT date_trunc('hour', m.at), m.project, coalesce(p.settings->>'storage', 'r2'),
                         coalesce(sum(m.value) FILTER (WHERE m.name='uploaded_bytes'), 0)::bigint,
                         coalesce(sum(m.value) FILTER (WHERE m.name='uploaded_files'), 0)::bigint
                  FROM metrics m LEFT JOIN projects p ON p.name = m.project
                  WHERE m.name IN ('uploaded_bytes', 'uploaded_files') AND m.at >= date_trunc('hour', {since})
                  GROUP BY 1, 2, 3
                  ON CONFLICT (hour, project) DO UPDATE SET bytes = EXCLUDED.bytes, files = EXCLUDED.files, store = EXCLUDED.store""")


def tick(c):
    """Called by the hub's janitor once a minute."""
    sample(c)
    rollup(c)


def _series(c, what, hours):
    return [(r["t"], r["b"]) for r in c.execute("""SELECT extract(epoch FROM at)::bigint AS t, bytes AS b FROM sizes
                                                    WHERE what=%s AND at > now() - make_interval(hours => %s) ORDER BY at""", (what, hours)).fetchall()]


def _per_hour(series, hours=3):
    """Growth in bytes per hour over the newest `hours` of a series of (epoch, bytes)."""
    if len(series) < 2:
        return None
    end = series[-1]
    start = next((p for p in series if p[0] >= end[0] - hours * 3600), series[0])
    return (end[1] - start[1]) / ((end[0] - start[0]) / 3600) if end[0] > start[0] else None


def report(c, left_by_family=None):
    """Everything the overview's storage section shows. left_by_family: {family: records still to save} for the projections."""
    db = _series(c, "db", 72)
    disk_free = _series(c, "disk_free", 72)
    now = {r["what"]: r["bytes"] for r in c.execute("SELECT DISTINCT ON (what) what, bytes FROM sizes ORDER BY what, at DESC").fetchall()}
    parts = {r["k"]: r["v"] for r in c.execute("""
        SELECT relname AS k, pg_total_relation_size(c.oid) AS v FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND relkind = 'r' ORDER BY 2 DESC LIMIT 6""").fetchall()}
    db_rate = _per_hour(db)
    st = os.statvfs("/")
    free = st.f_bavail * st.f_frsize
    rows = now.get("records_rows") or c.execute("SELECT reltuples::bigint AS n FROM pg_class WHERE relname='records'").fetchone()["n"]
    rec_bytes = (now.get("records_heap", 0) + now.get("records_toast", 0) + now.get("records_indexes", 0))
    per_record = rec_bytes / rows if rows and rec_bytes else None
    left = sum((left_by_family or {}).values())
    database = {
        "bytes": c.execute("SELECT pg_database_size(current_database()) AS n").fetchone()["n"], "parts": parts,
        "records": {"rows": rows, "heap": now.get("records_heap"), "toast": now.get("records_toast"), "indexes": now.get("records_indexes"),
                    "per_record": per_record},
        "growth_per_hour": db_rate, "history": db[-288:],
        "projected_final": (c.execute("SELECT pg_database_size(current_database()) AS n").fetchone()["n"] + left * per_record) if per_record and left else None,
        "disk_free": free, "disk_total": st.f_blocks * st.f_frsize, "disk_free_history": disk_free[-288:],
        "hours_to_full": free / db_rate if db_rate and db_rate > 0 else None,
        "left_records": left}

    # object storage: the latest listing per bucket, plus what the ledger says was uploaded since
    stores = []
    scans = c.execute("""SELECT DISTINCT ON (store, bucket) store, bucket, at FROM bucket_usage ORDER BY store, bucket, at DESC""").fetchall()
    for name, bucket in (("b2", config.B2_BUCKET), ("r2", config.R2_BUCKET)):
        if not bucket:
            continue
        scan = next((s for s in scans if s["store"] == name and s["bucket"] == bucket), None)
        listed = {"objects": 0, "bytes": 0, "prefixes": []}
        if scan:
            pr = c.execute("""SELECT prefix, objects, bytes FROM bucket_usage WHERE store=%s AND bucket=%s AND at=%s ORDER BY bytes DESC""",
                           (name, bucket, scan["at"])).fetchall()
            listed = {"objects": sum(r["objects"] for r in pr), "bytes": sum(r["bytes"] for r in pr), "prefixes": pr[:12], "at": scan["at"]}
        since = c.execute("""SELECT coalesce(sum(bytes), 0)::bigint AS b, coalesce(sum(files), 0)::bigint AS f FROM upload_ledger
                             WHERE store=%s AND hour >= date_trunc('hour', coalesce(%s::timestamptz, '-infinity'))""",
                          (name, scan["at"] if scan else None)).fetchone()
        hourly = [(r["t"], r["b"], r["f"]) for r in c.execute("""SELECT extract(epoch FROM hour)::bigint AS t, sum(bytes)::bigint AS b, sum(files)::bigint AS f
                                                                 FROM upload_ledger WHERE store=%s AND hour > now() - interval '72 hours'
                                                                 GROUP BY 1 ORDER BY 1""", (name,)).fetchall()]
        full = hourly[:-1][-1:]                       # the last whole hour (the upload mix changes: one bundle per tender since 20 Oct)
        per_hour = sum(h[1] for h in full) / len(full) if full else None
        per_file = sum(h[1] for h in full) / max(1, sum(h[2] for h in full)) if full else None
        total = listed["bytes"] + since["b"]
        price = PRICE_TB_MONTH[name]
        cost = lambda b: max(0.0, b - FREE_BYTES) / 1e12 * price
        proj = total + left * per_file if name == "b2" and per_file and left else None     # GePNIC's tenders go to B2, one bundle each
        stores.append({"store": name, "bucket": bucket, "price_tb_month": price, "listed": listed, "uploaded_since_listing": since["b"],
                       "files_since_listing": since["f"], "bytes": total, "objects": listed["objects"] + since["f"],
                       "per_hour": per_hour, "per_file": per_file, "hourly": hourly, "cost_month": cost(total),
                       "projected_bytes": proj, "projected_cost_month": cost(proj) if proj else None})
    return {"database": database, "stores": stores, "generated": time.time()}
