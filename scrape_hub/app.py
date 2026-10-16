import json
import re
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from pydantic import BaseModel, Field

from . import config, dash_auth, db, health
from .auth import Caller, caller, client_ip

def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


WEB = Path(__file__).resolve().parent.parent / "web"


def janitor():
    """Mark long-silent runs as lost and prune old raw batch rows."""
    while True:
        time.sleep(60)
        try:
            with db.conn() as c:
                c.execute(
                    "UPDATE runs SET state='failed', finished_at=now(), note='lost: no activity' "
                    "WHERE state='running' AND last_seen < now() - make_interval(secs => %s)",
                    (config.STALL_AFTER_S * 6,),
                )
                c.execute("DELETE FROM batches WHERE at < now() - make_interval(days => %s)", (config.BATCH_RETENTION_DAYS,))
                c.execute("DELETE FROM metrics WHERE at < now() - make_interval(days => %s)", (config.BATCH_RETENTION_DAYS,))
        except Exception as e:  # keep the thread alive; the next pass retries
            print("janitor error:", e, flush=True)


@asynccontextmanager
async def lifespan(app):
    db.init_schema()
    threading.Thread(target=janitor, daemon=True).start()
    yield
    db.close()


app = FastAPI(title="scrape-hub", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


# ----------------------------------------------------------------------------- runner API

class RunStart(BaseModel):
    project: str = Field(min_length=1, max_length=64)
    job_index: int = 0
    total_jobs: int = 1
    egress_ip: str = ""
    note: str = ""


class Record(BaseModel):
    record_id: str
    data: Any


class BatchIn(BaseModel):
    records: list[Record] = []
    errors: int = 0
    blocked: int = 0
    latency_ms: int = 0
    note: Optional[str] = None
    metrics: dict[str, float] = {}


class Finish(BaseModel):
    state: str = "finished"
    note: str = ""


class Claim(BaseModel):
    project: str
    limit: int = Field(default=1, ge=1, le=100)


class Complete(BaseModel):
    keys: list[str]
    status: str = "done"
    error: str = ""
    fetched: Optional[int] = None


class ProjectIn(BaseModel):
    title: str = ""
    unit_label: str = "item"
    record_label: str = "record"
    expected_units: int = 0
    expected_records: int = 0


class QueueItem(BaseModel):
    key: str = Field(min_length=1, max_length=200)
    partition_id: Optional[int] = None
    payload: dict = {}


class QueueSeed(BaseModel):
    items: list[QueueItem]
    reset_failed: bool = False


NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,63}$")


def ensure_project(c, name: str, who: Caller):
    """The first repo to use a project claims it; afterwards only listed repos may write to it."""
    if not NAME_RE.match(name):
        raise HTTPException(422, "project name must be 2-64 chars of a-z 0-9 - _")
    c.execute("INSERT INTO projects (name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))
    row = c.execute("SELECT * FROM projects WHERE name=%s FOR UPDATE", (name,)).fetchone()
    if not row["allowed_repos"]:
        c.execute("UPDATE projects SET allowed_repos=%s WHERE name=%s", ([who.repository], name))
    elif who.repository not in row["allowed_repos"]:
        raise HTTPException(403, f"{who.repository} may not write to project {name}")
    return row


@app.put("/api/v1/projects/{name}")
def put_project(name: str, body: ProjectIn, who: Caller = Depends(caller)):
    with db.conn() as c:
        ensure_project(c, name, who)
        c.execute("""UPDATE projects SET title=%s, unit_label=%s, record_label=%s, expected_units=%s, expected_records=%s
                     WHERE name=%s""", (body.title, body.unit_label, body.record_label, body.expected_units, body.expected_records, name))
    return {"ok": True}


@app.post("/api/v1/projects/{name}/queue")
def seed_queue(name: str, body: QueueSeed, who: Caller = Depends(caller)):
    with db.conn() as c:
        ensure_project(c, name, who)
        added = 0
        with c.cursor() as cur:
            for i, it in enumerate(body.items):
                cur.execute(
                    """INSERT INTO queue (project, key, partition_id, payload) VALUES (%s,%s,%s,%s::jsonb)
                       ON CONFLICT (project, key) DO NOTHING""",
                    (name, it.key, it.partition_id if it.partition_id is not None else i, json.dumps(it.payload)))
                added += cur.rowcount
        reset = 0
        if body.reset_failed:
            reset = c.execute("UPDATE queue SET status='pending', claimed_by=NULL WHERE project=%s AND status='failed'", (name,)).rowcount
    return {"added": added, "reset": reset}


def own_run(c, run_id: int, who: Caller):
    row = c.execute("SELECT * FROM runs WHERE id=%s", (run_id,)).fetchone()
    if not row or row["repository"] != who.repository:
        raise HTTPException(404, "run not found")
    return row


@app.post("/api/v1/runs")
def start_run(body: RunStart, request: Request, who: Caller = Depends(caller)):
    with db.conn() as c:
        ensure_project(c, body.project, who)
        row = c.execute(
            """INSERT INTO runs (project, repository, workflow, gh_run_id, run_attempt, job_index, total_jobs,
                                 ref, actor, auth_kind, runner_ip, egress_ip, note)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (project, repository, gh_run_id, run_attempt, job_index)
               DO UPDATE SET state='running', last_seen=now(), finished_at=NULL,
                             runner_ip=EXCLUDED.runner_ip, egress_ip=EXCLUDED.egress_ip
               RETURNING id""",
            (body.project, who.repository, who.workflow, who.gh_run_id, who.run_attempt, body.job_index,
             body.total_jobs, who.ref, who.actor, who.kind, client_ip(request), body.egress_ip[:64], body.note),
        ).fetchone()
    return {"run_id": row["id"]}


def store_metrics(c, run, metrics):
    rows = [(run["id"], run["project"], k[:64], float(v)) for k, v in metrics.items() if NAME_RE.match(k) and v]
    if rows:
        with c.cursor() as cur:
            cur.executemany("INSERT INTO metrics (run_id, project, name, value) VALUES (%s,%s,%s,%s)", rows)


@app.post("/api/v1/runs/{run_id}/batches")
def post_batch(run_id: int, body: BatchIn, who: Caller = Depends(caller)):
    with db.conn() as c:
        run = own_run(c, run_id, who)
        store_metrics(c, run, body.metrics)
        if body.records:
            with c.cursor() as cur:
                cur.executemany(
                    """INSERT INTO records (project, record_id, data, run_id) VALUES (%s,%s,%s::jsonb,%s)
                       ON CONFLICT (project, record_id) DO UPDATE SET data=EXCLUDED.data, run_id=EXCLUDED.run_id, scraped_at=now()""",
                    [(run["project"], r.record_id, json.dumps(r.data), run_id) for r in body.records],
                )
        n = len(body.records)
        c.execute(
            "INSERT INTO batches (run_id, project, records, errors, blocked, latency_ms) VALUES (%s,%s,%s,%s,%s,%s)",
            (run_id, run["project"], n, body.errors, body.blocked, body.latency_ms),
        )
        c.execute(
            """UPDATE runs SET records=records+%s, batches=batches+1, errors=errors+%s, blocked=blocked+%s,
                   last_seen=now(), state=CASE WHEN state='running' THEN state ELSE 'running' END,
                   note=COALESCE(%s, note) WHERE id=%s""",
            (n, body.errors, body.blocked, body.note, run_id),
        )
    return {"stored": len(body.records)}


@app.post("/api/v1/runs/{run_id}/heartbeat")
def heartbeat(run_id: int, body: BatchIn = BatchIn(), who: Caller = Depends(caller)):
    with db.conn() as c:
        run = own_run(c, run_id, who)
        store_metrics(c, run, body.metrics)
        c.execute(
            "UPDATE runs SET last_seen=now(), errors=errors+%s, blocked=blocked+%s, note=COALESCE(%s, note) WHERE id=%s",
            (body.errors, body.blocked, body.note, run_id),
        )
    return {"ok": True}


@app.post("/api/v1/runs/{run_id}/finish")
def finish(run_id: int, body: Finish, who: Caller = Depends(caller)):
    if body.state not in ("finished", "failed"):
        raise HTTPException(422, "state must be finished or failed")
    with db.conn() as c:
        own_run(c, run_id, who)
        c.execute("UPDATE runs SET state=%s, finished_at=now(), last_seen=now(), note=%s WHERE id=%s",
                  (body.state, body.note, run_id))
    return {"ok": True}


@app.post("/api/v1/runs/{run_id}/claim")
def claim(run_id: int, body: Claim, who: Caller = Depends(caller)):
    with db.conn() as c:
        run = own_run(c, run_id, who)
        if body.project != run["project"]:
            raise HTTPException(403, "a run can only claim from its own project")
        rows = c.execute(
            """UPDATE queue SET status='claimed', claimed_by=%(run)s, claimed_at=now(), attempts=attempts+1
               WHERE (project, key) IN (
                   SELECT project, key FROM queue
                   WHERE project=%(project)s AND partition_id %% %(total)s = %(idx)s
                     AND (status='pending' OR (status='claimed' AND claimed_at < now() - make_interval(secs => %(timeout)s)))
                   ORDER BY partition_id, key LIMIT %(limit)s FOR UPDATE SKIP LOCKED)
               RETURNING key, payload""",
            {"run": run_id, "project": body.project, "total": max(1, run["total_jobs"]), "idx": run["job_index"],
             "timeout": config.CLAIM_TIMEOUT_S, "limit": body.limit},
        ).fetchall()
        c.execute("UPDATE runs SET last_seen=now() WHERE id=%s", (run_id,))
    return {"items": [{"key": r["key"], "payload": r["payload"]} for r in rows]}


@app.post("/api/v1/runs/{run_id}/complete")
def complete(run_id: int, body: Complete, who: Caller = Depends(caller)):
    if body.status not in ("done", "failed", "pending"):
        raise HTTPException(422, "status must be done, failed or pending")
    with db.conn() as c:
        run = own_run(c, run_id, who)
        c.execute(
            "UPDATE queue SET status=%s, done_at=CASE WHEN %s='done' THEN now() END, claimed_by=NULL, "
            "last_error=%s, fetched=COALESCE(%s, fetched) "
            "WHERE project=%s AND key = ANY(%s) AND claimed_by=%s",
            (body.status, body.status, body.error[:500], body.fetched, run["project"], body.keys, run_id),
        )
        c.execute("UPDATE runs SET last_seen=now() WHERE id=%s", (run_id,))
    return {"ok": True}


# ----------------------------------------------------------------------------- dashboard

def logged_in(request: Request) -> bool:
    return dash_auth.cookie_ok(request.cookies.get(dash_auth.COOKIE, ""))


def need_login(request: Request):
    if not logged_in(request):
        raise HTTPException(401, "login required")


def page(name: str) -> str:
    return (WEB / name).read_text()


@app.get("/healthz")
def healthz():
    with db.conn() as c:
        c.execute("SELECT 1")
    return {"ok": True}


@app.get("/login", response_class=HTMLResponse)
def login_page():
    return page("login.html").replace("__ERR__", "")


@app.post("/login")
async def login(request: Request):
    ip = client_ip(request)
    if dash_auth.throttled(ip):
        return HTMLResponse(page("login.html").replace("__ERR__", '<p class="err">Too many attempts. Wait a few minutes.</p>'), 429)
    pw = parse_qs((await request.body()).decode()).get("password", [""])[0]
    if dash_auth.password_ok(pw):
        resp = RedirectResponse("/", 303)
        resp.set_cookie(dash_auth.COOKIE, dash_auth.session_token(), httponly=True, secure=request.url.scheme == "https",
                        samesite="lax", max_age=30 * 86400)
        return resp
    dash_auth.note_failure(ip)
    msg = "Wrong password." if dash_auth.record() else "No password has been set yet."
    return HTMLResponse(page("login.html").replace("__ERR__", f'<p class="err">{msg}</p>'), 401)


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", 303)
    resp.delete_cookie(dash_auth.COOKIE)
    return resp


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    if not logged_in(request):
        return RedirectResponse("/login", 303)
    return page("index.html")


def project_stats(c, project: str) -> dict:
    stall = config.STALL_AFTER_S
    r = c.execute(
        """SELECT count(*) FILTER (WHERE state='running') AS running,
                  count(*) FILTER (WHERE state='running' AND last_seen < now() - make_interval(secs => %s)) AS stalled,
                  count(*) FILTER (WHERE state='failed' AND finished_at > now() - interval '1 hour') AS failed_runs_1h
           FROM runs WHERE project=%s""", (stall, project)).fetchone()
    latest = c.execute(
        "SELECT repository, gh_run_id, run_attempt, total_jobs FROM runs WHERE project=%s ORDER BY started_at DESC LIMIT 1",
        (project,)).fetchone()
    seen = expected = 0
    if latest and r["running"]:
        expected = latest["total_jobs"]
        seen = c.execute(
            """SELECT count(DISTINCT job_index) AS n FROM runs
               WHERE project=%s AND repository=%s AND gh_run_id=%s AND run_attempt=%s
                 AND (state='finished' OR last_seen >= now() - make_interval(secs => %s))""",
            (project, latest["repository"], latest["gh_run_id"], latest["run_attempt"], stall)).fetchone()["n"]
    w = c.execute(
        """SELECT coalesce(sum(records) FILTER (WHERE at >= now() - interval '5 minutes'),0) AS rec_now,
                  coalesce(sum(records) FILTER (WHERE at <  now() - interval '5 minutes' AND at >= now() - interval '10 minutes'),0) AS rec_prev,
                  coalesce(sum(errors)  FILTER (WHERE at >= now() - interval '5 minutes'),0) AS err,
                  coalesce(sum(blocked) FILTER (WHERE at >= now() - interval '5 minutes'),0) AS blk,
                  count(*) FILTER (WHERE at >= now() - interval '5 minutes') AS nb
           FROM batches WHERE project=%s AND at >= now() - interval '10 minutes'""", (project,)).fetchone()
    q = {row["status"]: row["n"] for row in c.execute(
        "SELECT status, count(*) AS n FROM queue WHERE project=%s GROUP BY status", (project,)).fetchall()}
    return {"running": r["running"], "stalled": r["stalled"], "failed_runs_1h": r["failed_runs_1h"], "expected_runners": expected, "seen_runners": seen,
            "rate_now": float(w["rec_now"]) / 5, "rate_prev": float(w["rec_prev"]) / 5,
            "batches_5m": w["nb"], "errors_5m": int(w["err"]), "blocked_5m": int(w["blk"]),
            "records_5m": int(w["rec_now"]), "queue": q}


def progress(c, project: str, stats: dict) -> dict:
    """Records fetched vs expected, and an ETA from the recent record rate.

    Expected comes from the `expected` number scrapers put in each queue payload (falls back to the project's
    expected_records). Items already done count what they actually fetched.
    """
    q = c.execute(
        """SELECT coalesce(sum(coalesce((payload->>'expected')::bigint, 0)) FILTER (WHERE status IN ('pending','claimed')), 0) AS left_exp,
                  coalesce(sum(fetched) FILTER (WHERE status='done'), 0) AS done_fetched,
                  coalesce(sum(coalesce((payload->>'expected')::bigint, 0)), 0) AS total_exp
           FROM queue WHERE project=%s""", (project,)).fetchone()
    meta = c.execute("SELECT expected_records FROM projects WHERE name=%s", (project,)).fetchone() or {}
    expected = int(q["total_exp"]) or int(meta.get("expected_records") or 0)
    rate = c.execute("SELECT coalesce(sum(records),0)::float / 10 AS r FROM batches WHERE project=%s AND at > now() - interval '10 minutes'",
                     (project,)).fetchone()["r"]
    eta = int(float(q["left_exp"]) / rate * 60) if rate > 0 and q["left_exp"] > 0 and stats["running"] else None
    return {"expected_records": expected, "remaining_expected": int(q["left_exp"]), "rate_10m": rate, "eta_s": eta}


@app.get("/api/projects/{name}", dependencies=[Depends(need_login)])
def project_detail(name: str):
    with db.conn() as c:
        meta = c.execute("SELECT * FROM projects WHERE name=%s", (name,)).fetchone()
        if not meta:
            raise HTTPException(404, "unknown project")
        failed = c.execute(
            """SELECT key, payload, attempts, last_error, done_at FROM queue
               WHERE project=%s AND status='failed' ORDER BY key LIMIT 100""", (name,)).fetchall()
        inflight = c.execute(
            """SELECT q.key, q.payload, q.claimed_at, q.attempts, r.job_index, r.runner_ip
               FROM queue q LEFT JOIN runs r ON r.id = q.claimed_by
               WHERE q.project=%s AND q.status='claimed' ORDER BY q.claimed_at LIMIT 100""", (name,)).fetchall()
        mets = c.execute(
            """SELECT name,
                      sum(value) FILTER (WHERE at > now() - interval '5 minutes') AS last5,
                      sum(value) AS hour
               FROM metrics WHERE project=%s AND at > now() - interval '1 hour' GROUP BY name ORDER BY name""", (name,)).fetchall()
        fresh = c.execute("SELECT max(scraped_at) AS newest, count(*) AS n FROM records WHERE project=%s", (name,)).fetchone()
        stats = project_stats(c, name)
        prog = progress(c, name, stats)
    return JSONResponse(json.loads(json.dumps({"project": meta, "failed": failed, "inflight": inflight, "metrics": mets,
                                               "newest_record": fresh["newest"], "records": fresh["n"], **prog}, default=_iso)))


@app.post("/api/projects/{name}/retry-failed", dependencies=[Depends(need_login)])
def retry_failed(name: str):
    with db.conn() as c:
        n = c.execute("UPDATE queue SET status='pending', claimed_by=NULL, last_error='' WHERE project=%s AND status='failed'",
                      (name,)).rowcount
    return {"reset": n}


@app.get("/api/overview", dependencies=[Depends(need_login)])
def overview():
    with db.conn() as c:
        # Creation order, so a project keeps its chart colour when others appear or disappear.
        projects = [r["name"] for r in c.execute("SELECT name FROM projects ORDER BY created_at, name").fetchall()]
        out = []
        for p in projects:
            s = project_stats(c, p)
            h = health.evaluate(s)
            tot = c.execute("SELECT count(*) AS n FROM records WHERE project=%s", (p,)).fetchone()["n"]
            meta = c.execute("SELECT * FROM projects WHERE name=%s", (p,)).fetchone() or {}
            out.append({"project": p, "records_total": tot, "stats": s, "title": meta.get("title") or p,
                        "unit_label": meta.get("unit_label", "item"), "record_label": meta.get("record_label", "record"),
                        **progress(c, p, s), **h})
        repos = c.execute(
            """SELECT repository, auth_kind, count(*) FILTER (WHERE state='running') AS running, count(*) AS runs,
                      coalesce(sum(records),0)::bigint AS records, max(last_seen) AS last_seen
               FROM runs GROUP BY repository, auth_kind ORDER BY max(last_seen) DESC""").fetchall()
    worst = max((["healthy", "degraded", "unhealthy"].index(o["verdict"]) for o in out), default=0)
    return JSONResponse(json.loads(json.dumps(
        {"verdict": ["healthy", "degraded", "unhealthy"][worst], "projects": out, "repos": repos}, default=_iso)))


@app.get("/api/runs", dependencies=[Depends(need_login)])
def runs(state: str = "", limit: int = 100):
    limit = min(max(limit, 1), 500)
    with db.conn() as c:
        rows = c.execute(
            """SELECT id, project, repository, workflow, gh_run_id, run_attempt, job_index, total_jobs, actor, auth_kind,
                      runner_ip, egress_ip, state, started_at, last_seen, finished_at, records, batches, errors, blocked, note,
                      (state='running' AND last_seen < now() - make_interval(secs => %s)) AS stalled,
                      extract(epoch FROM (coalesce(finished_at, now()) - started_at))::int AS age_s
               FROM runs WHERE (%s = '' OR state = %s)
               ORDER BY (state='running') DESC, last_seen DESC LIMIT %s""",
            (config.STALL_AFTER_S, state, state, limit)).fetchall()
    return JSONResponse(json.loads(json.dumps({"runs": rows}, default=_iso)))


@app.get("/api/throughput", dependencies=[Depends(need_login)])
def throughput(minutes: int = 60, bucket: int = 1):
    """Per-project sums per time bucket. `t` is the bucket start (epoch seconds); divide by `bucket` for per-minute rates."""
    minutes = min(max(minutes, 5), 1440)
    bucket = min(max(bucket, 1), 60)
    with db.conn() as c:
        rows = c.execute(
            """SELECT (floor(extract(epoch FROM at) / %(b)s) * %(b)s)::bigint AS t, project, sum(records)::int AS records,
                      sum(errors)::int AS errors, sum(blocked)::int AS blocked, count(*)::int AS batches
               FROM batches WHERE at >= now() - make_interval(mins => %(m)s)
               GROUP BY 1, 2 ORDER BY 1""", {"b": bucket * 60, "m": minutes}).fetchall()
    return JSONResponse(json.loads(json.dumps({"points": rows, "bucket": bucket}, default=_iso)))


@app.get("/api/ips", dependencies=[Depends(need_login)])
def ips():
    with db.conn() as c:
        rows = c.execute(
            """SELECT runner_ip, egress_ip, count(*) AS runs, count(DISTINCT repository) AS repos,
                      max(last_seen) AS last_seen, coalesce(sum(records),0)::bigint AS records
               FROM runs WHERE started_at >= now() - interval '7 days'
               GROUP BY runner_ip, egress_ip ORDER BY max(last_seen) DESC LIMIT 200""").fetchall()
    return JSONResponse(json.loads(json.dumps({"ips": rows}, default=_iso)))


@app.get("/api/queues", dependencies=[Depends(need_login)])
def queues():
    with db.conn() as c:
        rows = c.execute(
            "SELECT project, status, count(*)::int AS n FROM queue GROUP BY 1, 2 ORDER BY 1, 2").fetchall()
    return {"queues": rows}
