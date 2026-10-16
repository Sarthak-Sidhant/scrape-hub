import json
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


class Finish(BaseModel):
    state: str = "finished"
    note: str = ""


class Claim(BaseModel):
    project: str
    limit: int = Field(default=1, ge=1, le=100)


class Complete(BaseModel):
    keys: list[str]
    status: str = "done"


def own_run(c, run_id: int, who: Caller):
    row = c.execute("SELECT * FROM runs WHERE id=%s", (run_id,)).fetchone()
    if not row or row["repository"] != who.repository:
        raise HTTPException(404, "run not found")
    return row


@app.post("/api/v1/runs")
def start_run(body: RunStart, request: Request, who: Caller = Depends(caller)):
    with db.conn() as c:
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


@app.post("/api/v1/runs/{run_id}/batches")
def post_batch(run_id: int, body: BatchIn, who: Caller = Depends(caller)):
    with db.conn() as c:
        run = own_run(c, run_id, who)
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
        own_run(c, run_id, who)
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
            "UPDATE queue SET status=%s, done_at=CASE WHEN %s='done' THEN now() END, claimed_by=NULL "
            "WHERE project=%s AND key = ANY(%s) AND claimed_by=%s",
            (body.status, body.status, run["project"], body.keys, run_id),
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
                  count(*) FILTER (WHERE state='running' AND last_seen < now() - make_interval(secs => %s)) AS stalled
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
    return {"running": r["running"], "stalled": r["stalled"], "expected_runners": expected, "seen_runners": seen,
            "rate_now": float(w["rec_now"]) / 5, "rate_prev": float(w["rec_prev"]) / 5,
            "batches_5m": w["nb"], "errors_5m": int(w["err"]), "blocked_5m": int(w["blk"]),
            "records_5m": int(w["rec_now"]), "queue": q}


@app.get("/api/overview", dependencies=[Depends(need_login)])
def overview():
    with db.conn() as c:
        projects = [r["project"] for r in c.execute(
            "SELECT project FROM runs UNION SELECT project FROM queue ORDER BY 1").fetchall()]
        out = []
        for p in projects:
            s = project_stats(c, p)
            h = health.evaluate(s)
            tot = c.execute("SELECT count(*) AS n FROM records WHERE project=%s", (p,)).fetchone()["n"]
            out.append({"project": p, "records_total": tot, "stats": s, **h})
        repos = c.execute(
            """SELECT repository, auth_kind, count(*) FILTER (WHERE state='running') AS running, count(*) AS runs,
                      coalesce(sum(records),0)::bigint AS records, max(last_seen) AS last_seen
               FROM runs GROUP BY repository, auth_kind ORDER BY max(last_seen) DESC""").fetchall()
    worst = max((["healthy", "degraded", "unhealthy"].index(o["verdict"]) for o in out), default=0)
    return JSONResponse(json.loads(json.dumps(
        {"verdict": ["healthy", "degraded", "unhealthy"][worst], "projects": out, "repos": repos}, default=str)))


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
    return JSONResponse(json.loads(json.dumps({"runs": rows}, default=str)))


@app.get("/api/throughput", dependencies=[Depends(need_login)])
def throughput(minutes: int = 60):
    minutes = min(max(minutes, 5), 1440)
    with db.conn() as c:
        rows = c.execute(
            """SELECT extract(epoch FROM date_trunc('minute', at))::bigint AS t, project, sum(records)::int AS records,
                      sum(errors)::int AS errors, sum(blocked)::int AS blocked, count(*)::int AS batches
               FROM batches WHERE at >= now() - make_interval(mins => %s)
               GROUP BY 1, 2 ORDER BY 1""", (minutes,)).fetchall()
    return JSONResponse(json.loads(json.dumps({"points": rows}, default=str)))


@app.get("/api/ips", dependencies=[Depends(need_login)])
def ips():
    with db.conn() as c:
        rows = c.execute(
            """SELECT runner_ip, egress_ip, count(*) AS runs, count(DISTINCT repository) AS repos,
                      max(last_seen) AS last_seen, coalesce(sum(records),0)::bigint AS records
               FROM runs WHERE started_at >= now() - interval '7 days'
               GROUP BY runner_ip, egress_ip ORDER BY max(last_seen) DESC LIMIT 200""").fetchall()
    return JSONResponse(json.loads(json.dumps({"ips": rows}, default=str)))


@app.get("/api/queues", dependencies=[Depends(need_login)])
def queues():
    with db.conn() as c:
        rows = c.execute(
            "SELECT project, status, count(*)::int AS n FROM queue GROUP BY 1, 2 ORDER BY 1, 2").fetchall()
    return {"queues": rows}
