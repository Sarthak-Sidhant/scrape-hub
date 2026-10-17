import json
import re
import threading
import time
import zlib
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
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


class FailureIn(BaseModel):
    kind: str = Field(min_length=1, max_length=32, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    key: str = Field(min_length=1, max_length=300)
    reason: str = Field(default="", max_length=500)
    detail: dict = {}


class FailureKey(BaseModel):
    kind: str = Field(min_length=1, max_length=32)
    key: str = Field(min_length=1, max_length=300)


class BatchIn(BaseModel):
    records: list[Record] = []
    errors: int = 0
    blocked: int = 0
    latency_ms: int = 0
    note: Optional[str] = None
    metrics: dict[str, float] = {}
    egress_ip: str = ""
    failures: list[FailureIn] = Field(default=[], max_length=500)
    clear: list[FailureKey] = Field(default=[], max_length=2000)   # failures that no longer apply (the thing now succeeded)


class Finish(BaseModel):
    state: str = "finished"
    note: str = ""


class Claim(BaseModel):
    project: str
    limit: int = Field(default=1, ge=1, le=100)
    steal: bool = False   # when this slice has nothing left, take pending work from any other slice


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


class UploadFile(BaseModel):
    path: str = Field(min_length=1, max_length=300)
    content_type: str = Field(default="application/octet-stream", max_length=100)


class UploadRequest(BaseModel):
    files: list[UploadFile] = Field(min_length=1, max_length=60)


class KnownReq(BaseModel):
    ids: list[str] = Field(max_length=2000)


class QueueReplace(BaseModel):
    remove: list[str] = []
    add: list[QueueItem] = []


class QueueSeed(BaseModel):
    items: list[QueueItem]
    reset_failed: bool = False


DEFAULT_SETTINGS = {"threads": 4, "delay_ms": 0, "paused": False, "auto_backoff": True}


class SettingsIn(BaseModel):
    threads: int = Field(default=4, ge=1, le=config.MAX_THREADS)
    delay_ms: int = Field(default=0, ge=0, le=5000)
    paused: bool = False
    auto_backoff: bool = True


PROBE_FAILS_TO_PAUSE = 3      # consecutive failed checks (one a minute) before runners are told to hold


def target_down(c, target: str) -> str:
    """'' while the target looks reachable; otherwise why runners should hold. Only the newest checks count, so a recovery
    resumes the runners within a minute or two."""
    rows = c.execute("SELECT ok, at FROM probes WHERE target=%s AND at > now() - interval '10 minutes' ORDER BY at DESC LIMIT %s",
                     (target, PROBE_FAILS_TO_PAUSE)).fetchall()
    if len(rows) >= PROBE_FAILS_TO_PAUSE and not any(r["ok"] for r in rows):
        return f"{target} is not responding ({PROBE_FAILS_TO_PAUSE} checks in a row failed); runners are holding until it answers"
    return ""


def effective_settings(c, project: str) -> dict:
    """What runners should actually do right now: the saved settings, backed off if the target is pushing back.

    Backoff (when auto_backoff is on): in the last 3 minutes, if over 10% of work was blocked (with enough samples) or at least
    5 HTTP 429/403/503 responses were counted, or dropped connections exceed half the records stored, halve the threads (min 1, or the project's `min_threads` floor if set, in which case the 500 ms spacing is skipped).
    """
    row = c.execute("SELECT settings FROM projects WHERE name=%s", (project,)).fetchone()
    saved = {**DEFAULT_SETTINGS, **((row or {}).get("settings") or {})}
    eff = {**saved, "throttled": False, "reason": ""}
    floor = max(0, int(saved.get("min_threads") or 0))      # the slowest the throttles may make a process, so the rate never collapses
    if saved.get("probe") and not saved["paused"]:
        down = target_down(c, saved["probe"])
        if down and floor:
            eff.update(threads=min(saved["threads"], floor), throttled=True, reason=down.replace("runners are holding until it answers", f"running at the minimum of {floor} threads"))
        elif down:
            eff.update(paused=True, throttled=True, reason=down)
    if saved["auto_backoff"] and not saved["paused"]:
        w = c.execute("""SELECT coalesce(sum(blocked),0) AS b, coalesce(sum(records),0) AS r FROM batches
                         WHERE project=%s AND at > now() - interval '3 minutes'""", (project,)).fetchone()
        m = c.execute("""SELECT coalesce(sum(value),0) AS n FROM metrics WHERE project=%s AND name IN ('http_429','http_403','http_503')
                         AND at > now() - interval '3 minutes'""", (project,)).fetchone()
        e = c.execute("""SELECT coalesce(sum(value),0) AS n FROM metrics WHERE project=%s AND name='http_error'
                         AND at > now() - interval '3 minutes'""", (project,)).fetchone()
        b, r, n, drops = int(w["b"]), int(w["r"]), float(m["n"]), float(e["n"])
        dropping = drops >= 20 and drops > 0.5 * r      # dropped connections: roughly 8% of requests (a tender takes ~6)
        if (b + r >= 10 and b / max(1, b + r) > 0.10) or n >= 5 or dropping:
            eff.update(threads=max(1, saved["threads"] // 2, min(floor, saved["threads"])), delay_ms=saved["delay_ms"] if floor else max(saved["delay_ms"], 500),
                       throttled=True, reason=f"{b} blocked, {int(n)} refusals and {int(drops)} dropped connections in the last 3 minutes")
    return eff


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
                    (name, it.key, it.partition_id if it.partition_id is not None else zlib.crc32(it.key.encode()) % 1_000_000, json.dumps(it.payload)))
                added += cur.rowcount
        reset = 0
        if body.reset_failed:
            reset = c.execute("UPDATE queue SET status='pending', claimed_by=NULL WHERE project=%s AND status='failed'", (name,)).rowcount
    return {"added": added, "reset": reset}


def touch_ip(c, run_id: int, ip: str, source: str = "api"):
    """Remember every address a run is seen from; the run's current address is the latest one."""
    ip = (ip or "").strip()[:64]
    if not ip:
        return
    c.execute(
        """INSERT INTO run_ips (run_id, ip, source) VALUES (%s,%s,%s)
           ON CONFLICT (run_id, ip, source) DO UPDATE SET last_seen=now(), hits=run_ips.hits+1""", (run_id, ip, source))
    c.execute("UPDATE runs SET runner_ip=%s WHERE id=%s" if source == "api" else "UPDATE runs SET egress_ip=%s WHERE id=%s", (ip, run_id))


def seen(c, run_id: int, request: Request, egress_ip: str = ""):
    touch_ip(c, run_id, client_ip(request), "api")
    if egress_ip:
        touch_ip(c, run_id, egress_ip, "reported")


@app.get("/api/v1/projects/{name}/queue")
def list_queue(name: str, status: str = "pending", after: str = "", limit: int = 1000, who: Caller = Depends(caller)):
    """Read a project's queue (keyset-paged by key) so its owner can restructure it."""
    limit = min(max(limit, 1), 2000)
    with db.conn() as c:
        ensure_project(c, name, who)
        rows = c.execute("""SELECT key, partition_id, status, payload FROM queue
                            WHERE project=%s AND status=%s AND key > %s ORDER BY key LIMIT %s""", (name, status, after, limit)).fetchall()
    return {"items": rows, "next": rows[-1]["key"] if len(rows) == limit else None}


@app.post("/api/v1/projects/{name}/queue/replace")
def replace_queue(name: str, body: QueueReplace, who: Caller = Depends(caller)):
    """Atomically swap pending items: every key in `remove` must still be pending (otherwise nothing changes and 409 is
    returned), then `add` is inserted. Lets an owner merge or re-bucket work that nobody has started."""
    keys = list(dict.fromkeys(body.remove))
    with db.conn() as c:
        ensure_project(c, name, who)
        if keys:
            gone = c.execute("DELETE FROM queue WHERE project=%s AND key = ANY(%s) AND status='pending' RETURNING key", (name, keys)).fetchall()
            if len(gone) != len(keys):
                raise HTTPException(409, f"{len(keys) - len(gone)} of the items to remove are no longer pending; nothing was changed")
        added = 0
        with c.cursor() as cur:
            for it in body.add:
                cur.execute(
                    """INSERT INTO queue (project, key, partition_id, payload) VALUES (%s,%s,%s,%s::jsonb)
                       ON CONFLICT (project, key) DO NOTHING""",
                    (name, it.key, it.partition_id if it.partition_id is not None else zlib.crc32(it.key.encode()) % 1_000_000, json.dumps(it.payload)))
                added += cur.rowcount
    return {"removed": len(keys), "added": added}


@app.post("/api/v1/runs/{run_id}/known-records")
def known_records(run_id: int, body: KnownReq, request: Request, who: Caller = Depends(caller)):
    """Which of these record ids are already stored with real parsed fields (a record with an empty title is a failed capture and is fetched again), so a scraper resuming a window can skip them."""
    with db.conn() as c:
        run = own_run(c, run_id, who)
        seen(c, run_id, request)
        rows = c.execute("""SELECT record_id FROM records WHERE project=%s AND record_id = ANY(%s)
                            AND coalesce(data->'details'->'fields'->>'tender_title', '') <> ''""", (run["project"], body.ids)).fetchall()
    return {"known": [r["record_id"] for r in rows]}


PATH_RE = re.compile(r"^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$")
_s3 = None


def s3_client():
    global _s3
    if _s3 is None:
        import boto3
        from botocore.config import Config
        _s3 = boto3.client("s3", endpoint_url=config.R2_ENDPOINT, aws_access_key_id=config.R2_ACCESS_KEY_ID,
                           aws_secret_access_key=config.R2_SECRET_ACCESS_KEY, region_name="auto",
                           config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    return _s3


@app.post("/api/v1/runs/{run_id}/upload-urls")
def upload_urls(run_id: int, body: UploadRequest, request: Request, who: Caller = Depends(caller)):
    """One-time upload links for the run's project. The runner PUTs the bytes straight to object storage, so neither the
    storage keys nor the data pass through GitHub or this server. Objects can only be written under `<project>/`."""
    if not (config.R2_BUCKET and config.R2_ENDPOINT and config.R2_ACCESS_KEY_ID):
        raise HTTPException(503, "object storage is not configured on the hub")
    for f in body.files:
        if not PATH_RE.match(f.path) or any(seg in (".", "..") for seg in f.path.split("/")):
            raise HTTPException(422, f"bad path {f.path!r}: use letters, digits . _ - and / between folders")
    with db.conn() as c:
        run = own_run(c, run_id, who)
        seen(c, run_id, request)
    client, out = s3_client(), []
    for f in body.files:
        key = f"{run['project']}/{f.path}"
        url = client.generate_presigned_url("put_object", Params={"Bucket": config.R2_BUCKET, "Key": key, "ContentType": f.content_type},
                                            ExpiresIn=config.R2_PRESIGN_SECONDS, HttpMethod="PUT")
        out.append({"path": f.path, "key": key, "url": url, "headers": {"Content-Type": f.content_type}})
    return {"urls": out, "expires_in": config.R2_PRESIGN_SECONDS}


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
        seen(c, row["id"], request, body.egress_ip)
    return {"run_id": row["id"]}


def store_metrics(c, run, metrics):
    rows = [(run["id"], run["project"], k[:64], float(v)) for k, v in metrics.items() if NAME_RE.match(k) and v]
    if rows:
        with c.cursor() as cur:
            cur.executemany("INSERT INTO metrics (run_id, project, name, value) VALUES (%s,%s,%s,%s)", rows)


@app.post("/api/v1/runs/{run_id}/batches")
def post_batch(run_id: int, body: BatchIn, request: Request, who: Caller = Depends(caller)):
    with db.conn() as c:
        run = own_run(c, run_id, who)
        seen(c, run_id, request, body.egress_ip)
        store_metrics(c, run, body.metrics)
        if body.records:
            with c.cursor() as cur:
                cur.executemany(
                    """INSERT INTO records (project, record_id, data, run_id) VALUES (%s,%s,%s::jsonb,%s)
                       ON CONFLICT (project, record_id) DO UPDATE SET data=EXCLUDED.data, run_id=EXCLUDED.run_id, scraped_at=now()""",
                    [(run["project"], r.record_id, json.dumps(r.data), run_id) for r in body.records],
                )
        if body.records:   # a tender that is now stored is no longer a tender failure
            c.execute("DELETE FROM failures WHERE project=%s AND kind='tender' AND key = ANY(%s)", (run["project"], [r.record_id for r in body.records]))
        if body.clear:
            c.execute("DELETE FROM failures WHERE project=%s AND (kind, key) IN (SELECT * FROM unnest(%s::text[], %s::text[]))",
                      (run["project"], [f.kind for f in body.clear], [f.key for f in body.clear]))
        if body.failures:
            with c.cursor() as cur:
                cur.executemany(
                    """INSERT INTO failures (project, kind, key, reason, detail, run_id) VALUES (%s,%s,%s,%s,%s::jsonb,%s)
                       ON CONFLICT (project, kind, key) DO UPDATE SET reason=EXCLUDED.reason, detail=EXCLUDED.detail,
                           run_id=EXCLUDED.run_id, last_seen=now(), attempts=failures.attempts+1""",
                    [(run["project"], f.kind, f.key, f.reason, json.dumps(f.detail), run_id) for f in body.failures])
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
        settings = effective_settings(c, run["project"])
    return {"stored": len(body.records), "settings": settings}


@app.post("/api/v1/runs/{run_id}/heartbeat")
def heartbeat(run_id: int, request: Request, body: BatchIn = BatchIn(), who: Caller = Depends(caller)):
    with db.conn() as c:
        run = own_run(c, run_id, who)
        seen(c, run_id, request, body.egress_ip)
        store_metrics(c, run, body.metrics)
        c.execute(
            "UPDATE runs SET last_seen=now(), errors=errors+%s, blocked=blocked+%s, note=COALESCE(%s, note) WHERE id=%s",
            (body.errors, body.blocked, body.note, run_id),
        )
        settings = effective_settings(c, run["project"])
    return {"ok": True, "settings": settings}


@app.post("/api/v1/runs/{run_id}/finish")
def finish(run_id: int, body: Finish, request: Request, who: Caller = Depends(caller)):
    if body.state not in ("finished", "failed"):
        raise HTTPException(422, "state must be finished or failed")
    with db.conn() as c:
        own_run(c, run_id, who)
        seen(c, run_id, request)
        c.execute("UPDATE runs SET state=%s, finished_at=now(), last_seen=now(), note=%s WHERE id=%s",
                  (body.state, body.note, run_id))
    return {"ok": True}


@app.post("/api/v1/runs/{run_id}/claim")
def claim(run_id: int, body: Claim, request: Request, who: Caller = Depends(caller)):
    with db.conn() as c:
        run = own_run(c, run_id, who)
        seen(c, run_id, request)
        if body.project != run["project"]:
            raise HTTPException(403, "a run can only claim from its own project")
        settings = effective_settings(c, run["project"])
        if settings["paused"]:
            c.execute("UPDATE runs SET last_seen=now() WHERE id=%s", (run_id,))
            return {"items": [], "settings": settings}
        claim_sql = """UPDATE queue SET status='claimed', claimed_by=%(run)s, claimed_at=now(), attempts=attempts+1
               WHERE (project, key) IN (
                   SELECT project, key FROM queue
                   WHERE project=%(project)s {part}
                     AND (status='pending' OR (status='claimed' AND claimed_at < now() - make_interval(secs => %(timeout)s)))
                   ORDER BY partition_id, key LIMIT %(limit)s FOR UPDATE SKIP LOCKED)
               RETURNING key, payload"""
        args = {"run": run_id, "project": body.project, "total": max(1, run["total_jobs"]), "idx": run["job_index"],
                "timeout": config.CLAIM_TIMEOUT_S, "limit": body.limit}
        rows = c.execute(claim_sql.format(part="AND partition_id %% %(total)s = %(idx)s"), args).fetchall()
        if not rows and body.steal:
            rows = c.execute(claim_sql.format(part=""), args).fetchall()
        counts = {r["status"]: r["n"] for r in c.execute(
            "SELECT status, count(*) AS n FROM queue WHERE project=%s AND status IN ('pending','claimed') GROUP BY status",
            (body.project,)).fetchall()}
        c.execute("UPDATE runs SET last_seen=now() WHERE id=%s", (run_id,))
    return {"items": [{"key": r["key"], "payload": r["payload"]} for r in rows], "settings": settings,
            "queue": {"pending": counts.get("pending", 0), "claimed": counts.get("claimed", 0)}}


@app.post("/api/v1/runs/{run_id}/complete")
def complete(run_id: int, body: Complete, request: Request, who: Caller = Depends(caller)):
    if body.status not in ("done", "failed", "pending"):
        raise HTTPException(422, "status must be done, failed or pending")
    with db.conn() as c:
        run = own_run(c, run_id, who)
        seen(c, run_id, request)
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
        effective = effective_settings(c, name)
        fsum = failure_summary(c, name)
        stats = project_stats(c, name)
        prog = progress(c, name, stats)
    return JSONResponse(json.loads(json.dumps({"project": meta, "failed": failed, "inflight": inflight, "metrics": mets,
                                               "newest_record": fresh["newest"], "records": fresh["n"],
                                               "effective": effective, "settings": {**DEFAULT_SETTINGS, **(meta.get("settings") or {})},
                                               "max_threads": config.MAX_THREADS, "failures": fsum, **prog}, default=_iso)))


@app.post("/api/projects/{name}/settings", dependencies=[Depends(need_login)])
def set_settings(name: str, body: SettingsIn):
    """Runners pick this up on their next call (within about 30 seconds)."""
    with db.conn() as c:
        if not c.execute("SELECT 1 FROM projects WHERE name=%s", (name,)).fetchone():
            raise HTTPException(404, "unknown project")
        c.execute("UPDATE projects SET settings = settings || %s::jsonb WHERE name=%s", (json.dumps(body.model_dump()), name))
        return effective_settings(c, name)


def failure_summary(c, name: str) -> dict:
    kinds = c.execute("SELECT kind, count(*)::int AS n, max(last_seen) AS last_seen FROM failures WHERE project=%s GROUP BY kind ORDER BY n DESC", (name,)).fetchall()
    reasons = c.execute("""SELECT kind, left(reason, 110) AS reason, count(*)::int AS n FROM failures WHERE project=%s
                           GROUP BY 1, 2 ORDER BY n DESC LIMIT 8""", (name,)).fetchall()
    orgs = c.execute("""SELECT coalesce(detail->>'org_name', '?') AS org, count(*)::int AS n FROM failures WHERE project=%s
                        GROUP BY 1 ORDER BY n DESC LIMIT 8""", (name,)).fetchall()
    return {"kinds": kinds, "reasons": reasons, "orgs": orgs}


@app.get("/api/projects/{name}/failures", dependencies=[Depends(need_login)])
def failures_api(name: str, kind: str = "", limit: int = 100, offset: int = 0):
    limit, offset = min(max(limit, 1), 500), max(offset, 0)
    with db.conn() as c:
        rows = c.execute("""SELECT kind, key, reason, detail, attempts, first_seen, last_seen FROM failures
                            WHERE project=%s AND (%s = '' OR kind = %s) ORDER BY last_seen DESC LIMIT %s OFFSET %s""",
                         (name, kind, kind, limit, offset)).fetchall()
        summary = failure_summary(c, name)
    return JSONResponse(json.loads(json.dumps({"summary": summary, "failures": rows}, default=_iso)))


def csv_cell(v):
    """Spreadsheet formulas in scraped text must not run when the CSV is opened."""
    v = "" if v is None else str(v)
    return "'" + v if v[:1] in ("=", "+", "-", "@", "\t", "\r") else v


@app.get("/api/projects/{name}/failures.csv", dependencies=[Depends(need_login)])
def failures_csv(name: str, kind: str = ""):
    import csv
    import io
    with db.conn() as c:
        rows = c.execute("""SELECT kind, key, reason, detail, attempts, first_seen, last_seen FROM failures
                            WHERE project=%s AND (%s = '' OR kind = %s) ORDER BY kind, key""", (name, kind, kind)).fetchall()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["kind", "key", "reason", "organisation", "window_from", "window_to", "document", "declared_bytes", "attempts", "first_seen", "last_seen"])
    for r in rows:
        d = r["detail"] or {}
        w.writerow([csv_cell(x) for x in (r["kind"], r["key"], r["reason"], d.get("org_name"), (d.get("window") or ["", ""])[0], (d.get("window") or ["", ""])[1],
                                         d.get("document"), d.get("declared_bytes"), r["attempts"], r["first_seen"].isoformat(), r["last_seen"].isoformat())])
    return Response(buf.getvalue(), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{name}-failures.csv"'})


@app.post("/api/projects/{name}/retry-failed", dependencies=[Depends(need_login)])
def retry_failed(name: str):
    with db.conn() as c:
        n = c.execute("UPDATE queue SET status='pending', claimed_by=NULL, last_error='' WHERE project=%s AND status='failed'",
                      (name,)).rowcount
    return {"reset": n}


def find_outages(rows, gap_s=360, min_fails=3):
    """rows: [(epoch_seconds, ok)] oldest first. An outage is 3 or more failed checks in a row; two outages less than 6 minutes
    apart (one lucky pass in the middle of a bad stretch) are one outage. Returns [(start, end, still_going)]."""
    out, run = [], []
    for t, ok in rows:
        if not ok:
            run.append(t)
            continue
        if len(run) >= min_fails:
            out.append([run[0], t, False])
        run = []
    if len(run) >= min_fails:
        out.append([run[0], rows[-1][0] + 60, True])
    merged = []
    for st, en, going in out:
        if merged and st - merged[-1][1] < gap_s:
            merged[-1][1], merged[-1][2] = en, going
        else:
            merged.append([st, en, going])
    return merged


REPORTS = WEB / "reports"


@app.get("/reports/{name}", dependencies=[Depends(need_login)])
def report_file(name: str):
    """Charts and exports saved by hand under web/reports/, served behind the dashboard login."""
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,80}\.(png|csv|html)", name) or not (REPORTS / name).is_file():
        raise HTTPException(404, "no such report")
    return FileResponse(REPORTS / name)


@app.get("/api/probes", dependencies=[Depends(need_login)])
def probes(minutes: int = 120, bucket: int = 0):
    """Reachability of each target: the latest check, uptime, the outages in the window, and a bucketed history for the chart."""
    minutes = max(5, min(minutes, 10080))
    bucket = bucket if bucket > 0 else (1 if minutes <= 360 else 5 if minutes <= 1440 else 30)
    with db.conn() as c:
        targets = [r["target"] for r in c.execute("SELECT DISTINCT target FROM probes WHERE at > now() - interval '1 day' ORDER BY 1").fetchall()]
        out = []
        for t in targets:
            last = c.execute("SELECT ok, ms, detail, at FROM probes WHERE target=%s ORDER BY at DESC LIMIT 1", (t,)).fetchone()
            since = c.execute("""SELECT min(at) AS at FROM probes WHERE target=%s AND ok = %s AND at > coalesce(
                                    (SELECT max(at) FROM probes WHERE target=%s AND ok <> %s), '-infinity')""",
                              (t, last["ok"], t, last["ok"])).fetchone()["at"]
            rows = c.execute("""SELECT extract(epoch FROM at)::float8 AS t, ok, ms FROM probes
                                WHERE target=%s AND at > now() - make_interval(mins => %s) ORDER BY at""", (t, minutes)).fetchall()
            hist = c.execute("""SELECT to_timestamp(floor(extract(epoch FROM at) / %s) * %s) AS at, count(*)::int AS n,
                                       count(*) FILTER (WHERE NOT ok)::int AS failed, coalesce(max(ms) FILTER (WHERE ok), 0)::int AS ms
                                FROM probes WHERE target=%s AND at > now() - make_interval(mins => %s) GROUP BY 1 ORDER BY 1""",
                              (bucket * 60, bucket * 60, t, minutes)).fetchall()
            outs = find_outages([(r["t"], r["ok"]) for r in rows])
            up = round(100.0 * sum(1 for r in rows if r["ok"]) / len(rows), 1) if rows else None
            out.append({"target": t, "ok": last["ok"], "ms": last["ms"], "detail": last["detail"], "checked": last["at"], "since": since,
                        "uptime_pct": up, "checks": len(rows), "bucket_min": bucket, "history": hist,
                        "outages": [{"start": a, "end": b, "going": g, "seconds": int(b - a)} for a, b, g in outs],
                        "down_seconds": sum(int(b - a) for a, b, _ in outs), "longest_seconds": max((int(b - a) for a, b, _ in outs), default=0)})
    return JSONResponse(json.loads(json.dumps({"targets": out, "minutes": minutes}, default=_iso)))


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
def runs(state: str = "", limit: int = 100, project: str = "", repository: str = "", gh_run_id: str = "", attempt: int = 0):
    limit = min(max(limit, 1), 500)
    with db.conn() as c:
        rows = c.execute(
            """SELECT id, project, repository, workflow, gh_run_id, run_attempt, job_index, total_jobs, actor, auth_kind,
                      runner_ip, egress_ip, state, started_at, last_seen, finished_at, records, batches, errors, blocked, note,
                      (state='running' AND last_seen < now() - make_interval(secs => %s)) AS stalled,
                      extract(epoch FROM (coalesce(finished_at, now()) - started_at))::int AS age_s
               FROM runs WHERE (%s = '' OR state = %s) AND (%s = '' OR project = %s) AND (%s = '' OR repository = %s)
                 AND (%s = '' OR gh_run_id = %s) AND (%s = 0 OR run_attempt = %s)
               ORDER BY (state='running') DESC, last_seen DESC, job_index LIMIT %s""",
            (config.STALL_AFTER_S, state, state, project, project, repository, repository, gh_run_id, gh_run_id, attempt, attempt, limit)).fetchall()
        ids = [r["id"] for r in rows]
        ips, spark = {}, {}
        if ids:
            for r in c.execute("SELECT run_id, ip, source, first_seen, last_seen, hits FROM run_ips WHERE run_id = ANY(%s) ORDER BY first_seen", (ids,)).fetchall():
                ips.setdefault(r["run_id"], []).append({k: r[k] for k in ("ip", "source", "first_seen", "last_seen", "hits")})
            now_min = int(time.time() // 60) * 60
            for r in c.execute(
                    """SELECT run_id, (floor(extract(epoch FROM at) / 60) * 60)::bigint AS t, sum(records)::int AS n
                       FROM batches WHERE run_id = ANY(%s) AND at > now() - interval '15 minutes' GROUP BY 1, 2""", (ids,)).fetchall():
                arr = spark.setdefault(r["run_id"], [0] * 15)
                i = 14 - (now_min - r["t"]) // 60
                if 0 <= i < 15:
                    arr[int(i)] += r["n"]
        for r in rows:
            r["ips"] = ips.get(r["id"], [])
            r["ip_count"] = len({i["ip"] for i in r["ips"] if i["source"] == "api"})
            r["spark"] = spark.get(r["id"], [0] * 15)
            r["rate_5m"] = sum(r["spark"][-5:]) / 5
    return JSONResponse(json.loads(json.dumps({"runs": rows}, default=_iso)))


@app.get("/api/workflow-runs", dependencies=[Depends(need_login)])
def workflow_runs(state: str = "all", limit: int = 30, offset: int = 0):
    """One row per workflow run (all of its runner slices together). state: ongoing | past | all."""
    limit, offset = min(max(limit, 1), 200), max(offset, 0)
    with db.conn() as c:
        rows = c.execute(
            """WITH g AS (
                 SELECT project, repository, gh_run_id, run_attempt,
                        max(workflow) AS workflow, max(actor) AS actor, max(total_jobs) AS total_jobs, count(*) AS slices,
                        min(started_at) AS started_at, max(last_seen) AS last_seen,
                        count(*) FILTER (WHERE state='running') AS running,
                        count(*) FILTER (WHERE state='running' AND last_seen < now() - make_interval(secs => %(stall)s)) AS stalled,
                        count(*) FILTER (WHERE state='finished') AS finished,
                        count(*) FILTER (WHERE state='failed') AS failed,
                        coalesce(sum(records),0)::bigint AS records, coalesce(sum(errors),0)::int AS errors, coalesce(sum(blocked),0)::int AS blocked,
                        max(finished_at) FILTER (WHERE state <> 'running') AS ended_at,
                        array_agg(id) AS run_ids
                 FROM runs GROUP BY project, repository, gh_run_id, run_attempt)
               SELECT g.*, (SELECT count(DISTINCT ip) FROM run_ips WHERE run_id = ANY(g.run_ids) AND source='api') AS ips,
                      (SELECT count(*) FROM (SELECT run_id FROM run_ips WHERE run_id = ANY(g.run_ids) AND source='api'
                                             GROUP BY run_id HAVING count(DISTINCT ip) > 1) x) AS ip_changed
               FROM g WHERE (%(state)s = 'all' OR (%(state)s = 'ongoing' AND running > 0) OR (%(state)s = 'past' AND running = 0))
               ORDER BY (running > 0) DESC, started_at DESC LIMIT %(limit)s OFFSET %(offset)s""",
            {"stall": config.STALL_AFTER_S, "state": state, "limit": limit, "offset": offset}).fetchall()
        now = c.execute("SELECT now() AS n").fetchone()["n"]
    for r in rows:
        r.pop("run_ids", None)
        if r["running"]:
            r["outcome"] = "stalled" if r["stalled"] == r["running"] else "ongoing"
        elif r["finished"] == 0 and r["failed"] > 0:
            r["outcome"] = "failed"
        elif r["failed"] > 0 or r["slices"] < r["total_jobs"]:
            r["outcome"] = "partial"
        else:
            r["outcome"] = "completed"
        end = r["ended_at"] if not r["running"] and r["ended_at"] else now
        r["duration_s"] = int((end - r["started_at"]).total_seconds())
        r["github_url"] = f"https://github.com/{r['repository']}/actions/runs/{r['gh_run_id']}" if r["gh_run_id"].isdigit() else ""
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
    """Every address seen in the last 7 days. `records` counts runs that used the address, so a run that changed IP appears under both."""
    with db.conn() as c:
        rows = c.execute(
            """SELECT i.ip, string_agg(DISTINCT i.source, '+') AS sources, count(DISTINCT i.run_id) AS runs,
                      count(DISTINCT r.repository) AS repos, max(i.last_seen) AS last_seen,
                      coalesce(sum(r.records), 0)::bigint AS records
               FROM run_ips i JOIN runs r ON r.id = i.run_id
               WHERE i.last_seen >= now() - interval '7 days'
               GROUP BY i.ip ORDER BY max(i.last_seen) DESC LIMIT 200""").fetchall()
    return JSONResponse(json.loads(json.dumps({"ips": rows}, default=_iso)))


@app.get("/api/queues", dependencies=[Depends(need_login)])
def queues():
    with db.conn() as c:
        rows = c.execute(
            "SELECT project, status, count(*)::int AS n FROM queue GROUP BY 1, 2 ORDER BY 1, 2").fetchall()
    return {"queues": rows}
