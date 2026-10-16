"""Client for scrape-hub, used by scrapers running in GitHub Actions (or anywhere with a token).

    from scrape_hub_client import HubClient
    hub = HubClient("my-project", job_index=3, total_jobs=20)
    for item in hub.claim(1):
        ... scrape ...
        hub.record(record_id, {...})
        hub.metric("captcha_ok")           # any counter name; the dashboard charts them
        hub.complete([item["key"]], "done", fetched=n)
    hub.close()

Speed is set from the dashboard and comes back on every call as `hub.settings`:
{"threads": 4, "delay_ms": 0, "paused": False, "throttled": False, ...}. A scraper should size its concurrency from
`hub.settings["threads"]`, sleep `delay_ms` between requests, and idle while `paused`.

In a workflow, grant `permissions: id-token: write`; the client then mints a short-lived OIDC token for the
hub's audience, which is how the hub knows the real repository. Outside Actions, set SCRAPE_HUB_TOKEN (the hub's
legacy token) and SCRAPE_HUB_REPO.
"""
import hashlib
import os
import threading
import time
from collections import Counter

import requests

__all__ = ["HubClient", "register_project", "seed_queue", "list_queue", "replace_queue"]

HUB_URL = os.environ.get("SCRAPE_HUB_URL", "https://scrape.s1d.me").rstrip("/")
AUDIENCE = os.environ.get("SCRAPE_HUB_AUDIENCE", HUB_URL)


DEFAULT_SETTINGS = {"threads": 4, "delay_ms": 0, "paused": False, "auto_backoff": True, "throttled": False, "reason": ""}


class _Auth:
    def __init__(self, hub_url=None):
        self.settings = dict(DEFAULT_SETTINGS)
        self.queue = {}
        self.hub = (hub_url or HUB_URL).rstrip("/")
        self.s = requests.Session()
        self._tok, self._tok_exp = None, 0

    def token(self):
        if os.environ.get("SCRAPE_HUB_TOKEN"):
            return os.environ["SCRAPE_HUB_TOKEN"]
        if time.time() < self._tok_exp - 60:
            return self._tok
        url, bearer = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"), os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
        if not url:
            raise RuntimeError("no OIDC available: add `permissions: id-token: write` or set SCRAPE_HUB_TOKEN")
        r = requests.get(url, params={"audience": AUDIENCE}, headers={"Authorization": f"Bearer {bearer}"}, timeout=20)
        r.raise_for_status()
        self._tok, self._tok_exp = r.json()["value"], time.time() + 240  # GitHub tokens last ~5 min
        return self._tok

    def call(self, method, path, body=None, retries=4):
        last = None
        for i in range(retries):
            try:
                headers = {"Authorization": f"Bearer {self.token()}"}
                if os.environ.get("SCRAPE_HUB_TOKEN"):
                    headers["X-Scrape-Repo"] = os.environ.get("SCRAPE_HUB_REPO", os.environ.get("GITHUB_REPOSITORY", "unknown/unknown"))
                    headers["X-Scrape-Run-Id"] = os.environ.get("GITHUB_RUN_ID", "")
                r = self.s.request(method, self.hub + path, json=body, headers=headers, timeout=30)
                if r.status_code < 400:
                    out = r.json()
                    if isinstance(out, dict) and isinstance(out.get("settings"), dict):
                        self.settings = out["settings"]   # speed knobs set on the dashboard; read via hub.settings
                    return out
                if r.status_code in (401, 403, 404, 422):
                    raise RuntimeError(f"hub rejected {path}: {r.status_code} {r.text[:200]}")
                last = RuntimeError(f"hub {path}: {r.status_code}")
            except requests.RequestException as e:
                last = e
            time.sleep(2 ** i)
        raise last


def register_project(name, title="", unit_label="item", record_label="record", expected_units=0, expected_records=0, hub_url=None):
    """Create or update a project. The calling repo becomes its owner on first use."""
    return _Auth(hub_url).call("PUT", f"/api/v1/projects/{name}", {
        "title": title, "unit_label": unit_label, "record_label": record_label,
        "expected_units": expected_units, "expected_records": expected_records})


def seed_queue(project, items, reset_failed=False, hub_url=None, chunk=500):
    """items: [{"key": str, "payload": {...}}]. Put the expected record count in payload["expected"] to get ETAs."""
    a, added, reset = _Auth(hub_url), 0, 0
    for i in range(0, max(len(items), 1), chunk):
        r = a.call("POST", f"/api/v1/projects/{project}/queue", {"items": items[i:i + chunk], "reset_failed": reset_failed})
        added, reset = added + r["added"], reset + r["reset"]
    return {"added": added, "reset": reset}


def list_queue(project, status="pending", hub_url=None, page=1000):
    """All queue items with this status, as [{"key", "partition_id", "status", "payload"}]. Owner only."""
    a, out, after = _Auth(hub_url), [], ""
    while True:
        r = a.call("GET", f"/api/v1/projects/{project}/queue?status={status}&limit={page}&after={requests.utils.quote(after)}")
        out += r["items"]
        if not r["next"]:
            return out
        after = r["next"]


def replace_queue(project, remove, add, hub_url=None):
    """Atomically swap pending items. Fails (nothing changes) if any key in `remove` is no longer pending."""
    return _Auth(hub_url).call("POST", f"/api/v1/projects/{project}/queue/replace", {"remove": list(remove), "add": list(add)})


class HubClient(_Auth):
    def __init__(self, project, job_index=0, total_jobs=1, hub_url=None, note=""):
        super().__init__(hub_url)
        self.project, self.job_index, self.total_jobs = project, job_index, total_jobs
        self.lock = threading.Lock()
        self.pending = []
        self.errors = self.blocked = 0
        self.metrics = Counter()
        self.run_id = self.call("POST", "/api/v1/runs", {
            "project": project, "job_index": job_index, "total_jobs": total_jobs,
            "egress_ip": self._egress_ip(), "note": note})["run_id"]
        self._stop = threading.Event()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    @staticmethod
    def _egress_ip():
        try:
            return requests.get("https://api.ipify.org", timeout=8).text.strip()
        except Exception:
            return ""

    # -- reporting ----------------------------------------------------------
    def record(self, record_id, data):
        with self.lock:
            self.pending.append({"record_id": str(record_id), "data": data})
            full = len(self.pending) >= 25
        if full:
            self.flush()

    def error(self, n=1):
        with self.lock:
            self.errors += n

    def block(self, n=1):
        with self.lock:
            self.blocked += n

    def metric(self, name, value=1):
        """Count anything: hub.metric("captcha_fail"). Names are a-z 0-9 - _ ; `x_ok` / `x_fail` pairs show a success rate."""
        with self.lock:
            self.metrics[name] += value

    def flush(self, note=None):
        with self.lock:
            recs, err, blk, met = self.pending, self.errors, self.blocked, dict(self.metrics)
            self.pending, self.errors, self.blocked, self.metrics = [], 0, 0, Counter()
        if not (recs or err or blk or met or note):
            return
        t0 = time.time()
        try:
            self.call("POST", f"/api/v1/runs/{self.run_id}/batches",
                      {"records": recs, "errors": err, "blocked": blk, "note": note, "metrics": met,
                       "latency_ms": int((time.time() - t0) * 1000)})
        except Exception:
            with self.lock:  # keep the data for the next flush
                self.pending = recs + self.pending
                self.errors += err
                self.blocked += blk
                self.metrics.update(met)
            raise

    def _heartbeat_loop(self):
        beats = 0
        while not self._stop.wait(30):
            beats += 1
            try:
                self.flush()
                # Re-check our own address every ~5 minutes so the hub notices if it changes mid-run.
                body = {"egress_ip": self._egress_ip()} if beats % 10 == 0 else {}
                self.call("POST", f"/api/v1/runs/{self.run_id}/heartbeat", body)
            except Exception as e:
                print(f"[hub] heartbeat failed: {e}", flush=True)

    # -- queue --------------------------------------------------------------
    def claim(self, limit=1, steal=False):
        """Next queue items for this runner's slice. steal=True also takes work from other slices once this one is empty.
        After the call, `hub.queue` is {"pending": n, "claimed": n} for the whole project, so a runner that gets nothing can
        tell "all done" from "others are still working and may add more"."""
        out = self.call("POST", f"/api/v1/runs/{self.run_id}/claim", {"project": self.project, "limit": limit, "steal": steal})
        self.queue = out.get("queue", {})
        return out["items"]

    def complete(self, keys, status="done", error="", fetched=None):
        self.flush()
        self.call("POST", f"/api/v1/runs/{self.run_id}/complete",
                  {"keys": list(keys), "status": status, "error": error, "fetched": fetched})

    def upload_many(self, files, attempts=3):
        """Store files in the hub's object storage. files: [(path, bytes, content_type)]; path is relative to this project.

        One call to the hub gets a one-time upload link per file; the bytes then go straight to storage (the hub never sees them).
        Each upload is checked against the storage's checksum of what it received. Returns [{path, key, bytes, md5, sha256}].
        Raises if a file still fails after `attempts` tries.
        """
        links = self.call("POST", f"/api/v1/runs/{self.run_id}/upload-urls",
                          {"files": [{"path": p, "content_type": ct} for p, _, ct in files]})["urls"]
        out = []
        for (path, data, ctype), link in zip(files, links):
            md5 = hashlib.md5(data).hexdigest()
            last = None
            for i in range(attempts):
                try:
                    r = requests.put(link["url"], data=data, headers=link["headers"], timeout=180)
                    etag = r.headers.get("ETag", "").strip('"')
                    if r.status_code in (200, 201) and (not etag or etag == md5):
                        break
                    last = RuntimeError(f"upload of {path}: HTTP {r.status_code}, etag {etag or '-'} vs md5 {md5}")
                except requests.RequestException as e:
                    last = e
                time.sleep(2 ** i)
            else:
                self.metric("upload_failed")
                raise last
            self.metric("uploaded_files")
            self.metric("uploaded_bytes", len(data))
            out.append({"path": path, "key": link["key"], "bytes": len(data), "md5": md5, "sha256": hashlib.sha256(data).hexdigest()})
        return out

    def note(self, text):
        self.call("POST", f"/api/v1/runs/{self.run_id}/heartbeat", {"note": text[:200]})

    def close(self, state="finished", note=""):
        self._stop.set()
        try:
            self.flush()
        finally:
            self.call("POST", f"/api/v1/runs/{self.run_id}/finish", {"state": state, "note": note})
