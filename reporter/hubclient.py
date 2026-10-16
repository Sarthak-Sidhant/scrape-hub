"""Client for scrape-hub, used by scrapers running in GitHub Actions (or anywhere with a token).

In a workflow, grant `permissions: id-token: write`; the client then mints a short-lived OIDC token
for the hub's audience, which is how the hub knows the real repository. Outside Actions, set
SCRAPE_HUB_TOKEN (the hub's legacy token) and SCRAPE_HUB_REPO.
"""
import os
import queue
import threading
import time

import requests

HUB_URL = os.environ.get("SCRAPE_HUB_URL", "https://scrape.s1d.me").rstrip("/")
AUDIENCE = os.environ.get("SCRAPE_HUB_AUDIENCE", HUB_URL)


class HubClient:
    def __init__(self, project, job_index=0, total_jobs=1, hub_url=None, note=""):
        self.hub = (hub_url or HUB_URL).rstrip("/")
        self.project, self.job_index, self.total_jobs = project, job_index, total_jobs
        self.s = requests.Session()
        self._tok, self._tok_exp = None, 0
        self.lock = threading.Lock()
        self.pending = []
        self.errors = self.blocked = 0
        self.q = queue.Queue()
        self.run_id = self._call("POST", "/api/v1/runs", {
            "project": project, "job_index": job_index, "total_jobs": total_jobs,
            "egress_ip": self._egress_ip(), "note": note})["run_id"]
        self._stop = threading.Event()
        self._hb = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._hb.start()

    # -- auth ---------------------------------------------------------------
    def _token(self):
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

    @staticmethod
    def _egress_ip():
        try:
            return requests.get("https://api.ipify.org", timeout=8).text.strip()
        except Exception:
            return ""

    def _call(self, method, path, body=None, retries=4):
        last = None
        for i in range(retries):
            try:
                headers = {"Authorization": f"Bearer {self._token()}"}
                if os.environ.get("SCRAPE_HUB_TOKEN"):
                    headers["X-Scrape-Repo"] = os.environ.get("SCRAPE_HUB_REPO", os.environ.get("GITHUB_REPOSITORY", "unknown/unknown"))
                    headers["X-Scrape-Run-Id"] = os.environ.get("GITHUB_RUN_ID", "")
                r = self.s.request(method, self.hub + path, json=body, headers=headers, timeout=30)
                if r.status_code < 400:
                    return r.json()
                if r.status_code in (401, 403, 404, 422):
                    raise RuntimeError(f"hub rejected {path}: {r.status_code} {r.text[:200]}")
                last = RuntimeError(f"hub {path}: {r.status_code}")
            except requests.RequestException as e:
                last = e
            time.sleep(2 ** i)
        raise last

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

    def flush(self, note=None):
        with self.lock:
            recs, err, blk = self.pending, self.errors, self.blocked
            self.pending, self.errors, self.blocked = [], 0, 0
        if not (recs or err or blk or note):
            return
        t0 = time.time()
        try:
            self._call("POST", f"/api/v1/runs/{self.run_id}/batches",
                       {"records": recs, "errors": err, "blocked": blk, "note": note,
                        "latency_ms": int((time.time() - t0) * 1000)})
        except Exception:
            with self.lock:  # keep the data for the next flush
                self.pending = recs + self.pending
                self.errors += err
                self.blocked += blk
            raise

    def _heartbeat_loop(self):
        while not self._stop.wait(30):
            try:
                self.flush()
                self._call("POST", f"/api/v1/runs/{self.run_id}/heartbeat", {})
            except Exception as e:
                print(f"[hub] heartbeat failed: {e}", flush=True)

    # -- queue --------------------------------------------------------------
    def claim(self, limit=1):
        return self._call("POST", f"/api/v1/runs/{self.run_id}/claim", {"project": self.project, "limit": limit})["items"]

    def complete(self, keys, status="done"):
        self.flush()
        self._call("POST", f"/api/v1/runs/{self.run_id}/complete", {"keys": list(keys), "status": status})

    def note(self, text):
        self._call("POST", f"/api/v1/runs/{self.run_id}/heartbeat", {"note": text[:200]})

    def close(self, state="finished", note=""):
        self._stop.set()
        try:
            self.flush()
        finally:
            self._call("POST", f"/api/v1/runs/{self.run_id}/finish", {"state": state, "note": note})
