import hashlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "client"))

import pytest  # noqa: E402

import scrape_hub_client as shc  # noqa: E402


class FakeResp:
    def __init__(self, status=200, etag=""):
        self.status_code, self.headers = status, {"ETag": f'"{etag}"'} if etag else {}


def make_client(monkeypatch, urls):
    c = object.__new__(shc.HubClient)          # skip __init__: no hub, no heartbeat thread
    c.run_id, c.settings, c.queue = 1, dict(shc.DEFAULT_SETTINGS), {}
    c.metrics, c.lock = shc.Counter(), shc.threading.Lock()
    c.pending, c.errors, c.blocked, c.failures, c.cleared = [], 0, 0, [], []
    c.call = lambda method, path, body=None, retries=4: {"urls": [{"path": u, "key": "proj/" + u, "url": "https://r2/" + u, "headers": {"Content-Type": "x"}} for u in urls]}
    monkeypatch.setattr(shc.time, "sleep", lambda s: None)
    return c


def test_upload_many_checks_the_storage_checksum_and_reports_hashes(monkeypatch):
    data = b"hello world"
    seen = []
    monkeypatch.setattr(shc.requests, "put", lambda url, data, headers, timeout: seen.append((url, headers)) or FakeResp(200, hashlib.md5(data).hexdigest()))
    c = make_client(monkeypatch, ["a/b.bin"])
    (r,) = c.upload_many([("a/b.bin", data, "application/octet-stream")])
    assert r["key"] == "proj/a/b.bin" and r["bytes"] == 11 and r["md5"] == hashlib.md5(data).hexdigest() and r["sha256"] == hashlib.sha256(data).hexdigest()
    assert seen == [("https://r2/a/b.bin", {"Content-Type": "x"})] and c.metrics["uploaded_files"] == 1 and c.metrics["uploaded_bytes"] == 11


def test_a_corrupted_upload_is_retried_then_succeeds(monkeypatch):
    data, calls = b"payload", []
    def put(url, data, headers, timeout):
        calls.append(1)
        return FakeResp(200, "deadbeef") if len(calls) == 1 else FakeResp(200, hashlib.md5(data).hexdigest())
    monkeypatch.setattr(shc.requests, "put", put)
    c = make_client(monkeypatch, ["x.bin"])
    assert c.upload_many([("x.bin", data, "a/b")])[0]["bytes"] == 7 and len(calls) == 2


def test_an_upload_that_keeps_failing_raises_and_counts_it(monkeypatch):
    monkeypatch.setattr(shc.requests, "put", lambda url, data, headers, timeout: FakeResp(403))
    c = make_client(monkeypatch, ["x.bin"])
    with pytest.raises(RuntimeError):
        c.upload_many([("x.bin", b"d", "a/b")])
    assert c.metrics["upload_failed"] == 1


def test_failures_and_clears_travel_with_the_next_flush_and_survive_a_failed_send(monkeypatch):
    sent = []
    c = make_client(monkeypatch, [])
    def call(method, path, body=None, retries=4):
        sent.append(body)
        if len(sent) == 1:
            raise RuntimeError("hub down")
        return {}
    c.call = call
    c.failure("document", "t1/report.pdf", "portal refused", org_name="DRDO")
    c.clear_failure("document", "t2/ok.pdf")
    with pytest.raises(RuntimeError):
        c.flush()
    c.flush()                                                                   # the retry carries the same items
    assert sent[1]["failures"] == [{"kind": "document", "key": "t1/report.pdf", "reason": "portal refused", "detail": {"org_name": "DRDO"}}]
    assert sent[1]["clear"] == [{"kind": "document", "key": "t2/ok.pdf"}]
