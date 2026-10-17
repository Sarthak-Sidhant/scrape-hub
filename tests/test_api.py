import pytest

from scrape_hub import db

from conftest import hdr, make_oidc


def start(client, project="proj", job_index=0, total_jobs=1, **hk):
    r = client.post("/api/v1/runs", json={"project": project, "job_index": job_index, "total_jobs": total_jobs,
                                          "egress_ip": "20.1.2.3"}, headers=hdr(**hk))
    assert r.status_code == 200, r.text
    return r.json()["run_id"]


def test_requires_auth(client, clean):
    assert client.post("/api/v1/runs", json={"project": "proj"}).status_code == 401
    assert client.post("/api/v1/runs", json={"project": "proj"}, headers={"Authorization": "Bearer nope"}).status_code == 401


@pytest.mark.parametrize("kw", [{"aud": "https://evil.example"}, {"owner": "someone-else"}, {"exp": -10}])
def test_rejects_bad_oidc(client, clean, kw):
    r = client.post("/api/v1/runs", json={"project": "proj"}, headers=hdr(**kw))
    assert r.status_code in (401, 403)


def test_rejects_token_signed_by_other_key(client, clean):
    import jwt
    from cryptography.hazmat.primitives.asymmetric import rsa
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    good = make_oidc()
    claims = jwt.decode(good, options={"verify_signature": False})
    forged = jwt.encode(claims, other, algorithm="RS256")
    assert client.post("/api/v1/runs", json={"project": "proj"}, headers={"Authorization": f"Bearer {forged}"}).status_code == 401


def test_repo_comes_from_token_not_body(client, clean, login):
    start(client, repo="Sarthak-Sidhant/active-tenders")
    runs = login.get("/api/runs").json()["runs"]
    assert runs[0]["repository"] == "Sarthak-Sidhant/active-tenders"
    assert runs[0]["auth_kind"] == "oidc"
    assert runs[0]["egress_ip"] == "20.1.2.3"
    assert runs[0]["gh_run_id"] == "111"


def test_batch_updates_counters_and_records(client, clean):
    rid = start(client)
    body = {"records": [{"record_id": "a", "data": {"x": 1}}, {"record_id": "b", "data": {"x": 2}}], "errors": 1, "latency_ms": 40}
    assert client.post(f"/api/v1/runs/{rid}/batches", json=body, headers=hdr()).json()["stored"] == 2
    # same record id again is an upsert, not a duplicate
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [{"record_id": "a", "data": {"x": 9}}]}, headers=hdr())
    from scrape_hub import db
    with db.conn() as c:
        assert c.execute("SELECT count(*) AS n FROM records").fetchone()["n"] == 2
        assert c.execute("SELECT data FROM records WHERE record_id='a'").fetchone()["data"] == {"x": 9}
        r = c.execute("SELECT records, batches, errors FROM runs WHERE id=%s", (rid,)).fetchone()
        assert (r["records"], r["batches"], r["errors"]) == (3, 2, 1)


def test_cannot_touch_another_repos_run(client, clean):
    rid = start(client, repo="Sarthak-Sidhant/results-tenders")
    r = client.post(f"/api/v1/runs/{rid}/batches", json={"records": []}, headers=hdr(repo="Sarthak-Sidhant/active-tenders"))
    assert r.status_code == 404


def test_start_is_idempotent(client, clean):
    assert start(client, job_index=3, total_jobs=20) == start(client, job_index=3, total_jobs=20)


def test_queue_partitioning_claim_and_complete(client, clean):
    from scrape_hub import db
    with db.conn() as c:
        for i in range(10):
            c.execute("INSERT INTO queue (project,key,partition_id,payload) VALUES ('proj',%s,%s,%s::jsonb)", (f"k{i}", i, '{"n": %s}' % i))
    r0 = start(client, job_index=0, total_jobs=2)
    r1 = start(client, job_index=1, total_jobs=2)
    a = client.post(f"/api/v1/runs/{r0}/claim", json={"project": "proj", "limit": 100}, headers=hdr()).json()["items"]
    b = client.post(f"/api/v1/runs/{r1}/claim", json={"project": "proj", "limit": 100}, headers=hdr()).json()["items"]
    assert sorted(i["key"] for i in a) == ["k0", "k2", "k4", "k6", "k8"]
    assert sorted(i["key"] for i in b) == ["k1", "k3", "k5", "k7", "k9"]
    # nothing left to hand out while claimed
    assert client.post(f"/api/v1/runs/{r0}/claim", json={"project": "proj", "limit": 5}, headers=hdr()).json()["items"] == []
    client.post(f"/api/v1/runs/{r0}/complete", json={"keys": ["k0", "k2"]}, headers=hdr())
    with db.conn() as c:
        st = {r["key"]: r["status"] for r in c.execute("SELECT key,status FROM queue").fetchall()}
    assert st["k0"] == "done" and st["k4"] == "claimed" and st["k1"] == "claimed"


def test_stale_claim_is_reclaimed(client, clean):
    from scrape_hub import db
    with db.conn() as c:
        c.execute("INSERT INTO queue (project,key,partition_id) VALUES ('proj','k',0)")
    r = start(client)
    assert len(client.post(f"/api/v1/runs/{r}/claim", json={"project": "proj"}, headers=hdr()).json()["items"]) == 1
    with db.conn() as c:
        c.execute("UPDATE queue SET claimed_at = now() - interval '2 hours'")
    r2 = start(client, run_id="222")
    assert len(client.post(f"/api/v1/runs/{r2}/claim", json={"project": "proj"}, headers=hdr(run_id="222")).json()["items"]) == 1


def test_legacy_token_is_self_reported(client, clean, login):
    r = client.post("/api/v1/runs", json={"project": "proj"}, headers={"Authorization": "Bearer legacy-secret", "X-Scrape-Repo": "fake/repo"})
    assert r.status_code == 200
    run = login.get("/api/runs").json()["runs"][0]
    assert run["auth_kind"] == "token" and run["repository"] == "fake/repo"


def test_dashboard_requires_login(client, clean):
    client.cookies.clear()
    for path in ("/api/overview", "/api/runs", "/api/throughput", "/api/ips", "/api/queues"):
        assert client.get(path).status_code == 401, path
    assert client.get("/", follow_redirects=False).status_code == 303


def test_wrong_password(client, clean):
    client.cookies.clear()
    r = client.post("/login", content="password=nope", headers={"content-type": "application/x-www-form-urlencoded"}, follow_redirects=False)
    assert r.status_code == 401


def test_overview_throughput_and_ips(client, clean, login):
    rid = start(client, project="fy2526", job_index=0, total_jobs=2)
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [{"record_id": "t1", "data": {}}]}, headers=hdr())
    ov = login.get("/api/overview").json()
    p = ov["projects"][0]
    assert p["project"] == "fy2526" and p["records_total"] == 1 and p["stats"]["running"] == 1
    assert ov["repos"][0]["repository"] == "Sarthak-Sidhant/results-tenders"
    pt = login.get("/api/throughput?minutes=30").json()["points"][0]
    assert pt["records"] == 1 and abs(pt["t"] - __import__("time").time()) < 120
    ips = {i["ip"]: i for i in login.get("/api/ips").json()["ips"]}
    assert "20.1.2.3" in ips and ips["20.1.2.3"]["sources"] == "reported"


def test_stalled_run_flagged(client, clean, login):
    from scrape_hub import db
    rid = start(client)
    with db.conn() as c:
        c.execute("UPDATE runs SET last_seen = now() - interval '10 minutes' WHERE id=%s", (rid,))
    ov = login.get("/api/overview").json()
    assert ov["verdict"] == "unhealthy"
    assert login.get("/api/runs").json()["runs"][0]["stalled"] is True


def test_finish(client, clean, login):
    rid = start(client)
    client.post(f"/api/v1/runs/{rid}/finish", json={"state": "finished", "note": "ok"}, headers=hdr())
    assert login.get("/api/runs").json()["runs"][0]["state"] == "finished"
    assert client.post(f"/api/v1/runs/{rid}/finish", json={"state": "bogus"}, headers=hdr()).status_code == 422


# ---------------------------------------------------------------- registry, seeding, metrics, detail

def test_first_repo_claims_project_and_others_are_refused(client, clean):
    start(client, project="owned", repo="Sarthak-Sidhant/eprocure-scrape")
    r = client.post("/api/v1/runs", json={"project": "owned"}, headers=hdr(repo="Sarthak-Sidhant/other-repo"))
    assert r.status_code == 403
    r = client.post("/api/v1/projects/owned/queue", json={"items": [{"key": "x"}]}, headers=hdr(repo="Sarthak-Sidhant/other-repo"))
    assert r.status_code == 403


def test_bad_project_name(client, clean):
    r = client.post("/api/v1/runs", json={"project": "Bad Name!"}, headers=hdr())
    assert r.status_code == 422


def test_register_and_seed_queue(client, clean):
    h = hdr()
    assert client.put("/api/v1/projects/seeded", json={"title": "Seeded", "unit_label": "organisation", "record_label": "tender",
                                                       "expected_records": 100}, headers=h).status_code == 200
    items = [{"key": f"o{i}", "payload": {"expected": 10}} for i in range(5)]
    assert client.post("/api/v1/projects/seeded/queue", json={"items": items}, headers=h).json() == {"added": 5, "reset": 0}
    # seeding twice does not duplicate
    assert client.post("/api/v1/projects/seeded/queue", json={"items": items}, headers=h).json()["added"] == 0


def test_complete_records_error_and_fetched_and_retry(client, clean, login):
    from scrape_hub import db
    h = hdr()
    client.post("/api/v1/projects/seeded/queue", json={"items": [{"key": "a", "payload": {"expected": 50, "org_name": "A"}},
                                                                {"key": "b", "payload": {"expected": 30}}]}, headers=h)
    rid = start(client, project="seeded")
    got = client.post(f"/api/v1/runs/{rid}/claim", json={"project": "seeded", "limit": 5}, headers=h).json()["items"]
    assert {i["key"] for i in got} == {"a", "b"}
    client.post(f"/api/v1/runs/{rid}/complete", json={"keys": ["a"], "status": "done", "fetched": 48}, headers=h)
    client.post(f"/api/v1/runs/{rid}/complete", json={"keys": ["b"], "status": "failed", "error": "listed 0 of 30"}, headers=h)
    d = login.get("/api/projects/seeded").json()
    assert d["failed"][0]["key"] == "b" and d["failed"][0]["last_error"] == "listed 0 of 30"
    assert d["expected_records"] == 80
    assert login.post("/api/projects/seeded/retry-failed").json() == {"reset": 1}
    with db.conn() as c:
        assert c.execute("SELECT status, last_error FROM queue WHERE key='b'").fetchone() == {"status": "pending", "last_error": ""}


def test_claim_is_limited_to_runs_own_project(client, clean):
    rid = start(client, project="mine")
    r = client.post(f"/api/v1/runs/{rid}/claim", json={"project": "someone-else", "limit": 1}, headers=hdr())
    assert r.status_code == 403


def test_metrics_stored_and_shown(client, clean, login):
    rid = start(client, project="metr")
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [], "metrics": {"captcha_ok": 6, "captcha_fail": 4}}, headers=hdr())
    client.post(f"/api/v1/runs/{rid}/heartbeat", json={"metrics": {"captcha_ok": 2}}, headers=hdr())
    m = {x["name"]: x for x in login.get("/api/projects/metr").json()["metrics"]}
    assert m["captcha_ok"]["hour"] == 8 and m["captcha_fail"]["hour"] == 4


def test_eta_from_recent_rate(client, clean, login):
    h = hdr()
    client.post("/api/v1/projects/etaproj/queue", json={"items": [{"key": "big", "payload": {"expected": 1000}}]}, headers=h)
    rid = start(client, project="etaproj")
    client.post(f"/api/v1/runs/{rid}/claim", json={"project": "etaproj"}, headers=h)
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [{"record_id": str(i), "data": {}} for i in range(100)]}, headers=h)
    ov = {p["project"]: p for p in login.get("/api/overview").json()["projects"]}["etaproj"]
    assert ov["remaining_expected"] == 1000 and ov["rate_10m"] == 10.0 and ov["eta_s"] == 6000


def test_idle_with_work_left_is_healthy(client, clean, login):
    client.post("/api/v1/projects/idleproj/queue", json={"items": [{"key": "a"}]}, headers=hdr())
    assert login.get("/api/overview").json()["verdict"] == "healthy"


def test_throughput_buckets(client, clean, login):
    rid = start(client, project="bucketed")
    for _ in range(3):
        client.post(f"/api/v1/runs/{rid}/batches", json={"records": [{"record_id": str(_), "data": {}}]}, headers=hdr())
    pts = login.get("/api/throughput?minutes=60&bucket=5").json()
    assert pts["bucket"] == 5 and all(p["t"] % 300 == 0 for p in pts["points"])
    assert sum(p["records"] for p in pts["points"]) == 3


def test_projects_listed_in_creation_order(client, clean, login):
    start(client, project="zzz-first")
    start(client, project="aaa-second", run_id="222")
    names = [p["project"] for p in login.get("/api/overview").json()["projects"]]
    assert names == ["zzz-first", "aaa-second"]


def test_ip_change_mid_run_is_recorded(client, clean, login, monkeypatch):
    from scrape_hub import config
    monkeypatch.setattr(config, "TRUST_PROXY", True)
    h = {**hdr(), "x-forwarded-for": "20.0.0.1"}
    rid = client.post("/api/v1/runs", json={"project": "roaming", "job_index": 0, "total_jobs": 2, "egress_ip": "20.0.0.1"}, headers=h).json()["run_id"]
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": []}, headers=h)
    # the runner's address changes halfway through
    h2 = {**hdr(), "x-forwarded-for": "20.0.0.99"}
    client.post(f"/api/v1/runs/{rid}/heartbeat", json={"egress_ip": "20.0.0.99"}, headers=h2)
    run = login.get("/api/runs").json()["runs"][0]
    assert run["runner_ip"] == "20.0.0.99" and run["egress_ip"] == "20.0.0.99"   # current address is the latest
    assert run["ip_count"] == 2
    assert {(i["ip"], i["source"]) for i in run["ips"]} == {("20.0.0.1", "api"), ("20.0.0.1", "reported"), ("20.0.0.99", "api"), ("20.0.0.99", "reported")}
    assert {i["ip"] for i in login.get("/api/ips").json()["ips"]} == {"20.0.0.1", "20.0.0.99"}


def test_runs_report_rate_and_sparkline(client, clean, login):
    rid = start(client, project="sparky")
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [{"record_id": str(i), "data": {}} for i in range(10)]}, headers=hdr())
    run = login.get("/api/runs").json()["runs"][0]
    assert len(run["spark"]) == 15 and sum(run["spark"]) == 10 and run["rate_5m"] == 2.0


def test_workflow_runs_group_slices_and_outcomes(client, clean, login):
    # run 777: 3 slices, one failed -> partial once nothing is running
    ids = [start(client, project="grouped", job_index=i, total_jobs=3, run_id="777") for i in range(3)]
    for rid, st in zip(ids, ("finished", "finished", "failed")):
        client.post(f"/api/v1/runs/{rid}/batches", json={"records": [{"record_id": f"r{rid}", "data": {}}], "errors": 1}, headers=hdr(run_id="777"))
    ongoing = login.get("/api/workflow-runs?state=ongoing").json()["runs"]
    assert len(ongoing) == 1 and ongoing[0]["outcome"] == "ongoing" and ongoing[0]["running"] == 3 and ongoing[0]["records"] == 3
    for rid, st in zip(ids, ("finished", "finished", "failed")):
        client.post(f"/api/v1/runs/{rid}/finish", json={"state": st}, headers=hdr(run_id="777"))
    assert login.get("/api/workflow-runs?state=ongoing").json()["runs"] == []
    past = login.get("/api/workflow-runs?state=past").json()["runs"][0]
    assert past["outcome"] == "partial" and past["finished"] == 2 and past["failed"] == 1 and past["slices"] == 3
    assert past["github_url"].endswith("/actions/runs/777") and past["duration_s"] >= 0 and past["errors"] == 3


def test_workflow_run_outcomes_failed_and_completed(client, clean, login):
    a = start(client, project="okproj", run_id="1")
    client.post(f"/api/v1/runs/{a}/finish", json={"state": "finished"}, headers=hdr(run_id="1"))
    b = start(client, project="badproj", run_id="2")
    client.post(f"/api/v1/runs/{b}/finish", json={"state": "failed"}, headers=hdr(run_id="2"))
    out = {r["project"]: r["outcome"] for r in login.get("/api/workflow-runs?state=past").json()["runs"]}
    assert out == {"okproj": "completed", "badproj": "failed"}


def test_runs_can_be_filtered_to_one_workflow_run(client, clean, login):
    start(client, project="filt", run_id="10")
    start(client, project="filt", run_id="11")
    got = login.get("/api/runs?project=filt&gh_run_id=10").json()["runs"]
    assert len(got) == 1 and got[0]["gh_run_id"] == "10"


# ---------------------------------------------------------------- live speed settings

def test_default_settings_come_back_on_every_call(client, clean):
    h = hdr()
    rid = start(client, project="speedy")
    hb = client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=h).json()["settings"]
    assert hb == {"threads": 4, "delay_ms": 0, "paused": False, "auto_backoff": True, "throttled": False, "reason": ""}
    assert client.post(f"/api/v1/runs/{rid}/batches", json={"records": []}, headers=h).json()["settings"]["threads"] == 4
    assert client.post(f"/api/v1/runs/{rid}/claim", json={"project": "speedy"}, headers=h).json()["settings"]["threads"] == 4


def test_dashboard_setting_reaches_runners_and_is_validated(client, clean, login):
    rid = start(client, project="tunable")
    ok = login.post("/api/projects/tunable/settings", json={"threads": 12, "delay_ms": 100, "paused": False, "auto_backoff": True})
    assert ok.status_code == 200 and ok.json()["threads"] == 12
    got = client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=hdr()).json()["settings"]
    assert got["threads"] == 12 and got["delay_ms"] == 100
    assert login.post("/api/projects/tunable/settings", json={"threads": 999}).status_code == 422   # above MAX_THREADS
    assert login.post("/api/projects/tunable/settings", json={"threads": 0}).status_code == 422
    assert login.post("/api/projects/nope/settings", json={"threads": 4}).status_code == 404
    assert login.get("/api/projects/tunable").json()["effective"]["threads"] == 12


def test_settings_require_login(client, clean):
    client.cookies.clear()
    assert client.post("/api/projects/anything/settings", json={"threads": 4}).status_code == 401


def test_pause_stops_work_being_handed_out(client, clean, login):
    h = hdr()
    client.post("/api/v1/projects/pausable/queue", json={"items": [{"key": "a"}, {"key": "b"}]}, headers=h)
    rid = start(client, project="pausable")
    login.post("/api/projects/pausable/settings", json={"threads": 4, "paused": True})
    r = client.post(f"/api/v1/runs/{rid}/claim", json={"project": "pausable", "limit": 5}, headers=h).json()
    assert r["items"] == [] and r["settings"]["paused"] is True
    login.post("/api/projects/pausable/settings", json={"threads": 4, "paused": False})
    assert len(client.post(f"/api/v1/runs/{rid}/claim", json={"project": "pausable", "limit": 5}, headers=h).json()["items"]) == 2


def test_auto_backoff_halves_threads_when_target_pushes_back(client, clean, login):
    h = hdr()
    rid = start(client, project="backoff")
    login.post("/api/projects/backoff/settings", json={"threads": 8, "delay_ms": 0, "auto_backoff": True})
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [], "metrics": {"http_429": 6}}, headers=h)
    eff = client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=h).json()["settings"]
    assert eff["throttled"] is True and eff["threads"] == 4 and eff["delay_ms"] == 500 and "refusals" in eff["reason"]
    # turning auto-backoff off honours the saved speed even under pushback
    login.post("/api/projects/backoff/settings", json={"threads": 8, "delay_ms": 0, "auto_backoff": False})
    eff = client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=h).json()["settings"]
    assert eff["throttled"] is False and eff["threads"] == 8


def test_auto_backoff_on_dropped_connections(client, clean, login):
    h = hdr()
    rid = start(client, project="dropper")
    login.post("/api/projects/dropper/settings", json={"threads": 16, "delay_ms": 0, "auto_backoff": True})
    recs = [{"record_id": str(i), "data": {}} for i in range(100)]
    # a few dropped connections among many records is normal (B2: 48 in 9,600 tenders)
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": recs, "metrics": {"http_error": 25}}, headers=h)
    assert client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=h).json()["settings"]["throttled"] is False
    # drops outnumbering half the records means the target is cutting us off
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [], "metrics": {"http_error": 60}}, headers=h)
    eff = client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=h).json()["settings"]
    assert eff["throttled"] is True and eff["threads"] == 8 and "dropped connections" in eff["reason"]


def test_auto_backoff_on_block_ratio(client, clean, login):
    h = hdr()
    rid = start(client, project="blocky")
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [{"record_id": str(i), "data": {}} for i in range(8)], "blocked": 4}, headers=h)
    eff = client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=h).json()["settings"]
    assert eff["throttled"] is True and eff["threads"] == 2


# ---------------------------------------------------------------- work stealing, queue counts, hashed partitions

def test_default_partition_is_a_stable_hash_so_added_items_spread_across_slices(client, clean):
    import zlib
    from scrape_hub import db
    items = [{"key": f"org{i}:2024-01"} for i in range(40)]
    client.post("/api/v1/projects/hashed/queue", json={"items": items}, headers=hdr())
    with db.conn() as c:
        parts = {r["key"]: r["partition_id"] for r in c.execute("SELECT key, partition_id FROM queue").fetchall()}
    assert parts["org7:2024-01"] == zlib.crc32(b"org7:2024-01") % 1_000_000
    assert len({p % 4 for p in parts.values()}) == 4          # not all stuck on slices 0..2


def test_claim_without_steal_stays_in_its_slice_and_with_steal_takes_others(client, clean):
    from scrape_hub import db
    h = hdr()
    client.post("/api/v1/projects/stealing/queue", json={"items": [{"key": "a", "partition_id": 1}, {"key": "b", "partition_id": 1}]}, headers=h)
    r0 = start(client, project="stealing", job_index=0, total_jobs=2)
    assert client.post(f"/api/v1/runs/{r0}/claim", json={"project": "stealing", "limit": 5}, headers=h).json()["items"] == []
    got = client.post(f"/api/v1/runs/{r0}/claim", json={"project": "stealing", "limit": 5, "steal": True}, headers=h).json()
    assert {i["key"] for i in got["items"]} == {"a", "b"}


def test_claim_reports_queue_counts_so_runners_can_wait_for_more_work(client, clean):
    h = hdr()
    client.post("/api/v1/projects/counted/queue", json={"items": [{"key": "a", "partition_id": 0}, {"key": "b", "partition_id": 0}]}, headers=h)
    rid = start(client, project="counted")
    out = client.post(f"/api/v1/runs/{rid}/claim", json={"project": "counted", "limit": 1}, headers=h).json()
    assert out["queue"] == {"pending": 1, "claimed": 1}


def test_a_runner_can_add_items_to_another_project_its_repo_owns(client, clean):
    h = hdr()
    start(client, project="plan-a")
    assert client.post("/api/v1/projects/crawl-b/queue", json={"items": [{"key": "x"}]}, headers=h).json()["added"] == 1
    other = hdr(repo="Sarthak-Sidhant/someone-else")
    assert client.post("/api/v1/projects/crawl-b/queue", json={"items": [{"key": "y"}]}, headers=other).status_code == 403


def test_bucketing_balances_load_and_orders_biggest_first():
    import importlib.util, os
    spec = importlib.util.spec_from_file_location("bucket_queue", os.path.join(os.path.dirname(__file__), "..", "scripts", "bucket_queue.py"))
    bq = importlib.util.module_from_spec(spec); spec.loader.exec_module(bq)
    weights = {f"k{i}": float(w) for i, w in enumerate([100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 10, 10])}
    assign, totals = bq.lpt(weights, 3)
    assert max(totals) - min(totals) <= 10                                  # near-perfect balance
    assert sum(totals) == sum(weights.values())
    for b in range(3):
        ranks = sorted((r, k) for k, (bb, r) in assign.items() if bb == b)
        ws = [weights[k] for _, k in ranks]
        assert ws == sorted(ws, reverse=True)                               # rank 0 is the biggest in its bucket
    assert {b for b, _ in assign.values()} == {0, 1, 2}


# ---------------------------------------------------------------- reading and replacing a pending queue

def test_owner_can_list_pending_items_in_pages(client, clean):
    h = hdr()
    client.post("/api/v1/projects/lister/queue", json={"items": [{"key": f"k{i:02d}", "payload": {"n": i}} for i in range(7)]}, headers=h)
    page1 = client.get("/api/v1/projects/lister/queue?limit=3", headers=h).json()
    assert [i["key"] for i in page1["items"]] == ["k00", "k01", "k02"] and page1["next"] == "k02"
    page2 = client.get("/api/v1/projects/lister/queue?limit=3&after=k02", headers=h).json()
    assert [i["key"] for i in page2["items"]] == ["k03", "k04", "k05"]
    last = client.get("/api/v1/projects/lister/queue?limit=3&after=k05", headers=h).json()
    assert [i["key"] for i in last["items"]] == ["k06"] and last["next"] is None
    assert client.get("/api/v1/projects/lister/queue", headers=hdr(repo="Sarthak-Sidhant/other")).status_code == 403


def test_replace_swaps_pending_items_atomically(client, clean):
    from scrape_hub import db
    h = hdr()
    client.post("/api/v1/projects/swapper/queue", json={"items": [{"key": "a", "payload": {"n": 1}}, {"key": "b", "payload": {"n": 2}}, {"key": "c"}]}, headers=h)
    out = client.post("/api/v1/projects/swapper/queue/replace",
                      json={"remove": ["a", "b"], "add": [{"key": "ab", "partition_id": 7, "payload": {"n": 3}}]}, headers=h).json()
    assert out == {"removed": 2, "added": 1}
    with db.conn() as c:
        rows = {r["key"]: (r["partition_id"], r["payload"]) for r in c.execute("SELECT key, partition_id, payload FROM queue").fetchall()}
    assert set(rows) == {"ab", "c"} and rows["ab"] == (7, {"n": 3})


def test_replace_refuses_if_an_item_was_already_claimed_and_changes_nothing(client, clean):
    from scrape_hub import db
    h = hdr()
    client.post("/api/v1/projects/guarded/queue", json={"items": [{"key": "a", "partition_id": 0}, {"key": "b", "partition_id": 1}]}, headers=h)
    rid = start(client, project="guarded", job_index=0, total_jobs=2)
    assert len(client.post(f"/api/v1/runs/{rid}/claim", json={"project": "guarded"}, headers=h).json()["items"]) == 1   # claims "a"
    r = client.post("/api/v1/projects/guarded/queue/replace", json={"remove": ["a", "b"], "add": [{"key": "ab"}]}, headers=h)
    assert r.status_code == 409
    with db.conn() as c:
        assert {x["key"] for x in c.execute("SELECT key FROM queue").fetchall()} == {"a", "b"}          # nothing was deleted or added


# ---------------------------------------------------------------- upload links for object storage

def test_upload_urls_are_scoped_to_the_project_and_expire(client, clean):
    rid = start(client, project="stored")
    out = client.post(f"/api/v1/runs/{rid}/upload-urls", json={"files": [
        {"path": "pages/2018/204/2018_X_1_1.json.gz", "content_type": "application/gzip"},
        {"path": "docs/2018/204/2018_X_1_1/techsummary_1.pdf", "content_type": "application/pdf"}]}, headers=hdr()).json()
    assert out["expires_in"] == 900 and len(out["urls"]) == 2
    u = out["urls"][0]
    assert u["key"] == "stored/pages/2018/204/2018_X_1_1.json.gz"                       # forced under the project's own folder
    assert u["url"].startswith("https://acct.r2.example.com/test-bucket/stored/pages/")  # path-style, on the storage endpoint
    assert "X-Amz-Expires=900" in u["url"] and "X-Amz-Signature=" in u["url"] and u["headers"] == {"Content-Type": "application/gzip"}
    assert "AKIATEST" in u["url"] and "secret" not in u["url"]                           # the secret key itself never leaves the hub


@pytest.mark.parametrize("bad", ["../etc/passwd", "/abs/path", "a//b", "a/./b", "a/../b", "spaces not allowed.pdf", "a/b/", ""])
def test_upload_paths_cannot_escape_or_be_malformed(client, clean, bad):
    rid = start(client, project="stored")
    r = client.post(f"/api/v1/runs/{rid}/upload-urls", json={"files": [{"path": bad}]}, headers=hdr())
    assert r.status_code == 422


def test_upload_urls_need_the_runs_own_repo_and_a_login(client, clean):
    rid = start(client, project="stored", repo="Sarthak-Sidhant/eprocure-scrape")
    body = {"files": [{"path": "x.bin"}]}
    assert client.post(f"/api/v1/runs/{rid}/upload-urls", json=body, headers=hdr(repo="Sarthak-Sidhant/other")).status_code == 404
    assert client.post(f"/api/v1/runs/{rid}/upload-urls", json=body).status_code == 401


def test_upload_urls_say_so_when_storage_is_not_configured(client, clean, monkeypatch):
    from scrape_hub import config
    monkeypatch.setattr(config, "R2_BUCKET", "")
    rid = start(client, project="stored")
    assert client.post(f"/api/v1/runs/{rid}/upload-urls", json={"files": [{"path": "x.bin"}]}, headers=hdr()).status_code == 503


# ---------------------------------------------------------------- failures: record, de-duplicate, clear, summarise, export

def post_failures(client, rid, failures=(), clear=(), records=()):
    return client.post(f"/api/v1/runs/{rid}/batches", json={"failures": list(failures), "clear": list(clear),
                                                           "records": [{"record_id": r, "data": {}} for r in records]}, headers=hdr())


def test_failures_are_stored_once_per_thing_and_count_attempts(client, clean, login):
    rid = start(client, project="failing")
    f = {"kind": "document", "key": "2019_X_1/report.pdf", "reason": "portal refused", "detail": {"org_name": "DRDO", "window": ["01/01/2019", "31/12/2019"], "document": "report.pdf", "declared_bytes": 100}}
    post_failures(client, rid, [f])
    post_failures(client, rid, [{**f, "reason": "portal refused again"}])
    rows = login.get("/api/projects/failing/failures").json()
    assert len(rows["failures"]) == 1 and rows["failures"][0]["attempts"] == 2 and rows["failures"][0]["reason"] == "portal refused again"
    assert rows["summary"]["kinds"][0]["kind"] == "document" and rows["summary"]["orgs"][0] == {"org": "DRDO", "n": 1}


def test_a_stored_tender_clears_its_tender_failure_and_clear_removes_others(client, clean, login):
    rid = start(client, project="clearing")
    post_failures(client, rid, [{"kind": "tender", "key": "T1", "reason": "timeout"}, {"kind": "tender", "key": "T2", "reason": "timeout"},
                                {"kind": "document", "key": "T9/a.pdf", "reason": "refused"}])
    assert len(login.get("/api/projects/clearing/failures").json()["failures"]) == 3
    post_failures(client, rid, records=["T1"])                                              # T1 now stored successfully
    post_failures(client, rid, clear=[{"kind": "document", "key": "T9/a.pdf"}])             # the document now downloads
    left = login.get("/api/projects/clearing/failures").json()["failures"]
    assert [(f["kind"], f["key"]) for f in left] == [("tender", "T2")]


def test_failures_can_be_filtered_by_kind_and_need_a_login(client, clean, login):
    rid = start(client, project="filtering")
    post_failures(client, rid, [{"kind": "document", "key": "a"}, {"kind": "tender", "key": "b"}])
    assert [f["key"] for f in login.get("/api/projects/filtering/failures?kind=tender").json()["failures"]] == ["b"]
    client.cookies.clear()
    assert client.get("/api/projects/filtering/failures").status_code == 401
    assert client.get("/api/projects/filtering/failures.csv").status_code == 401


def test_csv_export_has_the_columns_and_neutralises_spreadsheet_formulas(client, clean, login):
    import csv, io
    rid = start(client, project="exporting")
    post_failures(client, rid, [{"kind": "document", "key": "T1/r.pdf", "reason": "=HYPERLINK(\"http://evil\")",
                                 "detail": {"org_name": "+Org", "window": ["01/01/2019", "31/12/2019"], "document": "r.pdf", "declared_bytes": 5}}])
    r = login.get("/api/projects/exporting/failures.csv")
    assert r.headers["content-type"].startswith("text/csv") and "exporting-failures.csv" in r.headers["content-disposition"]
    rows = list(csv.reader(io.StringIO(r.text)))
    assert rows[0][:8] == ["kind", "key", "reason", "organisation", "window_from", "window_to", "document", "declared_bytes"]
    assert rows[1][2].startswith("'=") and rows[1][3] == "'+Org" and rows[1][4:8] == ["01/01/2019", "31/12/2019", "r.pdf", "5"]


def test_bad_failure_kind_is_rejected(client, clean):
    rid = start(client, project="strict")
    assert post_failures(client, rid, [{"kind": "Bad Kind!", "key": "x"}]).status_code == 422


# ---------------------------------------------------------------- skipping what is already stored

def test_known_records_returns_only_stored_ids_with_parsed_fields(client, clean):
    h = hdr()
    rid = start(client, project="resuming")
    client.post(f"/api/v1/runs/{rid}/batches", headers=h, json={"records": [
        {"record_id": "NEW1", "data": {"details": {"fields": {"tender_title": "Road works"}}}},
        {"record_id": "EMPTY1", "data": {"details": {"fields": {"tender_title": "", "tender_type": ""}}}},   # the portal's 'session expired' page parsed
        {"record_id": "OLD1", "data": {"details": {"bids": []}}}]})                       # stored by an older parser: no fields
    out = client.post(f"/api/v1/runs/{rid}/known-records", headers=h, json={"ids": ["NEW1", "EMPTY1", "OLD1", "NEVER"]}).json()
    assert out == {"known": ["NEW1"]}


def test_known_records_is_per_project_and_needs_the_runs_own_repo(client, clean):
    repo = "Sarthak-Sidhant/eprocure-scrape"
    rid = start(client, project="resuming", repo=repo)
    client.post(f"/api/v1/runs/{rid}/batches", headers=hdr(repo=repo), json={"records": [{"record_id": "A", "data": {"details": {"fields": {"tender_title": "x"}}}}]})
    other = start(client, project="elsewhere", repo=repo, run_id="9")
    assert client.post(f"/api/v1/runs/{other}/known-records", headers=hdr(repo=repo, run_id="9"), json={"ids": ["A"]}).json() == {"known": []}
    assert client.post(f"/api/v1/runs/{rid}/known-records", headers=hdr(repo=repo), json={"ids": ["A"]}).json() == {"known": ["A"]}
    assert client.post(f"/api/v1/runs/{rid}/known-records", headers=hdr(repo="Sarthak-Sidhant/other"), json={"ids": ["A"]}).status_code == 404


def test_runners_hold_while_the_target_is_down_and_resume_when_it_answers(client, clean, login):
    h = hdr()
    rid = start(client, project="flaky")
    login.post("/api/projects/flaky/settings", json={"threads": 8, "delay_ms": 0, "auto_backoff": True})
    with db.conn() as c:
        c.execute("UPDATE projects SET settings = settings || '{\"probe\": \"site\"}'::jsonb WHERE name='flaky'")
    beat = lambda: client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=h).json()["settings"]

    def probe(ok):
        with db.conn() as c:
            c.execute("INSERT INTO probes (target, ok, ms) VALUES ('site', %s, 5)", (ok,))
    for _ in range(2):
        probe(False)
    assert beat()["paused"] is False                      # two failures is not yet an outage
    probe(False)
    eff = beat()
    assert eff["paused"] is True and "not responding" in eff["reason"]
    probe(True)
    assert beat()["paused"] is False and beat()["threads"] == 8
    # the slider keeps the probe setting
    login.post("/api/projects/flaky/settings", json={"threads": 5, "delay_ms": 0, "auto_backoff": True})
    probe(False); probe(False); probe(False)
    assert beat()["paused"] is True
    pr = login.get("/api/probes").json()["targets"][0]
    assert pr["target"] == "site" and pr["ok"] is False and pr["since"]


def test_min_threads_floor_keeps_runners_going_under_backoff_and_outage(client, clean, login):
    h = hdr()
    rid = start(client, project="floored")
    login.post("/api/projects/floored/settings", json={"threads": 32, "delay_ms": 0, "auto_backoff": True})
    with db.conn() as c:
        c.execute("UPDATE projects SET settings = settings || '{\"probe\": \"site\", \"min_threads\": 20}'::jsonb WHERE name='floored'")
    beat = lambda: client.post(f"/api/v1/runs/{rid}/heartbeat", json={}, headers=h).json()["settings"]
    client.post(f"/api/v1/runs/{rid}/batches", json={"records": [], "metrics": {"http_429": 9}}, headers=h)
    eff = beat()
    assert eff["throttled"] is True and eff["threads"] == 20 and eff["delay_ms"] == 0 and eff["paused"] is False   # 32 // 2 = 16 would be below the floor
    with db.conn() as c:
        for _ in range(3):
            c.execute("INSERT INTO probes (target, ok, ms) VALUES ('site', false, 5)")
    eff = beat()
    assert eff["paused"] is False and eff["threads"] == 20
    # a manual pause from the dashboard is still honoured
    login.post("/api/projects/floored/settings", json={"threads": 32, "delay_ms": 0, "auto_backoff": True, "paused": True})
    assert beat()["paused"] is True


def test_find_outages_merges_close_stretches_and_ignores_short_blips():
    from scrape_hub.app import find_outages
    t = lambda m: m * 60
    ok = lambda m: (t(m), True)
    bad = lambda m: (t(m), False)
    rows = [ok(0), bad(1), bad(2), ok(3),                      # two failures: not an outage
            bad(10), bad(11), bad(12), ok(13), bad(14), bad(15), bad(16), ok(17),   # one lucky pass inside: one outage
            ok(30), bad(40), bad(41), bad(42)]                 # still down at the end
    out = find_outages(rows)
    assert [(a // 60, b // 60, g) for a, b, g in out] == [(10, 17, False), (40, 43, True)]


def test_probes_api_reports_outages_and_history(client, clean, login):
    with db.conn() as c:
        for i in range(30):       # 30 checks over the last 30 minutes, the middle ten failed
            c.execute("INSERT INTO probes (target, at, ok, ms) VALUES ('site', now() - make_interval(mins => %s), %s, 900)", (30 - i, not (10 <= i < 20)))
    t = login.get("/api/probes?minutes=60&bucket=5").json()["targets"][0]
    assert t["target"] == "site" and t["checks"] == 30 and t["uptime_pct"] == 66.7
    assert len(t["outages"]) == 1 and 540 <= t["outages"][0]["seconds"] <= 660 and t["down_seconds"] == t["outages"][0]["seconds"]
    assert t["ok"] is True and sum(h["n"] for h in t["history"]) == 30 and any(h["failed"] for h in t["history"])
    assert login.get("/api/probes?minutes=999999").json()["minutes"] == 10080
