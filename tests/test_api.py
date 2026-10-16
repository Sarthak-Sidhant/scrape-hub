import pytest

from conftest import hdr, make_oidc


def start(client, project="p", job_index=0, total_jobs=1, **hk):
    r = client.post("/api/v1/runs", json={"project": project, "job_index": job_index, "total_jobs": total_jobs,
                                          "egress_ip": "20.1.2.3"}, headers=hdr(**hk))
    assert r.status_code == 200, r.text
    return r.json()["run_id"]


def test_requires_auth(client, clean):
    assert client.post("/api/v1/runs", json={"project": "p"}).status_code == 401
    assert client.post("/api/v1/runs", json={"project": "p"}, headers={"Authorization": "Bearer nope"}).status_code == 401


@pytest.mark.parametrize("kw", [{"aud": "https://evil.example"}, {"owner": "someone-else"}, {"exp": -10}])
def test_rejects_bad_oidc(client, clean, kw):
    r = client.post("/api/v1/runs", json={"project": "p"}, headers=hdr(**kw))
    assert r.status_code in (401, 403)


def test_rejects_token_signed_by_other_key(client, clean):
    import jwt
    from cryptography.hazmat.primitives.asymmetric import rsa
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    good = make_oidc()
    claims = jwt.decode(good, options={"verify_signature": False})
    forged = jwt.encode(claims, other, algorithm="RS256")
    assert client.post("/api/v1/runs", json={"project": "p"}, headers={"Authorization": f"Bearer {forged}"}).status_code == 401


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
    assert client.post(f"/api/v1/runs/{rid}/batches", json=body, headers=hdr()).json() == {"stored": 2}
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
            c.execute("INSERT INTO queue (project,key,partition_id,payload) VALUES ('p',%s,%s,%s::jsonb)", (f"k{i}", i, '{"n": %s}' % i))
    r0 = start(client, job_index=0, total_jobs=2)
    r1 = start(client, job_index=1, total_jobs=2)
    a = client.post(f"/api/v1/runs/{r0}/claim", json={"project": "p", "limit": 100}, headers=hdr()).json()["items"]
    b = client.post(f"/api/v1/runs/{r1}/claim", json={"project": "p", "limit": 100}, headers=hdr()).json()["items"]
    assert sorted(i["key"] for i in a) == ["k0", "k2", "k4", "k6", "k8"]
    assert sorted(i["key"] for i in b) == ["k1", "k3", "k5", "k7", "k9"]
    # nothing left to hand out while claimed
    assert client.post(f"/api/v1/runs/{r0}/claim", json={"project": "p", "limit": 5}, headers=hdr()).json()["items"] == []
    client.post(f"/api/v1/runs/{r0}/complete", json={"keys": ["k0", "k2"]}, headers=hdr())
    with db.conn() as c:
        st = {r["key"]: r["status"] for r in c.execute("SELECT key,status FROM queue").fetchall()}
    assert st["k0"] == "done" and st["k4"] == "claimed" and st["k1"] == "claimed"


def test_stale_claim_is_reclaimed(client, clean):
    from scrape_hub import db
    with db.conn() as c:
        c.execute("INSERT INTO queue (project,key,partition_id) VALUES ('p','k',0)")
    r = start(client)
    assert len(client.post(f"/api/v1/runs/{r}/claim", json={"project": "p"}, headers=hdr()).json()["items"]) == 1
    with db.conn() as c:
        c.execute("UPDATE queue SET claimed_at = now() - interval '2 hours'")
    r2 = start(client, run_id="222")
    assert len(client.post(f"/api/v1/runs/{r2}/claim", json={"project": "p"}, headers=hdr(run_id="222")).json()["items"]) == 1


def test_legacy_token_is_self_reported(client, clean, login):
    r = client.post("/api/v1/runs", json={"project": "p"}, headers={"Authorization": "Bearer legacy-secret", "X-Scrape-Repo": "fake/repo"})
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
    assert login.get("/api/ips").json()["ips"][0]["egress_ip"] == "20.1.2.3"


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
