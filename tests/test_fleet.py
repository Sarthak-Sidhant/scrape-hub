"""The portal fleet: registry, checks, ramp verdicts, the /api/fleet and /api/showcase endpoints, page navigation and archiving."""
import json

from fastapi.testclient import TestClient

from scrape_hub import db, fleet
from scrape_hub.app import app


def test_registry_has_every_portal_once_and_groups_shared_servers():
    ps = fleet.load()
    assert len(ps) == 46 and len({p["portal"] for p in ps}) == 46
    groups = fleet.servers(ps)
    assert len(groups) == 40
    assert len(groups["164.100.79.193"]) == 6                       # the six shipyard / defence-PSU portals share one server
    first = next(iter(groups.values()))
    assert first[0]["portal"] == "wbtenders.gov.in"                 # biggest server first
    assert all(p["check_url"].startswith("https://") for p in ps)


def test_page_ok_wants_the_live_application():
    gepnic = {"ui": "gepnic"}
    assert fleet.page_ok(gepnic, 200, "<html><FORM id=x>")[0]
    assert fleet.page_ok(gepnic, 200, "<html>static front page</html>") == (False, "answered but the page has no form")
    assert fleet.page_ok(gepnic, 503, "<form>") == (False, "HTTP 503")
    assert fleet.page_ok({"ui": "gepnic2"}, 200, "Server is up")[0] and not fleet.page_ok({"ui": "gepnic2"}, 200, "error")[0]


def test_step_verdict_stops_on_failures_and_on_a_slowdown():
    fast = [(0.2, True)] * 40
    s, stop = fleet.step_verdict(fast, None)
    assert s["med_ms"] == 200 and not stop
    assert not fleet.step_verdict([(0.35, True)] * 40, 200)[1]          # 75% slower but under +500 ms: still fine
    assert fleet.step_verdict([(0.9, True)] * 40, 200)[1]               # more than doubled and +700 ms: overloaded
    assert fleet.step_verdict([(0.2, True)] * 18 + [(30, False)] * 2, 200)[1]   # 10% failed
    assert fleet.step_verdict([(30, False)] * 3, None)[1]               # nothing answered
    assert fleet.step_verdict([(14.5, True)] * 4, None)[1]              # answering, but 14.5 s even with one worker: struggling


def test_fleet_api_reports_status_uptime_and_capacity(client, clean, login):
    with db.conn() as c:
        for i in range(10):
            c.execute("INSERT INTO probes (target, at, ok, ms) VALUES ('gepnic:wbtenders.gov.in', now() - make_interval(mins => %s), %s, 400)", (10 - i, i >= 3))
            c.execute("INSERT INTO probes (target, at, ok, ms, detail) VALUES ('gepnic:etender.up.nic.in', now() - make_interval(mins => %s), false, 20000, 'ReadTimeout')", (10 - i,))
        c.execute("""INSERT INTO capacity (server_ip, at, portals, safe, tested_max, base_ms, steps) VALUES
                     ('164.100.69.49', '2026-10-18 22:00+00', '{wbtenders.gov.in}', 16, 16, 210, %s::jsonb),
                     ('164.100.69.49', '2026-10-18 10:00+00', '{wbtenders.gov.in}', 4, 8, 900, '[]')""",
                  (json.dumps([{"workers": 1, "med_ms": 210, "n": 50, "fail": 0, "rps": 4.8, "stopped": False}]),))
    d = login.get("/api/fleet").json()
    assert len(d["servers"]) == 40 and abs(sum(s["share"] for s in d["servers"]) - 1) < 1e-9
    wb = d["servers"][0]
    assert wb["ip"] == "164.100.69.49" and wb["portals"][0]["ok"] is True and wb["portals"][0]["uptime"] == 0.7
    assert wb["capacity"]["night"]["safe"] == 16 and wb["capacity"]["day"]["safe"] == 4        # 22:00 UTC is 03:30 IST; 10:00 UTC is 15:30 IST
    up = next(s for s in d["servers"] if s["portals"][0]["portal"] == "etender.up.nic.in")["portals"][0]
    assert up["ok"] is False and up["detail"] == "ReadTimeout" and up["uptime"] == 0
    assert login.get("/api/fleet/capacity?server_ip=164.100.69.49&days=30").json()["ramps"][0]["safe"] == 4


def test_runner_probe_list_leaves_out_the_fleet_checks(client, clean, login):
    with db.conn() as c:
        c.execute("INSERT INTO probes (target, ok, ms) VALUES ('eprocure', true, 500), ('gepnic:tntenders.gov.in', true, 300)")
    assert [t["target"] for t in login.get("/api/probes").json()["targets"]] == ["eprocure"]
    assert login.get("/api/probes?target=gepnic:tntenders.gov.in").json()["targets"][0]["ms"] == 300


def test_showcase_is_served_once_built(client, clean, login):
    assert login.get("/api/showcase").status_code == 404
    with db.conn() as c:
        c.execute("INSERT INTO showcase (project, data) VALUES ('eprocure-tenders', %s::jsonb)", (json.dumps({"totals": {"tenders": 5}, "by_year": []}),))
    d = login.get("/api/showcase").json()
    assert d["totals"]["tenders"] == 5 and d["project"] == "eprocure-tenders" and d["built_at"]


def test_quality_is_served_once_built(client, clean, login):
    assert login.get("/api/quality").status_code == 404
    with db.conn() as c:
        c.execute("INSERT INTO showcase (project, data) VALUES ('quality', %s::jsonb)", (json.dumps({"overall": {"tenders": 7}, "portals": []}),))
    d = login.get("/api/quality").json()
    assert d["overall"]["tenders"] == 7 and d["built_at"]
    assert TestClient(app).get("/api/quality").status_code == 401


def test_pages_share_the_navigation_and_need_login(client, clean, login):
    for path in ("/", "/portals", "/data", "/explore", "/tender"):
        html = login.get(path).text
        assert '<nav class="nav">' in html and f'<a href="{path}" aria-current="page">' in html and "<!--NAV-->" not in html
    html = login.get("/data/quality").text
    assert '<a href="/data" aria-current="page">' in html and "/api/quality" in html
    assert login.get("/assets/charts.js").status_code == 200 and login.get("/assets/hub.css").headers["content-type"].startswith("text/css")
    assert login.get("/assets/..%2Fapp.py").status_code == 404
    anon = TestClient(app)
    assert anon.get("/portals", follow_redirects=False).status_code == 303 and anon.get("/api/fleet").status_code == 401
    assert anon.get("/assets/charts.js").status_code == 401


def test_archiving_hides_a_project_without_deleting_it(client, clean, login):
    with db.conn() as c:
        c.execute("INSERT INTO projects (name) VALUES ('old-experiment'), ('live')")
    assert login.post("/api/projects/old-experiment/archive?archived=true").json() == {"archived": True}
    ov = login.get("/api/overview").json()
    assert [p["project"] for p in ov["projects"]] == ["live"] and ov["archived"] == ["old-experiment"]
    assert {p["project"] for p in login.get("/api/overview?archived=1").json()["projects"]} == {"old-experiment", "live"}
    login.post("/api/projects/live/settings", json={"threads": 8})                 # saving speed keeps the archive flag of others intact
    login.post("/api/projects/old-experiment/archive?archived=false")
    assert len(login.get("/api/overview").json()["projects"]) == 2
    assert login.post("/api/projects/nope/archive").status_code == 404


def test_fleet_shows_counting_and_scraping_progress_per_portal(client, clean, login):
    with db.conn() as c:
        c.execute("""INSERT INTO queue (project, key, partition_id, status, payload, fetched) VALUES
            ('gepnic-sikkimtender-plan', 'count:1', 0, 'done', '{"kind":"count"}', 96),
            ('gepnic-sikkimtender-plan', 'count:2', 0, 'pending', '{"kind":"count"}', 0),
            ('gepnic-wbtenders', 'w1', 0, 'done', '{"expected": 1000}', 990),
            ('gepnic-wbtenders', 'w2', 0, 'pending', '{"expected": 500}', 0)""")
    d = login.get("/api/fleet").json()
    by = {p["portal"]: p for s in d["servers"] for p in s["portals"]}
    sk, wb = by["sikkimtender.gov.in"]["progress"], by["wbtenders.gov.in"]["progress"]
    assert sk["count_done"] == 1 and sk["count_open"] == 1 and sk["counted"] == 96
    assert wb["windows_done"] == 1 and wb["windows_open"] == 1 and wb["tenders_expected"] == 1500 and wb["tenders_fetched"] == 990
    assert by["tntenders.gov.in"]["progress"] is None and by["eprocure.gov.in"]["progress"] is None


def test_archive_exports_whole_old_days_and_keeps_recent_rows(client, clean):
    import gzip, importlib.util, os, psycopg
    spec = importlib.util.spec_from_file_location("archive_telemetry", os.path.join(os.path.dirname(__file__), "..", "scripts", "archive_telemetry.py"))
    arch = importlib.util.module_from_spec(spec); spec.loader.exec_module(arch)
    with db.conn() as c:
        c.execute("""INSERT INTO probes (target, at, ok, ms) VALUES ('gepnic:a', '2026-01-02 10:00+00', true, 300), ('gepnic:a', '2026-01-02 23:59+00', false, 20000),
                     ('gepnic:a', '2026-01-03 00:01+00', true, 250), ('gepnic:a', now(), true, 200)""")
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        c.execute("SET TIME ZONE 'UTC'")
        days = arch.days_to_move(c, "probes", 30)
        assert [d.isoformat() for d in days] == ["2026-01-02", "2026-01-03"]          # today's row stays
        data, n = arch.export_day(c, "probes", days[0])
        text = gzip.decompress(data).decode()
        assert n == 2 and text.splitlines()[0] == "target,at,ok,ms,detail" and text.count("gepnic:a") == 2


def test_overview_sums_the_many_portal_scrape_into_one_card(client, clean, login):
    with db.conn() as c:
        c.execute("INSERT INTO projects (name, settings) VALUES ('gepnic-wbtenders', '{\"archived\": true}'), ('gepnic-wbtenders-plan', '{\"archived\": true}')")
        c.execute("""INSERT INTO queue (project, key, partition_id, status, payload, fetched) VALUES
            ('gepnic-wbtenders-plan', 'c1', 0, 'done', '{}', 900), ('gepnic-wbtenders-plan', 'c2', 0, 'pending', '{}', 0),
            ('gepnic-wbtenders', 'w1', 0, 'done', '{"expected": 900}', 880), ('gepnic-wbtenders', 'w2', 0, 'pending', '{"expected": 300}', 0)""")
        c.execute("""INSERT INTO runs (project, repository, gh_run_id, job_index, auth_kind, state) VALUES
            ('gepnic-wbtenders', 'r/x', '1', 0, 'oidc', 'running'), ('gepnic-wbtenders-plan', 'r/x', '1', 1, 'oidc', 'running')""")
        c.execute("INSERT INTO metrics (run_id, project, name, value) VALUES (1, 'gepnic-wbtenders', 'n_http', 300), (1, 'gepnic-wbtenders', 't_http_s', 450)")
    ov = login.get("/api/overview").json()
    g = ov["gepnic"]
    assert ov["projects"] == [] and g["portals"] == 1 and g["running"] == 2 and g["counting_runs"] == 1 and g["crawling_runs"] == 1
    assert g["count"] == {"done": 1, "open": 1, "failed": 0, "per_min": 0.0} and g["crawl"]["expected"] == 1200 and g["crawl"]["fetched"] == 880
    assert ov["totals"]["running"] == 2
    wb = login.get("/api/fleet").json()["servers"][0]["portals"][0]
    assert wb["load"] == {"req_per_min": 20.0, "s_per_req": 1.5}
