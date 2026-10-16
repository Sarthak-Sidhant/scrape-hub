#!/usr/bin/env python3
"""Fill the local dev database (scripts/dev.py) with believable activity so the dashboard can be reviewed. Dev only."""
import json, os, random, sys
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root)
import pgserver
os.environ["DATABASE_URL"] = pgserver.get_server(os.path.join(root, ".devdata", "pg")).get_uri()
from scrape_hub import db
random.seed(7)
db.init_schema()
with db.conn() as c:
    c.execute("TRUNCATE failures, run_ips, metrics, batches, records, queue, runs, projects RESTART IDENTITY CASCADE")
    c.execute("INSERT INTO projects (name,title,unit_label,record_label,allowed_repos,expected_records) VALUES "
              "('eprocure-fy2526','eProcure FY 2025-26 tenders','organisation','tender','{Sarthak-Sidhant/eprocure-scrape}',47973),"
              "('cppp-aoc','CPPP award of contract','page','award','{Sarthak-Sidhant/results-tenders}',0)")
    repos = [("Sarthak-Sidhant/scrape-hub", "eprocure-fy2526", 8), ("Sarthak-Sidhant/results-tenders", "cppp-aoc", 6)]
    rid = 0
    for repo, project, n in repos:
        for j in range(n):
            rid += 1
            state = "running"; seen = "now()"; note = f"org {100+j*7} Example Organisation {j}: 120 tenders"
            if j == n - 1: state, seen, note = "running", "now() - interval '9 minutes'", "org 4 Stalled Org: 80 tenders"
            if j == n - 2: state, seen, note = "finished", "now() - interval '3 minutes'", "3 orgs"
            c.execute("""INSERT INTO runs (project,repository,workflow,gh_run_id,job_index,total_jobs,auth_kind,runner_ip,egress_ip,
                         state,started_at,last_seen,note,actor) VALUES (%s,%s,'Crawl',%s,%s,%s,'oidc',%s,%s,%s,
                         now() - interval '42 minutes', """ + seen + """, %s,'Sarthak-Sidhant')""",
                      (project, repo, "9001", j, n, f"20.{random.randint(40,200)}.{random.randint(1,250)}.{random.randint(1,250)}",
                       f"20.{random.randint(40,200)}.{random.randint(1,250)}.{random.randint(1,250)}", state, note))
            tot = 0
            for m in range(40, 0, -1):
                if state == "running" and seen != "now()" and m < 9: break
                if state == "finished" and m < 3: break
                k = max(0, int(random.gauss(14 if project == "eprocure-fy2526" else 6, 4))) * (0 if 20 < m < 24 and project == "eprocure-fy2526" and j % 2 else 1)
                e = 1 if random.random() < 0.15 else 0; b = 1 if random.random() < 0.05 else 0
                c.execute("INSERT INTO batches (run_id,project,at,records,errors,blocked,latency_ms) VALUES (%s,%s,now()-make_interval(mins=>%s),%s,%s,%s,%s)",
                          (rid, project, m, k, e, b, random.randint(40, 400)))
                tot += k
            c.execute("UPDATE runs SET records=%s, batches=40, errors=%s, blocked=%s WHERE id=%s", (tot, random.randint(0, 9), random.randint(0, 3), rid))
    c.execute("INSERT INTO run_ips (run_id, ip, source, first_seen, last_seen, hits) SELECT id, runner_ip, 'api', started_at, last_seen, 40 FROM runs")
    c.execute("INSERT INTO run_ips (run_id, ip, source, first_seen, last_seen, hits) SELECT id, egress_ip, 'reported', started_at, last_seen, 3 FROM runs")
    # one runner moves to a new address halfway through
    c.execute("UPDATE runs SET runner_ip='20.99.99.99' WHERE id=3")
    c.execute("INSERT INTO run_ips (run_id, ip, source, first_seen, last_seen, hits) VALUES (3,'20.99.99.99','api', now() - interval '12 minutes', now(), 20)")
    c.execute("UPDATE runs SET gh_run_id='9002' WHERE project='cppp-aoc'")
    # past workflow runs: completed, partial (one slice failed), failed, and one that changed IP
    def past(gh, project, repo, n, states, hours_ago, mins, recs):
        for j in range(n):
            st = states[j % len(states)]
            c.execute("""INSERT INTO runs (project,repository,workflow,gh_run_id,job_index,total_jobs,auth_kind,runner_ip,egress_ip,state,started_at,
                         last_seen,finished_at,records,batches,errors,blocked,note,actor)
                         VALUES (%s,%s,'Crawl',%s,%s,%s,'oidc',%s,%s,%s, now() - make_interval(hours=>%s), now() - make_interval(hours=>%s, mins=>%s),
                         now() - make_interval(hours=>%s, mins=>%s), %s, 30, %s, %s, '2 orgs','Sarthak-Sidhant')""",
                      (project, repo, gh, j, n, f"20.7.{j}.{hours_ago}", f"20.7.{j}.{hours_ago}", st, hours_ago, hours_ago, -mins, hours_ago, -mins,
                       0 if st == "failed" else recs, 1 if st == "failed" else 0, 0))
    past("8001", "eprocure-fy2526", "Sarthak-Sidhant/eprocure-scrape", 6, ["finished"], 3, 5, 120)
    past("8002", "eprocure-fy2526", "Sarthak-Sidhant/eprocure-scrape", 6, ["finished", "finished", "finished", "failed"], 7, 9, 90)
    past("8003", "cppp-aoc", "Sarthak-Sidhant/results-tenders", 4, ["failed"], 20, 2, 0)
    c.execute("INSERT INTO run_ips (run_id, ip, source) SELECT id, runner_ip, 'api' FROM runs WHERE gh_run_id IN ('8001','8002','8003') ON CONFLICT DO NOTHING")
    c.execute("INSERT INTO run_ips (run_id, ip, source) SELECT id, '20.8.8.8', 'api' FROM runs WHERE gh_run_id='8002' AND job_index=1 ON CONFLICT DO NOTHING")
    reason = "the portal's download service failed for this file (it redirects to its generic error page, which says 'session expired'; a real browser gets the same page, so it is not something the scraper can fix)"
    for i, org in enumerate(["Defence Research and Development Organisation"] * 9 + ["National Testing Agency"] * 4 + ["DG,BSF,MHA"] * 2):
        c.execute("INSERT INTO failures (project, kind, key, reason, detail, attempts) VALUES ('eprocure-tenders', 'document', %s, %s, %s::jsonb, 1)",
                  (f"2019_X_{i}/techsummary_{i}.pdf", reason, json.dumps({"org_name": org, "window": ["01/01/2019", "31/12/2019"], "document": f"techsummary_{i}.pdf", "declared_bytes": 224245})))
    c.execute("INSERT INTO failures (project, kind, key, reason, detail, attempts) VALUES ('eprocure-tenders', 'tender', '2019_X_77', 'TimeoutError: slow', %s::jsonb, 2)", (json.dumps({"org_name": "National Testing Agency"}),))
    c.execute("INSERT INTO projects (name,title,unit_label,record_label,allowed_repos) VALUES ('eprocure-tenders','eProcure tenders 2011-2026','window','tender','{Sarthak-Sidhant/eprocure-scrape}') ON CONFLICT DO NOTHING")
    for i in range(428):
        st = "done" if i < 130 else "claimed" if i < 138 else "failed" if i in (200, 201, 202) else "pending"
        err = {200: "portal listed 0 of 3592 expected tenders", 201: "12 of 80 tender pages failed", 202: "search failed (RuntimeError: Failed to authenticate after 10 attempts)"}.get(i, "")
        c.execute("INSERT INTO queue (project,key,partition_id,status,payload,last_error,fetched,attempts,claimed_by,claimed_at) VALUES ('eprocure-fy2526',%s,%s,%s,%s::jsonb,%s,%s,1,%s,%s)",
                  (str(i), i, st, json.dumps({"org_name": f"Example Organisation {i}", "expected": 120 + (i * 37) % 900}), err,
                   (120 + (i * 37) % 900) if st == "done" else 0, (i % 8 + 1) if st == "claimed" else None, "2026-10-16 09:00:00+02" if st == "claimed" else None))
    for m in range(1, 58):
        for rid2 in range(1, 9):
            for name, val in (("captcha_ok", random.randint(2, 6)), ("captcha_fail", random.randint(1, 4)), ("http_429", 1 if random.random() < .1 else 0)):
                if val: c.execute("INSERT INTO metrics (run_id,project,at,name,value) VALUES (%s,'eprocure-fy2526',now()-make_interval(mins=>%s),%s,%s)", (rid2, m, name, val))
    c.execute("INSERT INTO records (project,record_id,data) SELECT 'eprocure-fy2526', g::text, '{}' FROM generate_series(1,5230) g")
print("demo data ready")
