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
    c.execute("TRUNCATE batches, records, queue, runs RESTART IDENTITY CASCADE")
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
    for i in range(428):
        c.execute("INSERT INTO queue (project,key,partition_id,status) VALUES ('eprocure-fy2526',%s,%s,%s)",
                  (str(i), i, "done" if i < 130 else "claimed" if i < 138 else "failed" if i in (200, 201, 202) else "pending"))
    c.execute("INSERT INTO records (project,record_id,data) SELECT 'eprocure-fy2526', g::text, '{}' FROM generate_series(1,5230) g")
print("demo data ready")
