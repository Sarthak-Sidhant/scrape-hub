import os, psycopg, json
c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
repos = c.execute("select allowed_repos from projects where name='eprocure-tenders'").fetchone()[0]
rows = c.execute("""select left(record_id,4) y, data->>'org_id' o, count(*) from records where project='eprocure-tenders'
                    and coalesce(data->'details'->'fields'->>'tender_title','')<>'' and data->'details' ? 'pages_key' and not (data->'details' ? 'sections')
                    group by 1,2 having data->>'org_id' is not null""".replace("having data->>'org_id' is not null", "")).fetchall()
rows = [r for r in rows if r[1]]
rows.sort(key=lambda r: -r[2])
total = sum(r[2] for r in rows)
c.execute("""insert into projects (name, title, unit_label, record_label, allowed_repos, expected_units, expected_records, settings)
             values ('eprocure-sections','eProcure tender document lists (backfill)','org-year','tender',%s,%s,%s,'{"threads":8,"delay_ms":0,"paused":false,"auto_backoff":false}'::jsonb)
             on conflict (name) do update set expected_units=excluded.expected_units, expected_records=excluded.expected_records""", (repos, len(rows), total))
# biggest first, dealt round-robin across 80 slices so every process gets a similar load
n = 0
for i, (y, o, k) in enumerate(rows):
    c.execute("""insert into queue (project, key, partition_id, status, payload) values ('eprocure-sections', %s, %s, 'pending', %s::jsonb)
                 on conflict do nothing""", (f"{y}/{o}", i % 80, json.dumps({"year": y, "org_id": o, "expected": k})))
    n += 1
print("seeded", n, "org-years,", total, "tenders; owners:", repos)
