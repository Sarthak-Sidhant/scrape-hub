import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"])
print("workflow runs:", c.execute("select gh_run_id, count(*), sum(records), min(started_at)::timestamp(0) from runs group by 1 having sum(records) > 20000 order by 4").fetchall())
base = """select i.ip, count(distinct r.gh_run_id) wf, count(distinct r.id) procs, sum(i.hits) hits, sum(r.records) tenders,
  round((sum(coalesce(u.gb, 0)))::numeric, 2) gb
  from run_ips i join runs r on r.id=i.run_id
  left join (select run_id, sum(value)/1e9 gb from metrics where name='uploaded_bytes' group by run_id) u on u.run_id=r.id
  where i.source=%s group by i.ip"""
for src, order in (("api", "hits"), ("api", "tenders"), ("api", "gb")):
    print(f"\n== top 10 by {order} (source {src})")
    for row in c.execute(base + f" order by {order} desc nulls last limit 10", (src,)).fetchall(): print(row)
print("\nbig scrape (17:50 run and later): distinct IPs / hits / tenders:", c.execute("""select count(distinct i.ip), sum(i.hits), sum(r.records) from run_ips i join runs r on r.id=i.run_id
  where i.source='api' and r.started_at > '2026-10-16 17:49'""").fetchone())
