import os, psycopg, json
c = psycopg.connect(os.environ["DATABASE_URL"])
q = lambda s, *a: c.execute(s, a).fetchall()
print("by size bucket (expected tenders): status counts")
for r in q("""select case when e<=100 then '1-100' when e<=300 then '101-300' when e<=600 then '301-600' when e<=900 then '601-900' else '901-1200+' end b,
        count(*) filter (where status='done') done, count(*) filter (where status='failed') failed, count(*) filter (where status in ('pending','claimed')) todo, sum(e) filter (where status<>'done') remaining
      from (select status, (payload->>'expected')::int e from queue where project='eprocure-tenders' and coalesce(payload->>'kind','')<>'count') x group by 1 order by min(e)"""): print(r)
print("\nremaining windows: expected vs already stored (by window key)")
rows = q("""with w as (select key, payload->>'org_id' org, payload->>'from_date' f, payload->>'to_date' t, (payload->>'expected')::int e from queue where project='eprocure-tenders' and status<>'done'),
 s as (select data->>'org_id' org, data->'window'->>0 f, data->'window'->>1 t, count(*) n from records where project='eprocure-tenders' and coalesce(data->'details'->'fields'->>'tender_title','')<>'' group by 1,2,3)
 select w.e, coalesce(s.n,0) from w left join s on s.org=w.org and s.f=w.f and s.t=w.t""")
tot_e = sum(r[0] for r in rows); tot_s = sum(min(r[1], r[0]) for r in rows)
print("pending/failed windows:", len(rows), "expected total", tot_e, "already stored", tot_s, f"-> {100*tot_s/tot_e:.0f}% known; missing {tot_e-tot_s}")
big = [r for r in rows if r[0] > 600]
print("windows >600:", len(big), "expected", sum(r[0] for r in big), "stored", sum(min(r[1], r[0]) for r in big))
print("windows with >=70% already stored:", sum(1 for r in rows if r[1] >= .7 * r[0]), "of", len(rows))
