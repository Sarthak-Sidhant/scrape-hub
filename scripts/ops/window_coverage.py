import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"])
rows = c.execute("""with w as (select payload->>'org_id' org, payload->>'from_date' f, payload->>'to_date' t, (payload->>'expected')::int e from queue where project='eprocure-tenders' and status<>'done' and coalesce(payload->>'kind','')<>'count'),
 s as (select data->>'org_id' org, data->'window'->>0 f, data->'window'->>1 t, count(*) n from records where project='eprocure-tenders' and coalesce(data->'details'->'fields'->>'tender_title','')<>'' group by 1,2,3)
 select w.e, coalesce(s.n,0) from w left join s on s.org=w.org and s.f=w.f and s.t=w.t""").fetchall()
tot = len(rows); miss = sum(max(0, e-n) for e, n in rows)
for th in (1.0, .98, .95, .90, .75, .5):
    k = [r for r in rows if r[0] and r[1] >= th*r[0]]
    print(f">= {int(th*100)}% stored: {len(k)} windows skipped, {sum(max(0,e-n) for e,n in k)} tenders still missing in them")
print("total windows", tot, "missing tenders overall", miss, "| windows with 0 stored:", sum(1 for r in rows if r[1] == 0))
