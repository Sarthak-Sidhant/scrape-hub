import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"])
q = lambda s: c.execute(s).fetchall()
print("YEAR", flush=True)
for r in q("""
 select left(r.record_id,4) y, count(distinct r.record_id) tenders,
        count(*) links, count(*) filter (where d->>'status'='stored') stored, count(*) filter (where d->>'status'='refused') refused,
        count(*) filter (where d->>'status'='failed') failed
 from records r, jsonb_array_elements(coalesce(r.data->'details'->'documents','[]'::jsonb)) d
 where r.project='eprocure-tenders' group by 1 order by 1"""):
    y, t, l, s, rf, f = r
    print(f"{y} tenders_with_docs={t:>7} links={l:>7} stored={s:>7} refused={rf:>7} failed={f:>4} refused%={100*rf/l:5.1f}", flush=True)
print("MONTH (2014-2020, by publication date)", flush=True)
for r in q("""
 select to_char(pd,'YYYY-MM') m, count(*) links, count(*) filter (where st='refused') refused from (
   select to_date(split_part(r.data->'details'->'fields'->>'published_date',' ',1),'DD-Mon-YYYY') pd, d->>'status' st
   from records r, jsonb_array_elements(coalesce(r.data->'details'->'documents','[]'::jsonb)) d
   where r.project='eprocure-tenders' and r.data->'details'->'fields'->>'published_date' ~ '^[0-9]{2}-[A-Za-z]{3}-[0-9]{4}') x
 where pd >= '2014-01-01' and pd < '2021-01-01' group by 1 order by 1"""):
    print(f"{r[0]} links={r[1]:>6} refused={r[2]:>6} {100*r[2]/r[1]:5.1f}%", flush=True)
