import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"])
rows = c.execute("""
 select substr(f,8,4) y, substr(f,4,3) m, count(*) links, count(*) filter (where st='refused') refused from (
   select r.data->'details'->'fields'->>'published_date' f, d->>'status' st
   from records r, jsonb_array_elements(coalesce(r.data->'details'->'documents','[]'::jsonb)) d
   where r.project='eprocure-tenders' and r.data->'details'->'fields'->>'published_date' ~ '^[0-9]{2}-[A-Za-z]{3}-20(1[3-9]|20)') x
 group by 1,2""").fetchall()
mon = {m: i for i, m in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1)}
for y, m, l, rf in sorted(rows, key=lambda r: (r[0], mon.get(r[1], 0))):
    print(f"{y}-{mon.get(m,0):02d} links={l:>6} refused={rf:>6} {100*rf/l:5.1f}%")
