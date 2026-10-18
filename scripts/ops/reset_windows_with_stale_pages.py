import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
n = c.execute("""update queue q set status='pending', claimed_by=null, claimed_at=null, done_at=null, attempts=0
  from (select distinct data->>'org_id' org, data->'window'->>0 f, data->'window'->>1 t from records where project='eprocure-tenders'
        and data->'details' ? 'sections' and jsonb_array_length(data->'details'->'sections'->'covers')=0) w
  where q.project='eprocure-tenders' and q.payload->>'org_id'=w.org and q.payload->>'from_date'=w.f and q.payload->>'to_date'=w.t and q.status='done'""").rowcount
print("windows put back to pending for the stale captures:", n)
print(c.execute("select status,count(*) from queue where project='eprocure-tenders' group by 1 order by 1").fetchall())
print("tenders in those windows still real & stored (skipped on the pass):", c.execute("select count(*) from records where project='eprocure-tenders' and data->'details' ? 'sections' and jsonb_array_length(data->'details'->'sections'->'covers')>0").fetchone()[0])
