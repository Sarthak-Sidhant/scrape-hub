import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
rows = c.execute("""select left(record_id,4), data->>'org_id' from records where project='eprocure-tenders' and not (data->'details' ? 'sections') and data->'details' ? 'pages_key' group by 1,2""").fetchall()
keys = [f"{y}/{o}" for y, o in rows]
n = c.execute("update queue set status='pending', claimed_by=null, claimed_at=null, done_at=null, attempts=0 where project='eprocure-sections' and key = any(%s)", (keys,)).rowcount
print("org-years with tenders still missing:", len(keys), "| queue items reset to pending:", n)
