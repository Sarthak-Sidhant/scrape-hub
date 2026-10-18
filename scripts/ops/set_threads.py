import os, sys, psycopg, json
c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
t = int(sys.argv[1]) if len(sys.argv) > 1 else None
if t: c.execute("update projects set settings = settings || %s::jsonb where name='eprocure-tenders'", (json.dumps({"threads": t}),))
print(c.execute("select settings from projects where name='eprocure-tenders'").fetchall())
