import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"])
print("missing sections, with pages_key:", c.execute("""select count(*) from records where project='eprocure-tenders' and coalesce(data->'details'->'fields'->>'tender_title','')<>'' and not (data->'details' ? 'sections') and data->'details' ? 'pages_key'""").fetchone())
print("missing, no pages_key:", c.execute("""select count(*) from records where project='eprocure-tenders' and coalesce(data->'details'->'fields'->>'tender_title','')<>'' and not (data->'details' ? 'sections') and not (data->'details' ? 'pages_key')""").fetchone())
print("top org-years still missing:", c.execute("""select left(record_id,4), data->>'org_id', count(*) from records where project='eprocure-tenders' and not (data->'details' ? 'sections') and data->'details' ? 'pages_key' group by 1,2 order by 3 desc limit 6""").fetchall())
print("org-years over 3000:", c.execute("select count(*) from queue where project='eprocure-sections' and (payload->>'expected')::int > 3000").fetchone())
