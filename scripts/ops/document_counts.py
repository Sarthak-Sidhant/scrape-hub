import os, psycopg
c = psycopg.connect(os.environ["DATABASE_URL"])
q = lambda s: c.execute(s).fetchall()
base = "records r, jsonb_array_elements(coalesce(r.data->'details'->'documents','[]'::jsonb)) d where r.project='eprocure-tenders'"
print("by status:", q(f"select d->>'status', count(*), round((sum(coalesce((d->>'bytes')::bigint,0))/1e9)::numeric,1) from {base} group by 1 order by 2 desc"), flush=True)
print("by kind (stored):", q(f"select d->>'kind', count(*) from {base} and d->>'status'='stored' group by 1 order by 2 desc"), flush=True)
print("by type (stored):", q(f"select d->>'content_type', count(*) from {base} and d->>'status'='stored' group by 1 order by 2 desc limit 6"), flush=True)
print("tenders with >=1 stored / with doc links / all:", q("""select count(*) filter (where exists (select 1 from jsonb_array_elements(coalesce(data->'details'->'documents','[]')) d where d->>'status'='stored')),
   count(*) filter (where jsonb_array_length(coalesce(data->'details'->'documents','[]'))>0), count(*) from records where project='eprocure-tenders'""")[0], flush=True)
print("listed on details pages (NIT, work item):", q("select sum(jsonb_array_length(data->'details'->'sections'->'nit_documents')), sum(jsonb_array_length(data->'details'->'sections'->'work_item_documents')) from records where project='eprocure-tenders' and data->'details' ? 'sections'")[0])
