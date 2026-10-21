-- One narrow, typed row per stored tender of EVERY portal (eprocure-tenders and every gepnic-<portal> project), for the data-quality page
-- (scripts/build_quality.py). Only reads `records`; writes the `quality` schema and nothing else.
--
-- Each record's JSON is read (detoasted) once: jsonb_to_record() in FROM splits it into columns, so Postgres does not re-read the 4 KB
-- compressed value for every field it extracts. (LATERAL (... OFFSET 0) would do the same but cannot run in parallel workers, and neither
-- can correlated subqueries in the select list, so there are none.) The portals write dates as '23-Aug-2024 10:00 AM' and money as '27,44,000.00', 'INR 85,786', 'NA' or 'Nil';
-- quality.ts / quality.num read those without exceptions (an impossible date such as 30-Feb gives NULL, never an error).
--
-- quality.facts_source is a view over `records` that does the parsing; quality.facts is that view stored.
-- First run (or with quality.full=on): one parallel CREATE TABLE AS in disk order (the 44 portals were crawled at the same time, so one
-- portal's rows are spread over the whole table and reading portal by portal was ~50x slower), about 1.5-2 hours for 15.7 million records;
-- the finished table replaces quality.facts. Later runs are incremental: only records whose scraped_at is newer than the facts row (the
-- hub sets it on every upsert) or that are new are parsed again, in one transaction.
-- Run as postgres:
--   sudo -u postgres psql -d scrape_hub -v ON_ERROR_STOP=1 -f scripts/ops/quality_facts.sql
--   sudo -u postgres env PGOPTIONS='-c quality.full=on' psql ...      (rebuild everything)
--   sudo -u postgres env PGOPTIONS='-c quality.projects=gepnic-eprocure-goa,gepnic-pudutenders -c quality.full=on' psql ...  (a test build)
-- The hub keeps serving while it runs: only shared read locks on `records`.
CREATE SCHEMA IF NOT EXISTS quality;

-- '23-Aug-2024 10:00 AM' / '23-Aug-2024' -> timestamp; anything else (NA, empty, 30-Feb-2024, 00:30 AM) -> NULL. Written as one expression so
-- Postgres inlines it (a function with a FROM clause costs ~40 us a call, which made a full build take a day). The nested CASEs fix the
-- order of the checks, so the day is only compared once the text is known to be a date.
CREATE OR REPLACE FUNCTION quality.ts(t text) RETURNS timestamp LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN t ~ '^\d{2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(199\d|20\d\d)( (0[1-9]|1[0-2]):[0-5]\d (AM|PM))?$' THEN
           CASE WHEN substr(t, 1, 2)::int BETWEEN 1 AND extract(day FROM to_date(substr(t, 4, 8), 'Mon-YYYY') + interval '1 month - 1 day')::int THEN
             CASE WHEN length(t) > 11 THEN to_timestamp(t, 'DD-Mon-YYYY HH12:MI AM') ELSE to_timestamp(t, 'DD-Mon-YYYY') END::timestamp END END
$$;

-- 'DD/MM/YYYY' (the crawler's window dates) -> date
CREATE OR REPLACE FUNCTION quality.dmy(t text) RETURNS date LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN t ~ '^\d{2}/(0[1-9]|1[0-2])/(199\d|20\d\d)$' THEN
           CASE WHEN substr(t, 1, 2)::int BETWEEN 1 AND extract(day FROM to_date(substr(t, 4, 7), 'MM/YYYY') + interval '1 month - 1 day')::int
                THEN to_date(t, 'DD/MM/YYYY') END END
$$;

-- '27,44,000.00', 'INR 85,786', '₹ 1,200', 85786.0 -> numeric; 'NA', 'Nil', '', '1.2.3', 'see BOQ' -> NULL
CREATE OR REPLACE FUNCTION quality.num(t text) RETURNS numeric LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN replace(t, chr(160), ' ') ~ '^\s*(INR|Rs\.?|₹)?\s*\d[\d,]*(\.\d+)?\s*(/-)?\s*$'
              THEN left(regexp_replace(t, '[^0-9.]', '', 'g'), 30)::numeric END
$$;

-- How a stated value reads: 'num' (a number above 0), 'zero', 'na' (NA / Nil / - / not applicable), 'empty' (label missing or blank),
-- 'bad' (text that is not a number)
CREATE OR REPLACE FUNCTION quality.vstate(t text) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN btrim(coalesce(t, '')) = '' THEN 'empty'
              WHEN lower(btrim(t)) IN ('na', 'n/a', 'n.a.', 'n.a', 'nil', '-', '--', 'not applicable', 'none', 'null') THEN 'na'
              WHEN quality.num(t) IS NULL THEN 'bad'
              WHEN quality.num(t) = 0 THEN 'zero'
              ELSE 'num' END
$$;

-- blank or a placeholder: the portal writes NA / Nil / - when a buyer leaves a field empty
CREATE OR REPLACE FUNCTION quality.filled(t text) RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT btrim(coalesce(t, '')) <> '' AND lower(btrim(t)) NOT IN ('na', 'n/a', 'n.a.', 'n.a', 'nil', '-', '--', 'none', 'null', '0')
$$;

CREATE OR REPLACE VIEW quality.facts_source AS
SELECT r.project, r.record_id AS tender_id,
       r.record_id ~ '^\d{4}_[^_]+_\d+_\d+$' AS id_ok,                                      -- tender id reads YEAR_ORGCODE_NUMBER_VERSION
       CASE WHEN r.record_id ~ '^\d{4}_' THEN left(r.record_id, 4)::int END AS id_year,
       split_part(r.record_id, '_', 2) AS org_code,
       CASE WHEN r.record_id ~ '_\d{1,4}$' THEN substring(r.record_id FROM '_(\d{1,4})$')::int END AS id_version,   -- 1 = first publication
       a.org_id,
       CASE WHEN coalesce(f->>'org_chain', '') = '' OR coalesce(a.org_name, '') = '' THEN NULL       -- the organisation searched for is the
            ELSE lower(regexp_replace(split_part(f->>'org_chain', '||', 1), '\s+', '', 'g'))           -- first link of the tender's own chain
                 = lower(regexp_replace(a.org_name, '\s+', '', 'g')) END AS org_name_match,
       win->>0 AS win_from, win->>1 AS win_to,                                                -- 'DD/MM/YYYY' as on the queue payloads
       r.scraped_at,
       coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(y.pages_history) = 'array' THEN y.pages_history END), 0) AS pages_history,
       coalesce(jsonb_typeof(a.details) = 'object', false) AS has_details,
       coalesce(jsonb_typeof(f) = 'object', false) AS has_fields,
       y.parse_error IS NOT NULL AS parse_error,
       y.sections_error IS NOT NULL AS sections_error,
       y.sections IS NOT NULL AS has_sections,
       coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(secs->'covers') = 'array' THEN secs->'covers' END), 0) AS covers,
       coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(secs->'nit_documents') = 'array' THEN secs->'nit_documents' END), 0) AS nit_docs,
       coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(secs->'work_item_documents') = 'array' THEN secs->'work_item_documents' END), 0) AS work_docs,
       coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(secs->'corrigenda') = 'array' THEN secs->'corrigenda' END), 0) AS corrigenda,
       coalesce(y.pages_key, '') <> '' AS has_pages,                                        -- raw pages stored
       coalesce(btrim(f->>'tender_title'), '') <> '' AS title_ok,
       CASE WHEN coalesce(lst->>'title', '') = '' OR coalesce(f->>'tender_title', '') = '' THEN NULL   -- results-row title = details title
            ELSE regexp_replace(lower(lst->>'title'), '\s+', '', 'g') = regexp_replace(lower(f->>'tender_title'), '\s+', '', 'g') END AS list_title_match,
       coalesce(btrim(f->>'tender_ref_no'), '') <> '' AS ref_ok,
       coalesce(nullif(f->>'stage', ''), lst->>'stage') AS stage,                                -- as the results row showed it
       coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(f->'stage_done') = 'array' THEN f->'stage_done' END), 0) AS stage_done_n,
       nullif(f->>'last_stage_completed', '') AS last_stage,
       CASE WHEN jsonb_typeof(f->'date_flags') = 'array' AND jsonb_array_length(f->'date_flags') > 0      -- the parser's own date flags
            THEN f->'date_flags' END AS date_flags,
       quality.ts(f->>'published_date') AS published_at, quality.filled(f->>'published_date') AS pub_raw,
       quality.ts(f->>'bid_submission_start_date') AS sub_start_at,
       quality.ts(f->>'bid_submission_end_date') AS sub_end_at, quality.filled(f->>'bid_submission_end_date') AS sub_end_raw,
       quality.ts(f->>'bid_opening_date') AS open_at, quality.filled(f->>'bid_opening_date') AS open_raw,
       quality.ts(f->>'financial_bid_opening_date') AS fin_open_at,
       quality.ts(f->>'award_date') AS award_at, quality.filled(f->>'award_date') AS award_raw,
       quality.vstate(f->>'tender_value') AS tv_state, quality.num(f->>'tender_value') AS tender_value,
       quality.vstate(f->>'emd_amount') AS emd_state, quality.num(f->>'emd_amount') AS emd_amount,
       quality.vstate(f->>'tender_fee') AS fee_state,
       nullif(btrim(f->>'tender_category'), '') AS tender_category, nullif(btrim(f->>'tender_type'), '') AS tender_type,
       nullif(btrim(f->>'form_of_contract'), '') AS form_of_contract,
       quality.filled(f->>'product_category') AS product_ok, quality.filled(f->>'location') AS location_ok,
       CASE WHEN btrim(coalesce(f->>'pincode', '')) = '' THEN 'empty'
            WHEN btrim(f->>'pincode') ~ '^[1-9]\d{5}$' THEN 'ok'
            WHEN NOT quality.filled(f->>'pincode') THEN 'na'
            ELSE 'bad' END AS pin_state,
       quality.filled(f->>'tia_name') AS tia_ok,
       CASE WHEN f->>'bid_validity_days' ~ '^\d{1,6}$' THEN (f->>'bid_validity_days')::int END AS bid_validity,
       CASE WHEN f->>'period_of_work_days' ~ '^\d{1,6}$' THEN (f->>'period_of_work_days')::int END AS work_days,
       CASE WHEN f->>'bids_count' ~ '^\d{1,6}$' THEN (f->>'bids_count')::int END AS bids_count,        -- NULL = no bids list on the page
       CASE WHEN jsonb_typeof(y.bids) = 'array' THEN jsonb_array_length(y.bids) END AS bids_len,           -- rows in details.bids
       CASE WHEN f->>'qualified_bids_count' ~ '^\d{1,6}$' THEN (f->>'qualified_bids_count')::int END AS qualified,
       CASE WHEN jsonb_typeof(y.bids) = 'array'                                                       -- bids marked Accepted-AOC
            THEN jsonb_array_length(jsonb_path_query_array(y.bids, '$[*] ? (@.status like_regex "^Accepted-AOC" flag "i")')) END AS aoc_bids,
       quality.filled(f->>'winner_bidder_name') AS winner_ok,
       coalesce(jsonb_array_length(CASE WHEN jsonb_typeof(f->'winners') = 'array' THEN f->'winners' END), 0) AS winners_n,
       quality.num(f->>'contract_value_aoc_total') AS awarded_value,                                  -- the AOC's Total Contract Value
       quality.num(f->>'awarded_value_sum') AS awarded_sum,                                           -- sum of the Awarded Bids List
       nullif(f->>'aoc_currency', '') AS aoc_currency,
       coalesce(jsonb_typeof(y.outcomes->'aoc') = 'object' AND y.outcomes->'aoc' <> '{}'::jsonb, false) AS has_aoc,   -- an AOC section
       quality.num(f->>'l1_bid_value') AS l1_value,
       CASE WHEN f->>'l1_inferred' IN ('true', 'false') THEN (f->>'l1_inferred')::boolean END AS l1_inferred,
       CASE WHEN f->>'priced_bidders' ~ '^\d{1,6}$' THEN (f->>'priced_bidders')::int END AS priced,
       jsonb_array_length(docs) AS docs_n,
       jsonb_array_length(jsonb_path_query_array(docs, '$[*] ? (@.status == "stored")')) AS docs_stored,
       jsonb_array_length(jsonb_path_query_array(docs, '$[*] ? (@.status == "refused")')) AS docs_refused,
       jsonb_array_length(jsonb_path_query_array(docs, '$[*] ? (@.status == "failed")')) AS docs_failed,
       jsonb_array_length(jsonb_path_query_array(docs, '$[*] ? (@.status == "not_stored")')) AS docs_unfetched,           -- listed, never downloaded
       jsonb_array_length(jsonb_path_query_array(docs, '$[*] ? (@.status == "stored" && @.size_ok == false)')) AS docs_size_bad,   -- bytes differ from the stated size
       jsonb_array_length(jsonb_path_query_array(docs, '$[*] ? (@.status == "stored" && @.content_type == "html")')) AS docs_html  -- "documents" that are an HTML page
FROM records r
CROSS JOIN LATERAL jsonb_to_record(r.data) AS a(details jsonb, list jsonb, "window" jsonb, org_id text, org_name text)   -- read the JSON once
CROSS JOIN LATERAL jsonb_to_record(CASE WHEN jsonb_typeof(a.details) = 'object' THEN a.details ELSE '{}'::jsonb END)
     AS y(fields jsonb, sections jsonb, bids jsonb, documents jsonb, outcomes jsonb, pages_key text, pages_history jsonb,
          parse_error jsonb, sections_error jsonb)
CROSS JOIN LATERAL (SELECT y.fields AS f, y.sections AS secs, a.list AS lst, a."window" AS win) v
CROSS JOIN LATERAL (SELECT CASE WHEN jsonb_typeof(y.documents) = 'array' THEN y.documents ELSE '[]'::jsonb END AS docs) dl
WHERE (r.project = 'eprocure-tenders' OR (r.project LIKE 'gepnic-%' AND r.project NOT LIKE '%-plan'))
  AND (coalesce(current_setting('quality.projects', true), '') = '' OR r.project = ANY(string_to_array(current_setting('quality.projects', true), ',')));

SELECT to_regclass('quality.facts') IS NULL OR coalesce(current_setting('quality.full', true), '') = 'on' AS full_build \gset
\if :full_build
SET max_parallel_workers_per_gather = 3;
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;
DROP TABLE IF EXISTS quality.facts_new;
CREATE TABLE quality.facts_new AS SELECT * FROM quality.facts_source;
CREATE INDEX ON quality.facts_new (project, id_year);
CREATE INDEX ON quality.facts_new (project, tender_id);
CREATE INDEX ON quality.facts_new (tender_id);
CREATE INDEX ON quality.facts_new (project, org_id, win_from, win_to);
BEGIN;
DROP TABLE IF EXISTS quality.facts;
ALTER TABLE quality.facts_new RENAME TO facts;
GRANT USAGE ON SCHEMA quality TO scrape_hub;
GRANT SELECT ON quality.facts TO scrape_hub;
COMMIT;
\else
-- records stored or re-fetched since the facts row was made: parse those again
BEGIN;
CREATE TEMP TABLE changed ON COMMIT DROP AS
  SELECT r.project, r.record_id FROM records r LEFT JOIN quality.facts f ON f.project = r.project AND f.tender_id = r.record_id
  WHERE (r.project = 'eprocure-tenders' OR (r.project LIKE 'gepnic-%' AND r.project NOT LIKE '%-plan'))
    AND (f.tender_id IS NULL OR r.scraped_at > f.scraped_at);
DELETE FROM quality.facts f USING changed c WHERE f.project = c.project AND f.tender_id = c.record_id;
INSERT INTO quality.facts SELECT s.* FROM quality.facts_source s JOIN changed c ON c.project = s.project AND c.record_id = s.tender_id;
SELECT count(*) AS reparsed FROM changed \gset
\echo re-parsed :reparsed records
COMMIT;
\endif
ANALYZE quality.facts;
