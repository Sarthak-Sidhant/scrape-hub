-- Indexed lookup tables over the hub's `records`, so questions like "where are this tender's files" or "all tenders of one organisation code
-- in 2019" do not scan the JSON of 850,000 records. Paths in object storage contain the portal's organisation id, which can change, so these
-- tables are the stable way to find things: they hold the exact stored key next to the tender id, the organisation CODE from the tender id
-- (stable) and the organisation id/name as scraped.
--
-- Rebuild (takes a few minutes, reads `records`, replaces both tables in one transaction):
--   sudo -u postgres psql -d scrape_hub -f scripts/ops/tender_index.sql
BEGIN;
CREATE SCHEMA IF NOT EXISTS eprocure;
DROP TABLE IF EXISTS eprocure.org_index, eprocure.tender_index, eprocure.file_index;

-- one row per downloaded or refused document file
CREATE TABLE eprocure.file_index AS
SELECT r.project, r.record_id AS tender_id, d->>'name' AS name, d->>'kind' AS kind, d->>'status' AS status, d->>'content_type' AS content_type,
       NULLIF(d->>'bytes', '')::bigint AS bytes, d->>'sha256' AS sha256, d->>'key' AS r2_key, d->>'reason' AS reason
FROM records r, jsonb_array_elements(coalesce(r.data->'details'->'documents', '[]'::jsonb)) d
WHERE r.project = 'eprocure-tenders';
CREATE INDEX file_index_tender ON eprocure.file_index (tender_id);
CREATE INDEX file_index_status ON eprocure.file_index (kind, status);
CREATE INDEX file_index_key ON eprocure.file_index (r2_key);

-- one row per tender: where it is, who it belongs to, what we hold
CREATE TABLE eprocure.tender_index AS
SELECT r.project, r.record_id AS tender_id,
       CASE WHEN r.record_id ~ '^\d{4}_' THEN split_part(r.record_id, '_', 1)::int END AS year,
       split_part(r.record_id, '_', 2) AS org_code,
       r.data->>'org_id' AS org_id, r.data->>'org_name' AS org_name,
       r.data->'details'->'fields'->>'stage' AS stage,
       r.data->'details'->'fields'->>'tender_title' AS title,
       r.data->'details'->'fields'->>'tender_ref_no' AS ref_no,
       eprocure.ts(r.data->'details'->'fields'->>'published_date')::date AS published_date,
       r.data->'details'->>'pages_key' AS pages_key,
       r.data->'window'->>0 AS window_from, r.data->'window'->>1 AS window_to,
       (r.data->'details' ? 'sections') AS has_sections,
       coalesce(jsonb_array_length(r.data->'details'->'sections'->'nit_documents'), 0) AS nit_documents,
       coalesce(jsonb_array_length(r.data->'details'->'sections'->'work_item_documents'), 0) AS work_item_documents,
       coalesce(jsonb_array_length(r.data->'details'->'sections'->'covers'), 0) AS covers,
       coalesce(f.stored, 0) AS files_stored, coalesce(f.refused, 0) AS files_refused, coalesce(f.failed, 0) AS files_failed, coalesce(f.bytes, 0) AS files_bytes,
       (coalesce(r.data->'details'->'fields'->>'tender_title', '') <> ''
        AND (NOT (r.data->'details' ? 'sections') OR coalesce(jsonb_array_length(r.data->'details'->'sections'->'covers'), 0) > 0)) AS is_real,
       r.scraped_at
FROM records r
LEFT JOIN (SELECT tender_id, count(*) FILTER (WHERE status = 'stored') AS stored, count(*) FILTER (WHERE status = 'refused') AS refused,
                  count(*) FILTER (WHERE status = 'failed') AS failed, coalesce(sum(bytes) FILTER (WHERE status = 'stored'), 0) AS bytes
           FROM eprocure.file_index GROUP BY tender_id) f ON f.tender_id = r.record_id
WHERE r.project = 'eprocure-tenders';
ALTER TABLE eprocure.tender_index ADD PRIMARY KEY (tender_id);
CREATE INDEX tender_index_org_year ON eprocure.tender_index (org_code, year);
CREATE INDEX tender_index_year ON eprocure.tender_index (year);
CREATE INDEX tender_index_stage ON eprocure.tender_index (stage);
CREATE INDEX tender_index_published ON eprocure.tender_index (published_date);
CREATE INDEX tender_index_org_id ON eprocure.tender_index (org_id);
CREATE INDEX tender_index_real ON eprocure.tender_index (is_real) WHERE NOT is_real;
CREATE INDEX tender_index_window ON eprocure.tender_index (org_id, window_from, window_to);

GRANT USAGE ON SCHEMA eprocure TO scrape_hub;
GRANT SELECT ON eprocure.tender_index, eprocure.file_index TO scrape_hub;
COMMIT;
ANALYZE eprocure.tender_index;
ANALYZE eprocure.file_index;

-- Which organisation belongs to which tenders, and where their files sit in object storage. The organisation id in the R2 path is the
-- portal's dropdown id at scrape time; the organisation code is the one inside the tender id and does not change. One row per pair.
DROP TABLE IF EXISTS eprocure.org_index;
CREATE TABLE eprocure.org_index AS
SELECT org_id, org_code, max(org_name) AS org_name, count(*) AS tenders, min(year) AS first_year, max(year) AS last_year,
       count(*) FILTER (WHERE is_real) AS real_tenders,
       array_agg(DISTINCT year ORDER BY year) AS years,
       'eprocure-tenders/pages/<year>/' || org_id || '/<tender id>.json.gz' AS pages_path,
       'eprocure-tenders/docs/<year>/' || org_id || '/<tender id>/<file>' AS docs_path
FROM eprocure.tender_index GROUP BY org_id, org_code;
CREATE INDEX org_index_code ON eprocure.org_index (org_code);
CREATE INDEX org_index_id ON eprocure.org_index (org_id);
CREATE INDEX org_index_name ON eprocure.org_index (lower(org_name) text_pattern_ops);
GRANT SELECT ON eprocure.org_index TO scrape_hub;
ANALYZE eprocure.org_index;
