-- One narrow, typed row per stored eprocure tender, for the /data page and for quick analysis. Reading the `eprocure.tenders` view parses
-- every record's JSON (about 7 minutes for 850,000 tenders); this table holds the result so aggregates take seconds.
-- Built daily by scrape-hub-showcase.service (as postgres, before scripts/build_showcase.py), or by hand:
--   sudo -u postgres psql -d scrape_hub -f scripts/ops/tender_facts.sql
BEGIN;
CREATE SCHEMA IF NOT EXISTS eprocure;
DROP TABLE IF EXISTS eprocure.tender_facts_new;
CREATE TABLE eprocure.tender_facts_new AS
SELECT t.tender_id,
       CASE WHEN t.tender_id ~ '^\d{4}_' THEN split_part(t.tender_id, '_', 1)::int END AS id_year,
       split_part(t.tender_id, '_', 2) AS org_code,
       t.org_id, t.org_name,
       CASE WHEN t.stage IN ('AOC') THEN 'awarded'
            WHEN t.stage IN ('Cancelled') THEN 'cancelled'
            WHEN t.stage IN ('Retender') THEN 'retendered'
            WHEN t.stage IN ('To_be_Opened', 'To Be Opened Tenders') THEN 'to be opened'
            WHEN t.stage IS NULL OR t.stage = '' THEN 'unknown'
            ELSE 'in evaluation' END AS stage_group,
       t.stage, t.tender_type, t.tender_category, t.form_of_contract, t.product_category,
       t.tender_value, t.emd_amount, t.bids_count, t.qualified_bids_count,
       -- the award: the bid the portal marks Accepted-AOC, and the AOC's "Total Contract Value" (rupees only; the portal puts a non-breaking space after INR)
       (SELECT b->>'bidder_name' FROM jsonb_array_elements(CASE WHEN jsonb_typeof(r.data->'details'->'bids') = 'array' THEN r.data->'details'->'bids' ELSE '[]'::jsonb END) b
         WHERE b->>'status' ILIKE 'Accepted-AOC%' LIMIT 1) AS winner_bidder_name,
       CASE WHEN r.data->'details'->'outcomes'->'aoc'->>'Total Contract Value' ~ '^\s*INR[^0-9]*[0-9,]+(\.[0-9]+)?\s*$'
            THEN regexp_replace(r.data->'details'->'outcomes'->'aoc'->>'Total Contract Value', '[^0-9.]', '', 'g')::numeric END AS awarded_value,
       t.published_at, t.bid_submission_end_at, t.bid_opening_at, t.awarded_at,
       coalesce(t.tender_title, '') <> '' AS is_real,
       coalesce(i.files_stored, 0) AS files_stored, coalesce(i.files_refused, 0) AS files_refused, coalesce(i.files_bytes, 0) AS files_bytes
FROM eprocure.tenders t
JOIN records r ON r.project = 'eprocure-tenders' AND r.record_id = t.tender_id
LEFT JOIN eprocure.tender_index i ON i.project = 'eprocure-tenders' AND i.tender_id = t.tender_id;
CREATE INDEX ON eprocure.tender_facts_new (published_at);
CREATE INDEX ON eprocure.tender_facts_new (org_code);
DROP TABLE IF EXISTS eprocure.tender_facts;
ALTER TABLE eprocure.tender_facts_new RENAME TO tender_facts;
GRANT USAGE ON SCHEMA eprocure TO scrape_hub;
GRANT SELECT ON ALL TABLES IN SCHEMA eprocure TO scrape_hub;
COMMIT;
ANALYZE eprocure.tender_facts;
