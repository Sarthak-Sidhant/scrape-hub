#!/usr/bin/env python3
"""Precompute the numbers behind the /data page from eprocure.tender_facts (see scripts/ops/tender_facts.sql) into the `showcase` table.
Takes seconds; run daily by scrape-hub-showcase.service right after the facts table is rebuilt.

Money: a stated value of 0 means 'not given' and is left out of medians. Tender values on the portal include obvious typos (a ₹2 lakh job entered as ₹2,00,000 crore). Sums leave out values above
VALUE_CAP and the page says how many were left out; medians are not affected.
"""
import json
import os

import psycopg
from psycopg.rows import dict_row

PROJECT = "eprocure-tenders"
VALUE_CAP = 1e11          # ₹10,000 crore: above this a single tender value is treated as a data-entry error in sums
F = "eprocure.tender_facts"
YEAR = "coalesce(extract(year FROM published_at)::int, id_year)"
REAL = f"is_real AND {YEAR} BETWEEN 2011 AND extract(year FROM now())::int"


def q(c, sql, **kw):
    return c.execute(sql, kw).fetchall()


def build(c):
    cap = {"cap": VALUE_CAP}
    totals = q(c, f"""SELECT count(*) AS tenders, count(DISTINCT org_code) AS org_codes, count(DISTINCT org_name) AS organisations,
                             min(published_at)::date AS first_published, max(published_at) FILTER (WHERE published_at <= now())::date AS last_published,
                             count(tender_value) FILTER (WHERE tender_value > 0) AS with_value, sum(tender_value) FILTER (WHERE tender_value <= %(cap)s) AS value_sum,
                             count(*) FILTER (WHERE tender_value > %(cap)s) AS value_outliers,
                             count(*) FILTER (WHERE stage_group = 'awarded') AS awarded,
                             sum(awarded_value) FILTER (WHERE awarded_value <= %(cap)s) AS awarded_sum,
                             sum(files_stored) AS files_stored, sum(files_refused) AS files_refused, sum(files_bytes) AS files_bytes
                      FROM {F} WHERE {REAL}""", **cap)[0]
    # what the portal said exists: the expected counts the crawler put on its queue windows
    planned = c.execute("SELECT coalesce(sum((payload->>'expected')::bigint), 0) AS n FROM queue WHERE project=%s", (PROJECT,)).fetchone()["n"]
    by_year = q(c, f"""SELECT {YEAR} AS year, count(*) AS tenders,
                              count(*) FILTER (WHERE stage_group = 'awarded') AS awarded,
                              count(*) FILTER (WHERE stage_group = 'in evaluation') AS evaluating,
                              count(*) FILTER (WHERE stage_group = 'to be opened') AS to_open,
                              count(*) FILTER (WHERE stage_group = 'cancelled') AS cancelled,
                              count(*) FILTER (WHERE stage_group = 'retendered') AS retendered,
                              count(*) FILTER (WHERE stage_group = 'unknown') AS unknown,
                              count(tender_value) FILTER (WHERE tender_value > 0) AS with_value,
                              percentile_cont(0.5) WITHIN GROUP (ORDER BY tender_value) FILTER (WHERE tender_value > 0) AS value_median,
                              percentile_cont(0.9) WITHIN GROUP (ORDER BY tender_value) FILTER (WHERE tender_value > 0) AS value_p90,
                              sum(tender_value) FILTER (WHERE tender_value <= %(cap)s) AS value_sum,
                              count(*) FILTER (WHERE bids_count > 0) AS with_bids,
                              count(*) FILTER (WHERE bids_count = 1) AS single_bid,
                              avg(bids_count) FILTER (WHERE bids_count > 0) AS bids_avg,
                              percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM awarded_at - published_at) / 86400)
                                  FILTER (WHERE awarded_at > published_at) AS award_days_median,
                              sum(files_stored) AS files_stored, sum(files_refused) AS files_refused, sum(files_bytes) AS files_bytes
                       FROM {F} WHERE {REAL} GROUP BY 1 ORDER BY 1""", **cap)
    months = q(c, f"""SELECT extract(year FROM published_at)::int AS year, extract(month FROM published_at)::int AS month, count(*) AS n,
                             sum(files_refused) AS refused, sum(files_stored) AS stored
                      FROM {F} WHERE {REAL} AND published_at IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2""")
    categories = q(c, f"""SELECT {YEAR} AS year, coalesce(nullif(tender_category, ''), 'not stated') AS category, count(*) AS n
                          FROM {F} WHERE {REAL} GROUP BY 1, 2 ORDER BY 1, 2""")
    types = q(c, f"""SELECT coalesce(nullif(tender_type, ''), 'not stated') AS type, count(*) AS n,
                            count(*) FILTER (WHERE bids_count = 1) AS single_bid, count(*) FILTER (WHERE bids_count > 0) AS with_bids
                     FROM {F} WHERE {REAL} GROUP BY 1 ORDER BY 2 DESC LIMIT 8""")
    bids = q(c, f"""SELECT CASE WHEN bids_count >= 21 THEN '21+' WHEN bids_count >= 11 THEN '11-20' WHEN bids_count >= 6 THEN '6-10'
                                ELSE bids_count::text END AS bucket, min(bids_count) AS lo, count(*) AS n
                    FROM {F} WHERE {REAL} AND bids_count > 0 GROUP BY 1 ORDER BY 2""")
    orgs = q(c, f"""SELECT org_name, count(*) AS tenders, count(*) FILTER (WHERE stage_group = 'awarded') AS awarded,
                           sum(tender_value) FILTER (WHERE tender_value <= %(cap)s) AS value_sum,
                           percentile_cont(0.5) WITHIN GROUP (ORDER BY tender_value) FILTER (WHERE tender_value > 0) AS value_median,
                           count(*) FILTER (WHERE bids_count = 1) AS single_bid, count(*) FILTER (WHERE bids_count > 0) AS with_bids,
                           count(*) FILTER (WHERE stage_group = 'cancelled') AS cancelled
                    FROM {F} WHERE {REAL} GROUP BY 1 ORDER BY 2 DESC LIMIT 25""", **cap)
    winners = q(c, f"""SELECT upper(btrim(regexp_replace(winner_bidder_name, '\\s+', ' ', 'g'))) AS winner, count(*) AS awards,
                              sum(awarded_value) FILTER (WHERE awarded_value <= %(cap)s) AS awarded_sum, count(DISTINCT org_code) AS buyers
                       FROM {F} WHERE {REAL} AND coalesce(winner_bidder_name, '') <> '' GROUP BY 1 ORDER BY 2 DESC LIMIT 20""", **cap)
    award_days = q(c, f"""SELECT CASE WHEN d < 30 THEN '< 1 month' WHEN d < 60 THEN '1-2 months' WHEN d < 90 THEN '2-3 months'
                                      WHEN d < 180 THEN '3-6 months' WHEN d < 365 THEN '6-12 months' ELSE 'over a year' END AS bucket,
                                 min(d) AS lo, count(*) AS n
                          FROM (SELECT extract(epoch FROM awarded_at - published_at) / 86400 AS d FROM {F}
                                WHERE {REAL} AND awarded_at > published_at) x GROUP BY 1 ORDER BY 2""")
    built = c.execute("SELECT max(scraped_at) AS t FROM eprocure.tender_index").fetchone()["t"]
    return {"totals": {**totals, "planned": planned}, "by_year": by_year, "months": months, "categories": categories, "types": types,
            "bids": bids, "orgs": orgs, "winners": winners, "award_days": award_days, "value_cap": VALUE_CAP,
            "data_as_of": built}


def main():
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, row_factory=dict_row) as c:
        data = build(c)
        c.execute("""INSERT INTO showcase (project, built_at, data) VALUES (%s, now(), %s::jsonb)
                     ON CONFLICT (project) DO UPDATE SET built_at = now(), data = EXCLUDED.data""",
                  (PROJECT, json.dumps(data, default=lambda v: float(v) if hasattr(v, "as_integer_ratio") else str(v))))
    t = data["totals"]
    print(f"showcase built: {t['tenders']:,} tenders, {len(data['by_year'])} years, {len(data['orgs'])} top organisations")


if __name__ == "__main__":
    main()
