#!/usr/bin/env python3
"""Precompute the data-quality numbers behind /data/quality from quality.facts (see scripts/ops/quality_facts.sql) into the `showcase`
table under the key 'quality'. Read-only on everything else: queue, failures and the facts table are only queried.

Every check is one SQL condition on a quality.facts row (CHECKS below): `when` says which tenders the check applies to (the denominator),
`bad` which of those fail it. Each check is counted per portal, per portal and year, and once over everything, with two example tender ids
per portal so a number can be followed to a real tender on /tender. Severity: 'error' means the stored record is wrong or unusable as it
is; 'warn' means the portal's data is doubtful or something we should fetch again; 'info' describes what the portals publish.

A tender is "clean" when no error-level check fails for it.
"""
import json
import os
import re
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

KEY = "quality"
F = "quality.facts"
YEAR = "coalesce(extract(year FROM published_at)::int, id_year)"
REAL = "title_ok"
AOC = "title_ok AND stage = 'AOC'"
OPENED = "title_ok AND stage IN ('AOC', 'Financial Bid Opening', 'Financial Evaluation', 'Technical Evaluation')"
VAL = "tv_state = 'num'"
# a window date 'DD/MM/YYYY' as a date (the crawler only writes real dates; quality.dmy validates too but is ~20x slower per row)
WIN_DATE = "make_date(substr({c}, 7, 4)::int, substr({c}, 4, 2)::int, substr({c}, 1, 2)::int)"

# (id, group, severity, label, what it means, applies to (SQL), fails when (SQL))
CHECKS = [
    # the stored record itself
    ("no_fields", "Record", "error", "No parsed fields", "The record has no `fields` object: the parser never ran on it or crashed.", "true", "NOT has_fields"),
    ("parse_error", "Record", "error", "Parser error", "The parser raised an error for this tender; only the raw pages are usable.", "true", "parse_error"),
    ("no_title", "Record", "error", "Empty tender (no title)",
     "Fields exist but the title is blank: usually an error or 'session expired' page stored in place of the tender.", "has_fields", "NOT title_ok"),
    ("bad_id", "Record", "error", "Malformed tender id", "The id does not read YEAR_ORGCODE_NUMBER_VERSION.", "true", "NOT id_ok"),
    ("title_mismatch", "Record", "error", "Details belong to another tender",
     "The title on the results row differs from the title on the details page: the pages fetched may be another tender's.", REAL,
     "list_title_match IS FALSE"),
    ("no_pages", "Record", "warn", "Raw pages not stored", "No pages_key: the portal pages were not saved, so the tender cannot be re-parsed.", "true",
     "NOT has_pages"),
    ("no_sections", "Record", "warn", "Document lists not parsed", "No `sections` (NIT documents, covers, corrigenda) on the record.", REAL,
     "NOT has_sections"),
    ("sections_error", "Record", "error", "Sections parser error", "Parsing the document lists raised an error.", "true", "sections_error"),
    ("no_covers", "Record", "warn", "No covers listed", "Sections were parsed but list no bid covers; every real tender has at least one.",
     f"{REAL} AND has_sections", "covers = 0"),
    ("org_mismatch", "Record", "warn", "Searched organisation is not the tender's",
     "The organisation the crawler searched for is not the first link of the tender's own organisation chain (a renamed or shifted dropdown entry).",
     REAL, "org_name_match IS FALSE"),
    ("no_ref", "Record", "warn", "No reference number", "The tender reference number is blank.", REAL, "NOT ref_ok"),
    # dates
    ("no_pub", "Dates", "error", "No published date", "The details page gives no publish date.", REAL, "NOT pub_raw"),
    ("bad_pub", "Dates", "error", "Unreadable published date", "A publish date is written but is not a real date (e.g. 30-Feb).", REAL,
     "pub_raw AND published_at IS NULL"),
    ("outside_window", "Dates", "error", "Published outside its search window",
     "The portal was asked for tenders published in a date range and returned one published outside it.",
     r"published_at IS NOT NULL AND win_from ~ '^\d{2}/\d{2}/\d{4}$' AND win_to ~ '^\d{2}/\d{2}/\d{4}$'",
     f"published_at::date NOT BETWEEN {WIN_DATE.format(c='win_from')} AND {WIN_DATE.format(c='win_to')}"),
    ("future_pub", "Dates", "error", "Published in the future", "The publish date is after the day the numbers were built.", "published_at IS NOT NULL",
     "published_at > now()"),
    ("year_far", "Dates", "warn", "Id year far from publish year", "The year in the tender id and the publish year differ by more than one.",
     "published_at IS NOT NULL AND id_year IS NOT NULL", f"abs(id_year - extract(year FROM published_at)) > 1"),
    ("closes_before_publish", "Dates", "warn", "Bids close before publication", "Bid submission ends before the tender was published.",
     "published_at IS NOT NULL AND sub_end_at IS NOT NULL", "sub_end_at < published_at"),
    ("opens_before_close", "Dates", "warn", "Bids opened before submission closed", "The bid opening date is before the bid submission end date.",
     "open_at IS NOT NULL AND sub_end_at IS NOT NULL", "open_at < sub_end_at"),
    ("award_before_open", "Dates", "warn", "Awarded before bids were opened", "The contract date is earlier than the bid opening date.",
     "award_at IS NOT NULL AND open_at IS NOT NULL", "award_at::date < open_at::date"),
    ("future_award", "Dates", "warn", "Award dated in the future", "The contract date is after the day the numbers were built.", "award_at IS NOT NULL",
     "award_at > now()"),
    ("impossible_date", "Dates", "warn", "Impossible date on the page", "The parser flagged a date that does not exist (e.g. 31-Apr) in one of the date fields.",
     REAL, "date_flags::text LIKE '%%impossible_date%%'"),
    # money
    ("value_missing", "Values", "info", "No tender value stated", "The tender value is NA, Nil, 0 or blank (buyers may leave it out).", REAL,
     "tv_state IN ('na', 'zero', 'empty')"),
    ("value_unreadable", "Values", "warn", "Unreadable tender value", "The tender value is text that is not a number.", REAL, "tv_state = 'bad'"),
    ("value_huge", "Values", "warn", "Tender value over ₹10,000 crore", "Almost always a data-entry error (paise entered as rupees, extra zeros).", VAL,
     "tender_value > 1e11"),
    ("value_tiny", "Values", "warn", "Tender value under ₹1,000", "Implausibly small for a tender: often lakhs or crores entered as rupees.", VAL,
     "tender_value < 1000"),
    ("emd_over_value", "Values", "warn", "EMD larger than the tender value", "The earnest money deposit exceeds the tender's own value.",
     f"{VAL} AND emd_state = 'num'", "emd_amount > tender_value"),
    ("award_vs_value", "Values", "warn", "Award far from the tender value", "The AOC contract value is over 10x or under 1% of the stated tender value.",
     f"{VAL} AND awarded_value > 0", "awarded_value > 10 * tender_value OR awarded_value < tender_value / 100"),
    ("award_sum_mismatch", "Values", "info", "AOC total differs from awarded bids", "The AOC 'Total Contract Value' and the sum of the Awarded Bids List differ by over 1%.",
     "awarded_value IS NOT NULL AND awarded_sum IS NOT NULL", "abs(awarded_sum - awarded_value) > greatest(1, awarded_value / 100)"),
    ("not_inr", "Values", "info", "Award not in rupees", "The AOC value is in another currency.", "awarded_value IS NOT NULL",
     "coalesce(aoc_currency, 'INR') <> 'INR'"),
    # awards and bids
    ("aoc_no_section", "Awards", "error", "Awarded, but no AOC section", "The stage is AOC but the summary page has no award-of-contract section.", AOC,
     "NOT has_aoc"),
    ("aoc_no_winner", "Awards", "error", "Awarded, but no winner", "The stage is AOC but no winning bidder could be read.", AOC, "NOT winner_ok"),
    ("aoc_no_bids", "Awards", "error", "Awarded, but no bids list", "The stage is AOC but the tender has no bids list.", AOC, "coalesce(bids_len, 0) = 0"),
    ("aoc_no_value", "Awards", "warn", "Awarded, but no contract value", "The AOC section gives no readable Total Contract Value.", AOC,
     "awarded_value IS NULL"),
    ("aoc_no_date", "Awards", "warn", "Awarded, but no contract date", "The AOC section gives no readable contract date.", AOC, "award_at IS NULL"),
    ("aoc_no_accepted", "Awards", "warn", "Awarded, but no bid marked Accepted-AOC", "The bids list has bids, none with status Accepted-AOC.",
     f"{AOC} AND bids_len > 0", "coalesce(aoc_bids, 0) = 0"),
    ("opened_no_bids", "Awards", "warn", "Bids opened, but no bids list",
     "The tender is at or past technical evaluation but shows no bids (zero-bid tenders are real, but should be rare).",
     f"{OPENED} AND stage <> 'AOC'", "coalesce(bids_len, 0) = 0"),
    ("bids_count_mismatch", "Awards", "error", "Bid count differs from bids list", "bids_count in the fields is not the number of rows in the bids list.",
     "bids_count IS NOT NULL AND bids_len IS NOT NULL", "bids_count <> bids_len"),
    ("qualified_over_bids", "Awards", "error", "More qualified bids than bids", "qualified_bids_count exceeds the number of bids.",
     "bids_count IS NOT NULL AND qualified IS NOT NULL", "qualified > bids_count"),
    ("l1_inferred", "Awards", "info", "Lowest bidder inferred", "No rank was printed; the only priced bid was taken as L1.", "priced > 0",
     "l1_inferred IS TRUE"),
    # documents (per tender)
    ("aoc_no_docs", "Documents", "warn", "Awarded, but no document saved", "An awarded tender with no stored file (refused by the portal, or never listed).", AOC,
     "docs_stored = 0"),
    ("docs_refused", "Documents", "info", "Tender with refused documents", "At least one document link answered with the portal's error page.",
     "docs_n > 0", "docs_refused > 0"),
    ("docs_failed", "Documents", "warn", "Tender with failed downloads", "At least one document failed to download (timeouts, errors) and was not retried successfully.",
     "docs_n > 0", "docs_failed > 0"),
    ("docs_unfetched", "Documents", "warn", "Documents listed, never downloaded", "The tender was stored with documents switched off.", REAL,
     "docs_unfetched > 0"),
    ("docs_size_bad", "Documents", "warn", "File size differs from the portal's", "A stored file's bytes differ from the size the portal states.",
     "docs_stored > 0", "docs_size_bad > 0"),
    ("docs_html", "Documents", "warn", "Stored 'document' is a web page", "A stored file is an HTML page, not a document.", "docs_stored > 0", "docs_html > 0"),
]

# What was found behind the biggest numbers when they were first looked at (21 Oct 2026), shown under the check on the page.
NOTES = {
    "outside_window": "21 Oct 2026: on UP, Maharashtra, Rajasthan and others the details page shows a publish date the portal has overwritten, "
                      "e.g. a 2014 tender 'published' 21-Oct-2026 08:20 PM, or the right day and month with the year set to 2026. The portal's own "
                      "search still matches the real date. Use the bid submission start date for these tenders.",
    "future_pub": "Same cause as 'published outside its search window': the portal overwrote the publish date.",
    "closes_before_publish": "Mostly the overwritten publish dates (see 'published outside its search window').",
    "year_far": "Mostly the overwritten publish dates (see 'published outside its search window').",
    "aoc_no_winner": "21 Oct 2026: all of eprocure.gov.in. Its records were parsed before the parser read the award lists; the AOC section is on the "
                     "stored pages, so re-parsing them (no scraping) fills the winner and the contract value.",
    "aoc_no_value": "Same as 'awarded, but no winner': eprocure.gov.in needs a re-parse of its stored pages.",
    "title_mismatch": "21 Oct 2026: checked by hand, these hold another tender's details (e.g. DDA tenders with each other's titles). They need "
                      "fetching again; the cause is not found yet (a session shared by two fetches is the likely suspect).",
    "aoc_no_docs": "Mostly the portals' purge of old documents: Coal India refuses 99.5-99.9% of document links for tenders of 2014-2021, "
                   "eprocure.gov.in most of early 2015 to early 2017.",
    "docs_refused": "A property of the publication period, not of our load: Coal India 2014-2021, eprocure.gov.in 2015-2017.",
    "docs_unfetched": "21 Oct 2026: West Bengal tenders stored on 19 Oct between 21:00 and 23:00 CEST by a run with documents off. Their documents "
                      "can still be fetched.",
    "value_tiny": "IOCL buyers write 1 as the tender value, meaning 'not stated'.",
    "aoc_no_bids": "etenders.gov.in (AAI, 2017): the portal shows awarded tenders without a bids list.",
    "award_sum_mismatch": "15-21% for every form of contract, mostly the awarded bids list summing to less than the AOC total: the two figures "
                          "mean different things on the portal, not a parsing fault.",
}

# share of real tenders with each field filled in
FILL = [  # (name, short column label, filled when)
    ("title", "title", "title_ok"), ("reference no.", "ref no", "ref_ok"), ("published", "publish", "published_at IS NOT NULL"),
    ("bid close", "close", "sub_end_at IS NOT NULL"), ("bid opening", "opening", "open_at IS NOT NULL"), ("tender value", "value", "tv_state = 'num'"),
    ("EMD", "EMD", "emd_state = 'num'"), ("tender fee", "fee", "fee_state = 'num'"), ("category", "category", "tender_category IS NOT NULL"),
    ("tender type", "type", "tender_type IS NOT NULL"), ("form of contract", "form", "form_of_contract IS NOT NULL"),
    ("product category", "product", "product_ok"), ("location", "location", "location_ok"), ("pincode", "pincode", "pin_state = 'ok'"),
    ("inviting authority", "authority", "tia_ok"), ("bid validity", "validity", "bid_validity IS NOT NULL"), ("work period", "work days", "work_days IS NOT NULL"),
    ("covers", "covers", "covers > 0"), ("bids list", "bids", "bids_len > 0"), ("documents listed", "docs", "docs_n > 0"),
]


def q(c, sql, *args):
    return c.execute(sql, args).fetchall()


def counts_sql(group_by, examples=False):
    """count(when) and count(when AND bad) for every check, grouped by `group_by` (a list of SQL expressions); with examples, also the
    smallest and largest failing tender id of each check."""
    cols = [f"count(*) AS tenders",
            "count(*) FILTER (WHERE NOT (" + " OR ".join(f"coalesce(({w}) AND ({b}), false)" for _, _, s, _, _, w, b in CHECKS if s == "error") + ")) AS clean"]
    for cid, _, _, _, _, w, b in CHECKS:
        cols += [f"count(*) FILTER (WHERE {w}) AS \"{cid}:of\"", f"count(*) FILTER (WHERE ({w}) AND ({b})) AS \"{cid}:n\""]
    for i, (_, _, cond) in enumerate(FILL):
        cols.append(f"count(*) FILTER (WHERE {REAL} AND ({cond})) AS \"fill{i}\"")
    cols += ["count(*) FILTER (WHERE title_ok) AS real", "sum(docs_n) AS docs_n", "sum(docs_stored) AS docs_stored", "sum(docs_refused) AS docs_refused",
             "sum(docs_failed) AS docs_failed", "sum(docs_unfetched) AS docs_unfetched", "count(*) FILTER (WHERE id_version > 1) AS republished",
             "min(scraped_at) AS first_scraped", "max(scraped_at) AS last_scraped"]
    if examples:
        for cid, _, _, _, _, w, b in CHECKS:
            cols += [f"min(tender_id) FILTER (WHERE ({w}) AND ({b})) AS \"{cid}:a\"", f"max(tender_id) FILTER (WHERE ({w}) AND ({b})) AS \"{cid}:b\""]
    keys = ", ".join(group_by)
    return f"SELECT {keys + ', ' if keys else ''}{', '.join(cols)} FROM {F} {'GROUP BY ' + ', '.join(str(i + 1) for i in range(len(group_by))) if keys else ''}"


def unpack(row):
    """{checks: {id: [n, of]}, fill: [n...], ...rest} from one counts row."""
    out, checks = {}, {}
    for k, v in row.items():
        if ":" in k:
            cid, part = k.split(":")
            if part in ("a", "b"):
                if v:
                    out.setdefault("examples", {}).setdefault(cid, [])
                    if v not in out["examples"][cid]:
                        out["examples"][cid].append(v)
            else:
                checks.setdefault(cid, [0, 0])[0 if part == "n" else 1] = int(v or 0)
        elif k.startswith("fill") and k[4:].isdigit():
            out.setdefault("fill", [0] * len(FILL))[int(k[4:])] = int(v or 0)
        else:
            out[k] = v
    out["checks"] = checks
    return out


def add_up(rows):
    """The all-portals row: counts summed, first/last scrape time over all."""
    rows = list(rows)
    out = {"checks": {cid: [sum(r["checks"][cid][0] for r in rows), sum(r["checks"][cid][1] for r in rows)] for cid, *_ in CHECKS},
           "fill": [sum(r["fill"][i] for r in rows) for i in range(len(FILL))]}
    for k in ("tenders", "clean", "real", "docs_n", "docs_stored", "docs_refused", "docs_failed", "docs_unfetched", "republished"):
        out[k] = sum(int(r.get(k) or 0) for r in rows)
    out["first_scraped"] = min((r["first_scraped"] for r in rows if r.get("first_scraped")), default=None)
    out["last_scraped"] = max((r["last_scraped"] for r in rows if r.get("last_scraped")), default=None)
    return out


def portal_titles():
    """Hub project name -> the portal's host and title, from data/gepnic_portals.json (names as eprocure-scrape's portal.py makes them)."""
    p = Path(os.environ.get("PORTALS_JSON") or Path(__file__).resolve().parent.parent / "data" / "gepnic_portals.json")
    out = {"eprocure-tenders": {"portal": "eprocure.gov.in", "title": "CPPP eprocure"}}
    try:
        for x in json.loads(p.read_text())["portals"]:
            slug = "gepnic-" + re.sub(r"[^a-z0-9]+", "-", re.sub(r"\.(gov|nic|co)\.in", "", x["portal"].lower())).strip("-")
            out.setdefault(slug, {"portal": x["portal"], "title": x.get("title") or x["portal"], "june2026": x.get("tenders_june2026")})
    except Exception as e:
        print("portal titles not read:", e, file=sys.stderr)
    return out


def build(c):
    c.execute("SET max_parallel_workers_per_gather = 3")
    c.execute("SET work_mem = '256MB'")
    titles = portal_titles()
    # one pass over the facts, by portal and year; portals and the overall row are sums of these
    cells = [unpack(r) for r in q(c, counts_sql(["project", f"{YEAR} AS year"], examples=True))]
    portals = {}
    for proj in sorted({x["project"] for x in cells}):
        mine = [x for x in cells if x["project"] == proj]
        portals[proj] = add_up(mine)
        exs = {}
        for x in mine:
            for cid, ids in x.get("examples", {}).items():
                exs.setdefault(cid, []).extend(ids)
        portals[proj]["examples"] = {cid: [min(ids), max(ids)] if min(ids) != max(ids) else [min(ids)] for cid, ids in exs.items()}
    overall = add_up(portals.values())
    print(f"counted {overall['tenders']:,} tenders in {len(portals)} projects", file=sys.stderr, flush=True)
    years = []
    for x in cells:
        if x["year"] is not None and 2005 <= x["year"] <= 2030:
            x.pop("examples", None), x.pop("first_scraped", None), x.pop("last_scraped", None)
            years.append(x)

    # coverage: what the queue planned (the portal's own counts per window) against what is stored per window
    windows = q(c, f"""
        WITH stored AS (SELECT project, org_id, win_from, win_to, count(*) AS n FROM {F} GROUP BY 1, 2, 3, 4),
             qw AS (SELECT project, key, status, attempts, last_error, (payload->>'expected')::int AS expected, payload->>'org_id' AS org_id,
                           payload->>'org_name' AS org_name, payload->>'from_date' AS win_from, payload->>'to_date' AS win_to
                    FROM queue WHERE (project = 'eprocure-tenders' OR (project LIKE 'gepnic-%%' AND project NOT LIKE '%%-plan')) AND status <> 'split')
        SELECT qw.*, coalesce(s.n, 0) AS stored FROM qw LEFT JOIN stored s USING (project, org_id, win_from, win_to)""")
    cov, short = {}, []
    for w in windows:
        p = cov.setdefault(w["project"], {"windows": 0, "expected": 0, "stored_in_windows": 0, "by_status": {}, "short_windows": 0, "short_missing": 0,
                                          "over_windows": 0})
        p["windows"] += 1
        p["expected"] += w["expected"] or 0
        p["stored_in_windows"] += min(w["stored"], w["expected"] or 0)
        st = p["by_status"].setdefault(w["status"], {"windows": 0, "expected": 0, "stored": 0})
        st["windows"] += 1
        st["expected"] += w["expected"] or 0
        st["stored"] += w["stored"]
        exp = w["expected"] or 0
        if w["status"] == "done" and exp and w["stored"] < exp * 0.98 and exp - w["stored"] >= 2:
            p["short_windows"] += 1
            p["short_missing"] += exp - w["stored"]
            short.append({k: w[k] for k in ("project", "org_id", "org_name", "win_from", "win_to", "expected", "stored", "attempts", "last_error")})
        if exp and w["stored"] > exp * 1.05 and w["stored"] - exp >= 5:
            p["over_windows"] += 1
    short.sort(key=lambda w: w["stored"] - w["expected"])

    # the same tender id stored under two portals
    # the same tender id under two portals: ids are numbered per portal, so this is almost always two different tenders
    dup_pairs = q(c, f"""WITH d AS (SELECT tender_id FROM {F} GROUP BY 1 HAVING count(*) > 1)
                         SELECT a.project AS a, b.project AS b, count(*) AS n, min(a.tender_id) AS example,
                                count(*) FILTER (WHERE lower(ra.data->'details'->'fields'->>'tender_title') = lower(rb.data->'details'->'fields'->>'tender_title')) AS same_title
                         FROM d JOIN {F} a USING (tender_id) JOIN {F} b ON b.tender_id = d.tender_id AND a.project < b.project
                         JOIN records ra ON ra.project = a.project AND ra.record_id = a.tender_id
                         JOIN records rb ON rb.project = b.project AND rb.record_id = b.tender_id
                         GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 40""")
    # tenders the crawler gave up on (a failure row is deleted when the tender is stored later, so these are still missing)
    fails = q(c, """SELECT project, kind, CASE WHEN reason ILIKE '%%session expired%%' OR reason ILIKE '%%stale session%%' THEN 'session expired page'
                                               WHEN reason ILIKE '%%Failed to request%%' OR reason ILIKE '%%timed out%%' OR reason ILIKE '%%ConnectionError%%' THEN 'portal did not answer'
                                               WHEN reason ILIKE '%%authenticate%%' THEN 'captcha / login failed'
                                               WHEN reason ILIKE '%%hub /api%%' OR reason ILIKE '%%upload%%' THEN 'hub or storage upload error'
                                               WHEN reason ILIKE '%%not a document%%' THEN 'download was not a document'
                                               ELSE left(reason, 60) END AS reason, count(*) AS n
                    FROM failures WHERE project = 'eprocure-tenders' OR project LIKE 'gepnic-%%' GROUP BY 1, 2, 3 ORDER BY 4 DESC""")
    # document files per year of publication
    doc_years = q(c, f"""SELECT project, {YEAR} AS year, sum(docs_n) AS n, sum(docs_stored) AS stored, sum(docs_refused) AS refused,
                                sum(docs_failed) AS failed, sum(docs_unfetched) AS unfetched
                         FROM {F} GROUP BY 1, 2 HAVING {YEAR} BETWEEN 2005 AND 2030""")

    plist = []
    for proj, p in sorted(portals.items(), key=lambda kv: -kv[1]["tenders"]):
        p.setdefault("examples", {})
        p.update({"project": proj, **titles.get(proj, {"portal": proj, "title": proj}), "coverage": cov.get(proj)})
        plist.append(p)
    return {"checks": [{"id": cid, "group": g, "severity": s, "label": lab, "about": about, "note": NOTES.get(cid)} for cid, g, s, lab, about, _, _ in CHECKS],
            "fill_fields": [f for f, _, _ in FILL], "fill_short": [sh for _, sh, _ in FILL], "overall": overall, "portals": plist, "years": years, "short_windows": short[:200],
            "short_windows_total": len(short), "duplicates": dup_pairs, "failures": fails, "doc_years": doc_years,
            "facts_as_of": overall.get("last_scraped")}


def main():
    """--out FILE (or - for stdout) writes the JSON there instead of the showcase table: a dry run."""
    out = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv else None
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, row_factory=dict_row) as c:
        data = build(c)
        if out:
            text = json.dumps(data, default=lambda v: float(v) if hasattr(v, "as_integer_ratio") else str(v))
            sys.stdout.write(text) if out == "-" else Path(out).write_text(text)
            return
        c.execute("""INSERT INTO showcase (project, built_at, data) VALUES (%s, now(), %s::jsonb)
                     ON CONFLICT (project) DO UPDATE SET built_at = now(), data = EXCLUDED.data""",
                  (KEY, json.dumps(data, default=lambda v: float(v) if hasattr(v, "as_integer_ratio") else str(v))))
    o = data["overall"]
    print(f"quality built: {o['tenders']:,} tenders, {o['clean']:,} clean, {len(data['portals'])} portals, {data['short_windows_total']:,} short windows")


if __name__ == "__main__":
    main()
