# Operator scripts for the hub host

One-off helpers written while running the eprocure scrape. They read the hub database through `DATABASE_URL`, so run them as the hub's
user with its environment, from `/opt/scrape-hub`:

```bash
sudo cp scripts/ops/state.py /opt/scrape-hub/_state.py
sudo -u scrapehub bash -c 'set -a; . /etc/scrape-hub/env; set +a; cd /opt/scrape-hub && .venv/bin/python _state.py'
```

| Script | What it prints or does |
|---|---|
| `state.py` | Records, queue by status (tenders and sections), speed settings, running runs, latest probe, failures |
| `analyze_run.py <gh_run_id> <procs per runner>` | Per-runner tenders/min, CPU, phase times, captcha and refusal counts for one workflow run |
| `phase_breakdown.py` | Seconds per tender by phase (list, fetch, docs, upload, parse), captcha attempts per tender |
| `window_coverage.py` | For pending/failed windows: how many tenders are already stored vs expected |
| `queue_by_size.py` | Queue status by window size, and time per window |
| `document_counts.py` | Document files stored/refused/failed, by kind and type; tenders with at least one file |
| `refused_by_year.py`, `refused_by_month.py` | Where the portal refuses documents (by tender year, by publication month) |
| `ip_report.py` | Runner IPs by hub calls, tenders and uploaded GB |
| `sections_quality.py`, `missing_sections.py` | Checks on the parsed document lists (counts, tenders still missing them) |
| `seed_sections_backfill.py` | Creates the `eprocure-sections` project and one queue item per (year, organisation) |
| `reseed_incomplete_sections.py` | Puts org-years that still have tenders without sections back to pending |
| `reset_windows_with_stale_pages.py` | Puts windows holding "session expired" / "Stale Session" captures back to pending |
| `set_threads.py <n>` | Sets the speed (threads per process) for `eprocure-tenders` (edit the name inside for another project) |

Always read a script before running it: some of them change the queue or settings.
