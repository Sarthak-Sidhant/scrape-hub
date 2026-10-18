# scrape-hub

The central hub every scraper reports to: an ingest API, generic Postgres storage and a live dashboard at `scrape.s1d.me`.
**No scraper code lives here.** Each scraper is its own repo (for example
[eprocure-scrape](https://github.com/Sarthak-Sidhant/eprocure-scrape)) that runs in GitHub Actions, installs the client from this
repo, and reports in.

## How scrapers use it

- **Identity**: each job mints a GitHub OIDC token (`permissions: id-token: write`). The hub verifies its signature against
  GitHub's keys and takes the repository, workflow, run id and actor from the signed claims, so a runner cannot claim to be another
  repo. `SCRAPE_LEGACY_TOKEN` is an optional shared-token fallback (repo then self-reported); leave it unset.
- **Projects**: a project is a named scrape (`eprocure-fy2526`). The first repo to use a name owns it; other repos get a 403.
  A project has a unit of work (an *organisation*, a *page*) kept in a queue, and records (a *tender*, a *row*) stored as JSONB.
- **Queue**: scrapers seed it through the API and each runner claims its slice (`partition % total_jobs == job_index`).
  Put `payload.expected` (expected records for that item) in each item to get progress and ETAs. Stale claims are re-issued.
- **Metrics**: any counter (`captcha_ok`, `captcha_fail`, `http_429`, ...). `x_ok` / `x_fail` pairs show as a success rate.
- **Client**: `pip install "scrape-hub-client @ git+https://github.com/Sarthak-Sidhant/scrape-hub@main#subdirectory=client"`,
  see `client/scrape_hub_client/__init__.py` for the whole API.

## Dashboard

Sign in with the same password as `status.s1d.me` (`STATUS_PASSWORD_FILE`). Every page shares one navigation bar.

| Page | What it shows |
|---|---|
| `/` Overview | Per project: records vs expected, ETA, queue progress, a tile per runner slice, the speed control, metrics, failures (CSV export), the target's reachability. Across projects: records/minute, live runners, run history, repositories, runner IPs. Finished projects can be archived (hidden, never deleted). Health rules: `scrape_hub/health.py` |
| `/portals` | Every GePNIC portal (`data/gepnic_portals.json`): which server it runs on, its share of all tenders (treemap), whether it answers now, the last 24 hours minute by minute, and how many parallel workers its server takes in the Indian night and daytime |
| `/data` | What the scraped eprocure tenders say: per year and stage, monthly seasonality, single-bid share, bids per tender, tender size, time to award, category mix, biggest buyers, most frequent winners, documents the portal refuses |
| `/explore` | Zoomable records-per-minute and outage chart over 7 days |
| `/tender` | Look up one stored tender with its documents |

Background jobs (systemd timers, all as the `scrapehub` user):

| Timer | Does |
|---|---|
| `scrape-hub-probe` (every minute) | `scripts/probe.py`: checks the runner targets (`PROBE_TARGETS`, used for the runners' auto-hold) and every portal in the registry (`gepnic:<portal>`) |
| `scrape-hub-ramp` (03:45 and 15:15 IST) | `scripts/ramp.py`: raises 1, 4, 8, 16 parallel workers per portal server and records where it slows down (table `capacity`); skips servers our runners are scraping |
| `scrape-hub-showcase` (05:00) | rebuilds `eprocure.tender_index`, `eprocure.tender_facts` (`scripts/ops/*.sql`, as postgres) and the `/data` numbers (`scripts/build_showcase.py`, table `showcase`) |
| `scrape-hub-backup` (03:30) | `pg_dump` to object storage |

## Layout

| Path | What |
|---|---|
| `scrape_hub/` | FastAPI app, schema, OIDC auth, health rules, `fleet.py` (portal registry, checks, ramp verdicts) |
| `web/` | dashboard pages (no build step); `web/assets/` holds the shared stylesheet and the small SVG chart kit |
| `data/gepnic_portals.json` | the portal registry: URL, server IP, kind, tender count per portal |
| `scripts/` | `probe.py`, `ramp.py`, `build_showcase.py`, `backup_db.py`; `scripts/ops/` holds operator scripts and the index/facts SQL |
| `client/` | installable client library (`scrape-hub-client`) |
| `deploy/` | systemd units and timers, env template |
| `scripts/dev.py`, `scripts/demo_data.py` | local hub with embedded Postgres, and synthetic activity to review the UI |
| `tests/` | API, auth and health tests (embedded Postgres) |

## Deploy (VPS)

1. Postgres: `CREATE USER scrape_hub ...; CREATE DATABASE scrape_hub OWNER scrape_hub;`
2. Clone to `/opt/scrape-hub`, `python3 -m venv .venv && .venv/bin/pip install fastapi "uvicorn[standard]" "psycopg[binary,pool]" pyjwt cryptography`
3. Copy `deploy/env.example` to `/etc/scrape-hub/env` and fill it in; install `deploy/scrape-hub.service`; `systemctl enable --now scrape-hub`.
   The schema migrates itself on start. Install and enable the timers in `deploy/` (probe, ramp, showcase, backup).
   Update an existing install: run the tests, then `sudo rsync -a --exclude .venv --exclude .git --exclude .devdata --exclude tests --exclude .pytest_cache --exclude web/reports ./ /opt/scrape-hub/`,
   `sudo chown -R scrapehub: /opt/scrape-hub/{scrape_hub,web,client,data,scripts}`, `sudo systemctl restart scrape-hub`.
4. Route `scrape.s1d.me` to `127.0.0.1:8787` (Cloudflare Tunnel). Do not put Cloudflare Access in front: runners cannot pass it and the
   dashboard has its own password.

## Tests

`.venv/bin/pip install pytest httpx pgserver && .venv/bin/python -m pytest tests`
