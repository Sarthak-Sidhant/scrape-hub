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

Sign in with the same password as `status.s1d.me` (`STATUS_PASSWORD_FILE`). Per project: records fetched vs expected, ETA from the
recent rate, queue progress, a tile per runner slice, scraper metrics, failed targets with their error and a retry button, and
in-flight targets. Across projects: records/minute chart, live runner rows (what each is doing, API-seen and runner-reported IP),
repositories, IP history. Health rules are in `scrape_hub/health.py`.

## Layout

| Path | What |
|---|---|
| `scrape_hub/` | FastAPI app, schema, OIDC auth, health rules |
| `web/` | dashboard and login pages (no build step) |
| `client/` | installable client library (`scrape-hub-client`) |
| `deploy/` | systemd unit and env template |
| `scripts/dev.py`, `scripts/demo_data.py` | local hub with embedded Postgres, and synthetic activity to review the UI |
| `tests/` | API, auth and health tests (embedded Postgres) |

## Deploy (VPS)

1. Postgres: `CREATE USER scrape_hub ...; CREATE DATABASE scrape_hub OWNER scrape_hub;`
2. Clone to `/opt/scrape-hub`, `python3 -m venv .venv && .venv/bin/pip install fastapi "uvicorn[standard]" "psycopg[binary,pool]" pyjwt cryptography`
3. Copy `deploy/env.example` to `/etc/scrape-hub/env` and fill it in; install `deploy/scrape-hub.service`; `systemctl enable --now scrape-hub`.
   The schema migrates itself on start.
4. Route `scrape.s1d.me` to `127.0.0.1:8787` (Cloudflare Tunnel). Do not put Cloudflare Access in front: runners cannot pass it and the
   dashboard has its own password.

## Tests

`.venv/bin/pip install pytest httpx pgserver && .venv/bin/python -m pytest tests`
