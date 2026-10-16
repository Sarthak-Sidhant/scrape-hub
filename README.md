# scrape-hub

Ingest API and live dashboard (`scrape.s1d.me`) for scrapers that run in GitHub Actions and write to Postgres on the VPS.

- **Runners** (GitHub Actions) claim work from a queue, scrape, and POST batches to `/api/v1/...`.
- **Identity**: each job mints a GitHub OIDC token (`permissions: id-token: write`). The hub verifies its signature
  against GitHub's keys and takes the repository, workflow, run id and actor from the signed claims, so a runner cannot
  claim to be another repo. `SCRAPE_LEGACY_TOKEN` is an optional shared-token fallback; repos are then self-reported.
- **Dashboard**: `/` after signing in with the same password as `status.s1d.me` (`STATUS_PASSWORD_FILE`). Shows repos,
  runners (API-seen and runner-reported IP), records/minute, queue progress and health rules (`scrape_hub/health.py`).

## Layout

| Path | What |
|---|---|
| `scrape_hub/` | FastAPI app, schema, OIDC auth, health rules |
| `web/` | dashboard and login pages (no build step) |
| `reporter/hubclient.py` | client library scrapers import |
| `crawler/` | FY 2025-26 eprocure crawler for Actions (vendors `scrape_organization.py` and the captcha CNN) |
| `scripts/seed_fy2526.py` | fills the work queue (428 organisations, ~47,973 tenders) |
| `scripts/dev.py` | local hub with embedded Postgres |
| `deploy/` | systemd unit and env template |

## Deploy (VPS)

1. Postgres: `CREATE USER scrape_hub ...; CREATE DATABASE scrape_hub OWNER scrape_hub;`
2. `git clone` to `/opt/scrape-hub`, `python3 -m venv .venv && .venv/bin/pip install fastapi "uvicorn[standard]" "psycopg[binary,pool]" pyjwt cryptography`
3. Copy `deploy/env.example` to `/etc/scrape-hub/env`, fill it in; install `deploy/scrape-hub.service`; `systemctl enable --now scrape-hub`.
4. Route `scrape.s1d.me` to `127.0.0.1:8787` (Cloudflare Tunnel, as for `status.s1d.me`). Do not put Cloudflare Access in front:
   runners cannot pass it, and the dashboard has its own password.
5. Seed: `DATABASE_URL=... .venv/bin/python scripts/seed_fy2526.py`
6. Smoke test from Actions: run **Crawl eprocure FY 2025-26** with `total_jobs=2`, `max_orgs=1`, `max_tenders=5`.

## Tests

`.venv/bin/pip install pytest httpx pgserver && .venv/bin/python -m pytest tests` (uses an embedded Postgres).
