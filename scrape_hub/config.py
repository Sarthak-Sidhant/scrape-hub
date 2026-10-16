import os

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://scrape:scrape@127.0.0.1:5432/scrape_hub")

# GitHub Actions OIDC: the audience a workflow must request, and which repos may write.
OIDC_AUDIENCE = os.environ.get("OIDC_AUDIENCE", "https://scrape.s1d.me")
OIDC_JWKS_URL = "https://token.actions.githubusercontent.com/.well-known/jwks"
OIDC_ISSUER = "https://token.actions.githubusercontent.com"
ALLOWED_OWNERS = {o for o in os.environ.get("ALLOWED_OWNERS", "Sarthak-Sidhant").split(",") if o}
# Optional exact "owner/repo" allow-list; empty means any repo under ALLOWED_OWNERS.
ALLOWED_REPOS = {r for r in os.environ.get("ALLOWED_REPOS", "").split(",") if r}

# Fallback for scrapers that cannot mint OIDC tokens. Off unless set. The repo name is then self-reported.
LEGACY_TOKEN = os.environ.get("SCRAPE_LEGACY_TOKEN", "")

# Only set behind nginx/cloudflared, otherwise X-Forwarded-For can be spoofed.
TRUST_PROXY = os.environ.get("TRUST_PROXY", "") == "1"

# Dashboard login reuses the status hub's scrypt record file.
PASSWORD_FILE = os.environ.get("STATUS_PASSWORD_FILE", "/etc/statusd/password")

# A claimed queue item with no completion for this long is handed to another runner.
CLAIM_TIMEOUT_S = int(os.environ.get("CLAIM_TIMEOUT_S", "1800"))
# A running run with no upload or heartbeat for this long is shown as stalled.
STALL_AFTER_S = int(os.environ.get("STALL_AFTER_S", "300"))
# Raw batch rows older than this are deleted (run totals are kept).
BATCH_RETENTION_DAYS = int(os.environ.get("BATCH_RETENTION_DAYS", "7"))

# Ceiling for the per-runner thread setting, so a slider cannot ask a runner for more than it can usefully do.
MAX_THREADS = int(os.environ.get("MAX_THREADS", "16"))

# Object storage (Cloudflare R2, S3-compatible). Runners never see these keys: the hub hands out short-lived upload links.
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_PRESIGN_SECONDS = int(os.environ.get("R2_PRESIGN_SECONDS", "900"))
