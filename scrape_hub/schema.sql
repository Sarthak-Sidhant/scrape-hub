CREATE TABLE IF NOT EXISTS runs (
    id          BIGSERIAL PRIMARY KEY,
    project     TEXT NOT NULL,
    repository  TEXT NOT NULL,
    workflow    TEXT NOT NULL DEFAULT '',
    gh_run_id   TEXT NOT NULL DEFAULT '',
    run_attempt INT  NOT NULL DEFAULT 1,
    job_index   INT  NOT NULL DEFAULT 0,
    total_jobs  INT  NOT NULL DEFAULT 1,
    ref         TEXT NOT NULL DEFAULT '',
    actor       TEXT NOT NULL DEFAULT '',
    auth_kind   TEXT NOT NULL,                 -- 'oidc' (verified) or 'token' (self-reported)
    runner_ip   TEXT NOT NULL DEFAULT '',      -- address the API saw
    egress_ip   TEXT NOT NULL DEFAULT '',      -- address the runner reported for itself
    state       TEXT NOT NULL DEFAULT 'running', -- running | finished | failed
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    records     BIGINT NOT NULL DEFAULT 0,
    batches     INT    NOT NULL DEFAULT 0,
    errors      INT    NOT NULL DEFAULT 0,
    blocked     INT    NOT NULL DEFAULT 0,
    note        TEXT   NOT NULL DEFAULT '',
    UNIQUE (project, repository, gh_run_id, run_attempt, job_index)
);
CREATE INDEX IF NOT EXISTS runs_state_seen ON runs (state, last_seen);
CREATE INDEX IF NOT EXISTS runs_started ON runs (started_at);

CREATE TABLE IF NOT EXISTS batches (
    id         BIGSERIAL PRIMARY KEY,
    run_id     BIGINT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    project    TEXT NOT NULL,
    at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    records    INT NOT NULL DEFAULT 0,
    errors     INT NOT NULL DEFAULT 0,
    blocked    INT NOT NULL DEFAULT 0,
    latency_ms INT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS batches_at ON batches (at);
CREATE INDEX IF NOT EXISTS batches_project_at ON batches (project, at);

CREATE TABLE IF NOT EXISTS queue (
    project      TEXT NOT NULL,
    key          TEXT NOT NULL,
    partition_id INT  NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending', -- pending | claimed | done | failed
    payload      JSONB NOT NULL DEFAULT '{}',
    claimed_by   BIGINT,
    claimed_at   TIMESTAMPTZ,
    done_at      TIMESTAMPTZ,
    attempts     INT NOT NULL DEFAULT 0,
    PRIMARY KEY (project, key)
);
CREATE INDEX IF NOT EXISTS queue_claim ON queue (project, status, partition_id);

CREATE TABLE IF NOT EXISTS records (
    project    TEXT NOT NULL,
    record_id  TEXT NOT NULL,
    data       JSONB NOT NULL,
    run_id     BIGINT REFERENCES runs(id) ON DELETE SET NULL,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project, record_id)
);

CREATE TABLE IF NOT EXISTS projects (
    name             TEXT PRIMARY KEY,
    title            TEXT NOT NULL DEFAULT '',
    unit_label       TEXT NOT NULL DEFAULT 'item',    -- what one queue item is, e.g. 'organisation'
    record_label     TEXT NOT NULL DEFAULT 'record',  -- what one stored row is, e.g. 'tender'
    allowed_repos    TEXT[] NOT NULL DEFAULT '{}',    -- empty = unclaimed; the first repo to use it claims it
    expected_units   INT NOT NULL DEFAULT 0,
    expected_records BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO projects (name) SELECT project FROM runs UNION SELECT project FROM queue ON CONFLICT DO NOTHING;

ALTER TABLE queue ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';
ALTER TABLE queue ADD COLUMN IF NOT EXISTS fetched    INT  NOT NULL DEFAULT 0;

-- Free-form counters a scraper reports (captcha_ok, http_429, ...). One row per upload per name.
CREATE TABLE IF NOT EXISTS metrics (
    id      BIGSERIAL PRIMARY KEY,
    run_id  BIGINT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    project TEXT NOT NULL,
    at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    name    TEXT NOT NULL,
    value   DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS metrics_project_name_at ON metrics (project, name, at);

-- Every IP a run has been seen from. source 'api' = the address the hub saw on a call, 'reported' = the runner's own egress lookup.
CREATE TABLE IF NOT EXISTS run_ips (
    run_id     BIGINT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    ip         TEXT NOT NULL,
    source     TEXT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    hits       INT NOT NULL DEFAULT 1,
    PRIMARY KEY (run_id, ip, source)
);
INSERT INTO run_ips (run_id, ip, source, first_seen, last_seen)
    SELECT id, runner_ip, 'api', started_at, last_seen FROM runs WHERE runner_ip <> '' ON CONFLICT DO NOTHING;
INSERT INTO run_ips (run_id, ip, source, first_seen, last_seen)
    SELECT id, egress_ip, 'reported', started_at, last_seen FROM runs WHERE egress_ip <> '' ON CONFLICT DO NOTHING;

-- Live knobs the dashboard sets and runners read back on every call: {threads, delay_ms, paused, auto_backoff}.
ALTER TABLE projects ADD COLUMN IF NOT EXISTS settings JSONB NOT NULL DEFAULT '{}';

-- Everything a scraper could not get, one row per thing: a document the portal refused, a tender that errored, ...
-- Re-reporting the same (project, kind, key) updates the row; a later success clears it.
CREATE TABLE IF NOT EXISTS failures (
    project    TEXT NOT NULL,
    kind       TEXT NOT NULL,
    key        TEXT NOT NULL,
    reason     TEXT NOT NULL DEFAULT '',
    detail     JSONB NOT NULL DEFAULT '{}',
    run_id     BIGINT REFERENCES runs(id) ON DELETE SET NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts   INT NOT NULL DEFAULT 1,
    PRIMARY KEY (project, kind, key)
);
CREATE INDEX IF NOT EXISTS failures_kind_seen ON failures (project, kind, last_seen);

-- Is the thing we scrape reachable? One row per check, written by scripts/probe.py every minute from the hub host.
CREATE TABLE IF NOT EXISTS probes (
    target TEXT NOT NULL,
    at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    ok     BOOLEAN NOT NULL,
    ms     INT NOT NULL DEFAULT 0,
    detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS probes_target_at ON probes (target, at);

-- Lets a runner ask "how many tenders of this organisation and window are already stored?" without scanning every record.
CREATE INDEX IF NOT EXISTS records_org_window ON records (project, (data->>'org_id'), (data->'window'->>0), (data->'window'->>1));

-- How much load each portal server takes right now: scripts/ramp.py raises parallel workers (1, 4, 8, 16) for a few seconds each and
-- stops at the first step where the server slows down. One row per server per ramp; `steps` holds every step measured.
CREATE TABLE IF NOT EXISTS capacity (
    server_ip  TEXT NOT NULL,
    at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    portals    TEXT[] NOT NULL DEFAULT '{}',
    safe       INT NOT NULL DEFAULT 0,          -- highest worker count that did not slow the server (0: not answering)
    tested_max INT NOT NULL DEFAULT 0,          -- highest worker count tried (safe = tested_max means the limit is above it)
    base_ms    INT,                             -- median answer time with one worker
    steps      JSONB NOT NULL DEFAULT '[]',
    note       TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS capacity_server_at ON capacity (server_ip, at);

-- Precomputed numbers for the data page (scripts/build_showcase.py), so the page never scans the records.
CREATE TABLE IF NOT EXISTS showcase (
    project  TEXT PRIMARY KEY,
    built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    data     JSONB NOT NULL
);

-- Every automatic speed change (settings.adaptive): what the runners measured and what the threads became.
CREATE TABLE IF NOT EXISTS adjustments (
    project      TEXT NOT NULL,
    at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    s_per_req    DOUBLE PRECISION,
    requests     INT NOT NULL DEFAULT 0,
    errors       INT NOT NULL DEFAULT 0,
    threads_from INT NOT NULL,
    threads_to   INT NOT NULL,
    reason       TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS adjustments_project_at ON adjustments (project, at);

ALTER TABLE adjustments ADD COLUMN IF NOT EXISTS claimed_from INT;
ALTER TABLE adjustments ADD COLUMN IF NOT EXISTS claimed_to   INT;
ALTER TABLE adjustments ADD COLUMN IF NOT EXISTS phase        TEXT NOT NULL DEFAULT '';
ALTER TABLE adjustments ADD COLUMN IF NOT EXISTS failed_frac  DOUBLE PRECISION;
