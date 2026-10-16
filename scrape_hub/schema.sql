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
