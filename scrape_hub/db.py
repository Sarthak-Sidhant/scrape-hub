from contextlib import contextmanager
from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from . import config

_pool = None


def pool():
    global _pool
    if _pool is None:
        _pool = ConnectionPool(config.DATABASE_URL, min_size=1, max_size=config.DB_POOL_MAX, kwargs={"row_factory": dict_row}, open=True)
    return _pool


def close():
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def conn():
    with pool().connection() as c:
        yield c


def init_schema():
    """Apply schema.sql once per version. Its DDL takes table locks, so with runners already calling the workers that have started, running
    it on every worker start deadlocks (2026-10-20: one worker's startup failure made uvicorn stop the whole hub, over and over). So: skip it
    when this exact schema.sql was applied already, serialise worker startups, and retry a deadlock instead of failing the start."""
    import hashlib
    import time
    import psycopg
    sql = (Path(__file__).parent / "schema.sql").read_text()
    digest = hashlib.sha256(sql.encode()).hexdigest()
    for attempt in range(8):
        try:
            with conn() as c:
                c.execute("SELECT pg_advisory_xact_lock(748210)")      # serialise concurrent worker startups
                c.execute("CREATE TABLE IF NOT EXISTS schema_applied (digest text PRIMARY KEY, at timestamptz NOT NULL DEFAULT now())")
                if c.execute("SELECT 1 FROM schema_applied WHERE digest=%s", (digest,)).fetchone():
                    return
                c.execute("SET LOCAL lock_timeout = '10s'")
                c.execute(sql)
                c.execute("INSERT INTO schema_applied (digest) VALUES (%s) ON CONFLICT DO NOTHING", (digest,))
            return
        except (psycopg.errors.DeadlockDetected, psycopg.errors.LockNotAvailable):
            time.sleep(1 + attempt)
    raise RuntimeError("could not apply schema.sql (kept deadlocking with live traffic)")
