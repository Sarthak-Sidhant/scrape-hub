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
    sql = (Path(__file__).parent / "schema.sql").read_text()
    with conn() as c:
        # serialise concurrent worker startups (parallel DDL deadlocks at boot)
        c.execute("SELECT pg_advisory_xact_lock(748210)")
        c.execute(sql)
