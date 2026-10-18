import hashlib
import os
import sys
import tempfile
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pgserver  # noqa: E402

_tmp = tempfile.mkdtemp()
_pg = pgserver.get_server(os.path.join(_tmp, "pg"))
os.environ["DATABASE_URL"] = _pg.get_uri()
os.environ["OIDC_AUDIENCE"] = "https://scrape.test"
os.environ["SCRAPE_LEGACY_TOKEN"] = "legacy-secret"
os.environ["STATUS_PASSWORD_FILE"] = os.path.join(_tmp, "password")
os.environ["STALL_AFTER_S"] = "300"
os.environ.update(R2_ENDPOINT="https://acct.r2.example.com", R2_BUCKET="test-bucket", R2_ACCESS_KEY_ID="AKIATEST", R2_SECRET_ACCESS_KEY="secret", R2_PRESIGN_SECONDS="900")

_salt = bytes(range(16))
_h = hashlib.scrypt(b"hunter2hunter2", salt=_salt, n=2 ** 14, r=8, p=1, dklen=32)
open(os.environ["STATUS_PASSWORD_FILE"], "w").write("scrypt$%s$%s\n" % (_salt.hex(), _h.hex()))

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)

from fastapi.testclient import TestClient  # noqa: E402

from scrape_hub import auth, db  # noqa: E402
from scrape_hub.app import app  # noqa: E402

auth._signing_key = lambda token: KEY.public_key()


def make_oidc(repo="Sarthak-Sidhant/results-tenders", owner="Sarthak-Sidhant", aud="https://scrape.test", exp=600, run_id="111", **kw):
    now = int(time.time())
    claims = {"iss": "https://token.actions.githubusercontent.com", "aud": aud, "iat": now, "exp": now + exp,
              "repository": repo, "repository_owner": owner, "run_id": run_id, "run_attempt": "1",
              "workflow": "crawl", "ref": "refs/heads/main", "actor": "sarthak", **kw}
    return jwt.encode(claims, KEY, algorithm="RS256")


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def clean(client):
    with db.conn() as c:
        c.execute("TRUNCATE probes, capacity, showcase, failures, metrics, batches, records, queue, runs, projects RESTART IDENTITY CASCADE")


def hdr(**kw):
    return {"Authorization": f"Bearer {make_oidc(**kw)}"}


@pytest.fixture
def login(client, clean):
    r = client.post("/login", content="password=hunter2hunter2", headers={"content-type": "application/x-www-form-urlencoded"},
                    follow_redirects=False)
    assert r.status_code == 303
    return client
