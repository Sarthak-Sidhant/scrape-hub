"""Dashboard login using the same scrypt password record as the status hub (statusd.py)."""
import hashlib
import hmac
import time

from . import config

COOKIE = "scrape_session"
FAILS = {}  # client ip -> recent failure timestamps


def record() -> str:
    try:
        with open(config.PASSWORD_FILE) as f:
            return f.read().strip()
    except OSError:
        return ""


def password_ok(candidate: str) -> bool:
    """Record format: scrypt$<salt hex>$<hash hex>. Fails closed when no password has been set."""
    try:
        scheme, salt, digest = record().split("$")
        got = hashlib.scrypt(candidate.encode(), salt=bytes.fromhex(salt), n=2 ** 14, r=8, p=1, dklen=32)
        return scheme == "scrypt" and hmac.compare_digest(got.hex(), digest)
    except ValueError:
        return False


def session_token() -> str:
    """Derived from the stored hash, so changing the password signs everyone out."""
    rec = record()
    return hmac.new(rec.encode(), b"scrape-hub-session-v1", "sha256").hexdigest() if rec else ""


def cookie_ok(value: str) -> bool:
    want = session_token()
    return bool(want) and bool(value) and hmac.compare_digest(value, want)


def throttled(ip: str) -> bool:
    now = time.time()
    FAILS[ip] = [t for t in FAILS.get(ip, []) if now - t < 300]
    return len(FAILS[ip]) >= 8


def note_failure(ip: str):
    FAILS.setdefault(ip, []).append(time.time())
