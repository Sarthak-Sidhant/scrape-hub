"""Runner authentication: GitHub Actions OIDC (verified identity) or an optional shared token (self-reported)."""
import hmac
from dataclasses import dataclass

import jwt
from fastapi import HTTPException, Request

from . import config

_jwks = None


def _signing_key(token):
    """Separate function so tests can swap in a local key."""
    global _jwks
    if _jwks is None:
        _jwks = jwt.PyJWKClient(config.OIDC_JWKS_URL, cache_keys=True, lifespan=3600)
    return _jwks.get_signing_key_from_jwt(token).key


@dataclass
class Caller:
    kind: str            # 'oidc' | 'token'
    repository: str      # owner/repo
    workflow: str = ""
    gh_run_id: str = ""
    run_attempt: int = 1
    ref: str = ""
    actor: str = ""


def client_ip(request: Request) -> str:
    if config.TRUST_PROXY:
        cf = request.headers.get("cf-connecting-ip")
        if cf:
            return cf.strip()
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
    return request.client.host if request.client else ""


def verify_oidc(token: str) -> Caller:
    try:
        claims = jwt.decode(
            token, _signing_key(token), algorithms=["RS256"],
            audience=config.OIDC_AUDIENCE, issuer=config.OIDC_ISSUER,
            options={"require": ["exp", "iat", "iss", "aud"]},
        )
    except jwt.PyJWTError as e:
        raise HTTPException(401, f"invalid OIDC token: {e}")
    repo = claims.get("repository", "")
    if claims.get("repository_owner") not in config.ALLOWED_OWNERS:
        raise HTTPException(403, "repository owner not allowed")
    if config.ALLOWED_REPOS and repo not in config.ALLOWED_REPOS:
        raise HTTPException(403, "repository not allowed")
    return Caller(
        kind="oidc", repository=repo, workflow=claims.get("workflow", ""),
        gh_run_id=str(claims.get("run_id", "")), run_attempt=int(claims.get("run_attempt") or 1),
        ref=claims.get("ref", ""), actor=claims.get("actor", ""),
    )


def caller(request: Request) -> Caller:
    """FastAPI dependency for every runner endpoint."""
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        raise HTTPException(401, "bearer token required")
    token = header[7:].strip()
    # A JWT has two dots; the legacy token is an opaque string.
    if token.count(".") == 2:
        return verify_oidc(token)
    if config.LEGACY_TOKEN and hmac.compare_digest(token, config.LEGACY_TOKEN):
        repo = request.headers.get("x-scrape-repo", "unknown/unknown")
        return Caller(kind="token", repository=repo, gh_run_id=request.headers.get("x-scrape-run-id", ""))
    raise HTTPException(401, "invalid token")
