"""Reverse proxy to a private Metabase (reachable only over a VPN from this host), served under /metabase/ behind the dashboard login.

The browser never sees Metabase's own login: the hub signs in server-side with the account in METABASE_USER / METABASE_PASSWORD
and adds that session to every upstream request. Metabase reads its base path from <base href> and <meta name="base-href">,
which are rewritten in the HTML so its relative asset and API URLs stay under /metabase/. <meta name="uri"> is the path as the
backend saw it and must stay untouched: Metabase compares it with the browser path to find its own mount point.
"""
import re
import threading

import requests
from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import RedirectResponse, Response, StreamingResponse

from . import config, dash_auth

PREFIX = "/metabase"
router = APIRouter()
_lock = threading.Lock()
_token = {"value": ""}
_http = requests.Session()

# Hop-by-hop and per-connection headers are never forwarded either way.
_SKIP_REQ = {"host", "cookie", "authorization", "content-length", "connection", "accept-encoding", "origin", "referer",
             "x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "x-real-ip", "cf-connecting-ip", "cf-ray", "cf-ipcountry",
             "cf-visitor", "cdn-loop"}
_SKIP_RESP = {"content-length", "content-encoding", "transfer-encoding", "connection", "set-cookie", "x-frame-options",
              "content-security-policy", "content-security-policy-report-only", "strict-transport-security"}


def _configured() -> bool:
    return bool(config.METABASE_URL and config.METABASE_USER and config.METABASE_PASSWORD)


def _login() -> str:
    r = _http.post(f"{config.METABASE_URL}/api/session", json={"username": config.METABASE_USER, "password": config.METABASE_PASSWORD}, timeout=20)
    r.raise_for_status()
    return r.json()["id"]


def _session(refresh: bool = False) -> str:
    with _lock:
        if refresh or not _token["value"]:
            _token["value"] = _login()
        return _token["value"]


def _upstream(request: Request, path: str, body: bytes, token: str):
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _SKIP_REQ}
    headers["X-Metabase-Session"] = token
    headers["Cookie"] = f"metabase.SESSION={token}"
    headers["Accept-Encoding"] = "identity"
    url = f"{config.METABASE_URL}/{path}"
    return _http.request(request.method, url, params=request.query_params.multi_items(), data=body, headers=headers,
                         stream=True, allow_redirects=False, timeout=(10, 300))


def _rewrite_html(html: str) -> str:
    html = html.replace('<base href="/"/>', f'<base href="{PREFIX}/"/>')
    html = html.replace('<meta name="base-href" content="/"/>', f'<meta name="base-href" content="{PREFIX}/"/>')
    return html


@router.get(PREFIX)
def bare():
    return RedirectResponse(PREFIX + "/", 307)


@router.api_route(PREFIX + "/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"])
async def proxy(request: Request, path: str):
    if not dash_auth.cookie_ok(request.cookies.get(dash_auth.COOKIE, "")):
        if request.method == "GET" and "text/html" in request.headers.get("accept", ""):
            return RedirectResponse("/login", 303)
        return Response("login required", 401)
    if not _configured():
        return Response("Metabase proxy is not configured", 503)
    # Cookie auth would let any other site fire state-changing requests through this proxy; only same-origin may.
    if request.method not in ("GET", "HEAD") and request.headers.get("sec-fetch-site", "same-origin") not in ("same-origin", "none"):
        return Response("cross-site request refused", 403)
    return await run_in_threadpool(_forward, request, path, await request.body())


def _forward(request: Request, path: str, body: bytes):
    try:
        resp = _upstream(request, path, body, _session())
        if resp.status_code == 401:  # the Metabase session expired or was revoked: sign in again once
            resp.close()
            resp = _upstream(request, path, body, _session(refresh=True))
    except requests.RequestException as e:
        return Response(f"Metabase is unreachable from the hub ({type(e).__name__}). The VPN may be down.", 502)
    headers = {k: v for k, v in resp.headers.items() if k.lower() not in _SKIP_RESP}
    loc = headers.get("Location") or headers.get("location")
    if loc:
        base = config.METABASE_URL
        loc = loc[len(base):] if loc.startswith(base) else loc
        for k in [k for k in headers if k.lower() == "location"]:
            headers.pop(k)
        headers["Location"] = PREFIX + loc if loc.startswith("/") else loc
    ctype = resp.headers.get("content-type", "")
    if ctype.startswith("text/html"):
        html = _rewrite_html(resp.content.decode(resp.encoding or "utf-8", "replace"))
        resp.close()
        return Response(html, resp.status_code, headers, media_type=ctype)
    if ctype.startswith("text/css"):
        css = re.sub(r"url\((['\"]?)/(?!/)", lambda m: f"url({m.group(1)}{PREFIX}/", resp.content.decode(resp.encoding or "utf-8", "replace"))
        resp.close()
        return Response(css, resp.status_code, headers, media_type=ctype)
    return StreamingResponse(resp.iter_content(65536), resp.status_code, headers, media_type=ctype or None, background=None)


@router.get("/app/fonts/{path:path}")
async def fonts(request: Request, path: str):
    """Metabase builds its @font-face URLs from the site root, ignoring the /metabase/ base path."""
    if not dash_auth.cookie_ok(request.cookies.get(dash_auth.COOKIE, "")):
        return Response("login required", 401)
    return await run_in_threadpool(_forward, request, "app/fonts/" + path, b"")
