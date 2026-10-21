"""Tender bundles: a zip of chosen tenders (their portal pages, documents and the hub's record), built from object storage and shared
behind a password at /bundle/<slug>.zip.

- A bundle is a row in `bundles`: which tenders, what to include, a scrypt password hash. The dashboard (/bundle) and scripts/bundlectl.py
  create them; scripts/bundler.py (its own service) builds the zip and uploads it to R2 under bundles/<slug>.zip.
- A tender is stored either as one tar per tender (details.bundle_key, since 2026-10-20: pages.json.gz plus docs/NN_name, each with its
  byte range) or as separate objects (details.pages_key = .json.gz, documents[].key). GePNIC projects live on B2, eprocure on R2.
- B2 reads go through a Cloudflare Worker relay when B2_RELAY_URL is set (B2 to Cloudflare transfer is free under the Bandwidth
  Alliance). The hub presigns each GET, so the Worker holds no keys and can only fetch what the hub signed.
- The public link only ever hands out a short-lived presigned R2 link to that one zip; the password opens nothing else.
"""
import csv
import gzip
import hashlib
import hmac
import html
import io
import json
import os
import re
import secrets
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from . import config

SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}$")
TENDER_RE = re.compile(r"(?:\b([a-z0-9][a-z0-9-]{2,63}):)?\b(\d{4}_[A-Za-z0-9&.-]+_\d+_\d+)\b")
MAX_TENDERS = int(os.environ.get("BUNDLE_MAX_TENDERS", "5000"))
OBJECT_PREFIX = "bundles/"
DOWNLOAD_LINK_S = int(os.environ.get("BUNDLE_LINK_SECONDS", "3600"))
RELAY = os.environ.get("B2_RELAY_URL", "").rstrip("/")
THREADS = int(os.environ.get("BUNDLE_THREADS", "8"))
STORED = {".pdf", ".zip", ".xlsx", ".docx", ".jpg", ".jpeg", ".png", ".gz", ".rar", ".7z"}   # already compressed: stored, not deflated

INDEX_COLS = [("tender_id", "Tender ID"), ("portal", "Portal"), ("organisation", "Organisation"), ("title", "Title"), ("ref_no", "Reference"),
              ("published", "Published"), ("value", "Tender value"), ("category", "Category"), ("stage", "Stage"), ("bids", "Bids"),
              ("winner", "Winner / L1"), ("awarded", "Awarded value"), ("location", "Location"), ("docs", "Documents in bundle"),
              ("docs_missing", "Documents not available")]


# ---------------------------------------------------------------------------- passwords

def hash_password(pw: str) -> str:
    salt = secrets.token_bytes(16)
    return "scrypt$%s$%s" % (salt.hex(), hashlib.scrypt(pw.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32).hex())


def check_password(pw: str, record: str) -> bool:
    try:
        scheme, salt, digest = (record or "").split("$")
        got = hashlib.scrypt(pw.encode(), salt=bytes.fromhex(salt), n=2 ** 14, r=8, p=1, dklen=32)
        return scheme == "scrypt" and hmac.compare_digest(got.hex(), digest)
    except ValueError:
        return False


def new_password() -> str:
    """Four groups of four from an alphabet without look-alikes: easy to read out, ~80 bits."""
    abc = "abcdefghjkmnpqrstuvwxyz23456789"
    return "-".join("".join(secrets.choice(abc) for _ in range(4)) for _ in range(4))


# ---------------------------------------------------------------------------- choosing tenders

def parse_tenders(text) -> list:
    """Tender ids out of pasted text (lines, commas, a spreadsheet column, URLs): '2024_PWD_123_1' or 'gepnic-mahatenders:2024_PWD_123_1'
    to pin the portal. Order kept, duplicates dropped."""
    if isinstance(text, list):
        text = "\n".join(map(str, text))
    out, seen = [], set()
    for proj, tid in TENDER_RE.findall(text or ""):
        e = f"{proj}:{tid}" if proj else tid
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out


def tender_projects(c) -> list:
    return [r["name"] for r in c.execute("""SELECT name FROM projects WHERE name = 'eprocure-tenders'
                                            OR (name LIKE 'gepnic-%' AND name NOT LIKE '%-plan') ORDER BY name""").fetchall()]


def portal_of(project: str) -> str:
    return "eprocure" if project.startswith("eprocure") else project.removeprefix("gepnic-")


def resolve(c, entries, with_data=False):
    """{found: [{project, record_id, ...}], missing: [entry]}. An unpinned id found on several portals is taken from each."""
    projects = tender_projects(c)
    ids = sorted({e.split(":", 1)[-1] for e in entries})
    rows = c.execute(f"""SELECT project, record_id, {'data' if with_data else "data->'details'->'documents' AS documents, data->>'org_name' AS org_name, data->'details'->'fields'->>'tender_title' AS title, data->'list'->>'title' AS list_title"}
                        FROM records WHERE project = ANY(%s) AND record_id = ANY(%s)""", (projects, ids)).fetchall() if ids else []
    by_id = {}
    for r in rows:
        by_id.setdefault(r["record_id"], []).append(r)
    found, missing = [], []
    for e in entries:
        proj, _, tid = e.rpartition(":")
        hits = [r for r in by_id.get(tid, []) if not proj or r["project"] == proj]
        if not hits:
            missing.append(e)
        for r in sorted(hits, key=lambda r: r["project"]):
            found.append(r)
    return {"found": found, "missing": missing}


def preview(c, entries) -> dict:
    """What a bundle of these tenders would hold, for the create form."""
    res = resolve(c, entries)
    out = []
    for r in res["found"]:
        docs = r["documents"] or []
        stored = [d for d in docs if d.get("status") == "stored"]
        out.append({"project": r["project"], "portal": portal_of(r["project"]), "tender_id": r["record_id"], "organisation": r["org_name"],
                    "title": r["title"] or r["list_title"], "docs_stored": len(stored), "docs_total": len(docs),
                    "doc_bytes": sum(d.get("bytes") or 0 for d in stored)})
    return {"tenders": out, "missing": res["missing"], "doc_bytes": sum(t["doc_bytes"] for t in out)}


# ---------------------------------------------------------------------------- reading from object storage

def store_of(c, project: str) -> str:
    row = c.execute("SELECT settings->>'storage' AS s FROM projects WHERE name=%s", (project,)).fetchone()
    return "b2" if row and row["s"] == "b2" and config.B2_BUCKET else "r2"


class Storage:
    """Reads objects for the builder. B2 GETs are presigned here and fetched through the relay Worker when one is configured."""

    def __init__(self, clients):
        self.clients = clients          # store -> (boto3 client, bucket)
        self.via_relay = 0
        self.direct = 0

    def get(self, store: str, key: str) -> bytes:
        client, bucket = self.clients(store)
        if store == "b2" and RELAY:
            url = client.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=900)
            try:
                with urllib.request.urlopen(f"{RELAY}/?u={urllib.parse.quote(url, safe='')}", timeout=300) as r:
                    data = r.read()
                self.via_relay += 1
                return data
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    raise KeyError(key)
                # anything else (relay down, over its daily limit): read it directly rather than fail the bundle
        try:
            data = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception as e:
            if getattr(e, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                raise KeyError(key)
            raise
        self.direct += 1
        return data

    def get_any(self, store: str, key: str) -> bytes:
        """Some early objects of a project sit in the other store; try both."""
        try:
            return self.get(store, key)
        except KeyError:
            return self.get("r2" if store == "b2" else "b2", key)


def tender_files(storage, store: str, data: dict, include: dict):
    """(files, notes) for one tender: files = [(path inside the tender folder, bytes, source)], notes = [why something is missing]."""
    det = data.get("details") or {}
    files, notes, tar = [], [], None
    bundle_key = det.get("bundle_key") or (det.get("pages_key") if str(det.get("pages_key", "")).endswith(".tar") else None)

    def member(name):
        nonlocal tar
        if tar is None:
            tar = tarfile.open(fileobj=io.BytesIO(storage.get_any(store, bundle_key)), mode="r:")
        return tar.extractfile(name).read()

    if include.get("pages"):
        try:
            raw = member("pages.json.gz") if bundle_key else storage.get_any(store, det["pages_key"]) if det.get("pages_key") else None
            if raw is None:
                notes.append("portal pages: not stored for this tender")
            else:
                pages = json.loads(gzip.decompress(raw))
                for name in ("status", "details", "summary"):
                    if pages.get(name):
                        files.append((f"pages/{name}.html", pages[name].encode("utf-8"), "portal page"))
        except (KeyError, tarfile.TarError, OSError, ValueError) as e:
            notes.append(f"portal pages: could not be read ({type(e).__name__})")
    if include.get("documents"):
        for n, d in enumerate(det.get("documents") or [], 1):
            if d.get("status") != "stored":
                notes.append(f"document {d.get('name')}: {d.get('status')}{' (' + d['reason'] + ')' if d.get('reason') else ''}")
                continue
            name = (d.get("member") or "").removeprefix("docs/") or f"{n:02d}_{safe(d.get('name'))}"
            try:
                body = member(d["member"]) if d.get("member") and bundle_key else storage.get_any(store, d["key"])
            except (KeyError, tarfile.TarError, OSError) as e:
                notes.append(f"document {d.get('name')}: stored but could not be read ({type(e).__name__})")
                continue
            if d.get("sha256") and hashlib.sha256(body).hexdigest() != d["sha256"]:
                notes.append(f"warning, document {d.get('name')}: bytes differ from the checksum taken when it was scraped")
            files.append((f"documents/{name}", body, d.get("kind") or "document"))
    if include.get("record"):
        files.append(("record.json", json.dumps(data, indent=1, ensure_ascii=False, default=str).encode("utf-8"), "hub record"))
    return files, notes


def safe(s) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s or "")).strip("._") or "file"


def summary_row(project: str, rid: str, data: dict) -> dict:
    det = data.get("details") or {}
    f, lst = det.get("fields") or {}, data.get("list") or {}
    docs = det.get("documents") or []
    return {"tender_id": rid, "portal": portal_of(project), "organisation": data.get("org_name") or f.get("org_name") or "",
            "title": f.get("tender_title") or lst.get("title") or det.get("Tender Title") or "",
            "ref_no": f.get("tender_ref_no") or lst.get("reference_no") or det.get("Tender Ref No") or "",
            "published": f.get("published_date") or "", "value": f.get("tender_value") or "", "category": f.get("tender_category") or "",
            "stage": f.get("stage") or lst.get("stage") or "", "bids": f.get("bids_count") if f.get("bids_count") is not None else "",
            "winner": f.get("winner_bidder_name") or f.get("l1_bidder_name") or "",
            "awarded": f.get("awarded_value_sum") or f.get("contract_value_aoc_total") or f.get("awarded_value") or "",
            "location": f.get("location") or "", "view_link": lst.get("view_link") or "",
            "docs": sum(1 for d in docs if d.get("status") == "stored"), "docs_missing": sum(1 for d in docs if d.get("status") != "stored")}


# ---------------------------------------------------------------------------- the zip

def index_html(b: dict, rows: list, missing: list) -> str:
    e = lambda v: html.escape(str(v if v is not None else ""))
    head = "".join(f"<th>{e(label)}</th>" for _, label in INDEX_COLS)
    body = []
    for r in rows:
        cells = []
        for k, _ in INDEX_COLS:
            v = r.get(k)
            if k == "tender_id":
                v = f'<a href="{e(r["folder"])}/">{e(v)}</a>'
                v += "".join(f' <a class="p" href="{e(r["folder"])}/pages/{p}.html">{p}</a>' for p in ("status", "details", "summary") if p in r["pages"])
            elif k == "docs":
                v = f'<a href="{e(r["folder"])}/documents/">{e(v)}</a>' if v else "0"
            else:
                v = e(v)
            cells.append(f"<td>{v}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    miss = f"<p><b>Not found in the hub ({len(missing)}):</b> {e(', '.join(missing))}</p>" if missing else ""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(b.get('title') or b['slug'])}</title><style>
body{{font:13px/1.45 system-ui,sans-serif;margin:20px;color:#1d2630;background:#fff}}h1{{font-size:18px;margin:0 0 4px}}
.dim{{color:#66727f}}table{{border-collapse:collapse;margin-top:14px;font-size:12px}}th,td{{border:1px solid #d3dae2;padding:4px 6px;text-align:left;vertical-align:top}}
th{{background:#f3f5f8;position:sticky;top:0}}a.p{{font-size:11px;color:#66727f}}
@media (prefers-color-scheme:dark){{body{{background:#0b0f14;color:#d5dde6}}th{{background:#16202b}}th,td{{border-color:#26323f}}a{{color:#6cb6ff}}}}
</style></head><body><h1>{e(b.get('title') or b['slug'])}</h1>
<div class="dim">{len(rows)} tenders &middot; built {e(time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime()))} from scrape.s1d.me &middot; one folder per tender with the
portal's own pages (pages/), the documents the portal served (documents/) and everything the hub parsed (record.json)</div>
{('<p>' + e(b.get('public_note')) + '</p>') if b.get('public_note') else ''}{miss}
<table><tr>{head}</tr>{''.join(body)}</table></body></html>"""


def readme(b: dict, rows: list, missing: list, notes: dict) -> str:
    lines = [b.get("title") or b["slug"], "=" * len(b.get("title") or b["slug"]), ""]
    if b.get("public_note"):
        lines += [b["public_note"], ""]
    lines += [f"Built {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} from the scrape hub (scrape.s1d.me).",
              f"{len(rows)} tenders. Open index.html for the overview, or tenders.csv in a spreadsheet.", "",
              "Each folder under tenders/ is one tender:",
              "  pages/status.html, details.html, summary.html   the portal's pages exactly as received when it was scraped",
              "  documents/                                       the files the portal served (evaluation summaries, BOQ charts, ...)",
              "  record.json                                      everything the hub stored and parsed for the tender",
              "", "manifest.json lists every file with its SHA-256 and where it came from.", ""]
    if missing:
        lines += ["Requested but not found in the hub:", *("  " + m for m in missing), ""]
    gaps = {k: v for k, v in notes.items() if v}
    if gaps:
        lines += ["What is missing per tender (the portal refused it, or it was not collected):"]
        for tid, ns in gaps.items():
            lines += [f"  {tid}", *("    - " + n for n in ns)]
    return "\n".join(lines) + "\n"


def build(c, b: dict, storage, out_path: str, progress=lambda **kw: None, cancelled=lambda: False) -> dict:
    """Write bundle `b` (a bundles row) as a zip at out_path. Returns the summary stored in bundles.progress."""
    include = b.get("include") or {}
    entries = b.get("tenders") or []
    res = resolve(c, entries, with_data=True)
    top = b["slug"]
    rows, notes, manifest = [], {}, []
    folders = {}
    total, done, bytes_in = len(res["found"]), 0, 0
    progress(total=total, done=0, missing=res["missing"])
    last = 0.0
    stores = {p: store_of(c, p) for p in {r["project"] for r in res["found"]}}
    pending, todo = deque(), iter(res["found"])

    def submit(ex):
        r = next(todo, None)
        if r is not None:
            pending.append((r, ex.submit(tender_files, storage, stores[r["project"]], r["data"], include)))

    # tenders are fetched THREADS at a time ahead of the writer; the zip itself is written in order, by this thread only
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True) as z, \
            ThreadPoolExecutor(THREADS) as ex:
        for _ in range(THREADS * 2):
            submit(ex)
        while pending:
            if cancelled():
                for _, f in pending:
                    f.cancel()
                raise InterruptedError("bundle was changed or deleted while building")
            r, fut = pending.popleft()
            files, ns = fut.result()
            row = summary_row(r["project"], r["record_id"], r["data"])
            r["data"] = None                                      # free it as we go
            submit(ex)
            rid, proj = r["record_id"], r["project"]
            folder = f"tenders/{safe(rid)}"
            if folder in folders.values():                       # the same id on two portals
                folder = f"tenders/{safe(rid)}__{safe(portal_of(proj))}"
            folders[(proj, rid)] = folder
            for path, body, source in files:
                name = f"{top}/{folder}/{path}"
                ext = os.path.splitext(path)[1].lower()
                z.writestr(zipfile.ZipInfo(name, time.gmtime()[:6]), body,
                           compress_type=zipfile.ZIP_STORED if ext in STORED else zipfile.ZIP_DEFLATED)
                manifest.append({"path": f"{folder}/{path}", "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                                 "source": source, "project": proj, "tender_id": rid})
                bytes_in += len(body)
            notes[folder.removeprefix("tenders/")] = ns
            row.update(folder=folder, pages={p.split("/")[1][:-5] for p, _, _ in files if p.startswith("pages/")},
                       docs=sum(1 for p, _, _ in files if p.startswith("documents/")))
            row["docs_missing"] = sum(1 for n in ns if n.startswith("document "))
            rows.append(row)
            done += 1
            if time.time() - last > 2:
                progress(total=total, done=done, bytes=bytes_in, missing=res["missing"])
                last = time.time()
        z.writestr(f"{top}/index.html", index_html(b, rows, res["missing"]))
        z.writestr(f"{top}/README.txt", readme(b, rows, res["missing"], notes))
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([k for k, _ in INDEX_COLS] + ["view_link", "folder"])
        for r in rows:
            w.writerow([r.get(k, "") for k, _ in INDEX_COLS] + [r["view_link"], r["folder"]])
        z.writestr(f"{top}/tenders.csv", "\ufeff" + buf.getvalue())          # BOM so Excel reads it as UTF-8
        z.writestr(f"{top}/manifest.json", json.dumps({"bundle": b["slug"], "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                                       "tenders": [{"project": p, "tender_id": t, "folder": f} for (p, t), f in folders.items()],
                                                       "missing": res["missing"], "files": manifest}, indent=1))
    docs = sum(r["docs"] for r in rows)
    return {"total": total, "done": done, "bytes": bytes_in, "missing": res["missing"], "documents": docs,
            "documents_unavailable": sum(r["docs_missing"] for r in rows), "files": len(manifest)}


# ---------------------------------------------------------------------------- bundle rows (shared by the dashboard API and bundlectl)

COLUMNS = ("slug, title, note, public_note, tenders, include, public, status, progress, error, object_key, bytes, sha256, created_by, "
           "created_at, updated_at, built_at, downloads, last_download_at, password <> '' AS has_password")
INCLUDE_KEYS = ("pages", "documents", "record")


def clean_include(inc) -> dict:
    inc = inc or {}
    return {k: bool(inc.get(k, True)) for k in INCLUDE_KEYS}


def get(c, slug: str):
    return c.execute(f"SELECT {COLUMNS} FROM bundles WHERE slug=%s", (slug,)).fetchone()


def list_all(c):
    return c.execute(f"SELECT {COLUMNS}, jsonb_array_length(tenders) AS n_tenders FROM bundles ORDER BY created_at DESC").fetchall()


def create(c, slug: str, tenders, title="", note="", public_note="", include=None, password="", public=True, created_by=""):
    """Queue a new bundle. tenders: pasted text or a list. Raises ValueError with a message meant for the person."""
    slug = (slug or "").strip().lower().removesuffix(".zip")
    if not SLUG_RE.match(slug):
        raise ValueError("name: 2 to 63 characters, lowercase letters, digits and dashes, starting with a letter or digit")
    entries = parse_tenders(tenders)
    if not entries:
        raise ValueError("no tender ids found (they look like 2024_PWD_123456_1)")
    if len(entries) > MAX_TENDERS:
        raise ValueError(f"{len(entries)} tenders; at most {MAX_TENDERS} per bundle")
    if slug in ("preview",):
        raise ValueError(f"{slug} is reserved, pick another name")
    if get(c, slug):
        raise ValueError(f"a bundle named {slug} already exists")
    c.execute("""INSERT INTO bundles (slug, title, note, public_note, tenders, include, password, public, created_by)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
              (slug, title.strip()[:200], note[:4000], public_note.strip()[:2000], json.dumps(entries), json.dumps(clean_include(include)),
               hash_password(password) if password else "", bool(public), created_by[:40]))
    return get(c, slug)


def update(c, slug: str, title=None, note=None, public_note=None, tenders=None, include=None, public=None):
    """Change a bundle. New tenders or contents queue a rebuild (the old zip stays downloadable until the new one replaces it)."""
    b = get(c, slug)
    if not b:
        raise KeyError(slug)
    sets, args, rebuild = [], [], False
    if title is not None:
        sets.append("title=%s"); args.append(title.strip()[:200])
    if note is not None:
        sets.append("note=%s"); args.append(note[:4000])
    if public_note is not None:
        sets.append("public_note=%s"); args.append(public_note.strip()[:2000])
        rebuild = rebuild or public_note.strip() != b["public_note"]      # it is printed in the zip's README and index
    if public is not None:
        sets.append("public=%s"); args.append(bool(public))
    if tenders is not None:
        entries = parse_tenders(tenders)
        if not entries:
            raise ValueError("no tender ids found")
        if len(entries) > MAX_TENDERS:
            raise ValueError(f"{len(entries)} tenders; at most {MAX_TENDERS} per bundle")
        if entries != b["tenders"]:
            sets.append("tenders=%s"); args.append(json.dumps(entries)); rebuild = True
    if include is not None and clean_include(include) != clean_include(b["include"]):
        sets.append("include=%s"); args.append(json.dumps(clean_include(include))); rebuild = True
    if rebuild:
        sets += ["status='queued'", "error=''"]
    if sets:
        c.execute(f"UPDATE bundles SET {', '.join(sets)}, updated_at=now() WHERE slug=%s", (*args, slug))
    return get(c, slug)


def queue_rebuild(c, slug: str):
    c.execute("UPDATE bundles SET status='queued', error='', updated_at=now() WHERE slug=%s", (slug,))


def set_password(c, slug: str, password: str):
    """'' closes the public link."""
    c.execute("UPDATE bundles SET password=%s, updated_at=now() WHERE slug=%s", (hash_password(password) if password else "", slug))


def password_record(c, slug: str) -> str:
    row = c.execute("SELECT password FROM bundles WHERE slug=%s", (slug,)).fetchone()
    return row["password"] if row else ""


def locked_out(c, slug: str, ip: str) -> bool:
    """8 wrong passwords from one address in 15 minutes, or 60 from anywhere in an hour, close the bundle's door for a while."""
    r = c.execute("""SELECT count(*) FILTER (WHERE ip=%s AND at > now() - interval '15 minutes') AS mine, count(*) AS all_
                     FROM bundle_attempts WHERE slug=%s AND NOT ok AND at > now() - interval '1 hour'""", (ip, slug)).fetchone()
    return r["mine"] >= 8 or r["all_"] >= 60


def note_attempt(c, slug: str, ip: str, ok: bool):
    c.execute("INSERT INTO bundle_attempts (slug, ip, ok) VALUES (%s, %s, %s)", (slug, ip, ok))
    c.execute("DELETE FROM bundle_attempts WHERE at < now() - interval '30 days'")
    if ok:
        c.execute("UPDATE bundles SET downloads = downloads + 1, last_download_at = now() WHERE slug=%s", (slug,))


def download_url(client, bucket: str, b, seconds: int = DOWNLOAD_LINK_S) -> str:
    return client.generate_presigned_url("get_object", ExpiresIn=seconds, Params={
        "Bucket": bucket, "Key": b["object_key"], "ResponseContentType": "application/zip",
        "ResponseContentDisposition": f'attachment; filename="{b["slug"]}.zip"'})
