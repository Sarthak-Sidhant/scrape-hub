#!/usr/bin/env python3
"""Tender bundles from the command line: the same operations as the /bundle page. Tender ids come from the arguments, --file, or stdin
(any text: ids are picked out of it). On the VPS run it through ~/bin/bundle, which runs it as the hub's user with the hub's environment.

  bundlectl.py list
  bundlectl.py preview 2024_PWD_1_1 2024_PWD_2_1          (or: ... preview --file ids.txt / ... preview < ids.txt)
  bundlectl.py create drdo-2024 --title "DRDO 2024" --generate-password [--public-note ...] [--no-pages|--no-documents|--no-record] < ids.txt
  bundlectl.py add drdo-2024 2024_DRDO_9_1             (append tenders; rebuilds)
  bundlectl.py remove drdo-2024 2024_DRDO_9_1          (drop tenders; rebuilds)
  bundlectl.py password drdo-2024 --generate | --set PW | --clear
  bundlectl.py show|wait|rebuild|delete drdo-2024
  bundlectl.py public drdo-2024 on|off
"""
import argparse
import json
import os
import sys
import time

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scrape_hub import bundles, config  # noqa: E402

BASE = os.environ.get("BUNDLE_PUBLIC_BASE", "https://scrape.s1d.me/bundle/")


def ids_from(args):
    text = " ".join(args.ids or [])
    if getattr(args, "file", None):
        text += "\n" + open(args.file).read()
    if not text.strip() and not sys.stdin.isatty():
        text = sys.stdin.read()
    return bundles.parse_tenders(text)


def size(b):
    return "" if b is None else f"{b / 1e6:,.1f} MB" if b >= 1e6 else f"{b / 1e3:,.0f} KB"


def show(b):
    p = b["progress"] or {}
    print(f"{b['slug']}.zip  [{b['status']}]  {b['title']}")
    print(f"  link      {BASE}{b['slug']}.zip  (public {'on' if b['public'] else 'off'}, password {'set' if b['has_password'] else 'NOT set: closed'})")
    print(f"  tenders   {len(b['tenders'])} asked, {p.get('done', '?')} found" + (f", {len(p['missing'])} not in the hub" if p.get("missing") else ""))
    if p.get("documents") is not None:
        print(f"  documents {p['documents']} in the zip, {p.get('documents_unavailable', 0)} not available (refused or not collected)")
    print(f"  zip       {size(b['bytes'])}  built {b['built_at'] or '-'}  downloads {b['downloads']}")
    print(f"  include   {', '.join(k for k, v in (b['include'] or {}).items() if v)}")
    if b["error"]:
        print(f"  error     {b['error']}")
    if p.get("missing"):
        print("  missing   " + " ".join(p["missing"][:40]) + (" ..." if len(p["missing"]) > 40 else ""))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    for name in ("show", "wait", "rebuild", "delete"):
        sub.add_parser(name).add_argument("slug")
    p = sub.add_parser("preview")
    p.add_argument("ids", nargs="*")
    p.add_argument("--file")
    p.add_argument("--json", action="store_true")
    p = sub.add_parser("create")
    p.add_argument("slug")
    p.add_argument("ids", nargs="*")
    p.add_argument("--file")
    p.add_argument("--title", default="")
    p.add_argument("--note", default="")
    p.add_argument("--public-note", default="")
    p.add_argument("--password", default="")
    p.add_argument("--generate-password", action="store_true")
    for k in bundles.INCLUDE_KEYS:
        p.add_argument(f"--no-{k}", action="store_true")
    p.add_argument("--private", action="store_true", help="public link off")
    p.add_argument("--wait", action="store_true")
    for name in ("add", "remove"):
        p = sub.add_parser(name)
        p.add_argument("slug")
        p.add_argument("ids", nargs="*")
        p.add_argument("--file")
    p = sub.add_parser("password")
    p.add_argument("slug")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--generate", action="store_true")
    g.add_argument("--set")
    g.add_argument("--clear", action="store_true")
    p = sub.add_parser("public")
    p.add_argument("slug")
    p.add_argument("state", choices=["on", "off"])
    p = sub.add_parser("edit")
    p.add_argument("slug")
    p.add_argument("--title")
    p.add_argument("--note")
    p.add_argument("--public-note")
    args = ap.parse_args()

    with psycopg.connect(config.DATABASE_URL, autocommit=True, row_factory=dict_row) as c:
        need = lambda s: bundles.get(c, s) or sys.exit(f"no bundle named {s}")
        if args.cmd == "list":
            for b in bundles.list_all(c):
                p = b["progress"] or {}
                print(f"{b['slug']:<32} {b['status']:<9} {b['n_tenders']:>6} tenders {p.get('documents', ''):>6} docs {size(b['bytes']):>10} "
                      f"{'pw' if b['has_password'] else '--'} {'on ' if b['public'] else 'off'} {b['downloads']:>4} dl  {b['title']}")
        elif args.cmd == "preview":
            entries = ids_from(args)
            d = bundles.preview(c, entries)
            if args.json:
                print(json.dumps(d, default=str, indent=1))
                return
            for t in d["tenders"]:
                print(f"{t['tender_id']:<28} {t['portal']:<22} docs {t['docs_stored']:>3}/{t['docs_total']:<3} {size(t['doc_bytes']):>10}  {(t['title'] or '')[:70]}")
            print(f"{len(entries)} ids read, {len(d['tenders'])} found, {len(d['missing'])} missing, documents about {size(d['doc_bytes'])}")
            if d["missing"]:
                print("missing: " + " ".join(d["missing"]))
        elif args.cmd == "create":
            pw = bundles.new_password() if args.generate_password else args.password
            try:
                b = bundles.create(c, args.slug, ids_from(args), args.title, args.note, args.public_note,
                                   {k: not getattr(args, f"no_{k}") for k in bundles.INCLUDE_KEYS}, pw, not args.private, created_by="claude")
            except ValueError as e:
                sys.exit(str(e))
            print(f"queued {b['slug']}.zip with {len(b['tenders'])} tenders: {BASE}{b['slug']}.zip")
            if pw:
                print(f"password: {pw}")
            if args.wait:
                wait(c, b["slug"])
        elif args.cmd in ("add", "remove"):
            b = need(args.slug)
            ids = ids_from(args)
            new = b["tenders"] + [i for i in ids if i not in b["tenders"]] if args.cmd == "add" else [t for t in b["tenders"] if t not in ids]
            try:
                b = bundles.update(c, args.slug, tenders=new)
            except ValueError as e:
                sys.exit(str(e))
            print(f"{b['slug']}: {len(b['tenders'])} tenders, {b['status']}")
        elif args.cmd == "edit":
            need(args.slug)
            show(bundles.update(c, args.slug, title=args.title, note=args.note, public_note=args.public_note))
        elif args.cmd == "password":
            need(args.slug)
            pw = bundles.new_password() if args.generate else ("" if args.clear else args.set)
            if pw and len(pw) < 6:
                sys.exit("use at least 6 characters")
            bundles.set_password(c, args.slug, pw)
            print(f"password: {pw}" if pw else "password removed; the public link is closed")
        elif args.cmd == "public":
            need(args.slug)
            bundles.update(c, args.slug, public=args.state == "on")
            print(f"public link {args.state}")
        elif args.cmd == "show":
            show(need(args.slug))
        elif args.cmd == "wait":
            need(args.slug)
            wait(c, args.slug)
        elif args.cmd == "rebuild":
            need(args.slug)
            bundles.queue_rebuild(c, args.slug)
            print("queued")
        elif args.cmd == "delete":
            b = need(args.slug)
            if b["object_key"]:
                from scrape_hub import app
                client, bucket = app.s3_client("r2")
                client.delete_object(Bucket=bucket, Key=b["object_key"])
            c.execute("DELETE FROM bundles WHERE slug=%s", (args.slug,))
            c.execute("DELETE FROM bundle_attempts WHERE slug=%s", (args.slug,))
            print(f"deleted {args.slug}.zip")


def wait(c, slug):
    last = None
    while True:
        b = bundles.get(c, slug)
        if not b:
            sys.exit("bundle was deleted")
        p = b["progress"] or {}
        line = f"{b['status']} {p.get('done', 0)}/{p.get('total', '?')}"
        if line != last:
            print(line, flush=True)
            last = line
        if b["status"] in ("ready", "failed"):
            show(b)
            return
        time.sleep(2)


if __name__ == "__main__":
    main()
