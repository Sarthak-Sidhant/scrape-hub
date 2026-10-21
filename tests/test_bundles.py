import gzip
import io
import json
import sys
import os
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scrape_hub import bundles, db  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts"))

PAGES = gzip.compress(json.dumps({"status": "<html>status</html>", "details": "<html>details</html>", "summary": "<html>summary</html>"}).encode())
PDF = b"%PDF-1.4 tender doc"


def tar_of(members):
    import tarfile
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as t:
        for name, data in members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


OBJECTS = {
    ("b2", "gepnic-x/tenders/2024/PWD/2024_PWD_1_1__t.tar"): tar_of([("pages.json.gz", PAGES), ("docs/01_tech.pdf", PDF)]),
    ("r2", "eprocure-tenders/pages/2023/9/2023_NTA_2_1.json.gz"): PAGES,
    ("r2", "eprocure-tenders/docs/2023/9/2023_NTA_2_1/fin.pdf"): PDF,
}


class FakeStorage(bundles.Storage):
    def __init__(self):
        super().__init__(None)
        self.calls = []

    def get(self, store, key):
        self.calls.append((store, key))
        if (store, key) not in OBJECTS:
            raise KeyError(key)
        return OBJECTS[(store, key)]


def seed():
    import hashlib
    sha = hashlib.sha256(PDF).hexdigest()
    with db.conn() as c:
        c.execute("INSERT INTO projects (name, settings) VALUES ('gepnic-x', '{\"storage\": \"b2\"}'), "
                  "('eprocure-tenders', '{}'), ('gepnic-x-plan', '{}')")
        key = "gepnic-x/tenders/2024/PWD/2024_PWD_1_1__t.tar"
        rec1 = {"org_name": "PWD", "list": {"title": "Road", "view_link": "https://x"}, "details": {
            "bundle_key": key, "pages_key": key, "fields": {"tender_title": "Road works", "tender_value": "100", "bids_count": 3},
            "documents": [{"name": "tech.pdf", "kind": "evaluation_summary", "status": "stored", "key": key, "member": "docs/01_tech.pdf",
                           "bytes": len(PDF), "sha256": sha},
                          {"name": "boq.xlsx", "kind": "boq", "status": "refused", "reason": "session expired"}]}}
        rec2 = {"org_name": "NTA", "details": {"pages_key": "eprocure-tenders/pages/2023/9/2023_NTA_2_1.json.gz", "documents": [
            {"name": "fin.pdf", "status": "stored", "key": "eprocure-tenders/docs/2023/9/2023_NTA_2_1/fin.pdf", "bytes": len(PDF), "sha256": "bad"}]}}
        c.execute("INSERT INTO records (project, record_id, data) VALUES ('gepnic-x', '2024_PWD_1_1', %s), ('eprocure-tenders', '2023_NTA_2_1', %s), "
                  "('gepnic-x-plan', '2022_PLAN_3_1', '{}')", (json.dumps(rec1), json.dumps(rec2)))


def test_parse_tenders():
    text = "see 2024_PWD_1_1, gepnic-x:2023_NTA_2_1\nhttps://p/x?id=2024_PWD_1_1 and 2019_M&M_5_2 junk 2024_PWD"
    assert bundles.parse_tenders(text) == ["2024_PWD_1_1", "gepnic-x:2023_NTA_2_1", "2019_M&M_5_2"]


def test_passwords():
    rec = bundles.hash_password("open sesame")
    assert bundles.check_password("open sesame", rec) and not bundles.check_password("open", rec) and not bundles.check_password("x", "")
    assert len(bundles.new_password()) == 19


def test_preview_and_create_need_login(client, clean):
    client.cookies.clear()
    assert client.post("/api/bundles/preview", json={"tenders": "2024_PWD_1_1"}).status_code == 401
    assert client.get("/api/bundles").status_code == 401
    assert client.get("/bundle", follow_redirects=False).status_code == 303


def test_preview_finds_tenders_across_projects(login):
    seed()
    d = login.post("/api/bundles/preview", json={"tenders": "2024_PWD_1_1 2023_NTA_2_1 2022_PLAN_3_1 gepnic-x:2023_NTA_2_1"}).json()
    assert {t["tender_id"]: t["project"] for t in d["tenders"]} == {"2024_PWD_1_1": "gepnic-x", "2023_NTA_2_1": "eprocure-tenders"}
    assert d["missing"] == ["2022_PLAN_3_1", "gepnic-x:2023_NTA_2_1"]          # plan projects are not tenders; a pin is honoured
    assert d["tenders"][0]["docs_stored"] == 1 and d["tenders"][0]["docs_total"] == 2


def test_build_zip(login, tmp_path):
    seed()
    b = login.post("/api/bundles", json={"slug": "Case-1", "title": "Case one", "tenders": "2024_PWD_1_1\n2023_NTA_2_1\n2099_NOPE_1_1",
                                         "public_note": "for the story"}).json()
    assert b["slug"] == "case-1" and b["status"] == "queued" and not b["has_password"]
    assert login.post("/api/bundles", json={"slug": "case-1", "tenders": "2024_PWD_1_1"}).status_code == 422
    assert login.post("/api/bundles", json={"slug": "x y", "tenders": "2024_PWD_1_1"}).status_code == 422
    st = FakeStorage()
    with db.conn() as c:
        summary = bundles.build(c, bundles.get(c, "case-1"), st, str(tmp_path / "z.zip"))
    assert summary["done"] == 2 and summary["missing"] == ["2099_NOPE_1_1"] and summary["documents"] == 2
    assert summary["documents_unavailable"] == 1
    # the tar was read once for both its pages and its document
    assert sum(1 for s, k in st.calls if k.endswith(".tar")) == 1
    z = zipfile.ZipFile(tmp_path / "z.zip")
    names = set(z.namelist())
    for n in ("case-1/index.html", "case-1/README.txt", "case-1/tenders.csv", "case-1/manifest.json",
              "case-1/tenders/2024_PWD_1_1/pages/details.html", "case-1/tenders/2024_PWD_1_1/documents/01_tech.pdf",
              "case-1/tenders/2024_PWD_1_1/record.json", "case-1/tenders/2023_NTA_2_1/documents/01_fin.pdf"):
        assert n in names, n
    assert z.read("case-1/tenders/2024_PWD_1_1/documents/01_tech.pdf") == PDF
    readme = z.read("case-1/README.txt").decode()
    assert "2099_NOPE_1_1" in readme and "boq.xlsx: refused (session expired)" in readme and "for the story" in readme
    assert "fin.pdf: bytes differ" in readme
    assert "Road works" in z.read("case-1/index.html").decode()
    assert z.getinfo("case-1/tenders/2024_PWD_1_1/documents/01_tech.pdf").compress_type == zipfile.ZIP_STORED


def test_build_without_documents(login, tmp_path):
    seed()
    login.post("/api/bundles", json={"slug": "pages-only", "tenders": "2024_PWD_1_1", "include": {"documents": False, "record": False}})
    with db.conn() as c:
        bundles.build(c, bundles.get(c, "pages-only"), FakeStorage(), str(tmp_path / "z.zip"))
    names = zipfile.ZipFile(tmp_path / "z.zip").namelist()
    assert not any("/documents/" in n or n.endswith("record.json") for n in names)
    assert "pages-only/tenders/2024_PWD_1_1/pages/status.html" in names


def ready(slug):
    with db.conn() as c:
        c.execute("UPDATE bundles SET status='ready', object_key=%s, bytes=1234567, built_at=now(), progress='{\"done\": 2}' WHERE slug=%s",
                  (f"bundles/{slug}.zip", slug))


def test_public_download(login):
    seed()
    login.post("/api/bundles", json={"slug": "story", "tenders": "2024_PWD_1_1", "password": "letmein-123"})
    login.post("/api/bundles", json={"slug": "other", "tenders": "2024_PWD_1_1", "password": "different-pw"})
    login.cookies.clear()
    r = login.get("/bundle/story.zip")
    assert r.status_code == 404 and "no-store" in r.headers["cache-control"]          # not built yet
    ready("story"), ready("other")
    r = login.get("/bundle/story.zip")
    assert r.status_code == 200 and "Password" in r.text and "1.2 MB" in r.text and r.headers["cache-control"].startswith("no-store")
    form = {"content-type": "application/x-www-form-urlencoded"}
    assert login.post("/bundle/story.zip", content="password=wrong", headers=form).status_code == 401
    assert login.post("/bundle/other.zip", content="password=letmein-123", headers=form).status_code == 401   # one password, one zip
    r = login.post("/bundle/story.zip", content="password=letmein-123", headers=form, follow_redirects=False)
    assert r.status_code == 303 and "bundles/story.zip" in r.headers["location"] and "no-store" in r.headers["cache-control"]
    assert "attachment" in r.headers["location"]
    with db.conn() as c:
        assert bundles.get(c, "story")["downloads"] == 1
    assert login.get("/bundle/nope.zip").status_code == 404
    assert login.get("/bundle/../etc.zip").status_code == 404
    # switched off, or no password: closed
    login.post("/login", content="password=hunter2hunter2", headers=form)
    login.post("/api/bundles/story", json={"public": False})
    assert login.get("/bundle/story.zip").status_code == 404
    login.post("/api/bundles/story", json={"public": True})
    login.post("/api/bundles/story/password", json={"password": ""})
    assert login.get("/bundle/story.zip").status_code == 404
    r = login.post("/api/bundles/story/password", json={"generate": True}).json()
    assert r["has_password"] and len(r["password"]) == 19
    login.cookies.clear()
    assert login.post("/bundle/story.zip", content=f"password={r['password']}", headers=form, follow_redirects=False).status_code == 303


def test_lockout(login):
    seed()
    login.post("/api/bundles", json={"slug": "locked", "tenders": "2024_PWD_1_1", "password": "right-one"})
    ready("locked")
    form = {"content-type": "application/x-www-form-urlencoded"}
    for _ in range(8):
        assert login.post("/bundle/locked.zip", content="password=nope", headers=form).status_code == 401
    assert login.post("/bundle/locked.zip", content="password=right-one", headers=form).status_code == 429


def test_edit_queues_rebuild_and_delete(login):
    seed()
    login.post("/api/bundles", json={"slug": "ed", "tenders": "2024_PWD_1_1"})
    ready("ed")
    b = login.post("/api/bundles/ed", json={"title": "new title"}).json()
    assert b["status"] == "ready" and b["title"] == "new title"                   # a title alone needs no rebuild
    b = login.post("/api/bundles/ed", json={"tenders": "2024_PWD_1_1 2023_NTA_2_1"}).json()
    assert b["status"] == "queued" and b["tenders"] == ["2024_PWD_1_1", "2023_NTA_2_1"] and b["object_key"]
    assert login.post("/api/bundles/ed", json={"tenders": "nothing here"}).status_code == 422
    assert login.post("/api/bundles/ed/rebuild").json()["ok"]
    with db.conn() as c:
        c.execute("UPDATE bundles SET object_key=NULL WHERE slug='ed'")        # nothing in storage to delete in the test
    assert login.delete("/api/bundles/ed").json()["ok"]
    assert login.get("/api/bundles/ed").status_code == 404
    assert login.get("/bundle").status_code == 200


def test_bundler_build_one(login, tmp_path, monkeypatch):
    import bundler
    seed()
    login.post("/api/bundles", json={"slug": "svc", "tenders": "2024_PWD_1_1"})
    uploaded = {}

    class FakeS3:
        def upload_file(self, path, bucket, key, ExtraArgs=None):
            uploaded[key] = zipfile.ZipFile(path).namelist()
    monkeypatch.setattr(bundler.app, "s3_client", lambda store: (FakeS3(), "bkt"))
    monkeypatch.setattr(bundler, "WORKDIR", str(tmp_path))
    monkeypatch.setattr(bundler, "MIN_FREE", 0)
    with bundler.connect() as c:
        b = bundler.claim(c)
        assert b["slug"] == "svc" and bundler.claim(c) is None
        bundler.build_one(c, b, FakeStorage())
        row = bundles.get(c, "svc")
    assert row["status"] == "ready" and row["object_key"] == "bundles/svc.zip" and row["bytes"] > 0 and row["progress"]["done"] == 1
    assert "svc/tenders/2024_PWD_1_1/documents/01_tech.pdf" in uploaded["bundles/svc.zip"]
    assert not list(tmp_path.iterdir())                                          # the local zip is removed after the upload
