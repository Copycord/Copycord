"""Scraper tokens as a text file: GET/PUT routes, the one-time migration from the table,
and the scrape task reading the file."""
import os
import stat

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth, scraper_tokens
from copycord.admin.security import CSRF_HEADER
from copycord.admin.tasks import scrape


@pytest.fixture()
def tc(tmp_path, monkeypatch):
    monkeypatch.setattr(app_module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    with TestClient(app_module.app) as client:
        client.headers.update({"origin": "http://testserver", CSRF_HEADER: "1",
                               "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
        yield client


def test_get_empty(tc):
    assert tc.get("/api/scraper/tokens").json() == {"ok": True, "tokens": []}


def test_put_trims_dedupes_and_saves(tc, tmp_path):
    resp = tc.put("/api/scraper/tokens", json={"tokens": [" a ", "", "b", "a", "   ", "c"]})
    assert resp.status_code == 200 and resp.json() == {"ok": True, "tokens": ["a", "b", "c"]}
    path = tmp_path / "scraper_tokens.txt"
    assert path.read_text() == "a\nb\nc\n"
    assert tc.get("/api/scraper/tokens").json() == {"ok": True, "tokens": ["a", "b", "c"]}
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_put_text_shape_and_clear(tc, tmp_path):
    resp = tc.put("/api/scraper/tokens", json={"text": "x\r\n\n y \nx\n"})
    assert resp.json() == {"ok": True, "tokens": ["x", "y"]}
    assert tc.put("/api/scraper/tokens", json={"tokens": []}).json() == {"ok": True, "tokens": []}
    assert (tmp_path / "scraper_tokens.txt").read_text() == ""


@pytest.mark.parametrize("body", [{}, {"tokens": "a"}, {"text": 5}, [1]])
def test_put_rejects_bad_bodies(tc, body):
    assert tc.put("/api/scraper/tokens", json=body).status_code == 400


def test_removed_routes(tc):
    assert tc.post("/api/scraper/tokens/add", data={"token_value": "x"}).status_code in (404, 405)
    assert tc.post("/api/scraper/tokens/i/update", data={"label": "x"}).status_code in (404, 405)
    assert tc.delete("/api/scraper/tokens/i").status_code in (404, 405)


class Db:
    def __init__(self, rows):
        self.rows = rows

    def list_scraper_tokens(self):
        return self.rows


def test_migration_writes_rows(tmp_path):
    db = Db([{"token_value": "t1"}, {"token_value": "t2"}, {"token_value": "t1"}])
    assert scraper_tokens.migrate_from_db(db, tmp_path) == 2
    assert scraper_tokens.read_tokens(tmp_path) == ["t1", "t2"]


def test_migration_keeps_existing_file(tmp_path):
    (tmp_path / "scraper_tokens.txt").write_text("mine\n")
    assert scraper_tokens.migrate_from_db(Db([{"token_value": "t1"}]), tmp_path) is None
    assert scraper_tokens.read_tokens(tmp_path) == ["mine"]
    # An empty file is still a file: tokens cleared on purpose are not brought back.
    (tmp_path / "scraper_tokens.txt").write_text("")
    assert scraper_tokens.migrate_from_db(Db([{"token_value": "t1"}]), tmp_path) is None
    assert scraper_tokens.read_tokens(tmp_path) == []


def test_migration_empty_table_makes_no_file(tmp_path):
    assert scraper_tokens.migrate_from_db(Db([]), tmp_path) is None
    assert not (tmp_path / "scraper_tokens.txt").exists()


def test_scrape_reads_the_file(tmp_path, monkeypatch):
    monkeypatch.setattr(scrape, "_wiring", scrape._Wiring(data_dir=tmp_path))
    assert scrape._tokens() == []
    scraper_tokens.write_tokens(tmp_path, ["z", "a", "z"])
    assert scrape._tokens() == ["z", "a"]
