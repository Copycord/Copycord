"""
Security tests for the admin app: path traversal in file routes and
websocket authentication.
"""
from unittest.mock import AsyncMock

import pytest

from copycord.admin.app import app
from copycord.admin.security import safe_child

from httpx import AsyncClient, ASGITransport


TRAVERSAL_NAMES = [
    "../x",
    "..\\x",
    "C:x",
    "C:\\x",
    "/etc/passwd",
    "\\\\server\\share",
    "..",
    ".",
    "",
    "a/../../x",
    "..%2Fx",
    "%2e%2e%2fx",
]


@pytest.fixture()
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture(autouse=True)
def _mock_ws(monkeypatch):
    import copycord.admin.app as app_mod
    mock = AsyncMock(return_value={"ok": True, "running": False, "status": "stopped"})
    monkeypatch.setattr(app_mod, "_ws_cmd", mock)


@pytest.fixture()
def dirs(tmp_path, monkeypatch):
    """A data dir with a secret outside the backup and scrape folders."""
    import copycord.admin.app as app_mod
    data = tmp_path / "data"
    backups = data / "backups"
    scrapes = data / "scrapes" / "123"
    backups.mkdir(parents=True)
    scrapes.mkdir(parents=True)
    (data / "secret.key").write_text("secret")
    (tmp_path / "x").write_text("outside")
    (backups / "copycord-backup-1.tar.gz").write_bytes(b"gz")
    (scrapes / "scrape_123_1.json").write_text("{}")
    (data / "scrapes" / "top.json").write_text("{}")
    monkeypatch.setattr(app_mod, "BACKUP_DIR", backups)
    monkeypatch.setattr(app_mod, "DATA_DIR", data)
    return data


class TestSafeChild:

    @pytest.mark.parametrize("name", TRAVERSAL_NAMES)
    def test_rejects(self, tmp_path, name):
        assert safe_child(tmp_path, name) is None
        assert safe_child(tmp_path, name, max_depth=2) is None

    def test_accepts_plain_name(self, tmp_path):
        assert safe_child(tmp_path, "a.tar.gz") == (tmp_path / "a.tar.gz").resolve()

    def test_subdir_only_when_allowed(self, tmp_path):
        assert safe_child(tmp_path, "123/a.json") is None
        assert safe_child(tmp_path, "123/a.json", max_depth=2) == (
            tmp_path / "123" / "a.json"
        ).resolve()
        assert safe_child(tmp_path, "1/2/a.json", max_depth=2) is None

    def test_symlink_escape(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        (tmp_path / "outside").write_text("x")
        try:
            (base / "link").symlink_to(tmp_path / "outside")
        except OSError:
            pytest.skip("symlinks not available")
        assert safe_child(base, "link") is None


class TestBackupTraversal:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", TRAVERSAL_NAMES + ["..%5Csecret.key", "%2e%2e%5csecret.key"])
    async def test_download_rejected(self, client, dirs, name):
        resp = await client.get(f"/api/backup/download/{name}")
        assert resp.content not in (b"secret", b"outside")
        assert resp.headers.get("content-type") != "application/gzip"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", ["..%5Csecret.key", "C:secret.key", "C%3A%5Csecret.key"])
    async def test_download_reaches_handler_and_400(self, client, dirs, name):
        resp = await client.get(f"/api/backup/download/{name}")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", TRAVERSAL_NAMES[:-3] + ["../secret.key", "..\\secret.key"])
    async def test_delete_rejected(self, client, dirs, name):
        resp = await client.post("/api/backup/delete", data={"name": name})
        assert resp.status_code in (400, 422)
        assert (dirs / "secret.key").exists()
        assert (dirs.parent / "x").exists()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", ["../secret.key", "..\\secret.key", "/etc/passwd"])
    async def test_restore_existing_rejected(self, client, dirs, name):
        resp = await client.post(
            "/api/backup/restore", data={"source": "existing", "name": name}
        )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_download_normal(self, client, dirs):
        resp = await client.get("/api/backup/download/copycord-backup-1.tar.gz")
        assert resp.status_code == 200
        assert resp.content == b"gz"

    @pytest.mark.asyncio
    async def test_delete_normal(self, client, dirs):
        resp = await client.post(
            "/api/backup/delete", data={"name": "copycord-backup-1.tar.gz"}
        )
        assert resp.status_code == 200
        assert not (dirs / "backups" / "copycord-backup-1.tar.gz").exists()


class TestScrapeTraversal:

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path",
        [
            "..%2Fsecret.key",
            "..%5Csecret.key",
            "123%2F..%2F..%2Fsecret.key",
            "C:secret.key",
            "C%3A%5Csecret.key",
            "%2Fetc%2Fpasswd",
            "1/2/3.json",
        ],
    )
    async def test_download_and_delete_rejected(self, client, dirs, path):
        resp = await client.get(f"/api/scraper/scrapes/{path}")
        assert resp.status_code in (400, 404)
        assert resp.content != b"secret"
        resp = await client.delete(f"/api/scraper/scrapes/{path}")
        assert resp.status_code in (400, 404)
        assert (dirs / "secret.key").exists()

    @pytest.mark.asyncio
    async def test_download_normal(self, client, dirs):
        resp = await client.get("/api/scraper/scrapes/123/scrape_123_1.json")
        assert resp.status_code == 200
        resp = await client.get("/api/scraper/scrapes/top.json")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_delete_normal(self, client, dirs):
        resp = await client.delete("/api/scraper/scrapes/123/scrape_123_1.json")
        assert resp.status_code == 200
        assert not (dirs / "scrapes" / "123").exists()
