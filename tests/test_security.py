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


# ---------------------------------------------------------------------------
# Websocket authentication
# ---------------------------------------------------------------------------

from starlette.testclient import TestClient  # noqa: E402
from starlette.websockets import WebSocketDisconnect  # noqa: E402

from copycord.admin import auth  # noqa: E402
from copycord.common.websockets import (  # noqa: E402
    INTERNAL_TOKEN_ENV,
    INTERNAL_TOKEN_HEADER,
    internal_connect_kwargs,
)

ORIGIN = "http://testserver"
TOKEN = "per-run-test-token"


@pytest.fixture()
def tc():
    # No context manager: the lifespan (agents, schedulers) is not started.
    return TestClient(app)


@pytest.fixture()
def password(monkeypatch):
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")


@pytest.fixture()
def no_password(monkeypatch):
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "")


@pytest.fixture()
def internal_token(monkeypatch):
    monkeypatch.setenv(INTERNAL_TOKEN_ENV, TOKEN)


def _session_cookie():
    return f"{auth.ADMIN_COOKIE_NAME}={auth._get_signer().dumps({'ok': True})}"


def _rejected(tc, path, headers):
    with pytest.raises(WebSocketDisconnect) as exc:
        with tc.websocket_connect(path, headers=headers):
            pass
    assert exc.value.code == 1008


class TestBrowserSockets:

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/out", "/ws/in"])
    def test_no_cookie_rejected(self, tc, password, path):
        _rejected(tc, path, {"origin": ORIGIN})

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/out", "/ws/in"])
    def test_bad_cookie_rejected(self, tc, password, path):
        _rejected(tc, path, {"origin": ORIGIN, "cookie": "cc_admin=forged"})

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/out", "/ws/in"])
    @pytest.mark.parametrize(
        "origin", ["http://evil.example", "http://testserver.evil.example", "null", ""]
    )
    def test_bad_origin_rejected_with_cookie(self, tc, password, path, origin):
        headers = {"cookie": _session_cookie()}
        if origin:
            headers["origin"] = origin
        _rejected(tc, path, headers)

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/out", "/ws/in"])
    def test_bad_origin_rejected_without_password(self, tc, no_password, path):
        _rejected(tc, path, {"origin": "http://evil.example"})

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/in"])
    def test_internal_token_not_enough_for_ui(self, tc, password, internal_token, path):
        _rejected(tc, path, {INTERNAL_TOKEN_HEADER: TOKEN})

    def test_valid_cookie_and_origin_accepted(self, tc, password):
        headers = {"origin": ORIGIN, "cookie": _session_cookie()}
        with tc.websocket_connect("/ws/ui", headers=headers):
            pass

    def test_no_password_same_origin_accepted(self, tc, no_password):
        with tc.websocket_connect("/ws/ui", headers={"origin": ORIGIN}):
            pass

    def test_forwarded_host_accepted(self, tc, password):
        headers = {
            "origin": "https://copycord.example.com",
            "x-forwarded-host": "copycord.example.com",
            "cookie": _session_cookie(),
        }
        with tc.websocket_connect("/ws/ui", headers=headers):
            pass


class TestInternalSockets:

    @pytest.mark.parametrize("path", ["/bus", "/ws/out"])
    def test_no_token_rejected(self, tc, internal_token, path):
        _rejected(tc, path, {})

    @pytest.mark.parametrize("path", ["/bus", "/ws/out"])
    def test_bad_token_rejected(self, tc, internal_token, path):
        _rejected(tc, path, {INTERNAL_TOKEN_HEADER: "wrong"})

    def test_token_required_even_without_password(self, tc, no_password, internal_token):
        _rejected(tc, "/bus", {"origin": ORIGIN})

    def test_session_cookie_not_enough_for_bus(self, tc, password, internal_token):
        _rejected(tc, "/bus", {"origin": ORIGIN, "cookie": _session_cookie()})

    def test_no_token_configured_rejects_everything(self, tc, monkeypatch):
        monkeypatch.delenv(INTERNAL_TOKEN_ENV, raising=False)
        _rejected(tc, "/bus", {INTERNAL_TOKEN_HEADER: ""})

    def test_valid_token_accepted(self, tc, password, internal_token):
        with tc.websocket_connect("/bus", headers={INTERNAL_TOKEN_HEADER: TOKEN}) as ws:
            ws.send_text('{"kind":"log","role":"test","payload":{"text":"hi"}}')


class TestInternalClientHeaders:

    def test_kwargs_carry_token(self, internal_token):
        kwargs = internal_connect_kwargs()
        assert len(kwargs) == 1
        (headers,) = kwargs.values()
        assert headers == {INTERNAL_TOKEN_HEADER: TOKEN}

    def test_no_token_no_kwargs(self, monkeypatch):
        monkeypatch.delenv(INTERNAL_TOKEN_ENV, raising=False)
        assert internal_connect_kwargs() == {}
