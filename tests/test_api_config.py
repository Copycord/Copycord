"""GET /api/config: the saved global config values the dashboard form shows."""
import os
import tempfile
from unittest.mock import AsyncMock

import pytest

# Environment must be set before the app is imported.
_tmpdir = tempfile.mkdtemp()
os.environ.setdefault("DATA_DIR", _tmpdir)
os.environ.setdefault("DB_PATH", os.path.join(_tmpdir, "test.db"))
os.environ.setdefault("LOG_LEVEL", "WARNING")

from copycord.admin.app import ALLOWED_ENV, app, db  # noqa: E402

from httpx import AsyncClient, ASGITransport


def _client(headers=None, cookies=None):
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers=headers or {},
        cookies=cookies or {},
        follow_redirects=False,
    )


@pytest.fixture()
def client(monkeypatch):
    from copycord.admin import auth

    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    return _client(
        {"Origin": "http://test", "X-Copycord-CSRF": "1"},
        {auth.ADMIN_COOKIE_NAME: auth.make_session_token()},
    )


@pytest.fixture()
def anon(monkeypatch):
    from copycord.admin import auth

    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    return _client()


@pytest.fixture(autouse=True)
def _no_discord(monkeypatch):
    import copycord.admin.app as app_mod

    monkeypatch.setattr(app_mod, "_verify_tokens_for_save", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        app_mod, "_ws_cmd", AsyncMock(return_value={"ok": True, "running": False})
    )


class TestApiConfig:

    @pytest.mark.asyncio
    async def test_logged_out_is_rejected(self, anon):
        resp = await anon.get("/api/config")
        assert resp.status_code in (302, 307)
        assert resp.headers["location"].endswith("/login")

    @pytest.mark.asyncio
    async def test_values_match_what_save_stored(self, client):
        saved = {
            "SERVER_TOKEN": "srv-tok",
            "CLIENT_TOKEN": "cli-tok",
            "COMMAND_USERS": "111,222",
            "COPYCORD_AUTOSTART": "true",
            "LOG_LEVEL": "DEBUG",
            "LOG_MAX_SIZE_MB": "25",
        }
        save = await client.post("/save", data=saved)
        assert save.status_code == 303

        resp = await client.get("/api/config")
        assert resp.status_code == 200
        assert resp.json() == saved

    @pytest.mark.asyncio
    async def test_only_the_form_keys_are_returned(self, client):
        db.set_config("SOMETHING_ELSE", "secret")
        try:
            resp = await client.get("/api/config")
        finally:
            db.delete_config("SOMETHING_ELSE")
        body = resp.json()
        assert set(body) == set(ALLOWED_ENV)
        assert "SOMETHING_ELSE" not in body
