"""SERVER_STATUS and CLIENT_STATUS: validation, defaults, saving and the live apply paths."""
import os
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

_tmpdir = tempfile.mkdtemp()
os.environ.setdefault("DATA_DIR", _tmpdir)
os.environ.setdefault("DB_PATH", os.path.join(_tmpdir, "test.db"))
os.environ.setdefault("LOG_LEVEL", "WARNING")

from copycord.admin.app import app, db  # noqa: E402
from copycord.common.bot_status import normalize_status, read_status  # noqa: E402

from httpx import AsyncClient, ASGITransport  # noqa: E402


@pytest.fixture()
def client(monkeypatch):
    from copycord.admin import auth

    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Origin": "http://test", "X-Copycord-CSRF": "1"},
        cookies={auth.ADMIN_COOKIE_NAME: auth.make_session_token()},
    )


@pytest.fixture()
def ws_cmd(monkeypatch):
    import copycord.admin.app as app_mod

    m = AsyncMock(return_value={"ok": False, "error": "unreachable"})
    monkeypatch.setattr(app_mod, "_ws_cmd", m)
    return m


@pytest.fixture(autouse=True)
def _clear_keys():
    for k in ("SERVER_STATUS", "CLIENT_STATUS"):
        db.set_config(k, "")
    yield


class TestNormalize:
    @pytest.mark.parametrize("v", ["online", "idle", "dnd", "invisible"])
    def test_known_values_pass(self, v):
        assert normalize_status(v) == v

    def test_case_and_space(self):
        assert normalize_status("  DnD ") == "dnd"

    @pytest.mark.parametrize("v", ["", None, "offline", "busy", "away", 3])
    def test_unknown_is_online(self, v):
        assert normalize_status(v) == "online"

    def test_read_status_unset_and_broken_db(self):
        assert read_status(db, "SERVER_STATUS") == "online"

        class Broken:
            def get_config(self, *a):
                raise RuntimeError("db gone")

        assert read_status(Broken(), "CLIENT_STATUS") == "online"


class TestApi:
    @pytest.mark.asyncio
    async def test_defaults_are_online(self, client):
        resp = await client.get("/api/bot-status")
        assert resp.status_code == 200
        assert resp.json() == {"SERVER_STATUS": "online", "CLIENT_STATUS": "online"}

    @pytest.mark.asyncio
    async def test_save_validates_and_touches_only_the_two_keys(self, client, ws_cmd):
        db.set_config("LOG_LEVEL", "DEBUG")
        resp = await client.post(
            "/api/bot-status", json={"SERVER_STATUS": "dnd", "CLIENT_STATUS": "nonsense"}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["SERVER_STATUS"] == "dnd"
        assert body["CLIENT_STATUS"] == "online"
        assert body["live"] == {"server": False, "client": False}
        assert db.get_config("SERVER_STATUS") == "dnd"
        assert db.get_config("CLIENT_STATUS") == "online"
        assert db.get_config("LOG_LEVEL") == "DEBUG"

    @pytest.mark.asyncio
    async def test_missing_key_keeps_saved_value(self, client, ws_cmd):
        db.set_config("CLIENT_STATUS", "idle")
        await client.post("/api/bot-status", json={"SERVER_STATUS": "invisible"})
        assert db.get_config("CLIENT_STATUS") == "idle"
        assert db.get_config("SERVER_STATUS") == "invisible"

    @pytest.mark.asyncio
    async def test_running_bots_are_told(self, client, ws_cmd):
        import copycord.admin.app as app_mod

        ws_cmd.return_value = {"ok": True, "applied": True}
        resp = await client.post(
            "/api/bot-status", json={"SERVER_STATUS": "idle", "CLIENT_STATUS": "dnd"}
        )
        assert resp.json()["live"] == {"server": True, "client": True}
        sent = {c.args[0]: c.args[1] for c in ws_cmd.await_args_list}
        assert sent[app_mod.SERVER_AGENT_URL] == {"type": "bot_status", "data": {"status": "idle"}}
        assert sent[app_mod.CLIENT_AGENT_URL] == {"type": "bot_status", "data": {"status": "dnd"}}

    @pytest.mark.asyncio
    async def test_global_save_does_not_wipe_status(self, client, ws_cmd, monkeypatch):
        import copycord.admin.app as app_mod

        monkeypatch.setattr(app_mod, "_verify_tokens_for_save", AsyncMock(return_value=[]))
        db.set_config("SERVER_STATUS", "dnd")
        resp = await client.post(
            "/save",
            data={"SERVER_TOKEN": "a", "CLIENT_TOKEN": "b", "LOG_LEVEL": "INFO"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert db.get_config("SERVER_STATUS") == "dnd"


class TestServerApply:
    def _server(self, ready=True):
        import discord

        from copycord.server.server import ServerReceiver

        srv = ServerReceiver.__new__(ServerReceiver)
        srv.bot = SimpleNamespace(is_ready=lambda: ready, change_presence=AsyncMock())
        srv._last_status = "Watching 2 servers"
        return srv, discord

    @pytest.mark.asyncio
    async def test_live_keeps_activity(self):
        srv, discord = self._server()
        res = await srv.apply_bot_status("dnd")
        assert res == {"ok": True, "applied": True}
        kw = srv.bot.change_presence.await_args.kwargs
        assert kw["status"] is discord.Status.dnd
        assert kw["activity"].name == "Watching 2 servers"

    @pytest.mark.asyncio
    async def test_not_ready_waits_for_login(self):
        srv, _ = self._server(ready=False)
        res = await srv.apply_bot_status("idle")
        assert res == {"ok": True, "applied": False}
        assert srv._bot_status == "idle"
        srv.bot.change_presence.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_activity_updates_keep_status(self):
        srv, discord = self._server()
        srv._bot_status = "invisible"
        await srv.update_status("Watching 3 servers")
        assert srv.bot.change_presence.await_args.kwargs["status"] is discord.Status.invisible

    @pytest.mark.asyncio
    async def test_unset_sends_online_like_the_library_default(self):
        srv, discord = self._server()
        del srv._last_status
        await srv.update_status("x")
        assert srv.bot.change_presence.await_args.kwargs["status"] is discord.Status.online


class TestClientApply:
    def _client(self, ready=True):
        from copycord.client.client import ClientListener

        cl = ClientListener.__new__(ClientListener)
        gw = SimpleNamespace(status="online")
        cl.bot = SimpleNamespace(
            is_ready=lambda: ready,
            change_presence=AsyncMock(),
            status="online",
            gateway=gw,
        )
        return cl

    @pytest.mark.asyncio
    async def test_live(self):
        cl = self._client()
        res = await cl.apply_bot_status("idle")
        assert res == {"ok": True, "applied": True}
        cl.bot.change_presence.assert_awaited_once_with(status="idle")

    @pytest.mark.asyncio
    async def test_not_ready_sets_pending_status(self):
        cl = self._client(ready=False)
        res = await cl.apply_bot_status("bogus")
        assert res == {"ok": True, "applied": False}
        assert cl.bot.status == "online"
        assert cl.bot.gateway.status == "online"
        cl.bot.change_presence.assert_not_awaited()

    def test_login_status_comes_from_db(self, monkeypatch):
        import copycord.client.client as client_mod

        seen = {}

        class FakeClient:
            def __init__(self, **kw):
                seen.update(kw)

            def event(self, fn):
                return fn

        monkeypatch.setattr(client_mod, "Client", FakeClient)
        cl = client_mod.ClientListener.__new__(client_mod.ClientListener)
        cl.db = SimpleNamespace(get_config=lambda k, d="": "dnd" if k == "CLIENT_STATUS" else d)
        monkeypatch.setattr(client_mod.ClientListener, "EVENT_HANDLERS", ())
        cl._new_bot(None)
        assert seen["status"] == "dnd"

    def test_selfclient_default_matches_online(self):
        import inspect

        from copycord.selfclient import Client

        assert inspect.signature(Client.__init__).parameters["status"].default == "online"
