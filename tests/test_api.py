"""
FastAPI endpoint tests for the Copycord admin API.

These tests use httpx AsyncClient to exercise endpoints that don't
require live Discord connections. WebSocket control commands are mocked.
"""
import os
import tempfile
from unittest.mock import AsyncMock

import pytest

# Environment must be set before the app is imported.
_tmpdir = tempfile.mkdtemp()
os.environ["DATA_DIR"] = _tmpdir
os.environ["DB_PATH"] = os.path.join(_tmpdir, "test.db")
os.environ["LOG_LEVEL"] = "WARNING"

# Importing the app runs its module-level init (DB, schema).
from copycord.admin.app import app, db  # noqa: E402

from httpx import AsyncClient, ASGITransport


@pytest.fixture()
def client(monkeypatch):
    """Provide an httpx AsyncClient against the test app, sending what the dashboard sends,
    logged in."""
    from copycord.admin import auth

    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    transport = ASGITransport(app=app)
    return AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Origin": "http://test", "X-Copycord-CSRF": "1"},
        cookies={auth.ADMIN_COOKIE_NAME: auth.make_session_token()},
    )


@pytest.fixture(autouse=True)
def _mock_ws(monkeypatch):
    """Ensure all WebSocket control commands return a safe mock."""
    import copycord.admin.app as app_mod
    mock = AsyncMock(return_value={"ok": True, "running": False, "status": "stopped"})
    monkeypatch.setattr(app_mod, "_ws_cmd", mock)


@pytest.fixture(autouse=True)
def _clean_db():
    """Clean event_logs and guild_mappings between tests."""
    yield
    try:
        db.clear_event_logs()
        for m in db.list_guild_mappings():
            db.delete_guild_mapping(m["mapping_id"])
    except Exception:
        pass


# Health

class TestHealth:

    @pytest.mark.asyncio
    async def test_health_ok(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.text == "ok"


# Event logs API

class TestEventLogsAPI:

    @pytest.mark.asyncio
    async def test_add_and_get(self, client):
        resp = await client.post(
            "/api/event-logs",
            json={
                "event_type": "channel_create",
                "details": "Created #general",
                "guild_id": 111,
                "guild_name": "Test Guild",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert "log_id" in body

        resp = await client.get("/api/event-logs")
        body = resp.json()
        assert body["total"] == 1
        assert body["logs"][0]["event_type"] == "channel_create"

    @pytest.mark.asyncio
    async def test_filter_by_type(self, client):
        await client.post("/api/event-logs", json={"event_type": "A", "details": "x"})
        await client.post("/api/event-logs", json={"event_type": "B", "details": "y"})

        resp = await client.get("/api/event-logs", params={"event_type": "A"})
        body = resp.json()
        assert body["total"] == 1
        assert body["logs"][0]["event_type"] == "A"

    @pytest.mark.asyncio
    async def test_delete_single(self, client):
        resp = await client.post("/api/event-logs", json={"event_type": "x", "details": "d"})
        log_id = resp.json()["log_id"]

        resp = await client.delete(f"/api/event-logs/{log_id}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        resp = await client.get("/api/event-logs")
        assert resp.json()["total"] == 0

    @pytest.mark.asyncio
    async def test_delete_bulk(self, client):
        r1 = await client.post("/api/event-logs", json={"event_type": "x", "details": "a"})
        r2 = await client.post("/api/event-logs", json={"event_type": "x", "details": "b"})
        await client.post("/api/event-logs", json={"event_type": "x", "details": "c"})

        resp = await client.post(
            "/api/event-logs/delete-bulk",
            json={"ids": [r1.json()["log_id"], r2.json()["log_id"]]},
        )
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 2

        resp = await client.get("/api/event-logs")
        assert resp.json()["total"] == 1

    @pytest.mark.asyncio
    async def test_clear_all(self, client):
        await client.post("/api/event-logs", json={"event_type": "x", "details": "a"})
        await client.post("/api/event-logs", json={"event_type": "x", "details": "b"})

        resp = await client.delete("/api/event-logs")
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 2

    @pytest.mark.asyncio
    async def test_get_event_log_types(self, client):
        await client.post("/api/event-logs", json={"event_type": "alpha", "details": ""})
        await client.post("/api/event-logs", json={"event_type": "beta", "details": ""})

        resp = await client.get("/api/event-log-types")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert "alpha" in body["types"]
        assert "beta" in body["types"]

# Guild mappings API

class TestGuildMappingsAPI:

    @pytest.mark.asyncio
    async def test_delete_mapping(self, client):
        mid = db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="API Test",
            original_guild_id=111,
            original_guild_name="Host",
            original_guild_icon_url=None,
            cloned_guild_id=222,
            cloned_guild_name="Clone",
        )

        resp = await client.get("/api/guild-mappings")
        assert len(resp.json()["mappings"]) == 1

        resp = await client.delete(f"/api/guild-mappings/{mid}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        resp = await client.get("/api/guild-mappings")
        assert resp.json()["mappings"] == []

    @pytest.mark.asyncio
    async def test_retired_setting_key_loads_and_saves(self, client):
        """A mapping stored with the removed DB_CLEANUP_MSG setting still lists, resolves and saves."""
        from types import SimpleNamespace

        from copycord.common.common_helpers import resolve_mapping_settings
        from copycord.common.config import Config

        mid = db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="Legacy",
            original_guild_id=555,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=666,
            cloned_guild_name="",
            settings={"DB_CLEANUP_MSG": False, "CLONE_EMOJI": False},
        )

        resp = await client.get("/api/guild-mappings")
        assert resp.status_code == 200
        (row,) = resp.json()["mappings"]
        assert row["settings"]["CLONE_EMOJI"] is False

        cfg = SimpleNamespace(default_mapping_settings=lambda: Config.default_mapping_settings(None))
        assert "DB_CLEANUP_MSG" not in cfg.default_mapping_settings()
        eff = resolve_mapping_settings(db, cfg, original_guild_id=555, cloned_guild_id=666)
        assert eff["CLONE_EMOJI"] is False

        # Same host and clone pair as stored, so the save skips the Discord membership checks.
        resp = await client.patch(
            f"/api/guild-mappings/{mid}",
            json={
                "mapping_name": "Legacy",
                "original_guild_id": "555",
                "cloned_guild_id": "666",
                "settings": dict(row["settings"], CLONE_EMOJI=True),
            },
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        assert db.get_mapping_by_id(mid)["mapping_name"] == "Legacy"
        eff = resolve_mapping_settings(db, cfg, original_guild_id=555, cloned_guild_id=666)
        assert eff["CLONE_EMOJI"] is True

    @pytest.mark.asyncio
    async def test_retired_keep_online_loads_and_saves(self, client):
        """A mapping stored with the removed USER_TOKEN_KEEP_ONLINE still lists, resolves and saves."""
        from types import SimpleNamespace

        import copycord.admin.app as app_mod
        from copycord.common.common_helpers import resolve_mapping_settings
        from copycord.common.config import Config

        cfg = SimpleNamespace(default_mapping_settings=lambda: Config.default_mapping_settings(None))
        assert "USER_TOKEN_KEEP_ONLINE" not in cfg.default_mapping_settings()
        assert "USER_TOKEN_KEEP_ONLINE" not in app_mod.DEFAULTS
        assert "USER_TOKEN_KEEP_ONLINE" not in app_mod.BOOL_KEYS

        mid = db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="Keep",
            original_guild_id=575,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=676,
            cloned_guild_name="",
            settings={"USER_TOKEN_KEEP_ONLINE": 0, "USER_TOKEN_PRESENCE": "idle"},
        )
        resp = await client.get("/api/guild-mappings")
        assert resp.status_code == 200
        (row,) = resp.json()["mappings"]
        assert row["settings"]["USER_TOKEN_PRESENCE"] == "idle"
        eff = resolve_mapping_settings(db, cfg, original_guild_id=575, cloned_guild_id=676)
        assert eff["USER_TOKEN_PRESENCE"] == "idle"

        resp = await client.patch(
            f"/api/guild-mappings/{mid}",
            json={
                "mapping_name": "Keep",
                "original_guild_id": "575",
                "cloned_guild_id": "676",
                "settings": dict(row["settings"], USER_TOKEN_PRESENCE="busy"),
            },
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        eff = resolve_mapping_settings(db, cfg, original_guild_id=575, cloned_guild_id=676)
        assert eff["USER_TOKEN_PRESENCE"] == "busy"

    @pytest.mark.asyncio
    async def test_presence_rotate_defaults_off_and_round_trips(self, client):
        from types import SimpleNamespace

        import copycord.admin.app as app_mod
        from copycord.common.common_helpers import resolve_mapping_settings
        from copycord.common.config import Config

        cfg = SimpleNamespace(default_mapping_settings=lambda: Config.default_mapping_settings(None))
        assert cfg.default_mapping_settings()["USER_TOKEN_PRESENCE_ROTATE"] is False
        assert "USER_TOKEN_PRESENCE_ROTATE" in app_mod.BOOL_KEYS
        assert app_mod.DEFAULTS["USER_TOKEN_PRESENCE_ROTATE"] is False

        mid = db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="Rotate",
            original_guild_id=777,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=888,
            cloned_guild_name="",
            settings={"USER_TOKEN_PRESENCE": "random"},
        )
        eff = resolve_mapping_settings(db, cfg, original_guild_id=777, cloned_guild_id=888)
        assert eff["USER_TOKEN_PRESENCE_ROTATE"] is False

        for on in (True, False):
            resp = await client.patch(
                f"/api/guild-mappings/{mid}",
                json={
                    "mapping_name": "Rotate",
                    "original_guild_id": "777",
                    "cloned_guild_id": "888",
                    "settings": {"USER_TOKEN_PRESENCE": "random", "USER_TOKEN_PRESENCE_ROTATE": on},
                },
            )
            assert resp.status_code == 200
            (row,) = (await client.get("/api/guild-mappings")).json()["mappings"]
            assert row["settings"]["USER_TOKEN_PRESENCE_ROTATE"] is on
            eff = resolve_mapping_settings(db, cfg, original_guild_id=777, cloned_guild_id=888)
            assert eff["USER_TOKEN_PRESENCE_ROTATE"] is on

    @pytest.mark.asyncio
    async def test_toggle_status(self, client):
        mid = db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="Toggle Test",
            original_guild_id=333,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=444,
            cloned_guild_name="",
        )

        # Toggle to paused
        resp = await client.post(f"/api/guild-mappings/{mid}/toggle-status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["status"] == "paused"

        # Toggle back to active
        resp = await client.post(f"/api/guild-mappings/{mid}/toggle-status")
        body = resp.json()
        assert body["status"] == "active"

# Mapping user tokens API

class TestMappingUserTokensAPI:

    def _make_mapping(self):
        return db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="UT API",
            original_guild_id=111,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=222,
            cloned_guild_name="",
        )

    @pytest.mark.asyncio
    async def test_add_validates_and_masks(self, client, monkeypatch):
        import copycord.admin.app as app_mod
        monkeypatch.setattr(
            app_mod, "_selfbot_in_guild", AsyncMock(return_value=True)
        )
        monkeypatch.setattr(
            app_mod,
            "_selfbot_identity",
            AsyncMock(return_value={"id": "999", "username": "tester"}),
        )
        mid = self._make_mapping()

        secret = "supersecrettoken1234"
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens",
            json={"token": secret, "label": "acct"},
        )
        assert resp.status_code == 200
        tok = resp.json()["token"]
        assert tok["username"] == "tester"
        assert tok["enabled"] is True
        # Full token is never returned to the client.
        assert secret not in str(resp.json())
        assert tok["token_masked"].startswith("supers")

        # Listing also returns it masked.
        resp = await client.get(f"/api/guild-mappings/{mid}/user-tokens")
        toks = resp.json()["tokens"]
        assert len(toks) == 1
        assert secret not in str(toks)

    @pytest.mark.asyncio
    async def test_add_rejects_account_not_in_clone(self, client, monkeypatch):
        import copycord.admin.app as app_mod
        monkeypatch.setattr(
            app_mod, "_selfbot_in_guild", AsyncMock(return_value=False)
        )
        mid = self._make_mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens", json={"token": "bad"}
        )
        assert resp.status_code == 400
        assert resp.json()["ok"] is False
        # Nothing was stored.
        resp = await client.get(f"/api/guild-mappings/{mid}/user-tokens")
        assert resp.json()["tokens"] == []

    @pytest.mark.asyncio
    async def test_toggle_and_delete(self, client, monkeypatch):
        import copycord.admin.app as app_mod
        monkeypatch.setattr(
            app_mod, "_selfbot_in_guild", AsyncMock(return_value=True)
        )
        monkeypatch.setattr(
            app_mod, "_selfbot_identity", AsyncMock(return_value=None)
        )
        mid = self._make_mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens",
            json={"token": "tok12345678"},
        )
        token_id = resp.json()["token"]["id"]

        resp = await client.patch(
            f"/api/guild-mappings/{mid}/user-tokens/{token_id}",
            json={"enabled": False},
        )
        assert resp.status_code == 200
        assert resp.json()["token"]["enabled"] is False

        resp = await client.delete(
            f"/api/guild-mappings/{mid}/user-tokens/{token_id}"
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        resp = await client.get(f"/api/guild-mappings/{mid}/user-tokens")
        assert resp.json()["tokens"] == []

    @pytest.mark.asyncio
    async def test_bulk_add_dedupes_and_adds(self, client, monkeypatch):
        import copycord.admin.app as app_mod
        monkeypatch.setattr(
            app_mod, "_selfbot_in_guild", AsyncMock(return_value=True)
        )
        monkeypatch.setattr(
            app_mod,
            "_selfbot_identity",
            AsyncMock(return_value={"id": "1", "username": "u"}),
        )
        mid = self._make_mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/bulk",
            json={"tokens": "tokaaaaaaaa\ntokbbbbbbbb\n  \ntokaaaaaaaa"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        # Blank + duplicate line collapse to two unique tokens.
        assert body["total"] == 2
        assert body["added"] == 2

        resp = await client.get(f"/api/guild-mappings/{mid}/user-tokens")
        assert len(resp.json()["tokens"]) == 2

    @pytest.mark.asyncio
    async def test_bulk_add_marks_invalid(self, client, monkeypatch):
        import copycord.admin.app as app_mod
        async def fake_in_guild(token, gid, proxy=None):
            assert proxy == "http://proxy.test:8080"
            return token == "goodtoken1"

        monkeypatch.setattr(app_mod, "_selfbot_in_guild", fake_in_guild)
        monkeypatch.setattr(
            app_mod, "_selfbot_identity", AsyncMock(return_value=None)
        )
        mid = self._make_mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/bulk",
            json={"tokens": "goodtoken1\nbadtoken2"},
        )
        body = resp.json()
        assert body["added"] == 1
        statuses = sorted(r["status"] for r in body["results"])
        assert statuses == ["added", "invalid"]

    async def _add_tokens(self, client, monkeypatch, mid, tokens):
        import copycord.admin.app as app_mod
        monkeypatch.setattr(
            app_mod, "_selfbot_in_guild", AsyncMock(return_value=True)
        )
        monkeypatch.setattr(
            app_mod, "_selfbot_identity", AsyncMock(return_value=None)
        )
        ids = []
        for t in tokens:
            resp = await client.post(
                f"/api/guild-mappings/{mid}/user-tokens", json={"token": t}
            )
            ids.append(resp.json()["token"]["id"])
        return ids

    @pytest.mark.asyncio
    async def test_verify_reports_working_and_broken(self, client, monkeypatch):
        import copycord.admin.app as app_mod
        mid = self._make_mapping()
        await self._add_tokens(
            client, monkeypatch, mid, ["goodtoken1", "badtoken22"]
        )

        # Re-check: only the first token is still a member.
        async def fake_in_guild(token, gid, proxy=None):
            assert proxy == "http://proxy.test:8080"
            return token == "goodtoken1"

        monkeypatch.setattr(app_mod, "_selfbot_in_guild", fake_in_guild)

        resp = await client.post(f"/api/guild-mappings/{mid}/user-tokens/verify")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["total"] == 2
        assert body["working"] == 1
        assert body["broken"] == 1
        # Full token never leaks; each result carries an id + ok flag.
        assert "goodtoken1" not in str(body)
        by_ok = {r["ok"] for r in body["results"]}
        assert by_ok == {True, False}
        assert all(r.get("id") for r in body["results"])

    @pytest.mark.asyncio
    async def test_delete_by_ids(self, client, monkeypatch):
        mid = self._make_mapping()
        ids = await self._add_tokens(
            client, monkeypatch, mid, ["tokaaaaaaaa", "tokbbbbbbbb", "tokcccccccc"]
        )

        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/delete",
            json={"ids": ids[:2]},
        )
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 2

        resp = await client.get(f"/api/guild-mappings/{mid}/user-tokens")
        remaining = resp.json()["tokens"]
        assert len(remaining) == 1
        assert remaining[0]["id"] == ids[2]

    @pytest.mark.asyncio
    async def test_delete_scope_all(self, client, monkeypatch):
        mid = self._make_mapping()
        await self._add_tokens(
            client, monkeypatch, mid, ["tokaaaaaaaa", "tokbbbbbbbb"]
        )

        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/delete",
            json={"scope": "all"},
        )
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 2

        resp = await client.get(f"/api/guild-mappings/{mid}/user-tokens")
        assert resp.json()["tokens"] == []

    @pytest.mark.asyncio
    async def test_delete_requires_scope_or_ids(self, client):
        mid = self._make_mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/delete", json={}
        )
        assert resp.status_code == 400

# Version endpoint

# Role logs: <home>/logs, and the old data dir location for one release

class TestRoleLogLocations:

    @pytest.fixture()
    def places(self, monkeypatch, tmp_path):
        import copycord.admin.app as app_mod
        new, old = tmp_path / "logs", tmp_path / "old"
        new.mkdir()
        old.mkdir()
        monkeypatch.setattr(
            app_mod, "out_candidates", lambda name: [new / name, old / name]
        )
        return new, old

    @pytest.mark.asyncio
    async def test_reads_old_location(self, client, places):
        _new, old = places
        (old / "server.out").write_text("old line\n", encoding="utf-8")
        resp = await client.get("/logs/server")
        assert resp.status_code == 200
        assert "old line" in resp.text

    @pytest.mark.asyncio
    async def test_new_location_wins(self, client, places):
        new, old = places
        (old / "client.out").write_text("old line\n", encoding="utf-8")
        (new / "client.out").write_text("new line\n", encoding="utf-8")
        resp = await client.get("/logs/client")
        assert resp.status_code == 200
        assert "new line" in resp.text

    @pytest.mark.asyncio
    async def test_clear_empties_both(self, client, places):
        new, old = places
        (old / "server.out").write_text("old\n", encoding="utf-8")
        (new / "server.out").write_text("new\n", encoding="utf-8")
        resp = await client.post("/logs/clear/server")
        assert resp.status_code == 200
        assert (old / "server.out").read_text() == ""
        assert (new / "server.out").read_text() == ""


# Forwarding rules: provider validation only runs for enabled rules

class TestForwardingSaveValidation:

    @pytest.fixture()
    def validator(self, monkeypatch):
        import copycord.admin.app as app_mod
        mock = AsyncMock(
            return_value=(False, ["Telegram: invalid bot token. (Unauthorized)"], {})
        )
        monkeypatch.setattr(app_mod, "_validate_forwarding_provider_config", mock)
        yield mock
        db.delete_message_forward_rule("fwd-dead-token")

    def _payload(self, enabled):
        return {
            "rule_id": "fwd-dead-token",
            "guild_id": "123",
            "label": "tg",
            "provider": "telegram",
            "enabled": enabled,
            "config": {"bot_token": "dead", "chat_id": "1"},
        }

    @pytest.mark.asyncio
    async def test_disabling_skips_provider_validation(self, client, validator):
        resp = await client.post("/api/forwarding", json=self._payload(False))
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        validator.assert_not_awaited()
        rule = db.get_message_forwarding_rule("fwd-dead-token")
        assert rule is not None
        assert rule["enabled"] is False
        assert rule["config"]["bot_token"] == "dead"

    @pytest.mark.asyncio
    async def test_enabling_validates_and_returns_the_error_text(self, client, validator):
        resp = await client.post("/api/forwarding", json=self._payload(False))
        assert resp.status_code == 200
        resp = await client.post("/api/forwarding", json=self._payload(True))
        assert resp.status_code == 400
        assert resp.text == "Telegram: invalid bot token. (Unauthorized)"
        validator.assert_awaited_once()
        assert db.get_message_forwarding_rule("fwd-dead-token")["enabled"] is False


# Account guild list: cache, single-flight, rate limits (Discord is faked)

class _GuildsResp:
    def __init__(self, status, body, headers=None, delay=0.0):
        self.status = status
        self._body = body
        self.headers = headers or {}
        self._delay = delay

    async def __aenter__(self):
        import asyncio

        if self._delay:
            await asyncio.sleep(self._delay)
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        import json

        return json.dumps(self._body)


class _GuildsSession:
    """Answers GET /users/@me/guilds from a queue of responses, recording each URL."""

    def __init__(self, *responses):
        self.queue = list(responses)
        self.urls = []
        self.proxies = []

    def get(self, url, headers=None, timeout=None, proxy=None):
        self.urls.append(url)
        self.proxies.append(proxy)
        resp = self.queue.pop(0) if len(self.queue) > 1 else self.queue[0]
        return resp


def _guilds(*ids):
    return [{"id": str(i), "name": f"G{i}"} for i in ids]


@pytest.fixture()
def fake_discord(monkeypatch):
    import copycord.admin.app as app_mod

    app_mod._user_guilds_invalidate()
    app_mod._user_guilds_inflight.clear()
    app_mod._bot_guild_cache.clear()
    monkeypatch.setattr(app_mod, "_selfbot_headers", lambda token: {"Authorization": "x"})

    def install(*responses):
        sess = _GuildsSession(*responses)
        monkeypatch.setattr(app_mod, "shared_session", lambda: sess)
        return sess

    yield install
    app_mod._user_guilds_invalidate()
    app_mod._user_guilds_inflight.clear()


class TestUserGuildsHelper:

    @pytest.mark.asyncio
    async def test_concurrent_callers_share_one_request(self, fake_discord):
        import asyncio
        import copycord.admin.app as app_mod

        sess = fake_discord(_GuildsResp(200, _guilds(1), delay=0.05))
        results = await asyncio.gather(*(app_mod._user_guilds("tok-a") for _ in range(6)))
        assert all(r == _guilds(1) for r in results)
        assert len(sess.urls) == 1

    @pytest.mark.asyncio
    async def test_429_with_cache_serves_stale_until_reset(self, fake_discord):
        import copycord.admin.app as app_mod

        sess = fake_discord(_GuildsResp(200, _guilds(1)))
        await app_mod._user_guilds("tok-a")
        key = app_mod._token_key("tok-a")
        at, items = app_mod._user_guilds_cache[key]
        app_mod._user_guilds_cache[key] = (at - app_mod.USER_GUILDS_TTL - 1, items)

        sess.queue = [_GuildsResp(429, {"retry_after": 30, "global": False}, {"Retry-After": "30"})]
        assert await app_mod._user_guilds("tok-a") == _guilds(1)
        assert len(sess.urls) == 2
        # Still inside the reset window: stale list, Discord not called again.
        assert await app_mod._user_guilds("tok-a") == _guilds(1)
        assert await app_mod._user_guilds("tok-a", refresh=True) == _guilds(1)
        assert len(sess.urls) == 2

    @pytest.mark.asyncio
    async def test_429_without_cache_long_wait_raises(self, fake_discord):
        import copycord.admin.app as app_mod

        sess = fake_discord(_GuildsResp(429, {"retry_after": 5, "global": False}))
        with pytest.raises(app_mod.DiscordGuildsError) as err:
            await app_mod._user_guilds("tok-a")
        assert err.value.status == 429
        assert err.value.detail == "discord-rate-limited"
        # Blocked until the reset: no second request.
        with pytest.raises(app_mod.DiscordGuildsError):
            await app_mod._user_guilds("tok-a")
        assert len(sess.urls) == 1

    @pytest.mark.asyncio
    async def test_token_change_misses_cache(self, fake_discord):
        import copycord.admin.app as app_mod

        sess = fake_discord(_GuildsResp(200, _guilds(1)))
        await app_mod._user_guilds("tok-a")
        await app_mod._user_guilds("tok-b")
        assert len(sess.urls) == 2
        assert "tok-a" not in str(app_mod._user_guilds_cache)

    @pytest.mark.asyncio
    async def test_membership_pages_past_200(self, fake_discord):
        import copycord.admin.app as app_mod

        sess = fake_discord(
            _GuildsResp(200, _guilds(*range(1, 201))),
            _GuildsResp(200, _guilds(500)),
        )
        assert await app_mod._selfbot_in_guild("tok-a", 500) is True
        assert len(sess.urls) == 2
        assert sess.urls[1].endswith("after=200")
        # Served from the cache now.
        assert await app_mod._selfbot_in_guild("tok-a", 3) is True
        assert len(sess.urls) == 2

    @pytest.mark.asyncio
    async def test_client_guilds_route_reports_rate_limit(self, client, fake_discord):
        fake_discord(_GuildsResp(429, {"retry_after": 9, "global": False}))
        db.set_config("CLIENT_TOKEN", "tok-route")
        try:
            resp = await client.get("/api/client-guilds")
        finally:
            db.set_config("CLIENT_TOKEN", "")
        assert resp.status_code == 429
        assert resp.json() == {"detail": "discord-rate-limited"}
        assert int(resp.headers["Retry-After"]) >= 1


# Mapping save: membership checks

class TestMappingSaveChecks:

    def _make_mapping(self):
        return db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="Save checks",
            original_guild_id=111,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=222,
            cloned_guild_name="",
        )

    def _concurrent_checks(self, monkeypatch):
        import asyncio
        import copycord.admin.app as app_mod

        state = {"active": 0, "peak": 0, "calls": []}

        def fake(name):
            async def check(token, gid):
                state["calls"].append((name, gid))
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
                await asyncio.sleep(0.05)
                state["active"] -= 1
                return True

            return check

        monkeypatch.setattr(app_mod, "_selfbot_in_guild", fake("host"))
        monkeypatch.setattr(app_mod, "_bot_in_guild", fake("clone"))
        return state

    @pytest.mark.asyncio
    async def test_patch_same_ids_skips_checks(self, client, monkeypatch):
        import copycord.admin.app as app_mod

        host = AsyncMock(return_value=False)
        bot = AsyncMock(return_value=False)
        monkeypatch.setattr(app_mod, "_selfbot_in_guild", host)
        monkeypatch.setattr(app_mod, "_bot_in_guild", bot)
        mid = self._make_mapping()
        resp = await client.patch(
            f"/api/guild-mappings/{mid}",
            json={"mapping_name": "Renamed", "original_guild_id": "111", "cloned_guild_id": "222"},
        )
        assert resp.status_code == 200
        host.assert_not_awaited()
        bot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_patch_changed_ids_checks_both_concurrently(self, client, monkeypatch):
        state = self._concurrent_checks(monkeypatch)
        mid = self._make_mapping()
        resp = await client.patch(
            f"/api/guild-mappings/{mid}",
            json={"mapping_name": "Moved", "original_guild_id": "333", "cloned_guild_id": "222"},
        )
        assert resp.status_code == 200
        assert sorted(state["calls"]) == [("clone", 222), ("host", 333)]
        assert state["peak"] == 2

    @pytest.mark.asyncio
    async def test_patch_changed_ids_still_rejects_non_member(self, client, monkeypatch):
        import copycord.admin.app as app_mod

        monkeypatch.setattr(app_mod, "_selfbot_in_guild", AsyncMock(return_value=False))
        monkeypatch.setattr(app_mod, "_bot_in_guild", AsyncMock(return_value=True))
        mid = self._make_mapping()
        resp = await client.patch(
            f"/api/guild-mappings/{mid}",
            json={"mapping_name": "Moved", "original_guild_id": "333", "cloned_guild_id": "222"},
        )
        assert resp.status_code == 400
        assert resp.json()["which"] == "original_guild_id"


# Warm token pool glue

class TestTokenPoolGlue:

    def test_enrich_from_bus_copies_token_pool(self):
        import copycord.admin.app as app_mod

        pool = {"keep_online": 5, "live": 1, "max_loop_lag_ms": 2.5, "tokens": {}}
        out = app_mod._enrich_from_bus(
            {"status": "running"}, {"pid": 7, "running": True, "token_pool": pool}
        )
        assert out["token_pool"] == pool
        assert out["status"] == "running"
        assert out["pid"] == 7
        assert out["running"] is True

    def test_enrich_from_bus_token_pool_null_or_absent(self):
        import copycord.admin.app as app_mod

        assert app_mod._enrich_from_bus({}, {"token_pool": None})["token_pool"] is None
        assert app_mod._enrich_from_bus({}, {"token_pool": "junk"})["token_pool"] is None
        assert "token_pool" not in app_mod._enrich_from_bus({}, {"status": "x"})

    def _make_mapping(self):
        return db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="Pool glue",
            original_guild_id=111,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=222,
            cloned_guild_name="",
        )

    def _reloads(self, mock):
        return [
            c for c in mock.call_args_list
            if len(c.args) > 1 and c.args[1] == {"type": "reload_user_tokens"}
        ]

    @pytest.mark.asyncio
    async def test_token_changes_and_settings_save_reload_pool(self, client, monkeypatch):
        import copycord.admin.app as app_mod

        ws = AsyncMock(return_value={"ok": True})
        monkeypatch.setattr(app_mod, "_ws_cmd", ws)
        monkeypatch.setattr(app_mod, "_selfbot_in_guild", AsyncMock(return_value=True))
        monkeypatch.setattr(app_mod, "_bot_in_guild", AsyncMock(return_value=True))
        monkeypatch.setattr(app_mod, "_selfbot_identity", AsyncMock(return_value=None))
        mid = self._make_mapping()

        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens", json={"token": "tokaaaaaaaa"}
        )
        tid = resp.json()["token"]["id"]
        assert len(self._reloads(ws)) == 1

        await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/bulk",
            json={"tokens": "tokbbbbbbbb\ntokcccccccc"},
        )
        assert len(self._reloads(ws)) == 2

        await client.patch(
            f"/api/guild-mappings/{mid}/user-tokens/{tid}", json={"enabled": False}
        )
        assert len(self._reloads(ws)) == 3

        await client.delete(f"/api/guild-mappings/{mid}/user-tokens/{tid}")
        assert len(self._reloads(ws)) == 4

        ids = [t["id"] for t in (await client.get(f"/api/guild-mappings/{mid}/user-tokens")).json()["tokens"]]
        await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/delete", json={"ids": ids[:1]}
        )
        assert len(self._reloads(ws)) == 5

        await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/delete", json={"scope": "all"}
        )
        assert len(self._reloads(ws)) == 6

        resp = await client.patch(
            f"/api/guild-mappings/{mid}",
            json={
                "mapping_name": "Pool glue",
                "original_guild_id": "111",
                "cloned_guild_id": "222",
                "settings": {"USER_TOKEN_PRESENCE": "busy", "USER_TOKEN_PRESENCE_ROTATE": True},
            },
        )
        assert resp.status_code == 200
        assert len(self._reloads(ws)) == 7
        assert all(c.args[0] == app_mod.SERVER_AGENT_URL for c in self._reloads(ws))

    @pytest.mark.asyncio
    async def test_reload_send_failure_only_logs(self, client, monkeypatch):
        import copycord.admin.app as app_mod

        from unittest.mock import Mock

        monkeypatch.setattr(app_mod, "_ws_cmd", Mock(side_effect=RuntimeError("down")))
        monkeypatch.setattr(app_mod, "_selfbot_in_guild", AsyncMock(return_value=True))
        monkeypatch.setattr(app_mod, "_selfbot_identity", AsyncMock(return_value=None))
        mid = self._make_mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens", json={"token": "tokdddddddd"}
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True


# Mappings that send through proxies need proxies for token checks

PROXIES_REQUIRED = (
    "No proxies are configured. Add proxies, or turn off Send through proxies for this mapping."
)


class TestProxiesRequired:

    def _mapping(self, settings=None):
        return db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="Proxy rule",
            original_guild_id=111,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=222,
            cloned_guild_name="",
            settings=settings or {},
        )

    def _discord(self, monkeypatch, *, proxies=()):
        import copycord.admin.app as app_mod

        monkeypatch.setattr(app_mod, "_user_token_proxies", lambda: list(proxies))
        mocks = {
            "_selfbot_in_guild": AsyncMock(return_value=True),
            "_selfbot_identity": AsyncMock(return_value=None),
            "_bot_in_guild": AsyncMock(return_value=True),
        }
        for name, mock in mocks.items():
            monkeypatch.setattr(app_mod, name, mock)
        return mocks

    def _assert_refused(self, resp, mocks):
        assert resp.status_code == 400
        assert resp.json() == {"ok": False, "error": PROXIES_REQUIRED}
        for mock in mocks.values():
            mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_add_without_proxies_is_refused_with_no_discord_call(self, client, monkeypatch):
        mocks = self._discord(monkeypatch)
        mid = self._mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens", json={"token": "tokaaaaaaaa"}
        )
        self._assert_refused(resp, mocks)
        assert db.list_mapping_tokens(mid) == []

    @pytest.mark.asyncio
    async def test_bulk_add_without_proxies_is_refused(self, client, monkeypatch):
        mocks = self._discord(monkeypatch)
        mid = self._mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/bulk",
            json={"tokens": "tokaaaaaaaa\ntokbbbbbbbb"},
        )
        self._assert_refused(resp, mocks)
        assert db.list_mapping_tokens(mid) == []

    @pytest.mark.asyncio
    async def test_verify_without_proxies_is_refused(self, client, monkeypatch):
        mocks = self._discord(monkeypatch)
        mid = self._mapping()
        db.add_mapping_token(mid, "tokaaaaaaaa")
        resp = await client.post(f"/api/guild-mappings/{mid}/user-tokens/verify")
        self._assert_refused(resp, mocks)

    @pytest.mark.asyncio
    async def test_reenabling_a_token_without_proxies_is_refused(self, client, monkeypatch):
        self._discord(monkeypatch)
        mid = self._mapping()
        tid = db.add_mapping_token(mid, "tokaaaaaaaa")
        db.set_mapping_token_enabled(tid, False)
        resp = await client.patch(
            f"/api/guild-mappings/{mid}/user-tokens/{tid}", json={"enabled": True}
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == PROXIES_REQUIRED
        # Turning one off is always allowed.
        db.set_mapping_token_enabled(tid, True)
        resp = await client.patch(
            f"/api/guild-mappings/{mid}/user-tokens/{tid}", json={"enabled": False}
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_add_with_proxies_checks_through_a_proxy(self, client, monkeypatch):
        mocks = self._discord(monkeypatch, proxies=["http://p1.test:1", "http://p2.test:2"])
        mid = self._mapping()
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens", json={"token": "tokaaaaaaaa"}
        )
        assert resp.status_code == 200
        proxy = mocks["_selfbot_in_guild"].await_args.kwargs["proxy"]
        assert proxy in ("http://p1.test:1", "http://p2.test:2")
        assert mocks["_selfbot_identity"].await_args.kwargs["proxy"] == proxy

    def _patch(self, client, mid, settings):
        return client.patch(
            f"/api/guild-mappings/{mid}",
            json={
                "mapping_name": "Proxy rule",
                "original_guild_id": "111",
                "cloned_guild_id": "222",
                "settings": settings,
            },
        )

    @pytest.mark.asyncio
    async def test_turning_user_tokens_on_without_proxies_is_refused(self, client, monkeypatch):
        self._discord(monkeypatch)
        mid = self._mapping({"USE_USER_TOKENS": False})
        db.add_mapping_token(mid, "tokaaaaaaaa")
        resp = await self._patch(client, mid, {"USE_USER_TOKENS": True})
        assert resp.status_code == 400
        assert resp.json() == {"ok": False, "error": PROXIES_REQUIRED}
        assert db.get_mapping_by_id(mid)["settings"].get("USE_USER_TOKENS") is False

    @pytest.mark.asyncio
    async def test_unrelated_save_with_tokens_on_is_kept_with_a_warning(self, client, monkeypatch):
        self._discord(monkeypatch)
        mid = self._mapping({"USE_USER_TOKENS": True})
        db.add_mapping_token(mid, "tokaaaaaaaa")
        resp = await self._patch(
            client, mid, {"USE_USER_TOKENS": True, "USER_TOKEN_TYPING": True}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["proxy_warning"] == PROXIES_REQUIRED
        assert db.get_mapping_by_id(mid)["settings"]["USER_TOKEN_TYPING"] is True

    @pytest.mark.asyncio
    async def test_turning_on_with_no_tokens_or_with_proxies_is_accepted(self, client, monkeypatch):
        self._discord(monkeypatch)
        mid = self._mapping({"USE_USER_TOKENS": False})
        resp = await self._patch(client, mid, {"USE_USER_TOKENS": True})
        assert resp.status_code == 200
        assert "proxy_warning" not in resp.json()

        db.delete_guild_mapping(mid)
        mid2 = self._mapping({"USE_USER_TOKENS": False})
        db.add_mapping_token(mid2, "tokbbbbbbbb")
        self._discord(monkeypatch, proxies=["http://p1.test:1"])
        resp = await self._patch(client, mid2, {"USE_USER_TOKENS": True})
        assert resp.status_code == 200
        assert "proxy_warning" not in resp.json()

    def test_check_proxy_is_stable_per_token(self, monkeypatch):
        import copycord.admin.app as app_mod

        monkeypatch.setattr(app_mod, "_user_token_proxies", lambda: ["http://a:1", "http://b:2"])
        picks = {app_mod._token_check_proxy("tokaaaaaaaa") for _ in range(5)}
        assert len(picks) == 1
        monkeypatch.setattr(app_mod, "_user_token_proxies", lambda: [])
        assert app_mod._token_check_proxy("tokaaaaaaaa") is None


class TestParallelTokenChecks:
    """Verify and bulk add check tokens in parallel across proxies, one at a time per proxy."""

    def _mapping(self, settings=None):
        return db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name="Parallel",
            original_guild_id=111,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=222,
            cloned_guild_name="",
            settings=settings or {},
        )

    def _no_proxy_lookups(self, monkeypatch):
        """Make any proxy lookup fail the test, for the direct check path."""
        import copycord.admin.app as app_mod

        def _boom(*a, **k):
            raise AssertionError("proxy lookup on the direct path")

        monkeypatch.setattr(app_mod, "_user_token_proxies", _boom)
        monkeypatch.setattr(app_mod, "_token_check_proxy", _boom)
        monkeypatch.setattr(app_mod, "_selfbot_identity", AsyncMock(return_value=None))

    def _proxies(self, monkeypatch, proxy_of):
        """Pin each token to a proxy; proxy_of maps token -> proxy url."""
        import copycord.admin.app as app_mod

        monkeypatch.setattr(
            app_mod, "_user_token_proxies", lambda: sorted(set(proxy_of.values()))
        )
        monkeypatch.setattr(app_mod, "_token_check_proxy", lambda t: proxy_of.get(t))
        monkeypatch.setattr(app_mod, "_selfbot_identity", AsyncMock(return_value=None))

    def _tracker(self, monkeypatch, *, delay=0.02, fail=(), member=None):
        """A fake _selfbot_in_guild recording how many checks overlap, overall and per proxy."""
        import asyncio
        import copycord.admin.app as app_mod

        state = {"active": 0, "max": 0, "per_proxy": {}, "max_per_proxy": {}, "calls": []}

        async def fake_in_guild(token, gid, proxy=None):
            state["calls"].append(token)
            state["active"] += 1
            state["max"] = max(state["max"], state["active"])
            n = state["per_proxy"].get(proxy, 0) + 1
            state["per_proxy"][proxy] = n
            state["max_per_proxy"][proxy] = max(state["max_per_proxy"].get(proxy, 0), n)
            try:
                await asyncio.sleep(delay(token) if callable(delay) else delay)
                if token in fail:
                    raise RuntimeError("network down")
                return True if member is None else token in member
            finally:
                state["active"] -= 1
                state["per_proxy"][proxy] -= 1

        monkeypatch.setattr(app_mod, "_selfbot_in_guild", fake_in_guild)
        return state

    def _store(self, mid, tokens):
        return [db.add_mapping_token(mid, t) for t in tokens]

    @pytest.mark.asyncio
    async def test_verify_keeps_stored_order(self, client, monkeypatch):
        mid = self._mapping()
        self._store(mid, [f"tok{i:08d}" for i in range(6)])
        rows = db.list_mapping_tokens(mid)
        stored_ids = [r["token_id"] for r in rows]
        tokens = [r["token_value"] for r in rows]
        self._proxies(monkeypatch, {t: f"http://p{i % 3}.test:1" for i, t in enumerate(tokens)})
        # Later tokens finish first, and only even ones are members.
        self._tracker(
            monkeypatch,
            delay=lambda t: 0.06 - 0.01 * tokens.index(t),
            member={t for i, t in enumerate(tokens) if i % 2 == 0},
        )

        resp = await client.post(f"/api/guild-mappings/{mid}/user-tokens/verify")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 6
        assert body["working"] == 3
        assert body["broken"] == 3
        assert [r["id"] for r in body["results"]] == stored_ids
        assert [r["ok"] for r in body["results"]] == [True, False] * 3
        assert set(body["results"][0]) == {"id", "masked", "username", "ok"}

    @pytest.mark.asyncio
    async def test_verify_serialises_per_proxy_and_overlaps_across_proxies(self, client, monkeypatch):
        tokens = [f"tok{i:08d}" for i in range(6)]
        proxy_of = {t: ("http://a.test:1" if i < 3 else "http://b.test:1") for i, t in enumerate(tokens)}
        self._proxies(monkeypatch, proxy_of)
        state = self._tracker(monkeypatch)
        mid = self._mapping()
        self._store(mid, tokens)

        resp = await client.post(f"/api/guild-mappings/{mid}/user-tokens/verify")
        assert resp.json()["working"] == 6
        # Never two checks at once on one proxy, but the two proxies did run together.
        assert state["max_per_proxy"] == {"http://a.test:1": 1, "http://b.test:1": 1}
        assert state["max"] == 2

    @pytest.mark.asyncio
    async def test_verify_respects_the_concurrency_cap(self, client, monkeypatch):
        import copycord.admin.app as app_mod

        monkeypatch.setattr(app_mod, "TOKEN_CHECK_CONCURRENCY", 3)
        tokens = [f"tok{i:08d}" for i in range(10)]
        self._proxies(monkeypatch, {t: f"http://p{i}.test:1" for i, t in enumerate(tokens)})
        state = self._tracker(monkeypatch)
        mid = self._mapping()
        self._store(mid, tokens)

        resp = await client.post(f"/api/guild-mappings/{mid}/user-tokens/verify")
        assert resp.json()["working"] == 10
        assert state["max"] == 3

    @pytest.mark.asyncio
    async def test_verify_one_failure_does_not_abort_the_rest(self, client, monkeypatch):
        mid = self._mapping()
        self._store(mid, [f"tok{i:08d}" for i in range(4)])
        tokens = [r["token_value"] for r in db.list_mapping_tokens(mid)]
        self._proxies(monkeypatch, {t: "http://a.test:1" for t in tokens})
        state = self._tracker(monkeypatch, delay=0, fail={tokens[1]})

        resp = await client.post(f"/api/guild-mappings/{mid}/user-tokens/verify")
        assert resp.status_code == 200
        body = resp.json()
        assert [r["ok"] for r in body["results"]] == [True, False, True, True]
        assert (body["working"], body["broken"]) == (3, 1)
        assert state["calls"] == tokens

    @pytest.mark.asyncio
    async def test_verify_without_proxies_is_refused(self, client, monkeypatch):
        self._proxies(monkeypatch, {})
        state = self._tracker(monkeypatch)
        mid = self._mapping()
        self._store(mid, ["tokaaaaaaaa"])

        resp = await client.post(f"/api/guild-mappings/{mid}/user-tokens/verify")
        assert resp.status_code == 400
        assert resp.json() == {"ok": False, "error": PROXIES_REQUIRED}
        assert state["calls"] == []

    @pytest.mark.asyncio
    async def test_verify_use_proxies_true_without_proxies_is_refused(self, client, monkeypatch):
        self._proxies(monkeypatch, {})
        state = self._tracker(monkeypatch)
        mid = self._mapping({"USER_TOKEN_USE_PROXIES": False})
        self._store(mid, ["tokaaaaaaaa"])

        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/verify", json={"use_proxies": True}
        )
        assert resp.status_code == 400
        assert resp.json() == {"ok": False, "error": PROXIES_REQUIRED}
        assert state["calls"] == []

    @pytest.mark.asyncio
    async def test_verify_use_proxies_false_checks_direct(self, client, monkeypatch):
        self._no_proxy_lookups(monkeypatch)
        state = self._tracker(monkeypatch, delay=0)
        mid = self._mapping()
        self._store(mid, ["tokaaaaaaaa", "tokbbbbbbbb"])

        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/verify", json={"use_proxies": False}
        )
        assert resp.status_code == 200
        assert resp.json()["working"] == 2
        assert set(state["max_per_proxy"]) == {None}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("use_proxies", [True, False])
    async def test_verify_without_a_body_follows_the_mapping(self, client, monkeypatch, use_proxies):
        import copycord.admin.app as app_mod

        if use_proxies:
            self._proxies(monkeypatch, {"tokaaaaaaaa": "http://a.test:1"})
        else:
            self._no_proxy_lookups(monkeypatch)
        state = self._tracker(monkeypatch, delay=0)
        mid = self._mapping({"USER_TOKEN_USE_PROXIES": use_proxies})
        self._store(mid, ["tokaaaaaaaa"])
        assert app_mod._mapping_uses_proxies(mid) is use_proxies

        resp = await client.post(f"/api/guild-mappings/{mid}/user-tokens/verify")
        assert resp.status_code == 200
        assert resp.json()["working"] == 1
        assert set(state["max_per_proxy"]) == {"http://a.test:1" if use_proxies else None}

    @pytest.mark.asyncio
    async def test_verify_direct_keeps_order_isolates_failures_and_caps_concurrency(self, client, monkeypatch):
        import copycord.admin.app as app_mod

        self._no_proxy_lookups(monkeypatch)
        mid = self._mapping()
        self._store(mid, [f"tok{i:08d}" for i in range(10)])
        rows = db.list_mapping_tokens(mid)
        stored_ids = [r["token_id"] for r in rows]
        tokens = [r["token_value"] for r in rows]
        state = self._tracker(
            monkeypatch,
            delay=lambda t: 0.05 - 0.004 * tokens.index(t),
            fail={tokens[3]},
            member=set(tokens) - {tokens[6]},
        )

        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/verify", json={"use_proxies": False}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert [r["id"] for r in body["results"]] == stored_ids
        assert [r["ok"] for r in body["results"]] == [i not in (3, 6) for i in range(10)]
        assert (body["working"], body["broken"]) == (8, 2)
        assert sorted(state["calls"]) == sorted(tokens)
        assert state["max"] == app_mod.DIRECT_CHECK_CONCURRENCY == 3

    @pytest.mark.asyncio
    async def test_add_and_bulk_add_with_the_setting_off_need_no_proxies(self, client, monkeypatch):
        self._no_proxy_lookups(monkeypatch)
        state = self._tracker(monkeypatch, delay=0)
        mid = self._mapping({"USER_TOKEN_USE_PROXIES": False})

        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens", json={"token": "tokaaaaaaaa"}
        )
        assert resp.status_code == 200, resp.text
        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/bulk",
            json={"tokens": "tokbbbbbbbb\ntokcccccccc"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["added"] == 2
        assert set(state["max_per_proxy"]) == {None}
        assert {r["token_value"] for r in db.list_mapping_tokens(mid)} == {
            "tokaaaaaaaa", "tokbbbbbbbb", "tokcccccccc"
        }

    @pytest.mark.asyncio
    async def test_bulk_add_runs_in_parallel_and_keeps_input_order(self, client, monkeypatch):
        import copycord.admin.app as app_mod

        tokens = [f"tok{i:08d}" for i in range(6)]
        proxy_of = {t: ("http://a.test:1" if i % 2 else "http://b.test:1") for i, t in enumerate(tokens)}
        self._proxies(monkeypatch, proxy_of)
        state = self._tracker(monkeypatch, fail={tokens[2]}, member=set(tokens) - {tokens[4]})
        mid = self._mapping()
        self._store(mid, [tokens[5]])

        resp = await client.post(
            f"/api/guild-mappings/{mid}/user-tokens/bulk", json={"tokens": "\n".join(tokens)}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 6
        assert body["added"] == 3
        assert [r["status"] for r in body["results"]] == [
            "added", "added", "error", "added", "invalid", "duplicate"
        ]
        assert [r["masked"] for r in body["results"]] == [app_mod._mask_token(t) for t in tokens]
        assert state["max_per_proxy"] == {"http://a.test:1": 1, "http://b.test:1": 1}
        assert state["max"] == 2
        stored = {r["token_value"] for r in db.list_mapping_tokens(mid)}
        assert stored == {tokens[5], tokens[0], tokens[1], tokens[3]}
