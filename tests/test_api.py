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


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:

    @pytest.mark.asyncio
    async def test_health_ok(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.text == "ok"


# ---------------------------------------------------------------------------
# Event logs API
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Guild mappings API
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Mapping user tokens API
# ---------------------------------------------------------------------------

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
        async def fake_in_guild(token, gid):
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
        async def fake_in_guild(token, gid):
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

# ---------------------------------------------------------------------------
# Version endpoint
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Role logs: <home>/logs, and the old data dir location for one release
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Forwarding rules: provider validation only runs for enabled rules
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Account guild list: cache, single-flight, rate limits (Discord is faked)
# ---------------------------------------------------------------------------

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

    def get(self, url, headers=None, timeout=None):
        self.urls.append(url)
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


# ---------------------------------------------------------------------------
# Mapping save: membership checks
# ---------------------------------------------------------------------------

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
