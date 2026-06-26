"""
FastAPI endpoint tests for the Copycord admin API.

These tests use httpx AsyncClient to exercise endpoints that don't
require live Discord connections. WebSocket control commands are mocked.
"""
import os
import sys
import tempfile
from unittest.mock import AsyncMock

import pytest

# Set environment before any app imports
_tmpdir = tempfile.mkdtemp()
os.environ["DATA_DIR"] = _tmpdir
os.environ["DB_PATH"] = os.path.join(_tmpdir, "test.db")
os.environ["LOG_LEVEL"] = "WARNING"

CODE_DIR = os.path.join(os.path.dirname(__file__), "..", "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(CODE_DIR))

# Import the app — this triggers module-level init (DB, schema, etc.)
from admin.app import app, db  # noqa: E402

from httpx import AsyncClient, ASGITransport


@pytest.fixture()
def client():
    """Provide an httpx AsyncClient against the test app."""
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture(autouse=True)
def _mock_ws(monkeypatch):
    """Ensure all WebSocket control commands return a safe mock."""
    import admin.app as app_mod

    mock = AsyncMock(return_value={"ok": True, "running": False, "status": "stopped"})
    monkeypatch.setattr(app_mod, "_ws_cmd", mock)


@pytest.fixture(autouse=True)
def _clean_db():
    """Clean event_logs, guild_mappings, and forwarding rules between tests."""
    yield
    try:
        db.clear_event_logs()
        for m in db.list_guild_mappings():
            db.delete_guild_mapping(m["mapping_id"])
        for r in db.list_message_forwarding_rules():
            db.delete_message_forward_rule(r["rule_id"])
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
    async def test_get_empty(self, client):
        resp = await client.get("/api/event-logs")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["logs"] == []
        assert body["total"] == 0

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
    async def test_delete_nonexistent(self, client):
        resp = await client.delete("/api/event-logs/nonexistent")
        assert resp.status_code == 404

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

    @pytest.mark.asyncio
    async def test_delete_bulk_empty_ids(self, client):
        resp = await client.post("/api/event-logs/delete-bulk", json={"ids": []})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Guild mappings API
# ---------------------------------------------------------------------------

class TestGuildMappingsAPI:

    @pytest.mark.asyncio
    async def test_list_empty(self, client):
        resp = await client.get("/api/guild-mappings")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["mappings"] == []

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

    @pytest.mark.asyncio
    async def test_toggle_nonexistent(self, client):
        resp = await client.post("/api/guild-mappings/nope/toggle-status")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Version endpoint
# ---------------------------------------------------------------------------

class TestVersion:

    @pytest.mark.asyncio
    async def test_version_returns_200(self, client):
        resp = await client.get("/version")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Forwarding API — Discord multi-URL
# ---------------------------------------------------------------------------

class TestForwardingAPI:

    _VALID_A = "https://discord.com/api/webhooks/111/aaa"
    _VALID_B = "https://discord.com/api/webhooks/222/bbb"

    def _payload(self, **cfg_over):
        config = {"urls": [self._VALID_A, self._VALID_B]}
        config.update(cfg_over)
        return {
            "guild_id": "111",
            "label": "Multi",
            "provider": "discord",
            "enabled": True,
            "config": config,
            "filters": {},
        }

    async def _get_rule(self, client, rule_id):
        resp = await client.get("/api/forwarding")
        for item in resp.json()["items"]:
            if item["rule_id"] == rule_id:
                return item
        return None

    @pytest.mark.asyncio
    async def test_save_persists_urls(self, client):
        resp = await client.post("/api/forwarding", json=self._payload())
        assert resp.status_code == 200
        rule_id = resp.json()["rule_id"]

        item = await self._get_rule(client, rule_id)
        assert item is not None
        cfg = item["config"]
        if isinstance(cfg, str):
            import json
            cfg = json.loads(cfg)
        assert cfg["urls"] == [self._VALID_A, self._VALID_B]
        assert "url" not in cfg

    @pytest.mark.asyncio
    async def test_legacy_url_accepted_and_normalized(self, client):
        payload = self._payload()
        payload["config"] = {"url": self._VALID_A}
        resp = await client.post("/api/forwarding", json=payload)
        assert resp.status_code == 200
        rule_id = resp.json()["rule_id"]

        item = await self._get_rule(client, rule_id)
        cfg = item["config"]
        if isinstance(cfg, str):
            import json
            cfg = json.loads(cfg)
        assert cfg["urls"] == [self._VALID_A]

    @pytest.mark.asyncio
    async def test_zero_urls_rejected(self, client):
        payload = self._payload()
        payload["config"] = {"urls": []}
        resp = await client.post("/api/forwarding", json=payload)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_invalid_url_rejected(self, client):
        payload = self._payload()
        payload["config"] = {"urls": [self._VALID_A, "https://example.com/x"]}
        resp = await client.post("/api/forwarding", json=payload)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_caps_at_ten(self, client):
        many = [f"https://discord.com/api/webhooks/{i}/tok" for i in range(12)]
        payload = self._payload()
        payload["config"] = {"urls": many}
        resp = await client.post("/api/forwarding", json=payload)
        assert resp.status_code == 200
        rule_id = resp.json()["rule_id"]

        item = await self._get_rule(client, rule_id)
        cfg = item["config"]
        if isinstance(cfg, str):
            import json
            cfg = json.loads(cfg)
        assert len(cfg["urls"]) == 10
