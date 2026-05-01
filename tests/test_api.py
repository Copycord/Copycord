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
