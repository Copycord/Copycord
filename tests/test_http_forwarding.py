"""
The generic HTTP forwarding provider: the JSON it posts, how it treats each
response class, and the admin API's validation and header masking.

Delivery runs against a real local aiohttp server.
"""
import asyncio
import json
import logging
import os
import tempfile
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

_tmpdir = tempfile.mkdtemp()
os.environ.setdefault("DATA_DIR", _tmpdir)
os.environ.setdefault("DB_PATH", os.path.join(_tmpdir, "test.db"))
os.environ.setdefault("LOG_LEVEL", "WARNING")

from copycord.admin import app as app_mod  # noqa: E402
from copycord.admin.app import app, db  # noqa: E402
from copycord.client.forwarding import (  # noqa: E402
    ForwardingFilters,
    ForwardingManager,
    ForwardingRule,
    RetryableForwardingError,
    build_http_payload,
)
from httpx import ASGITransport, AsyncClient  # noqa: E402

SECRET = "Bearer s3cr3t-token-value"
MESSAGE_ID = 1234567890123456789


def _rule(url, headers=None):
    config = {"url": url}
    if headers is not None:
        config["headers"] = headers
    return ForwardingRule(
        rule_id="rule-http",
        guild_id=5001,
        label="http",
        provider="http",
        enabled=True,
        config=config,
        filters=ForwardingFilters.from_dict({}),
    )


def _attrs():
    return {
        "message_id": MESSAGE_ID,
        "guild_id": 5001,
        "channel_id": 4001,
        "channel_name": "general",
        "author_id": 6001,
        "author_name": "Tunmise",
        "is_bot": False,
        "content": "hello",
        "embeds": [{"title": "t"}],
        "attachments": [
            {"url": "https://cdn.example/a.png", "filename": "a.png", "size": 1},
            {"url": "", "filename": "skipped.txt"},
        ],
    }


class _Db:
    def __init__(self):
        self.events = []

    def record_forwarding_event(self, **kw):
        self.events.append(kw)


def _server(responses, seen):
    """Answers each POST with the next (status, headers) from responses."""

    async def handle(request):
        seen.append(
            {
                "path": request.path,
                "query": request.query_string,
                "headers": dict(request.headers),
                "body": await request.read(),
            }
        )
        if request.path == "/landing":
            return web.Response(status=200, text="redirect followed")
        status, headers = responses.pop(0) if responses else (200, {})
        return web.Response(status=status, headers=headers, text="body")

    a = web.Application()
    a.router.add_route("*", "/{tail:.*}", handle)
    return a


def _deliver(responses, *, path="/hook?token=qs-secret", headers=None, timeout_server=False):
    """Run one _send_http against a local server; returns (outcome, seen, db, mgr)."""
    seen = []

    async def main():
        server = TestServer(_server(responses, seen))
        await server.start_server()
        mgr = ForwardingManager(config=None, db=_Db(), ws=None)
        outcome = None
        try:
            url = str(server.make_url(path))
            async with aiohttp.ClientSession() as session:
                try:
                    await mgr._send_http(
                        rule=_rule(url, headers),
                        message_id=str(MESSAGE_ID),
                        attrs=_attrs(),
                        session=session,
                    )
                except RetryableForwardingError as e:
                    outcome = e
        finally:
            await server.close()
        return outcome, mgr

    outcome, mgr = asyncio.run(main())
    return outcome, seen, mgr.db, mgr


# Payload


def test_payload_shape():
    p = build_http_payload(_rule("https://x.example/h"), _attrs())
    assert p == {
        "version": 1,
        "rule_id": "rule-http",
        "message_id": str(MESSAGE_ID),
        "guild": {"id": "5001", "name": None},
        "channel": {"id": "4001", "name": "general"},
        "author": {"id": "6001", "name": "Tunmise", "bot": False},
        "content": "hello",
        "embeds": [{"title": "t"}],
        "attachments": [{"url": "https://cdn.example/a.png", "filename": "a.png"}],
        # Decoded from the message id snowflake.
        "timestamp": "2024-04-29T18:12:02.167000+00:00",
    }


def test_payload_tolerates_missing_attrs():
    p = build_http_payload(_rule("https://x.example/h"), {})
    assert p["message_id"] is None
    assert p["timestamp"] is None
    assert p["guild"]["id"] == "5001"
    assert p["attachments"] == [] and p["embeds"] == []


# Delivery


def test_success_posts_json_with_headers(caplog):
    caplog.set_level(logging.DEBUG)
    outcome, seen, fake_db, _ = _deliver([(204, {})], headers={"Authorization": SECRET})
    assert outcome is None
    assert len(seen) == 1
    req = seen[0]
    assert req["path"] == "/hook" and req["query"] == "token=qs-secret"
    assert req["headers"]["Authorization"] == SECRET
    assert req["headers"]["Content-Type"] == "application/json"
    body = json.loads(req["body"])
    assert body["version"] == 1 and body["rule_id"] == "rule-http"
    assert body["message_id"] == str(MESSAGE_ID)
    assert fake_db.events and fake_db.events[0]["provider"] == "http"
    assert SECRET not in caplog.text
    assert "qs-secret" not in caplog.text


def test_429_is_retryable_with_retry_after():
    outcome, seen, fake_db, _ = _deliver([(429, {"Retry-After": "7"})])
    assert isinstance(outcome, RetryableForwardingError)
    assert outcome.status == 429
    assert outcome.delay == 7.0
    assert len(seen) == 1
    assert fake_db.events == []


@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_is_retryable(status):
    outcome, seen, _, _ = _deliver([(status, {})])
    assert isinstance(outcome, RetryableForwardingError)
    assert outcome.status == status


def test_network_error_is_retryable():
    async def main():
        mgr = ForwardingManager(config=None, db=_Db(), ws=None)
        # Nothing listens on port 1.
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RetryableForwardingError) as ei:
                await mgr._send_http(
                    rule=_rule("http://127.0.0.1:1/hook"),
                    message_id="1",
                    attrs=_attrs(),
                    session=session,
                )
        assert ei.value.status is None

    asyncio.run(main())


@pytest.mark.parametrize("status", [400, 401, 404])
def test_4xx_is_not_retried(status, caplog):
    caplog.set_level(logging.WARNING)
    outcome, seen, fake_db, _ = _deliver(
        [(status, {}), (200, {})], headers={"X-Api-Key": SECRET}
    )
    assert outcome is None
    assert len(seen) == 1
    assert fake_db.events == []
    assert "not retrying" in caplog.text
    assert SECRET not in caplog.text
    assert "qs-secret" not in caplog.text


def test_redirect_is_not_followed():
    outcome, seen, fake_db, _ = _deliver([(302, {"Location": "/landing"})])
    assert outcome is None
    assert [r["path"] for r in seen] == ["/hook"]
    assert fake_db.events == []


def test_worker_requeues_retryable_http_failure():
    async def main():
        mgr = ForwardingManager(config=None, db=_Db(), ws=None)
        mgr._maybe_retry = AsyncMock()
        mgr._send_http = AsyncMock(side_effect=RetryableForwardingError("x", delay=3))
        rule = _rule("https://x.example/h")
        assert mgr._queue_for_rule(rule) == "http"
        await mgr._dispatch_forwarding(rule, _attrs())
        q = mgr._queues["http"]
        assert q.qsize() == 1
        q.put_nowait(None)
        await mgr._provider_worker(provider="http", worker_idx=0, queue=q)
        mgr._send_http.assert_awaited_once()
        mgr._maybe_retry.assert_awaited_once()
        assert mgr._maybe_retry.await_args.kwargs["delay_override"] == 3

    asyncio.run(main())


def test_parse_rule_keeps_webhook_as_discord_and_accepts_http():
    mgr = ForwardingManager.__new__(ForwardingManager)
    mgr.log = logging.getLogger("test")
    r = mgr._parse_rule(
        {"rule_id": "r", "guild_id": 1, "provider": "http", "config": {"url": "https://a.example/x"}}
    )
    assert r is not None and r.provider == "http"
    assert mgr._parse_rule(
        {"rule_id": "r", "guild_id": 1, "provider": "http", "config": {"url": "ftp://a.example/x"}}
    ) is None
    w = mgr._parse_rule(
        {
            "rule_id": "w",
            "guild_id": 1,
            "provider": "webhook",
            "config": {"url": "https://discord.com/api/webhooks/1/abc"},
        }
    )
    assert w is not None and w.provider == "discord"


# Admin API


@pytest.fixture()
def client(monkeypatch):
    from copycord.admin import auth

    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(
        app_mod, "_ws_cmd", AsyncMock(return_value={"ok": True})
    )
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Origin": "http://test", "X-Copycord-CSRF": "1"},
        cookies={auth.ADMIN_COOKIE_NAME: auth.make_session_token()},
    )


@pytest.fixture()
def cleanup():
    yield
    db.delete_message_forward_rule("fwd-http")


def _save_payload(config, enabled=True):
    return {
        "rule_id": "fwd-http",
        "guild_id": "123",
        "label": "http",
        "provider": "http",
        "enabled": enabled,
        "config": config,
    }


@pytest.mark.parametrize(
    "config,needle",
    [
        ({}, "endpoint URL is required"),
        ({"url": "ftp://a.example/x"}, "http:// or https://"),
        ({"url": "javascript:alert(1)"}, "http:// or https://"),
        ({"url": "https://a.example", "headers": ["x"]}, "headers must be an object"),
        ({"url": "https://a.example", "headers": {"X-A": 1}}, "must be a string"),
        ({"url": "https://a.example", "headers": {"bad name": "v"}}, "invalid header name"),
        ({"url": "https://a.example", "headers": {"X-A": "a\r\nB: c"}}, "line breaks"),
        ({"url": "https://a.example", "headers": {"X-A": "v" * 5000}}, "longer than"),
        (
            {"url": "https://a.example", "headers": {f"X-{i}": "v" for i in range(21)}},
            "at most 20 headers",
        ),
        ({"url": "https://a.example", "headers": {"Content-Type": "text/plain"}}, "Content-Type"),
    ],
)
@pytest.mark.asyncio
async def test_validation_errors(client, cleanup, config, needle):
    resp = await client.post("/api/forwarding", json=_save_payload(config))
    assert resp.status_code == 400
    assert needle in resp.text
    assert db.get_message_forwarding_rule("fwd-http") is None

    resp = await client.post(
        "/api/forwarding/validate", json={"provider": "http", "config": config}
    )
    assert resp.status_code == 400
    assert any(needle in e for e in resp.json()["errors"])


@pytest.mark.asyncio
async def test_validate_accepts_good_config(client):
    resp = await client.post(
        "/api/forwarding/validate",
        json={
            "provider": "http",
            "config": {"url": "https://a.example/x", "headers": {"Authorization": SECRET}},
        },
    )
    assert resp.status_code == 200 and resp.json()["ok"] is True


@pytest.mark.asyncio
async def test_header_values_are_masked_and_mask_keeps_stored_value(client, cleanup):
    cfg = {"url": "https://a.example/x", "headers": {"Authorization": SECRET, "X-Other": "o"}}
    resp = await client.post("/api/forwarding", json=_save_payload(cfg))
    assert resp.status_code == 200, resp.text

    resp = await client.get("/api/forwarding", params={"guild_id": "123"})
    assert resp.status_code == 200
    assert SECRET not in resp.text
    item = next(i for i in resp.json()["items"] if i["rule_id"] == "fwd-http")
    masked = item["config"]["headers"]
    assert masked == {
        "Authorization": app_mod.FORWARDING_HTTP_HEADER_MASK,
        "X-Other": app_mod.FORWARDING_HTTP_HEADER_MASK,
    }

    # Send the masked config back with one value changed and one header dropped.
    resp = await client.post(
        "/api/forwarding",
        json=_save_payload(
            {"url": "https://a.example/y", "headers": {"Authorization": masked["Authorization"], "X-New": "n"}}
        ),
    )
    assert resp.status_code == 200, resp.text
    stored = db.get_message_forwarding_rule("fwd-http")["config"]
    assert stored["url"] == "https://a.example/y"
    assert stored["headers"] == {"Authorization": SECRET, "X-New": "n"}


@pytest.mark.asyncio
async def test_mask_without_stored_value_is_not_saved_as_secret(client, cleanup):
    mask = app_mod.FORWARDING_HTTP_HEADER_MASK
    resp = await client.post(
        "/api/forwarding",
        json=_save_payload({"url": "https://a.example/x", "headers": {"X-A": mask}}),
    )
    assert resp.status_code == 200
    # Nothing stored to restore: the literal value is kept as sent.
    assert db.get_message_forwarding_rule("fwd-http")["config"]["headers"] == {"X-A": mask}
