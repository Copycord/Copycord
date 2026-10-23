"""
Blocked words on forwarding rules: the admin API keeps them when a rule is
saved, and the client filter built from the stored row rejects messages that
contain one.
"""
import logging
import os
import tempfile
from unittest.mock import AsyncMock

import pytest

_tmpdir = tempfile.mkdtemp()
os.environ.setdefault("DATA_DIR", _tmpdir)
os.environ.setdefault("DB_PATH", os.path.join(_tmpdir, "test.db"))
os.environ.setdefault("LOG_LEVEL", "WARNING")

from copycord.admin import app as app_mod  # noqa: E402
from copycord.admin.app import _normalize_forwarding_rule_filters, app, db  # noqa: E402
from copycord.client.forwarding import ForwardingManager  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402

RULE_ID = "fwd-blocked"
GUILD_ID = "4242"


def test_normalize_keeps_exclude_keywords():
    out = _normalize_forwarding_rule_filters(
        {"exclude_keywords": [" spam ", "", None, "giveaway"]}
    )
    assert out["exclude_keywords"] == ["spam", "giveaway"]


def test_normalize_accepts_keywords_exclude_alias():
    out = _normalize_forwarding_rule_filters({"keywords_exclude": ["spam"]})
    assert out["exclude_keywords"] == ["spam"]
    assert "keywords_exclude" not in out


def test_normalize_prefers_exclude_keywords_over_alias():
    out = _normalize_forwarding_rule_filters(
        {"exclude_keywords": ["a"], "keywords_exclude": ["b"]}
    )
    assert out["exclude_keywords"] == ["a"]


def test_normalize_splits_comma_string():
    out = _normalize_forwarding_rule_filters({"exclude_keywords": "spam, free nitro ,,x"})
    assert out["exclude_keywords"] == ["spam", "free nitro", "x"]


def test_normalize_defaults_to_empty_list():
    assert _normalize_forwarding_rule_filters({})["exclude_keywords"] == []
    assert _normalize_forwarding_rule_filters(None)["exclude_keywords"] == []
    assert (
        _normalize_forwarding_rule_filters('{"exclude_keywords": ["spam"]}')["exclude_keywords"]
        == ["spam"]
    )


@pytest.fixture()
def client(monkeypatch):
    from copycord.admin import auth

    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_mod, "_ws_cmd", AsyncMock(return_value={"ok": True}))
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Origin": "http://test", "X-Copycord-CSRF": "1"},
        cookies={auth.ADMIN_COOKIE_NAME: auth.make_session_token()},
    )


@pytest.fixture()
def cleanup():
    yield
    db.delete_message_forward_rule(RULE_ID)


def _stored_rule():
    """The rule as the client builds it from the database on reload."""
    rows = db.list_message_forwarding_rules(guild_id=None)
    row = next(r for r in rows if r["rule_id"] == RULE_ID)
    mgr = ForwardingManager.__new__(ForwardingManager)
    mgr.log = logging.getLogger("test")
    rule = mgr._parse_rule(dict(row))
    assert rule is not None
    return rule


def _attrs(content):
    return {"channel_id": 1, "author_id": 2, "content": content}


async def _save(client, filters):
    resp = await client.post(
        "/api/forwarding",
        json={
            "rule_id": RULE_ID,
            "guild_id": GUILD_ID,
            "label": "blocked",
            "provider": "http",
            "enabled": True,
            "config": {"url": "https://a.example/hook"},
            "filters": filters,
        },
    )
    assert resp.status_code == 200, resp.text


@pytest.mark.asyncio
async def test_saved_blocked_words_reject_matching_message(client, cleanup):
    await _save(client, {"keywords_exclude": "Free Nitro, giveaway"})

    stored = db.get_message_forwarding_rule(RULE_ID)["filters"]
    assert stored["exclude_keywords"] == ["Free Nitro", "giveaway"]

    resp = await client.get("/api/forwarding", params={"guild_id": GUILD_ID})
    item = next(i for i in resp.json()["items"] if i["rule_id"] == RULE_ID)
    assert item["filters"]["exclude_keywords"] == ["Free Nitro", "giveaway"]

    filters = _stored_rule().filters
    assert filters.apply(_attrs("hello everyone")) is True
    assert filters.apply(_attrs("claim your free nitro now")) is False
    assert filters.apply(_attrs("GIVEAWAY tonight")) is False


@pytest.mark.asyncio
async def test_saved_blocked_words_case_sensitive(client, cleanup):
    await _save(
        client, {"exclude_keywords": ["Free Nitro"], "case_sensitive": True}
    )

    filters = _stored_rule().filters
    assert filters.apply(_attrs("claim your Free Nitro now")) is False
    assert filters.apply(_attrs("claim your free nitro now")) is True
    assert filters.apply(_attrs("hello everyone")) is True


@pytest.mark.asyncio
async def test_blocked_words_override_matching_keywords(client, cleanup):
    await _save(
        client, {"keywords_any": ["release"], "exclude_keywords": ["leak"]}
    )

    filters = _stored_rule().filters
    assert filters.apply(_attrs("new release is out")) is True
    assert filters.apply(_attrs("release leak inside")) is False
    assert filters.apply(_attrs("unrelated chatter")) is False
