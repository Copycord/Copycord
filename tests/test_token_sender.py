"""
Unit tests for the server-side user-token message sender.

These cover the pure, network-free helpers: per-token header fingerprinting
(unique per account, stable across calls) and embed flattening.
"""
import base64
import json
import types

import pytest

from server.token_sender import UserTokenSender


def _make_sender():
    return UserTokenSender(
        db=None,
        ratelimit=None,
        action_type=None,
        session_provider=lambda: None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )


class TestHeaderFingerprint:

    def test_headers_have_auth_and_super_properties(self):
        s = _make_sender()
        h = s._build_headers("token-abc")
        assert h["Authorization"] == "token-abc"
        assert "X-Super-Properties" in h
        assert "User-Agent" in h
        # Super properties decode to JSON with a client build number.
        props = json.loads(base64.b64decode(h["X-Super-Properties"]))
        assert props["browser"] == "Discord Client"
        assert "client_build_number" in props
        assert props["browser_user_agent"] == h["User-Agent"]

    def test_fingerprint_stable_per_token(self):
        s = _make_sender()
        first = s._build_headers("stable-token")
        second = s._build_headers("stable-token")
        assert first["User-Agent"] == second["User-Agent"]
        assert first["X-Super-Properties"] == second["X-Super-Properties"]

    def test_fingerprint_unique_across_tokens(self):
        s = _make_sender()
        # Different tokens should not share an identical device fingerprint.
        sp = {
            s._build_headers(f"token-{i}")["X-Super-Properties"] for i in range(12)
        }
        assert len(sp) > 1

    def test_fingerprint_deterministic_across_instances(self):
        # A given account looks the same even after a process restart.
        a = _make_sender()._build_headers("same")
        b = _make_sender()._build_headers("same")
        assert a["X-Super-Properties"] == b["X-Super-Properties"]


class TestFlattenEmbed:

    def test_flatten_collects_text_and_links(self):
        field = types.SimpleNamespace(name="Field", value="Val")
        embed = types.SimpleNamespace(
            author=types.SimpleNamespace(name="Author"),
            title="Title",
            url="https://example.com",
            description="Body",
            fields=[field],
            image=types.SimpleNamespace(url="https://img/x.png"),
            thumbnail=None,
            footer=types.SimpleNamespace(text="Footer"),
        )
        out = UserTokenSender._flatten_embed(embed)
        assert "Author" in out
        assert "**Title**" in out
        assert "https://example.com" in out
        assert "Body" in out
        assert "Field" in out and "Val" in out
        assert "https://img/x.png" in out
        assert "Footer" in out

    def test_compose_text_appends_embed(self):
        s = _make_sender()
        embed = types.SimpleNamespace(
            author=None,
            title="Hi",
            url=None,
            description=None,
            fields=[],
            image=None,
            thumbnail=None,
            footer=None,
        )
        text = s._compose_text("hello", [embed])
        assert text.startswith("hello")
        assert "**Hi**" in text


@pytest.mark.asyncio
async def test_send_returns_false_without_tokens():
    class _DB:
        def get_enabled_mapping_tokens(self, mapping_id):
            return []

    s = UserTokenSender(
        db=_DB(),
        ratelimit=None,
        action_type=None,
        session_provider=lambda: None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )
    ok = await s.send(
        mapping_id="m",
        target_channel_id=123,
        content="hello",
        embeds=None,
        attachments=None,
    )
    assert ok is False
