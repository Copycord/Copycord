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


class _FakeRateLimit:
    async def acquire(self, *a, **k):
        return None

    def penalize(self, *a, **k):
        return None


class TestSelectionStrategies:

    def test_round_robin_rotates_evenly(self):
        s = _make_sender()
        toks = [{"token_id": "a"}, {"token_id": "b"}, {"token_id": "c"}]
        firsts = [
            s._order_tokens(toks, 1, "round_robin", None)[0]["token_id"]
            for _ in range(6)
        ]
        assert firsts == ["a", "b", "c", "a", "b", "c"]

    def test_round_robin_independent_per_channel(self):
        s = _make_sender()
        toks = [{"token_id": "a"}, {"token_id": "b"}]
        # Channel 1 advances without affecting channel 2's rotation.
        s._order_tokens(toks, 1, "round_robin", None)
        assert s._order_tokens(toks, 2, "round_robin", None)[0]["token_id"] == "a"

    def test_sticky_author_is_consistent(self):
        s = _make_sender()
        toks = [{"token_id": "a"}, {"token_id": "b"}, {"token_id": "c"}]
        f1 = s._order_tokens(toks, 1, "sticky_author", "user123")[0]["token_id"]
        f2 = s._order_tokens(toks, 1, "sticky_author", "user123")[0]["token_id"]
        assert f1 == f2

    def test_sticky_author_spreads_across_authors(self):
        s = _make_sender()
        toks = [{"token_id": "a"}, {"token_id": "b"}, {"token_id": "c"}]
        firsts = {
            s._order_tokens(toks, 1, "sticky_author", f"u{i}")[0]["token_id"]
            for i in range(30)
        }
        assert len(firsts) > 1

    def test_sticky_author_distinct_authors_get_distinct_tokens(self):
        # Regression: different authors must not all land on the same account
        # when there are enough tokens to go around.
        s = _make_sender()
        toks = [{"token_id": "a"}, {"token_id": "b"}, {"token_id": "c"}]
        picks = [
            s._order_tokens(toks, 1, "sticky_author", a)[0]["token_id"]
            for a in ("userA", "userB", "userC")
        ]
        assert len(set(picks)) == 3
        # And each author keeps its assignment on repeat.
        assert s._order_tokens(toks, 1, "sticky_author", "userA")[0][
            "token_id"
        ] == picks[0]

    def test_sticky_author_scoped_per_mapping(self):
        s = _make_sender()
        toks_x = [{"token_id": "x1"}, {"token_id": "x2"}]
        toks_y = [{"token_id": "y1"}, {"token_id": "y2"}]
        # Same author in two mappings resolves within each mapping's own tokens.
        px = s._order_tokens(toks_x, 1, "sticky_author", "u", "mapX")[0]["token_id"]
        py = s._order_tokens(toks_y, 1, "sticky_author", "u", "mapY")[0]["token_id"]
        assert px in ("x1", "x2")
        assert py in ("y1", "y2")


@pytest.mark.asyncio
async def test_links_only_skips_upload(monkeypatch):
    class _DB:
        def get_enabled_mapping_tokens(self, mapping_id):
            return [{"token_id": "a", "token_value": "ta"}]

        def increment_mapping_token_usage(self, tid):
            return None

    captured = {}

    async def fake_send(self, token, channel_id, text, attachments, *, typing=False):
        captured["attachments"] = attachments
        return True

    monkeypatch.setattr(UserTokenSender, "_send_with_token", fake_send)

    s = UserTokenSender(
        db=_DB(),
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )

    ok = await s.send(
        mapping_id="m",
        target_channel_id=1,
        content="hi",
        attachments=[{"url": "http://x/a.png", "filename": "a.png"}],
        links_only=True,
    )
    assert ok is True
    # Links-only means nothing is handed to the uploader.
    assert captured["attachments"] == []


@pytest.mark.asyncio
async def test_forced_token_id_is_tried_first(monkeypatch):
    """When the identity manager pre-selects a token, that account posts —
    regardless of the strategy ordering."""

    class _DB:
        def get_enabled_mapping_tokens(self, mapping_id):
            return [
                {"token_id": "a", "token_value": "ta"},
                {"token_id": "b", "token_value": "tb"},
                {"token_id": "c", "token_value": "tc"},
            ]

        def increment_mapping_token_usage(self, tid):
            return None

    used = {}

    async def fake_send(self, token, channel_id, text, attachments, *, typing=False):
        used["token"] = token
        return True

    monkeypatch.setattr(UserTokenSender, "_send_with_token", fake_send)

    s = UserTokenSender(
        db=_DB(),
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )

    ok = await s.send(
        mapping_id="m",
        target_channel_id=1,
        content="hi",
        strategy="round_robin",
        forced_token_id="c",
    )
    assert ok is True
    assert used["token"] == "tc"


@pytest.mark.asyncio
async def test_uploaded_attachment_url_stripped_from_text(monkeypatch):
    """Regression: when an attachment is re-uploaded as a file (links_only off),
    its URL must be removed from the text so it isn't shown as a link *and* a
    file."""
    s = _make_sender()
    url = "http://cdn.example/att.png?ex=abc"

    async def fake_prepare(self, session, attachments):
        # Report the attachment as successfully uploaded.
        return [("att.png", b"data", "image/png")], {url}

    monkeypatch.setattr(UserTokenSender, "_prepare_files", fake_prepare)

    captured = {}

    def fake_multipart(self, content, files):
        captured["content"] = content
        return "FORM"

    monkeypatch.setattr(UserTokenSender, "_build_multipart", fake_multipart)

    class _Resp:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Session:
        def post(self, *a, **k):
            return _Resp()

    s._session_provider = lambda: _Session()

    ok = await s._send_with_token(
        "tok", 1, f"lol\n{url}", [{"url": url, "filename": "att.png"}]
    )
    assert ok is True
    # The link is gone; only the message text remains (the file carries it).
    assert url not in captured["content"]
    assert captured["content"].strip() == "lol"


@pytest.mark.asyncio
async def test_pace_immediate_when_no_delay():
    import time as _t

    s = _make_sender()
    t0 = _t.monotonic()
    for _ in range(5):
        await s._pace_channel(100, 0, 0)
    assert _t.monotonic() - t0 < 0.05


@pytest.mark.asyncio
async def test_pace_spaces_consecutive_messages():
    import time as _t

    s = _make_sender()
    # First message to a channel is immediate; the next must wait ~the delay
    # instead of firing in the same burst.
    await s._pace_channel(100, 0.05, 0.05)
    t0 = _t.monotonic()
    await s._pace_channel(100, 0.05, 0.05)
    assert _t.monotonic() - t0 >= 0.04


@pytest.mark.asyncio
async def test_pace_is_independent_per_channel():
    import time as _t

    s = _make_sender()
    await s._pace_channel(100, 0.05, 0.05)
    # A different channel is not throttled by channel 100's timer.
    t0 = _t.monotonic()
    await s._pace_channel(200, 0.05, 0.05)
    assert _t.monotonic() - t0 < 0.03


@pytest.mark.asyncio
async def test_no_consecutive_repeat_same_channel(monkeypatch):
    """With multiple tokens, the same account should not be picked twice in a
    row for one channel."""

    class _DB:
        def get_enabled_mapping_tokens(self, mapping_id):
            return [
                {"token_id": "a", "token_value": "ta", "username": "A"},
                {"token_id": "b", "token_value": "tb", "username": "B"},
                {"token_id": "c", "token_value": "tc", "username": "C"},
            ]

        def increment_mapping_token_usage(self, tid):
            return None

    used = []

    async def fake_send_with_token(
        self, token, channel_id, text, attachments, *, typing=False
    ):
        # Record which token actually sent (order[0] always succeeds here).
        used.append(token)
        return True

    monkeypatch.setattr(
        UserTokenSender, "_send_with_token", fake_send_with_token
    )

    s = UserTokenSender(
        db=_DB(),
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )

    for _ in range(30):
        ok = await s.send(
            mapping_id="m",
            target_channel_id=555,
            content="hi",
            embeds=None,
            attachments=None,
        )
        assert ok is True

    # No two consecutive sends into the same channel used the same token.
    assert all(used[i] != used[i + 1] for i in range(len(used) - 1))


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
