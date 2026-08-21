"""
Unit tests for the server-side user-token message sender.

These cover the pure, network-free helpers: per-token header fingerprinting
(unique per account, stable across calls) and embed flattening.
"""
import base64
import json
import types

import aiohttp
import pytest

from curl_cffi.requests.exceptions import RequestException as CurlRequestException

from server.token_sender import (
    UserTokenSender,
    SEND_OK,
    SEND_DEAD,
    SEND_UNDELIVERABLE,
    SEND_RATE_LIMITED,
    SEND_TRANSIENT,
    SEND_NO_TOKENS,
)


def _use_session(sender, provider):
    """Point a sender at a fake HTTP session.

    Two seams, because two different things reach for a session: attachment
    downloads go through ``_session_provider`` (aiohttp), while the Discord
    call itself goes through ``_tls_session`` (a per-token curl_cffi session).
    Setting only the first leaves the request hitting the real network.
    """
    sender._session_provider = provider

    async def _fake_tls(token, *, use_proxy=False):
        return provider()

    sender._tls_session = _fake_tls


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

    async def fake_send(
        self, token, channel_id, text, attachments, *,
        sticker_ids=None, use_proxy=False, reply_to=None,
    ):
        captured["attachments"] = attachments
        return SEND_OK, 111

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
    assert ok == SEND_OK
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

    async def fake_send(
        self, token, channel_id, text, attachments, *,
        sticker_ids=None, use_proxy=False, reply_to=None,
    ):
        used["token"] = token
        return SEND_OK, 111

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
    assert ok == SEND_OK
    assert used["token"] == "tc"


@pytest.mark.asyncio
async def test_sticky_exclusive_never_falls_back_to_other_tokens(monkeypatch):
    """A sticky identity must never post from a different account: when the
    forced token fails and sticky_exclusive is set, no other token is tried."""

    class _DB:
        def get_enabled_mapping_tokens(self, mapping_id):
            return [
                {"token_id": "a", "token_value": "ta"},
                {"token_id": "b", "token_value": "tb"},
            ]

        def increment_mapping_token_usage(self, tid):
            return None

    tried = []

    async def fake_send(
        self, token, channel_id, text, attachments, *,
        sticker_ids=None, use_proxy=False, reply_to=None,
    ):
        tried.append(token)
        return SEND_DEAD, None

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
        forced_token_id="a",
        sticky_exclusive=True,
    )
    # The forced account failed and no other may be tried → its failure status.
    assert ok == SEND_DEAD
    # Only the forced account was attempted — never the other token.
    assert tried == ["ta"]


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

    def fake_multipart(self, content, files, sticker_ids=None, *, reply_bits=None):
        captured["content"] = content
        return "FORM"

    monkeypatch.setattr(UserTokenSender, "_build_multipart", fake_multipart)

    class _Resp:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Session:
        async def post(self, *a, **k):
            return _Resp()

    _use_session(s, lambda: _Session())

    ok, _mid = await s._send_with_token(
        "tok", 1, f"lol\n{url}", [{"url": url, "filename": "att.png"}]
    )
    assert ok == SEND_OK
    # The link is gone; only the message text remains (the file carries it).
    assert url not in captured["content"]
    assert captured["content"].strip() == "lol"


@pytest.mark.asyncio
async def test_sticker_only_message_sends_sticker_ids():
    class _DB:
        def get_enabled_mapping_tokens(self, mapping_id):
            return [{"token_id": "a", "token_value": "ta"}]

        def increment_mapping_token_usage(self, tid):
            return None

    captured = {}

    class _Resp:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Session:
        async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
            captured["json"] = json
            return _Resp()

    s = UserTokenSender(
        db=_DB(),
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: _Session(),
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )
    _use_session(s, lambda: _Session())

    # A sticker-only message (no text/attachments) still sends via token.
    ok = await s.send(
        mapping_id="m",
        target_channel_id=1,
        content=None,
        sticker_ids=[123456789],
    )
    assert ok == SEND_OK
    assert captured["json"]["sticker_ids"] == ["123456789"]


@pytest.mark.asyncio
async def test_pace_immediate_when_no_delay():
    import time as _t

    s = _make_sender()
    t0 = _t.monotonic()
    for _ in range(5):
        await s._pace_channel(100, 0, 0)
    assert _t.monotonic() - t0 < 0.05


@pytest.mark.asyncio
async def test_pace_waits_the_delay_every_message():
    import time as _t

    s = _make_sender()
    # Every message waits its own delay before being sent.
    t0 = _t.monotonic()
    await s._pace_channel(100, 0.05, 0.05)
    assert _t.monotonic() - t0 >= 0.04


@pytest.mark.asyncio
async def test_chan_lock_serializes_same_channel():
    import asyncio as _a
    import time as _t

    s = _make_sender()

    async def hold():
        async with s._chan_lock(100):
            await _a.sleep(0.05)

    t0 = _t.monotonic()
    # Two holders of the same channel lock take turns (~0.10s), never bursting.
    await _a.gather(hold(), hold())
    assert _t.monotonic() - t0 >= 0.09


@pytest.mark.asyncio
async def test_pace_independent_channels_run_concurrently():
    import asyncio as _a
    import time as _t

    s = _make_sender()
    t0 = _t.monotonic()
    # Different channels delay at the same time, not one-after-another.
    await _a.gather(
        s._pace_channel(100, 0.05, 0.05),
        s._pace_channel(200, 0.05, 0.05),
    )
    assert _t.monotonic() - t0 < 0.09


@pytest.mark.asyncio
async def test_pace_shows_typing_for_the_delay():
    import time as _t

    posts = []

    class _Resp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Session:
        async def post(self, url, headers=None, timeout=None, **kw):
            posts.append(url)
            return _Resp()

    s = _make_sender()
    _use_session(s, lambda: _Session())
    t0 = _t.monotonic()
    # Typing enabled → the indicator fires and the message waits the delay.
    await s._pace_channel(100, 0.05, 0.05, typing=True, token="tok")
    assert _t.monotonic() - t0 >= 0.04
    assert any("/typing" in u for u in posts)


@pytest.mark.asyncio
async def test_send_serializes_typing_then_send_per_channel():
    """Regression: with typing + delay, queued messages to one channel take
    strict turns — typing → send → typing → send. A message's typing must never
    fire before the previous message is sent (which would clear the indicator)."""
    import asyncio as _a

    events = []

    class _DB:
        def get_enabled_mapping_tokens(self, mapping_id):
            return [{"token_id": "a", "token_value": "ta"}]

        def increment_mapping_token_usage(self, tid):
            return None

    class _Resp:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Session:
        async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
            events.append("type" if url.endswith("/typing") else "send")
            return _Resp()

    s = UserTokenSender(
        db=_DB(),
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: _Session(),
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )
    _use_session(s, lambda: _Session())

    async def one():
        await s.send(
            mapping_id="m",
            target_channel_id=1,
            content="hi",
            typing=True,
            min_delay=0.02,
            max_delay=0.02,
            forced_token_id="a",
            sticky_exclusive=True,
        )

    await _a.gather(one(), one())
    assert events == ["type", "send", "type", "send"]


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
        self, token, channel_id, text, attachments, *,
        sticker_ids=None, use_proxy=False, reply_to=None,
    ):
        # Record which token actually sent (order[0] always succeeds here).
        used.append(token)
        return SEND_OK, 111

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
        assert ok == SEND_OK

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
    assert ok == SEND_NO_TOKENS


class _JsonResp:
    """Minimal curl_cffi-style response returning a fixed status + JSON body."""

    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body or {}
        self.headers = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def json(self):
        return self._body

    @property
    def text(self):
        return json.dumps(self._body)


def _forum_sender(db, session):
    s = UserTokenSender(
        db=db,
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: session,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )
    _use_session(s, lambda: session)
    return s


class TestCreateForumThread:

    @pytest.mark.asyncio
    async def test_creates_thread_and_returns_id(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return [{"token_id": "a", "token_value": "ta"}]

            def increment_mapping_token_usage(self, tid):
                return None

        captured = {}

        class _Session:
            async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
                captured["url"] = url
                captured["json"] = json
                return _JsonResp(201, {"id": "998877"})

        s = _forum_sender(_DB(), _Session())
        new_id = await s.create_forum_thread(
            mapping_id="m",
            forum_channel_id=42,
            thread_name="My Thread",
            content="first post",
            applied_tag_ids=[111, 222],
        )
        assert new_id == 998877
        assert captured["url"].endswith("/channels/42/threads")
        assert captured["json"]["name"] == "My Thread"
        assert captured["json"]["message"]["content"] == "first post"
        assert captured["json"]["applied_tags"] == ["111", "222"]

    @pytest.mark.asyncio
    async def test_forced_token_is_tried_first(self):
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

        class _Session:
            async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
                # The Authorization header reveals which account is posting.
                used["auth"] = headers.get("Authorization")
                return _JsonResp(201, {"id": "5"})

        s = _forum_sender(_DB(), _Session())
        new_id = await s.create_forum_thread(
            mapping_id="m",
            forum_channel_id=1,
            thread_name="t",
            content="hi",
            strategy="round_robin",
            forced_token_id="c",
        )
        assert new_id == 5
        assert used["auth"] == "tc"

    @pytest.mark.asyncio
    async def test_empty_starter_returns_none(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return [{"token_id": "a", "token_value": "ta"}]

        posted = {"called": False}

        class _Session:
            async def post(self, *a, **k):
                posted["called"] = True
                return _JsonResp(201, {"id": "1"})

        s = _forum_sender(_DB(), _Session())
        # No text, no attachments, no stickers → webhook must create the thread.
        new_id = await s.create_forum_thread(
            mapping_id="m",
            forum_channel_id=1,
            thread_name="t",
            content=None,
        )
        assert new_id is None
        assert posted["called"] is False

    @pytest.mark.asyncio
    async def test_all_tokens_rejected_returns_none(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return [
                    {"token_id": "a", "token_value": "ta"},
                    {"token_id": "b", "token_value": "tb"},
                ]

            def increment_mapping_token_usage(self, tid):
                return None

        attempts = {"n": 0}

        class _Session:
            async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
                attempts["n"] += 1
                return _JsonResp(403, {"message": "Missing Access"})

        s = _forum_sender(_DB(), _Session())
        new_id = await s.create_forum_thread(
            mapping_id="m",
            forum_channel_id=1,
            thread_name="t",
            content="hi",
        )
        assert new_id is None
        # Every token was tried before giving up.
        assert attempts["n"] == 2

    @pytest.mark.asyncio
    async def test_sticker_only_forum_thread_includes_sticker_ids(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return [{"token_id": "a", "token_value": "ta"}]

            def increment_mapping_token_usage(self, tid):
                return None

        captured = {}

        class _Session:
            async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
                captured["json"] = json
                return _JsonResp(201, {"id": "77"})

        s = _forum_sender(_DB(), _Session())
        new_id = await s.create_forum_thread(
            mapping_id="m",
            forum_channel_id=1,
            thread_name="t",
            content=None,
            sticker_ids=[13579],
        )
        assert new_id == 77
        assert captured["json"]["message"]["sticker_ids"] == ["13579"]


class TestCreateTextThread:

    @pytest.mark.asyncio
    async def test_linked_thread_posts_to_message_threads_endpoint(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return [{"token_id": "a", "token_value": "ta"}]

            def increment_mapping_token_usage(self, tid):
                return None

        captured = {}

        class _Session:
            async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
                captured["url"] = url
                captured["json"] = json
                return _JsonResp(201, {"id": "444"})

        s = _forum_sender(_DB(), _Session())
        new_id = await s.create_text_thread(
            mapping_id="m",
            parent_channel_id=42,
            thread_name="chat",
            starter_message_id=999,
        )
        assert new_id == 444
        # Created FROM the message → id equals the message id in Discord.
        assert captured["url"].endswith("/channels/42/messages/999/threads")
        assert captured["json"]["name"] == "chat"
        # A message-linked thread must not force a standalone thread type.
        assert "type" not in captured["json"]

    @pytest.mark.asyncio
    async def test_standalone_thread_uses_channel_threads_endpoint(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return [{"token_id": "a", "token_value": "ta"}]

            def increment_mapping_token_usage(self, tid):
                return None

        captured = {}

        class _Session:
            async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
                captured["url"] = url
                captured["json"] = json
                return _JsonResp(201, {"id": "555"})

        s = _forum_sender(_DB(), _Session())
        new_id = await s.create_text_thread(
            mapping_id="m",
            parent_channel_id=42,
            thread_name="chat",
        )
        assert new_id == 555
        assert captured["url"].endswith("/channels/42/threads")
        assert captured["json"]["type"] == 11

    @pytest.mark.asyncio
    async def test_forced_token_is_tried_first(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return [
                    {"token_id": "a", "token_value": "ta"},
                    {"token_id": "b", "token_value": "tb"},
                ]

            def increment_mapping_token_usage(self, tid):
                return None

        used = {}

        class _Session:
            async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
                used["auth"] = headers.get("Authorization")
                return _JsonResp(201, {"id": "9"})

        s = _forum_sender(_DB(), _Session())
        new_id = await s.create_text_thread(
            mapping_id="m",
            parent_channel_id=1,
            thread_name="t",
            strategy="round_robin",
            forced_token_id="b",
        )
        assert new_id == 9
        assert used["auth"] == "tb"

    @pytest.mark.asyncio
    async def test_all_tokens_rejected_returns_none(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return [
                    {"token_id": "a", "token_value": "ta"},
                    {"token_id": "b", "token_value": "tb"},
                ]

            def increment_mapping_token_usage(self, tid):
                return None

        attempts = {"n": 0}

        class _Session:
            async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
                attempts["n"] += 1
                return _JsonResp(403, {"message": "Missing Access"})

        s = _forum_sender(_DB(), _Session())
        new_id = await s.create_text_thread(
            mapping_id="m",
            parent_channel_id=1,
            thread_name="t",
        )
        assert new_id is None
        assert attempts["n"] == 2

    @pytest.mark.asyncio
    async def test_no_tokens_returns_none(self):
        class _DB:
            def get_enabled_mapping_tokens(self, mapping_id):
                return []

        s = _forum_sender(_DB(), object())
        new_id = await s.create_text_thread(
            mapping_id="m",
            parent_channel_id=1,
            thread_name="t",
        )
        assert new_id is None


async def _instant_sleep(*a, **k):
    """No-op replacement for asyncio.sleep so retry/backoff tests don't wait."""
    return None


class _ScriptedSession:
    """Yields a scripted sequence of responses; ``("raise", exc)`` raises."""

    def __init__(self, script):
        self._script = list(script)
        self.calls = 0

    async def post(self, url, json=None, data=None, headers=None, timeout=None, **kw):
        self.calls += 1
        item = self._script.pop(0) if self._script else ("resp", 200, {})
        if item[0] == "raise":
            raise item[1]
        return _JsonResp(item[1], item[2])


class TestSendStatusTaxonomy:
    """A single-account send reports *why* it couldn't deliver so the sticky
    loop can tell a dead account from a transient blip from a rate limit."""

    @pytest.mark.asyncio
    async def test_401_is_dead(self):
        s = _make_sender()
        _use_session(s, lambda: _ScriptedSession([("resp", 401, {})]))
        # Revoked/invalid token → dead → caller benches it.
        assert (await s._send_with_token("t", 1, "hi", []))[0] == SEND_DEAD

    @pytest.mark.asyncio
    async def test_403_is_undeliverable(self):
        s = _make_sender()
        _use_session(
            s, lambda: _ScriptedSession([("resp", 403, {"message": "Missing Access"})])
        )
        # Can't post here, but the token itself isn't dead → try another account.
        assert (await s._send_with_token("t", 1, "hi", []))[0] == SEND_UNDELIVERABLE

    @pytest.mark.asyncio
    async def test_429_then_success_retries_and_parks_account(self, monkeypatch):
        import asyncio

        monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
        session = _ScriptedSession(
            [("resp", 429, {"retry_after": 5}), ("resp", 200, {})]
        )
        s = _make_sender()
        _use_session(s, lambda: session)
        # A 429 sleeps its retry_after then retries the SAME account (no swap).
        assert (await s._send_with_token("t", 1, "hi", []))[0] == SEND_OK
        assert session.calls == 2
        # The account was parked so its other queued messages wait too.
        assert "t" in s._token_cooldown

    @pytest.mark.asyncio
    async def test_persistent_429_is_rate_limited_not_webhooked(self, monkeypatch):
        import asyncio

        monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
        session = _ScriptedSession([("resp", 429, {"retry_after": 0.001})] * 20)
        s = _make_sender()
        _use_session(s, lambda: session)
        # Still limited after the retries → a distinct status the caller must NOT
        # webhook (it's not the token's fault).
        assert (await s._send_with_token("t", 1, "hi", []))[0] == SEND_RATE_LIMITED

    @pytest.mark.asyncio
    async def test_network_error_is_retried_then_transient(self, monkeypatch):
        import asyncio

        monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
        session = _ScriptedSession(
            [("raise", CurlRequestException("boom"))] * 20
        )
        s = _make_sender()
        _use_session(s, lambda: session)
        # Network errors are retried (never webhooked); after the cap → transient.
        assert (await s._send_with_token("t", 1, "hi", []))[0] == SEND_TRANSIENT
        assert session.calls > 1

    @pytest.mark.asyncio
    async def test_cooldown_blocks_until_elapsed(self):
        import time as _t

        s = _make_sender()
        # A parked account makes the next message wait out the cooldown.
        s._token_cooldown["t"] = _t.monotonic() + 0.05
        t0 = _t.monotonic()
        await s._await_cooldown("t")
        assert _t.monotonic() - t0 >= 0.04

    @pytest.mark.asyncio
    async def test_same_account_requests_serialize(self):
        import asyncio as _a

        # Regression: one account's messages never overlap — a rate limit on one
        # holds the others back instead of firing them all at once.
        active = {"n": 0, "max": 0}

        class _Resp:
            status_code = 200

            def json(self):
                return {}

        class _Session:
            async def post(self, *a, **k):
                active["n"] += 1
                active["max"] = max(active["max"], active["n"])
                await _a.sleep(0.02)
                active["n"] -= 1
                return _Resp()

        s = _make_sender()
        _use_session(s, lambda: _Session())
        await _a.gather(
            s._send_with_token("t", 1, "hi", []),
            s._send_with_token("t", 1, "hi", []),
        )
        assert active["max"] == 1
