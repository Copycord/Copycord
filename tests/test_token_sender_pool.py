"""UserTokenSender with the warm gateway pool, driven by a fake pool (no Discord)."""

import asyncio
import types

import pytest

from copycord.server import token_sender
from copycord.server.token_sender import (
    ONLINE_POLL,
    ONLINE_WAIT,
    SEND_NO_TOKENS,
    SEND_OK,
    SEND_TRANSIENT,
    UserTokenSender,
)
from tests.test_token_sender import _FakeRateLimit

FULL_WAIT = int(ONLINE_WAIT / ONLINE_POLL)


@pytest.fixture(autouse=True)
def polls(monkeypatch):
    """The online wait's sleep, without real time; each hook gets the poll count."""
    state = types.SimpleNamespace(count=0, hooks=[])

    async def fake_sleep(seconds):
        assert seconds == ONLINE_POLL
        state.count += 1
        for hook in state.hooks:
            hook(state.count)
        await asyncio.sleep(0)

    monkeypatch.setattr(token_sender, "_sleep", fake_sleep)
    return state


class FakePool:
    """The slice of TokenSessionPool the sender reads."""

    def __init__(self, online=(), dead=()):
        self.online = set(online)
        self.dead = set(dead)
        self.acquired = []
        self.reaped = 0

    def online_ids(self):
        return set(self.online)

    def dead_ids(self):
        return set(self.dead)

    async def acquire_online(self, candidates):
        self.acquired.append(list(candidates))
        for token_id in candidates:
            if token_id in self.online:
                return token_id
        return None

    async def reap_idle(self, idle_ttl=3600):
        self.reaped += 1
        return 0


class _DB:
    """Always returns every token, as if a disable write had not landed yet."""

    def __init__(self, ids):
        self.tokens = [{"token_id": i, "token_value": f"t{i}"} for i in ids]
        self.disabled = []

    def get_enabled_mapping_tokens(self, mapping_id):
        return [dict(t) for t in self.tokens]

    def increment_mapping_token_usage(self, tid):
        return None

    def set_mapping_token_enabled(self, token_id, enabled):
        self.disabled.append((token_id, enabled))


def _sender(db, pool=None, warnings=None):
    return UserTokenSender(
        db=db,
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: (warnings.append(a) if warnings is not None else None),
            exception=lambda *a, **k: None,
        ),
        pool=pool,
    )


def _record_sends(monkeypatch, status=SEND_OK):
    """Patch the REST send; returns the list of token values tried, in order."""
    tried = []

    async def fake_send(
        self,
        token,
        channel_id,
        text,
        attachments,
        *,
        sticker_ids=None,
        use_proxy=False,
        reply_to=None,
        guild_id=None,
    ):
        tried.append(token)
        return status, 111 if status == SEND_OK else None

    monkeypatch.setattr(UserTokenSender, "_send_with_token", fake_send)
    return tried


async def _send(s, **kw):
    return await s.send(mapping_id="m", target_channel_id=1, content="hi", **kw)


@pytest.mark.asyncio
async def test_authorless_send_takes_the_first_online_token_every_time(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online={"a", "c"})
    s = _sender(_DB(["a", "b", "c", "d"]), pool)

    orders = []
    for _ in range(4):
        tried.clear()
        await _send(s)
        orders.append(list(tried))

    # No rotation, and the offline tokens are never used.
    assert all(o == ["ta", "tc"] for o in orders)
    assert s._sticky_author_token == {}
    assert polls.count == 0
    # One acquire per send, with the ids in the order tried.
    assert pool.acquired == [["a", "c"]] * 4


@pytest.mark.asyncio
async def test_head_comes_online_during_the_wait_and_sends(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    pool = FakePool(online={"b"})
    s = _sender(_DB(["a", "b", "c"]), pool)
    polls.hooks.append(lambda n: pool.online.add("a") if n == 3 else None)

    assert await _send(s, author_id="u") == SEND_OK

    # The sticky account waited three polls and sent; b was online but not used.
    assert polls.count == 3
    assert tried == ["ta"]
    assert s._sticky_author_token[("m", "u")] == "a"
    assert pool.acquired == [["a", "b"]]


@pytest.mark.asyncio
async def test_head_offline_for_the_whole_wait_uses_another_online_token(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    warnings = []
    pool = FakePool(online={"c"})
    s = _sender(_DB(["a", "b", "c"]), pool, warnings)

    assert await _send(s, author_id="u") == SEND_OK

    assert polls.count == FULL_WAIT
    assert tried == ["tc"]
    # The assignment stays with a for when it comes back.
    assert s._sticky_author_token[("m", "u")] == "a"
    assert len(warnings) == 1 and "a" in warnings[0]


@pytest.mark.asyncio
async def test_sticky_author_keeps_assignment_with_every_token_online(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online={"a", "b", "c"})
    s = _sender(_DB(["a", "b", "c"]), pool)

    got = {}
    for author in ("userA", "userB", "userC"):
        tried.clear()
        await _send(s, author_id=author)
        got[author] = list(tried)

    assert s._sticky_author_token == {
        ("m", "userA"): "a",
        ("m", "userB"): "b",
        ("m", "userC"): "c",
    }
    assert got["userA"] == ["ta", "tb", "tc"]
    assert got["userB"] == ["tb", "tc", "ta"]
    assert got["userC"] == ["tc", "ta", "tb"]
    assert polls.count == 0


@pytest.mark.asyncio
async def test_forced_sticky_exclusive_online_sends_with_it_alone(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online={"a", "b"})
    s = _sender(_DB(["a", "b"]), pool)

    await _send(s, forced_token_id="a", sticky_exclusive=True)

    assert tried == ["ta"]
    assert pool.acquired == []


@pytest.mark.asyncio
async def test_forced_sticky_exclusive_offline_is_no_tokens(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    pool = FakePool(online={"b"})
    s = _sender(_DB(["a", "b"]), pool)

    assert await _send(s, forced_token_id="a", sticky_exclusive=True) == SEND_NO_TOKENS

    assert polls.count == FULL_WAIT
    assert tried == []
    assert pool.acquired == []


@pytest.mark.asyncio
async def test_forced_without_exclusive_waits_then_uses_the_online_rest(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online={"b"})
    s = _sender(_DB(["a", "b", "c"]), pool)

    await _send(s, forced_token_id="a")

    assert polls.count == FULL_WAIT
    assert tried == ["tb"]
    assert pool.acquired == [["b"]]


@pytest.mark.asyncio
async def test_nothing_online_is_no_tokens_without_any_request(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    typed = []

    async def fake_type(self, chan, token, duration, *, use_proxy=False):
        typed.append(token)

    monkeypatch.setattr(UserTokenSender, "_type_for", fake_type)
    pool = FakePool(online=set())
    s = _sender(_DB(["a", "b", "c"]), pool)

    assert await _send(s, typing=True, max_delay=1.0) == SEND_NO_TOKENS
    assert tried == [] and typed == []
    assert pool.acquired == []
    assert await s.create_text_thread(mapping_id="m", parent_channel_id=9, thread_name="t") is None
    assert await s.create_forum_thread(
        mapping_id="m", forum_channel_id=8, thread_name="t", content="hi"
    ) is None
    assert tried == [] and pool.acquired == []


@pytest.mark.asyncio
async def test_offline_warning_is_throttled_per_token(monkeypatch, polls):
    _record_sends(monkeypatch, status=SEND_OK)
    now = [1000.0]
    monkeypatch.setattr(token_sender, "time", types.SimpleNamespace(monotonic=lambda: now[0]))
    warnings = []
    pool = FakePool(online=set())
    s = _sender(_DB(["a", "b"]), pool, warnings)

    for _ in range(5):
        await _send(s)
    assert len(warnings) == 1
    now[0] += 30
    await _send(s)
    assert len(warnings) == 1
    now[0] += 31
    await _send(s)
    assert len(warnings) == 2
    # Another token has its own minute.
    await _send(s, forced_token_id="b")
    assert len(warnings) == 3


@pytest.mark.asyncio
async def test_typing_and_delay_start_after_the_head_is_online(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    pool = FakePool(online=set())
    seen = []

    async def fake_pace(self, chan, lo, hi, *, typing=False, token=None, use_proxy=False):
        seen.append((token, set(pool.online)))

    monkeypatch.setattr(UserTokenSender, "_pace_channel", fake_pace)
    s = _sender(_DB(["a", "b"]), pool)
    polls.hooks.append(lambda n: pool.online.add("a") if n == 2 else None)

    assert await _send(s, typing=True, max_delay=2.0) == SEND_OK

    assert seen == [("ta", {"a"})]
    assert tried == ["ta"]


@pytest.mark.asyncio
async def test_a_waiting_message_is_not_overtaken_in_its_channel(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    pool = FakePool(online={"b"})
    s = _sender(_DB(["a", "b"]), pool)
    polls.hooks.append(lambda n: pool.online.add("a") if n == 5 else None)

    first = asyncio.create_task(_send(s, author_id="u1"))
    await asyncio.sleep(0)
    second = asyncio.create_task(_send(s, author_id="u2"))

    assert await first == SEND_OK
    assert await second == SEND_OK
    # u1 is sticky to a, which waited; u2's b was online but went second.
    assert tried == ["ta", "tb"]


@pytest.mark.asyncio
async def test_pool_dead_id_benches_token_once(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    db = _DB(["a", "b"])
    pool = FakePool(online={"a", "b"}, dead={"a"})
    s = _sender(db, pool)

    for _ in range(4):
        assert await _send(s) == SEND_OK

    assert db.disabled == [("a", False)]
    assert "ta" in s._dead_tokens
    assert tried == ["tb"] * 4
    for candidates in pool.acquired:
        assert "a" not in candidates


@pytest.mark.asyncio
async def test_pool_dead_id_dropped_from_thread_creation(monkeypatch):
    tried = []

    async def fake_create(
        self,
        token,
        parent_channel_id,
        thread_name,
        *,
        starter_message_id=None,
        auto_archive_duration=60,
        use_proxy=False,
    ):
        tried.append(token)
        return 555

    monkeypatch.setattr(UserTokenSender, "_create_text_thread_with_token", fake_create)
    db = _DB(["a", "b", "c"])
    pool = FakePool(online={"c"}, dead={"a"})
    s = _sender(db, pool)

    new_id = await s.create_text_thread(
        mapping_id="m", parent_channel_id=9, thread_name="t"
    )

    assert new_id == 555
    assert tried == ["tc"]
    assert db.disabled == [("a", False)]
    # b leads once a is benched; it never came online, so only c is used.
    assert pool.acquired == [["c"]]


@pytest.mark.asyncio
async def test_pool_none_is_unchanged(monkeypatch, polls):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    db = _DB(["a", "b", "c"])
    s = _sender(db)

    for author in ("u1", "u2", "u3", "u1", "u2", "u3"):
        assert await _send(s, author_id=author) == SEND_OK
    assert await _send(s) == SEND_OK

    assert tried == ["ta", "tb", "tc", "ta", "tb", "tc", "ta"]
    assert db.disabled == []
    assert s._pool is None
    order = [{"token_id": "x"}, {"token_id": "y"}]
    assert await s._online_only(order) is order
    assert await s._online_only(order, exclusive=True) is order
    assert s._sync_pool_dead(order) is False
    assert polls.count == 0


@pytest.mark.asyncio
async def test_reap_idle_state_leaves_pool_alone():
    pool = FakePool(online={"a"})
    s = _sender(_DB(["a"]), pool)

    await s.reap_idle_state(idle_ttl=0)

    assert pool.reaped == 0
