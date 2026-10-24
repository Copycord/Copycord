"""UserTokenSender with the warm gateway pool, driven by a fake pool (no Discord)."""

import types

import pytest

from copycord.server.token_sender import SEND_OK, SEND_TRANSIENT, UserTokenSender
from tests.test_token_sender import _FakeRateLimit


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


def _sender(db, pool=None):
    return UserTokenSender(
        db=db,
        ratelimit=_FakeRateLimit(),
        action_type="user_message",
        session_provider=lambda: None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
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
    kw.setdefault("strategy", "round_robin")
    return await s.send(mapping_id="m", target_channel_id=1, content="hi", **kw)


@pytest.mark.asyncio
async def test_round_robin_rotates_inside_online_group(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online={"b", "d"})
    s = _sender(_DB(["a", "b", "c", "d"]), pool)

    orders = []
    for _ in range(8):
        tried.clear()
        await _send(s)
        orders.append(list(tried))

    for order in orders:
        assert sorted(order[:2]) == ["tb", "td"]
        assert sorted(order[2:]) == ["ta", "tc"]
    firsts = [o[0] for o in orders]
    assert firsts.count("tb") == firsts.count("td") == 4
    # The rotation steps over the online tokens only, so no account goes twice in a row.
    assert [o[:2] for o in orders[:4]] == [
        ["tb", "td"],
        ["td", "tb"],
        ["tb", "td"],
        ["td", "tb"],
    ]
    assert all(a != b for a, b in zip(firsts, firsts[1:]))
    # The offline fallbacks keep their sorted order.
    assert all(o[2:] == ["ta", "tc"] for o in orders)
    # One acquire per send, with the ids in the order tried.
    assert len(pool.acquired) == 8
    assert pool.acquired[0] == [t[1:] for t in orders[0]]


@pytest.mark.asyncio
async def test_sticky_author_keeps_assignment_order_inside_groups(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online={"c"})
    s = _sender(_DB(["a", "b", "c"]), pool)

    got = {}
    for author in ("userA", "userB", "userC"):
        tried.clear()
        await _send(s, strategy="sticky_author", author_id=author)
        got[author] = list(tried)

    assert s._sticky_author_token[("m", "userA")] == "a"
    assert s._sticky_author_token[("m", "userB")] == "b"
    assert s._sticky_author_token[("m", "userC")] == "c"
    assert got["userA"] == ["tc", "ta", "tb"]
    assert got["userB"] == ["tc", "tb", "ta"]
    assert got["userC"] == ["tc", "ta", "tb"]

    # With every token online the assignment order is untouched.
    pool.online = {"a", "b", "c"}
    tried.clear()
    await _send(s, strategy="sticky_author", author_id="userB")
    assert tried == ["tb", "tc", "ta"]


@pytest.mark.asyncio
async def test_forced_sticky_exclusive_is_untouched(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online={"b"})
    s = _sender(_DB(["a", "b"]), pool)

    await _send(s, forced_token_id="a", sticky_exclusive=True)

    assert tried == ["ta"]
    assert pool.acquired == []


@pytest.mark.asyncio
async def test_forced_without_exclusive_still_prefers_online(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online={"b"})
    s = _sender(_DB(["a", "b", "c"]), pool)

    await _send(s, forced_token_id="a")

    assert tried == ["tb", "ta", "tc"]
    assert pool.acquired == [["b", "a", "c"]]


@pytest.mark.asyncio
async def test_no_online_tokens_leaves_order_unchanged(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = FakePool(online=set())
    with_pool = _sender(_DB(["a", "b", "c"]), pool)
    without = _sender(_DB(["a", "b", "c"]))

    for strategy, author in (("round_robin", None),) * 4 + (("sticky_author", "u"),):
        tried.clear()
        await _send(with_pool, strategy=strategy, author_id=author)
        a = list(tried)
        tried.clear()
        await _send(without, strategy=strategy, author_id=author)
        assert a == tried
    assert len(pool.acquired) == 5


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
    assert pool.acquired == [["c", "b"]]


@pytest.mark.asyncio
async def test_pool_none_is_unchanged(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_OK)
    db = _DB(["a", "b", "c"])
    s = _sender(db)

    for _ in range(6):
        assert await _send(s) == SEND_OK

    assert tried == ["ta", "tb", "tc", "ta", "tb", "tc"]
    assert db.disabled == []
    assert s._pool is None
    order = [{"token_id": "x"}, {"token_id": "y"}]
    assert s._prefer_online(order) is order
    assert s._sync_pool_dead(order) is False


@pytest.mark.asyncio
async def test_reap_idle_state_leaves_pool_alone():
    pool = FakePool(online={"a"})
    s = _sender(_DB(["a"]), pool)

    await s.reap_idle_state(idle_ttl=0)

    assert pool.reaped == 0
