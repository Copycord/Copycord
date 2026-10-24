"""TokenSessionPool: the warm gateway pool, driven by fake clients (no Discord)."""
import asyncio
import logging
import random

import pytest

from copycord.selfclient.errors import ConnectionClosed, HTTPException, LoginFailure
from copycord.server import token_pool
from copycord.server.token_pool import RANDOM_STATUSES, TokenSessionPool

pytestmark = pytest.mark.asyncio


class FakeClient:
    """Just enough of selfclient.Client for the pool: login, connect, close, READY."""

    def __init__(self, fakes, **kwargs):
        self.fakes = fakes
        self.kwargs = kwargs
        self.status = kwargs.get("status")
        self.token = None
        self.presence_changes = []
        self._ready = asyncio.Event()
        self._ended = asyncio.Event()
        self._closed = False
        self._fail = None
        self._listeners = {}
        self.user = None

    async def login(self, token):
        self.token = token
        self.user = self.fakes.users.get(token)
        self.fakes.events.append(("login", token))
        err = self.fakes.next_error("login", token)
        if err is not None:
            raise err

    async def connect(self, *, reconnect=True):
        err = self.fakes.next_error("connect", self.token)
        if err is not None:
            raise err
        self._ready.set()
        self.fire("on_ready")
        await self._ended.wait()
        if self._fail is not None:
            raise self._fail

    async def close(self):
        self._closed = True
        self._ready.clear()
        self._ended.set()

    def is_ready(self):
        return self._ready.is_set()

    def is_closed(self):
        return self._closed

    async def wait_until_ready(self):
        await self._ready.wait()

    async def change_presence(self, *, status=None, **_):
        self.presence_changes.append(status)
        self.status = status

    def add_listener(self, func, name):
        self._listeners.setdefault(name, []).append(func)

    def fire(self, name):
        for func in self._listeners.get(name, []):
            asyncio.ensure_future(func())

    def end_with(self, exc):
        """The gateway stops on its own, raising ``exc`` from connect()."""
        self._fail = exc
        self._ended.set()


class Fakes:
    def __init__(self):
        self.clients = []
        self.events = []
        self.errors = {}
        # token value -> the selfclient User its login resolves to
        self.users = {}

    def factory(self, **kwargs):
        client = FakeClient(self, **kwargs)
        self.clients.append(client)
        return client

    def fail(self, stage, token, *excs):
        self.errors.setdefault((stage, token), []).extend(excs)

    def next_error(self, stage, token):
        queue = self.errors.get((stage, token))
        return queue.pop(0) if queue else None

    def for_token(self, token):
        return [c for c in self.clients if c.token == token]

    def live(self):
        return [c for c in self.clients if c.is_ready() and not c.is_closed()]


@pytest.fixture()
def fakes():
    return Fakes()


@pytest.fixture()
def sleeps(monkeypatch, fakes):
    """Every pool wait returns at once; the asked-for delays are recorded."""
    recorded = []

    async def fake_sleep(delay, *_, **__):
        recorded.append(delay)
        fakes.events.append(("sleep", delay))
        await asyncio.sleep(0)

    monkeypatch.setattr(token_pool, "_sleep", fake_sleep)
    return recorded


@pytest.fixture()
def proxies():
    return []


@pytest.fixture()
def pool(fakes, sleeps, proxies):
    def proxy_for(token):
        proxies.append(token)
        return "http://proxy.invalid:8080"

    return TokenSessionPool(
        logger=logging.getLogger("test.token_pool"),
        proxy_for=proxy_for,
        client_factory=fakes.factory,
    )


async def until(predicate, rounds=2000):
    for _ in range(rounds):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition never became true")


async def settle(rounds=200):
    for _ in range(rounds):
        await asyncio.sleep(0)


async def ensure_all(pool, pairs, *, keep_online, presence="online"):
    for token_id, token_value in pairs:
        await pool.ensure(token_id, token_value, keep_online=keep_online, presence=presence)


async def test_reconcile_up_and_down(pool, fakes):
    try:
        pairs = [("t1", "tok-1"), ("t2", "tok-2"), ("t3", "tok-3")]
        await ensure_all(pool, pairs, keep_online=2)
        await until(lambda: pool.online_ids() == {"t1", "t2"})
        assert len(fakes.clients) == 2

        await pool.ensure("t3", "tok-3", keep_online=3, presence="online")
        await until(lambda: pool.online_ids() == {"t1", "t2", "t3"})

        await pool.ensure("t1", "tok-1", keep_online=1, presence="online")
        assert pool.online_ids() == {"t1"}
        assert all(c.is_closed() for c in fakes.for_token("tok-2") + fakes.for_token("tok-3"))
        assert not fakes.for_token("tok-1")[0].is_closed()
        snap = pool.snapshot()
        assert snap["keep_online"] == 1
        assert snap["tokens"]["t1"]["state"] == "online"
        assert snap["tokens"]["t2"]["state"] == "offline"
    finally:
        await pool.close_all()


async def test_client_gets_small_cache_and_proxy(pool, fakes, proxies):
    try:
        await pool.ensure("t1", "tok-1", keep_online=1, presence="online")
        await until(lambda: pool.online_ids() == {"t1"})
        kwargs = fakes.clients[0].kwargs
        assert kwargs["max_messages"] == 0
        assert kwargs["refresh_build"] is False
        assert kwargs["proxy"] == "http://proxy.invalid:8080"
        assert proxies == ["tok-1"]
        # the shared pool REST leases from, so a gateway dial failure rotates that lease
        from copycord.common.proxy_pool import get_pool

        assert kwargs["proxy_pool"] is get_pool()
    finally:
        await pool.close_all()


async def test_no_proxy_means_no_connect(fakes, sleeps):
    """Proxies are mandatory: proxy_for None keeps the token offline and retries, never dialling direct."""
    asked = []

    def proxy_for(token):
        asked.append(token)
        return None

    pool = TokenSessionPool(
        logger=logging.getLogger("test.token_pool"),
        proxy_for=proxy_for,
        client_factory=fakes.factory,
    )
    try:
        await pool.ensure("t1", "tok-1", keep_online=1, presence="online")
        await until(lambda: len(asked) >= 2)
        assert fakes.clients == []
        assert pool.online_ids() == set()
        assert pool.snapshot()["tokens"]["t1"]["state"] != "online"
        assert "t1" not in pool.dead_ids()
        # it backs off between attempts
        assert any(d > 0 for d in sleeps)
    finally:
        await pool.close_all()


async def test_shared_token_is_one_session(pool, fakes):
    try:
        await pool.ensure("a", "same-token", keep_online=5, presence="online")
        await pool.ensure("b", "same-token", keep_online=5, presence="online")
        await until(lambda: pool.online_ids() == {"a", "b"})
        assert len(fakes.clients) == 1

        await pool.close("a")
        assert pool.online_ids() == {"b"}
        assert not fakes.clients[0].is_closed()

        await pool.close("b")
        assert pool.online_ids() == set()
        assert fakes.clients[0].is_closed()
    finally:
        await pool.close_all()


async def test_shared_token_counts_once_against_keep_online(pool, fakes):
    try:
        await ensure_all(
            pool, [("a", "tok-x"), ("b", "tok-x"), ("c", "tok-y")], keep_online=2
        )
        await until(lambda: pool.online_ids() == {"a", "b", "c"})
        assert len(fakes.clients) == 2
    finally:
        await pool.close_all()


async def test_connects_one_at_a_time_with_stagger_gap(pool, fakes, sleeps):
    try:
        pairs = [("t1", "tok-1"), ("t2", "tok-2"), ("t3", "tok-3")]
        await ensure_all(pool, pairs, keep_online=3)
        await until(lambda: pool.online_ids() == {"t1", "t2", "t3"})
        await settle()
        kinds = [kind for kind, _ in fakes.events]
        # each login is followed by its gap before the next login starts
        assert kinds[:6] == ["login", "sleep"] * 3
        gaps = [d for kind, d in fakes.events if kind == "sleep"]
        assert len(gaps) == 3
        assert all(2.0 <= g <= 6.0 for g in gaps)
    finally:
        await pool.close_all()


async def test_loop_lag_during_connect_warns_once_and_widens_next_gap(pool, caplog):
    caplog.set_level(logging.WARNING, logger="test.token_pool")
    pool._note_lag(0.4)
    assert "lagged" not in caplog.text  # no connect in flight
    assert 2.0 <= pool._next_gap() <= 6.0

    pool._connecting = 1
    pool._note_lag(0.3)
    pool._note_lag(0.5)
    pool._connecting = 0
    assert caplog.text.count("lagged") == 1
    assert 4.0 <= pool._next_gap() <= 12.0
    assert 2.0 <= pool._next_gap() <= 6.0
    assert pool.snapshot()["max_loop_lag_ms"] == 500.0


@pytest.mark.parametrize(
    "presence,status",
    [("online", "online"), ("idle", "idle"), ("busy", "dnd"), ("invisible", "invisible")],
)
async def test_presence_maps_to_discord_status(pool, fakes, presence, status):
    try:
        await pool.ensure("t1", "tok-1", keep_online=1, presence=presence)
        await until(lambda: pool.online_ids() == {"t1"})
        assert fakes.clients[0].kwargs["status"] == status
        assert pool.snapshot()["tokens"]["t1"]["presence"] == status
    finally:
        await pool.close_all()


async def test_random_presence_sticks_across_reconnect(pool, fakes):
    pool._rng = random.Random(1234)
    try:
        pairs = [(f"t{i}", f"tok-{i}") for i in range(12)]
        await ensure_all(pool, pairs, keep_online=12, presence="random")
        await until(lambda: len(pool.online_ids()) == 12)
        picked = {c.token: c.kwargs["status"] for c in fakes.clients}
        assert set(picked.values()) <= set(RANDOM_STATUSES)

        # the gateway drops for good and the pool reconnects with a new client
        first = fakes.for_token("tok-0")[0]
        first.end_with(ConnectionClosed(4000, "unknown error"))
        await until(lambda: len(fakes.for_token("tok-0")) == 2 and "t0" in pool.online_ids())
        assert fakes.for_token("tok-0")[1].kwargs["status"] == picked["tok-0"]

        # closed and ensured again later, still the same pick
        await pool.close("t0")
        await pool.ensure("t0", "tok-0", keep_online=12, presence="random")
        await until(lambda: len(fakes.for_token("tok-0")) == 3 and "t0" in pool.online_ids())
        assert fakes.for_token("tok-0")[2].kwargs["status"] == picked["tok-0"]
    finally:
        await pool.close_all()


async def test_presence_change_reaches_a_live_session(pool, fakes):
    try:
        await pool.ensure("t1", "tok-1", keep_online=1, presence="online")
        await until(lambda: pool.online_ids() == {"t1"})
        await pool.ensure("t1", "tok-1", keep_online=1, presence="busy")
        assert fakes.clients[0].presence_changes == ["dnd"]
        assert len(fakes.clients) == 1
    finally:
        await pool.close_all()


async def test_acquire_online_order_and_none_fallback(pool, fakes):
    try:
        await ensure_all(pool, [("a", "tok-a"), ("b", "tok-b"), ("c", "tok-c")], keep_online=2)
        await until(lambda: pool.online_ids() == {"a", "b"})
        assert await pool.acquire_online(["c", "b", "a"]) == "b"
        assert await pool.acquire_online(["a", "b"]) == "a"
        assert await pool.acquire_online(["c", "unknown"]) is None
        assert await pool.acquire_online([]) is None

        # a dropped socket is not ready even though READY was seen
        fakes.for_token("tok-b")[0].fire("on_disconnect")
        await until(lambda: pool.online_ids() == {"a"})
        assert await pool.acquire_online(["b", "a"]) == "a"
        assert pool.snapshot()["tokens"]["b"]["state"] == "connecting"

        fakes.for_token("tok-b")[0].fire("on_resumed")
        await until(lambda: pool.online_ids() == {"a", "b"})
        assert await pool.acquire_online(["b", "a"]) == "b"
    finally:
        await pool.close_all()


@pytest.mark.parametrize(
    "stage,exc",
    [
        ("login", LoginFailure("Improper token has been passed.")),
        ("login", HTTPException(401, {"message": "401: Unauthorized", "code": 0})),
        ("connect", LoginFailure("Improper token has been passed.")),
    ],
)
async def test_401_marks_token_dead_and_frees_the_slot(pool, fakes, sleeps, caplog, stage, exc):
    caplog.set_level(logging.DEBUG, logger="test.token_pool")
    fakes.fail(stage, "tok-a", exc)
    try:
        await ensure_all(pool, [("a", "tok-a"), ("b", "tok-b")], keep_online=1)
        await until(lambda: pool.dead_ids() == {"a"})
        await until(lambda: pool.online_ids() == {"b"})
        await settle()
        assert len(fakes.for_token("tok-a")) == 1  # never retried
        snap = pool.snapshot()["tokens"]["a"]
        assert snap["dead"] is True and snap["state"] == "offline"

        await pool.ensure("a", "tok-a", keep_online=2, presence="online")
        await settle()
        assert len(fakes.for_token("tok-a")) == 1
        assert "tok-a" not in caplog.text and "tok-b" not in caplog.text
    finally:
        await pool.close_all()


async def test_failed_connect_backs_off_exponentially_up_to_five_minutes(pool, fakes, sleeps):
    pool._next_gap = lambda: 0.0
    fakes.fail("login", "tok-1", *[HTTPException(502, None) for _ in range(9)])
    try:
        await pool.ensure("t1", "tok-1", keep_online=1, presence="online")
        await until(lambda: pool.online_ids() == {"t1"})
        backoffs = [d for d in sleeps if d > 0]
        assert len(backoffs) == 9
        assert 5.0 <= backoffs[0] <= 6.25
        assert 10.0 <= backoffs[1] <= 12.5
        assert backoffs == sorted(backoffs)
        assert max(backoffs) == 300.0
        assert pool.dead_ids() == set()
    finally:
        await pool.close_all()


async def test_keep_online_zero_closes_all(pool, fakes):
    try:
        await ensure_all(pool, [("a", "tok-a"), ("b", "tok-b")], keep_online=5)
        await until(lambda: pool.online_ids() == {"a", "b"})

        await pool.ensure("a", "tok-a", keep_online=0, presence="online")
        assert pool.online_ids() == set()
        assert all(c.is_closed() for c in fakes.clients)
        assert {t["state"] for t in pool.snapshot()["tokens"].values()} == {"offline"}

        await pool.ensure("a", "tok-a", keep_online=5, presence="online")
        await until(lambda: pool.online_ids() == {"a", "b"})
    finally:
        await pool.close_all()


async def test_close_all_stops_sessions_and_lag_monitor(pool, fakes):
    await ensure_all(pool, [("a", "tok-a"), ("b", "tok-b")], keep_online=5)
    await until(lambda: pool.online_ids() == {"a", "b"})
    lag_task = pool._lag_task
    assert lag_task is not None and not lag_task.done()
    await pool.close_all()
    assert pool.online_ids() == set()
    assert all(c.is_closed() for c in fakes.clients)
    assert lag_task.done()


async def test_token_rotation_moves_the_id_to_a_new_session(pool, fakes):
    try:
        await pool.ensure("a", "old-token", keep_online=5, presence="online")
        await until(lambda: pool.online_ids() == {"a"})
        await pool.ensure("a", "new-token", keep_online=5, presence="online")
        await until(lambda: "a" in pool.online_ids() and len(fakes.clients) == 2)
        assert fakes.for_token("old-token")[0].is_closed()
        assert not fakes.for_token("new-token")[0].is_closed()
    finally:
        await pool.close_all()


async def test_reap_idle_closes_sessions_not_seen_for_ttl(pool, fakes):
    try:
        await ensure_all(pool, [("a", "tok-a"), ("b", "tok-b")], keep_online=5)
        await until(lambda: pool.online_ids() == {"a", "b"})
        assert await pool.reap_idle(idle_ttl=3600) == 0
        assert await pool.reap_idle(idle_ttl=0) == 2
        assert pool.online_ids() == set()
        assert pool.snapshot()["tokens"] == {}
        assert all(c.is_closed() for c in fakes.clients)
    finally:
        await pool.close_all()


async def test_snapshot_identity_while_ready(pool, fakes):
    from copycord.selfclient.models import User

    fakes.users["tok-a"] = User(
        {"id": "175928847299117063", "username": "relay.alpha", "global_name": "Ava", "avatar": "abc123"}
    )
    fakes.users["tok-b"] = User({"id": "175928847299117063", "username": "relay.bravo"})
    try:
        await ensure_all(pool, [("a", "tok-a"), ("b", "tok-b"), ("c", "tok-c")], keep_online=2)
        await until(lambda: pool.online_ids() == {"a", "b"})
        tokens = pool.snapshot()["tokens"]
        assert tokens["a"]["identity"] == {
            "username": "relay.alpha",
            "display_name": "Ava",
            "avatar_url": "https://cdn.discordapp.com/avatars/175928847299117063/abc123.png?size=1024",
            "user_id": "175928847299117063",
        }
        # No avatar hash: the default avatar for the id; no global name: None.
        assert tokens["b"]["identity"]["avatar_url"] == "https://cdn.discordapp.com/embed/avatars/2.png"
        assert tokens["b"]["identity"]["display_name"] is None
        # Not connected: no identity.
        assert tokens["c"]["identity"] is None

        await pool.ensure("a", "tok-a", keep_online=0, presence="online")
        assert all(t["identity"] is None for t in pool.snapshot()["tokens"].values())
    finally:
        await pool.close_all()


async def test_weighted_pick_covers_idle_and_follows_the_weights():
    rng = random.Random(7)
    counts = {s: 0 for s in RANDOM_STATUSES}
    for _ in range(4000):
        counts[token_pool._pick_status(rng)] += 1
    assert set(RANDOM_STATUSES) == {"online", "idle", "dnd", "invisible"}
    assert all(n > 0 for n in counts.values())
    assert counts["online"] > counts["idle"] > counts["invisible"] > counts["dnd"]
    weights = dict(zip(RANDOM_STATUSES, token_pool.RANDOM_WEIGHTS))
    assert weights == {"online": 50, "idle": 25, "dnd": 10, "invisible": 15}


async def test_weighted_pick_never_repeats_the_excluded_status():
    rng = random.Random(3)
    for current in RANDOM_STATUSES:
        picks = {token_pool._pick_status(rng, exclude=current) for _ in range(300)}
        assert current not in picks
        assert picks == set(RANDOM_STATUSES) - {current}


async def test_unknown_presence_falls_back_to_random(pool):
    pool._rng = random.Random(5)
    assert pool._status_for("k1", "sleepy") in RANDOM_STATUSES
    assert pool._status_for("k1", "") == pool._status_for("k1", "random")
    assert pool._status_for("k2", " IDLE ") == "idle"
