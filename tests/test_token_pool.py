"""TokenSessionPool: the warm gateway pool, driven by fake clients (no Discord)."""
import asyncio
import logging
import random
import time

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


async def ensure_all(pool, pairs, *, keep_online, presence="online", rotate=False):
    for token_id, token_value in pairs:
        await pool.ensure(
            token_id, token_value, keep_online=keep_online, presence=presence, rotate=rotate
        )


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


async def test_connects_wait_no_gap_when_the_loop_is_fine(pool, fakes, sleeps):
    try:
        pairs = [("t1", "tok-1"), ("t2", "tok-2"), ("t3", "tok-3")]
        await ensure_all(pool, pairs, keep_online=3)
        await until(lambda: pool.online_ids() == {"t1", "t2", "t3"})
        await settle()
        assert [kind for kind, _ in fakes.events if kind == "sleep"] == []
    finally:
        await pool.close_all()


async def test_a_lag_seen_during_a_connect_holds_its_slot_for_a_widened_gap(pool, fakes, sleeps):
    pool._widen_next_gap = True
    try:
        await ensure_all(pool, [("t1", "tok-1"), ("t2", "tok-2")], keep_online=2)
        await until(lambda: pool.online_ids() == {"t1", "t2"})
        await settle()
        gaps = [d for kind, d in fakes.events if kind == "sleep"]
        # Only the first connect pays, doubled; the lag flag is spent after it.
        assert len(gaps) == 1
        assert 2.0 * token_pool.LAG_GAP_FACTOR <= gaps[0] <= 6.0 * token_pool.LAG_GAP_FACTOR
    finally:
        await pool.close_all()


async def test_connects_overlap_up_to_the_concurrency_limit(pool, fakes):
    release = asyncio.Event()

    class SlowClient(FakeClient):
        async def connect(self, *, reconnect=True):
            await release.wait()
            await super().connect(reconnect=reconnect)

    def factory(**kwargs):
        client = SlowClient(fakes, **kwargs)
        fakes.clients.append(client)
        return client

    pool._factory = factory
    total = token_pool.CONNECT_CONCURRENCY + 2
    pairs = [(f"t{i}", f"tok-{i}") for i in range(total)]
    try:
        await ensure_all(pool, pairs, keep_online=total)
        await settle()
        logins = [t for kind, t in fakes.events if kind == "login"]
        # Every slot is waiting for READY, so no more than the limit is in flight.
        assert token_pool.CONNECT_CONCURRENCY > 1
        assert len(logins) == token_pool.CONNECT_CONCURRENCY
        release.set()
        await until(lambda: len(pool.online_ids()) == total)
    finally:
        release.set()
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
    # a stored invisible (the status was removed) now means online
    [("online", "online"), ("idle", "idle"), ("busy", "dnd"), ("invisible", "online")],
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
        # Both connect at once; let each connect task mark its session online.
        await settle()
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
    assert set(RANDOM_STATUSES) == {"online", "idle", "dnd"}
    assert all(n > 0 for n in counts.values())
    assert counts["online"] > counts["idle"] > counts["dnd"]
    weights = dict(zip(RANDOM_STATUSES, token_pool.RANDOM_WEIGHTS))
    assert weights == {"online": 50, "idle": 25, "dnd": 10}
    assert "invisible" not in token_pool.PRESENCE_STATUS
    assert set(token_pool.PRESENCE_STATUS) == {"online", "idle", "busy"}


async def test_weighted_pick_never_repeats_the_excluded_status():
    rng = random.Random(3)
    assert len(RANDOM_STATUSES) == 3
    for current in RANDOM_STATUSES:
        picks = {token_pool._pick_status(rng, exclude=current) for _ in range(300)}
        assert current not in picks
        # the other two statuses both come up
        assert picks == set(RANDOM_STATUSES) - {current}
        assert len(picks) == 2


async def test_unknown_presence_falls_back_to_random(pool):
    pool._rng = random.Random(5)
    assert pool._status_for("k1", "sleepy") in RANDOM_STATUSES
    assert pool._status_for("k1", "") == pool._status_for("k1", "random")
    assert pool._status_for("k2", " IDLE ") == "idle"


async def test_stored_invisible_means_online_and_never_rotates(pool, fakes, clock):
    assert pool._status_for("k1", "invisible") == "online"
    assert pool._status_for("k1", " Invisible ") == "online"
    assert token_pool._presence_mode("invisible") == "online"
    assert token_pool._presence_mode("sleepy") == "random"
    try:
        await pool.ensure("t1", "tok-1", keep_online=1, presence="invisible", rotate=True)
        await until(lambda: pool.online_ids() == {"t1"})
        await settle()
        client = fakes.clients[0]
        assert client.kwargs["status"] == "online"
        assert pool.snapshot()["tokens"]["t1"]["presence"] == "online"
        assert pool._shuffle_due == {}
        clock[0] += 10 * token_pool.SHUFFLE_MAX_S
        await settle()
        assert client.presence_changes == []
    finally:
        await pool.close_all()


# random status reshuffle


@pytest.fixture()
def clock(pool):
    """The pool's reshuffle clock, moved by hand."""
    now = [1000.0]
    pool._clock = lambda: now[0]
    return now


async def online_random(pool, pairs, *, seed=11):
    pool._rng = random.Random(seed)
    await ensure_all(pool, pairs, keep_online=len(pairs), presence="random", rotate=True)
    await until(lambda: len(pool.online_ids()) == len(pairs))
    # READY shows before the connect step (or the on_ready listener) starts the clock
    await until(lambda: len(pool._shuffle_due) == len(pairs))


def key_of(token_value):
    return token_pool._token_key(token_value)


async def test_shuffle_constants():
    assert token_pool.SHUFFLE_MIN_S == 1200
    assert token_pool.SHUFFLE_MAX_S == 5400
    assert token_pool.SHUFFLE_GAP_S == 10


async def test_first_change_comes_20_to_90_minutes_after_online(pool, fakes, clock):
    try:
        await online_random(pool, [("t1", "tok-1")])
        client = fakes.clients[0]
        due = pool._shuffle_due[key_of("tok-1")]
        assert 1200 <= due - 1000.0 <= 5400

        clock[0] = due - 0.5
        await settle()
        assert client.presence_changes == []

        clock[0] = due
        await until(lambda: len(client.presence_changes) == 1)
        assert client.presence_changes[0] != client.kwargs["status"]
        assert client.presence_changes[0] in RANDOM_STATUSES
        nxt = pool._shuffle_due[key_of("tok-1")]
        assert 1200 <= nxt - due <= 5400
    finally:
        await pool.close_all()


async def test_each_change_is_a_new_status_and_shows_in_the_snapshot(pool, fakes, clock):
    try:
        await online_random(pool, [("t1", "tok-1")], seed=2)
        client = fakes.clients[0]
        key = key_of("tok-1")
        seen = [client.kwargs["status"]]
        for n in range(1, 81):
            clock[0] = pool._shuffle_due[key]
            await until(lambda: len(client.presence_changes) == n)
            seen.append(client.presence_changes[-1])
            assert pool.snapshot()["tokens"]["t1"]["presence"] == seen[-1]
            assert pool._random_status[key] == seen[-1]
        assert all(a != b for a, b in zip(seen, seen[1:]))
        assert set(seen) == set(RANDOM_STATUSES)
    finally:
        await pool.close_all()


async def test_two_due_tokens_change_at_least_10_seconds_apart(pool, fakes, clock):
    try:
        await online_random(pool, [("a", "tok-a"), ("b", "tok-b")])
        ca, cb = fakes.for_token("tok-a")[0], fakes.for_token("tok-b")[0]
        start = float(int(max(pool._shuffle_due.values())) + 100)
        clock[0] = start
        await until(lambda: len(ca.presence_changes) + len(cb.presence_changes) == 1)
        await settle()
        assert len(ca.presence_changes) + len(cb.presence_changes) == 1

        clock[0] = start + 9.9
        await settle()
        assert len(ca.presence_changes) + len(cb.presence_changes) == 1

        clock[0] = start + 10
        await until(lambda: len(ca.presence_changes) == 1 and len(cb.presence_changes) == 1)
    finally:
        await pool.close_all()


async def test_fixed_modes_never_change(pool, fakes, clock):
    try:
        for i, presence in enumerate(("online", "idle", "busy")):
            # rotate on changes nothing for a fixed status
            await pool.ensure(f"t{i}", f"tok-{i}", keep_online=3, presence=presence, rotate=True)
        await until(lambda: len(pool.online_ids()) == 3)
        await settle()
        assert pool._shuffle_due == {}
        assert pool._shuffle_task is None
        clock[0] += 10 * token_pool.SHUFFLE_MAX_S
        await settle()
        assert all(c.presence_changes == [] for c in fakes.clients)
    finally:
        await pool.close_all()


async def test_random_without_rotate_keeps_its_one_pick(pool, fakes, clock):
    pool._rng = random.Random(11)
    try:
        pairs = [("a", "tok-a"), ("b", "tok-b")]
        await ensure_all(pool, pairs, keep_online=2, presence="random")
        await until(lambda: len(pool.online_ids()) == 2)
        await settle()
        assert pool._shuffle_due == {}
        assert pool._shuffle_task is None
        picked = {c.token: c.kwargs["status"] for c in fakes.clients}
        assert set(picked.values()) <= set(RANDOM_STATUSES)

        clock[0] += 10 * token_pool.SHUFFLE_MAX_S
        await settle()
        assert all(c.presence_changes == [] for c in fakes.clients)
        for token_id, token_value in pairs:
            assert pool.snapshot()["tokens"][token_id]["presence"] == picked[token_value]
    finally:
        await pool.close_all()


async def test_rotate_off_cancels_and_on_starts_the_clock(pool, fakes, clock):
    try:
        await online_random(pool, [("t1", "tok-1")])
        client = fakes.clients[0]
        key = key_of("tok-1")
        current = client.kwargs["status"]
        assert key in pool._shuffle_due

        await pool.ensure("t1", "tok-1", keep_online=1, presence="random", rotate=False)
        assert key not in pool._shuffle_due
        clock[0] += 10 * token_pool.SHUFFLE_MAX_S
        await settle()
        assert client.presence_changes == []
        assert pool.snapshot()["tokens"]["t1"]["presence"] == current

        await pool.ensure("t1", "tok-1", keep_online=1, presence="random", rotate=True)
        due = pool._shuffle_due[key]
        assert 1200 <= due - clock[0] <= 5400
        # the current pick stays until the first timer fires
        assert client.presence_changes == []
        clock[0] = due - 0.5
        await settle()
        assert client.presence_changes == []
        clock[0] = due
        await until(lambda: len(client.presence_changes) == 1)
        assert client.presence_changes[0] != current
    finally:
        await pool.close_all()


async def test_random_to_fixed_cancels_and_back_sets_the_clock(pool, fakes, clock):
    try:
        await online_random(pool, [("t1", "tok-1")])
        client = fakes.clients[0]
        key = key_of("tok-1")
        assert key in pool._shuffle_due

        await pool.ensure("t1", "tok-1", keep_online=1, presence="busy", rotate=True)
        assert key not in pool._shuffle_due
        clock[0] += 10 * token_pool.SHUFFLE_MAX_S
        await settle()
        assert client.presence_changes in ([], ["dnd"])
        changes = list(client.presence_changes)

        await pool.ensure("t1", "tok-1", keep_online=1, presence="random", rotate=True)
        due = pool._shuffle_due[key]
        assert 1200 <= due - clock[0] <= 5400
        # back on the random pick it had; the reshuffle comes later
        changes = list(client.presence_changes)
        clock[0] = due
        await until(lambda: len(client.presence_changes) == len(changes) + 1)
    finally:
        await pool.close_all()


async def test_reconnect_keeps_the_current_pick(pool, fakes, clock):
    try:
        await online_random(pool, [("t1", "tok-1")])
        first = fakes.clients[0]
        clock[0] = pool._shuffle_due[key_of("tok-1")]
        await until(lambda: len(first.presence_changes) == 1)
        current = first.presence_changes[0]

        first.end_with(ConnectionClosed(4000, "unknown error"))
        await until(lambda: len(fakes.for_token("tok-1")) == 2 and "t1" in pool.online_ids())
        assert fakes.for_token("tok-1")[1].kwargs["status"] == current
        assert pool.snapshot()["tokens"]["t1"]["presence"] == current

        await pool.close("t1")
        await pool.ensure("t1", "tok-1", keep_online=1, presence="random", rotate=True)
        await until(lambda: len(fakes.for_token("tok-1")) == 3 and "t1" in pool.online_ids())
        assert fakes.for_token("tok-1")[2].kwargs["status"] == current
    finally:
        await pool.close_all()


async def test_not_ready_session_is_skipped_and_rescheduled(pool, fakes, clock):
    try:
        await online_random(pool, [("t1", "tok-1")])
        client = fakes.clients[0]
        key = key_of("tok-1")
        client.fire("on_disconnect")
        await until(lambda: pool.online_ids() == set())

        due = pool._shuffle_due[key]
        clock[0] = due
        await until(lambda: pool._shuffle_due[key] != due)
        assert client.presence_changes == []
        assert 1200 <= pool._shuffle_due[key] - due <= 5400

        client.fire("on_resumed")
        await until(lambda: pool.online_ids() == {"t1"})
        clock[0] = pool._shuffle_due[key]
        await until(lambda: len(client.presence_changes) == 1)
    finally:
        await pool.close_all()


async def test_a_failed_change_never_stops_the_reshuffle(pool, fakes, clock, caplog):
    caplog.set_level(logging.DEBUG, logger="test.token_pool")
    try:
        await online_random(pool, [("t1", "tok-1")])
        client = fakes.clients[0]
        key = key_of("tok-1")

        async def broken(**_):
            raise RuntimeError("socket gone")

        client.change_presence = broken
        clock[0] = pool._shuffle_due[key]
        await until(lambda: "presence change failed" in caplog.text)
        task = pool._shuffle_task
        assert task is not None and not task.done()
        assert pool._random_status[key] == pool.snapshot()["tokens"]["t1"]["presence"]

        # a raise inside the step is logged and the loop carries on
        real = pool._shuffle_once
        calls = []

        async def flaky():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("boom")
            return await real()

        pool._shuffle_once = flaky
        await until(lambda: len(calls) >= 3)
        assert "status reshuffle failed" in caplog.text
        assert not task.done()
    finally:
        await pool.close_all()


async def test_close_all_stops_the_reshuffle(pool, fakes, clock):
    await online_random(pool, [("a", "tok-a"), ("b", "tok-b")])
    task = pool._shuffle_task
    assert task is not None and not task.done()
    await pool.close_all()
    assert task.done()
    assert pool._shuffle_task is None
    assert pool._shuffle_due == {}
    clock[0] += 10 * token_pool.SHUFFLE_MAX_S
    await settle()
    assert all(c.presence_changes == [] for c in fakes.clients)


# no free proxy


def _waiting_lines(caplog):
    return [r for r in caplog.records if "waiting for a free proxy" in r.getMessage()]


@pytest.fixture()
def proxy_pool_of(fakes, sleeps, monkeypatch):
    """A pool whose proxy_for leases only the given tokens, over a proxy pool of ``total`` proxies."""
    from types import SimpleNamespace

    from copycord.common import token_proxy

    monkeypatch.setattr(token_proxy, "_tokens_disabled_noticed", False)

    def build(leased, total):
        pool = TokenSessionPool(
            logger=logging.getLogger("test.token_pool"),
            proxy_for=lambda token: "http://proxy.invalid:8080" if token in leased else None,
            client_factory=fakes.factory,
            proxy_pool=SimpleNamespace(enabled=total > 0, total=total),
        )
        now = [1000.0]
        pool._clock = lambda: now[0]
        return pool, now

    return build


async def test_no_free_proxy_is_one_rate_limited_warning_with_counts(proxy_pool_of, fakes, caplog):
    caplog.set_level(logging.DEBUG, logger="test.token_pool")
    pool, now = proxy_pool_of({"tok-1", "tok-2"}, total=2)
    pairs = [("t1", "tok-1"), ("t2", "tok-2"), ("t3", "tok-3"), ("t4", "tok-4")]
    try:
        await ensure_all(pool, pairs, keep_online=4)
        await until(lambda: pool.online_ids() == {"t1", "t2"})
        await until(lambda: sum(s.waiting_proxy for s in pool._sessions.values()) == 2)
        # many retries since, all inside one window
        await until(lambda: caplog.text.count("has no proxy; retrying") >= 6)
        assert len(_waiting_lines(caplog)) == 1
        assert _waiting_lines(caplog)[0].levelno == logging.WARNING
        assert "ended (NoProxyAvailable)" not in caplog.text
        debug = [r for r in caplog.records if "has no proxy; retrying" in r.getMessage()]
        assert debug and all(r.levelno == logging.DEBUG for r in debug)

        now[0] += token_pool.PROXY_WARN_INTERVAL - 1
        await until(lambda: caplog.text.count("has no proxy; retrying") >= 12)
        assert len(_waiting_lines(caplog)) == 1

        now[0] += 2
        await until(lambda: len(_waiting_lines(caplog)) == 2)
        assert _waiting_lines(caplog)[1].getMessage() == (
            "[token-pool] 2 token(s) are waiting for a free proxy (2 proxies configured, 4 needed)"
        )
        await settle()
        assert len(_waiting_lines(caplog)) == 2
        assert "tok-3" not in caplog.text and "tok-4" not in caplog.text
    finally:
        await pool.close_all()


async def test_pool_with_zero_proxies_logs_only_the_disabled_notice(proxy_pool_of, caplog):
    from copycord.common import token_proxy

    caplog.set_level(logging.DEBUG, logger="test.token_pool")
    pool, now = proxy_pool_of(set(), total=0)
    try:
        await ensure_all(pool, [("t1", "tok-1"), ("t2", "tok-2")], keep_online=2)
        await until(lambda: caplog.text.count("has no proxy; retrying") >= 6)
        now[0] += 10 * token_pool.PROXY_WARN_INTERVAL
        await until(lambda: caplog.text.count("has no proxy; retrying") >= 12)
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert warnings == [token_proxy.TOKENS_DISABLED_NOTICE]
    finally:
        await pool.close_all()


async def test_other_endings_keep_their_per_session_warning(proxy_pool_of, fakes, caplog):
    caplog.set_level(logging.WARNING, logger="test.token_pool")
    pool, _now = proxy_pool_of({"tok-1"}, total=1)
    fakes.fail("login", "tok-1", RuntimeError("boom"), RuntimeError("boom"))
    try:
        await pool.ensure("t1", "tok-1", keep_online=1, presence="online")
        await until(lambda: pool.online_ids() == {"t1"})
        assert caplog.text.count("ended (RuntimeError); retrying in") == 2
        assert _waiting_lines(caplog) == []
    finally:
        await pool.close_all()


# focus: every pool session's gateway is a focused tab


def _focus_session(**gateway):
    from types import SimpleNamespace

    from copycord.server.token_pool import _Session

    sess = _Session("k", "tok-focus", "online")
    sess.client = SimpleNamespace(
        is_ready=lambda: True,
        is_closed=lambda: False,
        gateway=SimpleNamespace(**gateway),
    )
    return sess


async def test_pool_clients_get_the_foreground_gateway():
    from copycord.selfclient.gateway import GatewayClient
    from copycord.server import token_pool

    assert token_pool._FOREGROUND_GATEWAY.func is GatewayClient
    assert token_pool._FOREGROUND_GATEWAY.keywords == {"foreground": True}
    gw = token_pool._FOREGROUND_GATEWAY("tok-not-real")
    assert gw.foreground_enabled is True
    assert GatewayClient("tok-not-real").foreground_enabled is False


async def test_a_session_sends_once_focused_or_past_the_grace():
    from copycord.server.token_pool import FOREGROUND_GRACE

    sess = _focus_session(
        foreground_enabled=True, foregrounded=False, heartbeat_session_id=None, ready_at=100.0
    )
    assert sess.is_online()
    assert not sess.is_sendable(now=100.0 + FOREGROUND_GRACE - 0.1)
    assert sess.is_sendable(now=100.0 + FOREGROUND_GRACE)
    assert sess.heartbeat_session_id() is None
    sess.client.gateway.foregrounded = True
    sess.client.gateway.heartbeat_session_id = "hb"
    assert sess.is_sendable(now=100.0)
    assert sess.heartbeat_session_id() == "hb"
    sess.dropped = True
    assert not sess.is_sendable(now=1e9) and sess.heartbeat_session_id() is None


@pytest.mark.asyncio
async def test_pool_reads_focus_by_token_key():
    from copycord.server.token_pool import TokenSessionPool, _token_key

    pool = TokenSessionPool(
        logger=logging.getLogger("t"), proxy_for=lambda _t: None, proxy_pool=object()
    )
    sess = _focus_session(
        foreground_enabled=True, foregrounded=True, heartbeat_session_id="hb-k", ready_at=0.0
    )
    key = _token_key("tok-focus")
    sess.key = key
    pool._sessions[key] = sess
    pool._key_by_id["7"] = key
    assert pool.sendable_ids() == {"7"}
    assert pool.heartbeat_session_id("tok-focus") == "hb-k"
    assert pool.heartbeat_session_id("other") is None
    sess.client.gateway.foregrounded = False
    sess.client.gateway.ready_at = time.monotonic()
    assert pool.sendable_ids() == set()
    assert pool.online_ids() == {"7"}
    assert pool.heartbeat_session_id("tok-focus") is None
