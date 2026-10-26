"""LoginLimiter: per exit IP login pacing, on a virtual clock (no real waits)."""
import asyncio
import heapq
import itertools
import logging

import pytest

from copycord.common import login_limiter
from copycord.common.login_limiter import (
    CAP_INTERVAL,
    FLOOR_INTERVAL,
    INITIAL_INTERVAL,
    MIN_PAUSE,
    LoginLimiter,
)

pytestmark = pytest.mark.asyncio


class VirtualTime:
    """A clock that only moves when every task is parked in sleep()."""

    def __init__(self):
        self.now = 0.0
        self._timers = []
        self._seq = itertools.count()

    def clock(self):
        return self.now

    async def sleep(self, delay):
        fut = asyncio.get_running_loop().create_future()
        heapq.heappush(self._timers, (self.now + max(0.0, delay), next(self._seq), fut))
        await fut

    async def _settle(self):
        for _ in range(20):
            await asyncio.sleep(0)

    async def run(self, *aws):
        tasks = [asyncio.ensure_future(a) for a in aws]
        while True:
            await self._settle()
            if all(t.done() for t in tasks):
                break
            while self._timers and self._timers[0][2].done():
                heapq.heappop(self._timers)
            if not self._timers:
                raise AssertionError("tasks are stuck with no timer pending")
            when, _, fut = heapq.heappop(self._timers)
            self.now = max(self.now, when)
            fut.set_result(None)
        return [t.result() for t in tasks]


@pytest.fixture()
def vt():
    return VirtualTime()


@pytest.fixture()
def limiter(vt):
    return LoginLimiter(clock=vt.clock, sleep=vt.sleep)


async def test_slots_on_one_key_are_one_interval_apart(vt, limiter):
    times = []

    async def one():
        await limiter.acquire("direct")
        times.append(vt.now)

    await vt.run(*(one() for _ in range(4)))
    assert times == pytest.approx([0.0, INITIAL_INTERVAL, 2 * INITIAL_INTERVAL, 3 * INITIAL_INTERVAL])


async def test_two_keys_proceed_in_parallel(vt, limiter):
    times = {"a": [], "b": []}

    async def one(key):
        await limiter.acquire(key)
        times[key].append(vt.now)

    await vt.run(*(one(k) for k in ("a", "b", "a", "b", "a", "b")))
    expected = [0.0, INITIAL_INTERVAL, 2 * INITIAL_INTERVAL]
    assert times["a"] == pytest.approx(expected)
    assert times["b"] == pytest.approx(expected)
    assert vt.now == pytest.approx(2 * INITIAL_INTERVAL)


async def test_a_paused_key_does_not_hold_up_another(vt, limiter):
    limiter.on_rate_limited("slow", retry_after=30)
    times = {}

    async def one(key):
        await limiter.acquire(key)
        times[key] = vt.now

    await vt.run(one("slow"), one("fast"))
    assert times["fast"] == 0.0
    assert times["slow"] == pytest.approx(30.0)


async def test_a_thousand_waiters_on_one_key_go_in_order(vt, limiter):
    order = []

    async def one(i):
        await limiter.acquire("http://user:pass@proxy.invalid:1")
        order.append((i, vt.now))

    await vt.run(*(one(i) for i in range(1000)))
    assert [i for i, _ in order] == list(range(1000))
    assert order[-1][1] == pytest.approx(999 * INITIAL_INTERVAL)


async def test_rate_limit_doubles_and_pauses_success_recovers(vt, limiter):
    key = "direct"
    limiter.on_rate_limited(key, retry_after=None)
    assert limiter.interval(key) == pytest.approx(2 * INITIAL_INTERVAL)

    # a second 429 inside the same pause window does not slow the key again
    limiter.on_rate_limited(key, retry_after=0.0)
    assert limiter.interval(key) == pytest.approx(2 * INITIAL_INTERVAL)

    got = []

    async def one():
        await limiter.acquire(key)
        got.append(vt.now)

    await vt.run(one())
    assert got == [pytest.approx(MIN_PAUSE)]

    # a retry-after longer than the short pause wins
    limiter.on_rate_limited(key, retry_after=5.0)
    assert limiter.interval(key) == pytest.approx(4 * INITIAL_INTERVAL)
    start = vt.now
    await vt.run(one())
    assert got[-1] - start == pytest.approx(5.0)

    for _ in range(40):
        limiter.on_success(key)
    assert limiter.interval(key) == pytest.approx(FLOOR_INTERVAL)


async def test_interval_is_capped_and_recovers_within_a_few_dozen_successes(vt, limiter):
    key = "direct"
    for _ in range(20):
        limiter.on_rate_limited(key)
        vt.now += 100  # past each pause, so every 429 counts
    assert limiter.interval(key) == CAP_INTERVAL
    for n in range(1, 60):
        limiter.on_success(key)
        if limiter.interval(key) <= INITIAL_INTERVAL:
            break
    assert n <= 40
    for _ in range(200):
        limiter.on_success(key)
    assert limiter.interval(key) == FLOOR_INTERVAL


async def test_a_429_seen_while_waiting_pauses_the_waiter(vt, limiter):
    got = []

    async def one():
        await limiter.acquire("direct")
        got.append(vt.now)

    async def limited_later():
        await vt.sleep(0.1)
        limiter.on_rate_limited("direct", retry_after=3.0)

    await vt.run(one(), one(), limited_later())
    assert got[0] == 0.0
    assert got[1] == pytest.approx(3.1)


async def test_a_cancelled_waiter_does_not_block_the_next(vt, limiter):
    got = []

    async def one(name):
        await limiter.acquire("direct")
        got.append((name, vt.now))

    first = asyncio.ensure_future(one("first"))
    stuck = asyncio.ensure_future(one("cancelled"))
    queued = asyncio.ensure_future(one("next"))
    await vt._settle()
    stuck.cancel()
    await vt.run(first, queued)
    assert stuck.cancelled()
    # the cancelled waiter took no slot: the next one gets the second slot
    assert got == [("first", 0.0), ("next", pytest.approx(INITIAL_INTERVAL))]


async def test_a_waiter_cancelled_while_queued_on_the_lock_frees_its_place(vt, limiter):
    got = []

    async def one(name):
        await limiter.acquire("direct")
        got.append(name)

    tasks = [asyncio.ensure_future(one(i)) for i in range(5)]
    await vt._settle()
    tasks[3].cancel()
    await vt.run(*(t for i, t in enumerate(tasks) if i != 3))
    assert got == [0, 1, 2, 4]
    assert vt.now == pytest.approx(3 * INITIAL_INTERVAL)


async def test_proxy_url_never_reaches_the_log(vt, limiter, caplog):
    caplog.set_level(logging.DEBUG, logger=login_limiter.__name__)
    key = "http://user:secret@proxy.invalid:8080"
    limiter.on_rate_limited(key, retry_after=1.0)
    limiter.on_success(key)
    await vt.run(limiter.acquire(key))
    assert "secret" not in caplog.text and "proxy.invalid" not in caplog.text
    assert login_limiter.key_hash(key) in caplog.text


async def test_module_default_uses_patched_clock_and_sleep(monkeypatch, vt):
    monkeypatch.setattr(login_limiter, "_clock", vt.clock)
    monkeypatch.setattr(login_limiter, "_sleep", vt.sleep)
    fresh = LoginLimiter()
    monkeypatch.setattr(login_limiter, "LIMITER", fresh)
    assert login_limiter.get_limiter() is fresh
    await vt.run(fresh.acquire("k"), fresh.acquire("k"))
    assert vt.now == pytest.approx(INITIAL_INTERVAL)
