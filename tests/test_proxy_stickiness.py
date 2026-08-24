"""
A token keeps the same proxy across sessions.

The lease was already sticky for the life of a TLS session, but release()
dropped it, so a token reconnecting after its session was reaped got a fresh
random proxy — the same account appearing from a different country every hour.
The proxy it last used is now remembered and preferred.

It is a preference, not a reservation: a remembered proxy that someone else
holds is not waited for, which is what lets a small pool serve far more tokens
than it has entries.
"""
import time

import pytest

from common.proxy_pool import ProxyPool

PROXIES = [f"http://u{i}:p{i}@host:8080" for i in range(4)]


def _pool(lines=None):
    p = ProxyPool()
    p.reload(lines if lines is not None else PROXIES)
    return p


class TestStickyAcrossSessions:
    @pytest.mark.asyncio
    async def test_same_token_gets_the_same_proxy_after_release(self):
        p = _pool()
        first = await p.lease("tokA")
        await p.release("tokA")
        assert await p.lease("tokA") == first

    @pytest.mark.asyncio
    async def test_a_released_proxy_is_still_available_to_others(self):
        # Stickiness must not turn a release into a reservation.
        p = _pool()
        a = await p.lease("tokA")
        await p.release("tokA")
        p._preferred["tokB"] = a
        assert await p.lease("tokB") == a

    @pytest.mark.asyncio
    async def test_preference_yields_when_another_token_holds_it(self):
        p = _pool()
        a = await p.lease("tokA")
        await p.release("tokA")
        p._preferred["tokB"] = a
        await p.lease("tokB")
        again = await p.lease("tokA")
        assert again is not None and again != a

    @pytest.mark.asyncio
    async def test_the_replacement_becomes_the_new_preference(self):
        # Otherwise the token would keep reaching for a proxy it cannot have
        # and drift to a different one every reconnect.
        p = _pool()
        a = await p.lease("tokA")
        await p.release("tokA")
        p._preferred["tokB"] = a
        await p.lease("tokB")
        moved = await p.lease("tokA")
        await p.release("tokA")
        assert await p.lease("tokA") == moved


class TestPoolIsStillShared:
    @pytest.mark.asyncio
    async def test_more_tokens_than_proxies_are_served_over_time(self):
        p = _pool()
        for i in range(20):
            got = await p.lease(f"tok{i}")
            assert got is not None
            await p.release(f"tok{i}")

    @pytest.mark.asyncio
    async def test_exclusivity_still_holds(self):
        p = _pool()
        held = {await p.lease(f"t{i}") for i in range(4)}
        assert len(held) == 4
        # A fifth token gets nothing rather than a shared proxy.
        assert await p.lease("t4") is None


class TestFailureRotation:
    @pytest.mark.asyncio
    async def test_a_failed_proxy_is_not_handed_straight_back(self):
        # Suspension only starts after several failures, so without dropping
        # the preference the sticky path would return the proxy that just
        # failed.
        p = _pool()
        first = await p.lease("tokF")
        replacement = await p.report_failure("tokF")
        assert replacement is not None and replacement != first

    @pytest.mark.asyncio
    async def test_the_replacement_is_what_sticks(self):
        p = _pool()
        await p.lease("tokF")
        replacement = await p.report_failure("tokF")
        await p.release("tokF")
        assert await p.lease("tokF") == replacement

    @pytest.mark.asyncio
    async def test_single_proxy_pool_returns_nothing_rather_than_the_failed_one(self):
        p = _pool([PROXIES[0]])
        await p.lease("solo")
        assert await p.report_failure("solo") is None

    @pytest.mark.asyncio
    async def test_one_failure_does_not_suspend(self):
        p = _pool()
        first = await p.lease("tokH")
        await p.report_failure("tokH")
        assert not p._is_suspended(first, time.monotonic())

    @pytest.mark.asyncio
    async def test_repeated_failures_on_one_proxy_suspend_it(self):
        # Failures are counted per PROXY. Because report_failure rotates away,
        # they only accumulate on one proxy if it keeps being handed back --
        # which is what a single-entry pool forces.
        p = _pool([PROXIES[0]])
        for _ in range(p.MAX_FAILURES):
            await p.lease("tokS")
            await p.report_failure("tokS")
        assert p._is_suspended(PROXIES[0], time.monotonic())
        assert await p.lease("other") is None


class TestReload:
    @pytest.mark.asyncio
    async def test_preference_for_a_removed_proxy_is_forgotten(self):
        # Nothing else would ever clear it.
        p = _pool()
        got = await p.lease("tokR")
        await p.release("tokR")
        p.reload([x for x in PROXIES if x != got])
        assert "tokR" not in p._preferred
        replacement = await p.lease("tokR")
        assert replacement in p._all and replacement != got
