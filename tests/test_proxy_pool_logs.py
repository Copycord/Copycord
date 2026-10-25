"""
The proxy pool warns once about exhaustion, however many tokens ask, and a reload drops
the leases on proxies that were removed from the list.
"""
import logging

import pytest

from copycord.common import proxy_pool
from copycord.common.proxy_pool import ProxyPool

PROXIES = [f"http://u{i}:p{i}@host{i}:8080" for i in range(3)]


def _pool(lines=None):
    p = ProxyPool()
    p.reload(lines if lines is not None else PROXIES)
    return p


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _exhausted(caplog, level):
    return [r for r in caplog.records if r.levelno == level and "Proxy pool exhausted" in r.getMessage()]


class TestExhaustedWarning:
    @pytest.mark.asyncio
    async def test_many_tokens_asking_log_one_warning(self, caplog):
        p = _pool()
        p._clock = _Clock()
        for i in range(3):
            assert await p.lease(f"held{i}")
        with caplog.at_level(logging.DEBUG, logger="common.proxy_pool"):
            for i in range(10):
                assert await p.lease(f"waiting{i}") is None
        assert len(_exhausted(caplog, logging.WARNING)) == 1
        assert len(_exhausted(caplog, logging.DEBUG)) == 9
        assert "3 total, 3 leased" in _exhausted(caplog, logging.WARNING)[0].getMessage()

    @pytest.mark.asyncio
    async def test_warns_again_after_the_interval(self, caplog):
        p = _pool()
        clock = _Clock()
        p._clock = clock
        for i in range(3):
            await p.lease(f"held{i}")
        with caplog.at_level(logging.DEBUG, logger="common.proxy_pool"):
            await p.lease("a")
            clock.now += proxy_pool.EXHAUSTED_WARN_INTERVAL - 1
            await p.lease("b")
            clock.now += 2
            await p.lease("c")
        assert len(_exhausted(caplog, logging.WARNING)) == 2

    @pytest.mark.asyncio
    async def test_no_warning_while_a_proxy_is_free(self, caplog):
        p = _pool()
        with caplog.at_level(logging.DEBUG, logger="common.proxy_pool"):
            assert await p.lease("a")
        assert not _exhausted(caplog, logging.WARNING)
        assert not _exhausted(caplog, logging.DEBUG)


class TestReloadDropsRemovedLeases:
    @pytest.mark.asyncio
    async def test_a_lease_on_a_removed_proxy_is_dropped(self):
        p = _pool()
        leased = await p.lease("tokA")
        remaining = [line for line in PROXIES if proxy_pool._normalise_proxy_url(line) != leased]
        assert len(remaining) == 2
        p.reload(remaining)
        assert p.current("tokA") is None
        assert leased not in p._in_use
        assert leased not in p._all
        new = await p.lease("tokA")
        assert new in p._all and new != leased

    @pytest.mark.asyncio
    async def test_leases_on_proxies_still_listed_survive(self):
        p = _pool()
        leased = await p.lease("tokA")
        p.reload(list(PROXIES))
        assert p.current("tokA") == leased
        assert leased in p._in_use

    @pytest.mark.asyncio
    async def test_emptying_the_list_drops_every_lease(self):
        p = _pool()
        await p.lease("tokA")
        await p.lease("tokB")
        p.reload([])
        assert p.current("tokA") is None and p.current("tokB") is None
        assert not p._in_use
        assert not p.enabled

    @pytest.mark.asyncio
    async def test_reload_logs_how_many_leases_were_dropped(self, caplog):
        p = _pool()
        leased = await p.lease("tokA")
        remaining = [line for line in PROXIES if proxy_pool._normalise_proxy_url(line) != leased]
        with caplog.at_level(logging.INFO, logger="common.proxy_pool"):
            p.reload(remaining)
        assert any("dropped 1 lease(s) on removed proxies" in r.getMessage() for r in caplog.records)
