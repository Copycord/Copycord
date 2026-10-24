"""A failed exit-timezone lookup rotates the token to another proxy. Lookups and pools are faked."""

import asyncio

import pytest

from copycord.common import exit_timezone as et
from copycord.common import selfbot_headers as sh
from copycord.common import token_proxy
from copycord.common.proxy_pool import ProxyPool
from copycord.common.token_proxy import NoProxyAvailable, lease_with_timezone
from copycord.selfclient.client import Client
from copycord.selfclient.gateway import GatewayClient
from copycord.selfclient.http import HTTPClient

PROXIES = [
    "http://user:secret@10.0.0.1:8000",
    "http://user:secret@10.0.0.2:8000",
    "http://user:secret@10.0.0.3:8000",
]


def run(coro):
    return asyncio.run(coro)


def _pool(n=3):
    pool = ProxyPool()
    pool.reload(PROXIES[:n])
    assert pool.total == n
    return pool


def _fake(monkeypatch, answers):
    """answers: proxy -> zone or Exception; returns the list of proxies looked up."""
    calls = []

    async def fetch(proxy, timeout):
        calls.append(proxy)
        a = answers[proxy]
        if isinstance(a, Exception):
            raise a
        return {"timezone": a}

    monkeypatch.setattr(et, "_fetch_json", fetch)
    return calls


def _count_failures(monkeypatch, pool):
    reported = []
    real = pool.report_failure

    async def report_failure(token):
        reported.append(pool.current(token))
        return await real(token)

    monkeypatch.setattr(pool, "report_failure", report_failure)
    return reported


class TestRotation:
    def test_failed_lookup_rotates_and_header_carries_the_new_zone(self, monkeypatch):
        pool = _pool(2)
        first = run(pool.lease("tok"))
        other = next(p for p in pool._all if p != first)
        _fake(monkeypatch, {first: OSError("dead"), other: "Asia/Tokyo"})
        reported = _count_failures(monkeypatch, pool)
        monkeypatch.setattr(sh, "_get_proxy_pool", lambda: pool)

        assert run(sh.require_token_proxy("tok")) == other
        assert pool.current("tok") == other
        assert reported == [first]
        assert pool._health[first]["failures"] == 1
        assert sh.build_headers("tok")["X-Discord-Timezone"] == "Asia/Tokyo"

    def test_every_proxy_failing_refuses_the_token_without_a_fallback(self, monkeypatch):
        pool = _pool(3)
        _fake(monkeypatch, {p: OSError("dead") for p in PROXIES})
        reported = _count_failures(monkeypatch, pool)
        monkeypatch.setattr(sh, "_get_proxy_pool", lambda: pool)

        with pytest.raises(NoProxyAvailable):
            run(sh.require_token_proxy("tok"))
        assert pool.current("tok") is None
        assert sorted(reported) == sorted(PROXIES)
        with pytest.raises(NoProxyAvailable):
            sh.build_headers("tok")
        with pytest.raises(et.ExitTimezoneUnavailable):
            et.timezone_for("tok")

    def test_loop_is_bounded_by_the_pool_size(self, monkeypatch):
        pool = _pool(3)
        calls = _fake(monkeypatch, {p: OSError("dead") for p in PROXIES})
        reported = _count_failures(monkeypatch, pool)
        # a fresh negative cache each time, so repeats would show up as lookups
        monkeypatch.setattr(et, "NEGATIVE_TTL", -1.0)

        with pytest.raises(NoProxyAvailable):
            run(lease_with_timezone(pool, "tok"))
        assert len(calls) == pool.total
        assert len(reported) == pool.total

    def test_loop_is_bounded_by_the_time_cap(self, monkeypatch):
        pool = _pool(3)
        calls = _fake(monkeypatch, {p: OSError("dead") for p in PROXIES})

        with pytest.raises(NoProxyAvailable):
            run(lease_with_timezone(pool, "tok", time_cap=0))
        assert len(calls) == 1
        assert pool.current("tok") is None

    def test_no_proxies_configured_raises(self):
        with pytest.raises(NoProxyAvailable):
            run(lease_with_timezone(ProxyPool(), "tok"))


class TestNegativeCache:
    def test_miss_is_cached_briefly_and_not_for_the_direct_window(self, monkeypatch):
        calls = _fake(monkeypatch, {PROXIES[0]: OSError("dead")})

        async def go():
            return [await et.resolve_exit_timezone(PROXIES[0]) for _ in range(3)]

        assert run(go()) == [None, None, None]
        assert calls == [PROXIES[0]]
        _zone, retry_at = et._CACHE[PROXIES[0]]
        import time

        assert retry_at - time.monotonic() <= et.NEGATIVE_TTL < et.RETRY_AFTER

    def test_miss_is_looked_up_again_after_the_window(self, monkeypatch):
        calls = _fake(monkeypatch, {PROXIES[0]: OSError("dead")})
        monkeypatch.setattr(et, "NEGATIVE_TTL", -1.0)

        async def go():
            await et.resolve_exit_timezone(PROXIES[0])
            await et.resolve_exit_timezone(PROXIES[0])

        run(go())
        assert calls == [PROXIES[0], PROXIES[0]]

    def test_second_token_on_a_dead_proxy_does_not_look_it_up_again(self, monkeypatch):
        pool = _pool(2)
        dead = run(pool.lease("a"))
        good = next(p for p in pool._all if p != dead)
        calls = _fake(monkeypatch, {dead: OSError("dead"), good: "Europe/Paris"})
        assert run(lease_with_timezone(pool, "a")) == good
        # "a" holds the good proxy, so "b" gets the dead one: the cached miss refuses it with no new lookup
        with pytest.raises(NoProxyAvailable):
            run(lease_with_timezone(pool, "b"))
        assert calls.count(dead) == 1


class TestCallers:
    def test_http_client_refuses_without_sending(self, monkeypatch):
        _fake(monkeypatch, {PROXIES[0]: OSError("dead")})

        class Session:
            calls = []

            async def request(self, *a, **kw):
                self.calls.append((a, kw))
                raise AssertionError("must not send")

        sess = Session()
        http = HTTPClient("tok", session=sess, proxy=PROXIES[0])
        with pytest.raises(NoProxyAvailable):
            run(http.request("GET", "/users/@me"))
        assert sess.calls == []

    def test_http_client_on_a_pool_lease_rotates(self, monkeypatch):
        pool = _pool(2)
        first = run(pool.lease("tok"))
        other = next(p for p in pool._all if p != first)
        _fake(monkeypatch, {first: OSError("dead"), other: "Asia/Tokyo"})
        monkeypatch.setattr("copycord.selfclient.http.get_proxy_pool", lambda: pool)

        class Resp:
            status_code = 200
            headers = {}
            content = b"{}"
            text = "{}"

            def json(self):
                return {}

        seen = []

        class Session:
            async def request(self, method, url, **kw):
                seen.append((kw.get("proxy"), kw["headers"].get("X-Discord-Timezone")))
                return Resp()

        http = HTTPClient("tok", session=Session(), proxy=first)
        try:
            run(http.request("GET", "/users/@me"))
        except Exception:
            pass
        assert http.proxy == other
        assert seen and seen[0] == (other, "Asia/Tokyo")

    def test_gateway_dial_failure_rotates_and_stays_on_the_rest_lease(self, monkeypatch):
        pool = _pool(2)
        _fake(monkeypatch, {p: "Europe/Paris" for p in PROXIES})
        gw = GatewayClient("tok", proxy_pool=pool)

        async def go():
            first = await gw._current_proxy()
            await gw._rotate_proxy()
            second = await gw._current_proxy()
            return first, second

        first, second = run(go())
        assert first != second
        assert second == pool.current("tok")

    def test_gateway_rotates_past_a_proxy_whose_lookup_fails(self, monkeypatch):
        pool = _pool(2)
        first = run(pool.lease("tok"))
        other = next(p for p in pool._all if p != first)
        _fake(monkeypatch, {first: OSError("dead"), other: "Asia/Tokyo"})
        gw = GatewayClient("tok", proxy_pool=pool)
        assert run(gw._current_proxy()) == other
        assert pool.current("tok") == other

    def test_gateway_without_a_pool_refuses_a_proxy_that_does_not_resolve(self, monkeypatch):
        _fake(monkeypatch, {PROXIES[0]: OSError("dead")})
        gw = GatewayClient("tok", proxy=PROXIES[0])
        from copycord.selfclient.gateway import GatewayDialError

        with pytest.raises(GatewayDialError):
            run(gw._current_proxy())

    def test_client_rest_follows_the_gateway_rotation(self):
        client = Client(proxy=PROXIES[0], connector=None)
        client.http = HTTPClient("tok", proxy=PROXIES[0])

        async def connector(url, headers, proxy):
            return object()

        client._connector = connector
        run(client._tracked_connector("wss://gateway.invalid", {}, PROXIES[1]))
        assert client.proxy == PROXIES[1]
        assert client.http.proxy == PROXIES[1]


class TestDirectUnchanged:
    def test_direct_lookup_still_falls_back_and_waits_the_long_window(self):
        import time

        assert run(et.ensure_timezone(None)) == et.FALLBACK_TIMEZONE
        _zone, retry_at = et._CACHE[None]
        assert retry_at - time.monotonic() > et.NEGATIVE_TTL

    def test_direct_token_header_uses_the_fallback(self):
        assert run(et.note_token_proxy("tok", None)) == et.FALLBACK_TIMEZONE
        assert sh.build_headers("tok")["X-Discord-Timezone"] == et.FALLBACK_TIMEZONE

    def test_unknown_token_header_uses_the_fallback(self):
        assert sh.build_headers("never-seen")["X-Discord-Timezone"] == et.FALLBACK_TIMEZONE

    def test_time_cap_default(self):
        assert token_proxy.ROTATE_TIME_CAP == 30.0
