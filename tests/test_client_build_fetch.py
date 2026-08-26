"""
Pinning the client's Discord build number before it logs in.

discord.py-self resolves this itself, single-shot, through whichever proxy it
was constructed with. Any failure falls through to FALLBACK_BUILD_NUMBER --
9999, a build Discord has never shipped -- so one slow proxy at startup made
the whole session identify as something that cannot exist. The browser version
falls back separately to Chrome 136, so a partial failure is its own tell.

These drive the real resolver.
"""
import types

import pytest

pytest.importorskip("discord", reason="client module needs a discord library")

# utils.Headers is discord.py-self's. CI installs py-cord, which has no such
# class, so there is nothing there to pin a build onto -- and the production
# client only ever runs on discord.py-self.
_discord_utils = pytest.importorskip("discord.utils")
if not hasattr(_discord_utils, "Headers"):
    pytest.skip(
        "discord.py-self only: py-cord has no utils.Headers",
        allow_module_level=True,
    )

import client.client as cc  # noqa: E402

BUILD = {
    "release_channel": "stable",
    "browser_user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
    ),
    "browser_version": "150.0.0.0",
    "client_build_number": 600590,
}


class _Rotator:
    def __init__(self, enabled=True, exhausted=False):
        self.enabled = enabled
        self._current_proxy = "http://p0:1"
        self.rotations = []
        self._exhausted = exhausted

    def rotate_now(self):
        if self._exhausted:
            return None
        self.rotations.append(1)
        self._current_proxy = f"http://p{len(self.rotations)}:1"
        return self._current_proxy


def _listener(monkeypatch, *, rotator=None, results=None, attempts=3):
    """A bare listener carrying just the fields the resolver reads."""
    self = cc.ClientListener.__new__(cc.ClientListener)
    self.proxy_rotator = rotator or _Rotator()
    self._initial_proxy = "http://p0:1"
    self._build_fetch_attempts = attempts
    self._build_fetch_timeout = 5
    self._build_pinned = False
    self.bot = types.SimpleNamespace(http=types.SimpleNamespace(proxy=None))

    calls = []
    queue = list(results if results is not None else [BUILD])

    async def fake_refresh():
        calls.append(1)
        outcome = queue.pop(0) if queue else BUILD
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(cc, "refresh_build_info", fake_refresh)
    return self, calls


class TestItPinsTheLiveBuild:
    @pytest.mark.asyncio
    async def test_a_resolved_build_is_applied(self, monkeypatch):
        self, _ = _listener(monkeypatch)
        assert await self._resolve_client_build() is True
        props = self.bot.http.headers.super_properties
        assert props["client_build_number"] == 600590

    @pytest.mark.asyncio
    async def test_it_never_ships_the_impossible_fallback(self, monkeypatch):
        from discord.utils import Headers

        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        props = self.bot.http.headers.super_properties
        assert props["client_build_number"] != Headers.FALLBACK_BUILD_NUMBER
        assert props["client_build_number"] != 9999

    @pytest.mark.asyncio
    async def test_the_browser_version_comes_from_the_same_build(self, monkeypatch):
        # The library resolves the build and the browser version with two
        # independent calls, so one could succeed while the other fell back to
        # Chrome 136. One source means they cannot disagree.
        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        h = self.bot.http.headers
        assert h.super_properties["browser_version"] == "150.0.0.0"
        assert h.major_version == 150
        assert "Chrome/150" in h.user_agent


class TestItRetriesAcrossProxies:
    @pytest.mark.asyncio
    async def test_it_rotates_and_retries_on_failure(self, monkeypatch):
        # The reported bug: a single slow proxy poisoned the whole session.
        rot = _Rotator()
        self, calls = _listener(
            monkeypatch,
            rotator=rot,
            results=[OSError("refused"), OSError("refused"), BUILD],
        )
        assert await self._resolve_client_build() is True
        assert len(calls) == 3
        assert len(rot.rotations) == 2
        assert self.bot.http.proxy == "http://p2:1"

    @pytest.mark.asyncio
    async def test_it_gives_up_after_the_configured_attempts(self, monkeypatch):
        rot = _Rotator()
        self, calls = _listener(monkeypatch, rotator=rot, results=[OSError("x")] * 5)
        assert await self._resolve_client_build() is False
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_failure_is_not_fatal(self, monkeypatch):
        # A False return means the library falls back exactly as it did
        # before, which is worse but still connects. It must never raise.
        self, _ = _listener(monkeypatch, results=[OSError("x")] * 5)
        assert await self._resolve_client_build() is False

    @pytest.mark.asyncio
    async def test_it_still_retries_without_a_proxy_pool(self, monkeypatch):
        # Direct connections get the retries too; the transient failure the
        # retry covers is not exclusively a proxy problem.
        self, calls = _listener(
            monkeypatch,
            rotator=_Rotator(enabled=False),
            results=[OSError("x"), BUILD],
        )
        assert await self._resolve_client_build() is True
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_exhausted_pool_does_not_stop_the_remaining_attempts(
        self, monkeypatch
    ):
        rot = _Rotator(exhausted=True)
        self, calls = _listener(
            monkeypatch, rotator=rot, results=[OSError("x"), OSError("x"), BUILD]
        )
        assert await self._resolve_client_build() is True
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_an_implausible_build_is_refused(self, monkeypatch):
        # A number below the sanity floor is treated as a failed fetch rather
        # than applied: otherwise a bad scrape is indistinguishable from a good
        # one and we would pin garbage with full confidence.
        low = dict(BUILD, client_build_number=9999)
        self, calls = _listener(monkeypatch, results=[low, low, BUILD])
        assert await self._resolve_client_build() is True
        assert self.bot.http.headers.super_properties["client_build_number"] == 600590
        assert len(calls) == 3


class TestItSurvivesAReconnect:
    @pytest.mark.asyncio
    async def test_the_pin_outlives_httpclient_startup(self, monkeypatch):
        # Why this patches Headers.default instead of http.headers:
        # HTTPClient.startup() reassigns self.headers unconditionally, and it
        # runs again on every reconnect because _clear_bot_state has to reset
        # _started to make the client restartable. Assigning the attribute
        # would survive only until the first retry -- exactly when a proxy is
        # least healthy and the fallback hurts most.
        import aiohttp
        from discord.utils import Headers

        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()

        async with aiohttp.ClientSession() as s:
            refetched = await Headers.default(s, None, None)
        assert refetched.super_properties["client_build_number"] == 600590

    @pytest.mark.asyncio
    async def test_no_third_party_call_is_made(self, monkeypatch):
        # Headers.default() POSTs to a third-party properties API first. That
        # put an outside host on the critical path of our identity and showed
        # it our proxy egress IP on every boot.
        import aiohttp
        from discord.utils import Headers

        hits = []

        async def spy(*a, **k):
            hits.append(1)
            raise AssertionError("third-party properties API was called")

        monkeypatch.setattr(Headers, "get_api_properties", staticmethod(spy))

        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        async with aiohttp.ClientSession() as s:
            await Headers.default(s, None, None)
        assert hits == []

    @pytest.mark.asyncio
    async def test_launch_ids_are_fresh_per_connect(self, monkeypatch):
        # A real browser mints these per page load, so reusing one across
        # reconnects would be its own signal.
        import aiohttp
        from discord.utils import Headers

        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        first = self.bot.http.headers.super_properties["client_launch_id"]
        async with aiohttp.ClientSession() as s:
            second = (await Headers.default(s, None, None)).super_properties[
                "client_launch_id"
            ]
        assert first != second
