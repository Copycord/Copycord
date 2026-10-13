"""Pinning the client's Discord build number before login.

A slow proxy used to leave the session on a fallback build that never existed.
"""
import pytest

import copycord.client.client as cc  # noqa: E402
from copycord.selfclient import Client, fingerprint

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
    self.bot = Client(proxy=None, refresh_build=False)

    calls = []
    queue = list(results if results is not None else [BUILD])

    async def fake_refresh():
        calls.append(1)
        outcome = queue.pop(0) if queue else BUILD
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(cc, "refresh_build_info", fake_refresh)
    # The pin is process-wide; put the shared build back after each test.
    saved = fingerprint.get_build_info()
    monkeypatch.setattr(
        fingerprint.selfbot_headers, "_BUILD", dict(saved), raising=True
    )
    monkeypatch.setattr(fingerprint.selfbot_headers, "_FINGERPRINT_CACHE", {})
    return self, calls


class TestItPinsTheLiveBuild:
    @pytest.mark.asyncio
    async def test_a_resolved_build_is_applied(self, monkeypatch):
        self, _ = _listener(monkeypatch)
        assert await self._resolve_client_build() is True
        assert fingerprint.get_build_info()["client_build_number"] == 600590

    @pytest.mark.asyncio
    async def test_it_never_ships_the_impossible_fallback(self, monkeypatch):
        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        props = fingerprint.gateway_properties("tok")
        assert props["client_build_number"] != 9999

    @pytest.mark.asyncio
    async def test_the_browser_version_comes_from_the_same_build(self, monkeypatch):
        # One source for the build and the browser version, so they cannot disagree.
        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        build = fingerprint.get_build_info()
        assert build["browser_version"] == "150.0.0.0"
        assert "Chrome/150" in build["browser_user_agent"]


class TestItRetriesAcrossProxies:
    @pytest.mark.asyncio
    async def test_it_rotates_and_retries_on_failure(self, monkeypatch):
        # A single slow proxy used to poison the whole session.
        rot = _Rotator()
        self, calls = _listener(
            monkeypatch,
            rotator=rot,
            results=[OSError("refused"), OSError("refused"), BUILD],
        )
        assert await self._resolve_client_build() is True
        assert len(calls) == 3
        assert len(rot.rotations) == 2
        assert self.bot.proxy == "http://p2:1"

    @pytest.mark.asyncio
    async def test_it_gives_up_after_the_configured_attempts(self, monkeypatch):
        rot = _Rotator()
        self, calls = _listener(monkeypatch, rotator=rot, results=[OSError("x")] * 5)
        assert await self._resolve_client_build() is False
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_failure_is_not_fatal(self, monkeypatch):
        # A False return falls back as before and must never raise.
        self, _ = _listener(monkeypatch, results=[OSError("x")] * 5)
        assert await self._resolve_client_build() is False

    @pytest.mark.asyncio
    async def test_it_still_retries_without_a_proxy_pool(self, monkeypatch):
        # Direct connections get the retries too.
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
        # A number below the sanity floor counts as a failed fetch, not a pin.
        low = dict(BUILD, client_build_number=9999)
        self, calls = _listener(monkeypatch, results=[low, low, BUILD])
        assert await self._resolve_client_build() is True
        assert fingerprint.get_build_info()["client_build_number"] == 600590
        assert len(calls) == 3


class TestItSurvivesAReconnect:
    @pytest.mark.asyncio
    async def test_the_pin_outlives_a_fresh_client(self, monkeypatch):
        # The pin lives in the shared fingerprint, so a new Client after a restart uses it.
        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        self.bot = Client(proxy=None, refresh_build=False)
        props = fingerprint.gateway_properties("tok")
        assert props["client_build_number"] == 600590
        assert "Chrome/150" in fingerprint.build_headers("tok")["User-Agent"]

    @pytest.mark.asyncio
    async def test_no_third_party_call_is_made(self, monkeypatch):
        self, calls = _listener(monkeypatch)
        await self._resolve_client_build()
        assert calls == [1]
