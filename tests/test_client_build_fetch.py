"""
Pinning the client's Discord build number before it logs in.

discord.py-self resolved this itself, single-shot, through whichever proxy it
was constructed with. Any failure fell through to FALLBACK_BUILD_NUMBER --
9999, a build Discord has never shipped -- so one slow proxy at startup made
the whole session identify as something that cannot exist. The client now
resolves it with retries and pins it into selfclient.fingerprint, which every
REST header and the gateway IDENTIFY are built from.

These drive the real resolver.
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
        # One source for the build and the browser version means they cannot
        # disagree.
        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        build = fingerprint.get_build_info()
        assert build["browser_version"] == "150.0.0.0"
        assert "Chrome/150" in build["browser_user_agent"]


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
        assert self.bot.proxy == "http://p2:1"

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
        assert fingerprint.get_build_info()["client_build_number"] == 600590
        assert len(calls) == 3


class TestItSurvivesAReconnect:
    @pytest.mark.asyncio
    async def test_the_pin_outlives_a_fresh_client(self, monkeypatch):
        # A restart builds a new Client; the pin lives in the shared
        # fingerprint, not on the Client, so the new one identifies with it.
        self, _ = _listener(monkeypatch)
        await self._resolve_client_build()
        self.bot = Client(proxy=None, refresh_build=False)
        props = fingerprint.gateway_properties("tok")
        assert props["client_build_number"] == 600590
        assert "Chrome/150" in fingerprint.build_headers("tok")["User-Agent"]

    @pytest.mark.asyncio
    async def test_no_third_party_call_is_made(self, monkeypatch):
        # Pinning is a local set_build_info: no properties API is called.
        self, calls = _listener(monkeypatch)
        await self._resolve_client_build()
        assert calls == [1]
