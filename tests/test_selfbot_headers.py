"""
Unit tests for the shared self-bot header/fingerprint builder.

Cover the desktop super-properties shape, per-account uniqueness, and the
best-effort build-info refresh (which keeps the fingerprint's build number
current with a baked fallback).
"""

import base64
import json

import aiohttp
import pytest

from common import selfbot_headers as sh


@pytest.fixture(autouse=True)
def _restore_build():
    """Snapshot and restore the module-global build so tests don't leak state."""
    original = sh.get_build_info()
    yield
    sh.set_build_info(original)


def _props(token):
    h = sh.build_headers(token)
    return h, json.loads(base64.b64decode(h["X-Super-Properties"]))


class TestFingerprintShape:
    def test_desktop_super_props_shape(self):
        h, props = _props("token-abc")
        assert props["browser"] == "Discord Client"
        assert props["os"] == "Windows"
        # The desktop client reports these keys — a browser fingerprint wouldn't.
        for key in (
            "client_version",
            "has_client_mods",
            "client_launch_id",
            "launch_signature",
            "os_sdk_version",
            "native_build_number",
            "client_build_number",
        ):
            assert key in props
        # UA and the fingerprint's UA agree, and Authorization is the raw token.
        assert h["User-Agent"] == props["browser_user_agent"]
        assert h["Authorization"] == "token-abc"
        # os_sdk_version is the build part of os_version (they must stay in sync).
        assert props["os_version"].endswith(props["os_sdk_version"])

    def test_no_contradictory_browser_headers(self):
        # Desktop identity → we must NOT send browser-only client-hint headers.
        h = sh.build_headers("token-abc")
        for banned in ("Sec-CH-UA", "Sec-CH-UA-Platform", "Origin", "Referer"):
            assert banned not in h
        assert h["X-Debug-Options"] == "bugReporterEnabled"

    def test_unique_per_account_but_shared_build(self):
        _, a = _props("account-1")
        _, b = _props("account-2")
        # Launch ids differ per account (a fleet sharing one is a tell)…
        assert a["client_launch_id"] != b["client_launch_id"]
        assert a["launch_signature"] != b["launch_signature"]
        # …but the build number is the same (all real clients on one build are).
        assert a["client_build_number"] == b["client_build_number"]

    def test_stable_per_account(self):
        _, a1 = _props("stable-account")
        _, a2 = _props("stable-account")
        assert a1 == a2

    def test_default_build_used_without_refresh(self):
        _, props = _props("token-abc")
        assert props["client_build_number"] == sh.DEFAULT_BUILD["client_build_number"]


class _FakeResp:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, *, status=200, payload=None, raise_exc=None):
        self._status = status
        self._payload = payload
        self._raise = raise_exc

    def get(self, url, timeout=None):
        if self._raise:
            raise self._raise
        return _FakeResp(self._status, self._payload)


def _payload(build_number=580123, ua=None):
    ua = ua or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) discord/1.0.9300 Chrome/140.0.0.0 "
        "Electron/38.0.0 Safari/537.36"
    )
    return {
        "clients": {
            "Discord": {
                "decoded": {
                    "release_channel": "stable",
                    "client_version": "1.0.9300",
                    "browser_user_agent": ua,
                    "browser_version": "38.0.0",
                    "client_build_number": build_number,
                    "native_build_number": 85000,
                }
            }
        }
    }


class TestRefreshBuildInfo:
    @pytest.mark.asyncio
    async def test_refresh_applies_current_build(self):
        session = _FakeSession(payload=_payload(build_number=580123))
        out = await sh.refresh_build_info(session)
        assert out["client_build_number"] == 580123
        # It propagates to freshly-built fingerprints.
        _, props = _props("token-abc")
        assert props["client_build_number"] == 580123
        assert props["client_version"] == "1.0.9300"

    @pytest.mark.asyncio
    async def test_implausible_build_is_rejected(self):
        # A too-low build number can't be real → keep the current (fallback) one.
        session = _FakeSession(payload=_payload(build_number=1234))
        before = sh.get_build_info()["client_build_number"]
        out = await sh.refresh_build_info(session)
        assert out["client_build_number"] == before

    @pytest.mark.asyncio
    async def test_non_desktop_ua_is_rejected(self):
        # A UA without "discord/" isn't the desktop client → keep current.
        session = _FakeSession(payload=_payload(ua="Mozilla/5.0 Chrome/140.0.0.0"))
        before = sh.get_build_info()["client_build_number"]
        out = await sh.refresh_build_info(session)
        assert out["client_build_number"] == before

    @pytest.mark.asyncio
    async def test_http_error_keeps_fallback(self):
        session = _FakeSession(status=500, payload={})
        before = sh.get_build_info()["client_build_number"]
        out = await sh.refresh_build_info(session)
        assert out["client_build_number"] == before

    @pytest.mark.asyncio
    async def test_network_error_keeps_fallback(self):
        session = _FakeSession(raise_exc=aiohttp.ClientError("boom"))
        before = sh.get_build_info()["client_build_number"]
        out = await sh.refresh_build_info(session)
        assert out["client_build_number"] == before


def test_context_properties_encodes_location():
    val = sh.context_properties("chat_input")
    assert json.loads(base64.b64decode(val)) == {"location": "chat_input"}
