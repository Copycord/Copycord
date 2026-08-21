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

    def test_sends_the_client_hints_the_real_client_sends(self):
        # This previously asserted the opposite, on the assumption that a
        # desktop client sends no client hints. A capture of the real desktop
        # client (2026-08-21) shows it sends all of these, so leaving them out
        # was itself the tell.
        h = sh.build_headers("token-abc", referer="https://discord.com/channels/1/2")
        assert h["Sec-CH-UA-Platform"] == '"Windows"'
        assert h["Sec-CH-UA-Mobile"] == "?0"
        assert h["Origin"] == "https://discord.com"
        assert h["Referer"] == "https://discord.com/channels/1/2"
        assert h["X-Debug-Options"] == "bugReporterEnabled"

        # Electron's brand list has no "Google Chrome" entry, and its Chromium
        # major must match the user agent's.
        chrome_major = h["User-Agent"].split("Chrome/")[1].split(".")[0]
        assert h["Sec-CH-UA"] == f'"Not/A)Brand";v="99", "Chromium";v="{chrome_major}"'
        assert "Google Chrome" not in h["Sec-CH-UA"]

    def test_every_header_value_is_a_string(self):
        # build_headers() is shared with aiohttp callers (the scraper,
        # message_utils, admin token validation). A None value is a curl_cffi
        # instruction to delete a header; aiohttp dies on it with
        # "Cannot serialize non-str key None", which surfaced as every token
        # being reported invalid. Keep the deletions out of here.
        h = sh.build_headers("token-abc", referer="https://discord.com/channels/1/2")
        assert all(isinstance(v, str) for v in h.values()), {
            k: v for k, v in h.items() if not isinstance(v, str)
        }

    def test_api_call_not_a_page_load(self):
        # curl_cffi's impersonation profile defaults these to a top-level
        # navigation, which contradicts a POST carrying an Authorization
        # header. We pin them, and remove the navigation-only extras (None
        # deletes a header in curl_cffi; an empty string leaves it in place).
        h = sh.build_headers("token-abc")
        assert h["Sec-Fetch-Site"] == "same-origin"
        assert h["Sec-Fetch-Mode"] == "cors"
        assert h["Sec-Fetch-Dest"] == "empty"

        # The deletions are curl_cffi-only and live apart from build_headers,
        # which aiohttp callers share and which must stay all-string.
        for banned in ("sec-fetch-user", "upgrade-insecure-requests", "priority"):
            assert sh.SUPPRESSED_PROFILE_HEADERS[banned] is None
            assert banned not in h

    def test_build_fingerprint_is_internally_consistent(self):
        # A build number belongs to exactly one client and Electron version.
        # Shipping a fresh number with stale version strings describes a client
        # that never existed, which is worse than simply being behind.
        b = sh.DEFAULT_BUILD
        assert b["client_version"] in b["browser_user_agent"]
        assert b["browser_version"] in b["browser_user_agent"]

    def test_locale_and_timezone_are_a_plausible_pair(self):
        # Drawn together, not independently: a French client on an
        # America/New_York clock is a combination almost nobody has.
        for i in range(200):
            h = sh.build_headers(f"tok-{i}")
            assert (h["X-Discord-Locale"], h["X-Discord-Timezone"]) in sh._LOCALE_TIMEZONES
            # The desktop client sends the bare locale, not a weighted list.
            assert h["Accept-Language"] == h["X-Discord-Locale"]

    def test_no_datacenter_windows_build(self):
        # 10.0.20348 is Windows Server 2022; a desktop app running on a
        # datacenter SKU is a tell by itself.
        assert all(b[0] != "10.0.20348" for b in sh._WINDOWS_BUILDS)

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
