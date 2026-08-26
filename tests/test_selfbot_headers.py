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
    def test_web_super_props_shape(self):
        h, props = _props("token-abc")

        assert props["browser"] == "Chrome"
        assert props["os"] == "Windows"
        assert props["os_version"] == "10"
        for key in (
            "device",
            "system_locale",
            "has_client_mods",
            "referrer_current",
            "referring_domain_current",
            "client_launch_id",
            "launch_signature",
            "client_build_number",
            "client_app_state",
        ):
            assert key in props

        for key in (
            "client_version",
            "native_build_number",
            "os_sdk_version",
            "os_arch",
            "app_arch",
        ):
            assert key not in props, key
        assert h["User-Agent"] == props["browser_user_agent"]
        assert h["Authorization"] == "token-abc"
        assert "Electron" not in h["User-Agent"]
        assert "discord/" not in h["User-Agent"]

    def test_sends_the_client_hints_a_browser_sends(self):
        h = sh.build_headers("token-abc", referer="https://discord.com/channels/1/2")
        assert h["Sec-CH-UA-Platform"] == '"Windows"'
        assert h["Sec-CH-UA-Mobile"] == "?0"
        assert h["Origin"] == "https://discord.com"
        assert h["Referer"] == "https://discord.com/channels/1/2"
        assert h["X-Debug-Options"] == "bugReporterEnabled"
        assert h["Sec-GPC"] == "1"
        assert h["Content-Type"] == "application/json"

        chrome_major = h["User-Agent"].split("Chrome/")[1].split(".")[0]
        assert h["Sec-CH-UA"] == (
            f'"Not=A?Brand";v="99", "Google Chrome";v="{chrome_major}", '
            f'"Chromium";v="{chrome_major}"'
        )
        assert "Google Chrome" in h["Sec-CH-UA"]

    def test_header_order_matches_chrome(self):

        h = sh.build_headers("token-abc")
        assert list(h) == sorted(h), list(h)

    def test_every_header_value_is_a_string(self):

        h = sh.build_headers("token-abc", referer="https://discord.com/channels/1/2")
        assert all(isinstance(v, str) for v in h.values()), {
            k: v for k, v in h.items() if not isinstance(v, str)
        }

    def test_api_call_not_a_page_load(self):

        h = sh.build_headers("token-abc")
        assert h["Sec-Fetch-Site"] == "same-origin"
        assert h["Sec-Fetch-Mode"] == "cors"
        assert h["Sec-Fetch-Dest"] == "empty"
        assert h["Priority"] == "u=1, i"

        for banned in ("sec-fetch-user", "upgrade-insecure-requests"):
            assert sh.SUPPRESSED_PROFILE_HEADERS[banned] is None
            assert banned not in h
        assert "priority" not in sh.SUPPRESSED_PROFILE_HEADERS

    def test_the_tls_target_and_the_claimed_chrome_agree(self):
        # These are one decision, not two. A chrome150 handshake under
        # Chrome/152 headers is a contradiction, and it is exactly what
        # discord.py-self ships: its TLS target comes from curl_cffi while its
        # browser version comes from Google's version-history API, and Google
        # always runs ahead of what curl_cffi can actually impersonate.
        major = sh.TLS_IMPERSONATE.replace("chrome", "").split("_")[0]
        h = sh.build_headers("token-abc")
        assert h["User-Agent"].split("Chrome/")[1].split(".")[0] == major
        assert sh.DEFAULT_BUILD["browser_version"].split(".")[0] == major
        assert f'"Chromium";v="{major}"' in h["Sec-CH-UA"]

    def test_the_chrome_major_is_derived_not_written_down(self):
        # Pins the derivation itself: hardcoding the version is what let the
        # two drift apart in the first place.
        import re

        assert sh.CHROME_MAJOR == re.search(r"(\d+)", sh.TLS_IMPERSONATE).group(1)
        assert sh.CHROME_MAJOR in sh.DEFAULT_BUILD["browser_user_agent"]

    def test_build_fingerprint_is_internally_consistent(self):
        b = sh.DEFAULT_BUILD

        assert b["browser_version"].split(".")[0] in b["browser_user_agent"]
        assert "Chrome/" in b["browser_user_agent"]

    def test_locale_and_timezone_are_a_plausible_pair(self):
        h = sh.build_headers("tok-1")
        assert h["X-Discord-Locale"] == sh.LOCALE
        assert h["X-Discord-Timezone"] == sh.TIMEZONE

        assert h["Accept-Language"] == "en-US,en;q=0.9"

    def test_unique_per_account_but_shared_build(self):
        _, a = _props("account-1")
        _, b = _props("account-2")
        assert a["client_launch_id"] != b["client_launch_id"]
        assert a["launch_signature"] != b["launch_signature"]
        assert a["client_build_number"] == b["client_build_number"]

    def test_installation_id_is_stable_per_account(self):

        a = sh.build_headers("stable-account")["X-Installation-Id"]
        b = sh.build_headers("stable-account")["X-Installation-Id"]
        assert a == b
        assert a != sh.build_headers("other-account")["X-Installation-Id"]

    def test_default_build_used_without_refresh(self):
        _, props = _props("token-abc")
        assert props["client_build_number"] == sh.DEFAULT_BUILD["client_build_number"]


class TestGatewayProperties:
    """IDENTIFY wants a different object than the REST header carries."""

    def test_gateway_props_carry_the_gateway_only_fields(self):
        g = sh.gateway_properties("token-abc")
        assert (
            g["installation_id"] == sh.build_headers("token-abc")["X-Installation-Id"]
        )
        assert g["is_fast_connect"] is True

    def test_gateway_props_omit_the_rest_only_fields(self):
        g = sh.gateway_properties("token-abc")
        for key in (
            "launch_signature",
            "client_app_state",
            "client_heartbeat_session_id",
        ):
            assert key not in g, key

    def test_both_describe_the_same_device(self):

        _, rest = _props("token-abc")
        g = sh.gateway_properties("token-abc")
        for key in (
            "os",
            "browser",
            "browser_user_agent",
            "browser_version",
            "os_version",
            "client_build_number",
            "client_launch_id",
        ):
            assert rest[key] == g[key], key


class TestHeartbeatSessionId:
    def test_absent_until_a_gateway_session_reports_one(self):
        sh.set_heartbeat_session_id("hb-token", None)
        _, props = _props("hb-token")

        assert props["client_heartbeat_session_id"] == ""

    def test_carried_on_every_request_once_known(self):
        sh.set_heartbeat_session_id("hb-token", "sess-42")
        try:
            _, props = _props("hb-token")
            assert props["client_heartbeat_session_id"] == "sess-42"
        finally:
            sh.set_heartbeat_session_id("hb-token", None)

    def test_clearing_it_restores_the_empty_value(self):
        sh.set_heartbeat_session_id("hb-token", "sess-42")
        sh.set_heartbeat_session_id("hb-token", None)
        _, props = _props("hb-token")
        assert props["client_heartbeat_session_id"] == ""


class _FakeResp:
    def __init__(self, text):
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def text(self):
        return self._text


class _ScrapeSession:
    """Stands in for Discord's web app: an index page linking JS assets."""

    def __init__(self, *, build=None, raise_exc=None, asset_body=None):
        self._build = build
        self._raise = raise_exc
        self._asset_body = asset_body

    def get(self, url, timeout=None):
        if self._raise:
            raise self._raise
        if url.endswith("/app"):
            return _FakeResp('<script src="/assets/main.abc123.js"></script>')
        if self._asset_body is not None:
            return _FakeResp(self._asset_body)
        return _FakeResp('x=1;buildNumber:"%s",y=2' % self._build)


class TestRefreshBuildInfo:
    """The build number is scraped from Discord's own assets.

    There is no third-party build feed any more, so these drive the scrape.
    """

    @pytest.mark.asyncio
    async def test_refresh_applies_scraped_build(self):
        out = await sh.refresh_build_info(_ScrapeSession(build=580123))
        assert out["client_build_number"] == 580123
        _, props = _props("token-abc")
        assert props["client_build_number"] == 580123

    @pytest.mark.asyncio
    async def test_refresh_never_invents_electron_fields(self):

        await sh.refresh_build_info(_ScrapeSession(build=580123))
        _, props = _props("token-abc")
        assert "client_version" not in props
        assert "native_build_number" not in props

    @pytest.mark.asyncio
    async def test_implausible_build_is_rejected(self):
        before = sh.get_build_info()["client_build_number"]
        out = await sh.refresh_build_info(_ScrapeSession(build=1234))
        assert out["client_build_number"] == before

    @pytest.mark.asyncio
    async def test_no_marker_in_assets_keeps_fallback(self):
        before = sh.get_build_info()["client_build_number"]
        out = await sh.refresh_build_info(
            _ScrapeSession(asset_body="nothing useful here")
        )
        assert out["client_build_number"] == before

    @pytest.mark.asyncio
    async def test_network_error_keeps_fallback(self):
        before = sh.get_build_info()["client_build_number"]
        out = await sh.refresh_build_info(
            _ScrapeSession(raise_exc=aiohttp.ClientError("boom"))
        )
        assert out["client_build_number"] == before


def test_context_properties_encodes_location():
    val = sh.context_properties("chat_input")
    assert json.loads(base64.b64decode(val)) == {"location": "chat_input"}
