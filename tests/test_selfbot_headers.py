"""
Unit tests for the shared self-bot header/fingerprint builder.

Cover the desktop super-properties shape, per-account uniqueness, and the
best-effort build-info refresh (which keeps the fingerprint's build number
current with a baked fallback).
"""

import base64
import json

import pytest

from copycord.common import selfbot_headers as sh


@pytest.fixture(autouse=True)
def _restore_build(tmp_path, monkeypatch):
    """Snapshot and restore the module-global build so tests don't leak state,
    and keep the build cache in a temp data dir. Not dev mode unless a test sets it."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("COPYCORD_ENV", raising=False)
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

    def test_super_props_key_order_matches_chrome(self):
        _, props = _props("token-order")
        assert list(props) == [
            "os",
            "browser",
            "device",
            "device_model_name",
            "system_locale",
            "has_client_mods",
            "browser_user_agent",
            "browser_version",
            "os_version",
            "referrer",
            "referring_domain",
            "referrer_current",
            "referring_domain_current",
            "release_channel",
            "client_build_number",
            "client_event_source",
            "client_launch_id",
            "launch_signature",
            "client_app_state",
        ]
        assert props["device_model_name"] == ""

    def test_heartbeat_session_id_keeps_its_place(self):
        sh.set_heartbeat_session_id("token-order-hb", "hb-1")
        try:
            _, props = _props("token-order-hb")
        finally:
            sh.set_heartbeat_session_id("token-order-hb", None)
        assert list(props)[-2:] == ["client_app_state", "client_heartbeat_session_id"]
        assert props["client_heartbeat_session_id"] == "hb-1"

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
        # A chrome150 handshake under Chrome/152 headers is a contradiction,
        # so the TLS target and the claimed version must be one decision.
        major = sh.TLS_IMPERSONATE.replace("chrome", "").split("_")[0]
        h = sh.build_headers("token-abc")
        assert h["User-Agent"].split("Chrome/")[1].split(".")[0] == major
        assert sh.DEFAULT_BUILD["browser_version"].split(".")[0] == major
        assert f'"Chromium";v="{major}"' in h["Sec-CH-UA"]

    def test_the_chrome_major_is_derived_not_written_down(self):
        # Pins the derivation itself: a hardcoded version lets the two drift
        # apart.
        import re

        assert sh.CHROME_MAJOR == re.search(r"(\d+)", sh.TLS_IMPERSONATE).group(1)
        assert sh.CHROME_MAJOR in sh.DEFAULT_BUILD["browser_user_agent"]

    def test_build_fingerprint_is_internally_consistent(self):
        b = sh.DEFAULT_BUILD

        assert b["browser_version"].split(".")[0] in b["browser_user_agent"]
        assert "Chrome/" in b["browser_user_agent"]

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
        assert g["is_fast_connect"] is False
        assert g["gateway_connect_reasons"] == "AppSkeleton"

    def test_gateway_props_omit_the_rest_only_fields(self):
        g = sh.gateway_properties("token-abc")
        assert "client_heartbeat_session_id" not in g
        # IDENTIFY carries qos_token beside the properties, not in them.
        assert "qos_token" not in g

    def test_gateway_props_key_order_matches_chrome(self):
        g = sh.gateway_properties("token-order")
        assert list(g)[:4] == ["os", "browser", "device", "device_model_name"]
        assert list(g)[-1] == "installation_id"

    def test_gateway_props_share_the_rest_launch_signature(self):
        _, rest = _props("token-abc")
        g = sh.gateway_properties("token-abc")
        assert g["launch_signature"] == rest["launch_signature"]
        assert g["client_app_state"] == rest["client_app_state"] == "focused"

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

        assert "client_heartbeat_session_id" not in props
        assert "client_heartbeat_session_id" not in sh.make_fingerprint("hb-token")["super_props"]

    def test_absent_again_once_the_id_is_cleared(self):
        sh.set_heartbeat_session_id("hb-token", "sess-1")
        sh.set_heartbeat_session_id("hb-token", None)
        _, props = _props("hb-token")
        assert "client_heartbeat_session_id" not in props

    def test_carried_on_every_request_once_known(self):
        sh.set_heartbeat_session_id("hb-token", "sess-42")
        try:
            _, props = _props("hb-token")
            assert props["client_heartbeat_session_id"] == "sess-42"
        finally:
            sh.set_heartbeat_session_id("hb-token", None)

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
    """The build number is scraped from Discord's own assets."""

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


class TestBuildCache:
    """The last scraped build is kept in <data dir>/client_headers.json (dev mode)."""

    @pytest.fixture(autouse=True)
    def _dev(self, monkeypatch):
        monkeypatch.setenv("COPYCORD_ENV", "dev")

    @pytest.mark.asyncio
    async def test_a_scraped_build_is_cached(self, tmp_path):
        await sh.refresh_build_info(_ScrapeSession(build=610001))
        data = json.loads((tmp_path / sh.BUILD_CACHE_FILE).read_text())
        assert data["build"]["client_build_number"] == 610001
        assert set(data["build"]) == {"client_build_number", "fetched_at"}

    @pytest.mark.asyncio
    async def test_failed_scrape_falls_back_to_the_cached_build(self):
        sh.save_cached_build_number(sh.DEFAULT_BUILD["client_build_number"] + 5000)
        out = await sh.refresh_build_info(_ScrapeSession(raise_exc=RuntimeError("offline")))
        assert out["client_build_number"] == sh.DEFAULT_BUILD["client_build_number"] + 5000

    @pytest.mark.asyncio
    async def test_unresolved_scrape_falls_back_to_the_cached_build(self):
        sh.save_cached_build_number(sh.DEFAULT_BUILD["client_build_number"] + 7)
        out = await sh.refresh_build_info(_ScrapeSession(asset_body="nothing useful here"))
        assert out["client_build_number"] == sh.DEFAULT_BUILD["client_build_number"] + 7

    @pytest.mark.asyncio
    async def test_an_older_cache_never_replaces_a_newer_build(self):
        sh.set_build_info({"client_build_number": 650000})
        sh.save_cached_build_number(620000)
        out = await sh.refresh_build_info(_ScrapeSession(build=1234))
        assert out["client_build_number"] == 650000

    @pytest.mark.parametrize(
        "body", ["not json", "[]", '{"build": []}', '{"build": {"client_build_number": "610000"}}',
                 '{"build": {"client_build_number": 12}}', '{"build": {"client_build_number": true}}']
    )
    def test_a_bad_cache_is_ignored(self, tmp_path, body):
        (tmp_path / sh.BUILD_CACHE_FILE).write_text(body)
        assert sh.load_cached_build_number() is None

    def test_no_cache_file(self):
        assert sh.load_cached_build_number() is None

    def test_cache_write_failure_is_quiet(self, monkeypatch, tmp_path):
        blocker = tmp_path / "file"
        blocker.write_text("x")
        monkeypatch.setenv("DATA_DIR", str(blocker / "sub"))
        sh.save_cached_build_number(610000)  # must not raise
        assert sh.load_cached_build_number() is None


class TestClientHeadersRecord:
    """Each account's full REST headers are recorded, without its token (dev mode)."""

    TOKEN = "record-token-abc.def.ghi"

    @pytest.fixture(autouse=True)
    def _fresh_fingerprints(self, monkeypatch):
        monkeypatch.setenv("COPYCORD_ENV", "dev")
        sh.set_build_info(sh.get_build_info())  # clears the per-token fingerprint cache

    def _file(self, tmp_path):
        return json.loads((tmp_path / sh.BUILD_CACHE_FILE).read_text())

    def test_first_use_records_the_headers_without_the_token(self, tmp_path):
        sent = sh.build_headers(self.TOKEN)
        raw = (tmp_path / sh.BUILD_CACHE_FILE).read_text()
        assert self.TOKEN not in raw
        entry = self._file(tmp_path)["accounts"][sh._token_log_id(self.TOKEN)]
        expected = {k: v for k, v in sent.items() if k != "Authorization"}
        assert entry["headers"] == expected
        assert "Authorization" not in entry["headers"]
        assert entry["super_properties"] == json.loads(base64.b64decode(sent["X-Super-Properties"]))

    def test_accounts_and_build_live_side_by_side(self, tmp_path):
        sh.save_cached_build_number(612345)
        sh.build_headers(self.TOKEN)
        sh.build_headers("other-token-xyz")
        data = self._file(tmp_path)
        assert data["build"]["client_build_number"] == 612345
        assert set(data["accounts"]) == {sh._token_log_id(self.TOKEN), sh._token_log_id("other-token-xyz")}

    def test_a_new_build_rerecords_on_next_use(self, tmp_path):
        sh.build_headers(self.TOKEN)
        sh.set_build_info({"client_build_number": 655555})
        sh.build_headers(self.TOKEN)
        entry = self._file(tmp_path)["accounts"][sh._token_log_id(self.TOKEN)]
        assert entry["super_properties"]["client_build_number"] == 655555

    def test_a_heartbeat_id_refreshes_the_snapshot(self, tmp_path):
        sh.build_headers(self.TOKEN)
        key = sh._token_log_id(self.TOKEN)
        data = self._file(tmp_path)
        data["accounts"][key]["wire"] = {"seen": 1}
        data["accounts"][key]["gateway"] = {"seen": 2}
        sh._write_cache(data)
        sh.set_heartbeat_session_id(self.TOKEN, "hb-123")
        try:
            entry = self._file(tmp_path)["accounts"][key]
            assert entry["super_properties"]["client_heartbeat_session_id"] == "hb-123"
            decoded = json.loads(base64.b64decode(entry["headers"]["X-Super-Properties"]))
            assert decoded == entry["super_properties"]
            assert "Authorization" not in entry["headers"]
            assert entry["wire"] == {"seen": 1}
            assert entry["gateway"] == {"seen": 2}
            sent = sh.build_headers(self.TOKEN)
            assert {k: v for k, v in sent.items() if k != "Authorization"} == entry["headers"]
        finally:
            sh.set_heartbeat_session_id(self.TOKEN, None)

    def test_an_unchanged_heartbeat_id_is_not_rerecorded(self, monkeypatch):
        sh.build_headers(self.TOKEN)
        calls = []
        monkeypatch.setattr(sh, "record_client_headers", lambda token: calls.append(token))
        try:
            sh.set_heartbeat_session_id(self.TOKEN, "hb-1")
            sh.set_heartbeat_session_id(self.TOKEN, "hb-1")
            assert calls == [self.TOKEN]
            sh.set_heartbeat_session_id(self.TOKEN, "hb-2")
            assert calls == [self.TOKEN, self.TOKEN]
        finally:
            sh.set_heartbeat_session_id(self.TOKEN, None)

    def test_a_heartbeat_record_failure_does_not_propagate(self, monkeypatch):
        def boom(token):
            raise RuntimeError("disk gone")
        sh.build_headers(self.TOKEN)
        monkeypatch.setattr(sh, "record_client_headers", boom)
        try:
            sh.set_heartbeat_session_id(self.TOKEN, "hb-9")
            assert sh._HEARTBEAT_SESSIONS[self.TOKEN] == "hb-9"
        finally:
            sh.set_heartbeat_session_id(self.TOKEN, None)

    def test_a_record_failure_never_breaks_a_request(self, monkeypatch):
        def boom(token):
            raise RuntimeError("disk gone")
        monkeypatch.setattr(sh, "record_client_headers", boom)
        sh.set_build_info(sh.get_build_info())  # clears the fingerprint cache
        assert sh.build_headers(self.TOKEN)["Authorization"] == self.TOKEN


class TestOutsideDev:
    """Outside dev mode client_headers.json is never written or read."""

    TOKEN = "record-token-abc.def.ghi"

    @pytest.fixture(autouse=True, params=[None, "", "prod", "development"])
    def _not_dev(self, request, monkeypatch):
        if request.param is None:
            monkeypatch.delenv("COPYCORD_ENV", raising=False)
        else:
            monkeypatch.setenv("COPYCORD_ENV", request.param)
        sh.set_build_info(sh.get_build_info())  # clears the per-token fingerprint cache

    @pytest.mark.asyncio
    async def test_nothing_is_written(self, tmp_path):
        await sh.refresh_build_info(_ScrapeSession(build=610001))
        sh.save_cached_build_number(610002)
        sh.build_headers(self.TOKEN)
        sh._write_cache({"build": {"client_build_number": 610003}})
        assert list(tmp_path.iterdir()) == []

    def test_a_heartbeat_id_writes_nothing(self, tmp_path):
        sh.build_headers(self.TOKEN)
        try:
            sh.set_heartbeat_session_id(self.TOKEN, "hb-123")
            assert list(tmp_path.iterdir()) == []
        finally:
            sh.set_heartbeat_session_id(self.TOKEN, None)

    @pytest.mark.asyncio
    async def test_the_cache_is_not_read(self, tmp_path):
        sh.set_build_info(dict(sh.DEFAULT_BUILD))
        cached = sh.DEFAULT_BUILD["client_build_number"] + 5000
        (tmp_path / sh.BUILD_CACHE_FILE).write_text(
            json.dumps({"build": {"client_build_number": cached, "fetched_at": 1}})
        )
        assert sh._read_cache() == {}
        assert sh.load_cached_build_number() is None
        out = await sh.refresh_build_info(_ScrapeSession(raise_exc=RuntimeError("offline")))
        assert out["client_build_number"] == sh.DEFAULT_BUILD["client_build_number"]


def test_context_properties_encodes_location():
    val = sh.context_properties("chat_input")
    assert json.loads(base64.b64decode(val)) == {"location": "chat_input"}
