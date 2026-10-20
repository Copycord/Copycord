"""The connect record: what Discord sees from a user token's requests, kept in
<data dir>/client_headers.json. The session is faked with a saved tls.peet.ws
response (tests/fixtures/peet_api_all.json); no network."""
import asyncio
import json
import types
from pathlib import Path

import pytest

import copycord.client.client as cc
from copycord.common import selfbot_headers as sh
from copycord.common import wire_probe as wp

TOKEN = "MTIzNDU2Nzg5MDEyMzQ1Njc4.GabcDE.wire-probe-test-token-not-real"
PROXY = "http://alice:s3cret-pw@proxy.example:8080"
FIXTURE = Path(__file__).parent / "fixtures" / "peet_api_all.json"


def _sample():
    return json.loads(FIXTURE.read_text())


def _h2_sample(lines):
    # The HTTP/2 section as tls.peet.ws's server (TrackMe) shapes it; the saved
    # sample was taken over HTTP/1.1, which has no such section.
    body = _sample()
    body.pop("http1")
    body["http_version"] = "h2"
    body["http2"] = {
        "akamai_fingerprint": "1:65536;2:0;4:6291456;6:262144|15663105|0|m,a,s,p",
        "akamai_fingerprint_hash": "52d84b11737d980aef856699f885ca86",
        "sent_frames": [
            {"frame_type": "SETTINGS", "length": 24, "settings": ["HEADER_TABLE_SIZE = 65536"]},
            {"frame_type": "WINDOW_UPDATE", "length": 4, "increment": 15663105},
            {"frame_type": "HEADERS", "stream_id": 1, "length": 400, "headers": lines},
        ],
    }
    return body


class _Resp:
    def __init__(self, body, status=200):
        self._body = body
        self.status_code = status

    def json(self):
        return self._body


class _Session:
    """Echoes the sent headers back inside the sample, like the real service."""

    def __init__(self, status=200, raise_exc=None, delay=0):
        self.status = status
        self.raise_exc = raise_exc
        self.delay = delay
        self.calls = []

    async def get(self, url, headers=None, timeout=None, proxy=None):
        self.calls.append({"url": url, "headers": dict(headers or {}), "proxy": proxy, "timeout": timeout})
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.raise_exc:
            raise self.raise_exc
        lines = [":method: GET", ":authority: tls.peet.ws", ":scheme: https", ":path: /api/all"]
        lines += [f"{k.lower()}: {v}" for k, v in (headers or {}).items() if v is not None]
        return _Resp(_h2_sample(lines), self.status)


class _Pool:
    def __init__(self, proxy):
        self.proxy = proxy

    def current(self, token):
        return self.proxy


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    original = sh.get_build_info()
    sh.set_build_info(original)  # clears the per-token fingerprint cache
    yield
    sh.set_build_info(original)


def _fake(monkeypatch, session, proxy=PROXY):
    async def get_tls_session(token, use_proxy=False):
        assert use_proxy is True
        return session

    monkeypatch.setattr(sh, "get_tls_session", get_tls_session)
    monkeypatch.setattr(wp, "_get_proxy_pool", lambda: _Pool(proxy))


def _gateway():
    return types.SimpleNamespace(
        connected_url="wss://gateway.discord.gg/?encoding=json&v=9&compress=zlib-stream",
        identify_properties=dict(sh.gateway_properties(TOKEN)),
        capabilities=1734653,
    )


def _file(tmp_path):
    return (tmp_path / sh.BUILD_CACHE_FILE).read_text()


class TestParser:
    def test_saved_http1_sample(self):
        out = wp.parse_peet(_sample())
        assert set(out) == {"exit_ip", "http_version", "headers", "header_order", "tls", "http2"}
        assert out["exit_ip"] == "203.0.113.7"
        assert out["http_version"] == "HTTP/1.1"
        assert out["headers"] == {"host": "tls.peet.ws", "user-agent": "curl/8.21.0", "accept": "*/*"}
        assert out["header_order"] == ["host", "user-agent", "accept"]
        assert out["tls"]["ja3_hash"] == "fae0e5d973c96ae1888b99538efa0363"
        assert out["tls"]["ja4"] == "t13d2013h1_2b729b4bf6f3_e24568c0d440"
        assert out["tls"]["peetprint_hash"] == "9500525742af93894c985ab6fc084ab1"
        assert out["tls"]["ja3"].startswith("771,")
        assert "ciphers" not in out["tls"] and "extensions" not in out["tls"]
        assert out["http2"] == {}

    def test_http2_headers_and_akamai(self):
        out = wp.parse_peet(_h2_sample([":method: GET", ":authority: tls.peet.ws", "accept: */*"]))
        assert out["http_version"] == "h2"
        assert out["headers"] == {"accept": "*/*"}
        assert out["http2"]["akamai_fingerprint"].endswith("|m,a,s,p")
        assert out["http2"]["akamai_fingerprint_hash"] == "52d84b11737d980aef856699f885ca86"
        assert out["http2"]["pseudo_header_order"] == [":method", ":authority"]

    @pytest.mark.parametrize(
        "addr, ip",
        [("1.2.3.4:5", "1.2.3.4"), ("[2001:db8::1]:443", "2001:db8::1"), ("2001:db8::1", "2001:db8::1"), (None, None)],
    )
    def test_exit_ip(self, addr, ip):
        assert wp._exit_ip(addr) == ip

    def test_garbage_does_not_raise(self):
        assert wp.parse_peet(None)["headers"] == {}
        assert wp.parse_peet({"http1": {"headers": [1, None]}, "tls": []})["tls"] == {}


class TestProbe:
    @pytest.mark.asyncio
    async def test_placeholder_goes_out_instead_of_the_token(self, monkeypatch):
        session = _Session()
        _fake(monkeypatch, session)
        out = await wp.probe_wire(TOKEN)

        sent = session.calls[0]
        assert sent["url"] == wp.PROBE_URL
        assert sent["proxy"] == PROXY
        auth = sent["headers"]["Authorization"]
        assert auth != TOKEN and auth.startswith("PROBE.") and len(auth) == len(TOKEN)
        assert TOKEN not in json.dumps(sent["headers"])
        assert "X-Context-Properties" in sent["headers"]
        assert sent["headers"]["X-Super-Properties"] == sh.build_headers(TOKEN)["X-Super-Properties"]

        assert out["ok"] is True
        assert out["exit_ip"] == "203.0.113.7"
        assert out["headers_on_wire"]["authorization"] == "[redacted]"
        assert out["tls"]["ja4"] and out["http2"]["akamai_fingerprint_hash"]
        assert "authorization" not in out["diff_vs_real_client"]["missing"]
        assert auth not in json.dumps(out)

    @pytest.mark.asyncio
    async def test_proxy_credentials_are_masked(self, monkeypatch):
        _fake(monkeypatch, _Session(raise_exc=RuntimeError(f"could not reach {PROXY}")))
        out = await wp.probe_wire(TOKEN)
        raw = json.dumps(out)
        assert "s3cret-pw" not in raw and "alice" not in raw
        assert out["proxy"] == "http://***@proxy.example:8080"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "session, error",
        [
            (_Session(status=503), "HTTP 503"),
            (_Session(raise_exc=OSError("network down")), "OSError: network down"),
        ],
    )
    async def test_a_failure_is_ok_false(self, monkeypatch, session, error):
        _fake(monkeypatch, session)
        out = await wp.probe_wire(TOKEN)
        assert out["ok"] is False and out["error"] == error and out["probed_at"]

    @pytest.mark.asyncio
    async def test_no_curl_cffi_is_ok_false(self, monkeypatch):
        async def missing(token, use_proxy=False):
            raise RuntimeError("curl_cffi is not installed")

        monkeypatch.setattr(sh, "get_tls_session", missing)
        monkeypatch.setattr(wp, "_get_proxy_pool", lambda: _Pool(None))
        out = await wp.probe_wire(TOKEN)
        assert out == {"ok": False, "error": "RuntimeError: curl_cffi is not installed", "probed_at": out["probed_at"], "proxy": None}

    @pytest.mark.asyncio
    async def test_time_limit(self, monkeypatch):
        monkeypatch.setattr(wp, "PROBE_TIMEOUT", 0.05)
        _fake(monkeypatch, _Session(delay=5))
        out = await wp.probe_wire(TOKEN)
        assert out["ok"] is False and "timed out" in out["error"]


class TestRecord:
    @pytest.mark.asyncio
    async def test_record_lands_beside_the_headers_without_secrets(self, monkeypatch, tmp_path):
        session = _Session()
        _fake(monkeypatch, session)
        sh.build_headers(TOKEN)  # writes headers and super_properties
        await wp.record_connect(TOKEN, wp.gateway_snapshot(_gateway(), "READY"))

        raw = _file(tmp_path)
        placeholder = session.calls[0]["headers"]["Authorization"]
        for secret in (TOKEN, TOKEN[:20], placeholder, "s3cret-pw", "alice:"):
            assert secret not in raw

        entry = json.loads(raw)["accounts"][sh._token_log_id(TOKEN)]
        assert {"headers", "super_properties", "wire", "gateway"} <= set(entry)
        wire = entry["wire"]
        assert {"probed_at", "ok", "exit_ip", "http_version", "headers_on_wire", "tls", "http2",
                "diff_vs_real_client"} <= set(wire)
        assert wire["proxy"] == "http://***@proxy.example:8080"
        gw = entry["gateway"]
        assert gw["event"] == "READY"
        assert gw["query"] == {"encoding": "json", "v": "9", "compress": "zlib-stream"}
        assert gw["properties"]["installation_id"] == sh.gateway_properties(TOKEN)["installation_id"]
        assert "token" not in gw["properties"]

    @pytest.mark.asyncio
    async def test_a_failed_probe_is_recorded(self, monkeypatch, tmp_path):
        _fake(monkeypatch, _Session(raise_exc=OSError("down")))
        await wp.record_connect(TOKEN, wp.gateway_snapshot(None, "RESUMED"))
        entry = json.loads(_file(tmp_path))["accounts"][sh._token_log_id(TOKEN)]
        assert entry["wire"]["ok"] is False
        assert entry["gateway"]["url"] is None and entry["gateway"]["event"] == "RESUMED"

    @pytest.mark.asyncio
    async def test_a_new_fingerprint_keeps_the_connect_record(self, monkeypatch, tmp_path):
        _fake(monkeypatch, _Session())
        await wp.record_connect(TOKEN, wp.gateway_snapshot(_gateway(), "READY"))
        sh.set_build_info({"client_build_number": 655555})
        sh.build_headers(TOKEN)
        entry = json.loads(_file(tmp_path))["accounts"][sh._token_log_id(TOKEN)]
        assert entry["super_properties"]["client_build_number"] == 655555
        assert entry["wire"]["ok"] is True


class TestConnectHook:
    @pytest.mark.asyncio
    async def test_schedule_does_not_block(self, monkeypatch, tmp_path):
        release = asyncio.Event()

        async def slow_probe(token):
            await release.wait()
            return {"ok": True}

        monkeypatch.setattr(wp, "probe_wire", slow_probe)
        task = wp.schedule_connect_record(TOKEN, _gateway(), event="READY")
        assert task is not None and not task.done()
        assert task in wp._TASKS
        release.set()
        await task
        assert json.loads(_file(tmp_path))["accounts"][sh._token_log_id(TOKEN)]["wire"] == {"ok": True}

    def test_no_token_or_no_loop_schedules_nothing(self):
        assert wp.schedule_connect_record(None) is None
        assert wp.schedule_connect_record(TOKEN) is None  # no running loop: quietly skipped

    def _listener(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            wp, "schedule_connect_record", lambda token, gateway=None, *, event: calls.append((token, gateway, event))
        )
        listener = cc.ClientListener.__new__(cc.ClientListener)
        listener.bot = types.SimpleNamespace(token=TOKEN, gateway=_gateway())
        listener.proxy_rotator = types.SimpleNamespace(enabled=False, _current_proxy=None)
        return listener, calls

    @pytest.mark.asyncio
    async def test_ready_schedules_first(self, monkeypatch):
        listener, calls = self._listener(monkeypatch)

        async def boom():
            raise RuntimeError("later startup step failed")

        listener.forwarding = types.SimpleNamespace(start=boom)
        with pytest.raises(RuntimeError):
            await listener.on_ready()
        assert [(c[0], c[2]) for c in calls] == [(TOKEN, "READY")]
        assert calls[0][1] is listener.bot.gateway

    @pytest.mark.asyncio
    async def test_resumed_schedules(self, monkeypatch):
        listener, calls = self._listener(monkeypatch)
        await listener.on_resumed()
        assert [(c[0], c[2]) for c in calls] == [(TOKEN, "RESUMED")]
