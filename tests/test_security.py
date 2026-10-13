"""
Security tests for the admin app: path traversal in file routes,
websocket authentication, the login limit and CSRF checks.
"""
from unittest.mock import AsyncMock

import pytest

from copycord.admin.app import app
from copycord.admin.security import safe_child

from httpx import AsyncClient, ASGITransport


TRAVERSAL_NAMES = [
    "../x",
    "..\\x",
    "C:x",
    "C:\\x",
    "/etc/passwd",
    "\\\\server\\share",
    "..",
    ".",
    "",
    "a/../../x",
    "..%2Fx",
    "%2e%2e%2fx",
]


@pytest.fixture()
def client(monkeypatch):
    from copycord.admin import auth

    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    transport = ASGITransport(app=app)
    return AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Origin": "http://test", "X-Copycord-CSRF": "1"},
        cookies={auth.ADMIN_COOKIE_NAME: auth.make_session_token()},
    )


@pytest.fixture(autouse=True)
def _mock_ws(monkeypatch):
    import copycord.admin.app as app_mod
    mock = AsyncMock(return_value={"ok": True, "running": False, "status": "stopped"})
    monkeypatch.setattr(app_mod, "_ws_cmd", mock)


@pytest.fixture()
def dirs(tmp_path, monkeypatch):
    """A data dir with a secret outside the backup and scrape folders."""
    import copycord.admin.app as app_mod
    data = tmp_path / "data"
    backups = data / "backups"
    scrapes = data / "scrapes" / "123"
    backups.mkdir(parents=True)
    scrapes.mkdir(parents=True)
    (data / "secret.key").write_text("secret")
    (tmp_path / "x").write_text("outside")
    (backups / "copycord-backup-1.tar.gz").write_bytes(b"gz")
    (scrapes / "scrape_123_1.json").write_text("{}")
    (data / "scrapes" / "top.json").write_text("{}")
    monkeypatch.setattr(app_mod, "BACKUP_DIR", backups)
    monkeypatch.setattr(app_mod, "DATA_DIR", data)
    return data


class TestSafeChild:

    @pytest.mark.parametrize("name", TRAVERSAL_NAMES)
    def test_rejects(self, tmp_path, name):
        assert safe_child(tmp_path, name) is None
        assert safe_child(tmp_path, name, max_depth=2) is None

    def test_accepts_plain_name(self, tmp_path):
        assert safe_child(tmp_path, "a.tar.gz") == (tmp_path / "a.tar.gz").resolve()

    def test_subdir_only_when_allowed(self, tmp_path):
        assert safe_child(tmp_path, "123/a.json") is None
        assert safe_child(tmp_path, "123/a.json", max_depth=2) == (
            tmp_path / "123" / "a.json"
        ).resolve()
        assert safe_child(tmp_path, "1/2/a.json", max_depth=2) is None

    def test_symlink_escape(self, tmp_path):
        base = tmp_path / "base"
        base.mkdir()
        (tmp_path / "outside").write_text("x")
        try:
            (base / "link").symlink_to(tmp_path / "outside")
        except OSError:
            pytest.skip("symlinks not available")
        assert safe_child(base, "link") is None


class TestBackupTraversal:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", TRAVERSAL_NAMES + ["..%5Csecret.key", "%2e%2e%5csecret.key"])
    async def test_download_rejected(self, client, dirs, name):
        resp = await client.get(f"/api/backup/download/{name}")
        assert resp.content not in (b"secret", b"outside")
        assert resp.headers.get("content-type") != "application/gzip"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", ["..%5Csecret.key", "C:secret.key", "C%3A%5Csecret.key"])
    async def test_download_reaches_handler_and_400(self, client, dirs, name):
        resp = await client.get(f"/api/backup/download/{name}")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", TRAVERSAL_NAMES[:-3] + ["../secret.key", "..\\secret.key"])
    async def test_delete_rejected(self, client, dirs, name):
        resp = await client.post("/api/backup/delete", data={"name": name})
        assert resp.status_code in (400, 422)
        assert (dirs / "secret.key").exists()
        assert (dirs.parent / "x").exists()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", ["../secret.key", "..\\secret.key", "/etc/passwd"])
    async def test_restore_existing_rejected(self, client, dirs, name):
        resp = await client.post(
            "/api/backup/restore", data={"source": "existing", "name": name}
        )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_download_normal(self, client, dirs):
        resp = await client.get("/api/backup/download/copycord-backup-1.tar.gz")
        assert resp.status_code == 200
        assert resp.content == b"gz"

    @pytest.mark.asyncio
    async def test_delete_normal(self, client, dirs):
        resp = await client.post(
            "/api/backup/delete", data={"name": "copycord-backup-1.tar.gz"}
        )
        assert resp.status_code == 200
        assert not (dirs / "backups" / "copycord-backup-1.tar.gz").exists()


class TestScrapeTraversal:

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path",
        [
            "..%2Fsecret.key",
            "..%5Csecret.key",
            "123%2F..%2F..%2Fsecret.key",
            "C:secret.key",
            "C%3A%5Csecret.key",
            "%2Fetc%2Fpasswd",
            "1/2/3.json",
        ],
    )
    async def test_download_and_delete_rejected(self, client, dirs, path):
        resp = await client.get(f"/api/scraper/scrapes/{path}")
        assert resp.status_code in (400, 404)
        assert resp.content != b"secret"
        resp = await client.delete(f"/api/scraper/scrapes/{path}")
        assert resp.status_code in (400, 404)
        assert (dirs / "secret.key").exists()

    @pytest.mark.asyncio
    async def test_download_normal(self, client, dirs):
        resp = await client.get("/api/scraper/scrapes/123/scrape_123_1.json")
        assert resp.status_code == 200
        resp = await client.get("/api/scraper/scrapes/top.json")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_delete_normal(self, client, dirs):
        resp = await client.delete("/api/scraper/scrapes/123/scrape_123_1.json")
        assert resp.status_code == 200
        assert not (dirs / "scrapes" / "123").exists()


# ---------------------------------------------------------------------------
# Websocket authentication
# ---------------------------------------------------------------------------

from starlette.testclient import TestClient  # noqa: E402
from starlette.websockets import WebSocketDisconnect  # noqa: E402

from copycord.admin import auth  # noqa: E402
from copycord.common.websockets import (  # noqa: E402
    INTERNAL_TOKEN_ENV,
    INTERNAL_TOKEN_HEADER,
    internal_connect_kwargs,
)

ORIGIN = "http://testserver"
TOKEN = "per-run-test-token"


@pytest.fixture()
def tc():
    # No context manager: the lifespan (agents, schedulers) is not started.
    return TestClient(app)


@pytest.fixture()
def password(monkeypatch):
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")


@pytest.fixture()
def logged_in(tc, monkeypatch):
    # The dashboard always has a password; this client holds a valid session.
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    tc.cookies.set(auth.ADMIN_COOKIE_NAME, auth.make_session_token())


@pytest.fixture()
def internal_token(monkeypatch):
    monkeypatch.setenv(INTERNAL_TOKEN_ENV, TOKEN)


def _session_cookie():
    return f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"


def _rejected(tc, path, headers):
    with pytest.raises(WebSocketDisconnect) as exc:
        with tc.websocket_connect(path, headers=headers):
            pass
    assert exc.value.code == 1008


class TestBrowserSockets:

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/out", "/ws/in"])
    def test_no_cookie_rejected(self, tc, password, path):
        _rejected(tc, path, {"origin": ORIGIN})

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/out", "/ws/in"])
    def test_bad_cookie_rejected(self, tc, password, path):
        _rejected(tc, path, {"origin": ORIGIN, "cookie": "cc_admin=forged"})

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/out", "/ws/in"])
    @pytest.mark.parametrize(
        "origin", ["http://evil.example", "http://testserver.evil.example", "null", ""]
    )
    def test_bad_origin_rejected_with_cookie(self, tc, password, path, origin):
        headers = {"cookie": _session_cookie()}
        if origin:
            headers["origin"] = origin
        _rejected(tc, path, headers)

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/out", "/ws/in"])
    def test_bad_origin_rejected_without_password(self, tc, logged_in, path):
        _rejected(tc, path, {"origin": "http://evil.example"})

    @pytest.mark.parametrize("path", ["/ws/ui", "/ws/in"])
    def test_internal_token_not_enough_for_ui(self, tc, password, internal_token, path):
        _rejected(tc, path, {INTERNAL_TOKEN_HEADER: TOKEN})

    def test_valid_cookie_and_origin_accepted(self, tc, password):
        headers = {"origin": ORIGIN, "cookie": _session_cookie()}
        with tc.websocket_connect("/ws/ui", headers=headers):
            pass

    def test_logged_in_same_origin_accepted(self, tc, logged_in):
        with tc.websocket_connect("/ws/ui", headers={"origin": ORIGIN}):
            pass

    def test_forwarded_host_accepted(self, tc, password):
        headers = {
            "origin": "https://copycord.example.com",
            "x-forwarded-host": "copycord.example.com",
            "cookie": _session_cookie(),
        }
        with tc.websocket_connect("/ws/ui", headers=headers):
            pass


class TestInternalSockets:

    @pytest.mark.parametrize("path", ["/bus", "/ws/out"])
    def test_no_token_rejected(self, tc, internal_token, path):
        _rejected(tc, path, {})

    @pytest.mark.parametrize("path", ["/bus", "/ws/out"])
    def test_bad_token_rejected(self, tc, internal_token, path):
        _rejected(tc, path, {INTERNAL_TOKEN_HEADER: "wrong"})

    def test_token_required_even_without_password(self, tc, logged_in, internal_token):
        _rejected(tc, "/bus", {"origin": ORIGIN})

    def test_session_cookie_not_enough_for_bus(self, tc, password, internal_token):
        _rejected(tc, "/bus", {"origin": ORIGIN, "cookie": _session_cookie()})

    def test_no_token_configured_rejects_everything(self, tc, monkeypatch):
        monkeypatch.delenv(INTERNAL_TOKEN_ENV, raising=False)
        _rejected(tc, "/bus", {INTERNAL_TOKEN_HEADER: ""})

    def test_valid_token_accepted(self, tc, password, internal_token):
        with tc.websocket_connect("/bus", headers={INTERNAL_TOKEN_HEADER: TOKEN}) as ws:
            ws.send_text('{"kind":"log","role":"test","payload":{"text":"hi"}}')


class TestInternalClientHeaders:

    def test_kwargs_carry_token(self, internal_token):
        kwargs = internal_connect_kwargs()
        assert len(kwargs) == 1
        (headers,) = kwargs.values()
        assert headers == {INTERNAL_TOKEN_HEADER: TOKEN}

    def test_no_token_no_kwargs(self, monkeypatch):
        monkeypatch.delenv(INTERNAL_TOKEN_ENV, raising=False)
        assert internal_connect_kwargs() == {}


# ---------------------------------------------------------------------------
# Login brute-force limit
# ---------------------------------------------------------------------------


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture()
def clock(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(auth, "LOGIN_LIMITER", auth.LoginLimiter(clock=c))
    return c


def _login(tc, pw, **headers):
    return tc.post(
        "/login",
        data={"password": pw},
        headers={"origin": ORIGIN, **headers},
        follow_redirects=False,
    )


class TestLoginLimit:

    def test_success_sets_cookie(self, tc, password, clock):
        resp = _login(tc, "pw")
        assert resp.status_code == 302
        assert auth.ADMIN_COOKIE_NAME in resp.cookies

    def test_locked_after_five_failures(self, tc, password, clock):
        for _ in range(4):
            assert _login(tc, "nope").status_code == 401
        resp = _login(tc, "nope")
        assert resp.status_code == 429
        assert resp.headers["retry-after"] == "30"
        # The right password does not get through while locked.
        resp = _login(tc, "pw")
        assert resp.status_code == 429
        assert auth.ADMIN_COOKIE_NAME not in resp.cookies

    def test_forwarded_headers_do_not_reset_the_key(self, tc, password, clock):
        for i in range(5):
            _login(tc, "nope", **{"x-forwarded-for": f"10.0.0.{i}"})
        resp = _login(tc, "pw", **{"x-forwarded-for": "10.9.9.9", "x-real-ip": "10.9.9.9"})
        assert resp.status_code == 429

    def test_unlocks_after_delay_and_success_resets(self, tc, password, clock):
        for _ in range(5):
            _login(tc, "nope")
        clock.now += 31
        assert _login(tc, "pw").status_code == 302
        # The counter was cleared: four more failures are plain 401s.
        for _ in range(4):
            assert _login(tc, "nope").status_code == 401

    def test_compare_digest_used(self, tc, password, clock, monkeypatch):
        calls = []
        real = auth.hmac.compare_digest

        def spy(a, b):
            calls.append((a, b))
            return real(a, b)

        monkeypatch.setattr(auth.hmac, "compare_digest", spy)
        assert _login(tc, "pw").status_code == 302
        assert calls == [(b"pw", b"pw")]


class TestLoginLimiter:

    def test_exponential_backoff_capped(self):
        c = FakeClock()
        lim = auth.LoginLimiter(clock=c, base_delay=30, max_delay=200)
        delays = [lim.failure("a") for _ in range(9)]
        assert delays == [0, 0, 0, 0, 30, 60, 120, 200, 200]
        assert lim.retry_after("a") == 200
        assert lim.retry_after("b") == 0

    def test_quiet_window_resets(self):
        c = FakeClock()
        lim = auth.LoginLimiter(clock=c, window=300)
        for _ in range(4):
            lim.failure("a")
        c.now += 301
        assert lim.failure("a") == 0

    def test_key_cap(self):
        c = FakeClock()
        lim = auth.LoginLimiter(clock=c, max_keys=3)
        for k in "abcd":
            c.now += 1
            lim.failure(k)
        assert len(lim._state) == 3
        assert "a" not in lim._state


# ---------------------------------------------------------------------------
# CSRF
# ---------------------------------------------------------------------------

from copycord.admin.security import CSRF_HEADER  # noqa: E402

EVENT = {"event_type": "csrf-test", "details": ""}


def _post(tc, headers, path="/api/event-logs"):
    return tc.post(path, json=EVENT, headers=headers, follow_redirects=False)


class TestCSRF:

    @pytest.mark.parametrize(
        "origin", ["http://evil.example", "http://testserver.evil.example", "null"]
    )
    def test_cross_origin_rejected(self, tc, logged_in, origin):
        resp = _post(tc, {"origin": origin, CSRF_HEADER: "1"})
        assert resp.status_code == 403

    def test_cross_origin_rejected_with_session(self, tc, password):
        headers = {"origin": "http://evil.example", CSRF_HEADER: "1", "cookie": _session_cookie()}
        assert _post(tc, headers).status_code == 403

    def test_missing_header_rejected(self, tc, logged_in):
        assert _post(tc, {"origin": ORIGIN}).status_code == 403

    def test_wrong_header_value_rejected(self, tc, logged_in):
        assert _post(tc, {"origin": ORIGIN, CSRF_HEADER: "0"}).status_code == 403

    def test_no_origin_or_referer_rejected(self, tc, logged_in):
        assert _post(tc, {CSRF_HEADER: "1"}).status_code == 403

    @pytest.mark.parametrize("method", ["put", "patch", "delete"])
    def test_other_methods_checked(self, tc, logged_in, method):
        resp = tc.request(method.upper(), "/api/event-logs/x", headers={"origin": "http://evil.example"})
        assert resp.status_code == 403

    def test_same_origin_with_header_accepted(self, tc, logged_in):
        resp = _post(tc, {"origin": ORIGIN, CSRF_HEADER: "1"})
        assert resp.status_code == 200

    def test_same_origin_with_header_and_session_accepted(self, tc, password):
        headers = {"origin": ORIGIN, CSRF_HEADER: "1", "cookie": _session_cookie()}
        assert _post(tc, headers).status_code == 200

    def test_referer_fallback(self, tc, logged_in):
        ok = _post(tc, {"referer": f"{ORIGIN}/channels", CSRF_HEADER: "1"})
        assert ok.status_code == 200
        bad = _post(tc, {"referer": "http://evil.example/x", CSRF_HEADER: "1"})
        assert bad.status_code == 403

    def test_forwarded_host_accepted(self, tc, logged_in):
        headers = {
            "origin": "https://copycord.example.com",
            "x-forwarded-host": "copycord.example.com",
            CSRF_HEADER: "1",
        }
        assert _post(tc, headers).status_code == 200

    def test_get_not_checked(self, tc, logged_in):
        resp = tc.get("/api/event-logs", headers={"origin": "http://evil.example"})
        assert resp.status_code == 200

    def test_login_exempt_from_header(self, tc, password, clock):
        assert _login(tc, "pw").status_code == 302
        assert _login(tc, "nope").status_code == 401

    def test_login_cross_origin_rejected(self, tc, password, clock):
        resp = _login(tc, "pw", origin="http://evil.example")
        assert resp.status_code == 403
        assert auth.ADMIN_COOKIE_NAME not in resp.cookies

    def test_internal_token_accepted(self, tc, logged_in, internal_token):
        assert _post(tc, {INTERNAL_TOKEN_HEADER: TOKEN}).status_code == 200

    def test_bad_internal_token_rejected(self, tc, logged_in, internal_token):
        assert _post(tc, {INTERNAL_TOKEN_HEADER: "wrong"}).status_code == 403

    def test_internal_token_unset_rejected(self, tc, logged_in, monkeypatch):
        monkeypatch.delenv(INTERNAL_TOKEN_ENV, raising=False)
        assert _post(tc, {INTERNAL_TOKEN_HEADER: ""}).status_code == 403
