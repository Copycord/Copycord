"""
Admin auth tests: secret.key storage, client IP behind proxies, session
invalidation on password change and /logout.
"""
import os
import stat

import pytest
from starlette.requests import Request
from starlette.testclient import TestClient

from copycord.admin import auth
from copycord.admin.app import app
from copycord.admin.security import CSRF_HEADER
from copycord.common.private_file import write_private_file

ORIGIN = "http://testserver"


@pytest.fixture()
def tc():
    # No context manager: the lifespan (agents, schedulers) is not started.
    return TestClient(app)


@pytest.fixture()
def password(monkeypatch):
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")


# ---------------------------------------------------------------------------
# secret.key
# ---------------------------------------------------------------------------


class TestSecretKey:

    @pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")
    def test_created_private(self, tmp_path):
        key = auth._load_or_create_secret_key(tmp_path, "")
        f = tmp_path / "secret.key"
        assert f.read_text(encoding="utf-8") == key
        assert stat.S_IMODE(f.stat().st_mode) == 0o600

    def test_existing_key_reused(self, tmp_path):
        (tmp_path / "secret.key").write_text("kept\n", encoding="utf-8")
        assert auth._load_or_create_secret_key(tmp_path, "") == "kept"

    def test_env_wins(self, tmp_path):
        assert auth._load_or_create_secret_key(tmp_path, "fromenv") == "fromenv"
        assert not (tmp_path / "secret.key").exists()

    def test_unreadable_key_raises(self, tmp_path):
        # A directory exists but cannot be read as text, even as root.
        (tmp_path / "secret.key").mkdir()
        with pytest.raises(RuntimeError, match="secret.key"):
            auth._load_or_create_secret_key(tmp_path, "")
        assert (tmp_path / "secret.key").is_dir()


class TestWritePrivateFile:

    @pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")
    def test_replaces_loose_file(self, tmp_path):
        f = tmp_path / "x.key"
        f.write_text("old")
        os.chmod(f, 0o644)
        write_private_file(f, "new")
        assert f.read_text() == "new"
        assert stat.S_IMODE(f.stat().st_mode) == 0o600

    def test_no_temp_left(self, tmp_path):
        write_private_file(tmp_path / "a", b"data")
        assert [p.name for p in tmp_path.iterdir()] == ["a"]


# ---------------------------------------------------------------------------
# Client IP
# ---------------------------------------------------------------------------


def _request(peer, headers):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
            "client": (peer, 1234),
        }
    )


SPOOFED = {
    "x-forwarded-for": "6.6.6.6, 10.0.0.1",
    "x-real-ip": "6.6.6.6",
    "cf-connecting-ip": "6.6.6.6",
}


class TestClientIp:

    def test_spoofed_headers_ignored_by_default(self, monkeypatch):
        monkeypatch.setattr(auth, "TRUSTED_PROXIES", frozenset())
        assert auth._client_ip(_request("203.0.113.5", SPOOFED)) == "203.0.113.5"

    def test_untrusted_peer_ignored(self, monkeypatch):
        monkeypatch.setattr(auth, "TRUSTED_PROXIES", frozenset({"10.0.0.9"}))
        assert auth._client_ip(_request("203.0.113.5", SPOOFED)) == "203.0.113.5"

    def test_trusted_proxy_honoured(self, monkeypatch):
        monkeypatch.setattr(auth, "TRUSTED_PROXIES", frozenset({"10.0.0.9"}))
        xff = {"x-forwarded-for": "198.51.100.7, 10.0.0.9"}
        assert auth._client_ip(_request("10.0.0.9", xff)) == "198.51.100.7"
        assert auth._client_ip(_request("10.0.0.9", SPOOFED)) == "6.6.6.6"

    def test_trusted_proxy_without_headers(self, monkeypatch):
        monkeypatch.setattr(auth, "TRUSTED_PROXIES", frozenset({"10.0.0.9"}))
        assert auth._client_ip(_request("10.0.0.9", {})) == "10.0.0.9"

    def test_parse_list(self):
        assert auth._parse_trusted_proxies(" 10.0.0.1, ,::1 ") == frozenset(
            {"10.0.0.1", "::1"}
        )
        assert auth._parse_trusted_proxies("") == frozenset()


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


class TestSessions:

    def test_token_valid_for_current_password(self, password):
        assert auth.decode_admin_session(auth.make_session_token())["ok"] is True

    def test_password_change_invalidates(self, password, monkeypatch):
        token = auth.make_session_token()
        monkeypatch.setattr(auth, "ADMIN_PASSWORD", "changed")
        assert auth.decode_admin_session(token) is None

    def test_old_style_token_rejected(self, password):
        assert auth.decode_admin_session(auth._get_signer().dumps({"ok": True})) is None

    def test_old_cookie_rejected_by_guard(self, tc, password, monkeypatch):
        cookie = f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"
        monkeypatch.setattr(auth, "ADMIN_PASSWORD", "changed")
        resp = tc.get("/login", headers={"cookie": cookie}, follow_redirects=False)
        assert resp.status_code == 200  # login page, not a redirect to the dashboard
        resp = tc.get("/api/event-logs", headers={"cookie": cookie}, follow_redirects=False)
        assert resp.status_code == 401
        resp = tc.get("/system", headers={"cookie": cookie}, follow_redirects=False)
        assert resp.status_code in (302, 307)
        assert resp.headers["location"].endswith("/login")


class TestGuardApi:

    @pytest.mark.parametrize("method", ["GET", "HEAD", "POST", "PUT", "DELETE"])
    def test_logged_out_api_is_401_json(self, tc, password, method):
        headers = {"origin": ORIGIN, CSRF_HEADER: "1"}
        resp = tc.request(method, "/api/x", headers=headers, follow_redirects=False)
        assert resp.status_code == 401
        assert "location" not in resp.headers
        assert resp.headers["content-type"].startswith("application/json")
        if method != "HEAD":
            assert resp.json() == {"detail": "Unauthorized"}

    def test_logged_out_page_get_redirects(self, tc, password):
        resp = tc.get("/somepage", headers={"accept": "text/html"}, follow_redirects=False)
        assert resp.status_code in (302, 307)
        assert resp.headers["location"] == "/login"

    def test_logged_out_page_post_stays_401(self, tc, password):
        resp = tc.post("/somepage", headers={"origin": ORIGIN, CSRF_HEADER: "1"}, follow_redirects=False)
        assert resp.status_code == 401


class TestLogout:

    def test_clears_cookie(self, tc, password):
        cookie = f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"
        resp = tc.post(
            "/logout",
            headers={"origin": ORIGIN, CSRF_HEADER: "1", "cookie": cookie},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert resp.headers["location"] == "/login"
        set_cookie = resp.headers["set-cookie"]
        assert set_cookie.startswith(f"{auth.ADMIN_COOKIE_NAME}=")
        assert "Max-Age=0" in set_cookie

    def test_needs_csrf_header(self, tc, password):
        resp = tc.post("/logout", headers={"origin": ORIGIN}, follow_redirects=False)
        assert resp.status_code == 403

    def test_cross_origin_rejected(self, tc, password):
        resp = tc.post(
            "/logout",
            headers={"origin": "http://evil.example", CSRF_HEADER: "1"},
            follow_redirects=False,
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Dashboard pages: the SPA at the root, behind the login guard
# ---------------------------------------------------------------------------

PAGES = ["/", "/channels", "/guilds", "/forwarding", "/scraper", "/system", "/logs"]


def _get_page(tc, path, with_session):
    headers = {"accept": "text/html"}
    if with_session:
        headers["cookie"] = f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"
    return tc.get(path, headers=headers, follow_redirects=False)


class TestPages:

    @pytest.mark.parametrize("path", PAGES)
    def test_page_serves_the_spa(self, tc, password, spa_build, path):
        resp = _get_page(tc, path, with_session=True)
        assert resp.status_code == 200
        assert resp.text == (spa_build / "index.html").read_text(encoding="utf-8")

    @pytest.mark.parametrize("path", PAGES + ["/no-such-page", "/event-logs", "/app/system"])
    def test_logged_out_redirects_to_login(self, tc, password, path):
        resp = _get_page(tc, path, with_session=False)
        assert resp.status_code in (302, 307)
        assert resp.headers["location"] == "/login"

    def test_unknown_page_is_the_spa_with_404(self, tc, password, spa_build):
        resp = _get_page(tc, "/no-such-page", with_session=True)
        assert resp.status_code == 404
        assert '<div id="root"></div>' in resp.text

    @pytest.mark.parametrize("path", ["/api/no-such-route", "/logs/stream/nope/x", "/assets/nope.js", "/ws/nope"])
    def test_unknown_api_path_is_plain_404(self, tc, password, path):
        resp = _get_page(tc, path, with_session=True)
        assert resp.status_code == 404
        assert resp.headers["content-type"].startswith("application/json")

    @pytest.mark.parametrize(
        "path,target",
        [
            ("/event-logs", "/logs"),
            ("/event-logs?type=x", "/logs?type=x"),
            ("/app", "/"),
            ("/app/", "/"),
            ("/app/channels", "/channels"),
            ("/app/logs?q=1", "/logs?q=1"),
            ("/app//evil.example", "/evil.example"),
            ("/app/%5C%5Cevil.example", "/evil.example"),
        ],
    )
    def test_old_paths_redirect_to_the_root(self, tc, password, path, target):
        resp = _get_page(tc, path, with_session=True)
        assert resp.status_code == 307
        assert resp.headers["location"] == target

    @pytest.mark.parametrize("pw", ["", "pw"])
    def test_login_always_required(self, tc, monkeypatch, pw):
        # Even an empty PASSWORD never opens the dashboard or logs anyone in.
        monkeypatch.setattr(auth, "ADMIN_PASSWORD", pw)
        resp = _get_page(tc, "/system", with_session=False)
        assert resp.status_code in (302, 307) and resp.headers["location"] == "/login"
        assert tc.get("/login", follow_redirects=False).status_code == 200
        assert not auth.password_matches("", pw)

    def test_log_out_then_dashboard_needs_login(self, tc, password):
        token = auth.make_session_token()
        resp = tc.post(
            "/logout",
            headers={
                "origin": ORIGIN,
                CSRF_HEADER: "1",
                "cookie": f"{auth.ADMIN_COOKIE_NAME}={token}",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert resp.headers["location"] == "/login"
        assert "Max-Age=0" in resp.headers["set-cookie"]
        # The client now has no cookie, so the dashboard sends it to /login.
        resp = tc.get("/system", headers={"accept": "text/html"}, follow_redirects=False)
        assert resp.status_code in (302, 307)
        assert resp.headers["location"].endswith("/login")
