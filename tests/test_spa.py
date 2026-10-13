"""
The new SPA at /app: behind the login guard, index.html for client routes,
hashed assets with a long cache, no path traversal.
"""
import sys

import pytest
from starlette.testclient import TestClient

from copycord.admin import auth
from copycord.admin.app import app

app_module = sys.modules["copycord.admin.app"]

INDEX = '<!doctype html><div id="root"></div>'


@pytest.fixture()
def tc():
    return TestClient(app)


@pytest.fixture()
def spa(tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text(INDEX, encoding="utf-8")
    (dist / "assets" / "index-abc123.js").write_text("console.log(1)", encoding="utf-8")
    (tmp_path / "secret.txt").write_text("secret", encoding="utf-8")
    monkeypatch.setattr(app_module, "SPA_DIRS", (tmp_path / "missing", dist))
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    return dist


def _cookie():
    return {"cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"}


@pytest.mark.parametrize("path", ["/app", "/app/", "/app/channels", "/app/assets/index-abc123.js"])
def test_requires_login(tc, spa, path):
    resp = tc.get(path, headers={"accept": "text/html"}, follow_redirects=False)
    assert resp.status_code in (302, 307)
    assert resp.headers["location"] == "/login"


@pytest.mark.parametrize("path", ["/app", "/app/", "/app/channels", "/app/system/deep/link"])
def test_client_routes_get_index(tc, spa, path):
    resp = tc.get(path, headers=_cookie(), follow_redirects=False)
    assert resp.status_code == 200
    assert resp.text == INDEX
    assert resp.headers["cache-control"] == "no-cache"
    assert resp.headers["content-type"].startswith("text/html")


def test_assets_served_with_long_cache(tc, spa):
    resp = tc.get("/app/assets/index-abc123.js", headers=_cookie())
    assert resp.status_code == 200
    assert resp.text == "console.log(1)"
    assert resp.headers["cache-control"] == app_module.SPA_ASSET_CACHE
    assert "javascript" in resp.headers["content-type"]


def test_missing_asset_is_404(tc, spa):
    assert tc.get("/app/assets/nope.js", headers=_cookie()).status_code == 404


@pytest.mark.parametrize(
    "path",
    [
        "/app/assets/..%2Fsecret.txt",
        "/app/assets/..%2F..%2Fsecret.txt",
        "/app/assets/%2e%2e/%2e%2e/secret.txt",
        "/app/assets/..%5C..%5Csecret.txt",
        "/app/assets/sub%2F..%2F..%2F..%2Fsecret.txt",
    ],
)
def test_path_traversal_rejected(tc, spa, path):
    resp = tc.get(path, headers=_cookie())
    assert "secret" not in resp.text
    assert resp.status_code in (400, 404) or resp.text == INDEX


def test_not_built_is_404(tc, tmp_path, monkeypatch):
    monkeypatch.setattr(app_module, "SPA_DIRS", (tmp_path / "none",))
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    resp = tc.get("/app/", headers=_cookie())
    assert resp.status_code == 404
    assert "npm run build" in resp.text


def test_old_pages_still_served(tc, spa):
    resp = tc.get("/login", follow_redirects=False)
    assert resp.status_code == 200
