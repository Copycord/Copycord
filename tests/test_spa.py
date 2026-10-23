"""
The SPA: behind the login guard, index.html for client routes,
hashed assets with a long cache, no path traversal.
"""

from urllib.parse import quote

import pytest
from fastapi import HTTPException
from starlette.testclient import TestClient

from copycord.admin import auth
from copycord.admin import spa as spa_module
from copycord.admin.app import app
from copycord.common.config import CURRENT_VERSION
from tests.conftest import SPA_INDEX as INDEX

# Read before the autouse spa_build fixture swaps in a stand-in build.
REAL_SPA_DIRS = spa_module.SPA_DIRS


@pytest.fixture()
def tc():
    return TestClient(app)


@pytest.fixture()
def spa(spa_build, monkeypatch):
    (spa_build.parent / "secret.txt").write_text("secret", encoding="utf-8")
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    return spa_build


def _cookie():
    return {"cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"}


@pytest.mark.parametrize("path", ["/", "/channels", "/assets-not", "/system/deep"])
def test_requires_login(tc, spa, path):
    resp = tc.get(path, headers={"accept": "text/html"}, follow_redirects=False)
    assert resp.status_code in (302, 307)
    assert resp.headers["location"] == "/login"


@pytest.mark.parametrize("path", ["/", "/channels", "/messages", "/logs"])
def test_client_routes_get_index(tc, spa, path):
    resp = tc.get(path, headers=_cookie(), follow_redirects=False)
    assert resp.status_code == 200
    assert resp.text == INDEX
    assert resp.headers["cache-control"] == "no-cache"
    assert resp.headers["content-type"].startswith("text/html")


def test_assets_served_with_long_cache(tc, spa):
    resp = tc.get("/assets/index-abc123.js", headers=_cookie())
    assert resp.status_code == 200
    assert resp.text == "console.log(1)"
    assert resp.headers["cache-control"] == spa_module.SPA_ASSET_CACHE
    assert "javascript" in resp.headers["content-type"]


def test_missing_asset_is_404(tc, spa):
    assert tc.get("/assets/nope.js", headers=_cookie()).status_code == 404


@pytest.fixture()
def hashed_assets(spa):
    # base64url hashes can start with - or _; sub/ and index.html must stay unreachable.
    assets = spa / "assets"
    for name in ("-abc123.js", "_x9.js", "Bv1.js"):
        (assets / name).write_text(f"// {name}", encoding="utf-8")
    (assets / "sub").mkdir()
    (assets / "sub" / "x.js").write_text("// nested", encoding="utf-8")
    return assets


@pytest.mark.parametrize("name", ["-abc123.js", "_x9.js", "Bv1.js"])
def test_hash_named_assets_served(tc, hashed_assets, name):
    resp = tc.get(f"/assets/{name}", headers=_cookie())
    assert resp.status_code == 200
    assert resp.text == f"// {name}"
    assert resp.headers["cache-control"] == spa_module.SPA_ASSET_CACHE
    assert tc.get(f"/assets/{name}", follow_redirects=False).status_code == 200


@pytest.mark.parametrize(
    "name",
    [
        "../index.html",
        "..",
        ".",
        "",
        "sub",
        "sub/x.js",
        "sub\\x.js",
        "..\\index.html",
        "/etc/passwd",
        str(spa_module.BASE_DIR / "spa.py"),
        "C:\\Windows\\win.ini",
        "-abc123.js:stream",
        "-abc123.js\x00",
        "missing.js",
    ],
)
def test_asset_response_rejects_non_children(hashed_assets, name):
    with pytest.raises(HTTPException) as err:
        spa_module.asset_response(name)
    assert err.value.status_code == 404


@pytest.mark.parametrize(
    "path",
    [
        "/assets/..%2Findex.html",
        "/assets/sub/x.js",
        "/assets/sub%2Fx.js",
        "/assets/..%5Cindex.html",
        "/assets/%2Fetc%2Fpasswd",
        "/assets/missing.js",
    ],
)
def test_asset_route_rejects_non_children(tc, hashed_assets, path):
    resp = tc.get(path, headers=_cookie())
    assert resp.status_code == 404
    assert '<div id="root">' not in resp.text


def test_real_build_assets_all_served(tc, spa, monkeypatch):
    monkeypatch.setattr(spa_module, "SPA_DIRS", REAL_SPA_DIRS)
    root = spa_module.spa_dir()
    if root is None or not (root / "assets").is_dir():
        pytest.skip("no dashboard build")
    names = sorted(p.name for p in (root / "assets").iterdir() if p.is_file())
    if not names:
        pytest.skip("dashboard build has no assets")
    bad = [n for n in names if tc.get(f"/assets/{quote(n)}", follow_redirects=False).status_code != 200]
    assert bad == []


@pytest.mark.parametrize(
    "path",
    [
        "/assets/..%2Fsecret.txt",
        "/assets/..%2F..%2Fsecret.txt",
        "/assets/%2e%2e/%2e%2e/secret.txt",
        "/assets/..%5C..%5Csecret.txt",
        "/assets/sub%2F..%2F..%2F..%2Fsecret.txt",
    ],
)
def test_path_traversal_rejected(tc, spa, path):
    resp = tc.get(path, headers=_cookie())
    assert "secret" not in resp.text
    assert resp.status_code in (400, 404) or resp.text == INDEX


def test_not_built_is_404(tc, tmp_path, monkeypatch):
    monkeypatch.setattr(spa_module, "SPA_DIRS", (tmp_path / "none",))
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    resp = tc.get("/", headers=_cookie())
    assert resp.status_code == 404
    assert "npm run build" in resp.text


# Login: the SPA renders the form; POST /login keeps its checks

ORIGIN = "http://testserver"


@pytest.fixture()
def limiter(monkeypatch):
    monkeypatch.setattr(auth, "LOGIN_LIMITER", auth.LoginLimiter())


def _login(tc, pw, **headers):
    return tc.post("/login", data={"password": pw}, headers={"origin": ORIGIN, **headers}, follow_redirects=False)


def _version_meta(version=CURRENT_VERSION):
    return f'<meta name="{spa_module.VERSION_META}" content="{version}" />'


def test_login_page_is_the_spa(tc, spa):
    resp = tc.get("/login", follow_redirects=False)
    assert resp.status_code == 200
    assert resp.text == INDEX.replace("</head>", _version_meta() + "</head>")
    assert resp.headers["cache-control"] == "no-cache"


def test_login_page_carries_only_the_version(tc, spa, limiter):
    # The footer version shows before sign-in; pages behind the password get the plain index.
    assert _version_meta() in tc.get("/login").text
    assert _version_meta() in _login(tc, "nope").text
    assert spa_module.VERSION_META not in tc.get("/", headers=_cookie()).text
    assert tc.get("/version", follow_redirects=False).status_code in (302, 307, 401)


def test_login_page_redirects_when_signed_in(tc, spa):
    resp = tc.get("/login", headers=_cookie(), follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"


def test_spa_assets_load_logged_out(tc, spa):
    resp = tc.get(f"{auth.SPA_ASSETS_PREFIX}index-abc123.js", follow_redirects=False)
    assert resp.status_code == 200
    assert resp.text == "console.log(1)"


def test_wrong_password_shows_error_in_meta(tc, spa, limiter):
    resp = _login(tc, "nope")
    assert resp.status_code == 401
    assert auth.ADMIN_COOKIE_NAME not in resp.cookies
    meta = f'<meta name="{spa_module.LOGIN_ERROR_META}" content="Incorrect password. Please try again." />'
    assert meta + "</head>" in resp.text
    assert '<div id="root"></div>' in resp.text


def test_lockout_message_and_retry_after(tc, spa, limiter):
    for _ in range(4):
        assert _login(tc, "nope").status_code == 401
    resp = _login(tc, "nope")
    assert resp.status_code == 429
    assert resp.headers["retry-after"] == "30"
    assert "Too many failed attempts. Try again in 30 seconds." in resp.text


def test_login_error_is_escaped(spa):
    resp = spa_module.index_response(401, login_error='"><script>alert(1)</script>')
    body = resp.body.decode()
    assert "<script>" not in body
    assert "&quot;&gt;&lt;script&gt;" in body


def test_login_success_and_cross_origin(tc, spa, limiter):
    resp = _login(tc, "pw")
    assert resp.status_code == 302 and resp.headers["location"] == "/"
    assert auth.ADMIN_COOKIE_NAME in resp.cookies
    assert _login(tc, "pw", origin="http://evil.example").status_code == 403


@pytest.mark.parametrize("path", ["/assets/..%2Fsecret.txt", "/assets/..%2F..%2Fsecret.txt", "/assets/..%5Cindex.html"])
def test_logged_out_assets_cannot_escape(tc, spa, path):
    resp = tc.get(path, follow_redirects=False)
    assert "secret" not in resp.text
    assert resp.status_code in (302, 307, 400, 404)
    assert '<div id="root">' not in resp.text
