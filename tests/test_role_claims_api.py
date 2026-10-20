"""/api/redeem: the admin proxy to the helper, with the helper client mocked."""
import logging

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin import role_claims
from copycord.admin.app import app
from copycord.admin.security import CSRF_HEADER
from copycord.common import helper_client
from copycord.common.helper_client import HelperError

ORIGIN = "http://testserver"
CODE = "SECRET-CODE-1234"
CLAIMED = {"status": "claimed", "message": "Role claimed", "reward": "role"}


class FakeHelper:
    def __init__(self):
        self.calls = []
        self.error = None
        self.answer = dict(CLAIMED)

    async def start_claim(self, code):
        self.calls.append(("start", code))
        if self.error:
            raise self.error
        return self.answer


@pytest.fixture()
def fake(monkeypatch):
    f = FakeHelper()
    monkeypatch.setattr(helper_client, "start_claim", f.start_claim)
    monkeypatch.setenv(helper_client.HELPER_URL_ENV, "https://helper.example:8443")
    return f


@pytest.fixture()
def client(monkeypatch, fake):
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    with TestClient(app) as tc:
        tc.headers.update({"origin": ORIGIN, CSRF_HEADER: "1",
                           "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
        yield tc


def _start(client, **body):
    return client.post("/api/redeem/start", json=body)


def test_logged_out_is_401(client, fake):
    client.headers.pop("cookie")
    for method, path in (("GET", "/api/redeem/config"), ("POST", "/api/redeem/start")):
        resp = client.request(method, path, json={"code": CODE}, follow_redirects=False)
        assert resp.status_code == 401
        assert resp.json() == {"detail": "Unauthorized"}
    assert fake.calls == []


def test_config_never_calls_the_helper(client, fake):
    fake.error = HelperError("helper says no", 500)
    resp = client.get("/api/redeem/config")
    assert resp.status_code == 200
    assert resp.json() == {"configured": True}
    assert fake.calls == []


def test_events_route_is_gone(client, fake):
    assert client.get("/api/redeem/events").status_code in (404, 405)
    assert fake.calls == []


def test_configured_by_default(client, fake, monkeypatch):
    monkeypatch.delenv(helper_client.HELPER_URL_ENV)
    assert client.get("/api/redeem/config").json() == {"configured": True}


def test_not_configured(client, fake, monkeypatch):
    monkeypatch.setenv(helper_client.HELPER_URL_ENV, "")
    resp = client.get("/api/redeem/config")
    assert resp.status_code == 200
    assert resp.json() == {"configured": False}
    resp = _start(client, code=CODE)
    assert resp.status_code == 503
    assert resp.json() == {"detail": role_claims.NOT_CONFIGURED}
    assert fake.calls == []


def test_start(client, fake):
    resp = _start(client, code=f"  {CODE}\n")
    assert resp.status_code == 200
    assert resp.json() == CLAIMED
    assert "authorize_url" not in resp.json()
    assert fake.calls == [("start", CODE)]


def test_start_passes_on_the_helper_message(client, fake):
    fake.answer = {"status": "claimed", "message": "Beta Tester added", "reward": "role", "extra": 1}
    assert _start(client, code=CODE).json() == {
        "status": "claimed", "message": "Beta Tester added", "reward": "role"}


@pytest.mark.parametrize("answer", [{}, {"status": "", "message": None, "reward": 3}])
def test_start_fills_in_a_bare_success(client, fake, answer):
    fake.answer = answer
    resp = _start(client, code=CODE)
    assert resp.status_code == 200
    assert resp.json() == CLAIMED


def test_start_ignores_extra_fields(client, fake):
    assert _start(client, event_id=3, code=CODE).status_code == 200
    assert fake.calls == [("start", CODE)]


def test_start_accepts_a_64_char_code(client, fake):
    assert _start(client, code="A" * 64).status_code == 200


@pytest.mark.parametrize("body", [
    {},
    {"event_id": 3},
    {"code": None},
    {"code": ""},
    {"code": "   "},
    {"code": "A" * 65},
    {"code": 1234},
    {"code": True},
    {"code": ["x"]},
])
def test_start_validation(client, fake, body):
    resp = client.post("/api/redeem/start", json=body)
    assert resp.status_code == 400
    assert isinstance(resp.json()["detail"], str)
    assert fake.calls == []


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", b"", b"null"])
def test_start_bad_json(client, fake, raw):
    resp = client.post("/api/redeem/start", content=raw, headers={"content-type": "application/json"})
    assert resp.status_code == 400
    assert fake.calls == []


@pytest.mark.parametrize("status,expected", [
    (400, 400), (403, 403), (404, 404), (409, 409), (429, 429),
    (401, 502), (500, 502), (503, 502), (None, 502),
])
def test_helper_errors_pass_through(client, fake, status, expected):
    fake.error = HelperError(f"helper says {status}", status)
    resp = _start(client, code=CODE)
    assert resp.status_code == expected
    assert resp.json() == {"detail": f"helper says {status}"}


@pytest.mark.parametrize("status,error,expected", [
    (400, "Invalid code", 400),
    (409, "Role already claimed", 409),
    (400, "Join the Copycord Discord server first", 400),
    (502, "Could not add the role, try again later", 502),
    (429, "Too many attempts, try again later", 429),
])
def test_helper_contract_errors(client, fake, status, error, expected):
    fake.error = HelperError(error, status)
    resp = _start(client, code=CODE)
    assert resp.status_code == expected
    assert resp.json() == {"detail": error}


def test_unreachable(client, fake):
    fake.error = HelperError(helper_client.UNREACHABLE)
    resp = _start(client, code=CODE)
    assert resp.status_code == 502
    assert resp.json() == {"detail": "Server is down"}


def test_code_is_never_logged(client, fake, caplog):
    caplog.set_level(logging.DEBUG)
    fake.error = HelperError("Invalid code", 400)
    _start(client, code=CODE)
    fake.error = None
    _start(client, code=CODE)
    assert "start failed" in caplog.text
    assert CODE not in caplog.text
