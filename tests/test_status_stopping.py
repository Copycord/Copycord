"""/api/status while a stop is in flight: stopping until a role confirms stopped (no Discord)."""
import asyncio

import pytest

from copycord.admin import app as app_module

RUNNING = {"ok": True, "running": True, "pid": 1, "started_at": "x", "uptime_sec": 5.0}
STOPPING = {**RUNNING, "status": "stopping", "stopping": True}
STOPPED = {"ok": True, "running": False, "pid": None, "started_at": None, "uptime_sec": None}
FAILED = {"ok": False, "running": False, "error": "timed out"}
# The bus's last word: a heartbeat from before the stop.
BUS_UP = {"running": True, "status": "Logged in as cc", "discord": {"ready": True}}


@pytest.fixture(autouse=True)
def _no_stop_requests(monkeypatch):
    monkeypatch.setattr(app_module, "_STOP_REQUESTED", {})


def _status(monkeypatch, server, client=None, bus=None):
    """_collect_status with the control answers and bus status given."""
    answers = {app_module.SERVER_CTRL_URL: server, app_module.CLIENT_CTRL_URL: client or server}

    async def fake_ws_cmd(url, payload, timeout=0.7):
        return dict(answers[url])

    monkeypatch.setattr(app_module, "_ws_cmd", fake_ws_cmd)
    monkeypatch.setattr(app_module.hub, "status", {"server": dict(bus or {}), "client": dict(bus or {})})
    return asyncio.run(app_module._collect_status())


def test_derive_state_reads_stopping_before_running():
    assert app_module._derive_state(STOPPING) == "stopping"
    assert app_module._derive_state({"running": True, "stopping": True}) == "stopping"
    assert app_module._derive_state(RUNNING) == "running"
    assert app_module._derive_state(STOPPED) == "stopped"
    assert app_module._derive_state({}) == "stopped"


def test_a_stopping_role_reads_stopping_not_ready_and_not_both_running(monkeypatch):
    res = _status(monkeypatch, STOPPING, RUNNING, bus=BUS_UP)
    assert res["server"]["state"] == "stopping"
    assert res["server"]["running"] is True
    assert res["server"]["ready"] is False
    assert res["client"]["state"] == "running"
    assert res["both_running"] is False and res["running"] is False
    assert res["stopping"] is True and res["status"] == "stopping"


def test_failed_query_while_a_stop_is_in_flight_stays_stopping(monkeypatch):
    app_module._mark_stop_requested("server", "client")
    # Timed out control sockets, with a stale running heartbeat on the bus.
    res = _status(monkeypatch, FAILED, bus=BUS_UP)
    for role in ("server", "client"):
        assert res[role]["state"] == "stopping"
        assert res[role]["running"] is True
        assert res[role]["ready"] is False
        # The stale bus status is not used to fill the gaps.
        assert res[role]["status"] == ""
    assert res["both_running"] is False
    # And with no bus status at all (the empty answer rule would say stopped).
    assert _status(monkeypatch, FAILED)["server"]["state"] == "stopping"
    # The shutdown's early "Stopped" bus note does not end it either.
    res = _status(monkeypatch, FAILED, bus={"running": False, "status": "Stopped"})
    assert res["server"]["state"] == "stopping"


def test_a_running_answer_that_raced_the_stop_reads_stopping(monkeypatch):
    app_module._mark_stop_requested("server", "client")
    assert _status(monkeypatch, RUNNING, bus=BUS_UP)["server"]["state"] == "stopping"


def test_without_a_stop_request_a_failed_query_is_not_stopping(monkeypatch):
    res = _status(monkeypatch, FAILED)
    assert res["server"]["state"] != "stopping"
    assert res["stopping"] is False


def test_flag_clears_on_a_confirmed_stopped_answer(monkeypatch):
    app_module._mark_stop_requested("server", "client")
    assert _status(monkeypatch, STOPPING)["server"]["state"] == "stopping"
    res = _status(monkeypatch, STOPPED)
    assert res["server"]["state"] == "stopped" and res["status"] == "stopped"
    assert app_module._STOP_REQUESTED == {}
    # Cleared: a later failed query is read as before.
    assert _status(monkeypatch, FAILED)["server"]["state"] != "stopping"


def test_flag_clears_on_a_start_that_went_through():
    app_module._mark_stop_requested("server", "client")
    app_module._clear_stop_requested("server", {"ok": False, "status": "stopping", "error": "still stopping"})
    assert "server" in app_module._STOP_REQUESTED
    app_module._clear_stop_requested("server", {"ok": True, "status": "started"})
    assert "server" not in app_module._STOP_REQUESTED
    assert "client" in app_module._STOP_REQUESTED


def test_flag_expires(monkeypatch):
    app_module._mark_stop_requested("server")
    app_module._STOP_REQUESTED["server"] -= app_module.STOP_PENDING_TTL + 1
    assert _status(monkeypatch, RUNNING)["server"]["state"] == "running"


def test_stop_endpoint_marks_both_roles(monkeypatch):
    from starlette.testclient import TestClient

    from copycord.admin import auth
    from copycord.admin.security import CSRF_HEADER

    async def fake_ws_cmd(url, payload, timeout=0.7):
        return {"ok": True, "status": "stopping"}

    async def no_runs():
        return None

    monkeypatch.setattr(app_module, "_ws_cmd", fake_ws_cmd)
    monkeypatch.setattr(app_module, "_stop_task_runs", no_runs)
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    with TestClient(app_module.app) as c:
        c.headers.update({"origin": "http://testserver", CSRF_HEADER: "1", "accept": "application/json",
                          "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
        r = c.post("/stop")
    assert r.status_code == 200
    assert set(app_module._STOP_REQUESTED) == {"server", "client"}
