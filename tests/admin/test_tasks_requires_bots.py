"""Task types that need the bots: /api/tasks/types says so, and POST /api/tasks/runs answers
409 "Start the bots first" while /api/status does not report both bots running."""
import time

import pytest
from pydantic import BaseModel
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin.app import app
from copycord.admin.security import CSRF_HEADER
from copycord.admin.tasks import registry
from copycord.admin.tasks import routes as task_routes
from copycord.admin.tasks.repo import SqliteTaskRepo
from copycord.admin.tasks.service import TaskService

NEEDS = "needs_bots_api"
FREE = "no_bots_api"
ORIGIN = "http://testserver"


class Inputs(BaseModel):
    pass


async def _run(ctx, inputs):
    step = ctx.step("work", "Work")
    await step.start()
    await step.finish("success", "ok")


@pytest.fixture()
def bots(monkeypatch):
    """Fakes the /api/status the start check reads; flip state["up"] to bring the bots up."""
    state = {"up": False, "calls": 0}

    async def fake_status():
        state["calls"] += 1
        return {"both_running": state["up"], "running": state["up"]}

    monkeypatch.setattr(app_module, "_collect_status", fake_status)
    return state


@pytest.fixture()
def client(db, monkeypatch, bots):
    registry.register(registry.TaskType(key=NEEDS, title="Needs bots", run=_run, inputs=Inputs,
                                        requires_bots=True))
    registry.register(registry.TaskType(key=FREE, title="No bots", run=_run, inputs=Inputs))
    task_routes.set_service(TaskService(SqliteTaskRepo(db)))
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    with TestClient(app) as tc:
        tc.headers.update({"origin": ORIGIN, CSRF_HEADER: "1",
                           "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
        yield tc
    task_routes.set_service(None)
    registry.unregister(NEEDS)
    registry.unregister(FREE)


def _start(tc, key):
    return tc.post("/api/tasks/runs", json={"type": key, "inputs": {}})


def _finished(tc, run_id, timeout=3.0):
    end = time.monotonic() + timeout
    while True:
        run = tc.get(f"/api/tasks/runs/{run_id}").json()["run"]
        if run["status"] in ("success", "failed", "cancelled"):
            return run
        assert time.monotonic() < end, f"run {run_id} stuck at {run['status']}"
        time.sleep(0.02)


def test_backfill_channels_requires_bots_in_the_registry():
    backfill = registry.get("backfill_channels")
    assert backfill is not None and backfill.requires_bots is True
    assert registry.TaskType(key="x", title="X", run=_run, inputs=Inputs).requires_bots is False


def test_types_expose_requires_bots(client):
    types = {t["key"]: t for t in client.get("/api/tasks/types").json()["types"]}
    assert types["backfill_channels"]["requires_bots"] is True
    assert types[NEEDS]["requires_bots"] is True
    assert types[FREE]["requires_bots"] is False


def test_start_is_409_while_the_bots_are_down(client, bots):
    resp = _start(client, NEEDS)
    assert resp.status_code == 409
    assert resp.json() == {"detail": "Start the bots first"}
    assert bots["calls"] == 1
    assert client.get(f"/api/tasks/runs?type={NEEDS}").json()["total"] == 0


def test_backfill_start_is_409_while_the_bots_are_down(client):
    resp = client.post("/api/tasks/runs", json={"type": "backfill_channels", "inputs": {"mapping_id": "m1"}})
    assert resp.status_code == 409
    assert resp.json()["detail"] == "Start the bots first"


def test_start_runs_once_the_bots_are_up(client, bots):
    bots["up"] = True
    resp = _start(client, NEEDS)
    assert resp.status_code == 201
    run = resp.json()["run"]
    assert run["type"] == NEEDS
    assert _finished(client, run["id"])["status"] == "success"


def test_a_type_without_requires_bots_never_asks_for_the_status(client, bots):
    resp = _start(client, FREE)
    assert resp.status_code == 201
    assert bots["calls"] == 0
    _finished(client, resp.json()["run"]["id"])
