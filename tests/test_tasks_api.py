"""/api/tasks REST surface and the /tasks SPA pages (Tasks framework, lane L3)."""
import asyncio
import collections
import json
import time

import pytest
from pydantic import BaseModel, Field
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin.app import app
from copycord.admin.security import CSRF_HEADER
from copycord.admin.tasks import registry
from copycord.admin.tasks import routes as task_routes
from copycord.admin.tasks.logstore import LogStore
from copycord.admin.tasks.repo import SqliteTaskRepo
from copycord.admin.tasks.service import TaskService

KEY = "noop_api"
ORIGIN = "http://testserver"


class Inputs(BaseModel):
    n: int = Field(1, ge=1)


async def _run(ctx, inputs):
    step = ctx.step("work", "Work")
    await step.start()
    for i in range(inputs.n):
        await ctx.log(f"line {i + 1}", step="work")
    await step.finish("success", "ok")


async def _options(params):
    return {"echo": params.get("x"), "guild_id": 2 ** 60}


@pytest.fixture()
def events():
    return []


@pytest.fixture()
def client(db, tmp_path, monkeypatch, events):
    registry.register(registry.TaskType(
        key=KEY, title="Noop", run=_run, inputs=Inputs, options=_options,
        describe=lambda i, lk: (f"Noop x{i.n}", {"label": "Ctx"}),
    ))

    async def publish(event):
        events.append(event)

    task_routes.set_service(TaskService(SqliteTaskRepo(db), LogStore(tmp_path / "logs"),
                                        publish=publish))
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    # One event loop for every request so started runs progress between calls;
    # the real startup hooks (agents, schedulers) stay off.
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    with TestClient(app) as tc:
        tc.headers.update({"origin": ORIGIN, CSRF_HEADER: "1",
                           "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
        yield tc
    task_routes.set_service(None)
    registry.unregister(KEY)


def _start(tc, inputs=None, **extra):
    return tc.post("/api/tasks/runs", json={"type": KEY, "inputs": inputs or {}, **extra})


def _finished(tc, run_id, timeout=3.0):
    end = time.monotonic() + timeout
    while True:
        run = tc.get(f"/api/tasks/runs/{run_id}").json()["run"]
        if run["status"] in ("success", "failed", "cancelled"):
            return run
        assert time.monotonic() < end, f"run {run_id} stuck at {run['status']}"
        time.sleep(0.02)


def test_types_lists_the_registered_type(client):
    types = client.get("/api/tasks/types").json()["types"]
    noop = next(t for t in types if t["key"] == KEY)
    assert noop["title"] == "Noop" and noop["active"] == 0 and noop["total"] == 0
    assert noop["inputs_schema"]["properties"]["n"]["minimum"] == 1


def test_options(client):
    resp = client.get(f"/api/tasks/types/{KEY}/options?x=7")
    assert resp.status_code == 200
    assert resp.json() == {"echo": "7", "guild_id": str(2 ** 60)}
    assert client.get("/api/tasks/types/nope/options").status_code == 404


def test_start_get_list_cancel_and_log(client, events):
    resp = _start(client, {"n": 3})
    assert resp.status_code == 201
    run = resp.json()["run"]
    assert run["id"] == str(int(run["id"])) and run["number"] == 1
    assert run["title"] == "Noop x3" and run["trigger"] == "manual" and run["actor"] == "admin"

    done = _finished(client, run["id"])
    assert done["status"] == "success" and done["steps"][0]["state"] == "success"
    assert done["log_lines"] == 3

    second = _start(client, trigger="rerun").json()["run"]
    assert second["number"] == 2 and second["trigger"] == "rerun"
    _finished(client, second["id"])

    body = client.get("/api/tasks/runs").json()
    assert body["total"] == 2 and body["page"] == 1
    assert [r["id"] for r in body["runs"]] == [second["id"], run["id"]]
    assert "steps" not in body["runs"][0]
    assert body["actors"] == ["admin"] and body["contexts"] == ["Ctx"]

    body = client.get("/api/tasks/runs", params={"q": "#1"}).json()
    assert body["total"] == 1 and body["runs"][0]["id"] == run["id"]
    body = client.get("/api/tasks/runs",
                      params={"type": KEY, "status": "success", "actor": "admin",
                              "context": "Ctx", "per_page": 1, "page": 2}).json()
    assert body["total"] == 2 and body["page"] == 2 and body["runs"][0]["id"] == run["id"]
    assert client.get("/api/tasks/runs", params={"status": "failed"}).json()["total"] == 0

    resp = client.post(f"/api/tasks/runs/{run['id']}/cancel")
    assert resp.status_code == 409
    assert resp.json()["run"]["status"] == "success"
    assert client.post("/api/tasks/runs/999/cancel").status_code == 404
    assert client.get("/api/tasks/runs/999").status_code == 404
    assert client.get("/api/tasks/runs/abc").status_code == 404

    log = client.get(f"/api/tasks/runs/{run['id']}/log").json()
    assert [ln["msg"] for ln in log["lines"]] == ["line 1", "line 2", "line 3"]
    assert log["lines"][0] == {"n": 1, "ts": log["lines"][0]["ts"], "level": "info",
                               "step": "work", "msg": "line 1"}
    assert log["next"] == 3 and log["complete"] is True
    log = client.get(f"/api/tasks/runs/{run['id']}/log", params={"after": 2, "step": "work"}).json()
    assert [ln["n"] for ln in log["lines"]] == [3]

    assert events and all(e["type"] == "task.run.updated" for e in events)
    assert all("inputs" not in e["run"] for e in events)


def test_start_errors(client):
    resp = _start(client, {"n": 0})
    assert resp.status_code == 422 and "n" in resp.json()["errors"]
    resp = client.post("/api/tasks/runs", json={"type": "nope", "inputs": {}})
    assert resp.status_code == 404
    assert _start(client, trigger="cron").status_code == 422
    assert client.get("/api/tasks/runs").json()["total"] == 0


def test_spa_pages_serve_index(client):
    for path in ("/tasks", "/tasks/runs/1"):
        resp = client.get(path, headers={"accept": "text/html"})
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/html")
        assert '<div id="root">' in resp.text


def test_logged_out_api_is_401(client):
    client.headers.pop("cookie")
    for method, path in (("GET", "/api/tasks/runs"), ("POST", "/api/tasks/runs"),
                         ("GET", "/api/tasks/types")):
        resp = client.request(method, path, follow_redirects=False)
        assert resp.status_code == 401
        assert resp.json() == {"detail": "Unauthorized"}


def test_publish_relay_frame(monkeypatch):
    sent = []

    async def fake_broadcast_text(text):
        sent.append(text)

    monkeypatch.setattr(app_module.hub, "_broadcast_text", fake_broadcast_text)
    monkeypatch.setattr(app_module.hub, "recent", collections.deque(maxlen=50))
    monkeypatch.setattr(app_module.hub, "subscribers", set())
    run = {"id": "5", "status": "running"}
    asyncio.run(app_module._publish_task_event(
        {"type": "task.run.updated", "run": run, "steps": [{"key": "a"}]}))

    frame = json.loads(sent[-1])
    assert frame == {"kind": "task.run.updated", "role": "unknown",
                     "payload": {"type": "task.run.updated", "run": run, "steps": [{"key": "a"}]}}
