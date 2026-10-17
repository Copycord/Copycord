"""Export DMs task type: a fake client sender and the client's export_dm_*
frames handed straight to the dispatcher, as the admin bus watcher does."""
import asyncio

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin.security import CSRF_HEADER
from copycord.admin.tasks import export_dms
from copycord.admin.tasks import routes as task_routes
from copycord.admin.tasks.service import MemoryRunRepo, TaskRun, TaskService

UID = "123456789012345678"
HOOK = "https://discord.com/api/webhooks/111/secret-token"


@pytest.fixture()
def sent(monkeypatch):
    frames = []
    replies = {}

    async def send(frame):
        frames.append(frame)
        return replies.get(frame["type"], {"ok": True})

    monkeypatch.setattr(export_dms, "_wiring", export_dms._Wiring(send=send))
    monkeypatch.setattr(export_dms, "CANCEL_WAIT", 0.2)
    export_dms._jobs.clear()
    yield frames, replies
    export_dms._jobs.clear()


@pytest.fixture()
def svc():
    return TaskService(MemoryRunRepo(), cancel_grace=2.0)


def _of(frames, typ):
    return [f["data"] for f in frames if f["type"] == typ]


async def _until(cond, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


async def _settled(svc, run_id):
    await _until(lambda: svc.get(run_id).status in ("success", "failed", "cancelled")
                 and run_id not in svc._active)
    return svc.get(run_id)


def _steps(svc, run_id):
    return {s["key"]: s for s in svc.get(run_id).steps}


async def _frame(typ, job_id, **data):
    return await export_dms.dispatch({"type": typ, "data": {"job_id": str(job_id), **data}})


async def _started(svc, sent, inputs):
    frames, _ = sent
    run = await svc.start("export_dms", inputs)
    await _until(lambda: _of(frames, "export_dm_history"))
    return run


@pytest.mark.asyncio
async def test_progress_per_batch_then_done(svc, sent):
    frames, _ = sent
    run = await _started(svc, sent, {"user_id": UID, "webhook_url": HOOK, "json_file": True})
    assert run.title == f"Export DMs with {UID}"
    req = _of(frames, "export_dm_history")[0]
    assert req == {"user_id": int(UID), "webhook_url": HOOK, "json_file": True, "job_id": str(run.id)}
    steps = _steps(svc, run.id)
    assert [k for k in steps] == ["read", "json", "forward"]
    assert steps["read"]["state"] == "running"

    await _frame("export_dm_progress", run.id, phase="count", done=100, username="alice")
    assert _steps(svc, run.id)["read"]["label"] == "Counting messages, 100 found"
    await _frame("export_dm_progress", run.id, phase="read", done=150, total=300)
    step = _steps(svc, run.id)["read"]
    assert (step["done"], step["total"], step["label"]) == (150, 300, "150 of 300 messages read")
    assert svc.get(run.id).summary["exported"] == 150
    await _frame("export_dm_progress", run.id, phase="read", done=300, total=300)
    await _frame("export_dm_progress", run.id, phase="json", done=300, total=300,
                 json_path="/data/exports/dm/1/x/messages.json")
    steps = _steps(svc, run.id)
    assert steps["read"]["state"] == "success" and steps["read"]["note"] == "300 messages read"
    assert steps["json"]["state"] == "success"
    assert steps["json"]["note"] == "300 messages saved"

    await _frame("export_dm_progress", run.id, phase="forward", done=0, total=300)
    await _frame("export_dm_progress", run.id, phase="forward", done=10, total=300)
    step = _steps(svc, run.id)["forward"]
    assert step["state"] == "running" and step["label"] == "10 of 300 forwarded"
    assert svc.get(run.id).summary == {"exported": 300, "failed": 0, "forwarded": 10}

    await _frame("export_dm_done", run.id, user_id=int(UID), username="alice", count=300, sent=300,
                 json_path="/data/exports/dm/1/x/messages.json")
    done = await _settled(svc, run.id)
    assert done.status == "success"
    steps = _steps(svc, run.id)
    assert steps["forward"]["state"] == "success" and steps["forward"]["note"] == "300 messages forwarded"
    assert done.summary == {"exported": 300, "failed": 0, "forwarded": 300}
    assert done.report["output"] == {"json_path": "/data/exports/dm/1/x/messages.json",
                                     "webhook": "https://discord.com/api/webhooks/111/..."}
    assert done.report["user"] == {"id": UID, "name": "alice"}
    assert "secret-token" not in str(done.report)
    assert export_dms._jobs == {}


@pytest.mark.asyncio
async def test_json_only_has_no_forward_step(svc, sent):
    run = await _started(svc, sent, {"user_id": UID})
    assert list(_steps(svc, run.id)) == ["read", "json"]
    await _frame("export_dm_done", run.id, count=3, sent=0, json_path="/tmp/m.json")
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert done.summary == {"exported": 3, "failed": 0}
    assert _steps(svc, run.id)["read"]["note"] == "3 messages read"
    assert _steps(svc, run.id)["json"]["note"] == "3 messages saved"
    assert done.report["output"]["json_path"] == "/tmp/m.json"
    assert all("/tmp/m.json" not in str(s) for s in svc.get(run.id).steps)


@pytest.mark.asyncio
async def test_cancel_stops_the_client_runner(svc, sent):
    frames, _ = sent
    run = await _started(svc, sent, {"user_id": UID, "webhook_url": HOOK, "json_file": False})
    await _frame("export_dm_progress", run.id, phase="read", done=20, total=20)
    await _frame("export_dm_progress", run.id, phase="forward", done=5, total=20)
    await svc.cancel(run.id)
    await _until(lambda: _of(frames, "export_dm_cancel"))
    assert _of(frames, "export_dm_cancel")[0] == {"user_id": int(UID), "job_id": str(run.id)}
    await _frame("export_dm_done", run.id, error="cancelled", count=20, sent=5)
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    steps = _steps(svc, run.id)
    assert steps["read"]["state"] == "success"
    assert steps["forward"]["state"] == "cancelled"
    assert steps["forward"]["note"] == "Cancelled after 5 forwarded"
    assert done.summary == {"exported": 20, "failed": 0, "forwarded": 5}
    assert done.report["error"] is None


@pytest.mark.asyncio
async def test_cancel_without_an_end_from_the_client(svc, sent):
    frames, _ = sent
    run = await _started(svc, sent, {"user_id": UID})
    await _frame("export_dm_progress", run.id, phase="read", done=40, total=100)
    await svc.cancel(run.id)
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    assert _of(frames, "export_dm_cancel")
    steps = _steps(svc, run.id)
    assert steps["read"]["state"] == "cancelled" and steps["read"]["note"] == "Cancelled after 40 read"
    assert steps["json"]["state"] == "cancelled"
    # A frame after the run ended belongs to no job.
    assert await _frame("export_dm_done", run.id, count=100) is False


@pytest.mark.asyncio
async def test_client_refuses_the_export(svc, sent):
    _, replies = sent
    replies["export_dm_history"] = {"ok": False, "error": "dm-export-in-progress"}
    run = await svc.start("export_dms", {"user_id": UID, "webhook_url": HOOK})
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    steps = _steps(svc, run.id)
    assert steps["read"]["state"] == "failed"
    assert steps["read"]["note"] == "A DM export for this user is already running"
    assert steps["json"]["state"] == "skipped" and steps["forward"]["state"] == "skipped"
    assert done.report["error"] == "A DM export for this user is already running"


@pytest.mark.asyncio
async def test_client_error_while_exporting(svc, sent):
    run = await _started(svc, sent, {"user_id": UID})
    await _frame("export_dm_done", run.id, error="dm-not-in-cache", count=0, sent=0)
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert done.error == "No open DM with this user on the client account"
    assert _steps(svc, run.id)["json"]["state"] == "skipped"


@pytest.mark.asyncio
async def test_json_write_failure_fails_its_step(svc, sent):
    run = await _started(svc, sent, {"user_id": UID})
    await _frame("export_dm_progress", run.id, phase="read", done=7, total=7)
    await _frame("export_dm_progress", run.id, phase="json", done=0, total=7, json_error="disk full")
    await _frame("export_dm_done", run.id, count=7, sent=0)
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert _steps(svc, run.id)["json"]["note"] == "disk full"
    assert done.summary == {"exported": 7, "failed": 7}


@pytest.mark.asyncio
async def test_inputs_are_validated(svc, sent):
    for bad, key in (({"user_id": "abc"}, "user_id"),
                     ({"user_id": UID, "webhook_url": "discord.com/x"}, "webhook_url"),
                     ({"user_id": UID, "json_file": False}, "inputs")):
        with pytest.raises(ValueError) as exc:
            await svc.start("export_dms", bad)
        assert key in exc.value.args[0]


def test_mask_webhook():
    assert export_dms.mask_webhook(HOOK) == "https://discord.com/api/webhooks/111/..."
    assert export_dms.mask_webhook(None) is None
    assert export_dms.mask_webhook("https://x.io/a") == "https://x.io/a"


# the export file route

STAMP = "20261017-022246"


@pytest.fixture()
def http(tmp_path, monkeypatch):
    repo = MemoryRunRepo()
    service = TaskService(repo)
    monkeypatch.setattr(export_dms, "_wiring", export_dms._Wiring(data_dir=tmp_path))
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    task_routes.set_service(service)
    try:
        with TestClient(app_module.app) as tc:
            tc.headers.update({"origin": "http://testserver", CSRF_HEADER: "1",
                               "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
            yield tc, repo, tmp_path
    finally:
        task_routes.set_service(None)


def _finished_run(repo, json_path, type_key="export_dms"):
    run = TaskRun(type=type_key, number=4, status="success", title=f"Export DMs with {UID}", trigger="manual",
                  actor="admin", inputs={"user_id": UID}, context={"label": UID, "user_id": UID},
                  summary={"exported": 62, "failed": 0},
                  report={"user": {"id": UID, "name": "alice"}, "output": {"json_path": json_path, "webhook": None},
                          "summary": {"exported": 62, "failed": 0}})
    return repo.insert_run(run)


def _saved(tmp_path, body=b'[{"id": "1"}]'):
    path = tmp_path / "exports" / "dm" / UID / STAMP / "messages.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(body)
    return path


def test_route_serves_the_export_file(http):
    tc, repo, tmp_path = http
    rid = _finished_run(repo, str(_saved(tmp_path)))
    info = tc.get(f"/api/tasks/export-dms/{rid}")
    assert info.status_code == 200
    assert info.json() == {"file": {"filename": f"dms-{UID}-{STAMP}.json", "size": 13, "messages": 62}}
    resp = tc.get(f"/api/tasks/export-dms/{rid}/file")
    assert resp.status_code == 200 and resp.content == b'[{"id": "1"}]'
    assert resp.headers["content-type"].startswith("application/json")
    assert f'filename="dms-{UID}-{STAMP}.json"' in resp.headers["content-disposition"]


def test_route_unknown_run_is_404(http):
    tc, repo, tmp_path = http
    other = _finished_run(repo, str(_saved(tmp_path)), type_key="backfill")
    for rid in ("0" * 32, "nope", other):
        for suffix in ("", "/file"):
            resp = tc.get(f"/api/tasks/export-dms/{rid}{suffix}")
            assert resp.status_code == 404 and resp.json() == {"detail": "Unknown run"}


def test_route_never_serves_outside_the_exports_dir(http):
    tc, repo, tmp_path = http
    _saved(tmp_path)
    secret = tmp_path / "secret.json"
    secret.write_text("{}")
    for bad in (str(secret), str(tmp_path / "exports" / "dm" / ".." / ".." / "secret.json"),
                f"{tmp_path}/exports/dm/{UID}/{STAMP}/../../../../secret.json", "/etc/passwd",
                str(tmp_path / "exports" / "dm")):
        rid = _finished_run(repo, bad)
        resp = tc.get(f"/api/tasks/export-dms/{rid}/file")
        assert resp.status_code == 404 and resp.json() == {"detail": "No export file"}, bad


def test_route_needs_a_session(http):
    tc, repo, tmp_path = http
    rid = _finished_run(repo, str(_saved(tmp_path)))
    tc.headers.pop("cookie")
    assert tc.get(f"/api/tasks/export-dms/{rid}/file", follow_redirects=False).status_code == 401
