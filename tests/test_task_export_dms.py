"""Export DMs task type: a fake client sender and the client's export_dm_*
frames handed straight to the dispatcher, as the admin bus watcher does."""
import asyncio

import pytest

from copycord.admin.tasks import export_dms
from copycord.admin.tasks.service import MemoryRunRepo, TaskService

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
    assert steps["json"]["note"] == "/data/exports/dm/1/x/messages.json"

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
    assert _steps(svc, run.id)["json"]["note"] == "/tmp/m.json"


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


@pytest.mark.asyncio
async def test_frames_it_does_not_own():
    assert await export_dms.dispatch({"type": "backfill_job", "data": {"job_id": "1"}}) is False
    assert await export_dms.dispatch({"type": "export_dm_done", "data": {"job_id": "nope"}}) is False


def test_mask_webhook():
    assert export_dms.mask_webhook(HOOK) == "https://discord.com/api/webhooks/111/..."
    assert export_dms.mask_webhook(None) is None
    assert export_dms.mask_webhook("https://x.io/a") == "https://x.io/a"
