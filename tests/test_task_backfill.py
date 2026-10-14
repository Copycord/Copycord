"""Backfill channels task type (Tasks framework, lane L6): a fake client sender
and job frames handed straight to the dispatcher, as the admin bus watcher does."""
import asyncio
import time

import pytest
from starlette.testclient import TestClient

from copycord.admin.tasks import backfill
from copycord.admin.tasks.service import MemoryRunRepo, TaskService

OG = 1234567890123456789
CG = 1234567890123456790
CAT = 1111111111111111111
CH = [2222222222222222221, 2222222222222222222, 2222222222222222223]
OTHER = 2222222222222222229


@pytest.fixture()
def mapping(db):
    mid = db.upsert_guild_mapping(
        mapping_id=None, mapping_name="Main", original_guild_id=OG,
        original_guild_name="Host", original_guild_icon_url=None,
        cloned_guild_id=CG, cloned_guild_name="Clone",
    )
    db.upsert_category_mapping(CAT, "Announcements", 3333333333333333333,
                               original_guild_id=OG, cloned_guild_id=CG)
    for i, cid in enumerate(CH):
        db.upsert_channel_mapping(
            original_channel_id=cid, original_channel_name=f"chan{i}",
            cloned_channel_id=4444444444444444440 + i, channel_webhook_url=None,
            original_parent_category_id=CAT, cloned_parent_category_id=3333333333333333333,
            channel_type=0, original_guild_id=OG, cloned_guild_id=CG,
        )
    db.upsert_channel_mapping(
        original_channel_id=OTHER, original_channel_name="orphan", cloned_channel_id=None,
        channel_webhook_url=None, original_parent_category_id=CAT,
        cloned_parent_category_id=None, channel_type=0, original_guild_id=OG, cloned_guild_id=CG,
    )
    return mid


@pytest.fixture()
def sent(db, monkeypatch):
    frames = []

    async def send(frame):
        frames.append(frame)
        return {"ok": True}

    monkeypatch.setattr(backfill, "_wiring", backfill._Wiring(db=db, send=send))
    monkeypatch.setattr(backfill, "CANCEL_WAIT", 0.2)
    backfill._jobs.clear()
    yield frames
    backfill._jobs.clear()


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


async def _frame(typ, **data):
    return await backfill.dispatch({"type": typ, "data": data})


@pytest.mark.asyncio
async def test_category_of_three_runs_queued_running_success(svc, sent, mapping):
    run = await svc.start("backfill_channels", {"mapping_id": mapping, "category_ids": [str(CAT)]})
    assert run.title == "Backfill category Announcements (3 channels)"
    assert run.context["label"] == "Main"
    await _until(lambda: len(_of(sent, "backfill_run")) == 3)
    runs = _of(sent, "backfill_run")
    assert [r["job_id"] for r in runs] == [f"{run.id}:{c}" for c in CH]
    assert [r["original_channel_id"] for r in runs] == [str(c) for c in CH]
    assert runs[0]["cloned_guild_id"] == str(CG) and runs[0]["range"]["mode"] == "all"

    for pos, r in enumerate(runs, 1):
        await _frame("backfill_job", job_id=r["job_id"], state="queued", queue_position=pos)
    steps = _steps(svc, run.id)
    assert [steps[str(c)]["queue_position"] for c in CH] == [1, 2, 3]
    assert steps[str(CH[0])]["title"] == "#chan0"

    for i, r in enumerate(runs):
        await _frame("backfill_job", job_id=r["job_id"], state="running")
        await _frame("backfill_job_progress", job_id=r["job_id"], pulled=50, total_estimate=100)
        await _frame("backfill_job_progress", job_id=r["job_id"], pulled=40, total_estimate=100)
        assert _steps(svc, run.id)[str(CH[i])]["done"] == 50
        await _frame("backfill_job_cleanup", job_id=r["job_id"], state="cleaning")
        assert _steps(svc, run.id)[str(CH[i])]["label"] == "Cleaning up"
        await _frame("backfill_job_end", job_id=r["job_id"], ok=True, error=None,
                     delivered=1000 + i, last_orig_message_id=str(9 * 10 ** 18))

    done = await _settled(svc, run.id)
    assert done.status == "success" and done.error is None
    assert done.summary == {"success": 3, "failed": 0, "skipped": 0, "cancelled": 0,
                            "delivered": 3003}
    report = done.report
    assert [c["channel_id"] for c in report["channels"]] == [str(c) for c in CH]
    assert report["channels"][0]["outcome"] == "success"
    assert report["channels"][0]["delivered"] == 1000
    assert report["channels"][0]["started_at"] and report["channels"][0]["finished_at"]
    assert report["inputs"]["category_ids"] == [str(CAT)]
    assert report["mapping"]["cloned_guild_id"] == str(CG)
    assert report["summary"]["delivered"] == 3003
    assert any(ln["msg"] == "Finished #chan0: 1,000 messages" for ln in done.log_lines)
    assert any(ln["msg"] == "Queued #chan1 (2 in queue)" for ln in done.log_lines)
    wire = done.to_wire(detail=True)
    assert wire["report"]["channels"][2]["channel_id"] == str(CH[2])
    assert wire["inputs"]["category_ids"] == [str(CAT)]


@pytest.mark.asyncio
async def test_busy_channel_is_skipped_and_cancel_ends_cancelled(svc, sent, mapping):
    first = await svc.start("backfill_channels", {"mapping_id": mapping, "channel_ids": [str(CH[0])]})
    await _until(lambda: len(_of(sent, "backfill_run")) == 1)
    await _frame("backfill_job", job_id=f"{first.id}:{CH[0]}", state="running")

    second = await svc.start("backfill_channels",
                             {"mapping_id": mapping, "channel_ids": [str(CH[0]), str(CH[1])]})
    await _until(lambda: len(_of(sent, "backfill_run")) == 2)
    assert _of(sent, "backfill_run")[1]["job_id"] == f"{second.id}:{CH[1]}"
    skipped = _steps(svc, second.id)[str(CH[0])]
    assert skipped["state"] == "skipped" and skipped["note"] == f"Already running in #{first.number}"
    await _frame("backfill_job_end", job_id=f"{second.id}:{CH[1]}", ok=True, delivered=5)
    done = await _settled(svc, second.id)
    assert done.status == "success"
    assert done.summary["skipped"] == 1 and done.summary["success"] == 1
    assert any(f"Skipped #chan0: already running in #{first.number}" == ln["msg"]
               for ln in done.log_lines)

    await svc.cancel(first.id)
    await _until(lambda: _of(sent, "backfill_cancel"))
    assert _of(sent, "backfill_cancel") == [{"job_id": f"{first.id}:{CH[0]}"}]
    await _frame("backfill_job_end", job_id=f"{first.id}:{CH[0]}", ok=False, error="cancelled",
                 delivered=3)
    cancelled = await _settled(svc, first.id)
    assert cancelled.status == "cancelled"
    assert _steps(svc, first.id)[str(CH[0])]["state"] == "cancelled"
    assert cancelled.report["summary"]["cancelled"] == 1


@pytest.mark.asyncio
async def test_cancel_without_end_frames_still_ends_cancelled(svc, sent, mapping):
    run = await svc.start("backfill_channels", {"mapping_id": mapping, "category_ids": [str(CAT)]})
    await _until(lambda: len(_of(sent, "backfill_run")) == 3)
    await svc.cancel(run.id)
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    assert len(_of(sent, "backfill_cancel")) == 3
    assert {s["state"] for s in done.steps} == {"cancelled"}


@pytest.mark.asyncio
async def test_one_failed_channel_fails_the_run(svc, sent, mapping):
    run = await svc.start("backfill_channels", {"mapping_id": mapping, "category_ids": [str(CAT)]})
    await _until(lambda: len(_of(sent, "backfill_run")) == 3)
    for i, c in enumerate(CH):
        ok = i != 1
        await _frame("backfill_job_end", job_id=f"{run.id}:{c}", ok=ok,
                     error=None if ok else "boom", delivered=10)
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "1 of 3 channels failed"
    assert done.report["channels"][1]["outcome"] == "failed"
    assert done.report["channels"][1]["error"] == "boom"
    assert _steps(svc, run.id)[str(CH[1])]["note"] == "boom"


@pytest.mark.asyncio
async def test_unknown_job_id_is_ignored(sent):
    assert await _frame("backfill_job", job_id="999:1", state="running") is False
    assert await backfill.dispatch({"type": "backfill_ack", "data": {}}) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("inputs, field", [
    ({"channel_ids": [], "category_ids": []}, None),
    ({"channel_ids": ["1"], "range": {"mode": "since"}}, "range"),
    ({"channel_ids": ["1"], "range": {"mode": "between", "since": "2024-02-01",
                                      "until": "2024-01-01"}}, "range"),
    ({"channel_ids": ["1"], "range": {"mode": "between", "since": "2024-02-01"}}, "range"),
    ({"channel_ids": ["1"], "range": {"mode": "last_n", "last_n": 0}}, "range"),
    ({"channel_ids": ["abc"]}, "channel_ids"),
])
async def test_inputs_validation(svc, sent, inputs, field):
    with pytest.raises(ValueError) as err:
        await svc.start("backfill_channels", {"mapping_id": "m1", **inputs})
    errors = err.value.args[0]
    assert isinstance(errors, dict) and errors
    if field:
        assert any(k.startswith(field) for k in errors)


def test_valid_inputs_keep_19_digit_ids_as_strings():
    inputs = backfill.BackfillInputs.model_validate(
        {"mapping_id": "m", "channel_ids": [CH[0], str(CH[0])],
         "range": {"mode": "last_n", "last_n": 5}})
    assert inputs.channel_ids == [str(CH[0])]
    assert inputs.model_dump(mode="json")["range"] == {"mode": "last_n", "since": None,
                                                        "until": None, "last_n": 5}


@pytest.mark.asyncio
async def test_options_flag_orphan_busy_and_resumable(svc, sent, mapping, db):
    db.backfill_create_run(CH[2], {}, run_id="old", original_guild_id=OG, cloned_guild_id=CG)
    db.backfill_update_checkpoint("old", last_orig_message_id="5", last_orig_timestamp="2026-10-03T00:00:00Z")
    run = await svc.start("backfill_channels", {"mapping_id": mapping, "channel_ids": [str(CH[0])]})
    await _until(lambda: len(_of(sent, "backfill_run")) == 1)
    opts = await backfill.options({"mapping_id": mapping})
    chans = {c["id"]: c for c in opts["categories"][0]["channels"]}
    assert chans[str(OTHER)]["orphan"] is True
    assert chans[str(CH[0])]["busy"] == {"run_id": str(run.id), "number": run.number}
    assert chans[str(CH[2])]["resumable"]["checkpoint_at"] == "2026-10-03T00:00:00Z"
    assert chans[str(CH[1])]["busy"] is None and chans[str(CH[1])]["resumable"] is None
    await svc.cancel(run.id)
    await _settled(svc, run.id)


def test_legacy_start_route_starts_a_task_run(db, sent, mapping, monkeypatch):
    from copycord.admin import app as app_module
    from copycord.admin import auth
    from copycord.admin.app import app
    from copycord.admin.security import CSRF_HEADER
    from copycord.admin.tasks import routes as task_routes
    from copycord.admin.tasks.repo import SqliteTaskRepo

    service = TaskService(SqliteTaskRepo(db), cancel_grace=2.0)
    task_routes.set_service(service)
    monkeypatch.setattr(app_module, "db", db)
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    try:
        with TestClient(app) as tc:
            tc.headers.update({"origin": "http://testserver", CSRF_HEADER: "1",
                               "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
            res = tc.post("/api/backfill/start", json={
                "channel_id": str(CH[0]), "mapping_id": mapping, "mode": "last_n", "last_n": 7,
            })
            assert res.status_code == 200, res.text
            body = res.json()
            assert body["ok"] is True
            run_id = body["task_run_id"]
            end = time.monotonic() + 3
            while not _of(sent, "backfill_run"):
                assert time.monotonic() < end
                time.sleep(0.02)
            frame = _of(sent, "backfill_run")[0]
            assert frame["job_id"] == f"{run_id}:{CH[0]}"
            assert frame["range"] == {"mode": "last_n", "since": None, "until": None, "last_n": 7}
            run = tc.get(f"/api/tasks/runs/{run_id}").json()["run"]
            assert run["type"] == "backfill_channels" and run["trigger"] == "api"

            again = tc.post("/api/backfill/start", json={"channel_id": str(CH[0]), "mapping_id": mapping})
            assert again.status_code == 409 and again.json()["error"] == "backfill-already-running"
            inflight = tc.get("/api/backfills/queue").json()
            assert inflight["items"][0]["channel_id"] == str(CH[0])

            assert tc.post(f"/api/tasks/runs/{run_id}/cancel").status_code == 200
            end = time.monotonic() + 3
            while tc.get(f"/api/tasks/runs/{run_id}").json()["run"]["status"] != "cancelled":
                assert time.monotonic() < end
                time.sleep(0.02)
    finally:
        task_routes.set_service(None)
