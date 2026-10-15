"""Tasks framework core: registry, TaskService outcomes, cancel, slots,
recover, the in-row log and report, wire shape and the RunContext write and publish throttles."""
import asyncio

import pytest
from pydantic import BaseModel, Field

from copycord.admin.tasks import registry
from copycord.admin.tasks import service as service_module
from copycord.admin.tasks.context import RunContext
from copycord.admin.tasks.service import (
    INTERRUPTED,
    LOG_KEEP,
    MemoryRunRepo,
    RunFinished,
    TaskRun,
    TaskService,
)


class NoopInputs(BaseModel):
    n: int = Field(1, ge=1)
    channel_ids: list[int] = []
    mode: str = "resume"


class Gate:
    """Lets a test hold runs open and release them."""

    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()


async def _noop(ctx, inputs):
    step = ctx.step("work", "Work")
    await step.start()
    for i in range(inputs.n):
        await step.progress(i + 1, inputs.n)
    await ctx.log("did the work", step="work")
    await ctx.summary(done=inputs.n)
    await step.finish("success", "ok")


async def _boom(ctx, inputs):
    await ctx.log("about to fail")
    raise RuntimeError("boom")


async def _soft_fail(ctx, inputs):
    step = ctx.step("a", "A")
    await step.start()
    await step.finish("failed", "1 channel failed")
    ctx.fail("1 of 3 channels failed")


async def _slow(ctx, inputs):
    step = ctx.step("loop", "Loop")
    await step.start()
    while True:
        ctx.check_cancelled()
        await asyncio.sleep(0.01)


async def _stubborn(ctx, inputs):
    step = ctx.step("stuck", "Stuck")
    await step.start()
    await asyncio.sleep(3600)


GATES: dict = {}


async def _gated(ctx, inputs):
    gate = GATES[ctx.run.number]
    gate.started.set()
    await gate.release.wait()


def _describe(inputs, lookups):
    return f"Noop x{inputs.n}", {"label": "Mirror A", "mapping_id": 2**60 + 7, "guild_id": 2**62 + 1}


TYPES = [
    registry.TaskType(key="t_noop", title="Noop", run=_noop, inputs=NoopInputs, describe=_describe),
    registry.TaskType(key="t_boom", title="Boom", run=_boom, inputs=NoopInputs),
    registry.TaskType(key="t_soft", title="Soft", run=_soft_fail, inputs=NoopInputs),
    registry.TaskType(key="t_slow", title="Slow", run=_slow, inputs=NoopInputs),
    registry.TaskType(key="t_stubborn", title="Stubborn", run=_stubborn, inputs=NoopInputs),
    registry.TaskType(key="t_gated", title="Gated", run=_gated, inputs=NoopInputs, concurrency=1),
]


@pytest.fixture(autouse=True)
def _types():
    for t in TYPES:
        registry.register(t)
    GATES.clear()
    yield
    for t in TYPES:
        registry.unregister(t.key)


@pytest.fixture()
def events():
    return []


@pytest.fixture()
def svc(events):
    async def publish(event):
        events.append(event)

    return TaskService(MemoryRunRepo(), publish, cancel_grace=0.1)


async def _settled(svc, run_id, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        run = svc.get(run_id)
        if run.status in ("success", "failed", "cancelled") and run_id not in svc._active:
            return run
        await asyncio.sleep(0.01)
    raise AssertionError(f"run {run_id} still {svc.get(run_id).status}")


def test_duplicate_key_raises():
    with pytest.raises(ValueError):
        registry.register(TYPES[0])
    assert registry.get("t_noop") is TYPES[0]
    assert "t_noop" in [t.key for t in registry.all()]


@pytest.mark.asyncio
async def test_start_to_success(svc, events):
    run = await svc.start("t_noop", {"n": 3}, actor="admin", trigger="manual")
    assert run.status == "queued" and run.number == 1
    assert run.title == "Noop x3"
    done = await _settled(svc, run.id)
    assert done.status == "success" and done.error is None
    row = svc.repo.get_run(run.id)
    assert row.status == "success" and row.started_at and row.finished_at
    assert row.summary == {"done": 3}
    assert row.steps[0]["state"] == "success" and row.steps[0]["done"] == 3
    assert row.steps[0]["note"] == "ok"
    assert row.log_count == 1 and len(row.log_lines) == 1
    line = row.log_lines[0]
    assert (line["n"], line["level"], line["step"], line["msg"]) == (1, "info", "work", "did the work")
    assert line["ts"].endswith("Z")
    assert row.progress()["log_lines"] == row.log_lines
    assert row.report is None
    statuses = [e["run"]["status"] for e in events]
    assert statuses[0] == "queued" and "running" in statuses and statuses[-1] == "success"
    seqs = [e["run"]["seq"] for e in events]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    second = await svc.start("t_noop", {})
    assert second.number == 2
    await _settled(svc, second.id)


@pytest.mark.asyncio
async def test_exception_fails_with_message(svc):
    run = await svc.start("t_boom", {})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "boom"
    assert svc.repo.get_run(run.id).error == "boom"


@pytest.mark.asyncio
async def test_ctx_fail_ends_failed_without_raising(svc):
    run = await svc.start("t_soft", {})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "1 of 3 channels failed"
    assert done.steps[0]["state"] == "failed"


@pytest.mark.asyncio
async def test_cancel_mid_run(svc):
    run = await svc.start("t_slow", {})
    await asyncio.sleep(0.05)
    assert svc.get(run.id).status == "running"
    await svc.cancel(str(run.id))
    done = await _settled(svc, run.id)
    assert done.status == "cancelled" and done.error is None
    assert done.steps[0]["state"] == "cancelled"
    assert any(ln["msg"] == "Cancel requested" and ln["level"] == "warning" for ln in done.log_lines)
    with pytest.raises(RunFinished):
        await svc.cancel(run.id)
    with pytest.raises(KeyError):
        await svc.cancel(999)


@pytest.mark.asyncio
async def test_cancel_grace_timeout_forces_cancelled(svc):
    run = await svc.start("t_stubborn", {})
    await asyncio.sleep(0.05)
    await svc.cancel(run.id)
    await asyncio.sleep(0.02)
    assert svc.get(run.id).status == "running"
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    assert svc.repo.get_run(run.id).status == "cancelled"


@pytest.mark.asyncio
async def test_bots_stopped_cancels_running_and_queued_runs_at_once(svc, events):
    GATES[1], GATES[2] = Gate(), Gate()
    stuck = await svc.start("t_stubborn", {})
    holder = await svc.start("t_gated", {})
    waiting = await svc.start("t_gated", {})
    await asyncio.wait_for(GATES[1].started.wait(), 1)
    await asyncio.sleep(0.02)
    assert svc.get(stuck.id).status == "running" and svc.get(waiting.id).status == "queued"
    events.clear()

    stopped = await svc.stop_all()

    # Ended on return, before the runners get another turn of the loop.
    assert {r.id for r in stopped} == {stuck.id, holder.id, waiting.id}
    run = svc.get(stuck.id)
    assert run.status == "cancelled" and run.finished_at and run.error is None
    assert run.steps[0]["state"] == "cancelled" and run.steps[0]["note"] == "Bots stopped"
    assert any(ln["msg"] == "Bots stopped" and ln["level"] == "warning" for ln in run.log_lines)
    assert svc.repo.get_run(stuck.id).status == "cancelled"
    assert svc.get(waiting.id).status == "cancelled" and svc.get(waiting.id).started_at is None
    ended = {e["run"]["id"]: e for e in events if e["run"]["status"] == "cancelled"}
    assert set(ended) == {str(stuck.id), str(holder.id), str(waiting.id)}
    assert ended[str(stuck.id)]["steps"][0]["note"] == "Bots stopped"

    for r in (stuck, holder, waiting):
        assert (await _settled(svc, r.id)).status == "cancelled"
    assert not GATES[2].started.is_set()
    assert svc.repo.get_run(stuck.id).steps[0]["note"] == "Bots stopped"
    assert await svc.stop_all() == []


@pytest.mark.asyncio
async def test_shutdown_ends_runs_cancelled_bots_stopped(svc, monkeypatch):
    monkeypatch.setattr(service_module, "SHUTDOWN_WAIT", 0.05)
    run = await svc.start("t_stubborn", {})
    await asyncio.sleep(0.02)
    await svc.shutdown()
    row = svc.repo.get_run(run.id)
    assert row.status == "cancelled" and row.steps[0]["note"] == "Bots stopped"
    assert svc._active == {} and svc._watchdogs == {}


@pytest.mark.asyncio
async def test_concurrency_one_queues_the_second_run(svc):
    GATES[1], GATES[2], GATES[3] = Gate(), Gate(), Gate()
    first = await svc.start("t_gated", {})
    second = await svc.start("t_gated", {})
    third = await svc.start("t_gated", {})
    await asyncio.wait_for(GATES[1].started.wait(), 1)
    await asyncio.sleep(0.05)
    assert svc.get(first.id).status == "running"
    assert svc.get(second.id).status == "queued"
    assert not GATES[2].started.is_set()
    await svc.cancel(third.id)
    assert (await _settled(svc, third.id)).status == "cancelled"
    assert not GATES[3].started.is_set()
    GATES[1].release.set()
    await asyncio.wait_for(GATES[2].started.wait(), 1)
    assert (await _settled(svc, first.id)).status == "success"
    assert svc.get(second.id).status == "running"
    GATES[2].release.set()
    assert (await _settled(svc, second.id)).status == "success"


def _row(repo, type_key, status, number):
    run = TaskRun(type=type_key, number=number, status=status, title=f"{type_key} {number}",
                  trigger="manual", actor="admin", inputs={},
                  steps=[{"key": "s", "title": "S", "state": "running"}])
    run.id = repo.insert_run(run)
    return run


def test_recover_marks_interrupted():
    repo = MemoryRunRepo()
    svc = TaskService(repo)
    q = _row(repo, "t_noop", "queued", 1)
    r = _row(repo, "t_noop", "running", 2)
    ok = _row(repo, "t_noop", "success", 3)
    assert svc.recover() == 2
    for run in (q, r):
        row = repo.get_run(run.id)
        assert row.status == "failed" and row.error == INTERRUPTED and row.finished_at
        assert row.steps[0]["state"] == "failed"
    assert repo.get_run(ok.id).status == "success"
    assert svc.recover() == 0


def test_service_has_no_prune():
    assert not hasattr(TaskService, "prune")


@pytest.mark.asyncio
async def test_log_keeps_newest_lines_and_cleans_text():
    repo = MemoryRunRepo()
    run = TaskRun(type="t_noop", number=1, status="running", title="x", trigger="manual",
                  actor="admin", inputs={})
    run.id = repo.insert_run(run)
    ctx = RunContext(run, repo, lambda e: asyncio.sleep(0), lambda: 1000.0)
    await ctx.log("multi\nline\ttext", level="warning", step="a")
    for i in range(LOG_KEEP + 4):
        await ctx.log(f"x{i}")
    await ctx.flush()
    row = repo.get_run(run.id)
    assert row.log_count == LOG_KEEP + 5 and len(row.log_lines) == LOG_KEEP
    assert row.log_lines[0]["n"] == 6 and row.log_lines[-1]["msg"] == f"x{LOG_KEEP + 3}"
    first = TaskRun(type="t", number=1, status="running", title="", trigger="manual",
                    actor="admin", inputs={})
    first.load_progress({"log_lines": 3})  # rows written before log_lines was a list
    assert first.log_lines == [] and first.log_count == 0


@pytest.mark.asyncio
async def test_report_dict_or_fields(svc):
    run = await svc.start("t_noop", {})
    ctx = svc._active[run.id][0]
    await ctx.report({"channels": [{"channel_id": 2**60 + 1, "delivered": 3}]}, took_s=1.5)
    assert ctx.run.report == {"channels": [{"channel_id": 2**60 + 1, "delivered": 3}], "took_s": 1.5}
    await ctx.report(ok=True)
    assert ctx.run.report == {"ok": True}
    await ctx.report({"data": 1}, data=2)
    assert ctx.run.report == {"data": 2}
    done = await _settled(svc, run.id)
    assert svc.repo.get_run(run.id).report == {"data": 2}
    detail = done.to_wire(detail=True)
    assert detail["report"] == {"data": 2}
    assert [ln["msg"] for ln in detail["log_lines"]] == ["did the work"]
    plain = done.to_wire()
    assert "report" not in plain and "log_lines" not in plain and plain["log_count"] == 1
    done.report = {"channel_id": 2**60 + 1}
    assert done.to_wire(detail=True)["report"] == {"channel_id": str(2**60 + 1)}


@pytest.mark.asyncio
async def test_to_wire_ids_are_strings(svc):
    run = await svc.start("t_noop", {"channel_ids": [2**60 + 1, 2**61 + 3]})
    done = await _settled(svc, run.id)
    wire = done.to_wire()
    assert wire["id"] == str(run.id)
    assert wire["context"] == {"label": "Mirror A", "mapping_id": str(2**60 + 7),
                               "guild_id": str(2**62 + 1)}
    assert wire["inputs"]["channel_ids"] == [str(2**60 + 1), str(2**61 + 3)]
    assert wire["inputs"]["n"] == 1
    assert set(wire) >= {"id", "type", "number", "title", "context", "status", "trigger", "actor",
                         "inputs", "created_at", "started_at", "finished_at", "error", "summary",
                         "log_count", "steps", "seq"}
    runs, total = svc.list({"type": "t_noop"})
    assert total == 1
    listed = runs[0].to_wire(include_steps=False)
    assert "steps" not in listed and listed["id"] == str(run.id)


@pytest.mark.asyncio
async def test_publish_throttle_transitions_immediate_progress_coalesced():
    now = [1000.0]
    repo, events = MemoryRunRepo(), []

    async def publish(event):
        events.append(event)

    run = TaskRun(type="t_noop", number=1, status="running", title="x", trigger="manual",
                  actor="admin", inputs={})
    run.id = repo.insert_run(run)
    ctx = RunContext(run, repo, publish, lambda: now[0])
    step = ctx.step("a", "A")
    await step.start()
    assert len(events) == 1 and repo.writes == 1
    for i in range(10):
        await step.progress(i + 1, 10)
    assert len(events) == 1 and repo.writes == 1
    await step.finish("success")
    assert len(events) == 2 and repo.writes == 2
    assert events[-1]["steps"][0]["done"] == 10 and events[-1]["steps"][0]["state"] == "success"
    assert "inputs" not in events[-1]["run"] and "steps" not in events[-1]["run"]
    await asyncio.sleep(0.6)
    assert len(events) == 2

    # A burst inside 500 ms ends with one trailing publish carrying the last value.
    other = ctx.step("b", "B")
    await other.start()
    count = len(events)
    await other.progress(1, 5)
    await other.progress(4, 5)
    assert len(events) == count
    await asyncio.sleep(0.6)
    assert len(events) == count + 1
    assert events[-1]["steps"] == [dict(other.data)] and events[-1]["steps"][0]["done"] == 4

    # Past the interval a progress publishes at once and writes once a second.
    writes = repo.writes
    now[0] += 1.2
    await other.progress(5, 5)
    assert len(events) == count + 2 and repo.writes == writes + 1
    seqs = [e["run"]["seq"] for e in events]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    ctx.close()


@pytest.mark.asyncio
async def test_frame_lists_changed_steps_in_run_order():
    """New steps reach the SPA in a frame and are appended in frame order, so the frame keeps
    the run's order (a set of dirty keys did not)."""
    events = []

    async def publish(event):
        events.append(event)

    repo = MemoryRunRepo()
    run = TaskRun(type="t_noop", number=1, status="running", title="x", trigger="manual",
                  actor="admin", inputs={})
    run.id = repo.insert_run(run)
    ctx = RunContext(run, repo, publish, lambda: 1000.0)
    keys = [str(2**60 + i * 7919) for i in range(40)]
    for key in keys:
        ctx.step(key, f"#{key}")
    await ctx.flush()
    assert [s["key"] for s in events[-1]["steps"]] == keys
    for key in (keys[30], keys[5], keys[17]):
        await ctx.step(key).progress(1, 2)
    await ctx.flush()
    assert [s["key"] for s in events[-1]["steps"]] == [keys[5], keys[17], keys[30]]
    ctx.close()


@pytest.mark.asyncio
async def test_progress_label_publishes_with_the_counts():
    events = []

    async def publish(event):
        events.append(event)

    repo = MemoryRunRepo()
    run = TaskRun(type="t_noop", number=1, status="running", title="x", trigger="manual",
                  actor="admin", inputs={})
    run.id = repo.insert_run(run)
    ctx = RunContext(run, repo, publish, lambda: 1000.0)
    step = ctx.step("a", "A")
    await step.start()
    count = len(events)
    await step.progress(3, 10, label="3 of 10 delivered, 10 pulled")
    assert len(events) == count, "a label with the counts is not a transition"
    await asyncio.sleep(0.6)
    assert events[-1]["steps"][0]["label"] == "3 of 10 delivered, 10 pulled"
    await step.progress(4)
    assert step.data["label"] == "3 of 10 delivered, 10 pulled" and step.data["total"] == 10
    ctx.close()


@pytest.mark.asyncio
async def test_invalid_inputs_raise_value_error_dict(svc):
    with pytest.raises(ValueError) as err:
        await svc.start("t_noop", {"n": 0})
    assert isinstance(err.value.args[0], dict) and "n" in err.value.args[0]
    with pytest.raises(ValueError) as err:
        await svc.start("t_noop", {"n": "many"})
    assert set(err.value.args[0]) == {"n"}
    with pytest.raises(ValueError) as err:
        await svc.start("nope", {})
    assert err.value.args[0] == {"type": "Unknown task type 'nope'"}
    assert svc.repo.rows == {}
