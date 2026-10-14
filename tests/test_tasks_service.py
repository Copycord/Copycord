"""Tasks framework core: registry, TaskService outcomes, cancel, slots,
recover, prune, wire shape and the RunContext write and publish throttles."""
import asyncio

import pytest
from pydantic import BaseModel, Field

from copycord.admin.tasks import registry
from copycord.admin.tasks.context import RunContext
from copycord.admin.tasks.service import (
    INTERRUPTED,
    MemoryLogSink,
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

    return TaskService(MemoryRunRepo(), MemoryLogSink(), publish, cancel_grace=0.1)


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
    lines = svc.log_sink.lines[run.id]
    assert len(lines) == 1 and row.log_lines == 1
    seq, ts, level, step, msg = lines[0].split("\t")
    assert (seq, level, step, msg) == ("1", "info", "work", "did the work")
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
    assert any("Cancel requested" in line for line in svc.log_sink.lines[run.id])
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
    svc = TaskService(repo, MemoryLogSink())
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


def test_prune_keeps_newest_500_per_type():
    repo, sink = MemoryRunRepo(), MemoryLogSink()
    svc = TaskService(repo, sink)
    many = [_row(repo, "t_noop", "success", i + 1) for i in range(505)]
    few = [_row(repo, "t_boom", "failed", i + 1) for i in range(3)]
    for run in many[:6]:
        sink.append(run.id, "1\tts\tinfo\t\tx")
    assert svc.prune() == 5
    _, total = repo.list_runs({"type": "t_noop"})
    assert total == 500
    assert all(repo.get_run(run.id) is None for run in many[:5])
    assert repo.get_run(many[5].id) is not None
    assert all(repo.get_run(run.id) is not None for run in few)
    assert set(sink.lines) == {many[5].id}
    assert svc.prune() == 0


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
                         "log_lines", "steps", "seq"}
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
    ctx = RunContext(run, repo, MemoryLogSink(), publish, lambda: now[0])
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
