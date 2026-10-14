"""TaskService end to end on the real storage: SqliteTaskRepo over DBManager,
with the short log in progress_json and the report in report_json."""
import asyncio

import pytest
from pydantic import BaseModel

from copycord.admin.tasks import registry
from copycord.admin.tasks.repo import SqliteTaskRepo
from copycord.admin.tasks.service import INTERRUPTED, TaskRun, TaskService

KEY = "noop_sqlite"


class Inputs(BaseModel):
    n: int = 2


async def _run(ctx, inputs):
    step = ctx.step("work", "Work")
    await step.start()
    for i in range(inputs.n):
        await step.progress(i + 1, inputs.n)
        await ctx.log(f"item {i + 1}\twith tab", step="work")
    await ctx.summary(done=inputs.n)
    await ctx.report({"items": inputs.n, "guild_id": 2 ** 60})
    await step.finish("success", "ok")


@pytest.fixture()
def noop_type():
    registry.register(registry.TaskType(
        key=KEY, title="Noop", run=_run, inputs=Inputs,
        describe=lambda i, lk: (f"Noop x{i.n}", {"label": "Ctx", "guild_id": 2 ** 60}),
    ))
    yield
    registry.unregister(KEY)


async def _settled(svc, run_id, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while run_id in svc._active:
        assert loop.time() < end, "run did not finish"
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_start_to_success_persists_progress_and_log(db, noop_type):
    repo = SqliteTaskRepo(db)
    svc = TaskService(repo)
    run = await svc.start(KEY, {"n": 3})
    assert run.number == 1 and isinstance(run.id, int)
    await _settled(svc, run.id)

    stored = repo.get_run(run.id)
    assert isinstance(stored, TaskRun)
    assert stored.status == "success" and stored.error is None
    assert stored.title == "Noop x3"
    assert stored.context == {"label": "Ctx", "guild_id": 2 ** 60}
    assert stored.inputs == {"n": 3}
    assert stored.summary == {"done": 3}
    assert stored.log_count == 3
    assert stored.report == {"items": 3, "guild_id": 2 ** 60}
    assert stored.steps[0]["state"] == "success"
    assert stored.steps[0]["done"] == 3 and stored.steps[0]["total"] == 3
    assert stored.started_at and stored.finished_at
    assert stored.to_wire()["context"]["guild_id"] == str(2 ** 60)

    assert [ln["n"] for ln in stored.log_lines] == [1, 2, 3]
    assert stored.log_lines[0]["step"] == "work" and stored.log_lines[0]["msg"] == "item 1 with tab"
    detail = stored.to_wire(detail=True)
    assert detail["report"] == {"items": 3, "guild_id": str(2 ** 60)}
    assert [ln["msg"] for ln in detail["log_lines"]][-1] == "item 3 with tab"

    second = await svc.start(KEY, {"n": 1})
    await _settled(svc, second.id)
    assert second.number == 2
    runs, total = svc.list({"q": "#2"})
    assert total == 1 and runs[0].id == second.id
    runs, total = svc.list({"type": KEY, "context": "Ctx"})
    assert total == 2 and [r.id for r in runs] == [second.id, run.id]


@pytest.mark.asyncio
async def test_bad_inputs_insert_nothing(db, noop_type):
    svc = TaskService(SqliteTaskRepo(db))
    with pytest.raises(ValueError) as err:
        await svc.start(KEY, {"n": "lots"})
    assert "n" in err.value.args[0]
    assert db.task_runs_list()[1] == 0


def test_recover_marks_active_rows_failed(db, noop_type):
    repo = SqliteTaskRepo(db)
    stuck = TaskRun(type=KEY, number=1, status="running", title="t", trigger="manual",
                    actor="admin", inputs={}, created_at="2026-10-14T00:00:00Z",
                    updated_at="2026-10-14T00:00:00Z",
                    steps=[{"key": "a", "title": "A", "state": "running"}])
    rid = repo.insert_run(stuck)
    done = TaskRun(type=KEY, number=2, status="success", title="t", trigger="manual",
                   actor="admin", inputs={}, created_at="2026-10-14T00:00:00Z",
                   updated_at="2026-10-14T00:00:00Z")
    done_id = repo.insert_run(done)
    svc = TaskService(repo)
    assert svc.recover() == 1
    row = repo.get_run(rid)
    assert row.status == "failed" and row.error == INTERRUPTED
    assert row.steps[0]["state"] == "failed"
    assert repo.get_run(done_id).status == "success"
    assert svc.recover() == 0


def test_delete_run_removes_the_row(db, noop_type):
    repo = SqliteTaskRepo(db)
    ids = []
    for i in range(3):
        run = TaskRun(type=KEY, number=0, status="success", title=f"r{i}", trigger="manual",
                      actor="admin", inputs={}, created_at="2026-10-14T00:00:00Z",
                      updated_at="2026-10-14T00:00:00Z", report={"i": i})
        ids.append(repo.insert_run(run))
        assert run.number == i + 1
    assert repo.get_run(ids[1]).report == {"i": 1}
    repo.delete_run(ids[1])
    assert repo.get_run(ids[1]) is None
    assert [r.id for r in TaskService(repo).list({"type": KEY})[0]] == [ids[2], ids[0]]
    assert repo.list_types() == [KEY]
