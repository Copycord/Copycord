"""Several Backfill channels runs at the same time: admin routing by job_id, the client's one
queue and the server's per clone state."""
import asyncio
import gc
from types import SimpleNamespace

import pytest

import copycord.client.client as cc
from copycord.admin.tasks import backfill
from copycord.admin.tasks.context import RunContext
from copycord.admin.tasks.service import MemoryRunRepo, TaskService
from copycord.server.backfill import BackfillManager

OG = 1234567890123456789
CG = 1234567890123456790
CAT = 1111111111111111111
CH = [2222222222222222221, 2222222222222222222, 2222222222222222223,
      2222222222222222224, 2222222222222222225, 2222222222222222226]


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
    backfill._late.clear()
    yield frames
    backfill._jobs.clear()
    backfill._late.clear()


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


def _ids(chans):
    return [str(c) for c in chans]


async def _start(svc, mapping, chans):
    return await svc.start("backfill_channels", {"mapping_id": mapping, "channel_ids": _ids(chans)})


async def _finish(job_id, delivered, ok=True, error=None):
    await _frame("backfill_job", job_id=job_id, state="running")
    await _frame("backfill_job_progress", job_id=job_id, pulled=delivered, total_estimate=delivered)
    await _frame("backfill_job_delivered", job_id=job_id, delivered=delivered, total=delivered)
    await _frame("backfill_job_cleanup", job_id=job_id, state="cleaning")
    await _frame("backfill_job_end", job_id=job_id, ok=ok, error=error, delivered=delivered)


# 1

@pytest.mark.asyncio
async def test_two_runs_on_disjoint_channels_keep_their_own_counts(svc, sent, mapping):
    a, b = await asyncio.gather(_start(svc, mapping, CH[:2]), _start(svc, mapping, CH[2:4]))
    assert a.number != b.number
    await _until(lambda: len(_of(sent, "backfill_run")) == 4)
    assert {r["job_id"] for r in _of(sent, "backfill_run")} == {
        f"{a.id}:{CH[0]}", f"{a.id}:{CH[1]}", f"{b.id}:{CH[2]}", f"{b.id}:{CH[3]}"}

    # Interleaved frames from both runs.
    await _frame("backfill_job", job_id=f"{a.id}:{CH[0]}", state="running")
    await _frame("backfill_job", job_id=f"{b.id}:{CH[2]}", state="running")
    await _frame("backfill_job_delivered", job_id=f"{a.id}:{CH[0]}", delivered=10, total=10)
    await _frame("backfill_job_delivered", job_id=f"{b.id}:{CH[2]}", delivered=200, total=300)
    assert svc.get(a.id).summary["delivered"] == 10
    assert svc.get(b.id).summary["delivered"] == 200
    await _frame("backfill_job_cleanup", job_id=f"{b.id}:{CH[2]}", state="cleaning")
    assert _steps(svc, a.id)[str(CH[0])]["label"] != "Cleaning up"
    await _finish(f"{a.id}:{CH[0]}", 10)
    await _finish(f"{b.id}:{CH[2]}", 300)
    await _finish(f"{b.id}:{CH[3]}", 7)
    await _finish(f"{a.id}:{CH[1]}", 20)

    da, db_ = await _settled(svc, a.id), await _settled(svc, b.id)
    assert da.status == db_.status == "success"
    assert da.summary == {"success": 2, "failed": 0, "skipped": 0, "cancelled": 0, "delivered": 30}
    assert db_.summary == {"success": 2, "failed": 0, "skipped": 0, "cancelled": 0, "delivered": 307}
    assert [c["channel_id"] for c in da.report["channels"]] == _ids(CH[:2])
    assert [c["channel_id"] for c in db_.report["channels"]] == _ids(CH[2:4])
    assert [c["delivered"] for c in da.report["channels"]] == [10, 20]
    assert [c["delivered"] for c in db_.report["channels"]] == [300, 7]
    assert set(_steps(svc, a.id)) == set(_ids(CH[:2]))
    assert set(_steps(svc, b.id)) == set(_ids(CH[2:4]))
    assert backfill._jobs == {} and backfill.busy_channels() == {}


# 2

@pytest.mark.asyncio
async def test_same_channel_skips_in_the_second_run_and_frees_after_the_first(svc, sent, mapping):
    first = await _start(svc, mapping, [CH[0]])
    await _until(lambda: len(_of(sent, "backfill_run")) == 1)
    second = await _start(svc, mapping, [CH[0], CH[1]])
    await _until(lambda: len(_of(sent, "backfill_run")) == 2)
    assert _of(sent, "backfill_run")[1]["job_id"] == f"{second.id}:{CH[1]}"
    step = _steps(svc, second.id)[str(CH[0])]
    assert step["state"] == "skipped" and step["note"] == f"Already running in #{first.number}"
    await _finish(f"{second.id}:{CH[1]}", 4)
    await _finish(f"{first.id}:{CH[0]}", 9)
    assert (await _settled(svc, first.id)).status == "success"
    done = await _settled(svc, second.id)
    assert done.status == "success" and done.summary["skipped"] == 1
    assert done.report["channels"][0]["outcome"] == "skipped"
    assert backfill.busy_channels() == {}

    third = await _start(svc, mapping, [CH[0]])
    await _until(lambda: len(_of(sent, "backfill_run")) == 3)
    assert _of(sent, "backfill_run")[2]["job_id"] == f"{third.id}:{CH[0]}"
    await _finish(f"{third.id}:{CH[0]}", 1)
    assert (await _settled(svc, third.id)).status == "success"


@pytest.mark.asyncio
async def test_runs_starting_together_never_both_take_a_channel(svc, sent, mapping, monkeypatch):
    """A run that yields while it hands out steps (a skip logs and writes) must still see the
    channels another run took meanwhile, not a busy snapshot from before it yielded."""
    real_log = RunContext.log

    async def slow_log(self, *a, **kw):
        await asyncio.sleep(0.01)
        return await real_log(self, *a, **kw)

    blocker = await _start(svc, mapping, [CH[0]])
    await _until(lambda: len(_of(sent, "backfill_run")) == 1)
    monkeypatch.setattr(RunContext, "log", slow_log)
    b = await _start(svc, mapping, [CH[0], CH[1]])
    await asyncio.sleep(0.015)
    a = await _start(svc, mapping, [CH[1]])
    await asyncio.sleep(0.2)
    owners = [r["job_id"] for r in _of(sent, "backfill_run") if r["original_channel_id"] == str(CH[1])]
    assert len(owners) == 1, owners
    for run in (blocker, a, b):
        if run.id in svc._active:
            await svc.cancel(run.id)
    for run in (blocker, a, b):
        await _settled(svc, run.id)


# 3

@pytest.mark.asyncio
async def test_cancel_one_run_leaves_the_other_running(svc, sent, mapping):
    a = await _start(svc, mapping, CH[:3])
    b = await _start(svc, mapping, CH[3:6])
    await _until(lambda: len(_of(sent, "backfill_run")) == 6)
    await _frame("backfill_job", job_id=f"{a.id}:{CH[0]}", state="running")
    await _frame("backfill_job_delivered", job_id=f"{a.id}:{CH[0]}", delivered=5, total=50)
    await _frame("backfill_job", job_id=f"{b.id}:{CH[3]}", state="running")
    await _frame("backfill_job_delivered", job_id=f"{b.id}:{CH[3]}", delivered=8, total=50)

    await svc.cancel(a.id)
    await _until(lambda: len(_of(sent, "backfill_cancel")) == 3)
    assert {c["job_id"] for c in _of(sent, "backfill_cancel")} == {f"{a.id}:{c}" for c in CH[:3]}
    await _frame("backfill_job_end", job_id=f"{a.id}:{CH[0]}", ok=False, error="cancelled", delivered=6)
    for c in CH[1:3]:
        await _frame("backfill_job_end", job_id=f"{a.id}:{c}", ok=False, error="cancelled")
    da = await _settled(svc, a.id)
    assert da.status == "cancelled"
    assert da.summary == {"success": 0, "failed": 0, "skipped": 0, "cancelled": 3, "delivered": 6}

    assert svc.get(b.id).status == "running"
    assert {s["state"] for s in svc.get(b.id).steps} <= {"queued", "running"}
    for c in CH[3:6]:
        await _finish(f"{b.id}:{c}", 8)
    db_ = await _settled(svc, b.id)
    assert db_.status == "success"
    assert db_.summary == {"success": 3, "failed": 0, "skipped": 0, "cancelled": 0, "delivered": 24}
    assert len(_of(sent, "backfill_cancel")) == 3


# 6

@pytest.mark.asyncio
async def test_bots_stopped_ends_both_active_runs(svc, sent, mapping, monkeypatch):
    monkeypatch.setattr(backfill, "CANCEL_WAIT", 30.0)
    loop = asyncio.get_running_loop()
    seen = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda lp, context: seen.append(context))
    try:
        a = await _start(svc, mapping, CH[:2])
        b = await _start(svc, mapping, CH[2:4])
        await _until(lambda: len(_of(sent, "backfill_run")) == 4)
        await _frame("backfill_job", job_id=f"{a.id}:{CH[0]}", state="running")
        await _frame("backfill_job_delivered", job_id=f"{b.id}:{CH[2]}", delivered=3, total=9)

        stopped = await svc.stop_all()
        assert {r.id for r in stopped} == {a.id, b.id}
        for run in (a, b):
            await _until(lambda: run.id not in svc._active, timeout=1.0)
            done = svc.get(run.id)
            assert done.status == "cancelled"
            assert {s["note"] for s in done.steps} == {"Bots stopped"}
            assert [c["note"] for c in done.report["channels"]] == ["Bots stopped"] * 2
        assert svc.get(b.id).summary["delivered"] == 3
        assert svc.get(a.id).summary["delivered"] == 0
        assert _of(sent, "backfill_cancel") == []
        assert backfill._jobs == {} and backfill._late == {}

        gc.collect()
        await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)
    assert seen == []


# 7

@pytest.mark.asyncio
async def test_deleting_a_finished_run_leaves_the_live_run_alone(svc, sent, mapping):
    old = await _start(svc, mapping, [CH[0]])
    await _until(lambda: len(_of(sent, "backfill_run")) == 1)
    await _finish(f"{old.id}:{CH[0]}", 2)
    await _settled(svc, old.id)

    live = await _start(svc, mapping, [CH[0], CH[1]])
    await _until(lambda: len(_of(sent, "backfill_run")) == 3)
    await _frame("backfill_job_delivered", job_id=f"{live.id}:{CH[0]}", delivered=4, total=8)
    svc.repo.delete_run(old.id)
    assert svc.get(old.id) is None
    # A stray frame for the deleted run's job is ignored.
    assert await _frame("backfill_job_end", job_id=f"{old.id}:{CH[0]}", ok=True, delivered=99) is False

    assert set(backfill.busy_channels()) == set(_ids(CH[:2]))
    await _finish(f"{live.id}:{CH[0]}", 8)
    await _finish(f"{live.id}:{CH[1]}", 1)
    done = await _settled(svc, live.id)
    assert done.status == "success" and done.summary["delivered"] == 9
    assert svc.repo.get_run(live.id).status == "success"
    nxt = await _start(svc, mapping, [CH[2]])
    assert nxt.number not in (live.number,)
    await svc.cancel(nxt.id)
    await _settled(svc, nxt.id)


# 4: the client's one queue across runs

class _FakeWs:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)

    def of(self, typ):
        return [p["data"] for p in self.sent if p.get("type") == typ]


class _GateEngine:
    """run_channel stand-in: each channel waits for its own gate."""

    def __init__(self):
        self.gates = {}
        self.calls = []

    async def run_channel(self, chan_id, **params):
        self.calls.append(chan_id)
        await self.gates.setdefault(chan_id, asyncio.Event()).wait()


def _listener(engine):
    self = cc.ClientListener.__new__(cc.ClientListener)
    self.ws = _FakeWs()
    self.backfill = engine
    self._bf_queue = asyncio.Queue()
    self._bf_active = set()
    self._bf_active_meta = {}
    self._bf_queued = set()
    self._bf_worker_task = None
    self._bf_waiters = {}
    self._bf_jobs = {}
    return self


def _run(job_id, chan):
    return {"job_id": job_id, "original_channel_id": str(chan),
            "range": {"mode": "all", "since": None, "until": None, "last_n": None},
            "exclude_ids": [], "checkpoint": None, "ignore_cloned": False}


@pytest.mark.asyncio
async def test_client_queue_runs_every_job_of_two_runs(monkeypatch):
    eng = _GateEngine()
    cl = _listener(eng)
    jobs = [(f"1:{c}", c) for c in CH[:3]] + [(f"2:{c}", c) for c in CH[3:6]]
    for job_id, chan in jobs:
        res = await cl._on_ws({"type": "backfill_run", "data": _run(job_id, chan)})
        assert res["ok"] is True and res.get("note") is None
    queued = {d["job_id"]: d["queue_position"] for d in cl.ws.of("backfill_job") if d["state"] == "queued"}
    assert set(queued) == {j for j, _ in jobs}
    assert all(isinstance(p, int) and p >= 1 for p in queued.values())

    for _, chan in jobs:
        await _until(lambda: eng.calls and eng.calls[-1] == chan)
        eng.gates[chan].set()
    await asyncio.wait_for(cl._bf_queue.join(), 2)
    cl._bf_worker_task.cancel()

    ends = cl.ws.of("backfill_job_end")
    assert [e["job_id"] for e in ends] == [j for j, _ in jobs]
    assert all(e["ok"] is True and e["error"] is None for e in ends)
    assert eng.calls == list(CH)
    for p in cl.ws.sent:
        if p["type"].startswith("backfill_job"):
            assert p["data"]["job_id"] in dict(jobs)
    assert cl._bf_jobs == {}


@pytest.mark.asyncio
async def test_client_cancel_drops_only_that_runs_jobs():
    eng = _GateEngine()
    cl = _listener(eng)
    await cl._on_ws({"type": "backfill_run", "data": _run(f"1:{CH[0]}", CH[0])})
    await cl._on_ws({"type": "backfill_run", "data": _run(f"2:{CH[1]}", CH[1])})
    await cl._on_ws({"type": "backfill_run", "data": _run(f"1:{CH[2]}", CH[2])})
    await _until(lambda: eng.calls == [CH[0]])
    await cl._on_ws({"type": "backfill_cancel", "data": {"job_id": f"1:{CH[2]}"}})
    ends = cl.ws.of("backfill_job_end")
    assert [(e["job_id"], e["error"]) for e in ends] == [(f"1:{CH[2]}", "cancelled")]
    eng.gates[CH[0]].set()
    await _until(lambda: eng.calls == [CH[0], CH[1]])
    eng.gates[CH[1]].set()
    await asyncio.wait_for(cl._bf_queue.join(), 2)
    cl._bf_worker_task.cancel()
    ends = {e["job_id"]: e for e in cl.ws.of("backfill_job_end")}
    assert ends[f"2:{CH[1]}"]["ok"] is True and ends[f"1:{CH[0]}"]["ok"] is True


# 5: the server's per clone state

CLONE = [5555555555555555551, 5555555555555555552]
ORIG = [CH[0], CH[1]]


class _Db:
    def __init__(self):
        self.done = []

    def backfill_mark_done(self, run_id):
        self.done.append(run_id)


def _manager():
    rx = SimpleNamespace(_shutting_down=False, db=_Db(), ratelimit=None)

    async def _flush(cid):
        return None

    rx._flush_channel_buffer = _flush
    rx._clear_bf_throttle = lambda clone: None
    mgr = BackfillManager(rx)
    deleted = []

    async def delete_created_temps_for(clone, created_ids=None):
        deleted.append((clone, list(created_ids or [])))
        return {"deleted": len(created_ids or [])}

    mgr.delete_created_temps_for = delete_created_temps_for
    for i, (orig, clone) in enumerate(zip(ORIG, CLONE)):
        mgr._progress[orig] = {"task_id": f"{i + 1}:{orig}", "run_id": f"{i + 1}:{orig}",
                               "clone_channel_id": clone, "delivered": 0,
                               "temp_created_ids": [900 + i]}
        mgr._flags.add(orig)
        mgr._by_clone[clone] = orig
        mgr.semaphores[clone] = asyncio.Semaphore(1)
        mgr._rot_locks[clone] = asyncio.Lock()
        mgr._temp_locks[clone] = asyncio.Lock()
        mgr._rotate_pool[clone] = [f"https://hook/{clone}"]
    return mgr, deleted


@pytest.mark.asyncio
async def test_one_runs_cleanup_keeps_the_other_clones_locks_and_webhooks():
    mgr, deleted = _manager()
    # Channel B still has a send in flight; channel A's drain must not wait on it.
    stuck = asyncio.Event()
    b_send = asyncio.ensure_future(stuck.wait())
    mgr.attach_task(ORIG[1], b_send)
    sem_b, rot_b, temp_b = mgr.semaphores[CLONE[1]], mgr._rot_locks[CLONE[1]], mgr._temp_locks[CLONE[1]]
    async with sem_b:
        await asyncio.wait_for(mgr.on_done(ORIG[0], wait_cleanup=True), 2)
        assert deleted == [(CLONE[0], [900])]
        assert CLONE[0] not in mgr.semaphores and CLONE[0] not in mgr._by_clone
        assert mgr.semaphores[CLONE[1]] is sem_b and mgr._rot_locks[CLONE[1]] is rot_b
        assert mgr._temp_locks[CLONE[1]] is temp_b
        assert mgr._rotate_pool[CLONE[1]] == [f"https://hook/{CLONE[1]}"]
        assert mgr._by_clone[CLONE[1]] == ORIG[1] and mgr.is_backfilling(ORIG[1])
        assert ORIG[1] in mgr._progress
    assert not b_send.done()
    stuck.set()
    await b_send
    await asyncio.wait_for(mgr.on_done(ORIG[1], wait_cleanup=True), 2)
    assert deleted == [(CLONE[0], [900]), (CLONE[1], [901])]
    assert mgr.semaphores == {} and mgr._by_clone == {} and mgr._progress == {}


@pytest.mark.asyncio
async def test_two_clones_share_the_global_lock_without_deadlock():
    mgr, _ = _manager()
    mgr._global_sync = {"original_id": ORIG[0]}

    async def deliver(orig, clone):
        sem = mgr.semaphores[clone]
        for _ in range(20):
            async with sem:
                async with mgr._global_lock:
                    await asyncio.sleep(0)
            await mgr.end_global_sync(orig)

    await asyncio.wait_for(asyncio.gather(deliver(ORIG[0], CLONE[0]), deliver(ORIG[1], CLONE[1]),
                                          mgr.on_done(ORIG[0], wait_cleanup=True),
                                          mgr.on_done(ORIG[1], wait_cleanup=True)), 3)
    assert mgr._global_sync is None
