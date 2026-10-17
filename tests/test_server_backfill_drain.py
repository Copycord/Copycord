"""Backfill message tasks are tracked per channel and drained before the end.

The server hands every __backfill__ message (channel or thread) to a task and
attaches it to the channel's BackfillManager; on_done waits for those tasks,
so backfill_job_end carries the messages the server actually wrote instead of
whatever had landed when backfill_stream_end arrived.
"""
import asyncio
import importlib
import json
import logging
import re
from types import SimpleNamespace

import pytest

from copycord.server.backfill import BackfillManager

CID = 1521898853337403511
JOB = "run-1:1521898853337403511"


@pytest.fixture
def ServerReceiver():
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    levels = {n: lg.level for n, lg in logging.root.manager.loggerDict.items() if isinstance(lg, logging.Logger)}
    try:
        yield importlib.import_module("copycord.server.server").ServerReceiver
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        for n, lg in list(logging.root.manager.loggerDict.items()):
            if isinstance(lg, logging.Logger):
                lg.setLevel(levels.get(n, logging.NOTSET))


class FakeBus:
    def __init__(self):
        self.sent = []

    async def publish(self, kind, payload):
        self.sent.append(json.loads(json.dumps({"kind": kind, "payload": payload})))


class FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, frame):
        self.sent.append(frame)


class FakeDB:
    def __init__(self):
        self.done = []

    def backfill_mark_done(self, run_id):
        self.done.append(run_id)

    def backfill_update_checkpoint(self, run_id, **kw):
        pass


def _rx(ServerReceiver, gate):
    rx = ServerReceiver.__new__(ServerReceiver)
    rx._shutting_down = False
    rx._resolve_guild_label = lambda typ, data: None
    rx._task_for_channel = {}
    rx._active_backfills = {CID}
    rx._bf_jobs = {}
    rx._bf_job_by_channel = {}
    rx._bf_event_buffer = {}
    rx._send_tasks = set()
    rx.bus = FakeBus()
    rx.db = FakeDB()
    rx.bot = SimpleNamespace(ws_manager=FakeWS())
    rx.ratelimit = None

    async def _flush(cid):
        return None

    rx._flush_channel_buffer = _flush

    async def _deliver(channel_id, mid):
        await gate.wait()
        rx.backfill.note_sent(channel_id, mid)

    async def _bf_message(data):
        await _deliver(int(data["channel_id"]), int(data["message_id"]))

    async def _bf_thread_message(data):
        await _deliver(int(data["thread_parent_id"]), int(data["message_id"]))

    rx._handle_backfill_message = _bf_message
    rx._handle_backfill_thread_message = _bf_thread_message

    rx.backfill = BackfillManager(rx)
    rx.backfill.register_sink(CID, user_id=None, clone_channel_id=None)
    st = rx.backfill._progress[CID]
    st["task_id"] = st["run_id"] = JOB
    rx.backfill._flags.add(CID)
    return rx


def _thread_frame(mid):
    return {"type": "thread_message", "data": {"__backfill__": True, "thread_parent_id": str(CID),
                                               "thread_id": "1521898853337400001", "message_id": str(mid)}}


def _message_frame(mid):
    return {"type": "message", "data": {"__backfill__": True, "channel_id": str(CID), "message_id": str(mid)}}


@pytest.mark.parametrize("frame", [_thread_frame, _message_frame], ids=["thread_message", "message"])
def test_backfill_message_task_is_attached_and_drained(ServerReceiver, frame):
    async def go():
        gate = asyncio.Event()
        rx = _rx(ServerReceiver, gate)
        bf = rx.backfill

        await rx._on_ws(frame(1521898853337409001))

        assert bf._inflight[CID] == 1
        (task,) = bf._attached[CID]
        assert not task.done()

        done = asyncio.create_task(bf.on_done(CID, wait_cleanup=True, expected_task_id=JOB))
        await asyncio.sleep(0.1)
        assert not done.done(), "on_done must wait for the attached message task"

        gate.set()
        written = await asyncio.wait_for(done, 2.0)
        assert written == 1
        assert task.done() and not bf._attached.get(CID)
        assert bf._inflight.get(CID, 0) == 0
        assert rx.db.done == [JOB]

    asyncio.run(go())


def test_job_end_carries_messages_written_after_drain(ServerReceiver):
    async def go():
        gate = asyncio.Event()
        rx = _rx(ServerReceiver, gate)

        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        for i in range(3):
            await rx._on_ws(_message_frame(1521898853337409001 + i))
        await rx._on_ws(_thread_frame(1521898853337409010))
        await rx._on_ws({"type": "backfill_job_progress", "data": {"job_id": JOB, "pulled": 4, "total_estimate": None}})

        stream_end = asyncio.create_task(rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}}))
        job_end = asyncio.create_task(rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": True, "error": None, "last_orig_message_id": None}}))
        await asyncio.sleep(0.1)
        assert "backfill_job_end" not in [f["payload"]["type"] for f in rx.bus.sent]

        gate.set()
        await asyncio.wait_for(asyncio.gather(stream_end, job_end), 2.0)
        return rx

    rx = asyncio.run(go())

    end = rx.bus.sent[-1]["payload"]
    assert end["type"] == "backfill_job_end"
    assert end["data"]["delivered"] == 4
    assert end["data"]["ok"] is True


def test_cancel_all_active_cancels_tracked_tasks(ServerReceiver):
    async def go():
        rx = _rx(ServerReceiver, asyncio.Event())
        await rx._on_ws(_thread_frame(1521898853337409001))
        (task,) = rx.backfill._attached[CID]
        await rx.backfill.cancel_all_active()
        assert task.cancelled()
        assert not rx.backfill._attached and not rx.backfill._inflight

    asyncio.run(go())


def _types(rx):
    return [f["payload"]["type"] for f in rx.bus.sent]


def test_cleanup_starts_after_delivery_and_reports_the_count_first(ServerReceiver):
    """The client has pulled everything; the server still forwards. Cleaning up waits for the last send."""

    async def go():
        gate = asyncio.Event()
        rx = _rx(ServerReceiver, gate)
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        for i in range(3):
            await rx._on_ws(_message_frame(1521898853337409001 + i))
        await rx._on_ws(_thread_frame(1521898853337409010))

        stream_end = asyncio.create_task(rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}}))
        await asyncio.sleep(0.1)
        assert "backfill_job_cleanup" not in _types(rx), "still delivering"

        gate.set()
        await asyncio.wait_for(stream_end, 2.0)
        return rx

    rx = asyncio.run(go())

    types = _types(rx)
    cleanup = types.index("backfill_job_cleanup")
    delivered = [f["payload"]["data"] for f in rx.bus.sent[:cleanup] if f["payload"]["type"] == "backfill_job_delivered"]
    assert delivered[-1] == {"job_id": JOB, "original_channel_id": str(CID), "delivered": 4, "total": None}
    assert "backfill_job_delivered" not in types[cleanup:]


def test_delivered_frames_are_throttled_with_a_trailing_publish(ServerReceiver):
    async def go():
        rx = _rx(ServerReceiver, asyncio.Event())
        rx.BF_PROGRESS_INTERVAL = 0.05
        rx.backfill.update_expected_total(CID, 35)
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        for i in range(10):
            rx.backfill.note_sent(CID, 1521898853337409001 + i)
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.1)
        return rx

    rx = asyncio.run(go())

    frames = [f["payload"]["data"] for f in rx.bus.sent if f["payload"]["type"] == "backfill_job_delivered"]
    counts = [f["delivered"] for f in frames]
    assert 2 <= len(frames) < 10
    assert counts == sorted(counts) and counts[-1] == 10
    assert all(f["total"] == 35 for f in frames)


def test_resumed_job_counts_from_its_own_start(ServerReceiver):
    async def go():
        rx = _rx(ServerReceiver, asyncio.Event())
        st = rx.backfill._progress[CID]
        st["delivered"], st["delivered_base"], st["expected_total"] = 100, 100, 135
        for i in range(5):
            rx.backfill.note_sent(CID, 1521898853337409001 + i)
        return rx.backfill.job_counts(CID)

    assert asyncio.run(go()) == (5, 35)


def test_cancel_while_delivering_stops_the_queued_forwards(ServerReceiver):
    """The client already ended its pull (stream end and job end are in); the server is still
    forwarding when the cancel comes. The send in flight finishes, the queued ones never go out,
    and the job ends cancelled with the one delivered message and nothing left behind."""

    async def go():
        release = asyncio.Event()
        rx = _rx(ServerReceiver, asyncio.Event())
        rx._bf_send_gate_by_source = {}
        sent = []

        async def _inner(msg):
            await release.wait()
            sent.append(int(msg["message_id"]))
            rx.backfill.note_sent(int(msg["channel_id"]), int(msg["message_id"]))

        async def _bf_message(data):
            await rx.forward_message(data)

        rx._forward_message_inner = _inner
        rx._handle_backfill_message = _bf_message
        bf = rx.backfill

        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        ids = [1521898853337409001 + i for i in range(5)]
        for mid in ids:
            await rx._on_ws(_message_frame(mid))
        stream_end = asyncio.create_task(rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}}))
        job_end = asyncio.create_task(rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": True, "error": None, "last_orig_message_id": str(ids[-1])}}))
        await asyncio.sleep(0.05)
        assert "backfill_job_end" not in _types(rx)

        res = await rx._on_ws({"type": "backfill_job_cancel", "data": {"job_id": JOB}})
        assert res == {"ok": True, "job_id": JOB, "state": "cancelling"}
        attached = bf._attach_count[CID]
        await rx._on_ws(_message_frame(1521898853337409100))
        assert bf._attach_count[CID] == attached, "a message after the cancel is dropped"

        release.set()
        await asyncio.wait_for(asyncio.gather(stream_end, job_end), 2.0)
        return rx, sent, ids

    rx, sent, ids = asyncio.run(go())

    assert sent == ids[:1]
    types = _types(rx)
    assert types.index("backfill_job_cleanup") < types.index("backfill_job_end")
    end = rx.bus.sent[-1]["payload"]["data"]
    assert end["ok"] is False and end["error"] == "cancelled" and end["delivered"] == 1
    assert rx._bf_jobs == {} and rx._bf_job_by_channel == {}
    assert CID not in rx._active_backfills
    assert not rx.backfill._attached.get(CID) and rx.backfill._inflight.get(CID, 0) == 0
    assert CID not in rx.backfill._progress
    assert [f["type"] for f in rx.bot.ws_manager.sent] == ["backfill_done"]


def test_an_explicit_hard_cap_still_cuts_the_wait_by_the_clock(ServerReceiver, monkeypatch):
    """BACKFILL_DRAIN_HARD_CAP_SECS overrides the stall window: sends still going out are cancelled
    when it runs out, and the end says how many of the job's messages went out."""
    monkeypatch.setenv("BACKFILL_DRAIN_HARD_CAP_SECS", "0.2")
    monkeypatch.setenv("BACKFILL_DRAIN_NO_PROGRESS_SECS", "5")

    async def go():
        rx = _rx(ServerReceiver, asyncio.Event())
        pace = asyncio.Lock()

        async def _bf_message(data):
            async with pace:
                await asyncio.sleep(0.05)
                rx.backfill.note_sent(int(data["channel_id"]), int(data["message_id"]))

        rx._handle_backfill_message = _bf_message
        rx.backfill.update_expected_total(CID, 8)
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        for i in range(8):
            await rx._on_ws(_message_frame(1521898853337409001 + i))
        stream_end = asyncio.create_task(rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}}))
        job_end = asyncio.create_task(rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": True, "error": None, "last_orig_message_id": None}}))
        await asyncio.wait_for(asyncio.gather(stream_end, job_end), 3.0)
        return rx

    rx = asyncio.run(go())

    end = rx.bus.sent[-1]["payload"]["data"]
    assert rx._bf_jobs == {} and not rx.backfill._attached.get(CID)
    m = re.fullmatch(r"delivery timed out after 0 s, (\d) of 8 delivered", end["error"])
    assert m, end["error"]
    assert end["ok"] is False and 0 < end["delivered"] == int(m.group(1)) < 8


def test_sends_stuck_for_the_stall_window_are_cancelled_with_the_counts(ServerReceiver, monkeypatch):
    """No hard cap: two sends go out, then the next two hang. Once no send finished for the stall
    window the hung ones are cancelled and the job ends with what was delivered of what was expected."""
    monkeypatch.delenv("BACKFILL_DRAIN_HARD_CAP_SECS", raising=False)
    monkeypatch.setenv("BACKFILL_DRAIN_NO_PROGRESS_SECS", "0.2")
    hung = []

    async def go():
        rx = _rx(ServerReceiver, asyncio.Event())

        async def _bf_message(data):
            mid = int(data["message_id"])
            if mid >= 1521898853337409003:
                hung.append(asyncio.current_task())
                await asyncio.Event().wait()
            rx.backfill.note_sent(int(data["channel_id"]), mid)

        rx._handle_backfill_message = _bf_message
        rx.backfill.update_expected_total(CID, 6)
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        for i in range(4):
            await rx._on_ws(_message_frame(1521898853337409001 + i))
        stream_end = asyncio.create_task(rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}}))
        job_end = asyncio.create_task(rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": True, "error": None, "last_orig_message_id": None}}))
        await asyncio.wait_for(asyncio.gather(stream_end, job_end), 3.0)
        return rx

    rx = asyncio.run(go())

    end = rx.bus.sent[-1]["payload"]
    assert end["type"] == "backfill_job_end"
    assert end["data"]["ok"] is False
    assert end["data"]["error"] == "delivery timed out after 0 s, 2 of 6 delivered"
    assert end["data"]["delivered"] == 2
    assert len(hung) == 2 and all(t.cancelled() for t in hung)
    assert rx._bf_jobs == {} and not rx.backfill._attached.get(CID)
    assert CID not in rx.backfill._drain_cut


def test_slow_sends_that_keep_completing_are_not_cut_off(ServerReceiver, monkeypatch):
    """Discord limits shared by several channels can slow sends below a fixed per-message budget.
    Scaled down: each send takes longer than its share of such a cap, yet one finishes well inside
    the stall window, so the drain waits for all of them."""
    monkeypatch.delenv("BACKFILL_DRAIN_HARD_CAP_SECS", raising=False)
    monkeypatch.setenv("BACKFILL_DRAIN_NO_PROGRESS_SECS", "0.3")

    async def go():
        rx = _rx(ServerReceiver, asyncio.Event())
        # A per-message cap, scaled down, that would cut these sends off; the drain must not read it.
        rx.backfill.DRAIN_CAP_FLOOR_SECS = 0.2
        rx.backfill.DRAIN_CAP_PER_MESSAGE_SECS = 0.01
        pace = asyncio.Lock()

        async def _bf_message(data):
            async with pace:
                await asyncio.sleep(0.05)
                rx.backfill.note_sent(int(data["channel_id"]), int(data["message_id"]))

        rx._handle_backfill_message = _bf_message
        rx.backfill.update_expected_total(CID, 8)
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        for i in range(8):
            await rx._on_ws(_message_frame(1521898853337409001 + i))
        stream_end = asyncio.create_task(rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}}))
        job_end = asyncio.create_task(rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": True, "error": None, "last_orig_message_id": None}}))
        await asyncio.wait_for(asyncio.gather(stream_end, job_end), 3.0)
        return rx

    rx = asyncio.run(go())

    end = rx.bus.sent[-1]["payload"]
    assert end["type"] == "backfill_job_end"
    assert end["data"]["error"] is None and end["data"]["ok"] is True
    assert end["data"]["delivered"] == 8


def test_cancel_after_every_send_went_out_ends_ok(ServerReceiver):
    """The cancel arrives once the server sent all it was given: nothing was cut, the job is ok."""

    async def go():
        gate = asyncio.Event()
        gate.set()
        rx = _rx(ServerReceiver, gate)
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        for i in range(3):
            await rx._on_ws(_message_frame(1521898853337409001 + i))
        await rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}})
        await rx._on_ws({"type": "backfill_job_cancel", "data": {"job_id": JOB}})
        await rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": False, "error": "cancelled", "last_orig_message_id": None}})
        return rx

    rx = asyncio.run(go())

    end = rx.bus.sent[-1]["payload"]["data"]
    assert end["ok"] is True and end["error"] is None and end["delivered"] == 3


def test_cancel_mid_pull_still_ends_cancelled(ServerReceiver):
    """The client cut its pull short (no stream end): the job stays cancelled though no send was skipped."""

    async def go():
        gate = asyncio.Event()
        gate.set()
        rx = _rx(ServerReceiver, gate)
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        await rx._on_ws(_message_frame(1521898853337409001))
        await rx._on_ws({"type": "backfill_job_cancel", "data": {"job_id": JOB}})
        await rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": False, "error": "cancelled", "last_orig_message_id": None}})
        return rx

    rx = asyncio.run(go())

    end = rx.bus.sent[-1]["payload"]["data"]
    assert end["ok"] is False and end["error"] == "cancelled" and end["delivered"] == 1
