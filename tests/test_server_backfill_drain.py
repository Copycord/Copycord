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
