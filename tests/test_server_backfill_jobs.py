"""Server side of the backfill job contract.

backfill_job and backfill_job_progress reach the bus unchanged; backfill_job_end
is published only after the server's cleanup for that job, bracketed by
backfill_job_cleanup, and carries the count of messages the server wrote.
Every id stays a string, so a 19 digit snowflake survives JSON in the browser.
"""
import asyncio
import importlib
import json
import logging

import pytest

CID = 1521898853337403511
JOB = "job-7"


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


class FakeBackfill:
    """No message tasks: every channel is already delivered."""

    def __init__(self):
        self.waited = []

    async def wait_delivered(self, orig):
        self.waited.append(orig)
        return True

    def job_counts(self, orig):
        return None, None


def _rx(ServerReceiver, finish):
    rx = ServerReceiver.__new__(ServerReceiver)
    rx._shutting_down = False
    rx._resolve_guild_label = lambda typ, data: None
    rx._task_for_channel = {}
    rx._active_backfills = {CID}
    rx._bf_jobs = {}
    rx._bf_job_by_channel = {}
    rx.bus = FakeBus()
    rx.backfill = FakeBackfill()
    rx._bf_finish_channel = finish
    return rx


def _types(rx):
    return [f["payload"]["type"] for f in rx.bus.sent]


def test_job_frames_relay_and_end_after_cleanup(ServerReceiver):
    order = []

    async def finish(orig):
        order.append(("cleanup", orig))
        rx._active_backfills.discard(orig)
        return 42

    rx = _rx(ServerReceiver, finish)

    async def go():
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "queued", "queue_position": 2}})
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        await rx._on_ws({"type": "backfill_job_progress", "data": {"job_id": JOB, "pulled": 10, "total_estimate": 50}})
        await rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}})
        await rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": True, "error": None, "last_orig_message_id": "1521898853337403999"}})

    asyncio.run(go())

    assert order == [("cleanup", CID)]
    assert _types(rx) == ["backfill_job", "backfill_job", "backfill_job_progress", "backfill_job_cleanup", "backfill_job_end"]
    assert all(f["kind"] == "client" for f in rx.bus.sent)
    assert rx.bus.sent[0]["payload"]["data"] == {"job_id": JOB, "original_channel_id": str(CID), "state": "queued", "queue_position": 2}
    assert rx.bus.sent[2]["payload"]["data"] == {"job_id": JOB, "pulled": 10, "total_estimate": 50}
    assert rx.bus.sent[3]["payload"]["data"] == {"job_id": JOB, "original_channel_id": str(CID), "state": "cleaning"}
    assert rx.bus.sent[4]["payload"]["data"] == {
        "job_id": JOB,
        "original_channel_id": str(CID),
        "ok": True,
        "error": None,
        "delivered": 42,
        "last_orig_message_id": "1521898853337403999",
    }
    assert rx._bf_jobs == {} and rx._bf_job_by_channel == {}


def test_job_end_without_stream_end_runs_cleanup_first(ServerReceiver):
    async def finish(orig):
        rx._active_backfills.discard(orig)
        return 3

    rx = _rx(ServerReceiver, finish)

    async def go():
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        await rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": False, "error": "cancelled", "last_orig_message_id": None}})

    asyncio.run(go())

    assert _types(rx) == ["backfill_job", "backfill_job_cleanup", "backfill_job_end"]
    end = rx.bus.sent[-1]["payload"]["data"]
    assert end["ok"] is False and end["error"] == "cancelled" and end["delivered"] == 3
    assert end["original_channel_id"] == str(CID)


def test_queued_job_cancelled_ends_without_cleanup(ServerReceiver):
    async def finish(orig):
        raise AssertionError("no cleanup for a job that never ran")

    rx = _rx(ServerReceiver, finish)
    rx._active_backfills = set()

    async def go():
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "queued", "queue_position": 1}})
        await rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": False, "error": "cancelled", "last_orig_message_id": None}})

    asyncio.run(go())

    assert _types(rx) == ["backfill_job", "backfill_job_end"]
    assert rx.bus.sent[-1]["payload"]["data"]["delivered"] == 0


def test_end_waits_for_cleanup_running_concurrently(ServerReceiver):
    gate = None

    async def finish(orig):
        await gate.wait()
        rx._active_backfills.discard(orig)
        return 5

    rx = _rx(ServerReceiver, finish)

    async def go():
        nonlocal gate
        gate = asyncio.Event()
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": JOB, "original_channel_id": str(CID), "state": "running", "queue_position": None}})
        stream_end = asyncio.create_task(rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}}))
        await asyncio.sleep(0)
        job_end = asyncio.create_task(rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": True, "error": None, "last_orig_message_id": None}}))
        await asyncio.sleep(0.01)
        assert "backfill_job_end" not in _types(rx)
        gate.set()
        await asyncio.gather(stream_end, job_end)

    asyncio.run(go())

    assert _types(rx) == ["backfill_job", "backfill_job_cleanup", "backfill_job_end"]
    assert rx.bus.sent[-1]["payload"]["data"]["delivered"] == 5


def test_snowflake_round_trips_as_string(ServerReceiver):
    async def finish(orig):
        return 0

    rx = _rx(ServerReceiver, finish)
    big = "9223372036854775807"

    async def go():
        await rx._on_ws({"type": "backfill_job", "data": {"job_id": "j", "original_channel_id": big, "state": "running", "queue_position": None}})
        await rx._on_ws({"type": "backfill_job_end", "data": {"job_id": "j", "ok": True, "error": None, "last_orig_message_id": big}})

    asyncio.run(go())

    for f in rx.bus.sent:
        d = f["payload"]["data"]
        if "original_channel_id" in d:
            assert d["original_channel_id"] == big
    assert rx.bus.sent[-1]["payload"]["data"]["last_orig_message_id"] == big


def test_webhook_pool_lives_in_its_own_module():
    from copycord.server.backfill import BackfillManager
    from copycord.server.backfill_webhooks import BackfillWebhookPool

    assert issubclass(BackfillManager, BackfillWebhookPool)
    for name in ("_ensure_temp_webhooks", "pick_url_for_send", "delete_created_temps_for", "cleanup_non_primary_webhooks", "_api_with_retry"):
        assert name in vars(BackfillWebhookPool) and name not in vars(BackfillManager)


def _running(job=JOB, cid=CID):
    return {"type": "backfill_job", "data": {"job_id": job, "original_channel_id": str(cid), "state": "running", "queue_position": None}}


def test_cleanup_failure_still_ends_the_job_with_the_counted_delivered(ServerReceiver):
    async def finish(orig):
        rx._active_backfills.discard(orig)
        raise RuntimeError("webhook cleanup blew up")

    rx = _rx(ServerReceiver, finish)
    rx.backfill.job_counts = lambda orig: (7, 10)

    async def go():
        await rx._on_ws(_running())
        await rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}})
        await asyncio.wait_for(rx._on_ws({"type": "backfill_job_end", "data": {"job_id": JOB, "ok": True, "error": None, "last_orig_message_id": None}}), 2.0)

    asyncio.run(go())

    end = rx.bus.sent[-1]["payload"]
    assert end["type"] == "backfill_job_end"
    assert end["data"]["ok"] is False and end["data"]["error"] == "cleanup failed"
    assert end["data"]["delivered"] == 7
    assert rx._bf_jobs == {} and rx._bf_job_by_channel == {}


async def _finish_ok(rx, orig):
    rx._active_backfills.discard(orig)
    return 4


def test_job_ends_when_the_client_never_sends_its_end(ServerReceiver):
    rx = _rx(ServerReceiver, lambda orig: _finish_ok(rx, orig))
    rx.BF_JOB_END_GRACE = 0.05

    async def go():
        await rx._on_ws(_running())
        await rx._on_ws({"type": "backfill_job_cancel", "data": {"job_id": JOB}})
        await rx._on_ws({"type": "backfill_stream_end", "data": {"channel_id": str(CID)}})
        await asyncio.sleep(0.2)

    asyncio.run(go())

    ends = [f["payload"]["data"] for f in rx.bus.sent if f["payload"]["type"] == "backfill_job_end"]
    assert len(ends) == 1
    assert ends[0]["error"] == "no end from client" and ends[0]["delivered"] == 4
    assert rx._bf_jobs == {} and rx._bf_job_by_channel == {}
    assert not rx._bf_job_cancelled(CID)


def test_client_reset_ends_the_jobs_it_left_open(ServerReceiver):
    rx = _rx(ServerReceiver, lambda orig: _finish_ok(rx, orig))

    async def go():
        await rx._on_ws(_running())
        await rx._on_ws({"type": "backfill_job_cancel", "data": {"job_id": JOB}})
        await rx._on_ws({"type": "backfill_jobs_reset", "data": {}})

    asyncio.run(go())

    ends = [f["payload"]["data"] for f in rx.bus.sent if f["payload"]["type"] == "backfill_job_end"]
    assert [(e["job_id"], e["ok"]) for e in ends] == [(JOB, False)]
    assert rx._bf_jobs == {} and rx._bf_job_by_channel == {}
    assert not rx._bf_job_cancelled(CID)


def test_stale_jobs_are_swept_when_a_new_job_starts(ServerReceiver):
    rx = _rx(ServerReceiver, lambda orig: _finish_ok(rx, orig))
    rx._send_tasks = set()
    other = CID + 1

    async def go():
        await rx._on_ws(_running("old", CID))
        await rx._on_ws({"type": "backfill_job_cancel", "data": {"job_id": "old"}})
        rx._bf_jobs["old"]["seen"] -= rx.BF_JOB_STALE_SECS + 1
        await rx._on_ws(_running("new", other))
        await asyncio.sleep(0.05)

    asyncio.run(go())

    ends = [f["payload"]["data"] for f in rx.bus.sent if f["payload"]["type"] == "backfill_job_end"]
    assert [(e["job_id"], e["error"]) for e in ends] == [("old", "stale")]
    assert set(rx._bf_jobs) == {"new"} and rx._bf_job_by_channel == {other: "new"}
    assert not rx._bf_job_cancelled(CID)
