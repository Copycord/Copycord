"""Client backfill contract: backfill_run / backfill_cancel in, backfill_job / backfill_job_progress / backfill_job_end out."""

import asyncio
import json
import logging
import types

import pytest

import copycord.client.client as cc
from copycord.client.export_runners import (
    _CURRENT_JOB,
    BackfillEngine,
    BackfillJob,
)

BIG = "1234567890123456789"


class _FakeWs:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(json.loads(json.dumps(payload)))

    def of(self, typ):
        return [p["data"] for p in self.sent if p.get("type") == typ]


class _PagedBot:
    """bot.history stub: each call returns the next page of fake messages."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = 0

    async def history(self, obj, raise_on_error=True, **kw):
        self.calls += 1
        page = self.pages.pop(0) if self.pages else []
        for mid in page:
            yield types.SimpleNamespace(id=mid)


def _engine(bot):
    eng = BackfillEngine.__new__(BackfillEngine)
    eng.bot = bot
    eng.logger = logging.getLogger("test.backfill")
    eng.PAGE_DELAY = 0
    return eng


class _StubEngine:
    """run_channel stand-in that reads pages through the real resumable iterator."""

    def __init__(self, pages, gate=None):
        self.eng = _engine(_PagedBot(pages))
        self.gate = gate
        self.calls = []

    async def run_channel(self, chan_id, **params):
        self.calls.append((chan_id, params))
        job = _CURRENT_JOB.get()
        sent = 0
        async for m in self.eng._iter_history_resumable(
            object(), n=None, after_dt=None, before_dt=None, oldest_first=True
        ):
            sent += 1
            await job.progress(pulled=sent, last_id=m.id)
            if self.gate is not None:
                await self.gate.wait()
        await job.progress(pulled=sent, force=True)


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


def _run(job_id="job-1", chan=BIG, **extra):
    return {
        "job_id": job_id,
        "original_channel_id": chan,
        "range": {"mode": "all", "since": None, "until": None, "last_n": None},
        "exclude_ids": [],
        "checkpoint": None,
        "ignore_cloned": False,
        **extra,
    }


async def _drain(listener):
    await asyncio.wait_for(listener._bf_queue.join(), 2)
    listener._bf_worker_task.cancel()


@pytest.mark.asyncio
async def test_backfill_run_queues_runs_and_ends_ok_with_string_ids():
    eng = _StubEngine([[int(BIG) + 1, int(BIG) + 2], []])
    cl = _listener(eng)
    res = await cl._on_ws({"type": "backfill_run", "data": _run()})
    assert res["ok"] is True and res["position"] == 1
    await _drain(cl)

    types_seen = [p["type"] for p in cl.ws.sent]
    assert types_seen[0] == "backfill_job"
    assert cl.ws.of("backfill_job") == [
        {"job_id": "job-1", "original_channel_id": BIG, "state": "queued", "queue_position": 1},
        {"job_id": "job-1", "original_channel_id": BIG, "state": "running", "queue_position": 0},
    ]
    assert cl.ws.of("backfill_job_end") == [
        {"job_id": "job-1", "ok": True, "error": None, "last_orig_message_id": str(int(BIG) + 2)}
    ]
    assert types_seen[-1] == "backfill_job_end"
    assert cl.ws.of("backfill_job_progress")[-1]["pulled"] == 2
    assert cl.ws.of("backfill_stream_end") == [{"channel_id": BIG}]
    assert eng.calls[0][0] == int(BIG)
    assert eng.calls[0][1]["resume"] is False
    assert "_job" not in eng.calls[0][1]
    assert cl._bf_jobs == {}


@pytest.mark.asyncio
async def test_cancel_while_queued_drops_the_job():
    gate = asyncio.Event()
    eng = _StubEngine([[1], [2], []], gate=gate)
    cl = _listener(eng)
    await cl._on_ws({"type": "backfill_run", "data": _run("a", "111")})
    await cl._on_ws({"type": "backfill_run", "data": _run("b", "222")})
    await asyncio.sleep(0.05)

    res = await cl._on_ws({"type": "backfill_cancel", "data": {"job_id": "b"}})
    assert res == {"ok": True, "job_id": "b", "state": "queued"}
    assert cl.ws.of("backfill_job_end") == [
        {"job_id": "b", "ok": False, "error": "cancelled", "last_orig_message_id": None}
    ]
    gate.set()
    await _drain(cl)

    assert [c[0] for c in eng.calls] == [111]
    ends = cl.ws.of("backfill_job_end")
    assert [(e["job_id"], e["ok"]) for e in ends] == [("b", False), ("a", True)]
    assert not any(
        j["job_id"] == "b" and j["state"] == "running" for j in cl.ws.of("backfill_job")
    )


@pytest.mark.asyncio
async def test_cancel_while_running_stops_at_next_page():
    gate = asyncio.Event()
    eng = _StubEngine([[1, 2, 3], [4, 5, 6], []], gate=gate)
    cl = _listener(eng)
    await cl._on_ws({"type": "backfill_run", "data": _run("r", "333")})
    await asyncio.sleep(0.05)

    res = await cl._on_ws({"type": "backfill_cancel", "data": {"job_id": "r"}})
    assert res["state"] == "running"
    gate.set()
    await _drain(cl)

    assert eng.eng.bot.calls == 1
    end = cl.ws.of("backfill_job_end")
    assert end == [{"job_id": "r", "ok": False, "error": "cancelled", "last_orig_message_id": "3"}]
    assert cl.ws.of("backfill_stream_end") == []


@pytest.mark.asyncio
async def test_progress_throttle_coalesces():
    now = [0.0]
    ws = _FakeWs()
    job = BackfillJob(BIG, BIG, ws.send, clock=lambda: now[0])
    for i in range(1, 11):
        await job.progress(pulled=i)
        now[0] += 0.125
    await job.progress(total=40)
    await job.progress(pulled=11, force=True)

    frames = ws.of("backfill_job_progress")
    assert [f["pulled"] for f in frames] == [1, 5, 9, 11]
    assert frames[-1] == {"job_id": BIG, "pulled": 11, "total_estimate": 40}
