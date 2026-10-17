# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Export DMs: a user's DM history from the client account to a JSON file
and, optionally, a webhook. Replaces the /export_dms slash command.

The run sends export_dm_history with job_id = run id to the client, whose
DmHistoryExporter reads the DM and sends export_dm_progress frames
(phase count, read, json, forward) and one export_dm_done with its counts.
The server relays both to the admin bus; app.py hands them to dispatch().

Steps: Read DM history, then Save JSON file and Forward to webhook when chosen.
A cancel sends export_dm_cancel; the client stops the exporter, which still
sends its export_dm_done (error "cancelled") with what it got through.

configure(send=...) wires the client command sender; tests hand in fakes."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .context import STEP_FINAL
from .registry import TaskType, register

log = logging.getLogger("copycord.tasks.export_dms")

TYPE_KEY = "export_dms"
CANCEL_WAIT = 15.0
FRAMES = ("export_dm_progress", "export_dm_done")

ERRORS = {
    "dm-not-in-cache": "No open DM with this user on the client account",
    "dm-export-in-progress": "A DM export for this user is already running",
}


# wiring

@dataclass
class _Wiring:
    send: Optional[Callable[[dict], Awaitable[Any]]] = None


_wiring = _Wiring()


def configure(send: Optional[Callable[[dict], Awaitable[Any]]] = None) -> None:
    if send is not None:
        _wiring.send = send


async def _send(frame: dict) -> dict:
    if _wiring.send is None:
        return {"ok": False, "error": "client sender not configured"}
    try:
        res = await _wiring.send(frame)
    except Exception as exc:
        return {"ok": False, "error": str(exc) or type(exc).__name__}
    return res if isinstance(res, dict) else {"ok": True}


# inputs

class ExportDmsInputs(BaseModel):
    user_id: str = Field(title="User ID")
    json_file: bool = Field(True, title="Save a JSON file")
    webhook_url: Optional[str] = Field(None, title="Webhook URL")

    @field_validator("user_id", mode="before")
    @classmethod
    def _user_id(cls, value: Any) -> str:
        s = str(value if value is not None else "").strip()
        if not s.isdigit():
            raise ValueError("Enter a numeric Discord user ID")
        return s

    @field_validator("webhook_url", mode="before")
    @classmethod
    def _webhook(cls, value: Any) -> Optional[str]:
        s = str(value or "").strip()
        if not s:
            return None
        if not s.startswith(("https://", "http://")):
            raise ValueError("Enter a webhook URL starting with https://")
        return s

    @model_validator(mode="after")
    def _destination(self) -> "ExportDmsInputs":
        if not self.json_file and not self.webhook_url:
            raise ValueError("Pick a destination: a JSON file, a webhook or both")
        return self


def mask_webhook(url: Optional[str]) -> Optional[str]:
    """The webhook without its token: https://discord.com/api/webhooks/123/..."""
    if not url:
        return None
    head, sep, _token = url.rstrip("/").rpartition("/")
    return f"{head}/..." if sep and head.count("/") >= 3 else url


def describe(inputs: ExportDmsInputs, lookups: Any = None) -> tuple[str, dict]:
    return f"Export DMs with {inputs.user_id}", {"label": inputs.user_id, "user_id": inputs.user_id}


# job routing

@dataclass
class _Job:
    job_id: str
    ctx: Any
    inputs: ExportDmsInputs
    read: Any
    json_step: Any = None
    forward: Any = None
    username: Optional[str] = None
    read_count: int = 0
    total: Optional[int] = None
    sent: int = 0
    json_path: Optional[str] = None
    json_error: Optional[str] = None
    error: Optional[str] = None
    done: asyncio.Event = field(default_factory=asyncio.Event)

    def steps(self) -> list:
        return [s for s in (self.read, self.json_step, self.forward) if s is not None]


_jobs: dict[str, _Job] = {}


def _fmt(n: Any) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "0"


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _messages(n: int) -> str:
    return f"{_fmt(n)} message{'s' if n != 1 else ''}"


def _failed(job: _Job) -> int:
    """Messages read that a failed step kept from a chosen destination (not the ones still to go)."""
    missed = 0
    if job.forward is not None and job.forward.data["state"] == "failed":
        missed = max(0, job.read_count - job.sent)
    if job.json_step is not None and job.json_step.data["state"] == "failed":
        missed = job.read_count
    return missed


async def _live_summary(job: _Job) -> None:
    counts = {"exported": job.read_count, "failed": _failed(job)}
    if job.forward is not None:
        counts["forwarded"] = job.sent
    await job.ctx.summary(**counts)


async def _ensure_running(step: Any) -> None:
    if step is not None and step.data["state"] == "queued":
        await step.start()


async def dispatch(frame: dict) -> bool:
    """Route one export_dm_* frame ({type, data}) to its run by job_id.
    Returns False for frames this module does not own."""
    typ = frame.get("type")
    data = frame.get("data") or {}
    if typ not in FRAMES:
        return False
    job = _jobs.get(str(data.get("job_id") or ""))
    if job is None:
        return False
    if job.done.is_set():
        return True
    if data.get("username"):
        job.username = str(data["username"])
    try:
        if typ == "export_dm_progress":
            await _progress(job, data)
        else:
            await _end(job, data)
    except Exception:
        log.exception("export_dms frame %s for job %s failed", typ, job.job_id)
    return True


async def _progress(job: _Job, data: dict) -> None:
    phase = data.get("phase")
    done = _int(data.get("done")) or 0
    total = _int(data.get("total"))
    ctx = job.ctx
    if phase == "count":
        await _ensure_running(job.read)
        await job.read.progress(0, None, label=f"Counting messages, {_fmt(done)} found")
    elif phase == "read":
        await _ensure_running(job.read)
        job.read_count = max(job.read_count, done)
        if total is not None:
            job.total = total
        label = (f"{_fmt(job.read_count)} of {_fmt(job.total)} messages read" if job.total is not None
                 else f"{_messages(job.read_count)} read")
        await job.read.progress(job.read_count, job.total, label=label)
        await _live_summary(job)
    elif phase == "json":
        if job.read.data["state"] not in STEP_FINAL:
            await job.read.finish("success", f"{_messages(job.read_count)} read")
        if job.json_step is not None:
            await _ensure_running(job.json_step)
            if data.get("json_path"):
                job.json_path = str(data["json_path"])
                await job.json_step.finish("success", job.json_path)
                await ctx.log(f"Saved {_messages(job.read_count)} to {job.json_path}", step="json")
            else:
                job.json_error = str(data.get("json_error") or "JSON file not written")
                await job.json_step.finish("failed", job.json_error)
                await ctx.log(f"JSON file not written: {job.json_error}", level="error", step="json")
            await _live_summary(job)
    elif phase == "forward":
        if job.read.data["state"] not in STEP_FINAL:
            await job.read.finish("success", f"{_messages(job.read_count)} read")
        if job.forward is not None:
            await _ensure_running(job.forward)
            job.sent = max(job.sent, done)
            fwd_total = total if total is not None else job.read_count
            await job.forward.progress(job.sent, fwd_total,
                                       label=f"{_fmt(job.sent)} of {_fmt(fwd_total)} forwarded")
            await _live_summary(job)


async def _end(job: _Job, data: dict) -> None:
    ctx = job.ctx
    job.read_count = max(job.read_count, _int(data.get("count")) or 0)
    job.sent = max(job.sent, _int(data.get("sent")) or 0)
    if data.get("json_path"):
        job.json_path = str(data["json_path"])
    error = data.get("error")
    job.error = error
    if not error:
        if job.read.data["state"] not in STEP_FINAL:
            await job.read.finish("success", f"{_messages(job.read_count)} read")
        if job.json_step is not None and job.json_step.data["state"] not in STEP_FINAL:
            if job.json_path:
                await job.json_step.finish("success", job.json_path)
            else:
                job.json_error = job.json_error or "JSON file not written"
                await job.json_step.finish("failed", job.json_error)
        if job.forward is not None and job.forward.data["state"] not in STEP_FINAL:
            fwd = job.forward
            await fwd.progress(job.sent, job.read_count)
            if job.sent >= job.read_count:
                await fwd.finish("success", f"{_messages(job.sent)} forwarded")
            else:
                await fwd.finish("failed", f"Stopped after {_fmt(job.sent)} of {_fmt(job.read_count)}")
    elif error == "cancelled" or ctx.cancelled.is_set():
        for step in job.steps():
            if step.data["state"] not in STEP_FINAL:
                await step.finish("cancelled", _cancelled_note(job, step))
    else:
        reason = ERRORS.get(str(error), str(error))
        failed_one = False
        for step in job.steps():
            if step.data["state"] in STEP_FINAL:
                continue
            if not failed_one:
                await step.finish("failed", reason)
                failed_one = True
            else:
                await step.finish("skipped", "Not reached")
        await ctx.log(f"Export failed: {reason}", level="error")
    await _live_summary(job)
    job.done.set()


def _cancelled_note(job: _Job, step: Any) -> str:
    if step is job.forward and job.sent:
        return f"Cancelled after {_fmt(job.sent)} forwarded"
    if step is job.read and job.read_count:
        return f"Cancelled after {_fmt(job.read_count)} read"
    return "Cancelled"


# run

async def run(ctx: Any, inputs: ExportDmsInputs) -> None:
    job_id = str(ctx.run.id)
    job = _Job(job_id, ctx, inputs, read=ctx.step("read", "Read DM history"))
    if inputs.json_file:
        job.json_step = ctx.step("json", "Save JSON file")
    if inputs.webhook_url:
        job.forward = ctx.step("forward", "Forward to webhook")
    _jobs[job_id] = job
    try:
        await job.read.start()
        await ctx.log(f"Exporting DMs with user {inputs.user_id}")
        res = await _send({"type": "export_dm_history", "data": {
            "user_id": int(inputs.user_id), "webhook_url": inputs.webhook_url,
            "json_file": inputs.json_file, "job_id": job_id,
        }})
        if not res.get("ok", True) and not job.done.is_set():
            await _end(job, {"error": res.get("error") or "Client did not accept the export"})
        else:
            await _wait(ctx, job)
    finally:
        _jobs.pop(job_id, None)

    for step in job.steps():
        if step.data["state"] not in STEP_FINAL:
            state = "cancelled" if ctx.cancelled.is_set() else "failed"
            await step.finish(state, _cancelled_note(job, step) if state == "cancelled" else "No end from the client")
    summary = {"exported": job.read_count, "failed": _failed(job)}
    if job.forward is not None:
        summary["forwarded"] = job.sent
    await ctx.summary(**summary)
    error = None if ctx.cancelled.is_set() and job.error in (None, "cancelled") else job.error
    await ctx.report({
        "inputs": {**inputs.model_dump(mode="json"), "webhook_url": mask_webhook(inputs.webhook_url)},
        "user": {"id": inputs.user_id, "name": job.username},
        "output": {"json_path": job.json_path, "webhook": mask_webhook(inputs.webhook_url)},
        "summary": summary,
        "error": ERRORS.get(str(error), error) if error else None,
    })
    failed = [s for s in job.steps() if s.data["state"] == "failed"]
    if failed:
        ctx.fail(failed[0].data.get("note") or "Export failed")
    elif ctx.cancelled.is_set():
        await ctx.log("Run cancelled", level="warning")
    else:
        where = job.json_path or "the webhook"
        await ctx.log(f"Done: {_messages(job.read_count)} exported to {where}")


async def _wait(ctx: Any, job: _Job) -> None:
    done = asyncio.ensure_future(job.done.wait())
    cancel_wait = asyncio.ensure_future(ctx.cancelled.wait())
    try:
        await asyncio.wait({done, cancel_wait}, return_when=asyncio.FIRST_COMPLETED)
        if done.done():
            return
        # Bots stopped (ctx.cancel_note): the service already ended the steps and no client is left to tell.
        if ctx.cancel_note is None:
            await ctx.log("Stopping the export", level="warning")
            await _send({"type": "export_dm_cancel",
                         "data": {"user_id": int(job.inputs.user_id), "job_id": job.job_id}})
            try:
                await asyncio.wait_for(asyncio.shield(done), timeout=CANCEL_WAIT)
            except asyncio.TimeoutError:
                await ctx.log("No end from the client after the cancel", level="warning")
    finally:
        cancel_wait.cancel()
        if not done.done():
            done.cancel()
        await asyncio.gather(done, cancel_wait, return_exceptions=True)


EXPORT_DMS = register(TaskType(
    key=TYPE_KEY,
    title="Export DMs",
    description="Export a user's DM history from the client account to a JSON file or a webhook.",
    run=run,
    inputs=ExportDmsInputs,
    describe=describe,
    requires_bots=True,
))
