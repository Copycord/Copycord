# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Backfill channels: the first task type.

One step per channel. Every channel becomes a client job keyed by
job_id = "<run_id>:<channel_id>"; the admin bus watcher hands the job frames
(backfill_job, backfill_job_progress, backfill_job_delivered,
backfill_job_cleanup, backfill_job_end) to dispatch(), which routes them by
job_id to the run's step.

The client pulls history (backfill_job_progress: pulled, total_estimate) well
ahead of the server, which forwards each message, thread and sticker in turn
(backfill_job_delivered: delivered, total). A step's progress is what the
server delivered; pulled only shows in its label. The server sends
backfill_job_cleanup once everything it queued for the channel went out.

configure(db=..., send=...) wires the DB and the client command sender
(an async callable taking {type, data} and returning a dict); app.py does it
at startup and tests hand in fakes."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .context import STEP_FINAL, iso
from .registry import TaskType, register

log = logging.getLogger("copycord.tasks.backfill")

TYPE_KEY = "backfill_channels"
CANCEL_WAIT = 20.0
# How long a job marked cancelled after CANCEL_WAIT still takes its late backfill_job_end.
LATE_END_WAIT = 600.0
JOB_FRAMES = ("backfill_job", "backfill_job_progress", "backfill_job_delivered",
              "backfill_job_cleanup", "backfill_job_end")


# wiring

@dataclass
class _Wiring:
    db: Any = None
    send: Optional[Callable[[dict], Awaitable[Any]]] = None


_wiring = _Wiring()


def configure(db: Any = None, send: Optional[Callable[[dict], Awaitable[Any]]] = None) -> None:
    if db is not None:
        _wiring.db = db
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

def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


class Range(BaseModel):
    mode: Literal["all", "since", "between", "last_n"] = "all"
    since: Optional[str] = None
    until: Optional[str] = None
    last_n: Optional[int] = None

    @model_validator(mode="after")
    def _check(self) -> "Range":
        # v3 rules: since needs a start date, between needs both, last_n a positive number.
        if self.mode in ("since", "between"):
            if not self.since:
                raise ValueError("Pick a start date")
            try:
                start = _parse_dt(self.since)
            except ValueError:
                raise ValueError("Start date is not a valid date") from None
        if self.mode == "between":
            if not self.until:
                raise ValueError("Pick both dates")
            try:
                end = _parse_dt(self.until)
            except ValueError:
                raise ValueError("End date is not a valid date") from None
            try:
                if end <= start:
                    raise ValueError("End date must be after the start date")
            except TypeError:
                raise ValueError("Use the same date format for both dates") from None
        if self.mode == "last_n" and (self.last_n is None or self.last_n < 1):
            raise ValueError("Enter a positive number of messages")
        if self.mode == "all":
            self.since = self.until = None
            self.last_n = None
        elif self.mode == "since":
            self.until = None
            self.last_n = None
        elif self.mode == "between":
            self.last_n = None
        else:
            self.since = self.until = None
        return self


def _id_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        raise ValueError("must be a list of ids")
    out: list[str] = []
    for v in value:
        s = str(v).strip()
        if not s.isdigit():
            raise ValueError(f"{s!r} is not an id")
        if s not in out:
            out.append(s)
    return out


class BackfillInputs(BaseModel):
    mapping_id: str = Field(min_length=1)
    channel_ids: list[str] = Field(default_factory=list)
    category_ids: list[str] = Field(default_factory=list)
    range: Range = Field(default_factory=Range)
    ignore_cloned: bool = False
    mode: Literal["resume", "new"] = "resume"

    @field_validator("channel_ids", "category_ids", mode="before")
    @classmethod
    def _ids(cls, value: Any) -> list[str]:
        return _id_list(value)

    @model_validator(mode="after")
    def _selection(self) -> "BackfillInputs":
        if not self.channel_ids and not self.category_ids:
            raise ValueError("Select at least one channel or category")
        return self


# mapping lookups

def _db(lookups: Any = None) -> Any:
    return lookups if lookups is not None and hasattr(lookups, "get_mapping_by_id") else _wiring.db


def _mapping_rows(db: Any, mapping: dict) -> tuple[list[dict], list[dict]]:
    og, cg = str(mapping.get("original_guild_id") or ""), str(mapping.get("cloned_guild_id") or "")

    def belongs(r: dict) -> bool:
        return str(r.get("original_guild_id") or "") == og and str(r.get("cloned_guild_id") or "") == cg

    chans = [dict(r) for r in db.get_all_channel_mappings()]
    cats = [dict(r) for r in db.get_all_category_mappings()]
    return [c for c in chans if belongs(c)], [c for c in cats if belongs(c)]


def _is_orphan(ch: dict) -> bool:
    return not ch.get("cloned_channel_id")


def _expand(inputs: BackfillInputs, chans: list[dict]) -> list[dict]:
    """Selected channels then the current non-orphan channels of each selected
    category, deduped, in selection order."""
    by_id = {str(c["original_channel_id"]): c for c in chans}
    out: dict[str, dict] = {}
    for cid in inputs.channel_ids:
        out.setdefault(cid, by_id.get(cid) or {"original_channel_id": cid, "original_channel_name": None})
    for cat in inputs.category_ids:
        for c in chans:
            if str(c.get("original_parent_category_id") or "") == cat and not _is_orphan(c):
                out.setdefault(str(c["original_channel_id"]), c)
    return list(out.values())


def describe(inputs: BackfillInputs, lookups: Any = None) -> tuple[str, dict]:
    db = _db(lookups)
    mapping = None
    try:
        mapping = db.get_mapping_by_id(inputs.mapping_id) if db is not None else None
    except Exception:
        mapping = None
    context = {"label": inputs.mapping_id, "mapping_id": inputs.mapping_id, "guild_id": None}
    if not mapping:
        n = len(inputs.channel_ids)
        return (f"Backfill {n} channels" if n != 1 else "Backfill 1 channel"), context
    context.update(label=mapping.get("mapping_name") or inputs.mapping_id,
                   guild_id=str(mapping.get("original_guild_id") or "") or None)
    chans, cats = _mapping_rows(db, mapping)
    channels = _expand(inputs, chans)
    if not inputs.channel_ids and len(inputs.category_ids) == 1:
        cat = next((c for c in cats if str(c.get("original_category_id")) == inputs.category_ids[0]), {})
        name = cat.get("original_category_name") or inputs.category_ids[0]
        return f"Backfill category {name} ({len(channels)} channels)", context
    if len(channels) == 1:
        ch = channels[0]
        return f"Backfill #{ch.get('original_channel_name') or ch['original_channel_id']}", context
    return f"Backfill {len(channels)} channels", context


# job routing

@dataclass
class _Job:
    job_id: str
    run: Any
    channel_id: str
    name: str
    step: Any
    mapping_id: str
    ctx: Any
    result: dict = field(default_factory=dict)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    pulled: int = 0
    total_estimate: Optional[int] = None
    delivered: int = 0
    server_total: Optional[int] = None
    cleaning: bool = False

    @property
    def terminal(self) -> bool:
        return self.step.data["state"] in STEP_FINAL

    @property
    def total(self) -> Optional[int]:
        known = [n for n in (self.total_estimate, self.server_total) if n is not None]
        return max(known + [self.delivered]) if known else None


_jobs: dict[str, _Job] = {}
# Jobs that ended cancelled without their backfill_job_end; the late end still brings the count.
_late: dict[str, _Job] = {}


def job_id_for(run_id: Any, channel_id: Any) -> str:
    return f"{run_id}:{channel_id}"


def busy_channels(mapping_id: Optional[str] = None) -> dict[str, _Job]:
    """Channels in a non-terminal step of an active run: {channel_id: job}."""
    return {j.channel_id: j for j in _jobs.values()
            if not j.terminal and (mapping_id is None or j.mapping_id == str(mapping_id))}


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


def progress_label(delivered: int, total: Optional[int], pulled: int) -> str:
    """"12 of 35 delivered, 35 pulled"; without a total "12 delivered, 35 pulled"."""
    sent = f"{_fmt(delivered)} of {_fmt(total)}" if total is not None else _fmt(delivered)
    return f"{sent} delivered, {_fmt(pulled)} pulled"


async def _show_progress(job: "_Job") -> None:
    """The step's bar is what the server delivered against the best total known."""
    label = None if job.cleaning else progress_label(job.delivered, job.total, job.pulled)
    await job.step.progress(job.delivered, job.total, label=label)


async def _live_summary(ctx: Any) -> None:
    """The run's counts while it goes: finished steps by outcome and the messages delivered so far."""
    counts = {"success": 0, "failed": 0, "skipped": 0, "cancelled": 0}
    for s in ctx.steps_dict():
        if s["state"] in counts:
            counts[s["state"]] += 1
    delivered = sum(int(j.result.get("delivered") or 0) for j in _jobs.values() if j.ctx is ctx)
    await ctx.summary(**counts, delivered=delivered)


async def dispatch(frame: dict) -> bool:
    """Route one backfill_job* frame ({type, data}) to its run by job_id.
    Returns False for frames this module does not own."""
    typ = frame.get("type")
    data = frame.get("data") or {}
    if typ not in JOB_FRAMES:
        return False
    job_id = str(data.get("job_id") or "")
    if typ == "backfill_job_end" and job_id in _late and job_id not in _jobs:
        try:
            await _late_end(_late.pop(job_id), data)
        except Exception:
            log.exception("late backfill_job_end for job %s failed", job_id)
        return True
    job = _jobs.get(job_id)
    if job is None:
        log.debug("backfill frame %s for unknown job %r ignored", typ, data.get("job_id"))
        return False
    if job.terminal:
        return True
    step, ctx, name = job.step, job.ctx, job.name
    try:
        if typ == "backfill_job":
            state = data.get("state")
            if state == "queued":
                pos = data.get("queue_position")
                await step.queued(pos)
                await ctx.log(f"Queued #{name} ({pos} in queue)" if pos else f"Queued #{name}", step=step.key)
            elif state == "running":
                if step.data["state"] != "running":
                    job.result["started_at"] = iso(ctx._clock())
                    await step.start()
                    await ctx.log(f"Started #{name}", step=step.key)
        elif typ in ("backfill_job_progress", "backfill_job_delivered"):
            if step.data["state"] == "queued":
                job.result["started_at"] = iso(ctx._clock())
                await step.start()
            if typ == "backfill_job_progress":
                job.pulled = max(job.pulled, _int(data.get("pulled")) or 0)
                if data.get("total_estimate") is not None:
                    job.total_estimate = _int(data.get("total_estimate"))
            else:
                job.delivered = max(job.delivered, _int(data.get("delivered")) or 0)
                job.result["delivered"] = job.delivered
                if data.get("total") is not None:
                    job.server_total = _int(data.get("total"))
            await _show_progress(job)
            if typ == "backfill_job_delivered":
                await _live_summary(ctx)
        elif typ == "backfill_job_cleanup":
            job.cleaning = True
            await step.state("Cleaning up")
        elif typ == "backfill_job_end":
            await _end_job(job, data)
    except Exception:
        log.exception("backfill frame %s for job %s failed", typ, job.job_id)
    return True


async def _end_job(job: _Job, data: dict) -> None:
    step, ctx, name = job.step, job.ctx, job.name
    # The server's count of what it wrote for the job. The count only grows, so an end that says
    # less (or nothing) keeps what the delivered frames already showed.
    delivered = max(job.delivered, _int(data.get("delivered")) or 0)
    job.delivered = delivered
    error = data.get("error")
    job.result.update(delivered=delivered, error=None if data.get("ok") else error,
                      last_orig_message_id=data.get("last_orig_message_id"))
    await step.progress(delivered, job.total)
    if data.get("ok"):
        await step.finish("success", f"{_fmt(delivered)} messages")
        await ctx.log(f"Finished #{name}: {_fmt(delivered)} messages", step=step.key)
    elif ctx.cancelled.is_set() or error == "cancelled":
        await step.finish("cancelled", _cancelled_note(delivered))
        await ctx.log(f"Cancelled #{name} after {_fmt(delivered)} messages", step=step.key)
    else:
        await step.finish("failed", error or "Failed")
        await ctx.log(f"Failed #{name}: {error or 'unknown error'}", level="error", step=step.key)
    job.result["finished_at"] = step.data["finished_at"]
    job.done.set()
    await _live_summary(ctx)


async def _late_end(job: _Job, data: dict) -> None:
    """backfill_job_end for a job already marked cancelled: the step stays cancelled, its count,
    last message id and note take the real end, and so do the run's report and summary."""
    step, ctx, name = job.step, job.ctx, job.name
    delivered = max(job.delivered, _int(data.get("delivered")) or 0)
    job.delivered = delivered
    note = _cancelled_note(delivered)
    job.result.update(delivered=delivered, note=note,
                      last_orig_message_id=data.get("last_orig_message_id"))
    step.data.update(done=delivered, note=note)
    report = ctx.run.report or {}
    channels = report.get("channels") or []
    if channels:
        total = sum(int(r.get("delivered") or 0) for r in channels)
        if isinstance(report.get("summary"), dict):
            report["summary"]["delivered"] = total
        await ctx.summary(delivered=total)
    await ctx.log(f"Cancelled #{name} after {_fmt(delivered)} messages (late end)", step=step.key)
    await ctx.flush()


def _forget_late(job_id: str, job: _Job) -> None:
    if _late.get(job_id) is job:
        del _late[job_id]


def _cancelled_note(delivered: int) -> str:
    return f"Cancelled after {_fmt(delivered)} messages" if delivered else "Cancelled"


# options

async def options(params: dict) -> dict:
    db = _wiring.db
    mapping_id = str((params or {}).get("mapping_id") or "")
    mapping = db.get_mapping_by_id(mapping_id) if (db is not None and mapping_id) else None
    if not mapping:
        return {"mapping_id": mapping_id, "categories": [], "error": "Unknown mapping"}
    chans, cats = _mapping_rows(db, mapping)
    cloned_gid = mapping.get("cloned_guild_id")
    busy = busy_channels()

    def channel(c: dict) -> dict:
        cid = str(c["original_channel_id"])
        job = busy.get(cid)
        cp = None
        try:
            if cloned_gid is not None:
                cp = db.backfill_get_incomplete_for_channel_in_clone(int(cid), int(cloned_gid))
        except Exception:
            cp = None
        return {
            "id": cid,
            "name": c.get("original_channel_name") or cid,
            "orphan": _is_orphan(c),
            "busy": ({"run_id": str(job.run.id), "number": job.run.number} if job else None),
            "resumable": ({"checkpoint_at": cp.get("last_orig_timestamp") or cp.get("updated_at")}
                          if cp else None),
        }

    out_cats = []
    for cat in cats:
        cat_id = str(cat.get("original_category_id"))
        out_cats.append({
            "id": cat_id, "name": cat.get("original_category_name") or cat_id,
            "channels": [channel(c) for c in chans
                         if str(c.get("original_parent_category_id") or "") == cat_id],
        })
    uncategorized = [channel(c) for c in chans if not c.get("original_parent_category_id")]
    if uncategorized:
        out_cats.append({"id": None, "name": "Uncategorized", "channels": uncategorized})
    return {"mapping_id": mapping_id, "categories": out_cats}


# run

async def run(ctx: Any, inputs: BackfillInputs) -> None:
    db = _wiring.db
    task_run = ctx.run
    mapping = db.get_mapping_by_id(inputs.mapping_id) if db is not None else None
    if not mapping:
        raise RuntimeError(f"Unknown mapping {inputs.mapping_id}")
    original_gid = mapping.get("original_guild_id")
    cloned_gid = mapping.get("cloned_guild_id")
    chans, _ = _mapping_rows(db, mapping)
    channels = _expand(inputs, chans)
    rng = inputs.range.model_dump()
    await ctx.log(f"Backfilling {len(channels)} channel{'s' if len(channels) != 1 else ''} "
                  f"in {mapping.get('mapping_name') or inputs.mapping_id}")

    results: list[dict] = []
    mine: list[_Job] = []
    busy = busy_channels()
    for c in channels:
        cid = str(c["original_channel_id"])
        name = c.get("original_channel_name") or cid
        step = ctx.step(cid, f"#{name}")
        result = {"channel_id": cid, "name": name, "job_id": None, "outcome": None,
                  "delivered": 0, "error": None, "started_at": None, "finished_at": None,
                  "checkpoint": None, "note": None}
        results.append(result)
        other = busy.get(cid)
        if other is not None and other.run.id != task_run.id:
            note = f"Already running in #{other.run.number}"
            await step.finish("skipped", note)
            result.update(outcome="skipped", note=note, finished_at=step.data["finished_at"])
            await ctx.log(f"Skipped #{name}: already running in #{other.run.number}", step=cid)
            continue
        job = _Job(job_id_for(task_run.id, cid), task_run, cid, name, step,
                   str(inputs.mapping_id), ctx, result)
        result["job_id"] = job.job_id
        _jobs[job.job_id] = job
        _late.pop(job.job_id, None)
        mine.append(job)

    try:
        for job in mine:
            cid = job.channel_id
            if ctx.cancelled.is_set():
                # Cancelled while the jobs were being handed out: the rest never reach the client.
                if not job.terminal:
                    await job.step.finish("cancelled", ctx.cancel_note or "Cancelled")
                job.result["finished_at"] = job.step.data["finished_at"]
                job.done.set()
                continue
            checkpoint = None
            if inputs.mode == "resume" and cloned_gid is not None:
                try:
                    row = db.backfill_get_incomplete_for_channel_in_clone(int(cid), int(cloned_gid))
                except Exception:
                    row = None
                if row and row.get("last_orig_message_id"):
                    checkpoint = {"last_orig_message_id": str(row["last_orig_message_id"]),
                                  "last_orig_timestamp": row.get("last_orig_timestamp")}
                    await ctx.log(f"Resuming #{job.name} from {checkpoint['last_orig_timestamp'] or 'its checkpoint'}",
                                  step=cid)
            job.result["checkpoint"] = checkpoint
            exclude_ids: list[str] = []
            if inputs.ignore_cloned and cloned_gid is not None:
                try:
                    exclude_ids = [str(x) for x in db.get_cloned_original_ids_for_channel(int(cid), int(cloned_gid))]
                except Exception:
                    exclude_ids = []
                if exclude_ids:
                    await ctx.log(f"#{job.name}: skipping {_fmt(len(exclude_ids))} messages already cloned", step=cid)
            try:
                db.backfill_create_run(int(cid), rng, run_id=job.job_id,
                                       original_guild_id=original_gid, cloned_guild_id=cloned_gid,
                                       task_run_id=task_run.id)
            except Exception:
                log.exception("backfill_runs insert for %s failed", job.job_id)
            res = await _send({"type": "backfill_run", "data": {
                "job_id": job.job_id, "original_channel_id": cid, "range": rng,
                "exclude_ids": exclude_ids, "checkpoint": checkpoint,
                "ignore_cloned": inputs.ignore_cloned, "mapping_id": str(inputs.mapping_id),
                "original_guild_id": str(original_gid) if original_gid is not None else None,
                "cloned_guild_id": str(cloned_gid) if cloned_gid is not None else None,
            }})
            if not res.get("ok", True) and not job.terminal:
                error = res.get("error") or "client did not accept the job"
                job.result["error"] = error
                await job.step.finish("failed", error)
                await ctx.log(f"Failed #{job.name}: {error}", level="error", step=cid)
                job.done.set()

        await _wait_jobs(ctx, mine)
    finally:
        for job in mine:
            _jobs.pop(job.job_id, None)

    counts = {"success": 0, "failed": 0, "skipped": 0, "cancelled": 0}
    delivered = 0
    for result, step in ((r, ctx.step(r["channel_id"])) for r in results):
        state = step.data["state"]
        if state not in STEP_FINAL:
            state = "cancelled" if ctx.cancelled.is_set() else "failed"
            await step.finish(state, "Cancelled" if state == "cancelled" else "No end from the client")
        result["outcome"] = state
        result["finished_at"] = result.get("finished_at") or step.data["finished_at"]
        if result.get("note") is None:
            result["note"] = step.data.get("note")
        counts[state] += 1
        delivered += int(result.get("delivered") or 0)
    summary = {**counts, "delivered": delivered}
    await ctx.summary(**summary)
    await ctx.report({
        "inputs": inputs.model_dump(mode="json"),
        "mapping": {"mapping_id": str(inputs.mapping_id), "name": mapping.get("mapping_name"),
                    "original_guild_id": str(original_gid) if original_gid is not None else None,
                    "cloned_guild_id": str(cloned_gid) if cloned_gid is not None else None},
        "channels": results,
        "summary": summary,
    })
    total = len(results)
    if counts["failed"]:
        await ctx.log(f"{counts['failed']} of {total} channels failed", level="error")
        ctx.fail(f"{counts['failed']} of {total} channels failed")
    elif ctx.cancelled.is_set():
        await ctx.log("Run cancelled", level="warning")
    else:
        await ctx.log(f"Done: {counts['success']} of {total} channels, {_fmt(delivered)} messages")


async def _wait_jobs(ctx: Any, jobs: list[_Job]) -> None:
    pending = [j for j in jobs if not j.done.is_set()]
    if not pending:
        return
    all_done = asyncio.ensure_future(asyncio.gather(*(j.done.wait() for j in pending)))
    cancel_wait = asyncio.ensure_future(ctx.cancelled.wait())
    try:
        await asyncio.wait({all_done, cancel_wait}, return_when=asyncio.FIRST_COMPLETED)
        if all_done.done():
            return
        live = [j for j in jobs if not j.terminal]
        # Bots stopped (ctx.cancel_note): the service already ended the steps and no client is
        # left to tell or to send an end, so nothing is sent and nothing waited for.
        if live and ctx.cancel_note is None:
            await ctx.log(f"Cancelling {len(live)} job{'s' if len(live) != 1 else ''}", level="warning")
            # The client stops pulling (or drops a queued job) and tells the server, which stops
            # forwarding; each job then ends with backfill_job_end and what it delivered.
            for job in live:
                await _send({"type": "backfill_cancel", "data": {"job_id": job.job_id}})
            try:
                await asyncio.wait_for(asyncio.shield(all_done), timeout=CANCEL_WAIT)
            except asyncio.TimeoutError:
                pass
        for job in jobs:
            if not job.terminal:
                job.result["delivered"] = job.delivered
                await job.step.finish("cancelled", _cancelled_note(job.delivered))
                job.result["finished_at"] = job.step.data["finished_at"]
                await ctx.log(f"Cancelled #{job.name} after {_fmt(job.delivered)} messages "
                              f"(no end from the client)", level="warning", step=job.step.key)
                _late[job.job_id] = job
                asyncio.get_running_loop().call_later(LATE_END_WAIT, _forget_late, job.job_id, job)
    finally:
        cancel_wait.cancel()
        if not all_done.done():
            all_done.cancel()


BACKFILL = register(TaskType(
    key=TYPE_KEY,
    title="Backfill channels",
    description="Clone the message history of selected channels into their clones.",
    run=run,
    inputs=BackfillInputs,
    describe=describe,
    options=options,
    requires_bots=True,
))
