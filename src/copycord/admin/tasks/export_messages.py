# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Export messages: a guild's messages, filtered as the Guilds page export dialog
does, to a JSON file and, optionally, a webhook. Replaces the dialog's direct
POST /api/export/messages (still served until the button moves here).

The run sends export_messages with job_id = run id to the client, whose
ExportMessagesRunner scans the guild and sends export_messages_progress frames
(phase scan, json, media, forward) and one export_messages_done with its counts.
The server relays both to the admin bus; app.py hands them to dispatch().

Steps: Scan channels, Save JSON file, then Download media and Forward to webhook
when chosen. A cancel sends export_messages_cancel; the client cancels the runner,
which still sends its export_messages_done (error "cancelled") with its counts.

The JSON file's path is in the run report only. GET /api/tasks/export-messages/<run id>
tells the run page whether the file is still there (name, size), and .../file serves it.

configure(send=..., data_dir=..., db=...) wires the client command sender, the data
directory the client writes exports/<guild id>/ under and the DB the guild list
comes from; tests hand in fakes."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from .context import STEP_FINAL
from .export_dms import mask_webhook
from .registry import TaskType, register
from .routes import _run_id, get_service, router

log = logging.getLogger("copycord.tasks.export_messages")

TYPE_KEY = "export_messages"
# How long a cancel waits for the client's export_messages_done; the client cancels at once,
# so this only bounds a client that went quiet.
CANCEL_WAIT = 1.0
FRAMES = ("export_messages_progress", "export_messages_done")

ERRORS = {
    "Guild not found": "The client account is not in this server",
    "export-already-running": "An export for this server is already running",
    "Export already running for this guild": "An export for this server is already running",
    "no-readable-channels": "The client account cannot read any channel in this server",
}


# wiring

@dataclass
class _Wiring:
    send: Optional[Callable[[dict], Awaitable[Any]]] = None
    data_dir: Optional[Path] = None
    db: Any = None


_wiring = _Wiring()

# Server names seen by options() or the DB, so a run's title names its server.
_guild_names: dict[str, str] = {}


def configure(send: Optional[Callable[[dict], Awaitable[Any]]] = None, data_dir: Any = None,
              db: Any = None) -> None:
    if send is not None:
        _wiring.send = send
    if data_dir is not None:
        _wiring.data_dir = Path(data_dir)
    if db is not None:
        _wiring.db = db


async def _send(frame: dict) -> dict:
    if _wiring.send is None:
        return {"ok": False, "error": "client sender not configured"}
    try:
        res = await _wiring.send(frame)
    except Exception as exc:
        return {"ok": False, "error": str(exc) or type(exc).__name__}
    return res if isinstance(res, dict) else {"ok": True}


# inputs (the body the Guilds page export dialog posts)

def _snowflake(value: Any, what: str, required: bool) -> Optional[str]:
    s = str(value if value is not None else "").strip()
    if not s:
        if required:
            raise ValueError(f"Enter a numeric Discord {what} ID")
        return None
    if not s.isdigit():
        raise ValueError(f"Enter a numeric Discord {what} ID")
    return s


class MediaTypes(BaseModel):
    images: bool = True
    videos: bool = True
    audio: bool = True
    other: bool = True


class DownloadMedia(BaseModel):
    images: bool = False
    videos: bool = False
    audio: bool = False
    other: bool = False

    def any(self) -> bool:
        return self.images or self.videos or self.audio or self.other


class ExportFilters(BaseModel):
    has_content: bool = Field(True, title="Messages with text")
    embeds: bool = Field(True, title="Embeds")
    attachments: bool = Field(True, title="Attachments")
    att_types: MediaTypes = Field(default_factory=MediaTypes, title="Attachment types")
    links: bool = Field(True, title="Links")
    emojis: bool = Field(True, title="Emojis")
    word_on: bool = Field(False, title="Includes word")
    word: str = Field("", title="Word")
    replies: bool = Field(True, title="Replies")
    bots: bool = Field(True, title="Bot messages")
    system: bool = Field(True, title="System messages")
    min_length: int = Field(0, ge=0, title="Minimum length")
    min_reactions: int = Field(0, ge=0, title="Minimum reactions")
    pinned: bool = Field(True, title="Pinned")
    stickers: bool = Field(True, title="Stickers")
    mentions: bool = Field(True, title="Mentions")
    threads: bool = Field(True, title="Threads")
    forum_threads: bool = Field(True, title="Forum threads")
    private_threads: bool = Field(True, title="Private threads")
    download_media: DownloadMedia = Field(default_factory=DownloadMedia, title="Download media")

    @field_validator("word", mode="before")
    @classmethod
    def _word(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("min_length", "min_reactions", mode="before")
    @classmethod
    def _count(cls, value: Any) -> int:
        if value is None or value == "":
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            raise ValueError("Enter a whole number") from None


class ExportMessagesInputs(BaseModel):
    guild_id: str = Field(title="Server")
    channel_id: Optional[str] = Field(None, title="Channel ID")
    user_id: Optional[str] = Field(None, title="User ID")
    webhook_url: Optional[str] = Field(None, title="Webhook URL")
    has_attachments: bool = Field(False, title="Only messages with attachments")
    after_iso: Optional[str] = Field(None, title="After")
    before_iso: Optional[str] = Field(None, title="Before")
    filters: ExportFilters = Field(default_factory=ExportFilters, title="Filters")

    @field_validator("guild_id", mode="before")
    @classmethod
    def _guild_id(cls, value: Any) -> str:
        return _snowflake(value, "server", True)

    @field_validator("channel_id", mode="before")
    @classmethod
    def _channel_id(cls, value: Any) -> Optional[str]:
        return _snowflake(value, "channel", False)

    @field_validator("user_id", mode="before")
    @classmethod
    def _user_id(cls, value: Any) -> Optional[str]:
        return _snowflake(value, "user", False)

    @field_validator("webhook_url", mode="before")
    @classmethod
    def _webhook(cls, value: Any) -> Optional[str]:
        s = str(value or "").strip()
        if not s:
            return None
        if not s.startswith(("https://", "http://")):
            raise ValueError("Enter a webhook URL starting with https://")
        return s

    @field_validator("after_iso", "before_iso", mode="before")
    @classmethod
    def _iso(cls, value: Any) -> Optional[str]:
        s = str(value or "").strip()
        if not s:
            return None
        try:
            datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("Enter an ISO date and time, like 2026-10-17T12:00:00Z") from None
        return s

    @field_validator("filters", mode="before")
    @classmethod
    def _filters(cls, value: Any) -> Any:
        return value if value is not None else {}


def _guild_name(guild_id: str) -> str:
    name = _guild_names.get(guild_id)
    if name:
        return name
    db = _wiring.db
    if db is not None:
        try:
            for row in db.get_all_guilds() or []:
                if str(row.get("guild_id")) == guild_id and row.get("name"):
                    _guild_names[guild_id] = str(row["name"])
                    return _guild_names[guild_id]
        except Exception:
            pass
    return guild_id


def describe(inputs: ExportMessagesInputs, lookups: Any = None) -> tuple[str, dict]:
    name = _guild_name(inputs.guild_id)
    return f"Export messages from {name}", {"label": name, "guild_id": inputs.guild_id}


# options: the guilds the Guilds page lists

async def options(params: dict) -> dict:
    """The client account's servers from the DB, by name: {guilds: [{id, name, icon_url}]}."""
    db = _wiring.db
    if db is None:
        return {"guilds": [], "error": "Server list not configured"}
    try:
        rows = db.get_all_guilds() or []
    except Exception as exc:
        return {"guilds": [], "error": f"Could not load the servers ({exc})"}
    guilds = []
    for row in rows:
        gid = str(row.get("guild_id") or "").strip()
        if not gid.isdigit():
            continue
        name = str(row.get("name") or gid)
        _guild_names[gid] = name
        guilds.append({"id": gid, "name": name, "icon_url": row.get("icon_url")})
    guilds.sort(key=lambda g: g["name"].casefold())
    return {"guilds": guilds}


# job routing

@dataclass
class _Job:
    job_id: str
    ctx: Any
    inputs: ExportMessagesInputs
    scan: Any
    json_step: Any = None
    media: Any = None
    forward: Any = None
    targets_done: int = 0
    targets_total: Optional[int] = None
    scanned: int = 0
    matched: int = 0
    forwarded: int = 0
    media_report: Optional[dict] = None
    json_path: Optional[str] = None
    json_error: Optional[str] = None
    error: Optional[str] = None
    done: asyncio.Event = field(default_factory=asyncio.Event)

    def steps(self) -> list:
        return [s for s in (self.scan, self.json_step, self.media, self.forward) if s is not None]


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


def _files(n: int) -> str:
    return f"{_fmt(n)} file{'s' if n != 1 else ''}"


def _media_counts(job: _Job) -> tuple[int, int]:
    """(files downloaded, files failed) from the client's media report."""
    report = job.media_report or {}
    ok = sum(_int(report.get(k)) or 0 for k in ("images", "videos", "audio", "other"))
    return ok, _int(report.get("errors")) or 0


def _failed(job: _Job) -> int:
    """Matched messages that a failed step kept from a chosen destination."""
    missed = 0
    if job.forward is not None and job.forward.data["state"] == "failed":
        missed = max(0, job.matched - job.forwarded)
    if job.json_step is not None and job.json_step.data["state"] == "failed":
        missed = job.matched
    return missed


def _summary(job: _Job) -> dict:
    counts = {"scanned": job.scanned, "exported": job.matched, "failed": _failed(job)}
    if job.forward is not None:
        counts["forwarded"] = job.forwarded
    if job.media is not None:
        counts["downloaded"], counts["download_errors"] = _media_counts(job)
    return counts


def _scan_note(job: _Job) -> str:
    return f"{_fmt(job.scanned)} scanned, {_messages(job.matched)} matched"


async def _ensure_running(step: Any) -> None:
    if step is not None and step.data["state"] == "queued":
        await step.start()


async def _scan_over(job: _Job) -> None:
    if job.scan.data["state"] not in STEP_FINAL:
        await job.scan.finish("success", _scan_note(job))


async def dispatch(frame: dict) -> bool:
    """Route one export_messages_* frame ({type, data}) to its run by job_id.
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
    try:
        if typ == "export_messages_progress":
            await _progress(job, data)
        else:
            await _end(job, data)
    except Exception:
        log.exception("export_messages frame %s for job %s failed", typ, job.job_id)
    return True


async def _progress(job: _Job, data: dict) -> None:
    phase = data.get("phase")
    done = _int(data.get("done")) or 0
    total = _int(data.get("total"))
    ctx = job.ctx
    if phase == "scan":
        await _ensure_running(job.scan)
        job.targets_done = max(job.targets_done, done)
        if total is not None:
            job.targets_total = total
        job.scanned = max(job.scanned, _int(data.get("scanned")) or 0)
        job.matched = max(job.matched, _int(data.get("matched")) or 0)
        where = f" ({data['channel']})" if data.get("channel") else ""
        label = (f"{_fmt(job.targets_done)} of {_fmt(job.targets_total)} channels{where}, "
                 f"{_fmt(job.scanned)} scanned, {_fmt(job.matched)} matched")
        await job.scan.progress(job.targets_done, job.targets_total, label=label)
        await ctx.summary(**_summary(job))
    elif phase == "json":
        job.matched = max(job.matched, _int(data.get("matched")) or 0)
        await _scan_over(job)
        await _ensure_running(job.json_step)
        if data.get("json_path"):
            job.json_path = str(data["json_path"])
            await job.json_step.finish("success", f"{_messages(job.matched)} saved")
            await ctx.log(f"Saved {_messages(job.matched)} to a JSON file", step="json")
        else:
            job.json_error = str(data.get("json_error") or "JSON file not written")
            await job.json_step.finish("failed", job.json_error)
            await ctx.log(f"JSON file not written: {job.json_error}", level="error", step="json")
        await ctx.summary(**_summary(job))
    elif phase == "media":
        await _scan_over(job)
        if job.media is not None:
            await _ensure_running(job.media)
            if isinstance(data.get("media"), dict):
                job.media_report = data["media"]
            if data.get("media_error"):
                await job.media.finish("failed", f"Media not downloaded: {data['media_error']}")
            elif total == 0:
                await job.media.finish("success", "No media matched")
            else:
                await job.media.progress(done, total, label=f"{_fmt(done)} of {_files(total or 0)} downloaded")
            await ctx.summary(**_summary(job))
    elif phase == "forward":
        await _scan_over(job)
        if job.forward is not None:
            await _ensure_running(job.forward)
            job.forwarded = max(job.forwarded, done)
            fwd_total = total if total is not None else job.matched
            await job.forward.progress(job.forwarded, fwd_total,
                                       label=f"{_fmt(job.forwarded)} of {_fmt(fwd_total)} forwarded")
            await ctx.summary(**_summary(job))


async def _end(job: _Job, data: dict) -> None:
    ctx = job.ctx
    job.scanned = max(job.scanned, _int(data.get("scanned")) or 0)
    job.matched = max(job.matched, _int(data.get("matched")) or 0)
    job.forwarded = max(job.forwarded, _int(data.get("forwarded")) or 0)
    if data.get("json_path"):
        job.json_path = str(data["json_path"])
    if isinstance(data.get("media_download"), dict):
        job.media_report = data["media_download"]
    error = data.get("error")
    job.error = error
    if not error:
        await _scan_over(job)
        if job.json_step.data["state"] not in STEP_FINAL:
            if job.json_path:
                await job.json_step.finish("success", f"{_messages(job.matched)} saved")
            else:
                job.json_error = job.json_error or "JSON file not written"
                await job.json_step.finish("failed", job.json_error)
        if job.media is not None and job.media.data["state"] not in STEP_FINAL:
            ok, bad = _media_counts(job)
            note = f"{_files(ok)} downloaded" + (f", {_fmt(bad)} failed" if bad else "")
            await job.media.finish("success", note)
        if job.forward is not None and job.forward.data["state"] not in STEP_FINAL:
            fwd = job.forward
            await fwd.progress(job.forwarded, job.matched)
            if job.forwarded >= job.matched:
                await fwd.finish("success", f"{_messages(job.forwarded)} forwarded")
            else:
                await fwd.finish("failed", f"Stopped after {_fmt(job.forwarded)} of {_fmt(job.matched)}")
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
    await ctx.summary(**_summary(job))
    job.done.set()


def _cancelled_note(job: _Job, step: Any) -> str:
    if step is job.forward and job.forwarded:
        return f"Cancelled after {_fmt(job.forwarded)} forwarded"
    if step is job.scan and job.scanned:
        return f"Cancelled after {_fmt(job.scanned)} scanned"
    return "Cancelled"


def _command(inputs: ExportMessagesInputs, job_id: str) -> dict:
    """The export_messages data, as POST /api/export/messages sends it, plus the job_id."""
    return {
        "guild_id": inputs.guild_id,
        "channel_id": inputs.channel_id,
        "user_id": inputs.user_id,
        "webhook_url": inputs.webhook_url,
        "has_attachments": inputs.has_attachments,
        "after_iso": inputs.after_iso,
        "before_iso": inputs.before_iso,
        "filters": inputs.filters.model_dump(mode="json"),
        "job_id": job_id,
    }


# run

async def run(ctx: Any, inputs: ExportMessagesInputs) -> None:
    job_id = str(ctx.run.id)
    job = _Job(job_id, ctx, inputs, scan=ctx.step("scan", "Scan channels"))
    job.json_step = ctx.step("json", "Save JSON file")
    if inputs.filters.download_media.any():
        job.media = ctx.step("media", "Download media")
    if inputs.webhook_url:
        job.forward = ctx.step("forward", "Forward to webhook")
    _jobs[job_id] = job
    name = _guild_name(inputs.guild_id)
    try:
        await job.scan.start()
        await ctx.log(f"Exporting messages from {name}")
        res = await _send({"type": "export_messages", "data": _command(inputs, job_id)})
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
    summary = _summary(job)
    await ctx.summary(**summary)
    error = None if ctx.cancelled.is_set() and job.error in (None, "cancelled") else job.error
    await ctx.report({
        "inputs": {**inputs.model_dump(mode="json"), "webhook_url": mask_webhook(inputs.webhook_url)},
        "guild": {"id": inputs.guild_id, "name": None if name == inputs.guild_id else name},
        "output": {
            "json_path": job.json_path,
            "media_dir": (str(Path(job.json_path).parent / "media")
                          if job.json_path and job.media is not None and _media_counts(job)[0] else None),
            "webhook": mask_webhook(inputs.webhook_url),
        },
        "media": job.media_report if job.media is not None else None,
        "summary": summary,
        "error": ERRORS.get(str(error), error) if error else None,
    })
    failed = [s for s in job.steps() if s.data["state"] == "failed"]
    if failed:
        ctx.fail(failed[0].data.get("note") or "Export failed")
    elif ctx.cancelled.is_set():
        await ctx.log("Run cancelled", level="warning")
    else:
        where = " and ".join(w for w in ("a JSON file" if job.json_path else "",
                                         "the webhook" if job.forward is not None else "") if w)
        await ctx.log(f"Done: {_messages(job.matched)} exported to {where}")


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
            # The client cancels the runner's task at once; a slow link never holds this up.
            try:
                await asyncio.wait_for(_send({"type": "export_messages_cancel", "data": {
                    "guild_id": int(job.inputs.guild_id), "job_id": job.job_id}}), CANCEL_WAIT)
            except asyncio.TimeoutError:
                pass
            try:
                await asyncio.wait_for(asyncio.shield(done), timeout=CANCEL_WAIT)
            except asyncio.TimeoutError:
                await ctx.log("No end from the client after the cancel", level="warning")
    finally:
        cancel_wait.cancel()
        if not done.done():
            done.cancel()
        await asyncio.gather(done, cancel_wait, return_exceptions=True)


# the export file (behind the /api session guard like every /api path)

_STAMP_RE = re.compile(r"\d{8}-\d{6}")
NO_FILE = "No export file"
GONE = "File no longer on disk"


def _export_file(run_id: str) -> tuple[Optional[Path], Optional[dict], Optional[JSONResponse]]:
    """The run's saved JSON file and what the run page shows about it, or the 404 to answer.
    Only a file under <data dir>/exports/<the run's guild id>/ is served, whatever the report says."""
    svc = get_service()
    rid = _run_id(run_id)
    run = svc.get(rid) if svc is not None and rid is not None else None
    if run is None or run.type != TYPE_KEY:
        return None, None, JSONResponse({"detail": "Unknown run"}, status_code=404)
    report = run.report or {}
    raw = (report.get("output") or {}).get("json_path")
    guild_id = str((run.inputs or {}).get("guild_id") or "")
    if not raw or _wiring.data_dir is None or not guild_id.isdigit():
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    root = (_wiring.data_dir / "exports" / guild_id).resolve()
    try:
        path = Path(str(raw)).resolve()
    except (OSError, ValueError):
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    if not path.is_relative_to(root) or path == root:
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    if not path.is_file():
        return None, None, JSONResponse({"detail": GONE}, status_code=404)
    stamp = path.parent.name if _STAMP_RE.fullmatch(path.parent.name) else str(run.number)
    try:
        size = path.stat().st_size
    except OSError:
        size = None
    messages = _int((report.get("summary") or {}).get("exported"))
    return path, {"filename": f"messages-{guild_id}-{stamp}.json", "size": size, "messages": messages}, None


@router.get("/export-messages/{run_id}")
async def export_messages_file_info(run_id: str):
    _path, info, error = _export_file(run_id)
    return error if error is not None else {"file": info}


@router.get("/export-messages/{run_id}/file")
async def export_messages_file(run_id: str):
    path, info, error = _export_file(run_id)
    if error is not None:
        return error
    return FileResponse(path, media_type="application/json", filename=info["filename"])


EXPORT_MESSAGES = register(TaskType(
    key=TYPE_KEY,
    title="Export messages",
    description="Export a server's messages, filtered, to a JSON file and optionally a webhook.",
    run=run,
    inputs=ExportMessagesInputs,
    describe=describe,
    options=options,
    requires_bots=True,
))
