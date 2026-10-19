# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Export assets: a server's emojis and stickers from the client account, as one .tar.gz.

The run sends export_assets {job_id = run id, guild_id, asset} to the client, whose
AssetExportRunner saves each emoji and sticker and publishes export_assets_progress frames
(kind, done, total, saved, failed) on the admin bus, then one export_assets_done with its counts
and the archive path. This module watches the bus for both and hands them to dispatch().

Steps: Emojis and Stickers, only the chosen ones. A cancel sends export_assets_cancel; the client
stops between assets, still archives what it saved and sends export_assets_done (error "cancelled").
One export per server at a time: a second run for a server with one running fails at once.

The archive's path is in the run report only. GET /api/tasks/export-assets/<run id> tells the run
page whether the file is still there (name, size, counts), and .../file serves it while it exists.

configure(...) wires the client command sender, the account guild list, the DB, the data dir
(the client writes assets/ under it) and the admin bus; whatever is not wired here comes from the
Download avatars wiring, which app.py sets at startup. Tests hand in fakes."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal, Optional

from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from . import avatars
from .context import STEP_FINAL
from .registry import TaskType, register
from .routes import _run_id, get_service, router

log = logging.getLogger("copycord.tasks.export_assets")

TYPE_KEY = "export_assets"
FRAMES = ("export_assets_progress", "export_assets_done")
# How long a cancel waits for the client's export_assets_done: the client drops the download in
# flight at once, but still writes the archive of what it saved.
CANCEL_WAIT = 10.0
KINDS = ("emojis", "stickers")
TITLES = {"emojis": "Emojis", "stickers": "Stickers"}
NOUNS = {"emojis": ("emoji", "emojis"), "stickers": ("sticker", "stickers")}

IN_PROGRESS = "An asset export for this server is already running"
NOT_IN_GUILD = "The client account is not in this server"
ERRORS = {
    "not-in-guild": NOT_IN_GUILD,
    "guild-not-found": NOT_IN_GUILD,
    "asset-export-in-progress": IN_PROGRESS,
}


# wiring

@dataclass
class _Wiring:
    send: Optional[Callable[[dict], Awaitable[Any]]] = None
    user_guilds: Optional[Callable[[str], Awaitable[list]]] = None
    db: Any = None
    data_dir: Optional[Path] = None
    bus: Any = None


_wiring = _Wiring()
_listener: Optional[asyncio.Task] = None
# Server names seen by options(): run titles name their server and inputs check the id against them.
_guild_names: dict[str, str] = {}


def configure(send: Optional[Callable[[dict], Awaitable[Any]]] = None,
              user_guilds: Optional[Callable[[str], Awaitable[list]]] = None,
              db: Any = None, data_dir: Any = None, bus: Any = None) -> None:
    if send is not None:
        _wiring.send = send
    if user_guilds is not None:
        _wiring.user_guilds = user_guilds
    if db is not None:
        _wiring.db = db
    if data_dir is not None:
        _wiring.data_dir = Path(data_dir)
    if bus is not None:
        _wiring.bus = bus


def _wired(name: str) -> Any:
    own = getattr(_wiring, name)
    return own if own is not None else getattr(avatars._wiring, name, None)


def _data_dir() -> Path:
    root = _wired("data_dir")
    if root is None:
        from copycord.common.paths import data_dir
        root = data_dir()
    return Path(root)


async def _send(frame: dict) -> dict:
    send = _wired("send")
    if send is None:
        return {"ok": False, "error": "client sender not configured"}
    try:
        res = await send(frame)
    except Exception as exc:
        return {"ok": False, "error": str(exc) or type(exc).__name__}
    return res if isinstance(res, dict) else {"ok": True}


# inputs

class ExportAssetsInputs(BaseModel):
    guild_id: str = Field(title="Server")
    asset: Literal["both", "emojis", "stickers"] = Field("both", title="Assets")

    @field_validator("guild_id", mode="before")
    @classmethod
    def _gid(cls, value: Any) -> str:
        s = str(value).strip() if value is not None else ""
        if not s.isdigit():
            raise ValueError("Pick a server")
        # Checked against the list the form loaded; without one the client checks it.
        if _guild_names and s not in _guild_names:
            raise ValueError(NOT_IN_GUILD)
        return s

    @field_validator("asset", mode="before")
    @classmethod
    def _asset(cls, value: Any) -> str:
        s = str(value if value is not None else "both").strip().lower() or "both"
        if s not in ("both", *KINDS):
            raise ValueError("Pick emojis, stickers or both")
        return s

    def kinds(self) -> tuple[str, ...]:
        return KINDS if self.asset == "both" else (self.asset,)


def _guild_name(inputs: ExportAssetsInputs) -> str:
    return _guild_names.get(inputs.guild_id) or inputs.guild_id


def describe(inputs: ExportAssetsInputs, lookups: Any = None) -> tuple[str, dict]:
    name = _guild_name(inputs)
    return f"Export assets from {name}", {"label": name, "guild_id": inputs.guild_id}


# options: the client account's servers

def _client_token() -> str:
    db = _wired("db")
    try:
        return (db.get_config("CLIENT_TOKEN", "") or "").strip() if db is not None else ""
    except Exception:
        return ""


async def options(params: dict) -> dict:
    """The servers the client account is in, by name: {guilds: [{id, name, icon?}]} or {guilds: [], error}."""
    user_guilds = _wired("user_guilds")
    if user_guilds is None:
        return {"guilds": [], "error": "Server list not configured"}
    token = _client_token()
    if not token:
        return {"guilds": [], "error": "Set the client account token first."}
    try:
        raw = await user_guilds(token)
    except Exception as exc:
        return {"guilds": [], "error": avatars._guilds_error(exc)}
    guilds = []
    for g in raw or []:
        gid = str(g.get("id") or "").strip()
        if not gid.isdigit():
            continue
        name = str(g.get("name") or gid)
        _guild_names[gid] = name
        entry = {"id": gid, "name": name}
        if g.get("icon"):
            entry["icon"] = str(g["icon"])
        guilds.append(entry)
    guilds.sort(key=lambda g: g["name"].casefold())
    return {"guilds": guilds}


# job routing

@dataclass
class _Kind:
    step: Any
    saved: int = 0
    failed: int = 0
    done: int = 0
    total: Optional[int] = None


@dataclass
class _Job:
    job_id: str
    ctx: Any
    inputs: ExportAssetsInputs
    kinds: dict[str, _Kind]
    archive: Optional[str] = None
    error: Optional[str] = None
    ended: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event)


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


def _count(kind: str, n: int) -> str:
    one, many = NOUNS[kind]
    return f"{_fmt(n)} {one if n == 1 else many}"


def _saved_words(kind: str, k: _Kind) -> str:
    """"12 of 15 saved"; "No emojis in this server" when there were none."""
    if not k.total:
        return f"No {NOUNS[kind][1]} in this server"
    return f"{_fmt(k.saved)} of {_fmt(k.total)} saved"


def _summary(job: _Job) -> dict:
    out: dict = {}
    for kind, k in job.kinds.items():
        out[f"{kind}_saved"] = k.saved
        out[f"{kind}_total"] = k.total or 0
    return out


async def _finish_kind(kind: str, k: _Kind) -> None:
    """A kind whose assets are all through: success, or failed when none of them could be saved."""
    if k.step.data["state"] in STEP_FINAL:
        return
    if k.step.data["state"] == "queued":
        await k.step.start()
    await k.step.progress(k.done, k.total or 0, label=None)
    if k.total and not k.saved:
        await k.step.finish("failed", f"None of the {_count(kind, k.total)} could be saved")
    else:
        await k.step.finish("success", _saved_words(kind, k))


async def dispatch(frame: dict) -> bool:
    """Route one export_assets_* frame ({type, data}) to its run by job_id.
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
        if typ == "export_assets_progress":
            await _progress(job, data)
        else:
            await _end(job, data)
    except Exception:
        log.exception("export_assets frame %s for job %s failed", typ, job.job_id)
    return True


async def _progress(job: _Job, data: dict) -> None:
    kind = data.get("kind")
    k = job.kinds.get(str(kind))
    if k is None or k.step.data["state"] in STEP_FINAL:
        return
    # Kinds run in order: the one before this is through.
    for other, ok in job.kinds.items():
        if other == kind:
            break
        await _finish_kind(other, ok)
    if k.step.data["state"] == "queued":
        await k.step.start()
    k.done = max(k.done, _int(data.get("done")) or 0)
    k.saved = max(k.saved, _int(data.get("saved")) or 0)
    k.failed = max(k.failed, _int(data.get("failed")) or 0)
    if _int(data.get("total")) is not None:
        k.total = _int(data.get("total"))
    await k.step.progress(k.done, k.total, label=f"{_fmt(k.saved)} of {_fmt(k.total)} saved")
    await job.ctx.summary(**_summary(job))
    if k.total is not None and k.done >= k.total:
        await _finish_kind(str(kind), k)


async def _end(job: _Job, data: dict) -> None:
    ctx = job.ctx
    for kind, k in job.kinds.items():
        k.saved = max(k.saved, _int(data.get(f"saved_{kind}")) or 0)
        k.failed = max(k.failed, _int(data.get(f"failed_{kind}")) or 0)
        total = _int(data.get(f"total_{kind}"))
        if total is not None:
            k.total = total
    if data.get("archive"):
        job.archive = str(data["archive"])
    error = data.get("error")
    job.error = error
    job.ended = True
    if not error:
        for kind, k in job.kinds.items():
            k.done = max(k.done, k.saved + k.failed)
            await _finish_kind(kind, k)
    elif error == "cancelled" or ctx.cancelled.is_set():
        for kind, k in job.kinds.items():
            if k.step.data["state"] not in STEP_FINAL:
                await k.step.finish("cancelled", _cancelled_note(kind, k))
    else:
        reason = ERRORS.get(str(error), str(error))
        failed_one = False
        for k in job.kinds.values():
            if k.step.data["state"] in STEP_FINAL:
                continue
            if not failed_one:
                await k.step.finish("failed", reason)
                failed_one = True
            else:
                await k.step.finish("skipped", "Not reached")
        await ctx.log(f"Export failed: {reason}", level="error")
    await ctx.summary(**_summary(job))
    job.done.set()


def _cancelled_note(kind: str, k: _Kind) -> str:
    if k.step.data["state"] == "running" and k.total:
        return f"Cancelled after {_fmt(k.saved)} of {_fmt(k.total)} saved"
    return "Cancelled"


# the bus

def _ensure_listener() -> None:
    """Watch the admin bus for this module's frames; started by the first run."""
    global _listener
    bus = _wired("bus")
    if bus is None or (_listener is not None and not _listener.done()):
        return
    _listener = asyncio.ensure_future(_listen(bus))


async def _listen(bus: Any) -> None:
    q = bus.subscribe()
    try:
        while True:
            raw = await q.get()
            try:
                ev = json.loads(raw)
            except Exception:
                continue
            if not isinstance(ev, dict) or ev.get("kind") != "client":
                continue
            payload = ev.get("payload") or {}
            if isinstance(payload, dict) and payload.get("type") in FRAMES:
                await dispatch(payload)
    finally:
        try:
            bus.unsubscribe(q)
        except Exception:
            pass


# run

async def run(ctx: Any, inputs: ExportAssetsInputs) -> None:
    _ensure_listener()
    job_id = str(ctx.run.id)
    name = _guild_name(inputs)
    job = _Job(job_id, ctx, inputs, {kind: _Kind(ctx.step(kind, TITLES[kind])) for kind in inputs.kinds()})
    first = next(iter(job.kinds.values()))
    busy = any(j.inputs.guild_id == inputs.guild_id for j in _jobs.values())
    _jobs[job_id] = job
    try:
        await first.step.start()
        if busy:
            await _end(job, {"error": "asset-export-in-progress"})
        else:
            await ctx.log(f"Exporting {_what(inputs)} from {name}")
            res = await _send({"type": "export_assets", "data": {
                "job_id": job_id, "guild_id": int(inputs.guild_id), "asset": inputs.asset,
            }})
            if not res.get("ok", True) and not job.done.is_set():
                await _end(job, {"error": res.get("error") or "Client did not accept the export"})
            else:
                await _wait(ctx, job)
    finally:
        _jobs.pop(job_id, None)

    for kind, k in job.kinds.items():
        if k.step.data["state"] not in STEP_FINAL:
            if ctx.cancelled.is_set():
                await k.step.finish("cancelled", ctx.cancel_note or _cancelled_note(kind, k))
            else:
                await k.step.finish("failed", "No end from the client")
    summary = _summary(job)
    await ctx.summary(**summary)
    error = None if ctx.cancelled.is_set() and job.error in (None, "cancelled") else job.error
    if error is None and job.ended and not job.archive and not ctx.cancelled.is_set():
        error = "The archive could not be written"
    await ctx.report({
        "inputs": inputs.model_dump(mode="json"),
        "guild": {"guild_id": inputs.guild_id, "name": name},
        "output": {"archive_path": job.archive},
        "failed": {kind: k.failed for kind, k in job.kinds.items()},
        "summary": summary,
        "error": ERRORS.get(str(error), error) if error else None,
    })
    failed = [k.step for k in job.kinds.values() if k.step.data["state"] == "failed"]
    if failed:
        ctx.fail(failed[0].data.get("note") or "Export failed")
    elif error:
        ctx.fail(ERRORS.get(str(error), error))
    elif ctx.cancelled.is_set():
        if job.archive:
            await ctx.log(f"Saved {_saved_list(job)} before the cancel to {Path(job.archive).name}")
        await ctx.log("Run cancelled", level="warning")
    else:
        await ctx.log(f"Done: {_saved_list(job)} saved to {Path(job.archive).name}")


def _what(inputs: ExportAssetsInputs) -> str:
    return "emojis and stickers" if inputs.asset == "both" else inputs.asset


def _saved_list(job: _Job) -> str:
    return " and ".join(_count(kind, k.saved) for kind, k in job.kinds.items())


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
            try:
                await asyncio.wait_for(_send({"type": "export_assets_cancel",
                                              "data": {"job_id": job.job_id}}), CANCEL_WAIT)
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


# the archive (behind the /api session guard like every /api path)

NO_FILE = "No export file"
GONE = "File no longer on disk"


def _archive(run_id: str) -> tuple[Optional[Path], Optional[dict], Optional[JSONResponse]]:
    """The run's archive and what the run page shows about it, or the 404 to answer.
    Only a .tar.gz under <data dir>/assets/ is served, whatever the report says."""
    svc = get_service()
    rid = _run_id(run_id)
    run = svc.get(rid) if svc is not None and rid is not None else None
    if run is None or run.type != TYPE_KEY:
        return None, None, JSONResponse({"detail": "Unknown run"}, status_code=404)
    report = run.report or {}
    raw = (report.get("output") or {}).get("archive_path")
    if not raw:
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    root = (_data_dir() / "assets").resolve()
    try:
        path = Path(str(raw)).resolve()
    except (OSError, ValueError):
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    if not path.is_relative_to(root) or path == root or not path.name.endswith(".tar.gz"):
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    if not path.is_file():
        return None, None, JSONResponse({"detail": GONE}, status_code=404)
    try:
        size = path.stat().st_size
    except OSError:
        size = None
    summary = report.get("summary") or {}
    return path, {"filename": path.name, "size": size,
                  "emojis": _int(summary.get("emojis_saved")),
                  "stickers": _int(summary.get("stickers_saved"))}, None


@router.get("/export-assets/{run_id}")
async def export_assets_file_info(run_id: str):
    _path, info, error = _archive(run_id)
    return error if error is not None else {"file": info}


@router.get("/export-assets/{run_id}/file")
async def export_assets_file(run_id: str):
    path, info, error = _archive(run_id)
    if error is not None:
        return error
    return FileResponse(path, media_type="application/gzip", filename=info["filename"])


EXPORT_ASSETS = register(TaskType(
    key=TYPE_KEY,
    title="Export assets",
    description="Save a server's emojis and stickers from the client account to a .tar.gz archive.",
    run=run,
    inputs=ExportAssetsInputs,
    describe=describe,
    options=options,
    requires_bots=True,
))
