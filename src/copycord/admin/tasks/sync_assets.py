# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Sync assets: bring a mapping's cloned roles, emojis and stickers in line with the host.

One step per kind the user picked (Roles, Emojis, Stickers) and, with roles, a Role order
step that puts the cloned roles in the host's order. The server bot does the work
(server/asset_sync.py): the run sends it sync_assets with job_id = "<run_id>:assets", and
the admin bus watcher hands the job's frames (sync_assets_step, sync_assets_progress,
sync_assets_log, sync_assets_end) to dispatch(), which routes them by job_id to the run.
Each asset moves its step's bar with the asset's name as the label; a cancel asks the
server to stop before its next asset.

configure(db=..., send=...) wires the DB and the server command sender (an async callable
taking {type, data} and returning a dict); app.py does it at startup and tests hand in fakes."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .context import STEP_FINAL
from .registry import TaskType, register

log = logging.getLogger("copycord.tasks.sync_assets")

TYPE_KEY = "sync_assets"
KINDS = ("roles", "emojis", "stickers")
ROLE_ORDER = "role_order"
TITLES = {"roles": "Roles", ROLE_ORDER: "Role order", "emojis": "Emojis", "stickers": "Stickers"}
FRAMES = ("sync_assets_step", "sync_assets_progress", "sync_assets_log", "sync_assets_end")
SUMMARY_KEYS = ("created", "updated", "deleted", "reordered", "skipped", "failed")
# How long a cancelled job may take to stop at its next asset and send its end.
CANCEL_WAIT = 30.0


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
        return {"ok": False, "error": "server sender not configured"}
    try:
        res = await _wiring.send(frame)
    except Exception as exc:
        return {"ok": False, "error": str(exc) or type(exc).__name__}
    return res if isinstance(res, dict) else {"ok": True}


# inputs

class SyncAssetsInputs(BaseModel):
    mapping_id: str = Field(min_length=1)
    kinds: list[Literal["roles", "emojis", "stickers"]] = Field(default_factory=lambda: list(KINDS))
    delete_extras: bool = False

    @field_validator("kinds")
    @classmethod
    def _kinds(cls, value: list[str]) -> list[str]:
        picked = [k for k in KINDS if k in set(value or [])]
        if not picked:
            raise ValueError("Pick at least one kind of asset")
        return picked


def _db(lookups: Any = None) -> Any:
    return lookups if lookups is not None and hasattr(lookups, "get_mapping_by_id") else _wiring.db


def _mapping(db: Any, mapping_id: str) -> Optional[dict]:
    try:
        row = db.get_mapping_by_id(mapping_id) if db is not None else None
    except Exception:
        row = None
    return dict(row) if row else None


def _kinds_words(kinds: list[str]) -> str:
    words = list(kinds)
    if len(words) == 1:
        return words[0]
    return ", ".join(words[:-1]) + " and " + words[-1]


def describe(inputs: SyncAssetsInputs, lookups: Any = None) -> tuple[str, dict]:
    mapping = _mapping(_db(lookups), inputs.mapping_id)
    context = {"label": inputs.mapping_id, "mapping_id": inputs.mapping_id, "guild_id": None}
    if mapping:
        context.update(label=mapping.get("mapping_name") or inputs.mapping_id,
                       guild_id=str(mapping.get("original_guild_id") or "") or None)
    return f"Sync {_kinds_words(inputs.kinds)}", context


def step_keys(kinds: list[str]) -> list[str]:
    """The run's steps in the order the server works them: Role order right after Roles."""
    out: list[str] = []
    for k in KINDS:
        if k in kinds:
            out.append(k)
            if k == "roles":
                out.append(ROLE_ORDER)
    return out


# job routing

@dataclass
class _Job:
    job_id: str
    ctx: Any
    totals: dict = field(default_factory=dict)
    end: Optional[dict] = None
    done: asyncio.Event = field(default_factory=asyncio.Event)


_jobs: dict[str, _Job] = {}


def job_id_for(run_id: Any) -> str:
    return f"{run_id}:assets"


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def summary_of(totals: dict) -> dict:
    return {k: int((totals or {}).get(k) or 0) for k in SUMMARY_KEYS}


async def dispatch(frame: dict) -> bool:
    """Route one sync_assets_* frame ({type, data}) to its run by job_id.
    Returns False for frames this module does not own."""
    typ = frame.get("type")
    data = frame.get("data") or {}
    if typ not in FRAMES:
        return False
    job = _jobs.get(str(data.get("job_id") or ""))
    if job is None:
        log.debug("sync assets frame %s for unknown job %r ignored", typ, data.get("job_id"))
        return False
    ctx = job.ctx
    try:
        if typ == "sync_assets_end":
            job.end = data
            if isinstance(data.get("totals"), dict):
                job.totals = data["totals"]
            job.done.set()
            return True
        key = str(data.get("step") or "")
        if typ == "sync_assets_log":
            level = str(data.get("level") or "info")
            await ctx.log(data.get("msg") or "", level=level, step=key or None)
            return True
        if key not in TITLES:
            return True
        step = ctx.step(key, TITLES[key])
        if step.data["state"] in STEP_FINAL:
            return True
        if typ == "sync_assets_step":
            state = data.get("state")
            if state == "running":
                if step.data["state"] != "running":
                    await step.start()
                await step.state(data.get("label"))
            elif state in STEP_FINAL:
                done, total = _int(data.get("done")), _int(data.get("total"))
                if done is not None:
                    await step.progress(done, total)
                await step.finish(state, data.get("note"))
        elif typ == "sync_assets_progress":
            if step.data["state"] != "running":
                await step.start()
            await step.progress(_int(data.get("done")) or 0, _int(data.get("total")),
                                label=data.get("label"))
            if isinstance(data.get("totals"), dict) and data["totals"]:
                job.totals = data["totals"]
                await ctx.summary(**summary_of(job.totals))
    except Exception:
        log.exception("sync assets frame %s for job %s failed", typ, job.job_id)
    return True


# options

async def options(params: dict) -> dict:
    return {"kinds": [{"key": k, "title": TITLES[k]} for k in KINDS]}


# run

async def _wait(ctx: Any, job: _Job) -> None:
    cancel_wait = asyncio.ensure_future(ctx.cancelled.wait())
    end_wait = asyncio.ensure_future(job.done.wait())
    try:
        await asyncio.wait({end_wait, cancel_wait}, return_when=asyncio.FIRST_COMPLETED)
        if end_wait.done():
            return
        # Bots stopped (ctx.cancel_note): no server is left to tell or to send an end.
        if ctx.cancel_note is None:
            await ctx.log("Cancelling: the server stops before its next asset", level="warning")
            await _send({"type": "sync_assets_cancel", "data": {"job_id": job.job_id}})
            try:
                await asyncio.wait_for(asyncio.shield(end_wait), timeout=CANCEL_WAIT)
            except asyncio.TimeoutError:
                await ctx.log("No end from the server after the cancel", level="warning")
    finally:
        for fut in (cancel_wait, end_wait):
            if not fut.done():
                fut.cancel()
        await asyncio.gather(cancel_wait, end_wait, return_exceptions=True)


def _failures(result: dict) -> list[dict]:
    """Every asset that failed or was skipped, and every pass that failed as a whole."""
    out: list[dict] = []
    for kind, counts in ((result or {}).get("kinds") or {}).items():
        if counts.get("error"):
            out.append({"kind": kind, "name": None, "outcome": "failed", "note": counts["error"]})
        for it in counts.get("items") or []:
            out.append({"kind": kind, "name": it.get("name"), "outcome": it.get("outcome"),
                        "note": it.get("note")})
    order = (result or {}).get("role_order") or {}
    if order.get("error"):
        out.append({"kind": ROLE_ORDER, "name": None, "outcome": "failed", "note": order["error"]})
    return out


async def run(ctx: Any, inputs: SyncAssetsInputs) -> None:
    mapping = _mapping(_wiring.db, inputs.mapping_id)
    if not mapping:
        raise RuntimeError(f"Unknown mapping {inputs.mapping_id}")
    og, cg = mapping.get("original_guild_id"), mapping.get("cloned_guild_id")
    name = mapping.get("mapping_name") or inputs.mapping_id
    keys = step_keys(inputs.kinds)
    for key in keys:
        ctx.step(key, TITLES[key])
    await ctx.log(f"Syncing {_kinds_words(inputs.kinds)} in {name}")

    job = _Job(job_id_for(ctx.run.id), ctx)
    _jobs[job.job_id] = job
    error: Optional[str] = None
    try:
        res = await _send({"type": "sync_assets", "data": {
            "job_id": job.job_id, "kinds": list(inputs.kinds),
            "delete_extras": inputs.delete_extras, "mapping_id": str(inputs.mapping_id),
            "original_guild_id": str(og) if og is not None else None,
            "cloned_guild_id": str(cg) if cg is not None else None,
        }})
        if not res.get("ok", True):
            error = res.get("error") or "The server bot did not accept the job"
        else:
            await _wait(ctx, job)
    finally:
        _jobs.pop(job.job_id, None)

    end = job.end or {}
    error = error or end.get("error")
    cancelled = ctx.cancelled.is_set() or bool(end.get("cancelled"))
    result = end.get("result") or {"kinds": {}, "role_order": None}
    for key in keys:
        step = ctx.step(key)
        if step.data["state"] in STEP_FINAL:
            continue
        if cancelled:
            await step.finish("cancelled", ctx.cancel_note or "Cancelled")
        else:
            await step.finish("failed", error or "No end from the server")

    summary = summary_of(job.totals)
    await ctx.summary(**summary)
    order = result.get("role_order") or {}
    failures = _failures(result)
    await ctx.report({
        "inputs": inputs.model_dump(mode="json"),
        "mapping": {"mapping_id": str(inputs.mapping_id), "name": mapping.get("mapping_name"),
                    "original_guild_id": str(og) if og is not None else None,
                    "cloned_guild_id": str(cg) if cg is not None else None},
        "kinds": result.get("kinds") or {},
        "role_order": order or None,
        "failures": failures,
        "unmovable": order.get("unmovable") or [],
        "summary": summary,
        "error": error,
    })
    failed_steps = [k for k in keys if ctx.step(k).data["state"] == "failed"]
    if error:
        await ctx.log(f"Sync failed: {error}", level="error")
        ctx.fail(error)
    elif failed_steps:
        msg = f"{len(failed_steps)} of {len(keys)} steps failed"
        await ctx.log(msg, level="error")
        ctx.fail(msg)
    elif cancelled:
        await ctx.log("Run cancelled", level="warning")
    else:
        await ctx.log(f"Done: {summary['created']} created, {summary['updated']} updated, "
                      f"{summary['reordered']} reordered, {summary['skipped']} skipped")


SYNC_ASSETS = register(TaskType(
    key=TYPE_KEY,
    title="Sync assets",
    description="Bring a mapping's cloned roles, emojis and stickers in line with the host, "
                "with the roles in the host's order.",
    run=run,
    inputs=SyncAssetsInputs,
    describe=describe,
    options=options,
    requires_bots=True,
))
