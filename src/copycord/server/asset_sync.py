# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Sync assets: the server half of the admin's Sync assets task (admin/tasks/sync_assets.py).

sync_assets {job_id, original_guild_id, cloned_guild_id, kinds, delete_extras} starts a job
and answers at once. The job asks the client for the host's roles, emojis and stickers, runs
the regular role, emoji and sticker passes (RoleManager._sync, EmojiManager._sync,
StickerManager._sync) with per item hooks, and after the roles puts the cloned roles in the
host's order with one edit_role_positions call. It streams frames on the admin bus:

  sync_assets_step     {job_id, step, state, label, note, done, total}  a step starts or ends
  sync_assets_progress {job_id, step, done, total, label, totals}       assets done so far
  sync_assets_log      {job_id, level, msg, step}
  sync_assets_end      {job_id, ok, error, cancelled, result, totals}

sync_assets_cancel {job_id} stops the job at once: the asset in flight (its download, rate limit
wait or Discord call) is cancelled with the job's task and never reported done, and the end
goes out with cancelled=true."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, Optional

import discord

from copycord.server.item_hooks import OUTCOMES, ItemHooks
from copycord.server.roles import (RoleManager, above_top_note, my_role_ids, plan_role_order,
                                   plan_tie_report, rank_role_facts)

logger = logging.getLogger("server.asset_sync")

KINDS = ("roles", "emojis", "stickers")
ROLE_ORDER = "role_order"
# The client builds the host's assets from Discord; a big server takes a while.
SNAPSHOT_TIMEOUT = 120.0
# Seconds between progress frames of one step; the last asset of a step always goes out.
PROGRESS_INTERVAL = 0.25
# Kept per kind and named in the report when an asset ends this way.
LISTED = ("failed", "skipped")


def kind_note(counts: dict) -> str:
    """"2 created, 1 updated, 40 unchanged"; "Nothing to sync" for an empty pass."""
    parts = [f"{counts[k]} {k}" for k in ("created", "updated", "deleted", "skipped", "failed", "unchanged")
             if counts.get(k)]
    return ", ".join(parts) or "Nothing to sync"


def order_note(moved: int, unmovable: int) -> str:
    """"Moved 4 roles, 1 could not move"; "Already in order"."""
    text = (f"Moved {moved} role{'s' if moved != 1 else ''}" if moved else "Already in order")
    if unmovable:
        text += f", {unmovable} could not move"
    return text


class _Job:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.cancelled = False
        # Set with cancelled: rate limit waits wake on it.
        self.stop = asyncio.Event()
        self.started = False
        self.task: Optional[asyncio.Task] = None
        self.last_progress: dict[str, float] = {}


class _StepHooks(ItemHooks):
    """Counts one kind's assets, lists the failed and skipped ones and streams progress."""

    def __init__(self, sync: "AssetSync", job: _Job, step: str, counts: dict, totals: dict):
        self.sync, self.job, self.step = sync, job, step
        self.counts, self.totals = counts, totals
        self.total = 0
        self.done = 0
        self.stop_event = job.stop

    async def begin(self, total: int) -> None:
        self.total = int(total)
        await self.sync._progress(self.job, self.step, 0, self.total, None, self.totals, force=True)

    async def item(self, name: str, outcome: str, note: Optional[str] = None) -> None:
        if outcome not in OUTCOMES:
            outcome = "failed"
        self.done += 1
        self.counts[outcome] = self.counts.get(outcome, 0) + 1
        if outcome in self.totals:
            self.totals[outcome] += 1
        if outcome in LISTED:
            self.counts["items"].append({"name": str(name), "outcome": outcome, "note": note})
        self.total = max(self.total, self.done)
        await self.sync._progress(self.job, self.step, self.done, self.total, str(name), self.totals)

    def stop(self) -> bool:
        return self.job.cancelled


class AssetSync:
    def __init__(self, server: Any):
        self.server = server
        self._jobs: dict[str, _Job] = {}

    # ws commands

    async def start(self, data: dict) -> dict:
        job_id = str(data.get("job_id") or "")
        if not job_id:
            return {"ok": False, "error": "job_id missing"}
        if job_id in self._jobs:
            return {"ok": True, "job_id": job_id, "state": "running"}
        try:
            host_id = int(data.get("original_guild_id"))
            clone_id = int(data.get("cloned_guild_id"))
        except (TypeError, ValueError):
            return {"ok": False, "error": "The mapping has no host or clone server id"}
        wanted = {str(k) for k in (data.get("kinds") or [])}
        kinds = [k for k in KINDS if k in wanted]
        if not kinds:
            return {"ok": False, "error": "No asset kinds to sync"}
        guild = self.server.bot.get_guild(clone_id)
        if guild is None:
            return {"ok": False, "error": "The server bot is not in the clone server"}
        job = _Job(job_id)
        self._jobs[job_id] = job
        job.task = asyncio.create_task(
            self._run(job, guild, host_id, kinds, bool(data.get("delete_extras")))
        )
        return {"ok": True, "job_id": job_id, "state": "accepted"}

    def cancel(self, job_id: str) -> dict:
        job = self._jobs.get(job_id)
        if job is None:
            return {"ok": True, "job_id": job_id, "state": "unknown"}
        job.cancelled = True
        job.stop.set()
        # The asset in flight stops too (its download, rate limit wait or Discord call), not
        # only the next one; _run still sends the end, cancelled.
        # A task not yet started would never enter _run's try: it sees cancelled at its top.
        if job.started and job.task is not None and not job.task.done():
            job.task.cancel()
        logger.info("[sync assets] Cancelling job %s", job_id)
        return {"ok": True, "job_id": job_id, "state": "cancelling"}

    # frames

    async def _publish(self, typ: str, **data: Any) -> None:
        # Job frames go out as kind "client", as the server's backfill_job_delivered does;
        # the admin's job listener reads that kind.
        with contextlib.suppress(Exception):
            await self.server.bus.publish("client", {"type": typ, "data": data})

    async def _step(self, job: _Job, step: str, state: str, **fields: Any) -> None:
        await self._publish("sync_assets_step", job_id=job.job_id, step=step, state=state, **fields)

    async def _log(self, job: _Job, msg: str, level: str = "info", step: Optional[str] = None) -> None:
        await self._publish("sync_assets_log", job_id=job.job_id, level=level, msg=msg, step=step)

    async def _progress(self, job: _Job, step: str, done: int, total: int, label: Optional[str],
                        totals: Optional[dict] = None, force: bool = False) -> None:
        now = asyncio.get_running_loop().time()
        last = job.last_progress.get(step)
        if not force and done < total and last is not None and now - last < PROGRESS_INTERVAL:
            return
        job.last_progress[step] = now
        await self._publish("sync_assets_progress", job_id=job.job_id, step=step, done=done,
                            total=total, label=label, totals=dict(totals or {}))

    # the job

    async def _run(self, job: _Job, guild: discord.Guild, host_id: int, kinds: list[str],
                   delete_extras: bool) -> None:
        result: dict = {"kinds": {}, "role_order": None}
        totals = {k: 0 for k in OUTCOMES}
        totals["reordered"] = 0
        error = None
        job.started = True
        try:
            if job.cancelled:
                raise asyncio.CancelledError()
            snap = await self._snapshot(host_id, guild.id)
            for kind in kinds:
                if job.cancelled:
                    break
                await self._run_kind(job, kind, guild, host_id, snap, delete_extras, result, totals)
                if kind == "roles" and not job.cancelled:
                    await self._run_role_order(job, guild, host_id, snap, result, totals)
        except asyncio.CancelledError:
            # A cancel from the admin ends here and still sends the end; the admin marks the
            # steps not yet ended cancelled. The asset in flight was never reported done.
            if not job.cancelled:
                self._jobs.pop(job.job_id, None)
                raise
        except Exception as exc:
            error = str(exc) or type(exc).__name__
            logger.warning("[sync assets] Job %s failed: %s", job.job_id, error)
        finally:
            self._jobs.pop(job.job_id, None)
        await self._publish("sync_assets_end", job_id=job.job_id, ok=error is None and not job.cancelled,
                            error=error, cancelled=job.cancelled, result=result, totals=totals)

    async def _snapshot(self, host_id: int, clone_id: int) -> dict:
        res = await self.server.bot.ws_manager.request(
            {"type": "asset_snapshot",
             "data": {"guild_id": str(host_id), "cloned_guild_id": str(clone_id)}},
            timeout=SNAPSHOT_TIMEOUT,
        )
        if not res or not res.get("ok"):
            raise RuntimeError((res or {}).get("error") or "The client sent no assets for the host server")
        return res

    async def _run_kind(self, job: _Job, kind: str, guild: discord.Guild, host_id: int, snap: dict,
                        delete_extras: bool, result: dict, totals: dict) -> None:
        manager = {"roles": self.server.roles, "emojis": self.server.emojis,
                   "stickers": self.server.stickers}[kind]
        counts: dict = {k: 0 for k in OUTCOMES}
        counts.update(items=[], error=None)
        result["kinds"][kind] = counts
        hooks = _StepHooks(self, job, kind, counts, totals)
        lock = manager._get_lock_for_clone(guild.id)
        # The regular sync of this clone holds the lock while it runs; the step waits its turn.
        await self._step(job, kind, "running", label="Waiting for the running sync" if lock.locked() else None)
        async with lock:
            if job.cancelled:
                await self._step(job, kind, "cancelled", note="Cancelled")
                return
            try:
                if kind == "roles":
                    features = set(getattr(guild, "features", []) or [])
                    await manager._sync(
                        guild=guild, incoming=snap.get("roles") or [], host_id=host_id,
                        clone_id=guild.id, delete_roles=delete_extras, mirror_permissions=True,
                        update_roles=True, rearrange_roles=False,
                        clone_role_icons="ROLE_ICONS" in features,
                        clone_role_styles="ENHANCED_ROLE_COLORS" in features, hooks=hooks,
                    )
                elif kind == "emojis":
                    await manager._sync(guild, snap.get("emojis") or [], host_id,
                                        hooks=hooks, delete_removed=delete_extras)
                else:
                    stickers = snap.get("stickers") or []
                    manager.set_last_sitemap(guild.id, stickers, host_id)
                    await manager._sync(guild=guild, stickers=stickers, host_id=host_id,
                                        hooks=hooks, delete_removed=delete_extras)
                    await manager.refresh_cache(guild.id)
            except asyncio.CancelledError:
                # The job's cancel landed mid-pass: the asset in flight is dropped, never
                # counted, and the step ends cancelled below with what was done.
                if not job.cancelled:
                    raise
            except Exception as exc:
                counts["error"] = str(exc) or type(exc).__name__
                await self._step(job, kind, "failed", note=counts["error"],
                                 done=hooks.done, total=hooks.total)
                await self._log(job, f"{kind.capitalize()} failed: {counts['error']}", "error", step=kind)
                return
        await self._progress(job, kind, hooks.done, hooks.total, None, totals, force=True)
        state = "cancelled" if job.cancelled else ("failed" if counts["failed"] else "success")
        note = kind_note(counts) + (", then cancelled" if job.cancelled else "")
        await self._step(job, kind, state, note=note, done=hooks.done, total=hooks.total)
        for it in counts["items"]:
            level = "error" if it["outcome"] == "failed" else "warning"
            await self._log(job, f"{it['name']}: {it['outcome']}"
                                 + (f", {it['note']}" if it.get("note") else ""), level, step=kind)

    async def _run_role_order(self, job: _Job, guild: discord.Guild, host_id: int, snap: dict,
                              result: dict, totals: dict) -> None:
        order: dict = {"moved": 0, "unmovable": [], "error": None}
        result["role_order"] = order
        tie_note: Optional[str] = None
        roles = self.server.roles
        await self._step(job, ROLE_ORDER, "running", label="Reading the clone's roles")
        try:
            async with roles._get_lock_for_clone(guild.id):
                fetched = await guild.fetch_roles()
                me = guild.me
                top = me.top_role if me else None
                if top is None:
                    raise RuntimeError("The server bot has no role in the clone server")
                facts = [RoleManager.role_facts(r) for r in fetched]
                ranked = rank_role_facts(facts, my_role_ids(me)) or (facts, top.position, top.id)
                plan = plan_role_order(snap.get("roles") or [], roles.mapped_roles(host_id, guild.id),
                                       *ranked)
                order["unmovable"] = plan["unmovable"]
                ties = plan_tie_report(ranked[0], ranked[2], plan["unmovable"])
                if ties:
                    order["bot_top"] = ties["bot_top"]
                    order["tied_with_bot"] = ties["tied_with_bot"]
                    if ties["skipped"]:
                        tie_note = above_top_note(ties)
                total = len(plan["order"])
                await self._progress(job, ROLE_ORDER, 0, total, "Checking the order", force=True)
                if plan["changed"]:
                    await self._progress(job, ROLE_ORDER, 0, total, f"Moving {plan['moved']} roles", force=True)
                    if job.cancelled:
                        raise asyncio.CancelledError()
                    await guild.edit_role_positions(
                        positions={discord.Object(id=rid): pos for rid, pos in plan["positions"].items()},
                        reason="Copycord Sync assets: role order",
                    )
                    order["moved"] = plan["moved"]
                    totals["reordered"] += plan["moved"]
                await self._progress(job, ROLE_ORDER, total, total, None, totals, force=True)
        except Exception as exc:
            order["error"] = str(exc) or type(exc).__name__
            if isinstance(exc, discord.Forbidden):
                order["error"] = "Missing the Manage Roles permission to move roles"
            await self._step(job, ROLE_ORDER, "failed", note=order["error"])
            await self._log(job, f"Role order failed: {order['error']}", "error", step=ROLE_ORDER)
            return
        for u in order["unmovable"]:
            await self._log(job, f"Could not move {u['name']}: {u['reason']}", "warning", step=ROLE_ORDER)
        if tie_note:
            logger.warning("[⚠️] Sync assets role order: %s", tie_note)
            await self._log(job, tie_note,"warning", step=ROLE_ORDER)
        await self._step(job, ROLE_ORDER, "success", note=order_note(order["moved"], len(order["unmovable"])),
                         done=total, total=total)
