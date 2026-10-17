# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Purge assets: delete emojis, stickers and roles from a mapping's clone guild.

The server bot holds the clone guild, so the run asks it over the server agent:
purge_assets_list once for what the guild holds (each asset says whether Copycord
cloned it), then purge_assets_delete for one asset at a time. One step per asset
kind; every delete moves its step on, a cancel stops before the next asset, and a
Discord rate limit waits its Retry-After before trying again. The scope picks which
assets go: cloned (mapped in the DB, the default), unmapped, or all. Deleting a
cloned asset drops its mapping row. A dry run lists and deletes nothing.

configure(db=..., send=...) wires the DB and the server command sender (an async
callable taking ({type, data}, timeout) and returning a dict); app.py does it at
startup and tests hand in fakes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .context import STEP_FINAL
from .registry import TaskType, register

TYPE_KEY = "purge_assets"
KINDS = ("emojis", "stickers", "roles")
KIND_TITLES = {"emojis": "Emojis", "stickers": "Stickers", "roles": "Roles"}
KIND_ONE = {"emojis": "emoji", "stickers": "sticker", "roles": "role"}
SCOPE_WORDS = {"cloned": "cloned", "unmapped": "uncloned", "all": "all"}
LIST_TIMEOUT = 20.0
DELETE_TIMEOUT = 30.0
# Pause between deletes so a long purge stays under Discord's per-route limits.
DELETE_GAP = 0.3
RATE_RETRIES = 3
RATE_WAIT_MAX = 60.0
# Mapping rows the preview counts when the server bot does not answer.
_MAP_COUNT_SQL = {
    "emojis": "SELECT COUNT(*) FROM emoji_mappings WHERE cloned_guild_id = ? AND cloned_emoji_id IS NOT NULL",
    "stickers": "SELECT COUNT(*) FROM sticker_mappings WHERE cloned_guild_id = ? AND cloned_sticker_id IS NOT NULL",
    "roles": "SELECT COUNT(*) FROM role_mappings WHERE cloned_guild_id = ? AND cloned_role_id IS NOT NULL",
}


@dataclass
class _Wiring:
    db: Any = None
    send: Optional[Callable[[dict, float], Awaitable[Any]]] = None


_wiring = _Wiring()


def configure(db: Any = None, send: Optional[Callable[[dict, float], Awaitable[Any]]] = None) -> None:
    if db is not None:
        _wiring.db = db
    if send is not None:
        _wiring.send = send


async def _send(frame: dict, timeout: float) -> dict:
    if _wiring.send is None:
        return {"ok": False, "error": "server sender not configured"}
    try:
        res = await _wiring.send(frame, timeout)
    except Exception as exc:
        return {"ok": False, "error": str(exc) or type(exc).__name__}
    return res if isinstance(res, dict) else {"ok": False, "error": "bad reply from the server bot"}


class PurgeAssetsInputs(BaseModel):
    mapping_id: str = Field(min_length=1, title="Mapping")
    kinds: list[Literal["emojis", "stickers", "roles"]] = Field(default_factory=lambda: list(KINDS),
                                                                 title="Assets")
    scope: Literal["cloned", "unmapped", "all"] = Field("cloned", title="Which assets")
    dry_run: bool = Field(False, title="Dry run")

    @field_validator("mapping_id", mode="before")
    @classmethod
    def _mapping(cls, value: Any) -> str:
        return str(value).strip() if value is not None else ""

    @field_validator("kinds", mode="after")
    @classmethod
    def _kinds(cls, value: list[str]) -> list[str]:
        out = [k for k in KINDS if k in value]
        if not out:
            raise ValueError("Pick at least one kind of asset")
        return out


def _fmt(n: Any) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "0"


def _db(lookups: Any = None) -> Any:
    return lookups if lookups is not None and hasattr(lookups, "get_mapping_by_id") else _wiring.db


def _name(m: dict) -> str:
    return m.get("mapping_name") or str(m.get("mapping_id") or "")


def _words(kinds: list[str]) -> str:
    """"emojis", "emojis and roles", "emojis, stickers and roles"."""
    if len(kinds) == 1:
        return kinds[0]
    return ", ".join(kinds[:-1]) + " and " + kinds[-1]


def _in_scope(item: dict, scope: str) -> bool:
    if scope == "cloned":
        return bool(item.get("mapped"))
    if scope == "unmapped":
        return not item.get("mapped")
    return True


def _assets(n: int, kind: str) -> str:
    return f"{_fmt(n)} {KIND_ONE[kind] if n == 1 else kind}"


def _above_bot(n: int, kind: str) -> str:
    return f"{_assets(n, kind)} at or above the bot's role {'is' if n == 1 else 'are'} left alone"


def describe(inputs: PurgeAssetsInputs, lookups: Any = None) -> tuple[str, dict]:
    db = _db(lookups)
    mapping = None
    try:
        mapping = db.get_mapping_by_id(inputs.mapping_id) if db is not None else None
    except Exception:
        mapping = None
    context = {"label": inputs.mapping_id, "mapping_id": inputs.mapping_id, "guild_id": None}
    if mapping:
        context.update(label=_name(mapping) or inputs.mapping_id,
                       guild_id=str(mapping.get("original_guild_id") or "") or None)
    verb = "Count" if inputs.dry_run else "Purge"
    return f"{verb} {SCOPE_WORDS[inputs.scope]} {_words(list(inputs.kinds))}", context


def _mappings(db: Any) -> list[dict]:
    rows = sorted((dict(m) for m in db.list_guild_mappings()), key=lambda m: _name(m).casefold())
    return [{"mapping_id": str(m["mapping_id"]), "name": _name(m),
             "cloned_guild_id": str(m.get("cloned_guild_id") or "") or None,
             "cloned_guild_name": m.get("cloned_guild_name")}
            for m in rows if m.get("cloned_guild_id")]


def _db_counts(db: Any, cloned_gid: Any) -> dict:
    out = {}
    for kind, sql in _MAP_COUNT_SQL.items():
        try:
            out[kind] = int(db.conn.execute(sql, (int(cloned_gid),)).fetchone()[0])
        except Exception:
            out[kind] = None
    return out


async def options(params: dict) -> dict:
    """{mappings, counts: {kind: n | None}, live, notes, error}. counts are what a run with this
    mapping and scope would delete, asked of the server bot; without the bot a cloned scope falls
    back to the DB's mapping rows (live false) and the other scopes have no count."""
    db = _wiring.db
    if db is None:
        return {"mappings": [], "counts": {}, "live": False, "error": "Database not configured"}
    params = params or {}
    out: dict = {"mappings": _mappings(db), "counts": {k: None for k in KINDS}, "live": False,
                 "notes": {}, "error": None}
    mapping_id = str(params.get("mapping_id") or "").strip()
    if not mapping_id:
        return out
    scope = params.get("scope") if params.get("scope") in SCOPE_WORDS else "cloned"
    mapping = db.get_mapping_by_id(mapping_id)
    if not mapping or not mapping.get("cloned_guild_id"):
        out["error"] = "Unknown mapping"
        return out
    res = await _send({"type": "purge_assets_list",
                       "data": {"guild_id": str(mapping["cloned_guild_id"]), "kinds": list(KINDS)}},
                      LIST_TIMEOUT)
    if res.get("ok"):
        out["live"] = True
        for kind in KINDS:
            info = (res.get("kinds") or {}).get(kind) or {}
            if info.get("error"):
                out["notes"][kind] = info["error"]
                continue
            out["counts"][kind] = sum(1 for i in info.get("items") or [] if _in_scope(i, scope))
            if info.get("above_bot"):
                out["notes"][kind] = _above_bot(info["above_bot"], kind)
        return out
    out["error"] = "The server bot did not answer, so these counts come from Copycord's records"
    if scope == "cloned":
        out["counts"] = _db_counts(db, mapping["cloned_guild_id"])
    return out


async def _pause(ctx: Any, seconds: float) -> bool:
    """Sleep, waking early on a cancel; True when cancelled."""
    if seconds <= 0:
        return ctx.cancelled.is_set()
    try:
        await asyncio.wait_for(ctx.cancelled.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
    return ctx.cancelled.is_set()


async def _delete_one(ctx: Any, step: Any, gid: Any, kind: str, scope: str, item: dict) -> tuple[str, Optional[str]]:
    """(outcome, error): deleted, skipped, failed, or cancelled while waiting out a rate limit."""
    frame = {"type": "purge_assets_delete",
             "data": {"guild_id": str(gid), "kind": kind, "scope": scope, "id": str(item["id"])}}
    for attempt in range(RATE_RETRIES + 1):
        res = await _send(frame, DELETE_TIMEOUT)
        outcome = res.get("outcome")
        if outcome == "rate_limited":
            if attempt == RATE_RETRIES:
                return "failed", "Rate limited by Discord"
            try:
                wait = min(max(float(res.get("retry_after") or 1), 0.0), RATE_WAIT_MAX)
            except (TypeError, ValueError):
                wait = 1.0
            await step.state(f"Rate limited, waiting {wait:g} s")
            await ctx.log(f"Rate limited by Discord, waiting {wait:g} s", level="warning", step=kind)
            if await _pause(ctx, wait):
                return "cancelled", None
            continue
        if outcome in ("deleted", "skipped", "failed"):
            return outcome, res.get("error")
        return "failed", res.get("error") or "No reply from the server bot"
    return "failed", "Rate limited by Discord"


def _note(r: dict, dry_run: bool) -> str:
    if dry_run:
        return f"{_fmt(r['matched'])} would be deleted"
    parts = [f"{_fmt(r['deleted'])} deleted"]
    if r["failed"]:
        parts.append(f"{_fmt(r['failed'])} failed")
    if r["skipped"]:
        parts.append(f"{_fmt(r['skipped'])} skipped")
    return ", ".join(parts)


async def run(ctx: Any, inputs: PurgeAssetsInputs) -> None:
    db = _wiring.db
    mapping = db.get_mapping_by_id(inputs.mapping_id) if db is not None else None
    if not mapping or not mapping.get("cloned_guild_id"):
        raise RuntimeError(f"Unknown mapping {inputs.mapping_id}")
    gid = mapping["cloned_guild_id"]
    kinds = list(inputs.kinds)
    scope = inputs.scope
    steps = {k: ctx.step(k, KIND_TITLES[k]) for k in kinds}
    results = {k: {"kind": k, "title": KIND_TITLES[k], "matched": 0, "deleted": 0, "failed": 0,
                   "skipped": 0, "kept": 0, "outcome": None, "note": None, "error": None, "items": []}
               for k in kinds}
    await ctx.log(f"{'Counting' if inputs.dry_run else 'Purging'} {SCOPE_WORDS[scope]} {_words(kinds)} "
                  f"in {_name(mapping)}{' (dry run, nothing is deleted)' if inputs.dry_run else ''}")

    listing = await _send({"type": "purge_assets_list", "data": {"guild_id": str(gid), "kinds": kinds}},
                          LIST_TIMEOUT)
    if not listing.get("ok"):
        raise RuntimeError(listing.get("error") or "The server bot did not answer")

    async def live_summary() -> None:
        rs = results.values()
        await ctx.summary(matched=sum(r["matched"] for r in rs), deleted=sum(r["deleted"] for r in rs),
                          failed=sum(r["failed"] for r in rs), skipped=sum(r["skipped"] for r in rs))

    for kind in kinds:
        step, r = steps[kind], results[kind]
        if ctx.cancelled.is_set():
            if step.data["state"] not in STEP_FINAL:
                await step.finish("cancelled", ctx.cancel_note or "Cancelled")
            continue
        await step.start()
        info = (listing.get("kinds") or {}).get(kind) or {}
        if info.get("error"):
            r["error"] = info["error"]
            await step.finish("failed", info["error"])
            await ctx.log(f"{KIND_TITLES[kind]}: {info['error']}", level="error", step=kind)
            continue
        found = list(info.get("items") or [])
        items = [i for i in found if _in_scope(i, scope)]
        r["matched"], r["kept"] = len(items), len(found) - len(items)
        total = len(items)
        await ctx.log(f"{KIND_TITLES[kind]}: {_assets(total, kind)} to "
                      f"{'count' if inputs.dry_run else 'delete'}"
                      + (f", {_fmt(r['kept'])} left alone" if r["kept"] else ""), step=kind)
        if info.get("above_bot"):
            await ctx.log(f"{KIND_TITLES[kind]}: {_above_bot(info['above_bot'], kind)}",
                          level="warning", step=kind)
        await live_summary()

        if inputs.dry_run:
            r["items"] = [{"id": str(i["id"]), "name": i.get("name"), "outcome": "would delete", "error": None}
                          for i in items]
            await step.progress(total, total, label=f"{_fmt(total)} would be deleted")
            await step.finish("success", _note(r, True))
            continue

        await step.progress(0, total)
        done = 0
        for item in items:
            if ctx.cancelled.is_set():
                break
            name = item.get("name") or str(item["id"])
            outcome, error = await _delete_one(ctx, step, gid, kind, scope, item)
            if outcome == "cancelled":
                break
            done += 1
            r[outcome] += 1
            r["items"].append({"id": str(item["id"]), "name": name, "outcome": outcome, "error": error})
            if outcome == "failed":
                await ctx.log(f"Failed {KIND_ONE[kind]} {name}: {error}", level="error", step=kind)
            elif outcome == "skipped":
                await ctx.log(f"Skipped {KIND_ONE[kind]} {name}: {error}", step=kind)
            await step.progress(done, total, label=name)
            await live_summary()
            if done < total and await _pause(ctx, DELETE_GAP):
                break

        if step.data["state"] in STEP_FINAL:
            pass  # the service ended it already (bots stopped)
        elif done < total and ctx.cancelled.is_set():
            await step.finish("cancelled", f"Cancelled after {_note(r, False)}")
        elif r["failed"]:
            await step.finish("failed", _note(r, False))
        else:
            await step.finish("success", _note(r, False))
        await ctx.log(f"{KIND_TITLES[kind]}: {_note(r, False)}", step=kind)

    for kind in kinds:
        r, state = results[kind], steps[kind].data["state"]
        r["outcome"] = state if state in STEP_FINAL else "cancelled"
        r["note"] = steps[kind].data.get("note")
    rs = list(results.values())
    summary = {"matched": sum(r["matched"] for r in rs), "deleted": sum(r["deleted"] for r in rs),
               "failed": sum(r["failed"] for r in rs), "skipped": sum(r["skipped"] for r in rs)}
    await ctx.summary(**summary)
    await ctx.report({
        "inputs": inputs.model_dump(mode="json"),
        "mapping": {"mapping_id": str(inputs.mapping_id), "name": _name(mapping),
                    "cloned_guild_id": str(gid), "cloned_guild_name": listing.get("guild_name")},
        "dry_run": inputs.dry_run,
        "kinds": rs,
        "failures": [{"kind": r["kind"], **i} for r in rs for i in r["items"] if i["outcome"] == "failed"],
        "summary": summary,
    })
    failed_kinds = [r for r in rs if r["outcome"] == "failed"]
    if failed_kinds:
        if summary["failed"]:
            ctx.fail(f"{_fmt(summary['failed'])} of {_fmt(summary['matched'])} assets failed")
        else:
            ctx.fail(f"{len(failed_kinds)} of {len(rs)} asset kinds failed")
    elif ctx.cancelled.is_set():
        await ctx.log("Run cancelled", level="warning")
    elif inputs.dry_run:
        await ctx.log(f"Done: {_fmt(summary['matched'])} assets would be deleted")
    else:
        await ctx.log(f"Done: deleted {_fmt(summary['deleted'])} of {_fmt(summary['matched'])} assets")


PURGE_ASSETS = register(TaskType(
    key=TYPE_KEY,
    title="Purge assets",
    description="Delete emojis, stickers and roles from a clone guild. Only what Copycord cloned unless you pick otherwise.",
    run=run,
    inputs=PurgeAssetsInputs,
    describe=describe,
    options=options,
    requires_bots=True,
))
