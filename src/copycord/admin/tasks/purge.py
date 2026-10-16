# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Purge messages: delete Copycord's stored message records older than a cutoff.

The messages table maps each source message to its clone; edits, deletes and the
backfill's Ignore cloned read it. Discord messages are never touched. One step per
mapping (plus one for records whose guild pair no mapping has, when every mapping is
in scope); a dry run counts and deletes nothing.

configure(db=...) wires the DB; app.py does it at startup and tests hand in a fake."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .registry import TaskType, register

TYPE_KEY = "purge_messages"
UNIT_SECONDS = {"hours": 3600, "days": 86400, "weeks": 7 * 86400}
UNIT_WORDS = {"hours": ("hour", "hours"), "days": ("day", "days"), "weeks": ("week", "weeks")}
# Rows not under any mapping's guild pair: their own step when every mapping is in scope.
OTHER_KEY = "other"


@dataclass
class _Wiring:
    db: Any = None


_wiring = _Wiring()


def configure(db: Any = None) -> None:
    if db is not None:
        _wiring.db = db


class PurgeInputs(BaseModel):
    older_than: int = Field(30, title="Older than")
    unit: Literal["hours", "days", "weeks"] = Field("days", title="Unit")
    mapping_id: Optional[str] = Field(None, title="Mapping")
    dry_run: bool = Field(False, title="Dry run")

    @field_validator("older_than", mode="before")
    @classmethod
    def _positive(cls, value: Any) -> int:
        if isinstance(value, bool):
            raise ValueError("Enter a whole number above 0")
        try:
            n = int(value)
        except (TypeError, ValueError):
            raise ValueError("Enter a whole number above 0") from None
        if isinstance(value, float) and n != value:
            raise ValueError("Enter a whole number above 0")
        if n < 1:
            raise ValueError("Enter a whole number above 0")
        return n

    @field_validator("mapping_id", mode="before")
    @classmethod
    def _mapping(cls, value: Any) -> Optional[str]:
        s = str(value).strip() if value is not None else ""
        return s or None

    @property
    def seconds(self) -> int:
        return self.older_than * UNIT_SECONDS[self.unit]


def age_words(n: int, unit: str) -> str:
    """"30 days", "1 week"."""
    one, many = UNIT_WORDS.get(unit, (unit, unit))
    return f"{n:,} {one if n == 1 else many}"


def _fmt(n: Any) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "0"


def _db(lookups: Any = None) -> Any:
    return lookups if lookups is not None and hasattr(lookups, "get_mapping_by_id") else _wiring.db


def _pair(m: dict) -> Optional[tuple[int, int]]:
    try:
        return int(m["original_guild_id"]), int(m["cloned_guild_id"])
    except (KeyError, TypeError, ValueError):
        return None


def _name(m: dict) -> str:
    return m.get("mapping_name") or str(m.get("mapping_id") or "")


def _scopes(db: Any, inputs: PurgeInputs) -> list[dict]:
    """[{key, name, mapping_id, pair, skip}]: one per mapping in scope by name, then the unmapped rows
    when every mapping is. pair limits a scope to a mapping; skip leaves out every mapping."""
    if inputs.mapping_id:
        m = db.get_mapping_by_id(inputs.mapping_id)
        if not m:
            raise RuntimeError(f"Unknown mapping {inputs.mapping_id}")
        maps = [dict(m)]
    else:
        maps = sorted((dict(m) for m in db.list_guild_mappings()), key=lambda m: _name(m).casefold())
    out: list[dict] = []
    for m in maps:
        pair = _pair(m)
        if pair is None:
            continue
        out.append({"key": str(m["mapping_id"]), "name": _name(m), "mapping_id": str(m["mapping_id"]),
                    "pair": pair, "skip": None})
    if not inputs.mapping_id:
        out.append({"key": OTHER_KEY, "name": "Not under a mapping", "mapping_id": None,
                    "pair": None, "skip": [s["pair"] for s in out]})
    return out


def _count(db: Any, seconds: int, scope: dict) -> int:
    if scope["pair"] is not None:
        return db.count_old_messages(seconds, pair=scope["pair"])
    return db.count_old_messages(seconds, skip_pairs=scope["skip"])


def _delete(db: Any, seconds: int, scope: dict) -> int:
    if scope["pair"] is not None:
        return db.delete_old_messages(seconds, pair=scope["pair"])
    return db.delete_old_messages(seconds, skip_pairs=scope["skip"])


def describe(inputs: PurgeInputs, lookups: Any = None) -> tuple[str, dict]:
    db = _db(lookups)
    age = age_words(inputs.older_than, inputs.unit)
    verb = "Count" if inputs.dry_run else "Purge"
    context: dict = {"label": "All mappings", "mapping_id": inputs.mapping_id, "guild_id": None}
    if inputs.mapping_id:
        mapping = None
        try:
            mapping = db.get_mapping_by_id(inputs.mapping_id) if db is not None else None
        except Exception:
            mapping = None
        context["label"] = (_name(mapping) if mapping else None) or inputs.mapping_id
        if mapping:
            context["guild_id"] = str(mapping.get("original_guild_id") or "") or None
    return f"{verb} messages older than {age}", context


async def options(params: dict) -> dict:
    """The preview: what a run with these inputs would delete, per mapping. Bad inputs come
    back as {errors}."""
    db = _wiring.db
    if db is None:
        return {"matched": 0, "mappings": [], "error": "Database not configured"}
    raw = {k: v for k, v in (params or {}).items() if k in ("older_than", "unit", "mapping_id")}
    try:
        inputs = PurgeInputs(**raw)
    except Exception as exc:
        errors = {}
        for err in getattr(exc, "errors", lambda: [])():
            errors[".".join(str(p) for p in err.get("loc") or ()) or "inputs"] = err.get("msg") or "Invalid value"
        return {"matched": 0, "mappings": [], "errors": errors or {"inputs": str(exc)}}
    try:
        scopes = _scopes(db, inputs)
    except RuntimeError as exc:
        return {"matched": 0, "mappings": [], "errors": {"mapping_id": str(exc)}}
    rows = [{"mapping_id": s["mapping_id"], "name": s["name"], "matched": _count(db, inputs.seconds, s)}
            for s in scopes]
    rows = [r for r in rows if r["mapping_id"] is not None or r["matched"]]
    return {"matched": sum(r["matched"] for r in rows), "mappings": rows}


async def run(ctx: Any, inputs: PurgeInputs) -> None:
    db = _wiring.db
    if db is None:
        raise RuntimeError("Database not configured")
    seconds = inputs.seconds
    age = age_words(inputs.older_than, inputs.unit)
    scopes = _scopes(db, inputs)
    await ctx.log(f"{'Counting' if inputs.dry_run else 'Purging'} message records older than {age}"
                  f"{' (dry run, nothing is deleted)' if inputs.dry_run else ''}")

    results: list[dict] = []
    steps = []
    for s in scopes:
        steps.append(ctx.step(s["key"], s["name"]))
        results.append({"mapping_id": s["mapping_id"], "name": s["name"], "matched": 0,
                        "deleted": 0, "outcome": None, "error": None})

    for scope, step, result in zip(scopes, steps, results):
        if ctx.cancelled.is_set():
            await step.finish("cancelled", ctx.cancel_note or "Cancelled")
            continue
        await step.start()
        try:
            matched = _count(db, seconds, scope)
            result["matched"] = matched
            if inputs.dry_run:
                await step.progress(matched, matched, label=f"{_fmt(matched)} matched")
                await step.finish("success", f"{_fmt(matched)} would be deleted")
                await ctx.log(f"{scope['name']}: {_fmt(matched)} records would be deleted", step=step.key)
            else:
                deleted = _delete(db, seconds, scope)
                result["deleted"] = deleted
                await step.progress(deleted, max(matched, deleted))
                await step.finish("success", f"{_fmt(deleted)} deleted")
                await ctx.log(f"{scope['name']}: deleted {_fmt(deleted)} records", step=step.key)
        except Exception as exc:
            result["error"] = str(exc) or type(exc).__name__
            await step.finish("failed", result["error"])
            await ctx.log(f"{scope['name']}: {result['error']}", level="error", step=step.key)
        result["outcome"] = step.data["state"]
        await ctx.summary(matched=sum(r["matched"] for r in results),
                          deleted=sum(r["deleted"] for r in results))

    # A scope with nothing in it outside a single-mapping run drops out of the report.
    shown = [r for r in results if r["mapping_id"] is not None or r["matched"] or r["outcome"] == "failed"]
    for r in results:
        if r["outcome"] is None:
            r["outcome"] = "cancelled"
    matched = sum(r["matched"] for r in results)
    deleted = sum(r["deleted"] for r in results)
    failed = sum(1 for r in results if r["outcome"] == "failed")
    summary = {"matched": matched, "deleted": deleted}
    await ctx.summary(**summary)
    await ctx.report({
        "inputs": inputs.model_dump(mode="json"),
        "older_than_seconds": seconds,
        "dry_run": inputs.dry_run,
        "mappings": shown,
        "summary": summary,
    })
    if failed:
        ctx.fail(f"{failed} of {len(results)} mappings failed")
    elif ctx.cancelled.is_set():
        await ctx.log("Run cancelled", level="warning")
    elif inputs.dry_run:
        await ctx.log(f"Done: {_fmt(matched)} records would be deleted")
    else:
        await ctx.log(f"Done: deleted {_fmt(deleted)} of {_fmt(matched)} records")


PURGE = register(TaskType(
    key=TYPE_KEY,
    title="Purge messages",
    description="Delete Copycord's stored message records older than a cutoff. Discord messages are not touched.",
    run=run,
    inputs=PurgeInputs,
    describe=describe,
    options=options,
))
