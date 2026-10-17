# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Scrape members: the Scraper page's member scrape as a task. Wraps the
StandaloneScraper (standalone_scraper.py), which uses the scraper's own user
tokens and talks to Discord itself, so the server and client bots are not needed.

Steps: Check tokens (the saved scraper tokens that can see the server; there is no
token validation, a token that fails or is not in the server is skipped), Scrape
members (members found of the server's approximate count, as the scraper reports
them), Save file. One run at a time, as the old queue ran them; later runs wait
in the framework.

A cancel stops the scraper and cancels its asyncio task at once; the run waits at
most CANCEL_WAIT for it to unwind and nothing the scraper reports after the cancel
reaches the run. Members found before the cancel are still saved, as the old page did.

The result is the same file the old page wrote and lists under past scrapes:
<data dir>/scrapes/<guild id>/scrape_<guild id>_<stamp>.json (save_scrape_file).
GET /api/tasks/scrape-members/<run id> tells the run page whether it is still there,
and .../file serves it.

configure(db=..., data_dir=..., in_guild=..., user_guilds=..., log_path=...,
make_scraper=...) wires the DB (scraper tokens), the data dir (scrapes/, proxies.txt),
the per token membership check (app.py's _selfbot_in_guild), the cached account
guild list, scraper.out and the scraper class; tests hand in fakes."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from copycord.admin.standalone_scraper import ScraperConfig, StandaloneScraper

from .context import STEP_FINAL
from .registry import TaskType, register
from .routes import _run_id, get_service, router

log = logging.getLogger("copycord.tasks.scrape")

TYPE_KEY = "scrape_members"
# How long a cancel waits for the scraper's task to unwind after it was cancelled.
CANCEL_WAIT = 1.0
DIRNAME = "scrapes"
PROXY_FILE = "proxies.txt"
# The scraper's own progress lines (every 1.5 s); the step's progress shows them instead.
PROGRESS_MARK = "⛏"
LEVELS = {"warn": "warning", "warning": "warning", "error": "error"}
# The scraper's messages use this dash; run log lines get ": " or "-" instead.
DASH = chr(0x2014)


# wiring

@dataclass
class _Wiring:
    db: Any = None
    data_dir: Optional[Path] = None
    in_guild: Optional[Callable[[str, int], Awaitable[bool]]] = None
    user_guilds: Optional[Callable[[str], Awaitable[list]]] = None
    log_path: Optional[Path] = None
    make_scraper: Callable[[Any], Any] = StandaloneScraper


_wiring = _Wiring()


def configure(db: Any = None, data_dir: Any = None,
              in_guild: Optional[Callable[[str, int], Awaitable[bool]]] = None,
              user_guilds: Optional[Callable[[str], Awaitable[list]]] = None,
              log_path: Any = None, make_scraper: Optional[Callable[[Any], Any]] = None) -> None:
    if db is not None:
        _wiring.db = db
    if data_dir is not None:
        _wiring.data_dir = Path(data_dir)
    if in_guild is not None:
        _wiring.in_guild = in_guild
    if user_guilds is not None:
        _wiring.user_guilds = user_guilds
    if log_path is not None:
        _wiring.log_path = Path(log_path)
    if make_scraper is not None:
        _wiring.make_scraper = make_scraper


# inputs

class ScrapeInputs(BaseModel):
    guild_id: str = Field(title="Server ID")
    include_username: bool = Field(True, title="Usernames")
    include_avatar_url: bool = Field(True, title="Avatar URLs")
    include_bio: bool = Field(False, title="Bios")
    include_roles: bool = Field(False, title="Roles")
    use_proxies: bool = Field(True, title="Use the saved proxies")

    @field_validator("guild_id", mode="before")
    @classmethod
    def _guild_id(cls, value: Any) -> str:
        s = str(value if value is not None else "").strip()
        if not s.isdigit():
            raise ValueError("Pick a server or enter a numeric server ID")
        return s


# Server names seen by options(), for run titles.
_guild_names: dict[str, str] = {}


def _guild_name(inputs: ScrapeInputs) -> str:
    return _guild_names.get(inputs.guild_id) or inputs.guild_id


def describe(inputs: ScrapeInputs, lookups: Any = None) -> tuple[str, dict]:
    name = _guild_name(inputs)
    return f"Scrape members of {name}", {"label": name, "guild_id": inputs.guild_id}


# settings: tokens and proxies

def _tokens() -> list[dict]:
    """Every saved scraper token, least used first. Tokens are not validated: one that
    fails or cannot see the server is skipped when it is used."""
    db = _wiring.db
    if db is None:
        return []
    try:
        tokens = [t for t in (db.list_scraper_tokens() or []) if t.get("token_value")]
    except Exception:
        log.exception("scraper tokens unreadable")
        return []
    return sorted(tokens, key=lambda t: int(t.get("use_count") or 0))


def saved_proxies() -> list[str]:
    """The proxy list the Scraper page saves (data/proxies.txt)."""
    if _wiring.data_dir is None:
        return []
    try:
        text = (_wiring.data_dir / PROXY_FILE).read_text(encoding="utf-8")
    except OSError:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


async def _access(tokens: list[dict], guild_id: int) -> list[dict]:
    """The tokens that can see the server. A token whose check fails (bad token, Discord
    error) counts as not in the server."""
    if _wiring.in_guild is None:
        return []
    usable: list[dict] = []
    for token in tokens:
        try:
            ok = await _wiring.in_guild(token["token_value"], guild_id)
        except Exception as exc:
            log.info("scraper token %s skipped: %s", token.get("token_id"), exc)
            ok = False
        if ok:
            usable.append(token)
    return usable


def _guilds_error(exc: Exception) -> str:
    detail = str(getattr(exc, "detail", "") or exc or type(exc).__name__)
    if detail == "discord-rate-limited":
        return "Discord is rate limiting the server list. Try again in a minute."
    if detail in ("discord-unauthorized", "client-token-invalid"):
        return "Discord did not accept a scraper token."
    return f"Could not load the servers ({detail})"


async def options(params: dict) -> dict:
    """What the form needs: {guilds: [{id, name, tokens}], tokens: {total, skipped}, proxies, error?}.
    The guild list is the union of the saved tokens' servers; a token whose list does not load
    is skipped (counted in tokens.skipped), never an error unless none loads."""
    tokens = _tokens()
    out: dict = {"guilds": [], "tokens": {"total": len(tokens), "skipped": 0},
                 "proxies": len(saved_proxies())}
    if not tokens:
        out["error"] = "No scraper tokens. Add one first."
        return out
    if _wiring.user_guilds is not None:
        seen: dict[str, dict] = {}
        errors: list[str] = []
        for token in tokens:
            try:
                raw = await _wiring.user_guilds(token["token_value"])
            except Exception as exc:
                errors.append(_guilds_error(exc))
                out["tokens"]["skipped"] += 1
                continue
            for g in raw or []:
                g_id = str(g.get("id") or "").strip()
                if not g_id.isdigit():
                    continue
                entry = seen.setdefault(g_id, {"id": g_id, "name": str(g.get("name") or g_id), "tokens": 0})
                entry["tokens"] += 1
                _guild_names[g_id] = entry["name"]
        out["guilds"] = sorted(seen.values(), key=lambda g: g["name"].casefold())
        if errors and not seen:
            out["error"] = errors[0]
    return out


# the result file

def save_scrape_file(data_dir: Path, guild_id: Any, members: list, metadata: Optional[dict],
                     total_count: int, elapsed_seconds: float) -> Optional[str]:
    """Write <data dir>/scrapes/<guild id>/scrape_<guild id>_<stamp>.json; returns
    "<guild id>/<file name>", or None when there are no members or the write failed."""
    if not members:
        return None
    try:
        guild_dir = Path(data_dir) / DIRNAME / str(guild_id)
        guild_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"scrape_{guild_id}_{ts}.json"
        save_data = {
            "guild_id": str(guild_id),
            "guild_name": (metadata or {}).get("guild_name") or "",
            "total_count": total_count,
            "elapsed_seconds": round(elapsed_seconds, 2),
            "scraped_at": datetime.now().isoformat(),
            "metadata": metadata,
            "members": members,
        }
        (guild_dir / fname).write_text(json.dumps(save_data, indent=2), encoding="utf-8")
        log.info("Scrape results saved to %s/%s", guild_id, fname)
        return f"{guild_id}/{fname}"
    except Exception as save_err:
        log.exception("Failed to save scrape results: %s", save_err)
        return None


def append_scraper_log(path: Optional[Path], guild_id: Any, message: str, level: str) -> None:
    """One line in scraper.out, which the Logs page shows."""
    if path is None:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] [{str(level).upper()}] [Guild {guild_id}] {message}\n")
    except Exception:
        pass


# run

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


def _members(n: int) -> str:
    return f"{_fmt(n)} member{'s' if n != 1 else ''}"


def _clean(message: Any) -> str:
    return str(message).replace(f" {DASH} ", ": ").replace(DASH, "-").strip()


@dataclass
class _Job:
    """What the scraper's callbacks hand the run. The callbacks are plain functions called
    from the scraper's tasks; the run loop turns what they gathered into steps and log lines."""

    guild_id: str
    found: int = 0
    hint: Optional[int] = None
    progressed: bool = False
    closed: bool = False
    lines: list = field(default_factory=list)
    changed: asyncio.Event = field(default_factory=asyncio.Event)

    def on_progress(self, current: Any, total: Any, message: Any = "") -> None:
        if self.closed:
            return
        self.found = _int(current) or 0
        hint = _int(total)
        if hint:
            self.hint = hint
        self.progressed = True
        self.changed.set()

    def on_log(self, message: Any, level: Any = "info") -> None:
        if self.closed:
            return
        append_scraper_log(_wiring.log_path, self.guild_id, str(message), str(level))
        text = _clean(message)
        if not text or text.startswith(PROGRESS_MARK):
            return
        self.lines.append((text, LEVELS.get(str(level).lower(), "info")))
        self.changed.set()


async def _flush(ctx: Any, step: Any, job: _Job) -> None:
    lines, job.lines = job.lines, []
    for text, level in lines:
        await ctx.log(text, level=level, step="scrape")
    if job.progressed:
        job.progressed = False
        if job.hint:
            total = max(job.hint, job.found)
            await step.progress(job.found, total,
                                label=f"{_fmt(job.found)} of about {_fmt(job.hint)} members found")
        else:
            await step.progress(job.found, label=f"{_members(job.found)} found")


def _reap(task: asyncio.Future) -> None:
    if not task.cancelled():
        task.exception()


async def _race(ctx: Any, coro: Awaitable) -> tuple[bool, Any]:
    """Run coro until it ends or the run is cancelled: (cancelled, result)."""
    work = asyncio.ensure_future(coro)
    cancel_wait = asyncio.ensure_future(ctx.cancelled.wait())
    try:
        await asyncio.wait({work, cancel_wait}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        cancel_wait.cancel()
    if work.done():
        return False, work.result()
    work.cancel()
    work.add_done_callback(_reap)
    return True, None


async def _scrape(ctx: Any, step: Any, job: _Job, scraper: Any) -> tuple[Any, Optional[str], bool]:
    """Run the scraper: (result or None, error, cancelled)."""
    task = asyncio.ensure_future(scraper.scrape())
    cancel_wait = asyncio.ensure_future(ctx.cancelled.wait())
    try:
        while True:
            changed = asyncio.ensure_future(job.changed.wait())
            try:
                await asyncio.wait({task, cancel_wait, changed}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                changed.cancel()
            if ctx.cancelled.is_set():
                break
            job.changed.clear()
            await _flush(ctx, step, job)
            if task.done():
                job.closed = True
                try:
                    return task.result(), None, False
                except Exception as exc:
                    return None, str(exc) or type(exc).__name__, False
    finally:
        cancel_wait.cancel()
        if not task.done() and not ctx.cancelled.is_set():
            # The runner itself was cancelled (watchdog, shutdown): stop the scraper with it.
            job.closed = True
            _stop(scraper, task)

    # Cancelled: nothing the scraper reports from here on reaches the run.
    job.closed = True
    job.lines.clear()
    if task.done():
        try:
            return task.result(), None, True
        except Exception:
            return None, None, True
    _stop(scraper, task)
    done, _ = await asyncio.wait({task}, timeout=CANCEL_WAIT)
    if not done:
        await ctx.log(f"The scraper did not stop within {CANCEL_WAIT:g} s", level="warning", step="scrape")
        return None, None, True
    if task.cancelled():
        return None, None, True
    try:
        return task.result(), None, True
    except Exception:
        return None, None, True


def _stop(scraper: Any, task: asyncio.Future) -> None:
    try:
        scraper.stop()
    except Exception:
        pass
    task.cancel()
    task.add_done_callback(_reap)


async def run(ctx: Any, inputs: ScrapeInputs) -> None:
    gid = inputs.guild_id
    name = _guild_name(inputs)
    tokens_step = ctx.step("tokens", "Check tokens")
    scrape_step = ctx.step("scrape", "Scrape members")
    save_step = ctx.step("save", "Save file")
    proxies = saved_proxies() if inputs.use_proxies else []
    counts = {"members": 0, "tokens": 0, "tokens_skipped": 0}
    report: dict = {
        "inputs": {**inputs.model_dump(mode="json"), "proxies": len(proxies)},
        "guild": {"id": gid, "name": name if name != gid else None},
        "output": {"path": None, "size": None},
        "result": None,
        "error": None,
    }

    async def end(error: Optional[str] = None) -> None:
        await ctx.summary(**counts)
        report["summary"] = dict(counts)
        report["error"] = error
        await ctx.report(report)

    # Check tokens
    await tokens_step.start()
    tokens = _tokens()
    if not tokens:
        note = "No scraper tokens saved"
        await tokens_step.finish("failed", note)
        await scrape_step.finish("skipped", "Not reached")
        await save_step.finish("skipped", "Not reached")
        await end(note)
        ctx.fail(note)
        return
    await tokens_step.state(f"Checking {len(tokens)} token{'s' if len(tokens) != 1 else ''}")
    cancelled, usable = await _race(ctx, _access(tokens, int(gid)))
    if cancelled:
        await end()
        await ctx.log("Run cancelled", level="warning")
        return
    skipped = len(tokens) - len(usable)
    counts["tokens"] = len(usable)
    counts["tokens_skipped"] = skipped
    if not usable:
        note = (f"None of the {len(tokens)} scraper tokens work in this server" if len(tokens) != 1
                else "The scraper token does not work in this server")
        await tokens_step.finish("failed", note)
        await scrape_step.finish("skipped", "Not reached")
        await save_step.finish("skipped", "Not reached")
        await ctx.log(note, level="error", step="tokens")
        await end(note)
        ctx.fail(note)
        return
    for token in usable:
        try:
            _wiring.db.increment_scraper_token_usage(token["token_id"])
        except Exception:
            pass
    note = f"{len(usable)} of {len(tokens)} token{'s' if len(tokens) != 1 else ''} used"
    if skipped:
        note += f", {skipped} skipped"
    await tokens_step.finish("success", note)
    await ctx.log(note, step="tokens")
    if skipped:
        await ctx.log(f"{skipped} token{'s' if skipped != 1 else ''} skipped: failed or not in this server",
                      level="warning", step="tokens")

    # Scrape members
    job = _Job(gid)
    config = ScraperConfig(
        guild_id=int(gid),
        tokens=[t["token_value"] for t in usable],
        proxies=proxies,
        include_username=inputs.include_username,
        include_avatar_url=inputs.include_avatar_url,
        include_bio=inputs.include_bio,
        include_roles=inputs.include_roles,
        progress_callback=job.on_progress,
        log_callback=job.on_log,
    )
    if ctx.cancelled.is_set():
        await end()
        return
    await scrape_step.start()
    await scrape_step.state("Starting")
    log.info("scrape_members run %s: guild=%s tokens=%d proxies=%d", ctx.run.id, gid, len(usable), len(proxies))
    scraper = _wiring.make_scraper(config)
    result, error, cancelled = await _scrape(ctx, scrape_step, job, scraper)

    members = list(getattr(result, "members", None) or [])
    if cancelled and not members:
        members = list((getattr(scraper, "_members", None) or {}).values())
    metadata = dict(getattr(result, "metadata", None) or {})
    elapsed = float(getattr(result, "elapsed_seconds", 0) or 0)
    if result is not None and not error and not cancelled and not getattr(result, "success", False):
        error = str(getattr(result, "error", None) or "Scrape failed")
    if metadata.get("guild_name"):
        report["guild"]["name"] = metadata["guild_name"]
    counts["members"] = len(members)
    report["result"] = {
        "method": metadata.get("method"),
        "elapsed_seconds": round(elapsed, 2),
        "member_count_hint": metadata.get("member_count_hint") or job.hint,
        "cancelled": cancelled,
    }

    if cancelled:
        await scrape_step.progress(len(members))
        await scrape_step.finish("cancelled", f"Cancelled after {_members(len(members))} found"
                                 if members else "Cancelled")
    elif error:
        await scrape_step.finish("failed", error)
        await ctx.log(f"Scrape failed: {error}", level="error", step="scrape")
    else:
        await scrape_step.progress(len(members), max(len(members), job.hint or 0) or None)
        await scrape_step.finish("success", f"{_members(len(members))} found")

    # Save file
    if error and not members:
        await save_step.finish("skipped", "Not reached")
    elif not members:
        await save_step.finish("cancelled" if cancelled else "skipped",
                               "No members found, no file written")
    elif ctx.cancel_note is not None:
        # Bots stopped or app shutdown: the run already ended; keep the members anyway.
        _save(gid, members, metadata, elapsed, report)
    else:
        await save_step.start()
        saved = _save(gid, members, metadata, elapsed, report)
        if saved:
            what = f"{_members(len(members))} found before the cancel" if cancelled else _members(len(members))
            await save_step.finish("success", f"Saved {what}")
            await ctx.log(f"Saved {what} to {saved}", step="save")
        else:
            await save_step.finish("failed", "File not written")
            error = error or "File not written"

    await end(error)
    if error:
        ctx.fail(error)
    elif cancelled:
        await ctx.log("Run cancelled", level="warning")
    else:
        await ctx.log(f"Done: {_members(len(members))} from {report['guild']['name'] or gid}")


def _save(gid: str, members: list, metadata: dict, elapsed: float, report: dict) -> Optional[str]:
    if _wiring.data_dir is None:
        return None
    saved = save_scrape_file(_wiring.data_dir, gid, members, metadata, len(members), elapsed)
    if saved:
        path = _wiring.data_dir / DIRNAME / saved
        try:
            size = path.stat().st_size
        except OSError:
            size = None
        report["output"] = {"path": saved, "size": size}
    return saved


# the result file (behind the /api session guard like every /api path)

NO_FILE = "No scrape file"
GONE = "File no longer on disk"


def _scrape_file(run_id: str) -> tuple[Optional[Path], Optional[dict], Optional[JSONResponse]]:
    """The run's scrape file and what the run page shows about it, or the 404 to answer.
    Only a file under <data dir>/scrapes/ is served, whatever the report says."""
    svc = get_service()
    rid = _run_id(run_id)
    run = svc.get(rid) if svc is not None and rid is not None else None
    if run is None or run.type != TYPE_KEY:
        return None, None, JSONResponse({"detail": "Unknown run"}, status_code=404)
    report = run.report or {}
    raw = (report.get("output") or {}).get("path")
    if not raw or _wiring.data_dir is None:
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    root = (_wiring.data_dir / DIRNAME).resolve()
    try:
        path = (root / str(raw)).resolve()
    except (OSError, ValueError):
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    if not path.is_relative_to(root) or path == root:
        return None, None, JSONResponse({"detail": NO_FILE}, status_code=404)
    if not path.is_file():
        return None, None, JSONResponse({"detail": GONE}, status_code=404)
    try:
        size = path.stat().st_size
    except OSError:
        size = None
    members = _int((report.get("summary") or {}).get("members"))
    return path, {"filename": path.name, "path": path.relative_to(root).as_posix(),
                  "size": size, "members": members}, None


@router.get("/scrape-members/{run_id}")
async def scrape_file_info(run_id: str):
    _path, info, error = _scrape_file(run_id)
    return error if error is not None else {"file": info}


@router.get("/scrape-members/{run_id}/file")
async def scrape_file(run_id: str):
    path, info, error = _scrape_file(run_id)
    if error is not None:
        return error
    return FileResponse(path, media_type="application/json", filename=info["filename"])


SCRAPE_MEMBERS = register(TaskType(
    key=TYPE_KEY,
    title="Scrape members",
    description="List a server's members with the scraper tokens and save them to a JSON file.",
    run=run,
    inputs=ScrapeInputs,
    describe=describe,
    options=options,
    concurrency=1,
    requires_bots=False,
))
