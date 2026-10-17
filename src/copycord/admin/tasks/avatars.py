# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Download avatars: every profile picture the client account can see in one of its servers,
as a zip behind a download link that expires.

Two steps. Members: the client agent lists the members it can see (avatars_members; it streams
avatars_members_progress and ends with avatars_members_end on the admin bus) and this side pulls
the list in pages (avatars_members_page). Avatars: each member's avatar comes from Discord's CDN,
one file at a time and politely spaced, deduped by user id; a member without an avatar gets the
default one. The zip and a small meta file sit in <data dir>/avatar-downloads under a random
token; GET /api/tasks/avatars/<token> serves it until it expires. Expired files are deleted on
startup, when a run starts and when any link is opened.

configure(db=..., send=..., user_guilds=..., data_dir=..., bus=...) wires the DB, the client
command sender, the cached account guild list, the data dir and the admin bus; app.py does it
at startup and tests hand in fakes (fetch= replaces the CDN download)."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import re
import secrets
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from .context import iso
from .registry import TaskType, register
from .routes import router

log = logging.getLogger("copycord.tasks.avatars")

TYPE_KEY = "download_avatars"
FRAMES = ("avatars_members_progress", "avatars_members_end")
CDN = "https://cdn.discordapp.com"
# The largest size Discord serves without upscaling most avatars.
AVATAR_SIZE = 1024
# Seconds between two CDN requests, and the retries a 429 or a 5xx gets.
DOWNLOAD_INTERVAL = 0.1
RETRIES = 3
RETRY_WAIT_MAX = 30.0
# How long the client may take to list the members, and how often the run asks it directly
# in case a progress frame went missing.
MEMBERS_TIMEOUT = 900.0
STATUS_POLL = 5.0
PAGE_SIZE = 1000
SEND_TIMEOUT = 10.0
DEFAULT_EXPIRES_HOURS = 24
EXPIRES_HOURS_MAX = 168
DIRNAME = "avatar-downloads"
URL_PREFIX = "/api/tasks/avatars/"
# Failed downloads the report names one by one.
REPORT_FAILURES = 50

_TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{43}")
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


# wiring

Fetch = Callable[[str], Awaitable["tuple[int, bytes, dict]"]]


@dataclass
class _Wiring:
    db: Any = None
    send: Optional[Callable[[dict], Awaitable[Any]]] = None
    user_guilds: Optional[Callable[[str], Awaitable[list]]] = None
    data_dir: Optional[Path] = None
    bus: Any = None
    fetch: Optional[Fetch] = None
    clock: Callable[[], float] = time.time


_wiring = _Wiring()
_listener: Optional[asyncio.Task] = None
# Server names seen by options(), so a run's title names its server.
_guild_names: dict[str, str] = {}


def configure(db: Any = None, send: Optional[Callable[[dict], Awaitable[Any]]] = None,
              user_guilds: Optional[Callable[[str], Awaitable[list]]] = None,
              data_dir: Any = None, bus: Any = None, fetch: Optional[Fetch] = None) -> None:
    if db is not None:
        _wiring.db = db
    if send is not None:
        _wiring.send = send
    if user_guilds is not None:
        _wiring.user_guilds = user_guilds
    if bus is not None:
        _wiring.bus = bus
    if fetch is not None:
        _wiring.fetch = fetch
    if data_dir is not None:
        _wiring.data_dir = Path(data_dir)
        # Startup: what expired while Copycord was down goes, and so does a zip a dead run left half written.
        try:
            sweep(partials=True)
        except Exception:
            log.exception("avatar downloads: startup sweep failed")


async def _send(frame: dict) -> dict:
    if _wiring.send is None:
        return {"ok": False, "error": "client sender not configured"}
    try:
        res = await _wiring.send(frame)
    except Exception as exc:
        return {"ok": False, "error": str(exc) or type(exc).__name__}
    return res if isinstance(res, dict) else {"ok": True}


# inputs

class AvatarInputs(BaseModel):
    guild_id: str = Field(title="Server")
    guild_name: Optional[str] = Field(None, title="Server name")
    expires_hours: int = Field(DEFAULT_EXPIRES_HOURS, title="Link expires after (hours)")

    @field_validator("guild_id", mode="before")
    @classmethod
    def _gid(cls, value: Any) -> str:
        s = str(value).strip() if value is not None else ""
        if not s.isdigit():
            raise ValueError("Pick a server")
        return s

    @field_validator("guild_name", mode="before")
    @classmethod
    def _name(cls, value: Any) -> Optional[str]:
        s = str(value).strip() if value is not None else ""
        return s[:100] or None

    @field_validator("expires_hours", mode="before")
    @classmethod
    def _hours(cls, value: Any) -> int:
        msg = f"Enter a whole number of hours from 1 to {EXPIRES_HOURS_MAX}"
        if isinstance(value, bool):
            raise ValueError(msg)
        try:
            n = int(value)
        except (TypeError, ValueError):
            raise ValueError(msg) from None
        if isinstance(value, float) and n != value or not 1 <= n <= EXPIRES_HOURS_MAX:
            raise ValueError(msg)
        return n


def _guild_name(inputs: AvatarInputs) -> str:
    return _guild_names.get(inputs.guild_id) or inputs.guild_name or inputs.guild_id


def describe(inputs: AvatarInputs, lookups: Any = None) -> tuple[str, dict]:
    name = _guild_name(inputs)
    return f"Download avatars from {name}", {"label": name, "guild_id": inputs.guild_id}


# options: the client account's servers

def _client_token() -> str:
    db = _wiring.db
    try:
        return (db.get_config("CLIENT_TOKEN", "") or "").strip() if db is not None else ""
    except Exception:
        return ""


def _guilds_error(exc: Exception) -> str:
    detail = str(getattr(exc, "detail", "") or exc or type(exc).__name__)
    if detail == "discord-rate-limited":
        return "Discord is rate limiting the server list. Try again in a minute."
    if detail in ("discord-unauthorized", "client-token-invalid"):
        return "Discord did not accept the client token."
    return f"Could not load the servers ({detail})"


async def options(params: dict) -> dict:
    """The servers the client account is in, by name: {guilds: [{id, name}]} or {guilds: [], error}."""
    if _wiring.user_guilds is None:
        return {"guilds": [], "error": "Server list not configured"}
    token = _client_token()
    if not token:
        return {"guilds": [], "error": "Set the client account token first."}
    try:
        raw = await _wiring.user_guilds(token)
    except Exception as exc:
        return {"guilds": [], "error": _guilds_error(exc)}
    guilds = []
    for g in raw or []:
        gid = str(g.get("id") or "").strip()
        if not gid.isdigit():
            continue
        name = str(g.get("name") or gid)
        _guild_names[gid] = name
        guilds.append({"id": gid, "name": name})
    guilds.sort(key=lambda g: g["name"].casefold())
    return {"guilds": guilds}


# files: <data dir>/avatar-downloads/<token>.zip and <token>.json

def _root() -> Path:
    if _wiring.data_dir is None:
        from copycord.common.paths import data_dir
        _wiring.data_dir = data_dir()
    return _wiring.data_dir / DIRNAME


def _paths(token: str) -> tuple[Path, Path]:
    root = _root()
    return root / f"{token}.zip", root / f"{token}.json"


def _read_meta(token: str) -> Optional[dict]:
    try:
        meta = json.loads(_paths(token)[1].read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return meta if isinstance(meta, dict) else None


def _expired(meta: dict, now: float) -> bool:
    try:
        return float(meta.get("expires_at")) <= now
    except (TypeError, ValueError):
        return True


def _remove(token: str) -> None:
    for p in _paths(token):
        try:
            p.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            log.warning("avatar downloads: could not delete %s", p.name)


def sweep(now: Optional[float] = None, partials: bool = False) -> int:
    """Delete expired downloads and zips without a meta file; partials=True (startup only, no run
    is writing) also drops half written zips. Returns how many downloads went."""
    root = _root()
    if not root.is_dir():
        return 0
    now = _wiring.clock() if now is None else now
    gone = 0
    for p in root.iterdir():
        name = p.name
        if name.endswith(".json"):
            token = name[:-5]
            meta = _read_meta(token)
            if meta is None or _expired(meta, now):
                _remove(token)
                gone += 1
        elif name.endswith(".zip"):
            if not _paths(name[:-4])[1].exists():
                _remove(name[:-4])
                gone += 1
        elif partials and name.endswith(".zip.part"):
            try:
                p.unlink()
            except OSError:
                pass
    return gone


def _safe(text: Any, fallback: str = "user") -> str:
    s = _UNSAFE.sub("_", str(text or "")).strip("._")
    return s[:64] or fallback


# the download route (behind the /api session guard like every /api path)

@router.get("/avatars/{token}")
async def download_avatars_file(token: str):
    if not _TOKEN_RE.fullmatch(token or ""):
        return JSONResponse({"detail": "Unknown download"}, status_code=404)
    now = _wiring.clock()
    meta = _read_meta(token)
    zip_path = _paths(token)[0]
    if meta is None or not zip_path.is_file():
        sweep(now)
        return JSONResponse({"detail": "Unknown download"}, status_code=404)
    if _expired(meta, now):
        _remove(token)
        sweep(now)
        return JSONResponse({"detail": "This download link expired"}, status_code=410)
    sweep(now)
    return FileResponse(zip_path, media_type="application/zip",
                        filename=str(meta.get("filename") or "avatars.zip"))


# members: frames from the client agent

@dataclass
class _MemberJob:
    job_id: str
    step: Any
    found: int = 0
    member_count: Optional[int] = None
    end: Optional[dict] = None
    done: asyncio.Event = field(default_factory=asyncio.Event)


_jobs: dict[str, _MemberJob] = {}


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fmt(n: Any) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "0"


def members_label(found: int, member_count: Optional[int]) -> str:
    """"120 of 5,000 found"; without a member count "120 found"."""
    return f"{_fmt(found)} of {_fmt(member_count)} found" if member_count else f"{_fmt(found)} found"


async def _members_progress(job: _MemberJob, data: dict) -> None:
    job.found = max(job.found, _int(data.get("found")) or 0)
    if _int(data.get("member_count")):
        job.member_count = _int(data.get("member_count"))
    await job.step.progress(job.found, job.member_count, label=members_label(job.found, job.member_count))


async def dispatch(frame: dict) -> bool:
    """Route one avatars_members_* frame ({type, data}) to its run by job_id."""
    typ = frame.get("type")
    data = frame.get("data") or {}
    if typ not in FRAMES:
        return False
    job = _jobs.get(str(data.get("job_id") or ""))
    if job is None:
        return False
    try:
        await _members_progress(job, data)
        if typ == "avatars_members_end":
            job.end = dict(data)
            job.done.set()
    except Exception:
        log.exception("avatars frame %s for job %s failed", typ, job.job_id)
    return True


def _ensure_listener() -> None:
    """Watch the admin bus for this module's frames; started by the first run."""
    global _listener
    if _wiring.bus is None or (_listener is not None and not _listener.done()):
        return
    _listener = asyncio.ensure_future(_listen(_wiring.bus))


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


async def _wait_members(ctx: Any, job: _MemberJob) -> Optional[dict]:
    """The client's end of the listing, or None when the run was cancelled first. A quiet spell
    asks the client directly, so a lost frame never stalls the run."""
    deadline = _wiring.clock() + MEMBERS_TIMEOUT
    while not job.done.is_set():
        if ctx.cancelled.is_set():
            return None
        if _wiring.clock() >= deadline:
            return {"ok": False, "error": "The client did not finish listing the members"}
        waits = [asyncio.ensure_future(job.done.wait()), asyncio.ensure_future(ctx.cancelled.wait())]
        try:
            done, _ = await asyncio.wait(waits, timeout=STATUS_POLL, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waits:
                w.cancel()
            await asyncio.gather(*waits, return_exceptions=True)
        if done:
            continue
        res = await _send({"type": "avatars_members_page", "data": {"job_id": job.job_id, "offset": 0, "limit": 0}})
        state = res.get("state")
        if state == "missing":
            # The client restarted and lost the job.
            job.end = {"ok": False, "error": res.get("error") or "The client lost the job"}
            job.done.set()
        elif state in ("running", "done", "failed"):
            await _members_progress(job, res)
            if state != "running":
                job.end = {**res, "ok": state == "done"}
                job.done.set()
    return job.end


async def _pull_members(job_id: str) -> list[dict]:
    """The listed members in pages, deduped by user id, in the order the client found them."""
    out: dict[str, dict] = {}
    offset = 0
    while True:
        res = await _send({"type": "avatars_members_page",
                           "data": {"job_id": job_id, "offset": offset, "limit": PAGE_SIZE}})
        if not res.get("ok"):
            raise RuntimeError(res.get("error") or "The client did not send the member list")
        page = res.get("members") or []
        for m in page:
            uid = str((m or {}).get("id") or "")
            if uid.isdigit():
                out.setdefault(uid, m)
        offset += len(page)
        if len(page) < PAGE_SIZE or offset >= (_int(res.get("found")) or 0):
            return list(out.values())


# avatars

def avatar_url(member: dict) -> tuple[str, str]:
    """(url, extension): the avatar in its original format (gif when animated) at AVATAR_SIZE,
    else Discord's default avatar for the user."""
    uid = int(member["id"])
    key = member.get("avatar")
    if key:
        ext = "gif" if str(key).startswith("a_") else "png"
        return f"{CDN}/avatars/{uid}/{key}.{ext}?size={AVATAR_SIZE}", ext
    disc = str(member.get("discriminator") or "0")
    index = (uid >> 22) % 6 if disc in ("0", "0000") or not disc.isdigit() else int(disc) % 5
    return f"{CDN}/embed/avatars/{index}.png", "png"


async def _http_get(url: str) -> tuple[int, bytes, dict]:
    from copycord.common.http_session import shared_session
    async with shared_session().get(url) as resp:
        body = await resp.read() if resp.status == 200 else b""
        return resp.status, body, {"Retry-After": resp.headers.get("Retry-After")}


async def _sleep(ctx: Any, seconds: float) -> None:
    """Sleep, but wake at once on a cancel."""
    if seconds <= 0 or ctx.cancelled.is_set():
        return
    try:
        await asyncio.wait_for(ctx.cancelled.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def _download(ctx: Any, url: str) -> bytes:
    fetch = _wiring.fetch or _http_get
    error = "Failed"
    for attempt in range(RETRIES + 1):
        try:
            status, body, headers = await fetch(url)
        except Exception as exc:
            status, body, headers = 0, b"", {}
            error = str(exc) or type(exc).__name__
        if status == 200 and body:
            return body
        if status == 404:
            raise RuntimeError("Not found on Discord")
        if status == 429:
            try:
                wait = float((headers or {}).get("Retry-After") or 1.0)
            except (TypeError, ValueError):
                wait = 1.0
            error = "Rate limited"
        else:
            wait = float(2 ** attempt)
            if status:
                error = f"HTTP {status}"
        if attempt < RETRIES and not ctx.cancelled.is_set():
            await _sleep(ctx, min(wait, RETRY_WAIT_MAX))
    raise RuntimeError(error)


def _size_words(n: int) -> str:
    size = float(n)
    for unit in ("bytes", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{int(size)} {unit}" if unit == "bytes" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} bytes"


# run

async def run(ctx: Any, inputs: AvatarInputs) -> None:
    _ensure_listener()
    try:
        sweep()
    except Exception:
        log.exception("avatar downloads: sweep failed")
    gid = inputs.guild_id
    name = _guild_name(inputs)
    members_step = ctx.step("members", "Members")
    avatars_step = ctx.step("avatars", "Avatars")
    await ctx.log(f"Listing the members of {name} the client account can see")
    await members_step.start()

    job = _MemberJob(f"{ctx.run.id}:members", members_step)
    _jobs[job.job_id] = job
    members: list[dict] = []
    try:
        res = await _send({"type": "avatars_members", "data": {"job_id": job.job_id, "guild_id": gid}})
        end = {"ok": False, "error": res.get("error") or "The client did not take the job"} \
            if not res.get("ok") else await _wait_members(ctx, job)
        if end is not None and end.get("ok"):
            members = await _pull_members(job.job_id)
    finally:
        _jobs.pop(job.job_id, None)
        # Frees the client's copy of the list, or stops the listing on a cancel.
        await _send({"type": "avatars_members_cancel", "data": {"job_id": job.job_id}})

    member_count = _int((end or {}).get("member_count")) or job.member_count
    if end is None:
        await members_step.finish("cancelled", ctx.cancel_note or "Cancelled")
        await avatars_step.finish("cancelled", ctx.cancel_note or "Cancelled")
        await ctx.summary(members=job.found, downloaded=0, failed=0, skipped=0)
        await ctx.report(_report(inputs, name, members_found=job.found, member_count=member_count,
                                 partial=True, download=None, summary=ctx.run.summary, failures=[]))
        await ctx.log("Run cancelled", level="warning")
        return
    if not end.get("ok"):
        error = end.get("error") or "Listing the members failed"
        await members_step.finish("failed", error)
        await avatars_step.finish("skipped", "No members")
        await ctx.log(f"Members: {error}", level="error", step="members")
        raise RuntimeError(error)

    total = len(members)
    partial = bool(end.get("partial")) or (member_count is not None and total < member_count)
    await members_step.progress(total, member_count or total, label=None)
    await members_step.finish("success", (f"{_fmt(total)} of {_fmt(member_count)} visible" if partial and member_count
                                          else f"{_fmt(total)} found"))
    await ctx.summary(members=total)
    if partial:
        await ctx.log(f"Only part of the member list is visible to the client account: {_fmt(total)}"
                      + (f" of {_fmt(member_count)}" if member_count else "") + " members", level="warning",
                      step="members")
    else:
        await ctx.log(f"Found {_fmt(total)} members", step="members")
    if not total:
        await avatars_step.finish("skipped", "No members")
        await ctx.report(_report(inputs, name, members_found=0, member_count=member_count, partial=partial,
                                 download=None, summary={"members": 0, "downloaded": 0, "failed": 0, "skipped": 0},
                                 failures=[]))
        ctx.fail("The client account sees no members in this server")
        return

    await _download_all(ctx, inputs, name, members, member_count, partial, avatars_step)


async def _download_all(ctx: Any, inputs: AvatarInputs, name: str, members: list[dict],
                        member_count: Optional[int], partial: bool, step: Any) -> None:
    total = len(members)
    await step.start()
    await step.progress(0, total, label=None)
    root = _root()
    root.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(32)
    part = root / f"{token}.zip.part"
    zip_path, meta_path = _paths(token)
    counts = {"downloaded": 0, "failed": 0, "skipped": 0}
    failures: list[dict] = []
    manifest = io.StringIO()
    writer = csv.writer(manifest)
    writer.writerow(["user_id", "username", "global_name", "file", "status"])
    # The six default avatars are the same files for everyone: each comes from Discord once.
    defaults: dict[str, bytes] = {}
    done = 0
    try:
        with zipfile.ZipFile(part, "w", compression=zipfile.ZIP_STORED) as zf:
            for m in members:
                uid = str(m["id"])
                username = str(m.get("username") or uid)
                if ctx.cancelled.is_set():
                    counts["skipped"] += 1
                    writer.writerow([uid, username, m.get("global_name") or "", "", "skipped"])
                    continue
                url, ext = avatar_url(m)
                fname = f"{_safe(username)}_{uid}.{ext}"
                cached = url in defaults
                try:
                    body = defaults[url] if cached else await _download(ctx, url)
                    if "/embed/avatars/" in url:
                        defaults[url] = body
                    zf.writestr(fname, body)
                    counts["downloaded"] += 1
                    writer.writerow([uid, username, m.get("global_name") or "", fname,
                                     "default avatar" if "/embed/avatars/" in url else "downloaded"])
                except Exception as exc:
                    error = str(exc) or type(exc).__name__
                    counts["failed"] += 1
                    writer.writerow([uid, username, m.get("global_name") or "", "", f"failed: {error}"])
                    if len(failures) < REPORT_FAILURES:
                        failures.append({"user_id": uid, "username": username, "error": error})
                    await ctx.log(f"{username}: {error}", level="warning", step="avatars")
                done += 1
                await step.progress(done, total, label=username)
                await ctx.summary(**counts)
                if not cached and done < total:
                    await _sleep(ctx, DOWNLOAD_INTERVAL)
            zf.writestr("members.csv", manifest.getvalue())
    except Exception:
        try:
            part.unlink()
        except OSError:
            pass
        raise

    download = None
    if counts["downloaded"]:
        part.replace(zip_path)
        size = zip_path.stat().st_size
        expires_at = _wiring.clock() + inputs.expires_hours * 3600
        filename = f"avatars-{_safe(name, 'server')}-{ctx.run.number}.zip"
        meta = {"token": token, "run_id": ctx.run.id, "guild_id": inputs.guild_id, "guild_name": name,
                "filename": filename, "files": counts["downloaded"], "size": size, "expires_at": expires_at}
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        download = {"url": f"{URL_PREFIX}{token}", "filename": filename, "files": counts["downloaded"],
                    "size": size, "expires_at": iso(expires_at)}
    else:
        part.unlink(missing_ok=True)

    summary = {"members": total, **counts}
    await ctx.summary(**summary)
    await ctx.report(_report(inputs, name, members_found=total, member_count=member_count, partial=partial,
                             download=download, summary=summary, failures=failures))
    cancelled = ctx.cancelled.is_set()
    if cancelled:
        await step.finish("cancelled", f"Cancelled after {_fmt(done)} of {_fmt(total)}")
    elif counts["downloaded"]:
        await step.finish("success", f"{_fmt(counts['downloaded'])} downloaded"
                          + (f", {_fmt(counts['failed'])} failed" if counts["failed"] else ""))
    else:
        await step.finish("failed", "No avatar could be downloaded")
    if download:
        await ctx.log(f"Zip ready: {_fmt(counts['downloaded'])} avatars, {_size_words(download['size'])}; "
                      f"the link expires in {inputs.expires_hours} hours")
    if cancelled:
        await ctx.log("Run cancelled", level="warning")
    elif not counts["downloaded"]:
        ctx.fail("No avatar could be downloaded")
    else:
        await ctx.log(f"Done: {_fmt(counts['downloaded'])} downloaded, {_fmt(counts['failed'])} failed, "
                      f"{_fmt(counts['skipped'])} skipped")


def _report(inputs: AvatarInputs, name: str, *, members_found: int, member_count: Optional[int],
            partial: bool, download: Optional[dict], summary: dict, failures: list[dict]) -> dict:
    return {
        "inputs": inputs.model_dump(mode="json"),
        "guild": {"guild_id": inputs.guild_id, "name": name},
        "members": {"found": members_found, "member_count": member_count, "partial": partial},
        "download": download,
        "failures": failures,
        "summary": dict(summary),
    }


DOWNLOAD_AVATARS = register(TaskType(
    key=TYPE_KEY,
    title="Download avatars",
    description="Zip the profile pictures of every member the client account can see in one of its servers.",
    run=run,
    inputs=AvatarInputs,
    describe=describe,
    options=options,
    concurrency=1,
    requires_bots=True,
))
