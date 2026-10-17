# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""The client half of the Download avatars task: list the members of one server the account
can see.

User accounts cannot list members over REST, so the list is what the gateway already cached
for the server plus the answers to op 8 member searches by name prefix (the GUILD_MEMBERS_CHUNK
answers the selfclient's fetch_members collects), one prefix at a time and spaced out. A prefix
that comes back full is searched one character deeper. Discord answers each search with at most
QUERY_LIMIT members, so in a big server the list can stay partial; the end frame says so.

Frames: avatars_members {job_id, guild_id} starts a listing in the background and answers at
once; progress goes to the admin bus as avatars_members_progress {job_id, found, member_count}
and the listing ends with avatars_members_end {job_id, ok, error, found, member_count, partial}.
avatars_members_page {job_id, offset, limit} answers with the job's state and a slice of the
members; avatars_members_cancel {job_id} stops the listing and forgets the job."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

log = logging.getLogger("client.avatars")

FRAMES = ("avatars_members", "avatars_members_page", "avatars_members_cancel")
PREFIXES = "abcdefghijklmnopqrstuvwxyz0123456789_."
QUERY_LIMIT = 100
QUERY_TIMEOUT = 10.0
# Seconds between two searches, and the most searches one listing sends.
QUERY_INTERVAL = 0.6
MAX_QUERIES = 400
# A prefix this long is not searched deeper.
MAX_PREFIX = 3
PROGRESS_INTERVAL = 1.0
# How long a finished listing waits for its pages before it is dropped.
KEEP_DONE = 900.0


def member_record(m: Any) -> dict:
    """What the admin needs of one member to fetch its avatar."""
    avatar = getattr(m, "avatar", None)
    return {
        "id": str(m.id),
        "username": getattr(m, "name", "") or "",
        "global_name": getattr(m, "global_name", None),
        "discriminator": str(getattr(m, "discriminator", "0") or "0"),
        "avatar": getattr(avatar, "key", None) if avatar is not None else None,
    }


@dataclass
class _Job:
    job_id: str
    guild_id: int
    members: dict = field(default_factory=dict)
    state: str = "running"
    member_count: Optional[int] = None
    partial: bool = False
    error: Optional[str] = None
    task: Optional[asyncio.Task] = None
    finished_at: Optional[float] = None
    listing: Optional[list] = None
    last_progress: float = 0.0


class AvatarMembers:
    def __init__(self, bot: Callable[[], Any], publish: Callable[[dict], Awaitable[Any]],
                 interval: float = QUERY_INTERVAL):
        self._bot = bot
        self._publish = publish
        self._interval = interval
        self._jobs: dict[str, _Job] = {}

    async def handle(self, typ: str, data: dict) -> dict:
        data = data or {}
        job_id = str(data.get("job_id") or "")
        if typ == "avatars_members":
            return self._start(job_id, data.get("guild_id"))
        if typ == "avatars_members_page":
            return self._page(job_id, data.get("offset"), data.get("limit"))
        if typ == "avatars_members_cancel":
            job = self._jobs.pop(job_id, None)
            if job is not None and job.task is not None and not job.task.done():
                job.task.cancel()
            return {"ok": True}
        return {"ok": False, "error": f"unknown frame {typ}"}

    def _start(self, job_id: str, guild_id: Any) -> dict:
        self._drop_stale()
        try:
            gid = int(guild_id)
        except (TypeError, ValueError):
            return {"ok": False, "error": "bad guild id"}
        if not job_id:
            return {"ok": False, "error": "missing job id"}
        if job_id in self._jobs:
            return {"ok": True, "note": "already running"}
        job = _Job(job_id, gid)
        self._jobs[job_id] = job
        job.task = asyncio.ensure_future(self._run(job))
        return {"ok": True}

    def _drop_stale(self) -> None:
        now = time.monotonic()
        for job_id, job in list(self._jobs.items()):
            if job.finished_at is not None and now - job.finished_at > KEEP_DONE:
                del self._jobs[job_id]

    def _page(self, job_id: str, offset: Any, limit: Any) -> dict:
        job = self._jobs.get(job_id)
        if job is None:
            return {"ok": False, "state": "missing", "error": "The client has no such listing"}
        try:
            start, size = max(0, int(offset or 0)), max(0, int(limit or 0))
        except (TypeError, ValueError):
            start, size = 0, 0
        items: list = []
        if size:
            if job.listing is None or job.state == "running":
                job.listing = list(job.members.values())
            items = job.listing[start:start + size]
        return {**self._status(job), "ok": True, "state": job.state, "members": items}

    def _status(self, job: _Job) -> dict:
        return {"job_id": job.job_id, "found": len(job.members), "member_count": job.member_count,
                "partial": job.partial, "error": job.error}

    async def _emit(self, typ: str, data: dict) -> None:
        try:
            await self._publish({"type": typ, "data": data})
        except Exception:
            log.debug("avatars frame %s not sent", typ, exc_info=True)

    async def _progress(self, job: _Job, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - job.last_progress < PROGRESS_INTERVAL:
            return
        job.last_progress = now
        await self._emit("avatars_members_progress", self._status(job))

    def _add(self, job: _Job, members: Any) -> int:
        """Members not seen yet, by user id; returns how many of these were given."""
        n = 0
        for m in members or []:
            n += 1
            uid = getattr(m, "id", None)
            if uid and uid not in job.members:
                job.members[uid] = member_record(m)
        return n

    def _short(self, job: _Job) -> bool:
        return job.member_count is None or len(job.members) < job.member_count

    async def _run(self, job: _Job) -> None:
        try:
            await self._collect(job)
            job.partial = self._short(job)
            job.state = "done"
        except asyncio.CancelledError:
            job.state = "failed"
            job.error = "cancelled"
            raise
        except Exception as exc:
            log.exception("avatars: listing members of %s failed", job.guild_id)
            job.state = "failed"
            job.error = str(exc) or type(exc).__name__
        finally:
            job.finished_at = time.monotonic()
            job.listing = list(job.members.values())
        await self._emit("avatars_members_end", {**self._status(job), "ok": job.state == "done"})

    async def _collect(self, job: _Job) -> None:
        bot = self._bot()
        guild = bot.get_guild(job.guild_id) if bot is not None else None
        if guild is None:
            raise RuntimeError("The client account is not in this server")
        if getattr(bot, "gateway", None) is None:
            raise RuntimeError("The client is not connected to Discord")
        job.member_count = getattr(guild, "member_count", None) or None
        self._add(job, guild.members)
        await self._progress(job, force=True)
        queue = deque(PREFIXES)
        sent = 0
        while queue and sent < MAX_QUERIES and self._short(job):
            prefix = queue.popleft()
            if sent:
                await asyncio.sleep(self._interval)
            sent += 1
            try:
                got = await bot.fetch_members(job.guild_id, query=prefix, limit=QUERY_LIMIT, timeout=QUERY_TIMEOUT)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.debug("avatars: member search %r failed: %s", prefix, exc)
                continue
            if self._add(job, got) >= QUERY_LIMIT and len(prefix) < MAX_PREFIX:
                queue.extend(prefix + c for c in PREFIXES)
            await self._progress(job)
        await self._progress(job, force=True)
