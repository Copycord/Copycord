# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations

import os
import json
import time
import asyncio
import contextlib
from pathlib import Path
from typing import Optional, Dict

import aiohttp
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse


LINKS_REMOTE_URL = os.getenv(
    "LINKS_REMOTE_URL",
    "https://cdn.jsdelivr.net/gh/Copycord/Copycord@main/code/common/links.json",
)
LINKS_TTL_SECONDS = int(os.getenv("LINKS_TTL_SECONDS", "900"))
LINKS_LOCAL_FALLBACK = Path(__file__).resolve().parent.parent / "common" / "links.json"

LINKS_DISK_CACHE = Path(
    os.getenv(
        "LINKS_DISK_CACHE",
        str(Path(__file__).resolve().parent / "data" / "links.cache.json"),
    )
)


def _unwrap_starlette_app(obj):
    """
    Return the underlying Starlette/FastAPI app that has `.state`.
    Handles wrappers like custom ASGI adapters that expose `.app`.
    """
    cur = obj
    for _ in range(5):
        if hasattr(cur, "state"):
            return cur
        cur = getattr(cur, "app", None)
        if cur is None:
            break
    raise AttributeError("Could not locate underlying Starlette app with `.state`")


class LinksManager:
    """
    Fetches JSON link config from a remote URL (live repo), caches it in memory (+optional disk),
    and refreshes on a TTL. Supports ETag to minimize bandwidth and latency.

    No built-in defaults: returns only what it can retrieve from remote/disk/local.
    """

    def __init__(
        self,
        url: str = LINKS_REMOTE_URL,
        ttl_seconds: int = LINKS_TTL_SECONDS,
        local_fallback: Optional[Path] = LINKS_LOCAL_FALLBACK,
        disk_cache: Optional[Path] = LINKS_DISK_CACHE,
    ):
        self.url = url
        self.ttl = ttl_seconds
        self.local_fallback = local_fallback
        self.disk_cache = disk_cache

        self._session: Optional[aiohttp.ClientSession] = None
        self._etag: Optional[str] = None
        self._cache: Dict[str, str] = {}
        self._last_fetch: float = 0.0
        self._lock = asyncio.Lock()

    async def attach_session(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    def _load_local(self) -> None:
        if self.local_fallback and self.local_fallback.exists():
            try:
                data = json.loads(self.local_fallback.read_text("utf-8"))
                if isinstance(data, dict):
                    self._cache.update(data)
            except Exception:
                pass

    def _load_disk_cache(self) -> None:
        if self.disk_cache and self.disk_cache.exists():
            try:
                data = json.loads(self.disk_cache.read_text("utf-8"))
                if isinstance(data, dict):
                    self._cache.update(data)
            except Exception:
                pass

    def _save_disk_cache(self) -> None:
        if not self.disk_cache:
            return
        try:
            self.disk_cache.parent.mkdir(parents=True, exist_ok=True)
            self.disk_cache.write_text(
                json.dumps(self._cache, indent=2), encoding="utf-8"
            )
        except Exception:
            pass

    async def _fetch_remote(self) -> bool:
        if not self._session:
            return False
        headers = {}
        if self._etag:
            headers["If-None-Match"] = self._etag
        timeout = aiohttp.ClientTimeout(total=8)
        try:
            async with self._session.get(
                self.url, headers=headers, timeout=timeout
            ) as resp:
                if resp.status == 304:
                    return True
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, dict):
                        self._cache = data
                        self._etag = resp.headers.get("ETag")
                        self._save_disk_cache()
                        return True
        except Exception:
            return False
        return False

    async def refresh(self, force: bool = False) -> None:
        async with self._lock:
            now = time.time()
            if not force and (now - self._last_fetch) < self.ttl:
                return

            ok = await self._fetch_remote()

            if not ok and self._last_fetch == 0:

                self._cache = {}
                self._load_disk_cache()
                if not self._cache:
                    self._load_local()

            self._last_fetch = now

    async def get_links(self) -> Dict[str, str]:
        await self.refresh(force=False)
        return dict(self._cache)


router = APIRouter()


@router.get("/api/links")
async def api_links(request: Request):
    mgr: LinksManager = request.app.state.links_mgr
    return JSONResponse(await mgr.get_links())


@router.post("/admin/links/refresh")
async def refresh_links(request: Request):
    mgr: LinksManager = request.app.state.links_mgr
    await mgr.refresh(force=True)
    return {"ok": True}


async def startup_links(
    app, templates_env=None, *, set_jinja_global: bool = False
) -> None:
    """
    Call from app startup. Optionally inject a Jinja global so templates
    can use {{ links.github }} everywhere.
    """
    base_app = _unwrap_starlette_app(app)

    session = aiohttp.ClientSession()
    mgr = LinksManager()
    try:
        await mgr.attach_session(session)
        await mgr.refresh(force=True)

        base_app.state.http_session = session
        base_app.state.links_mgr = mgr
    except Exception:

        await session.close()
        raise

    if set_jinja_global and templates_env is not None:

        templates_env.globals["links"] = mgr._cache


async def shutdown_links(app) -> None:
    base_app = _unwrap_starlette_app(app)
    with contextlib.suppress(Exception):
        session = base_app.state.http_session
        await session.close()
