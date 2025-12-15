# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations

import os
import json
import asyncio
import contextlib
from pathlib import Path
from typing import Optional, Dict, List

import aiohttp
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse


REMOTE_URLS: List[str] = [
    os.getenv(
        "LINKS_REMOTE_URL",
        "https://raw.githubusercontent.com/Copycord/Copycord/main/code/common/links.json",
    ),
    os.getenv(
        "LINKS_REMOTE_URL_FALLBACK",
        "https://cdn.jsdelivr.net/gh/Copycord/Copycord@main/code/common/links.json",
    ),
]
USER_AGENT = os.getenv(
    "LINKS_USER_AGENT", "Copycord-Admin/1.0 (+https://github.com/Copycord/Copycord)"
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
    Handles wrappers that expose the real app on `.app`.
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
    Live links loader with robust remote fetch, disk/local fallback, and in-place cache mutation.
    On every browser refresh (via /api/links or your page route), we force a remote fetch.
    If remote fails and cache is empty, we fall back to disk cache, then local JSON.
    """

    def __init__(
        self,
        urls: List[str] = REMOTE_URLS,
        ttl_seconds: int = LINKS_TTL_SECONDS,
        local_fallback: Optional[Path] = LINKS_LOCAL_FALLBACK,
        disk_cache: Optional[Path] = LINKS_DISK_CACHE,
    ):
        self.urls = [u for u in urls if u] or []
        self.ttl = ttl_seconds
        self.local_fallback = local_fallback
        self.disk_cache = disk_cache

        self._session: Optional[aiohttp.ClientSession] = None
        self._etag: Optional[str] = None
        self._cache: Dict[str, str] = {}
        self._lock = asyncio.Lock()

        self.last_source: str = "empty"
        self.last_http_status: Optional[int] = None
        self.last_error: Optional[str] = None

    async def attach_session(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    def _replace_cache(self, data: Dict[str, str], source: str) -> None:
        self._cache.clear()
        self._cache.update(data)
        self.last_source = source

    def _load_local(self) -> None:
        p = self.local_fallback
        if p and Path(p).exists():
            try:
                data = json.loads(Path(p).read_text("utf-8"))
                if isinstance(data, dict):
                    self._replace_cache(data, f"local:{p}")
            except Exception as e:
                self.last_error = f"local read error: {e}"

    def _load_disk_cache(self) -> None:
        p = self.disk_cache
        if p and Path(p).exists():
            try:
                data = json.loads(Path(p).read_text("utf-8"))
                if isinstance(data, dict):
                    self._replace_cache(data, f"disk:{p}")
            except Exception as e:
                self.last_error = f"disk read error: {e}"

    def _save_disk_cache(self) -> None:
        p = self.disk_cache
        if not p:
            return
        try:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            Path(p).write_text(json.dumps(self._cache, indent=2), encoding="utf-8")
        except Exception as e:
            self.last_error = f"disk write error: {e}"

    async def _fetch_remote(self) -> bool:
        if not self._session:
            self.last_error = "no session"
            return False

        self.last_http_status = None
        self.last_error = None

        headers = {
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }
        if self._etag:
            headers["If-None-Match"] = self._etag

        timeout = aiohttp.ClientTimeout(total=8)

        for url in self.urls:
            try:
                async with self._session.get(
                    url, headers=headers, timeout=timeout
                ) as resp:
                    self.last_http_status = resp.status
                    if resp.status == 304:
                        self.last_source = "remote-304"
                        return True
                    if resp.status == 200:

                        content_type = resp.headers.get("Content-Type", "")
                        try:
                            data = await resp.json()
                        except Exception:
                            text = await resp.text()
                            try:
                                data = json.loads(text)
                            except Exception:
                                self.last_error = f"invalid json from {url} (content-type={content_type})"
                                continue
                        if isinstance(data, dict):
                            self._replace_cache(data, f"remote:{url}")
                            self._etag = resp.headers.get("ETag")
                            self._save_disk_cache()
                            return True
                        else:
                            self.last_error = f"non-dict json from {url}"
                            continue
                    else:
                        snippet = ""
                        try:
                            snippet = (await resp.text())[:200]
                        except Exception:
                            pass
                        self.last_error = f"http {resp.status} from {url} {snippet!r}"

                        continue
            except Exception as e:
                self.last_error = f"request error for {url}: {e}"
                continue
        return False

    async def refresh(self, force: bool = False) -> None:
        async with self._lock:
            if force:
                self._etag = None

            ok = await self._fetch_remote()

            if not ok and not self._cache:
                self._load_disk_cache()
                if not self._cache:
                    self._load_local()

    async def get_links(self) -> Dict[str, str]:

        return dict(self._cache)


router = APIRouter()


@router.get("/api/links")
async def api_links(request: Request):
    mgr: LinksManager = request.app.state.links_mgr

    await mgr.refresh(force=True)
    return JSONResponse(await mgr.get_links())


@router.get("/api/links/debug")
async def links_debug(request: Request):
    mgr: LinksManager = request.app.state.links_mgr
    return JSONResponse(
        {
            "remote_urls": REMOTE_URLS,
            "etag": mgr._etag,
            "local_fallback": str(mgr.local_fallback) if mgr.local_fallback else None,
            "disk_cache": str(mgr.disk_cache) if mgr.disk_cache else None,
            "cache_empty": not bool(mgr._cache),
            "last_source": getattr(mgr, "last_source", "unknown"),
            "last_http_status": mgr.last_http_status,
            "last_error": mgr.last_error,
        }
    )


@router.post("/admin/links/refresh")
async def refresh_links(request: Request):
    mgr: LinksManager = request.app.state.links_mgr
    await mgr.refresh(force=True)
    return {"ok": True}


@router.post("/admin/links/purge")
async def links_purge(request: Request):
    mgr: LinksManager = request.app.state.links_mgr
    async with mgr._lock:
        mgr._etag = None
        mgr._cache.clear()
        mgr.last_source = "purged"
    return {"ok": True}


async def startup_links(
    app, templates_env=None, *, set_jinja_global: bool = False
) -> None:
    base_app = _unwrap_starlette_app(app)

    session = aiohttp.ClientSession(trust_env=True)
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
