# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations
from collections import deque
import contextlib
import json
import os
import sqlite3
import uuid
import asyncio
import websockets
from pathlib import Path
import unicodedata
import tarfile, tempfile, shutil
import re
import time
import logging
from dotenv import load_dotenv
from typing import Dict, List, Set, Literal, Optional, Any, Union
from admin.auth import init_admin_auth
from admin.logging_setup import (
    LOGGER,
    get_logger,
    configure_app_logging,
    req_id_var,
    route_var,
    client_var,
    REDACT_KEYS,
)
from admin.standalone_scraper import StandaloneScraper, ScraperConfig, ScraperResult
from fastapi import (
    FastAPI,
    Request,
    WebSocket,
    WebSocketDisconnect,
    Body,
    status,
    HTTPException,
    File,
    UploadFile,
    Form,
    Query,
)
from anyio import EndOfStream
from fastapi.responses import (
    RedirectResponse,
    PlainTextResponse,
    StreamingResponse,
    JSONResponse,
    FileResponse,
    HTMLResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.types import Scope, Receive, Send
from starlette.datastructures import MutableHeaders
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from common.config import CURRENT_VERSION
from common.db import DBManager
from common.backup_scheduler import BackupConfig, DailySQLiteBackupScheduler
from admin.web_config import router as links_router
from admin.web_config import startup_links, shutdown_links
from contextlib import suppress
from time import perf_counter
import sys as _sys, json as _json, contextvars
from datetime import datetime
import aiohttp

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent


load_dotenv(PROJECT_ROOT / ".env")


GITHUB_REPO = os.getenv("GITHUB_REPO", "Copycord/Copycord")
RELEASE_POLL_SECONDS = int(os.getenv("RELEASE_POLL_SECONDS", "1800"))


def _set_ws_context(route: str, ws: WebSocket):
    route_var.set(route)

    c = getattr(ws, "client", None)
    if c:
        client_var.set(f"{getattr(c, 'host', '?')}:{getattr(c, 'port', '?')}")
    else:
        client_var.set("-")

    req_id_var.set(uuid.uuid4().hex[:8])


def _redact_dict(d: dict) -> dict:
    try:
        rd = dict(d or {})
        for k in REDACT_KEYS:
            if k in rd and rd[k]:
                rd[k] = "***REDACTED***"
        return rd
    except Exception:
        return {"_redact_error": True}


def _redact_token(tok: str) -> str:
    """
    Keep only the first 8 chars of a token for debug logs.
    """
    if not tok:
        return "<empty>"
    t = str(tok)
    return t[:8] + "...len=" + str(len(t))


class _Timer:
    def __init__(self, label: str):
        self.label = label
        self._t0 = None
        self.ms = 0.0

    def __enter__(self):
        self._t0 = perf_counter()
        return self

    def __exit__(self, *exc):
        self.ms = (perf_counter() - self._t0) * 1000.0


def _safe(x):
    try:
        s = str(x)
        return (s[:500] + "…") if len(s) > 500 else s
    except Exception:
        return "<unprintable>"


APP_TITLE = "Copycord"

DEFAULT_DATA_DIR = "/data"
DATA_DIR = Path(os.getenv("DATA_DIR", DEFAULT_DATA_DIR)).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = os.getenv("DB_PATH", str(DATA_DIR / "data.db"))
db = DBManager(DB_PATH, init_schema=True)

BACKUP_DIR = Path(os.getenv("BACKUP_DIR", str(DATA_DIR / "backups")))
BACKUP_RETAIN = int(os.getenv("BACKUP_RETAIN", "14"))
BACKUP_AT = os.getenv("BACKUP_AT", "03:17")
BACKUP_TZ = os.getenv("TZ", "UTC")

_backup_cfg = BackupConfig(
    db_path=DB_PATH,
    backup_dir=BACKUP_DIR,
    retain=BACKUP_RETAIN,
    run_at=BACKUP_AT,
    timezone=BACKUP_TZ,
)


async def _record_backup_stats(archive_path: Path):
    try:
        size = archive_path.stat().st_size if archive_path.exists() else 0
        now_iso = datetime.utcnow().isoformat() + "Z"
        db.set_config("DB_LAST_BACKUP_AT", now_iso)
        db.set_config("DB_LAST_BACKUP_FILE", archive_path.name)
        db.set_config("DB_LAST_BACKUP_SIZE", str(size))
    except Exception as e:
        LOGGER.exception("Failed writing backup stats: %s", e)


backup_scheduler = DailySQLiteBackupScheduler(
    cfg=_backup_cfg,
    logger=LOGGER,
    on_complete=_record_backup_stats,
)


SERVER_CTRL_URL = os.getenv("WS_SERVER_CTRL_URL", "ws://server:9101")
CLIENT_CTRL_URL = os.getenv("WS_CLIENT_CTRL_URL", "ws://client:9102")

CLIENT_AGENT_URL = os.getenv("WS_CLIENT_URL", "ws://client:8766")
SERVER_AGENT_URL = os.getenv("WS_SERVER_URL", "ws://server:8765")


ALLOWED_ENV = [
    "SERVER_TOKEN",
    "CLIENT_TOKEN",
    "COMMAND_USERS",
    "LOG_LEVEL",
]

REQUIRED = ["SERVER_TOKEN", "CLIENT_TOKEN"]

BOOL_KEYS = [
    "ENABLE_CLONING",
    "CLONE_MESSAGES",
    "DELETE_CHANNELS",
    "DELETE_THREADS",
    "DELETE_MESSAGES",
    "EDIT_MESSAGES",
    "RESEND_EDITED_MESSAGES",
    "TAG_REPLY_MSG",
    "REPOSITION_CHANNELS",
    "CLONE_VOICE",
    "CLONE_VOICE_PROPERTIES",
    "CLONE_STAGE",
    "CLONE_STAGE_PROPERTIES",
    "SYNC_FORUM_PROPERTIES",
    "RENAME_CHANNELS",
    "SYNC_CHANNEL_NSFW",
    "SYNC_CHANNEL_TOPIC",
    "SYNC_CHANNEL_SLOWMODE",
    "CLONE_ROLES",
    "DELETE_ROLES",
    "UPDATE_ROLES",
    "REARRANGE_ROLES",
    "CLONE_EMOJI",
    "CLONE_STICKER",
    "MIRROR_ROLE_PERMISSIONS",
    "MIRROR_CHANNEL_PERMISSIONS",
    "CLONE_GUILD_ICON",
    "CLONE_GUILD_BANNER",
    "CLONE_GUILD_SPLASH",
    "CLONE_GUILD_DISCOVERY_SPLASH",
    "SYNC_GUILD_DESCRIPTION",
    "ANONYMIZE_USERS",
    "DISABLE_EVERYONE_MENTIONS",
    "DISABLE_ROLE_MENTIONS",
    "DB_CLEANUP_MSG",
]
DEFAULTS: Dict[str, Union[bool, str]] = {
    "DELETE_CHANNELS": True,
    "DELETE_THREADS": True,
    "DELETE_ROLES": True,
    "UPDATE_ROLES": True,
    "EDIT_MESSAGES": True,
    "RESEND_EDITED_MESSAGES": True,
    "REPOSITION_CHANNELS": True,
    "DELETE_MESSAGES": True,
    "CLONE_EMOJI": True,
    "CLONE_STICKER": True,
    "CLONE_ROLES": True,
    "REARRANGE_ROLES": False,
    "MIRROR_ROLE_PERMISSIONS": False,
    "ENABLE_CLONING": True,
    "CLONE_MESSAGES": True,
    "LOG_LEVEL": "INFO",
    "COMMAND_USERS": "",
    "MIRROR_CHANNEL_PERMISSIONS": False,
    "RENAME_CHANNELS": True,
    "SYNC_CHANNEL_NSFW": False,
    "SYNC_CHANNEL_TOPIC": False,
    "SYNC_CHANNEL_SLOWMODE": False,
    "CLONE_VOICE": True,
    "CLONE_VOICE_PROPERTIES": False,
    "CLONE_GUILD_ICON": False,
    "CLONE_GUILD_BANNER": False,
    "CLONE_GUILD_SPLASH": False,
    "CLONE_GUILD_DISCOVERY_SPLASH": False,
    "SYNC_GUILD_DESCRIPTION": False,
    "SYNC_FORUM_PROPERTIES": False,
    "CLONE_STAGE": True,
    "CLONE_STAGE_PROPERTIES": False,
    "ANONYMIZE_USERS": False,
    "DISABLE_EVERYONE_MENTIONS": False,
    "DISABLE_ROLE_MENTIONS": False,
    "TAG_REPLY_MSG": False,
    "DB_CLEANUP_MSG": True,
}

app = FastAPI(title=APP_TITLE)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals.setdefault("links", {})
app.include_router(links_router)
shutdown_event = asyncio.Event()
init_admin_auth(app, templates, DATA_DIR)


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404 and "text/html" in (request.headers.get("accept") or ""):
        return templates.TemplateResponse(
            "404.html",
            {
                "request": request,
                "version": CURRENT_VERSION,
            },
            status_code=404,
        )
    return JSONResponse(
        {"detail": exc.detail},
        status_code=exc.status_code,
    )


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:8]
        token_r = req_id_var.set(rid)
        token_s = route_var.set(request.url.path or "-")
        token_c = client_var.set(
            f"{getattr(request.client, 'host', '?')}:{getattr(request.client, 'port', '?')}"
        )

        response = None
        try:
            try:
                response = await call_next(request)
            except EndOfStream:

                LOGGER.info(
                    "Client disconnected during request body read",
                )

                return PlainTextResponse("client disconnected", status_code=499)
            except asyncio.CancelledError:

                LOGGER.debug("Request task cancelled (client gone)")
                return PlainTextResponse("client disconnected", status_code=499)
        finally:

            if response is not None:
                try:
                    response.headers["X-Request-ID"] = rid
                except Exception:
                    pass

            req_id_var.reset(token_r)
            route_var.reset(token_s)
            client_var.reset(token_c)

        return response


app.add_middleware(RequestContextMiddleware)


class ConnCloseOnShutdownASGI:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def send_wrapper(message):
            if message["type"] == "http.response.start" and shutdown_event.is_set():
                headers = MutableHeaders(raw=message.setdefault("headers", []))
                headers["Connection"] = "close"
                LOGGER.debug(
                    "ConnCloseOnShutdownASGI | injected Connection: close for path=%s",
                    scope.get("path"),
                )
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except asyncio.CancelledError:
            LOGGER.debug(
                "ConnCloseOnShutdownASGI | request cancelled path=%s", scope.get("path")
            )
            return
        except Exception:
            LOGGER.exception(
                "ASGI pipeline error at path=%s method=%s",
                scope.get("path"),
                scope.get("method"),
            )
            raise


class BusHub:
    def __init__(self):
        self.status = {"server": {}, "client": {}}
        self.subscribers: Set[asyncio.Queue[str]] = set()
        self.ui_sockets: Set[WebSocket] = set()
        self.lock = asyncio.Lock()
        self.recent = deque(maxlen=200)

    def subscribe(self) -> asyncio.Queue[str]:
        q = asyncio.Queue(maxsize=200)
        self.subscribers.add(q)
        LOGGER.debug("BusHub.subscribe | subscribers=%d", len(self.subscribers))
        return q

    async def remove_ui(self, ws: WebSocket):
        async with self.lock:
            self.ui_sockets.discard(ws)

    def unsubscribe(self, q: asyncio.Queue[str]):
        self.subscribers.discard(q)
        LOGGER.debug("BusHub.unsubscribe | subscribers=%d", len(self.subscribers))

    def _mkmsg(self, kind, role, payload=None):
        return json.dumps({"kind": kind, "role": role, "payload": payload or {}})

    def _normalize(self, obj: dict) -> dict:
        if not isinstance(obj, dict):
            return {"kind": "log", "role": "unknown", "payload": {"raw": _safe(obj)}}
        kind = obj.get("kind") or obj.get("type") or "event"
        role = obj.get("role") or "unknown"
        payload = obj.get("payload")
        if payload is None:
            payload = {k: v for k, v in obj.items() if k not in ("kind", "role")}
        return {"kind": kind, "role": role, "payload": payload or {}}

    async def publish(self, kind: str, role: str, payload: dict):
        if kind == "status" and role in ("server", "client"):
            self.status[role] = payload or {}

        rec = {"kind": kind, "role": role, "payload": payload or {}}
        self.recent.append(rec)

        text = json.dumps(rec, separators=(",", ":"))

        dead_q = []
        for q in list(self.subscribers):
            try:
                q.put_nowait(text)
            except asyncio.QueueFull:
                dead_q.append(q)
        for q in dead_q:
            self.subscribers.discard(q)

        await self._broadcast_text(text)

        LOGGER.debug(
            "BusHub.publish | kind=%s role=%s sse=%d ui=%d recent=%d",
            kind,
            role,
            len(self.subscribers),
            len(self.ui_sockets),
            len(self.recent),
        )

    async def add_ui(self, ws: WebSocket):
        async with self.lock:
            self.ui_sockets.add(ws)
        LOGGER.debug("BusHub.add_ui | ui_sockets=%d", len(self.ui_sockets))
        for role, payload in self.status.items():
            if payload:
                await ws.send_text(self._mkmsg("status", role, payload))
        for m in list(self.recent)[-20:]:
            try:
                rec = self._normalize(m)
                await ws.send_text(json.dumps(rec, separators=(",", ":")))
            except Exception as e:
                LOGGER.debug("BusHub.add_ui replay failed: %s", repr(e))

    async def broadcast(self, obj: dict):
        rec = self._normalize(obj)
        self.recent.append(rec)
        text = json.dumps(rec, separators=(",", ":"))

        dead_q = []
        for q in list(self.subscribers):
            try:
                q.put_nowait(text)
            except asyncio.QueueFull:
                dead_q.append(q)
        for q in dead_q:
            self.subscribers.discard(q)

        await self._broadcast_text(text)
        LOGGER.debug(
            "BusHub.broadcast | kind=%s role=%s ui_sockets=%d",
            rec.get("kind"),
            rec.get("role"),
            len(self.ui_sockets),
        )

    async def _broadcast_text(self, text: str):
        dead = []
        async with self.lock:
            for ws in list(self.ui_sockets):
                try:
                    await ws.send_text(text)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                with suppress(Exception):
                    await ws.close()
                self.ui_sockets.discard(ws)
        if dead:
            LOGGER.debug(
                "BusHub._broadcast_text | cleaned_dead=%d remaining=%d",
                len(dead),
                len(self.ui_sockets),
            )


hub = BusHub()
agent_sockets: Set[WebSocket] = set()
bus_sockets: Set[WebSocket] = set()


scraper_state = {
    "running": False,
    "scraper": None,
    "task": None,
    "result": None,
    "guild_id": None,
    "started_at": None,
    "queue": [],          # list of dicts: {id, guild_id, include_username, include_avatar_url, include_bio, include_roles, proxies, status, added_at}
    "queue_task": None,   # the asyncio.Task running the queue processor
    "queue_running": False,
}
scraper_lock = asyncio.Lock()
_queue_id_counter = 0


def _next_queue_id():
    global _queue_id_counter
    _queue_id_counter += 1
    return _queue_id_counter


class BackfillLocks:
    def __init__(self, ttl_launching_sec: float = 20.0):
        self._launching_ttl = ttl_launching_sec
        self._launching: Dict[int, float] = {}
        self._running: set[int] = set()
        self._lock = asyncio.Lock()

    async def clear_all(self):
        async with self._lock:
            self._launching.clear()
            self._running.clear()

    async def try_acquire_launching(
        self, channel_id: int, cloned_guild_id: int | None
    ) -> bool:

        key = int(channel_id)
        now = time.time()
        async with self._lock:

            self._launching = {
                k: exp for k, exp in self._launching.items() if exp > now
            }
            if key in self._running or key in self._launching:
                return False
            self._launching[key] = now + self._launching_ttl
            return True

    async def promote_to_running(self, channel_id: int, cloned_guild_id: int | None):
        key = int(channel_id)
        async with self._lock:
            self._launching.pop(key, None)
            self._running.add(key)

    async def release(self, channel_id: int, cloned_guild_id: int | None):
        key = int(channel_id)
        async with self._lock:
            self._launching.pop(key, None)
            self._running.discard(key)

    async def status(
        self, channel_id: int, cloned_guild_id: int | None
    ) -> Literal["idle", "launching", "running"]:
        key = int(channel_id)
        now = time.time()
        async with self._lock:
            if key in self._running:
                return "running"
            if self._launching.get(key, 0) > now:
                return "launching"
            return "idle"


locks = BackfillLocks()


async def _lock_listener():
    q = hub.subscribe()
    while True:
        raw = await q.get()
        try:
            ev = json.loads(raw)
        except Exception:
            continue
        if ev.get("kind") != "client":
            continue
        p = ev.get("payload") or {}
        t = p.get("type")
        d = p.get("data") or {}
        cid = d.get("channel_id") or p.get("channel_id")

        try:
            cid = int(cid)
        except Exception:
            continue

        gid = d.get("cloned_guild_id") or p.get("cloned_guild_id")
        try:
            gid = int(gid) if gid is not None else 0
        except Exception:
            gid = 0

        if t in ("backfill_ack",):
            await locks.promote_to_running(cid, gid)
        elif t in ("backfill_done",):
            await locks.release(cid, gid)
        elif t in ("backfill_busy",):
            await locks.promote_to_running(cid, gid)


async def _close_ws_quietly(
    ws: WebSocket, code: int = 1001, reason: str = "server shutdown"
):
    with contextlib.suppress(RuntimeError, WebSocketDisconnect, Exception):
        await ws.close(code=code, reason=reason)


DISCORD_API_BASE = "https://discord.com/api/v10"


async def _check_client_token_valid(raw_token: str) -> bool:
    """
    Returns True if CLIENT_TOKEN (selfbot/user token) is a valid *user* session.
    We hit /users/@me with the raw token and also reject bot tokens here.
    """
    token = (raw_token or "").strip()
    if not token:
        LOGGER.debug("_check_client_token_valid | no token provided")
        return False

    url = f"{DISCORD_API_BASE}/users/@me"
    headers = {
        "Authorization": token,
        "User-Agent": "Copycord-ConfigCheck/1.0",
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, timeout=10) as resp:
                body_preview = None
                is_bot = None
                uid = None
                uname = None

                try:
                    j = await resp.json()
                    uid = j.get("id")
                    uname = j.get("username")
                    is_bot = bool(j.get("bot", False))
                    body_preview = {
                        "id": uid,
                        "username": uname,
                        "bot": is_bot,
                        "keys": list(j.keys())[:10],
                    }
                except Exception:
                    body_preview = "<non-json or parse-failed>"

                ok = (resp.status == 200) and not is_bot

                LOGGER.debug(
                    "_check_client_token_valid | status=%s ok=%s token=%s is_bot=%s body_preview=%s",
                    resp.status,
                    ok,
                    _redact_token(token),
                    is_bot,
                    body_preview,
                )

                if resp.status == 200 and is_bot:
                    LOGGER.warning(
                        "_check_client_token_valid | token looks like a bot token; rejecting for CLIENT_TOKEN use"
                    )

                return ok
    except Exception as e:
        LOGGER.warning(
            "_check_client_token_valid | exception=%s token=%s",
            repr(e),
            _redact_token(token),
        )
        return False


async def _check_server_token_valid(bot_token: str) -> bool:
    """
    Returns True if SERVER_TOKEN (bot token) is valid and actually a bot.
    We hit /users/@me with Authorization: Bot <token>.
    """
    token = (bot_token or "").strip()
    if not token:
        LOGGER.debug("_check_server_token_valid | no token provided")
        return False

    url = f"{DISCORD_API_BASE}/users/@me"
    headers = {
        "Authorization": f"Bot {token}",
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, timeout=10) as resp:
                body_preview = None
                is_bot = None
                uid = None
                uname = None

                try:
                    j = await resp.json()
                    uid = j.get("id")
                    uname = j.get("username")
                    is_bot = bool(j.get("bot", False))
                    body_preview = {
                        "id": uid,
                        "username": uname,
                        "bot": is_bot,
                        "keys": list(j.keys())[:10],
                    }
                except Exception:
                    body_preview = "<non-json or parse-failed>"

                ok = (resp.status == 200) and bool(is_bot)

                LOGGER.debug(
                    "_check_server_token_valid | status=%s ok=%s bot_token=%s is_bot=%s body_preview=%s",
                    resp.status,
                    ok,
                    _redact_token(token),
                    is_bot,
                    body_preview,
                )

                if resp.status == 200 and not is_bot:
                    LOGGER.warning(
                        "_check_server_token_valid | token is not a bot user; rejecting for SERVER_TOKEN use"
                    )

                return ok
    except Exception as e:
        LOGGER.warning(
            "_check_server_token_valid | exception=%s bot_token=%s",
            repr(e),
            _redact_token(token),
        )
        return False


async def _verify_tokens_for_save(values: dict[str, str]) -> list[str]:
    """
    Run both checks and build human-friendly error messages.
    Called by /save before we actually persist.
    """
    errs: list[str] = []

    raw_client = values.get("CLIENT_TOKEN", "") or ""
    raw_server = values.get("SERVER_TOKEN", "") or ""

    LOGGER.debug(
        "_verify_tokens_for_save | starting client_token=%s server_token=%s",
        _redact_token(raw_client),
        _redact_token(raw_server),
    )

    if (
        raw_client.strip()
        and raw_server.strip()
        and raw_client.strip() == raw_server.strip()
    ):
        errs.append(
            "CLIENT_TOKEN and SERVER_TOKEN must be different tokens (account vs bot)."
        )
        return errs

    client_ok = await _check_client_token_valid(raw_client)
    server_ok = await _check_server_token_valid(raw_server)

    LOGGER.debug(
        "_verify_tokens_for_save | results client_ok=%s server_ok=%s",
        client_ok,
        server_ok,
    )

    if not client_ok:
        errs.append(
            "CLIENT_TOKEN: Discord account token is invalid or appears to be a bot token."
        )

    if not server_ok:
        errs.append(
            "SERVER_TOKEN: Discord bot token is invalid or does not look like a bot account."
        )

    return errs


async def _selfbot_in_guild(client_token: str, guild_id: int) -> bool:
    """
    Returns True if the user account (CLIENT_TOKEN / self bot)
    is a member of guild_id.
    Strategy: Paginate GET /users/@me/guilds?limit=200&after=<last_id>
              until we find the guild or run out of pages.
    Handles 429 rate-limits by sleeping and retrying.
    """
    if not client_token or not guild_id:
        LOGGER.debug(
            "_selfbot_in_guild | missing client_token or guild_id token=%s guild_id=%s",
            _redact_token(client_token),
            guild_id,
        )
        return False

    base_url = f"{DISCORD_API_BASE}/users/@me/guilds"
    headers = {
        "Authorization": client_token,
    }
    wanted = str(guild_id)

    try:
        async with aiohttp.ClientSession() as sess:
            after = "0"
            total_checked = 0

            while True:
                url = f"{base_url}?limit=200&after={after}"
                async with sess.get(url, headers=headers, timeout=10) as resp:
                    status = resp.status

                    if status == 429:
                        try:
                            body = await resp.json()
                            retry_after = float(body.get("retry_after", 2))
                        except Exception:
                            retry_after = 2.0
                        LOGGER.info(
                            "_selfbot_in_guild | rate limited, retrying in %.1fs token=%s",
                            retry_after,
                            _redact_token(client_token),
                        )
                        await asyncio.sleep(retry_after + 0.25)
                        continue

                    if status != 200:
                        LOGGER.warning(
                            "_selfbot_in_guild | discord status=%s token=%s guild_id=%s -> not_member",
                            status,
                            _redact_token(client_token),
                            guild_id,
                        )
                        return False

                    data = await resp.json()
                    if not data:
                        break

                    for g in data:
                        gid = str(g.get("id", ""))
                        if gid == wanted:
                            LOGGER.debug(
                                "_selfbot_in_guild | found guild_id=%s for token=%s after checking %d guilds",
                                guild_id,
                                _redact_token(client_token),
                                total_checked + 1,
                            )
                            return True

                    total_checked += len(data)
                    after = str(data[-1].get("id", "0"))

                    if len(data) < 200:
                        break

            LOGGER.debug(
                "_selfbot_in_guild | guild_id=%s NOT found for token=%s total_guilds_checked=%d",
                guild_id,
                _redact_token(client_token),
                total_checked,
            )
            return False
    except Exception as e:
        LOGGER.warning(
            "_selfbot_in_guild | exception=%s token=%s guild_id=%s",
            repr(e),
            _redact_token(client_token),
            guild_id,
        )
        return False


async def _bot_in_guild(server_token: str, guild_id: int) -> bool:
    """
    Returns True if the bot (SERVER_TOKEN) is in guild_id.
    Strategy: GET /guilds/{guild_id} with Bot <token>.
    If the bot is *not* in that guild, Discord responds 403.
    """
    if not server_token or not guild_id:
        LOGGER.debug(
            "_bot_in_guild | missing server_token or guild_id bot=%s guild_id=%s",
            _redact_token(server_token),
            guild_id,
        )
        return False

    url = f"{DISCORD_API_BASE}/guilds/{guild_id}?with_counts=true"
    headers = {
        "Authorization": f"Bot {server_token}",
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, timeout=10) as resp:
                status = resp.status
                ok = status == 200

                body_preview = None
                try:
                    j = await resp.json()
                    body_preview = {
                        "id": j.get("id"),
                        "name": j.get("name"),
                        "approx_member_count": j.get("approximate_member_count"),
                        "keys": list(j.keys())[:10],
                    }
                except Exception:
                    body_preview = "<non-json or parse-failed>"

                LOGGER.debug(
                    "_bot_in_guild | status=%s ok=%s bot=%s guild_id=%s body_preview=%s",
                    status,
                    ok,
                    _redact_token(server_token),
                    guild_id,
                    body_preview,
                )

                return ok
    except Exception as e:
        LOGGER.warning(
            "_bot_in_guild | exception=%s bot=%s guild_id=%s",
            repr(e),
            _redact_token(server_token),
            guild_id,
        )
        return False


@app.websocket("/bus")
async def admin_bus(ws: WebSocket):
    await ws.accept()
    _set_ws_context("/bus", ws)
    bus_sockets.add(ws)
    socket_id = id(ws)
    local_log = get_logger("copycord.ws.bus", socket_id=socket_id)
    route_var.set("/bus")
    client_var.set("-")

    local_log.info("WS connected | peers=%d", len(bus_sockets))
    count = 0
    try:
        while True:
            raw = await ws.receive_text()
            local_log.debug("Recv | raw=%s", raw[:300])
            count += 1
            try:
                ev = json.loads(raw)
                local_log.debug(
                    "Parsed | kind=%s role=%s keys=%s",
                    ev.get("kind"),
                    ev.get("role"),
                    list(ev.keys()),
                )
            except Exception:
                ev = {"kind": "log", "role": "unknown", "payload": {"raw": raw}}
                local_log.warning("JSON parse failed | raw=%s", raw[:200])
            if not isinstance(ev, dict):
                ev = {"kind": "log", "role": "unknown", "payload": {"raw": _safe(ev)}}
            kind = ev.get("kind") or "log"
            role = ev.get("role") or "unknown"
            payload = ev.get("payload") or {}
            await hub.publish(kind, role, payload)
            if count % 50 == 0:
                local_log.debug("Forwarded=%d", count)
    except WebSocketDisconnect:
        local_log.info("WS disconnected | forwarded=%d", count)
    finally:
        bus_sockets.discard(ws)


@app.get("/bus/stream")
async def bus_stream(request: Request):

    client = request.client
    client_addr = f"{getattr(client, 'host', '?')}:{getattr(client, 'port', '?')}"
    conn_id = uuid.uuid4().hex[:8]

    local_log = get_logger("copycord.sse", conn_id=conn_id)
    route_var.set("/bus/stream")
    client_var.set(client_addr)

    local_log.info("Client connected")

    async def gen():
        q = hub.subscribe()
        events_sent = 0
        heartbeats_sent = 0

        def _summarize(msg: str) -> str:
            try:
                obj = json.loads(msg)
                kind = obj.get("kind") or obj.get("type") or "?"
                role = obj.get("role") or obj.get("source") or "-"
                return f"kind={kind} role={role} len={len(msg)}"
            except Exception:
                return f"kind=? (non-json) len={len(msg)}"

        try:

            initial = 0
            for role, payload in hub.status.items():
                if payload:
                    data = json.dumps(
                        {"kind": "status", "role": role, "payload": payload}
                    )
                    yield f"data: {data}\n\n"
                    initial += 1
                    events_sent += 1
            local_log.debug("Initial status flush | entries=%d", initial)

            while not shutdown_event.is_set():
                if await request.is_disconnected():
                    local_log.info(
                        "Client disconnected",
                        extra={
                            "events_sent": events_sent,
                            "heartbeats": heartbeats_sent,
                        },
                    )
                    return
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=1.0)
                    local_log.debug(
                        "Yield event | %s | qsize=%d", _summarize(msg), q.qsize()
                    )
                    yield f"data: {msg}\n\n"
                    events_sent += 1
                except asyncio.TimeoutError:
                    yield ":ka\n\n"
                    heartbeats_sent += 1
                    if heartbeats_sent % 60 == 0:
                        local_log.debug(
                            "Heartbeat checkpoint",
                            extra={"heartbeats": heartbeats_sent},
                        )
        except asyncio.CancelledError:
            local_log.debug(
                "Closed by client",
                extra={"events_sent": events_sent, "heartbeats": heartbeats_sent},
            )
            return
        finally:
            hub.unsubscribe(q)
            local_log.info(
                "Closed",
                extra={"events_sent": events_sent, "heartbeats": heartbeats_sent},
            )

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.websocket("/ws/ui")
async def ws_ui(ws: WebSocket):
    await ws.accept()
    _set_ws_context("/ws/ui", ws)
    hub.ui_sockets.add(ws)
    socket_id = id(ws)
    local_log = get_logger("copycord.ws.ui", socket_id=socket_id)
    route_var.set("/ws/ui")
    client_var.set("-")

    local_log.info("Connected | ui_sockets=%d", len(hub.ui_sockets))

    backlog = list(hub.recent)[-20:]
    for m in backlog:
        await ws.send_text(json.dumps(m))
    local_log.debug("Sent backlog | count=%d", len(backlog))

    try:
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
    except WebSocketDisconnect:
        local_log.info("Disconnected")
    except asyncio.CancelledError:
        local_log.debug("Cancelled")
        return
    finally:
        hub.ui_sockets.discard(ws)
        local_log.debug("Removed | ui_sockets=%d", len(hub.ui_sockets))


@app.websocket("/ws/out")
async def ws_out(websocket: WebSocket):
    await websocket.accept()
    _set_ws_context("/ws/out", websocket)
    await hub.add_ui(websocket)

    socket_id = id(websocket)
    local_log = get_logger("copycord.ws.out", socket_id=socket_id)
    route_var.set("/ws/out")
    client_var.set("-")

    local_log.info("Client connected", extra={"events_sent": 0})
    local_log.debug("WebSocket attached to hub")

    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        local_log.info("Client disconnected", extra={"events_sent": 0})
    except asyncio.CancelledError:
        local_log.debug("Connection cancelled (client closed)")
        return
    finally:
        await hub.remove_ui(websocket)
        local_log.debug("Cleanup complete | active_ui_sockets=%d", len(hub.ui_sockets))


@app.websocket("/ws/in")
async def ws_in(websocket: WebSocket):
    await websocket.accept()
    _set_ws_context("/ws/in", websocket)
    agent_sockets.add(websocket)

    socket_id = id(websocket)
    local_log = get_logger("copycord.ws.in", socket_id=socket_id)
    route_var.set("/ws/in")
    client_var.set("-")

    local_log.info("Agent connected", extra={"forwarded": 0})

    forwarded = 0
    try:
        while not shutdown_event.is_set():
            try:
                ev = await asyncio.wait_for(websocket.receive(), timeout=1.0)
                local_log.debug(
                    "Event received | type=%s keys=%s", ev.get("type"), list(ev.keys())
                )
            except asyncio.TimeoutError:
                continue

            typ = ev.get("type")
            if typ == "websocket.disconnect":
                local_log.debug("Disconnect signal received")
                break
            if typ != "websocket.receive":
                local_log.debug("Ignored non-receive event | type=%s", typ)
                continue

            raw = ev.get("text")
            if raw:
                local_log.debug(
                    "Raw text received | length=%d | preview=%s", len(raw), raw[:200]
                )
            else:
                raw_bytes = ev.get("bytes") or []
                local_log.debug("Raw bytes received | length=%d", len(raw_bytes))
                if raw_bytes:
                    try:
                        raw = raw_bytes.decode("utf-8", "ignore")
                    except Exception as e:
                        local_log.warning("Failed to decode raw bytes | error=%s", e)
                        continue

            try:
                msg = json.loads(raw)
                local_log.debug("JSON parsed successfully | keys=%s", list(msg.keys()))
            except Exception:
                msg = {"type": "raw", "data": raw}
                local_log.warning("JSON parse failed | raw_preview=%s", raw[:200])

            if isinstance(msg, dict) and ("kind" in msg or "payload" in msg):
                await hub.publish(
                    kind=msg.get("kind") or msg.get("type") or "event",
                    role=msg.get("role") or "ui",
                    payload=msg.get("payload") or {},
                )
                forwarded += 1
                local_log.debug(
                    "Published message to hub", extra={"forwarded": forwarded}
                )
                with contextlib.suppress(Exception):
                    await websocket.send_text('{"ok":true}')
                continue

            out = {
                "kind": "agent",
                "role": msg.get("role") or "unknown",
                "type": msg.get("type") or "event",
                "ts": msg.get("ts"),
                "data": msg.get("data", {}),
            }

            await hub.broadcast(out)
            forwarded += 1
            local_log.debug("Broadcast message", extra={"forwarded": forwarded})
            if forwarded % 100 == 0:
                local_log.info(
                    "Forwarding checkpoint reached", extra={"forwarded": forwarded}
                )

            with contextlib.suppress(Exception):
                await websocket.send_text('{"ok":true}')

    except WebSocketDisconnect:
        local_log.info("Agent disconnected", extra={"forwarded": forwarded})
    except asyncio.CancelledError:
        local_log.debug(
            "Connection cancelled (client closed)", extra={"forwarded": forwarded}
        )
        return
    finally:
        agent_sockets.discard(websocket)
        with contextlib.suppress(Exception):
            await websocket.close()
        local_log.debug("Cleanup complete | active_agents=%d", len(agent_sockets))


async def _ws_cmd(url: str, payload: dict, timeout: float = 0.7) -> dict:
    with _Timer(f"_ws_cmd {url}") as t:
        try:
            async with asyncio.timeout(timeout):
                async with websockets.connect(
                    url,
                    open_timeout=timeout,
                    close_timeout=0.1,
                    ping_interval=None,
                ) as ws:
                    await ws.send(json.dumps(payload))
                    msg = await ws.recv()
                    if isinstance(msg, (bytes, str)):
                        res = json.loads(msg)
                        LOGGER.debug(
                            "_ws_cmd ok | url=%s took_ms=%.1f payload=%s -> %s",
                            url,
                            t.ms,
                            _safe(payload),
                            _safe(res),
                            extra={"took_ms": round(t.ms, 1)},
                        )
                        return res
                    LOGGER.debug(
                        "_ws_cmd bad-response | url=%s took_ms=%.1f",
                        url,
                        t.ms,
                        extra={"took_ms": round(t.ms, 1)},
                    )
                    return {"ok": False, "running": False, "error": "bad-response"}
        except Exception as e:

            err_str = (str(e) or "").strip()

            if not err_str:

                strerror = getattr(e, "strerror", "") or ""
                errno = getattr(e, "errno", None)
                if strerror:
                    if errno is not None:
                        err_str = f"[Errno {errno}] {strerror}"
                    else:
                        err_str = strerror

            if not err_str:

                err_str = e.__class__.__name__

            LOGGER.debug(
                "_ws_cmd error | url=%s took_ms=%.1f err=%s",
                url,
                t.ms,
                repr(e),
                extra={"took_ms": round(t.ms, 1)},
            )
            return {"ok": False, "running": False, "error": err_str}


def _as_bool(v: str | None, default: bool = False) -> bool:
    """
    Normalize legacy string-y boolean config values ("true", "1", "yes") -> bool.
    """
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _bootstrap_legacy_mapping_if_needed() -> dict:
    """
    One-time V2 -> V3 migration.

    If guild_mappings is EMPTY and legacy single-guild fields still live
    in app_config (HOST_GUILD_ID / CLONE_GUILD_ID + old per-guild flags),
    we do three things in this order:

      1. Backfill original_guild_id / cloned_guild_id into every legacy row
         across tables (messages, threads, channel_mappings, etc.).
      2. Create the first row in guild_mappings using those legacy values.
      3. Wipe the legacy keys from app_config so we never do this again.

    We return a dict that includes:
      - created: bool
      - mapping_id: str | None
      - host_guild_id / clone_guild_id
      - host_guild_name / clone_guild_name
      - backfill: { <table.change_key>: rowcount, ... } or {"error": "..."}
      - cleaned_keys: [list of legacy keys we attempted to remove]
      - skipped_reason if we didn't migrate
      - count if we already had mappings
    """

    try:
        existing = db.list_guild_mappings() or []
    except Exception:
        existing = []

    if existing:
        return {
            "created": False,
            "skipped_reason": "guild_mappings_already_present",
            "count": len(existing),
        }

    legacy_host_id = (db.get_config("HOST_GUILD_ID", "") or "").strip()
    legacy_clone_id = (db.get_config("CLONE_GUILD_ID", "") or "").strip()

    if not legacy_host_id or not legacy_clone_id:
        return {
            "created": False,
            "skipped_reason": "no_legacy_ids_found",
            "host": legacy_host_id,
            "clone": legacy_clone_id,
        }

    try:
        host_gid = int(legacy_host_id)
        clone_gid = int(legacy_clone_id)
    except Exception:
        return {
            "created": False,
            "skipped_reason": "legacy_ids_not_int",
            "host": legacy_host_id,
            "clone": legacy_clone_id,
        }

    backfill_summary: dict[str, object] = {}
    try:
        backfill_summary = db.bulk_fill_guild_ids(
            host_guild_id=host_gid,
            clone_guild_id=clone_gid,
        )
    except Exception as e:
        backfill_summary = {"error": str(e)}

    host_name = db.get_config("HOST_GUILD_NAME", "") or ""
    clone_name = (
        db.get_config("CLONED_GUILD_NAME", "")
        or db.get_config("CLONE_GUILD_NAME", "")
        or ""
    )
    host_icon = db.get_config("HOST_GUILD_ICON_URL", "") or ""

    settings_obj: dict[str, bool] = {}
    for key in BOOL_KEYS:
        legacy_val = db.get_config(key, None)
        default_val = DEFAULTS.get(key, False)
        settings_obj[key] = _as_bool(legacy_val, default=default_val)

    mapping_name = f"{host_gid}"

    new_mapping_id = db.upsert_guild_mapping(
        mapping_id=None,
        mapping_name=mapping_name,
        original_guild_id=host_gid,
        original_guild_name=host_name,
        original_guild_icon_url=host_icon,
        cloned_guild_id=clone_gid,
        cloned_guild_name=clone_name,
        settings=settings_obj,
    )

    cleanup_keys = [
        "HOST_GUILD_ID",
        "CLONE_GUILD_ID",
        "HOST_GUILD_NAME",
        "CLONE_GUILD_NAME",
        "CLONED_GUILD_NAME",
        "HOST_GUILD_ICON_URL",
        "CLONE_CHANNEL_PERMISSIONS",
        "LOG_FORMAT",
    ]
    cleanup_keys.extend(list(BOOL_KEYS))

    removed_keys: list[str] = []
    for k in cleanup_keys:
        try:
            db.delete_config(k)
            removed_keys.append(k)
        except Exception:
            pass

    return {
        "created": True,
        "mapping_id": new_mapping_id,
        "host_guild_id": host_gid,
        "host_guild_name": host_name,
        "clone_guild_id": clone_gid,
        "cloned_guild_name": clone_name,
        "backfill": backfill_summary,
        "cleaned_keys": removed_keys,
    }


@app.get("/", response_class=None)
async def index(request: Request):
    env = _read_env()

    s_server = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "status"})
    s_client = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "status"})

    both_running = bool(s_server.get("running")) and bool(s_client.get("running"))

    text_keys = [k for k in ALLOWED_ENV if k != "LOG_LEVEL"]

    bool_keys = BOOL_KEYS
    guild_mappings = db.list_guild_mappings()
    mapping_bool_keys = BOOL_KEYS

    current_log_level = (env.get("LOG_LEVEL") or "INFO").upper()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": APP_TITLE,
            "env": env,
            "text_keys": text_keys,
            "bool_keys": bool_keys,
            "log_level": current_log_level,
            "guild_mappings": guild_mappings,
            "mapping_bool_keys": mapping_bool_keys,
            "server_status": s_server,
            "client_status": s_client,
            "both_running": both_running,
            "version": CURRENT_VERSION,
        },
    )


@app.get("/health", response_class=PlainTextResponse)
async def health():
    s1 = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "status"})
    s2 = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "status"})
    ok = s1.get("ok", True) and s2.get("ok", True)
    LOGGER.info(
        "Health check | server=%s client=%s ok=%s",
        s1.get("status"),
        s2.get("status"),
        ok,
    )
    return "ok" if ok else PlainTextResponse("control not reachable", status_code=500)


@app.post("/save")
async def save(request: Request):
    form = await request.form()

    values = {k: str(form.get(k, "") or "").strip() for k in ALLOWED_ENV}
    LOGGER.debug("POST /save | form=%s", _redact_dict(values))

    errs = _validate(values)
    if errs:
        LOGGER.warning("POST /save invalid | errs=%s", errs)
        return PlainTextResponse(
            "Invalid config: " + "; ".join(errs),
            status_code=400,
        )

    token_errs = await _verify_tokens_for_save(values)
    if token_errs:
        LOGGER.warning("POST /save invalid | token_errs=%s", token_errs)
        pretty_msg = "Invalid config:\n" + "\n".join(f"- {msg}" for msg in token_errs)
        return PlainTextResponse(pretty_msg, status_code=400)

    try:
        _write_env(values)
        LOGGER.info(
            "Config saved successfully",
            extra={"keys": list(values.keys())},
        )
    except Exception as e:
        LOGGER.exception("Failed to persist config to DB: %s", e)
        return PlainTextResponse(
            "Internal error saving config.",
            status_code=500,
        )

    try:
        env_after = _read_env()
        new_level = (env_after.get("LOG_LEVEL") or "INFO").upper()

        os.environ["LOG_LEVEL"] = new_level

        import logging as _logging

        LOGGER.logger.setLevel(getattr(_logging, new_level, _logging.INFO))

        LOGGER.info("LOG_LEVEL applied", extra={"LOG_LEVEL": new_level})
    except Exception as e:
        LOGGER.exception("Failed to apply LOG_LEVEL: %s", e)

    return RedirectResponse("/", status_code=303)


async def _check_controller(
    name: str, url: str, timeout: float = 0.7
) -> tuple[bool, str, dict]:
    """
    Ping a controller with {"cmd": "status"} and decide if it's reachable.

    Returns (reachable, friendly_reason, raw_response).
    - reachable = did the Admin reach the controller WebSocket at all?
    - friendly_reason = short text like "running", "stopped", or an error string.
    """
    res = await _ws_cmd(url, {"cmd": "status"}, timeout=timeout)

    if not res.get("ok") or res.get("error"):
        reason = (res.get("error") or "unreachable").strip()
        return False, reason, res

    running = bool(res.get("running"))
    reason = "running" if running else "stopped"
    return True, reason, res


@app.post("/start")
async def start_all():

    errs = _validate(_read_env(), for_start=True)
    if errs:
        LOGGER.warning("POST /start blocked | errs=%s", errs)
        return PlainTextResponse(
            "Cannot start: " + "; ".join(errs),
            status_code=400,
        )

    srv_ok, srv_reason, srv_ping = await _check_controller("Server", SERVER_CTRL_URL)
    cli_ok, cli_reason, cli_ping = await _check_controller("Client", CLIENT_CTRL_URL)

    unreachable_parts: list[str] = []
    if not srv_ok:
        unreachable_parts.append(f"- Server: {srv_reason}")
    if not cli_ok:
        unreachable_parts.append(f"- Client: {cli_reason}")

    if unreachable_parts:
        if not srv_ok and not cli_ok:
            msg = (
                "Couldn’t start Copycord.\n\n"
                "The Admin panel cannot reach either the Server or Client controller.\n"
                "Make sure the Server, Client, and Admin containers are all running and that "
                "the control ports are reachable, then try again."
            )
        elif not srv_ok:
            msg = (
                "Couldn’t start Copycord.\n\n"
                "The Admin panel cannot reach the Server controller.\n"
                "Make sure the Server container is running and reachable, then try again."
            )
        else:
            msg = (
                "Couldn’t start Copycord.\n\n"
                "The Admin panel cannot reach the Client controller.\n"
                "Make sure the Client container is running and reachable, then try again."
            )

        msg += "\n\nDetails:\n" + "\n".join(unreachable_parts)

        LOGGER.error(
            "POST /start preflight failed | server=%s client=%s",
            srv_ping,
            cli_ping,
        )
        return PlainTextResponse(msg, status_code=502)

    srv = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "start"})
    cli = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "start"})

    errors: list[str] = []
    if not srv.get("ok") or srv.get("error"):
        errors.append(
            f"- Server: {srv.get('error') or srv.get('status') or 'failed to start'}"
        )
    if not cli.get("ok") or cli.get("error"):
        errors.append(
            f"- Client: {cli.get('error') or cli.get('status') or 'failed to start'}"
        )

    if errors:
        detail = "\n".join(errors)
        LOGGER.error("POST /start failed | %s", detail.replace("\n", " | "))
        friendly = (
            "Couldn’t start Copycord.\n\n"
            "The Admin panel reached the controllers, but there was a problem starting:\n"
            f"{detail}\n\n"
            "Check the Server and Client logs for more details."
        )
        return PlainTextResponse(friendly, status_code=502)

    LOGGER.info(
        "POST /start ok | server=%s client=%s",
        srv.get("status") or srv.get("running"),
        cli.get("status") or cli.get("running"),
    )
    return RedirectResponse("/", status_code=303)


@app.post("/stop")
async def stop_all():
    LOGGER.info("POST /stop requested")
    await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "stop"})
    await _ws_cmd(SERVER_CTRL_URL, {"cmd": "stop"})

    await locks.clear_all()

    return RedirectResponse("/", status_code=303)


@app.on_event("shutdown")
async def on_shutdown():
    LOGGER.info("Shutdown initiated")
    shutdown_event.set()

    async def _close_group(peers: Set[WebSocket], timeout: float = 0.2):
        sockets = list(peers)
        peers.clear()
        if not sockets:
            return
        tasks = [asyncio.create_task(_close_ws_quietly(ws)) for ws in sockets]
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for t in pending:
            t.cancel()
        LOGGER.debug(
            "Closed WS group | closed=%d cancelled=%d", len(done), len(pending)
        )

    await asyncio.gather(
        _close_group(hub.ui_sockets),
        _close_group(bus_sockets),
        _close_group(agent_sockets),
    )
    LOGGER.info("Shutdown complete")


@app.post("/logs/clear")
async def clear_logs():
    cleared = []
    for name in ("server.log", "client.log", "server.out", "client.out"):
        p = DATA_DIR / name
        try:
            if p.exists():
                with open(p, "w", encoding="utf-8"):
                    pass
                cleared.append(name)
        except Exception:
            pass
    LOGGER.info("POST /logs/clear done | cleared=%s", cleared)
    return RedirectResponse("/", status_code=303)


@app.get("/logs/{which}", response_class=PlainTextResponse)
async def logs(which: str, tail: int = 20000):
    if which == "server":
        candidates = ["server.out", "server.log"]
    elif which == "client":
        candidates = ["client.out", "client.log"]
    else:
        return PlainTextResponse("invalid", status_code=400)

    for name in candidates:
        p = DATA_DIR / name
        try:
            if p.exists() and p.stat().st_size > 0:
                text = p.read_text(encoding="utf-8", errors="ignore")
                if tail and tail > 0 and len(text) > tail:
                    text = text[-tail:]
                return PlainTextResponse(text)
        except Exception:
            continue

    return PlainTextResponse("No logs yet.", status_code=404)


@app.on_event("startup")
async def _startup_links():
    await startup_links(app, templates_env=templates.env, set_jinja_global=True)


@app.on_event("startup")
async def _migrate_legacy_single_mapping():
    """
    Runs once on startup and (if needed) upgrades a legacy single-guild install
    to the new multi-guild model used in Copycord v3.
    """
    try:
        result = _bootstrap_legacy_mapping_if_needed()

        if result.get("created"):

            LOGGER.warning(
                "[🧙‍♂️] Copycord auto-migrated this install from legacy "
                "single-guild mode to v3 multi-guild mode.\n"
                " - New mapping_id=%s (%s ➜ %s)\n"
                " - Backfilled guild IDs into legacy tables (counts below)\n"
                " - Saved per-guild settings into guild_mappings\n"
                " - Removed old single-guild config keys\n"
                "Details: %s",
                result.get("mapping_id"),
                result.get("host_guild_id"),
                result.get("clone_guild_id"),
                result.get("backfill"),
            )

    except Exception:

        LOGGER.exception(
            "[migrate:v3] Legacy single-guild → multi-guild bootstrap failed"
        )


@app.on_event("shutdown")
async def _shutdown():
    await shutdown_links(app)


@app.on_event("startup")
async def _apply_db_log_level_and_banner():
    try:
        env = _read_env()
        lvl_name = (env.get("LOG_LEVEL") or "INFO").upper()
        LOGGER.logger.setLevel(getattr(logging, lvl_name, logging.INFO))
    except Exception:
        pass
    LOGGER.debug(
        "Starting %s | LOG_LEVEL=%s | WS_SERVER_CTRL=%s | WS_CLIENT_CTRL=%s",
        APP_TITLE,
        logging.getLevelName(LOGGER.logger.level),
        SERVER_CTRL_URL,
        CLIENT_CTRL_URL,
    )


@app.on_event("startup")
async def _start_bg_tasks():
    asyncio.create_task(_lock_listener())


@app.on_event("startup")
async def _start_release_watcher():
    asyncio.create_task(_release_watch_loop())


@app.on_event("startup")
async def _start_backup_scheduler():
    backup_scheduler.start()


@app.on_event("startup")
async def _cleanup_stale_mapping_pairs_on_boot():
    """
    On admin API startup, purge any per-(host, clone) state that no longer
    has a corresponding row in guild_mappings. This keeps the DB free of
    stale mappings that could confuse the server.
    """
    try:
        stats = db.cleanup_stale_mapping_pairs()
        pairs = int(stats.get("pairs_cleared") or 0)
        rb_only = int(stats.get("role_blocks_only") or 0)

        if pairs:
            LOGGER.info(
                "Startup cleanup: removed %d stale guild mapping pair(s) with "
                "no active mapping. Your database is now in sync with current "
                "guild mappings.",
                pairs,
            )
        elif rb_only:
            LOGGER.debug(
                "Startup cleanup: only orphaned blocked-role entries were removed."
            )
        else:
            LOGGER.debug(
                "Startup cleanup: no stale mapping state found. "
                "All guild mappings are already in sync."
            )
    except Exception:
        LOGGER.exception("Startup cleanup: failed while removing stale mapping state.")


@app.on_event("shutdown")
async def _stop_backup_scheduler():
    await backup_scheduler.stop()


@app.get("/api/validate-tokens", response_class=JSONResponse)
async def api_validate_tokens():
    """
    Check whether the saved CLIENT_TOKEN and SERVER_TOKEN are currently valid.

    """
    env = _read_env()
    raw_client = (env.get("CLIENT_TOKEN") or "").strip()
    raw_server = (env.get("SERVER_TOKEN") or "").strip()

    has_tokens = bool(raw_client and raw_server)
    if not has_tokens:
        return JSONResponse({"ok": False, "has_tokens": False, "errors": []})

    errs = await _verify_tokens_for_save(
        {
            "CLIENT_TOKEN": raw_client,
            "SERVER_TOKEN": raw_server,
        }
    )
    ok = not errs

    return JSONResponse(
        {
            "ok": ok,
            "has_tokens": True,
            "errors": errs,
        }
    )


def _mask_token(val: str) -> str:
    v = (val or "").strip()
    if not v:
        return ""
    if len(v) <= 10:
        return "•" * len(v)
    return f"{v[:4]}…{v[-4:]}"


@app.get("/api/backup-tokens", response_class=JSONResponse)
async def api_backup_tokens():
    """List backup CLIENT_TOKEN entries (masked) for the Admin UI."""
    try:
        rows = db.list_backup_tokens()
        tokens = []
        for r in rows:
            tokens.append(
                {
                    "token_id": r.get("token_id"),
                    "masked": _mask_token(r.get("token_value") or ""),
                    "note": r.get("note") or "",
                    "added_at": r.get("added_at") or None,
                    "last_used": r.get("last_used") or None,
                }
            )
        return JSONResponse({"ok": True, "tokens": tokens})
    except Exception:
        LOGGER.exception("backup tokens: list failed")
        return JSONResponse({"ok": False, "tokens": []}, status_code=500)


@app.post("/api/backup-tokens/add", response_class=JSONResponse)
async def api_backup_tokens_add(
    token_value: str = Form(...),
    note: str = Form(""),
):
    token_value = (token_value or "").strip()
    if not token_value:
        raise HTTPException(status_code=400, detail="token_value is required")

    if not await _check_client_token_valid(token_value):
        raise HTTPException(
            status_code=400,
            detail="Invalid backup token. It must be a valid Discord user token.",
        )

    try:
        for r in db.list_backup_tokens() or []:
            if (r.get("token_value") or "").strip() == token_value:
                raise HTTPException(
                    status_code=400, detail="That backup token is already stored."
                )
    except HTTPException:
        raise
    except Exception:
        pass

    try:
        token_id = db.add_backup_token(token_value, (note or "").strip() or None)
        return JSONResponse({"ok": True, "token_id": token_id})
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        LOGGER.exception("backup tokens: add failed")
        return JSONResponse({"ok": False}, status_code=500)


@app.post("/api/backup-tokens/delete", response_class=JSONResponse)
async def api_backup_tokens_delete(token_id: str = Form(...)):
    token_id = (token_id or "").strip()
    if not token_id:
        raise HTTPException(status_code=400, detail="token_id is required")

    try:
        ok = db.delete_backup_token(token_id)
        if not ok:
            return JSONResponse({"ok": False, "not_found": True})
        return JSONResponse({"ok": True})
    except Exception:
        LOGGER.exception("backup tokens: delete failed")
        return JSONResponse({"ok": False}, status_code=500)


@app.api_route("/admin/backup-now", methods=["GET", "POST"])
async def backup_now():
    out_path = await backup_scheduler.run_now()

    return {"ok": True, "file": out_path.name}


@app.get("/api/backup/info")
async def backup_info():
    def _cfg(k, d=""):
        return db.get_config(k, d)

    last_at = _cfg("DB_LAST_BACKUP_AT", "")
    last_file = _cfg("DB_LAST_BACKUP_FILE", "")
    last_size = int(_cfg("DB_LAST_BACKUP_SIZE", "0") or 0)
    archives = []
    if BACKUP_DIR.exists():
        for p in sorted(
            BACKUP_DIR.glob("*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True
        ):
            try:
                st = p.stat()
                archives.append(
                    {"name": p.name, "size": st.st_size, "mtime": int(st.st_mtime)}
                )
            except Exception:
                pass
    return {
        "ok": True,
        "last_backup_at": last_at,
        "last_backup_file": last_file,
        "last_backup_size": last_size,
        "dir": str(BACKUP_DIR),
        "archives": archives,
    }


@app.get("/api/backup/download/{name}")
async def backup_download(name: str):
    p = BACKUP_DIR / name
    if not p.exists() or not p.is_file():
        return PlainTextResponse("not found", status_code=404)
    return FileResponse(str(p), filename=name, media_type="application/gzip")


@app.post("/api/backup/delete")
async def backup_delete(name: str = Form(...)):
    """
    Permanently delete a backup archive from BACKUP_DIR.
    """
    p = BACKUP_DIR / name
    if not p.exists() or not p.is_file():
        return PlainTextResponse("not found", status_code=404)
    try:
        p.unlink()
    except Exception as e:
        return PlainTextResponse(f"delete failed: {e}", status_code=500)
    return {"ok": True, "deleted": name}


@app.post("/api/backup/restore")
async def backup_restore(
    source: str = Form("upload"),
    file: UploadFile | None = File(None),
    name: str | None = Form(None),
):
    """
    Restore from an uploaded .tar.gz or from an existing archive in BACKUP_DIR.
    Safeguards:
      - Stops agents
      - Atomic replace of live DB
    """
    if source not in ("upload", "existing"):
        return PlainTextResponse("bad source", status_code=400)

    if source == "existing":
        if not name:
            return PlainTextResponse("name required", status_code=400)
        arc = BACKUP_DIR / name
        if not arc.exists():
            return PlainTextResponse("archive not found", status_code=404)
    else:
        if not file:
            return PlainTextResponse("file required", status_code=400)
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        arc = BACKUP_DIR / f"restore-{int(time.time())}.tar.gz"
        with open(arc, "wb") as f:
            f.write(await file.read())

    try:
        await _ws_cmd(SERVER_CTRL_URL, {"cmd": "stop"})
        await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "stop"})
    except Exception as e:
        LOGGER.warning("restore: stop agents failed (continuing): %s", e)

    with tempfile.TemporaryDirectory() as td:
        tmp_dir = Path(td)
        with tarfile.open(arc, "r:gz") as tar:
            members = tar.getmembers()
            names = [m.name for m in members]
            if "data.db" not in names:
                return PlainTextResponse("archive missing data.db", status_code=400)
            tar.extract("data.db", path=tmp_dir)
        extracted = tmp_dir / "data.db"
        if not extracted.exists():
            return PlainTextResponse("extraction failed", status_code=500)

        live = Path(DB_PATH)
        bak = live.with_suffix(".bak")
        try:
            if live.exists():
                shutil.copy2(live, bak)

            shutil.copy2(extracted, live)
        except Exception as e:
            return PlainTextResponse(f"restore failed: {e}", status_code=500)

    db.set_config("DB_LAST_RESTORE_AT", datetime.utcnow().isoformat() + "Z")
    return {"ok": True, "restored_from": arc.name}


@app.get("/system")
async def system_page(request: Request):
    return templates.TemplateResponse(
        "system.html",
        {
            "request": request,
            "title": f"System · {APP_TITLE}",
            "version": CURRENT_VERSION,
        },
    )


# ── Event Logs ───────────────────────────────────────────────────────

@app.get("/event-logs")
async def event_logs_page(request: Request):
    return templates.TemplateResponse(
        "logs.html",
        {
            "request": request,
            "title": f"Logs · {APP_TITLE}",
            "version": CURRENT_VERSION,
        },
    )


@app.get("/api/event-log-types")
async def api_get_event_log_types():
    """Lightweight endpoint that returns only the distinct event types."""
    types = db.get_event_log_types()
    return JSONResponse(
        content={"ok": True, "types": types},
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/event-logs")
async def api_get_event_logs(
    event_type: Optional[str] = None,
    guild_id: Optional[int] = None,
    search: Optional[str] = None,
    limit: int = Query(200, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    logs = db.get_event_logs(
        event_type=event_type,
        guild_id=guild_id,
        search=search,
        limit=limit,
        offset=offset,
    )
    total = db.count_event_logs(
        event_type=event_type,
        guild_id=guild_id,
        search=search,
    )
    types = db.get_event_log_types()
    return JSONResponse(
        content={"ok": True, "logs": logs, "total": total, "types": types},
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.delete("/api/event-logs/{log_id}")
async def api_delete_event_log(log_id: str):
    ok = db.delete_event_log(log_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Log not found")
    return {"ok": True}


@app.post("/api/event-logs/delete-bulk")
async def api_delete_event_logs_bulk(body: dict = Body(...)):
    ids = body.get("ids") or []
    if not ids:
        raise HTTPException(status_code=400, detail="No ids provided")
    deleted = db.delete_event_logs_bulk(ids)
    return {"ok": True, "deleted": deleted}


@app.delete("/api/event-logs")
async def api_clear_event_logs():
    deleted = db.clear_event_logs()
    return {"ok": True, "deleted": deleted}


@app.post("/api/event-logs")
async def api_add_event_log(body: dict = Body(...)):
    """Receive event log entries from server/client agents."""
    event_type = body.get("event_type") or "unknown"
    details = body.get("details") or ""
    log_id = db.add_event_log(
        event_type=event_type,
        details=details,
        guild_id=body.get("guild_id"),
        guild_name=body.get("guild_name"),
        channel_id=body.get("channel_id"),
        channel_name=body.get("channel_name"),
        category_id=body.get("category_id"),
        category_name=body.get("category_name"),
        extra=body.get("extra"),
    )
    # Broadcast the new log to all connected UI clients
    await hub.broadcast({
        "kind": "event_log",
        "role": "server",
        "payload": {
            "log_id": log_id,
            "event_type": event_type,
            "details": details,
            "guild_id": body.get("guild_id"),
            "guild_name": body.get("guild_name"),
            "channel_id": body.get("channel_id"),
            "channel_name": body.get("channel_name"),
            "category_id": body.get("category_id"),
            "category_name": body.get("category_name"),
            "extra": body.get("extra"),
        },
    })
    return {"ok": True, "log_id": log_id}


@app.get("/forwarding")
async def forwarding_page(request: Request):
    """
    Render the Message Forwarding page.
    """
    env = _read_env()
    return templates.TemplateResponse(
        "forwarding.html",
        {
            "request": request,
            "title": f"Forwarding · {APP_TITLE}",
            "version": CURRENT_VERSION,
            "log_level": env.get("LOG_LEVEL", "INFO"),
        },
    )


@app.get("/logs/stream/{which}")
async def logs_stream(which: str, request: Request, tail_bytes: int = 50000):
    if which == "server":
        candidates = ["server.out", "server.log"]
    elif which == "client":
        candidates = ["client.out", "client.log"]
    elif which == "scraper":
        candidates = ["scraper.out", "scraper.log"]
    else:
        return PlainTextResponse("invalid", status_code=400)

    async def gen():
        def pick_path():
            for n in candidates:
                p = DATA_DIR / n
                if p.exists():
                    return p
            return None

        HEARTBEAT_EVERY = 15.0

        while not shutdown_event.is_set():
            if await request.is_disconnected():
                break

            path = pick_path()
            if not path:
                yield ": keepalive\n\n"
                await asyncio.sleep(0.2)
                continue

            try:
                last_stat = path.stat()
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(0, os.SEEK_END)
                    size = f.tell()
                    start = max(0, size - int(tail_bytes))
                    f.seek(start)
                    if start > 0:
                        f.readline()

                    last_hb = time.monotonic()

                    batch = []
                    for line in f:
                        if shutdown_event.is_set() or await request.is_disconnected():
                            break
                        batch.append(line.rstrip())
                        if len(batch) >= 50:
                            yield f"data: {json.dumps({'lines': batch})}\n\n"
                            batch.clear()
                    if batch:
                        yield f"data: {json.dumps({'lines': batch})}\n\n"

                    while not shutdown_event.is_set():
                        if await request.is_disconnected():
                            break

                        pos = f.tell()
                        line = f.readline()
                        if line:
                            yield f"data: {json.dumps({'line': line.rstrip()})}\n\n"
                            last_hb = time.monotonic()
                        else:
                            await asyncio.sleep(0.2)
                            now = time.monotonic()
                            if now - last_hb >= HEARTBEAT_EVERY:
                                yield ":ka\n\n"
                                last_hb = now
                            try:
                                st = os.stat(path)
                            except FileNotFoundError:
                                break
                            if (st.st_ino != last_stat.st_ino) or (
                                st.st_dev != last_stat.st_dev
                            ):
                                break
                            if pos > st.st_size:
                                f.seek(st.st_size)
                            else:
                                f.seek(pos)
            except Exception:
                if shutdown_event.is_set() or await request.is_disconnected():
                    break
                yield ": keepalive\n\n"
                await asyncio.sleep(0.2)

        LOGGER.info("SSE /logs/stream/%s closed", which)
        yield "event: close\ndata: bye\n\n"

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


def _derive_state(obj: dict) -> str:
    """
    Normalize a status payload into 'running' or 'stopped' (or passthrough).
    Accepts either:
      - running=True
      - status string in a set of "good" states
      - presence of a pid as a strong hint of running
    """
    s = str(obj.get("status", "")).strip().lower()
    if obj.get("running") is True:
        return "running"

    good = {
        "running",
        "started",
        "active",
        "online",
        "ok",
        "ready",
        "up",
        "connected",
        "logged_in",
        "logged-in",
        "authenticated",
        "awake",
    }
    bad = {"stopped", "offline", "down", "error", "dead", "failed"}

    if s in good:
        return "running"
    if s in bad:
        return "stopped"
    if obj.get("pid"):
        return "running"
    return "stopped" if s == "" else s


def _enrich_from_bus(ctrl: dict, bus: dict) -> dict:
    out = dict(ctrl or {})
    if not out.get("status") and bus.get("status"):
        out["status"] = bus["status"]
    if not out.get("pid") and bus.get("pid"):
        out["pid"] = bus["pid"]
    if "running" not in out and "running" in bus:
        out["running"] = bus["running"]
    if "discord" not in out and isinstance(bus.get("discord"), dict):
        out["discord"] = bus["discord"]
    out.setdefault("status", "")
    return out


def _is_discord_ready(obj: dict) -> bool:
    """
    Accepts multiple shapes so agents can send whatever is convenient.
    We consider the bot 'ready' if any of these are truthy/readyish:
      - obj['discord']['ready' | 'connected' | 'online'] is True
      - obj['discord']['state'] in {'ready','connected','online'}
      - obj['gateway'] in {'ready','connected','online'}
      - obj['discord_status'] in {'ready','connected','online'}
    """
    if not isinstance(obj, dict):
        return False

    d = obj.get("discord")
    if isinstance(d, dict):
        if d.get("ready") or d.get("connected") or d.get("online"):
            return True
        st = str(d.get("state", "")).lower()
        if st in {"ready", "connected", "online"}:
            return True

    st2 = str(obj.get("gateway") or obj.get("discord_status") or "").lower()
    return st2 in {"ready", "connected", "online"}


async def _collect_status() -> dict:
    with _Timer("/status") as t:
        s_server = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "status"}, timeout=0.7)
        s_client = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "status"}, timeout=0.7)

    bus_srv = hub.status.get("server") or {}
    bus_cli = hub.status.get("client") or {}

    s_server = _enrich_from_bus(s_server, bus_srv)
    s_client = _enrich_from_bus(s_client, bus_cli)

    server_state = _derive_state(s_server)
    client_state = _derive_state(s_client)
    both_running = (server_state == "running") and (client_state == "running")

    server_ready = _is_discord_ready(s_server) or _is_discord_ready(bus_srv)
    client_ready = _is_discord_ready(s_client) or _is_discord_ready(bus_cli)
    both_ready = server_ready and client_ready

    res = {
        "server": {**s_server, "state": server_state, "ready": server_ready},
        "client": {**s_client, "state": client_state, "ready": client_ready},
        "both_running": both_running,
        "both_ready": both_ready,
        "running_and_ready": both_running and both_ready,
        "running": both_running,
        "status": "running" if both_running else "stopped",
    }

    LOGGER.debug(
        "GET /status | took_ms=%.1f running=%s ready=%s",
        t.ms,
        both_running,
        both_ready,
        extra={"took_ms": round(t.ms, 1)},
    )
    return res


@app.get("/api/status", response_class=JSONResponse)
async def api_status_alias():
    return await _collect_status()


@app.get("/filters/{mapping_id}")
async def api_get_filters(mapping_id: str):
    filters = db.get_filters_for_mapping(mapping_id)

    mapping = db.get_mapping_by_id(mapping_id)
    blocked_role_ids: list[int] = []
    if mapping:
        try:
            clone_gid = int(mapping["cloned_guild_id"] or 0)
        except Exception:
            clone_gid = 0
        if clone_gid:
            blocked_role_ids = db.get_blocked_role_ids(cloned_guild_id=clone_gid)

    user_filters = db.get_user_filters_for_mapping(mapping_id)

    channel_name_blacklist: list[str] = []
    if mapping:
        try:
            host_gid = int(mapping["original_guild_id"] or 0)
            clone_gid_val = int(mapping["cloned_guild_id"] or 0)
            if host_gid and clone_gid_val:
                channel_name_blacklist = db.get_channel_name_blacklist_for_mapping(
                    host_gid, clone_gid_val
                )
        except Exception:
            pass

    return JSONResponse(
        {
            "wl_categories": filters["whitelist"]["category"],
            "wl_channels": filters["whitelist"]["channel"],
            "ex_categories": filters["exclude"]["category"],
            "ex_channels": filters["exclude"]["channel"],
            "blocked_words": filters.get("blocked_words", []),
            "blocked_role_ids": [str(x) for x in blocked_role_ids],
            "wl_users": [str(x) for x in user_filters["whitelist"]],
            "bl_users": [str(x) for x in user_filters["blacklist"]],
            "channel_name_blacklist": channel_name_blacklist,
        }
    )


@app.post("/filters/{mapping_id}/save")
async def api_save_filters(mapping_id: str, request: Request):
    form = await request.form()

    def _split_csv_ids(s: str) -> list[int]:
        out: list[int] = []
        for tok in str(s or "").split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                out.append(int(tok))
            except Exception:
                pass
        return out

    def _split_csv_words(s: str) -> list[str]:
        out: list[str] = []
        for tok in str(s or "").split(","):
            w = tok.strip()
            if w and w not in out:
                out.append(w)
        return out

    wl_categories = _split_csv_ids(form.get("wl_categories", ""))
    wl_channels = _split_csv_ids(form.get("wl_channels", ""))
    ex_categories = _split_csv_ids(form.get("ex_categories", ""))
    ex_channels = _split_csv_ids(form.get("ex_channels", ""))

    blocked_words = _split_csv_words(form.get("blocked_words", ""))
    blocked_role_ids = _split_csv_ids(form.get("blocked_role_ids", ""))
    channel_name_blacklist = _split_csv_words(form.get("channel_name_blacklist", ""))

    wl_users = _split_csv_ids(form.get("wl_users", ""))
    bl_users = _split_csv_ids(form.get("bl_users", ""))

    db.replace_filters_for_mapping(
        mapping_id=mapping_id,
        wl_categories=wl_categories,
        wl_channels=wl_channels,
        ex_categories=ex_categories,
        ex_channels=ex_channels,
    )

    db.replace_blocked_keywords_for_mapping(
        mapping_id=mapping_id,
        words=blocked_words,
    )

    db.replace_role_blocks_for_mapping(
        mapping_id=mapping_id,
        original_role_ids=blocked_role_ids,
    )

    db.replace_user_filters_for_mapping(
        mapping_id=mapping_id,
        whitelist_users=wl_users,
        blacklist_users=bl_users,
    )

    db.replace_channel_name_blacklist_for_mapping(
        mapping_id=mapping_id,
        patterns=channel_name_blacklist,
    )

    try:
        asyncio.create_task(
            _ws_cmd(
                SERVER_AGENT_URL,
                {"type": "reload_channel_name_blacklist"},
                timeout=1.0,
            )
        )
    except Exception:
        LOGGER.warning("reload_channel_name_blacklist ws send failed", exc_info=True)

    return JSONResponse({"ok": True})


@app.post("/api/guild-mappings/{mapping_id}/toggle-status", response_class=JSONResponse)
async def api_toggle_mapping_status(mapping_id: str):
    """
    Toggle a mapping between 'active' and 'paused'.
    """
    row = db.get_mapping_by_id(mapping_id)
    if not row:
        raise HTTPException(status_code=404, detail="mapping-not-found")

    cur_status = (
        row.get("status") if isinstance(row, dict) else row["status"]
    ) or "active"
    cur_status = str(cur_status).lower()

    new_status = "paused" if cur_status == "active" else "active"

    db.update_mapping_status(mapping_id, new_status)

    return JSONResponse(
        {
            "ok": True,
            "mapping_id": mapping_id,
            "status": new_status,
        }
    )


@app.get("/api/mappings/{mapping_id}/channels", response_class=JSONResponse)
async def api_mapping_channels(mapping_id: str):
    """
    Fetch categories + channels for the ORIGINAL guild for this mapping
    using the Discord HTTP API and the CLIENT_TOKEN from config.
    """
    mapping = db.get_mapping_by_id(mapping_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="mapping-not-found")

    try:
        orig_id = int(mapping["original_guild_id"] or 0)
    except Exception:
        orig_id = 0

    if not orig_id:
        raise HTTPException(status_code=400, detail="original-guild-missing")

    cfg = db.get_all_config()
    client_token = (cfg.get("CLIENT_TOKEN") or "").strip()
    if not client_token:
        raise HTTPException(status_code=400, detail="client-token-missing")

    url = f"{DISCORD_API_BASE}/guilds/{orig_id}/channels"
    headers = {
        "Authorization": f"{client_token}",
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, timeout=10) as resp:
                text = await resp.text()
                if resp.status != 200:
                    LOGGER.warning(
                        "Discord channels fetch failed for original %s: %s %s",
                        orig_id,
                        resp.status,
                        text[:300],
                    )
                    raise HTTPException(
                        status_code=502,
                        detail="discord-channels-fetch-failed",
                    )
                try:
                    raw_channels = json.loads(text)
                except Exception:
                    LOGGER.exception(
                        "Failed to decode Discord channels JSON for original %s",
                        orig_id,
                    )
                    raise HTTPException(
                        status_code=502,
                        detail="discord-channels-json-error",
                    )
    except HTTPException:
        raise
    except Exception:
        LOGGER.exception("Error while calling Discord channels endpoint")
        raise HTTPException(status_code=502, detail="discord-channels-error")

    categories: list[dict] = []
    channels: list[dict] = []

    for ch in raw_channels or []:
        try:
            cid = str(ch.get("id"))
        except Exception:
            continue

        try:
            ctype = int(ch.get("type") or 0)
        except Exception:
            ctype = 0

        name = str(ch.get("name") or f"ID {cid}")
        position = int(ch.get("position") or 0)

        parent_id = ch.get("parent_id")
        parent_id_str = str(parent_id) if parent_id else None

        base = {
            "id": cid,
            "name": name,
            "type": ctype,
            "position": position,
            "parent_id": parent_id_str,
        }

        if ctype == 4:
            categories.append(base)
        else:
            channels.append(base)

    categories.sort(key=lambda c: (c["position"], c["name"].lower()))
    channels.sort(
        key=lambda c: (c["parent_id"] or "", c["position"], c["name"].lower())
    )

    return JSONResponse(
        {
            "ok": True,
            "categories": categories,
            "channels": channels,
        }
    )


@app.get("/api/mappings/{mapping_id}/roles", response_class=JSONResponse)
async def api_mapping_roles(mapping_id: str):
    mapping = db.get_mapping_by_id(mapping_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="mapping-not-found")

    try:
        orig_id = int(mapping["original_guild_id"] or 0)
    except Exception:
        orig_id = 0

    if not orig_id:
        raise HTTPException(status_code=400, detail="original-guild-missing")

    try:
        clone_gid = int(mapping["cloned_guild_id"] or 0)
    except Exception:
        clone_gid = 0

    if not clone_gid:
        raise HTTPException(status_code=400, detail="clone-guild-missing")

    cfg = db.get_all_config()
    client_token = (cfg.get("CLIENT_TOKEN") or "").strip()
    if not client_token:
        raise HTTPException(status_code=400, detail="client-token-missing")

    url = f"{DISCORD_API_BASE}/guilds/{orig_id}/roles"
    headers = {
        "Authorization": f"{client_token}",
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, timeout=10) as resp:
                text = await resp.text()
                if resp.status != 200:
                    LOGGER.warning(
                        "Discord roles fetch failed for original %s: %s %s",
                        orig_id,
                        resp.status,
                        text[:300],
                    )
                    raise HTTPException(
                        status_code=502,
                        detail="discord-roles-fetch-failed",
                    )
                try:
                    raw_roles = json.loads(text)
                except Exception:
                    LOGGER.exception(
                        "Failed to decode Discord roles JSON for original %s", orig_id
                    )
                    raise HTTPException(
                        status_code=502, detail="discord-roles-json-error"
                    )
    except HTTPException:
        raise
    except Exception:
        LOGGER.exception("Error while calling Discord roles endpoint")
        raise HTTPException(status_code=502, detail="discord-roles-error")

    out_roles: list[dict] = []
    for r in raw_roles or []:
        try:
            rid = str(r.get("id"))
        except Exception:
            continue

        name = str(r.get("name") or "@unknown")

        if name == "@everyone" or rid == str(orig_id):
            continue

        position = int(r.get("position") or 0)

        raw_color = r.get("color", 0)
        try:
            color_int = int(raw_color or 0)
        except Exception:
            color_int = 0

        color_hex = "#{:06X}".format(color_int) if color_int else None

        out_roles.append(
            {
                "id": rid,
                "name": name,
                "position": position,
                "color": color_int,
                "color_hex": color_hex,
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "roles": out_roles,
        }
    )


@app.post("/api/filters/blacklist", response_class=JSONResponse)
async def api_blacklist_add(payload: dict = Body(...)):

    scope = str(payload.get("scope", "")).strip().lower()
    if scope not in ("category", "channel"):
        raise HTTPException(status_code=400, detail="invalid-scope")

    raw_id = str(payload.get("obj_id", "")).strip()
    if not raw_id.isdigit():
        raise HTTPException(status_code=400, detail="invalid-id")
    obj_id = int(raw_id)

    raw_orig = str(payload.get("original_guild_id", "")).strip()
    raw_clone = str(payload.get("cloned_guild_id", "")).strip()

    original_guild_id = int(raw_orig) if raw_orig.isdigit() else None
    cloned_guild_id = int(raw_clone) if raw_clone.isdigit() else None

    try:
        db.add_filter(
            "exclude",
            scope,
            obj_id,
            original_guild_id=original_guild_id,
            cloned_guild_id=cloned_guild_id,
        )
    except Exception:
        raise HTTPException(status_code=500, detail="db-failure")

    msg = {
        "type": "filters_reload",
        "data": {
            "original_guild_id": original_guild_id,
            "cloned_guild_id": cloned_guild_id,
        },
    }

    try:
        asyncio.create_task(_ws_cmd(CLIENT_AGENT_URL, msg, timeout=1.0))
    except Exception:
        LOGGER.warning("filters_reload ws send failed", exc_info=True)

    return {
        "ok": True,
        "scope": scope,
        "obj_id": str(obj_id),
        "original_guild_id": str(original_guild_id or ""),
        "cloned_guild_id": str(cloned_guild_id or ""),
    }


def _read_env() -> Dict[str, str]:
    vals = DEFAULTS.copy()
    try:
        stored = db.get_all_config()
        for k, v in stored.items():
            if k in ALLOWED_ENV and v is not None:
                vals[k] = str(v)
    except Exception:
        pass
    for k in ALLOWED_ENV:
        vals.setdefault(k, "")
    LOGGER.debug("Config read | %s", _redact_dict(vals))
    return vals


def _write_env(values: Dict[str, str]) -> None:
    for k in ALLOWED_ENV:
        v = values.get(k, "") or ""
        if k in BOOL_KEYS:
            v = _norm_bool_str(v)
        if k == "LOG_LEVEL":
            v = "DEBUG" if str(v).upper() == "DEBUG" else "INFO"
        db.set_config(k, v)
    LOGGER.info("Config saved | %s", _redact_dict(values))


def _validate(values: Dict[str, str], *, for_start: bool = False) -> List[str]:
    errs: List[str] = []

    for k in REQUIRED:
        if not (values.get(k) or "").strip():
            errs.append(f"Missing {k}")

    cg = (values.get("CLONE_GUILD_ID") or "").strip()
    if cg != "":
        try:
            if int(cg) <= 0:
                errs.append("CLONE_GUILD_ID must be a positive integer")
        except Exception:
            errs.append("CLONE_GUILD_ID must be an integer")

    hg = (values.get("HOST_GUILD_ID") or "").strip()
    if hg != "":
        try:
            if int(hg) <= 0:
                errs.append("HOST_GUILD_ID must be a positive integer")
        except Exception:
            errs.append("HOST_GUILD_ID must be an integer")

    if not errs and for_start:
        try:
            if len(db.list_guild_mappings()) == 0:
                errs.append("At least one guild_mapping is required")
        except Exception:
            errs.append("At least one guild_mapping is required")

    if errs:
        LOGGER.warning("Config validation failed | errs=%s", errs)
    else:
        LOGGER.debug("Config validation ok")

    return errs


def _norm_bool_str(v: str) -> str:
    return "True" if str(v).strip().lower() in ("true", "1", "yes", "on") else "False"


@app.get("/channels")
async def channels_page(request: Request):
    env = _read_env()
    guild_mappings = db.list_guild_mappings()

    return templates.TemplateResponse(
        "channels.html",
        {
            "request": request,
            "title": APP_TITLE + " – Channels",
            "env": env,
            "guild_mappings": guild_mappings,
            "version": CURRENT_VERSION,
            "log_level": env.get("LOG_LEVEL", "INFO"),
        },
    )


@app.get("/api/channels", response_class=JSONResponse)
async def api_channels(mapping_id: str | None = Query(default=None)):

    raw_rows = db.get_all_channel_mappings()
    raw_cat_rows = db.get_all_category_mappings()

    if mapping_id:
        mapping_row = db.get_mapping_by_id(mapping_id)
        if mapping_row:
            allowed_host = str(mapping_row["original_guild_id"])
            allowed_clone = str(mapping_row["cloned_guild_id"])

            def _belongs(row: sqlite3.Row) -> bool:
                og = str(row["original_guild_id"] or "")
                cg = str(row["cloned_guild_id"] or "")
                return (og == allowed_host) and (cg == allowed_clone)

            raw_rows = [r for r in raw_rows if _belongs(r)]
            raw_cat_rows = [r for r in raw_cat_rows if _belongs(r)]
        else:

            raw_rows = []
            raw_cat_rows = []

    rows = [dict(r) for r in raw_rows]
    cat_rows = [dict(r) for r in raw_cat_rows]

    cat_channels: dict[str, list[dict]] = {}
    for ch in rows:
        parent = ch.get("original_parent_category_id")
        if parent:
            key = str(parent)
            cat_channels.setdefault(key, []).append(ch)

    grouped_categories = []
    for cr in cat_rows:
        cat_key = str(cr["original_category_id"])

        chs_for_cat = cat_channels.get(cat_key, [])

        grouped_categories.append(
            {
                "original_category_id": (
                    str(cr["original_category_id"])
                    if cr.get("original_category_id")
                    else ""
                ),
                "original_category_name": cr.get("original_category_name") or "",
                "cloned_category_id": (
                    str(cr["cloned_category_id"])
                    if cr.get("cloned_category_id")
                    else ""
                ),
                "cloned_category_name": cr.get("cloned_category_name") or "",
                "original_guild_id": str(cr.get("original_guild_id") or ""),
                "cloned_guild_id": str(cr.get("cloned_guild_id") or ""),
                "channels": [
                    {
                        "original_channel_id": str(c["original_channel_id"]),
                        "original_channel_name": c["original_channel_name"],
                        "cloned_channel_id": (
                            str(c["cloned_channel_id"])
                            if c.get("cloned_channel_id")
                            else ""
                        ),
                        "clone_channel_name": c.get("clone_channel_name") or "",
                        "is_thread": False,
                        "pin_count": 0,
                        "channel_webhook_url": c.get("channel_webhook_url") or "",
                        "channel_type": (
                            c.get("channel_type")
                            if c.get("channel_type") is not None
                            else ""
                        ),
                        "original_guild_id": str(c.get("original_guild_id") or ""),
                        "cloned_guild_id": str(c.get("cloned_guild_id") or ""),
                    }
                    for c in chs_for_cat
                ],
            }
        )

    uncategorized_channels = [
        {
            "original_channel_id": str(ch["original_channel_id"]),
            "original_channel_name": ch["original_channel_name"],
            "cloned_channel_id": (
                str(ch["cloned_channel_id"]) if ch.get("cloned_channel_id") else ""
            ),
            "clone_channel_name": ch.get("clone_channel_name") or "",
            "is_thread": False,
            "pin_count": 0,
            "channel_webhook_url": ch.get("channel_webhook_url") or "",
            "channel_type": (
                ch.get("channel_type") if ch.get("channel_type") is not None else ""
            ),
            "original_guild_id": str(ch.get("original_guild_id") or ""),
            "cloned_guild_id": str(ch.get("cloned_guild_id") or ""),
        }
        for ch in rows
        if not ch.get("original_parent_category_id")
    ]

    if uncategorized_channels:
        uc_ogid = next(
            (
                str(ch.get("original_guild_id") or "")
                for ch in uncategorized_channels
                if ch.get("original_guild_id")
            ),
            "",
        )
        uc_cgid = next(
            (
                str(ch.get("cloned_guild_id") or "")
                for ch in uncategorized_channels
                if ch.get("cloned_guild_id")
            ),
            "",
        )
        grouped_categories.append(
            {
                "original_category_id": "",
                "original_category_name": "Uncategorized",
                "cloned_category_id": "",
                "cloned_category_name": "",
                "original_guild_id": uc_ogid,
                "cloned_guild_id": uc_cgid,
                "channels": uncategorized_channels,
            }
        )

    items: list[dict] = []
    for cat in grouped_categories:
        cat_name = (
            cat.get("cloned_category_name")
            or cat.get("original_category_name")
            or "Uncategorized"
        )

        orig_cat_id_str = str(cat.get("original_category_id") or "")
        clone_cat_id_str = str(cat.get("cloned_category_id") or "")

        for ch in cat["channels"]:
            items.append(
                {
                    **ch,
                    "category_name": cat_name,
                    "original_category_id": orig_cat_id_str,
                    "cloned_category_id": clone_cat_id_str,
                }
            )

    return {"items": items}


@app.get("/api/backfills/queue")
async def api_backfills_queue(mapping_id: Optional[str] = Query(default=None)):
    """
    Ask the client for its current backfill queue (active + queued).

    Optional mapping_id makes the queue clone-aware so we only see
    entries for the currently selected mapping.
    """
    payload: dict[str, Any] = {"type": "backfills_queue_query"}
    if mapping_id:
        payload["data"] = {"mapping_id": str(mapping_id)}

    res = await _ws_cmd(CLIENT_AGENT_URL, payload)
    items = (res or {}).get("data", {}).get("items", []) or []

    if mapping_id:
        mid = str(mapping_id)
        items = [it for it in items if str((it or {}).get("mapping_id") or "") == mid]

    return JSONResponse({"ok": True, "items": items})


@app.get("/api/backfills/inflight")
async def api_backfills_inflight(mapping_id: Optional[str] = Query(default=None)):
    res = await _ws_cmd(SERVER_AGENT_URL, {"type": "backfills_status_query"})

    items = (res or {}).get("data", {}).get("items", {}) or {}

    if mapping_id:
        mid = str(mapping_id)
        items = {
            cid: st
            for cid, st in items.items()
            if str((st or {}).get("mapping_id")) == mid
        }

    return JSONResponse({"ok": True, "items": items})


@app.get("/api/backfills/resume-info", response_class=JSONResponse)
async def api_backfills_resume_info(channel_id: int, mapping_id: str | None = None):
    try:
        cid = int(channel_id)
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "invalid-channel_id"}, status_code=400
        )

    row = None
    if mapping_id:
        m = db.get_mapping_by_id(mapping_id)
        if not m:
            return JSONResponse(
                {"ok": False, "error": "unknown-mapping"}, status_code=404
            )

        try:
            cloned_gid = int(m["cloned_guild_id"])
        except Exception:
            cloned_gid = None

        if cloned_gid is not None:
            row = db.backfill_get_incomplete_for_channel_in_clone(cid, cloned_gid)
        else:
            row = None
    else:
        row = db.backfill_get_incomplete_for_channel(cid)

    def _parse_range(r):
        try:
            return json.loads(r or "{}")
        except Exception:
            return {}

    payload = {
        "channel_id": str(cid),
        "original_guild_id": "",
        "cloned_guild_id": "",
        "active": bool(row is not None),
        "resumable": False,
        "run_id": None,
        "delivered": None,
        "expected_total": None,
        "checkpoint": {
            "last_orig_message_id": None,
            "last_orig_timestamp": None,
        },
        "clone_channel_id": None,
        "range": None,
        "started_at": None,
        "updated_at": None,
    }

    if row:
        payload.update(
            {
                "resumable": True,
                "run_id": row.get("run_id"),
                "delivered": row.get("delivered"),
                "expected_total": row.get("expected_total"),
                "checkpoint": {
                    "last_orig_message_id": row.get("last_orig_message_id"),
                    "last_orig_timestamp": row.get("last_orig_timestamp"),
                },
                "clone_channel_id": row.get("clone_channel_id"),
                "range": _parse_range(row.get("range_json")),
                "started_at": row.get("started_at"),
                "updated_at": row.get("updated_at"),
                "original_guild_id": str(row.get("original_guild_id") or ""),
                "cloned_guild_id": str(row.get("cloned_guild_id") or ""),
            }
        )

    return JSONResponse({"ok": True, "data": payload})


@app.post("/api/backfill/start", response_class=JSONResponse)
async def api_backfill_start(payload: dict = Body(...)):
    try:
        channel_id = int(payload.get("channel_id") or payload.get("clone_channel_id"))
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "invalid-channel_id"}, status_code=400
        )

    mapping_id = (payload.get("mapping_id") or "").strip()
    m = db.get_mapping_by_id(mapping_id) if mapping_id else None
    if not m:
        return JSONResponse({"ok": False, "error": "unknown-mapping"}, status_code=404)
    cloned_guild_id = int(m["cloned_guild_id"])
    original_guild_id = int(m["original_guild_id"])

    st = await locks.status(channel_id, cloned_guild_id)
    if st in ("launching", "running"):
        return JSONResponse(
            {
                "ok": False,
                "error": "backfill-already-running",
                "channel_id": channel_id,
                "cloned_guild_id": cloned_guild_id,
                "state": st,
            },
            status_code=409,
        )

    if not await locks.try_acquire_launching(channel_id, cloned_guild_id):
        return JSONResponse(
            {
                "ok": False,
                "error": "backfill-already-running",
                "channel_id": channel_id,
                "cloned_guild_id": cloned_guild_id,
                "state": "launching",
            },
            status_code=409,
        )

    mode = payload.get("mode") or (payload.get("range") or {}).get("mode") or "all"
    after_iso = payload.get("since") or payload.get("after_iso")
    before_iso = (
        payload.get("before_iso")
        or (payload.get("range") or {}).get("before")
        or payload.get("until")
        or payload.get("to_iso")
    )
    last_n = payload.get("last_n")

    data = {
        "channel_id": channel_id,
        "mapping_id": mapping_id,
        "original_guild_id": original_guild_id,
        "cloned_guild_id": cloned_guild_id,
    }
    if after_iso:
        data["after_iso"] = str(after_iso)
    if before_iso:
        data["before_iso"] = str(before_iso)
    if last_n is not None:
        try:
            data["last_n"] = int(last_n)
        except Exception:
            await locks.release(channel_id, cloned_guild_id)
            return JSONResponse(
                {"ok": False, "error": "invalid-last_n"}, status_code=400
            )

    if mode == "between":
        data["range"] = {
            "mode": mode,
            "value": {"after": after_iso, "before": before_iso},
        }
    else:
        rng_val = after_iso or (data.get("last_n") if "last_n" in data else None)
        data["range"] = {"mode": mode, "value": rng_val} if mode else None

    if payload.get("resume"):
        data["resume"] = True
        cp = payload.get("checkpoint") or {}
        after_id = cp.get("last_orig_message_id") or payload.get("after_id")
        after_ts = cp.get("last_orig_timestamp") or payload.get("after_ts")
        if after_id:
            data["after_id"] = str(after_id)
        if after_ts and not data.get("after_iso"):
            data["after_iso"] = str(after_ts)

    res = await _ws_cmd(CLIENT_AGENT_URL, {"type": "clone_messages", "data": data})
    if not res.get("ok", True):
        await locks.release(channel_id, cloned_guild_id)
        return JSONResponse(
            {"ok": False, "error": res.get("error") or "client-agent-failed"},
            status_code=502,
        )

    return JSONResponse({"ok": True})


@app.post("/api/backfill/start-batch", response_class=JSONResponse)
async def api_backfill_start_batch(payload: dict = Body(...)):
    mapping_id = (payload.get("mapping_id") or "").strip()
    m = db.get_mapping_by_id(mapping_id) if mapping_id else None
    if not m:
        return JSONResponse({"ok": False, "error": "unknown-mapping"}, status_code=404)
    cloned_guild_id = int(m["cloned_guild_id"])

    raw_ids = (
        payload.get("channel_ids") or payload.get("channels") or payload.get("ids")
    )
    if not isinstance(raw_ids, (list, tuple)) or not raw_ids:
        return JSONResponse(
            {"ok": False, "error": "invalid-channel_ids"}, status_code=400
        )

    ids, bad, seen = [], [], set()
    for x in raw_ids:
        try:
            cid = int(x)
            if cid not in seen:
                ids.append(cid)
                seen.add(cid)
        except Exception:
            bad.append(x)
    if not ids:
        return JSONResponse(
            {"ok": False, "error": "no-valid-channel_ids", "bad": bad}, status_code=400
        )

    mode = payload.get("mode") or (payload.get("range") or {}).get("mode") or "all"
    after_iso = payload.get("since") or payload.get("after_iso")
    before_iso = (
        payload.get("before_iso")
        or (payload.get("range") or {}).get("before")
        or payload.get("until")
        or payload.get("to_iso")
    )
    last_n = payload.get("last_n")

    def base_payload_for(cid: int) -> dict:
        data = {
            "channel_id": cid,
            "mapping_id": mapping_id,
            "original_guild_id": int(m["original_guild_id"]),
            "cloned_guild_id": cloned_guild_id,
        }
        if after_iso:
            data["after_iso"] = str(after_iso)
        if before_iso:
            data["before_iso"] = str(before_iso)
        if last_n is not None:
            data["last_n"] = int(last_n)
        if mode == "between":
            data["range"] = {
                "mode": mode,
                "value": {"after": after_iso, "before": before_iso},
            }
        else:
            rng_val = after_iso or (data.get("last_n") if "last_n" in data else None)
            data["range"] = {"mode": mode, "value": rng_val} if mode else None
        if payload.get("resume"):
            data["resume"] = True
            cp = payload.get("checkpoint") or {}
            after_id = cp.get("last_orig_message_id") or payload.get("after_id")
            after_ts = cp.get("last_orig_timestamp") or payload.get("after_ts")
            if after_id:
                data["after_id"] = str(after_id)
            if after_ts and not data.get("after_iso"):
                data["after_iso"] = str(after_ts)
        return data

    results, started, locked, failed = [], 0, 0, 0
    for cid in ids:
        st = await locks.status(cid, cloned_guild_id)
        if st in ("launching", "running"):
            results.append(
                {
                    "channel_id": cid,
                    "ok": False,
                    "error": "backfill-already-running",
                    "state": st,
                    "status": 409,
                }
            )
            locked += 1
            continue

        if not await locks.try_acquire_launching(cid, cloned_guild_id):
            results.append(
                {
                    "channel_id": cid,
                    "ok": False,
                    "error": "backfill-already-running",
                    "state": "launching",
                    "status": 409,
                }
            )
            locked += 1
            continue

        data = base_payload_for(cid)
        res = await _ws_cmd(CLIENT_AGENT_URL, {"type": "clone_messages", "data": data})
        if not res or not res.get("ok", True):
            await locks.release(cid, cloned_guild_id)
            results.append(
                {
                    "channel_id": cid,
                    "ok": False,
                    "error": (res or {}).get("error") or "agent-error",
                }
            )
            failed += 1
        else:
            results.append({"channel_id": cid, "ok": True, "state": "started"})
            started += 1

    return JSONResponse(
        {
            "ok": True,
            "counts": {
                "total": len(ids),
                "started": started,
                "locked": locked,
                "failed": failed,
                "invalid": len(bad),
            },
            "results": results,
            "invalid": bad,
        }
    )


@app.get("/guilds")
async def guilds_page(request: Request):
    env = _read_env()
    return templates.TemplateResponse(
        "guilds.html",
        {
            "request": request,
            "title": APP_TITLE,
            "version": CURRENT_VERSION,
            "log_level": env.get("LOG_LEVEL", "INFO"),
        },
    )


@app.get("/api/guilds", response_class=JSONResponse)
async def guilds_api():
    """
    Return list of guilds for the UI.
    Shape:
      { items: [ { id, name, icon_url, member_count }, ... ] }
    """
    rows = db.get_all_guilds()
    items = []
    for r in rows:
        items.append(
            {
                "id": str(r.get("guild_id", "")),
                "name": r.get("name") or "Unknown guild",
                "icon_url": r.get("icon_url"),
                "member_count": r.get("member_count"),
            }
        )
    return {"items": items}


CLIENT_AGENT_TIMEOUT = int(os.getenv("CLIENT_AGENT_TIMEOUT", "10"))


@app.get("/scraper")
async def scraper_page(request: Request):
    """Render the standalone scraper page."""
    env = _read_env()
    return templates.TemplateResponse(
        "scraper.html",
        {
            "request": request,
            "title": APP_TITLE,
            "version": CURRENT_VERSION,
            "log_level": env.get("LOG_LEVEL", "INFO"),
        },
    )


@app.get("/api/scraper/tokens", response_class=JSONResponse)
async def api_scraper_tokens_list():
    """List all scraper tokens with masked values."""
    try:
        tokens = db.list_scraper_tokens()
        masked_tokens = []
        for t in tokens:
            masked_tokens.append(
                {
                    "token_id": t["token_id"],
                    "label": t.get("label") or "Unlabeled",
                    "masked": _mask_token(t["token_value"]),
                    "is_valid": bool(t.get("is_valid")),
                    "last_validated": t.get("last_validated"),
                    "username": t.get("username"),
                    "user_id": t.get("user_id"),
                    "added_at": t.get("added_at"),
                    "last_used": t.get("last_used"),
                    "use_count": t.get("use_count", 0),
                }
            )
        return JSONResponse({"ok": True, "tokens": masked_tokens})
    except Exception as e:
        LOGGER.exception("Failed to list scraper tokens: %s", e)
        return JSONResponse({"ok": False, "error": "server-error"}, status_code=500)


@app.post("/api/scraper/tokens/add", response_class=JSONResponse)
async def api_scraper_tokens_add(
    token_value: str = Form(...),
    label: str = Form(""),
):
    """Add and validate a new scraper token."""
    token_value = (token_value or "").strip()
    label = (label or "").strip()

    if not token_value:
        raise HTTPException(status_code=400, detail="token_value is required")

    try:
        existing = db.list_scraper_tokens()
        for t in existing:
            if t["token_value"] == token_value:
                raise HTTPException(400, detail="Token already exists")
    except HTTPException:
        raise
    except Exception:
        pass

    is_valid = await _check_client_token_valid(token_value)

    username = None
    user_id = None
    if is_valid:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    f"{DISCORD_API_BASE}/users/@me",
                    headers={"Authorization": token_value},
                    timeout=5,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        username = data.get("username")
                        user_id = str(data.get("id"))
        except Exception:
            pass

    try:
        token_id = db.add_scraper_token(token_value, label)
        db.update_scraper_token(
            token_id,
            is_valid=is_valid,
            username=username,
            user_id=user_id,
        )

        return JSONResponse(
            {
                "ok": True,
                "token_id": token_id,
                "is_valid": is_valid,
                "username": username,
                "user_id": user_id,
            }
        )
    except Exception as e:
        LOGGER.exception("Failed to add scraper token: %s", e)
        return JSONResponse({"ok": False, "error": "server-error"}, status_code=500)


@app.post("/api/scraper/tokens/{token_id}/validate", response_class=JSONResponse)
async def api_scraper_tokens_validate(token_id: str):
    """Validate a scraper token."""
    token = db.get_scraper_token(token_id)
    if not token:
        raise HTTPException(404, detail="Token not found")

    is_valid = await _check_client_token_valid(token["token_value"])

    username = None
    user_id = None
    if is_valid:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(
                    f"{DISCORD_API_BASE}/users/@me",
                    headers={"Authorization": token["token_value"]},
                    timeout=5,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        username = data.get("username")
                        user_id = str(data.get("id"))
        except Exception:
            pass

    db.update_scraper_token(
        token_id,
        is_valid=is_valid,
        username=username,
        user_id=user_id,
    )

    return JSONResponse(
        {
            "ok": True,
            "is_valid": is_valid,
            "username": username,
            "user_id": user_id,
        }
    )


@app.post("/api/scraper/tokens/{token_id}/update", response_class=JSONResponse)
async def api_scraper_tokens_update(token_id: str, label: str = Form(...)):
    """Update a scraper token's label."""
    if not db.get_scraper_token(token_id):
        raise HTTPException(404, detail="Token not found")

    db.update_scraper_token(token_id, label=label.strip())
    return JSONResponse({"ok": True})


@app.delete("/api/scraper/tokens/{token_id}", response_class=JSONResponse)
async def api_scraper_tokens_delete(token_id: str):
    """Delete a scraper token."""
    if not db.delete_scraper_token(token_id):
        raise HTTPException(404, detail="Token not found")
    return JSONResponse({"ok": True})


@app.post("/api/scraper/validate-setup", response_class=JSONResponse)
async def api_scraper_validate_setup(guild_id: str = Form(...)):
    """Validate that all tokens have access to the specified guild."""
    try:
        gid = int(guild_id)
    except ValueError:
        raise HTTPException(400, detail="Invalid guild_id")

    tokens = db.get_valid_scraper_tokens()
    if not tokens:
        return JSONResponse(
            {
                "ok": False,
                "error": "No valid tokens configured",
                "tokens_in_guild": 0,
                "tokens_missing": 0,
            },
            status_code=400,
        )

    results = []
    in_guild = 0
    missing = 0

    for token in tokens:
        has_access = await _selfbot_in_guild(token["token_value"], gid)
        results.append(
            {
                "token_id": token["token_id"],
                "label": token.get("label") or token.get("username") or "Unknown",
                "has_access": has_access,
            }
        )
        if has_access:
            in_guild += 1
        else:
            missing += 1

    return JSONResponse(
        {
            "ok": in_guild > 0,
            "tokens_in_guild": in_guild,
            "tokens_missing": missing,
            "results": results,
        }
    )


_PROXY_FILE = DATA_DIR / "proxies.txt"


@app.get("/api/scraper/proxies", response_class=JSONResponse)
async def api_scraper_proxies_get():
    """Return saved proxy list."""
    try:
        if _PROXY_FILE.exists():
            text = _PROXY_FILE.read_text(encoding="utf-8").strip()
            lines = [l.strip() for l in text.splitlines() if l.strip()]
        else:
            lines = []
        return JSONResponse({"ok": True, "proxies": lines})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/scraper/proxies", response_class=JSONResponse)
async def api_scraper_proxies_put(request: Request):
    """Save proxy list."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    proxies = payload.get("proxies", [])
    if not isinstance(proxies, list):
        raise HTTPException(400, detail="proxies must be a list")
    lines = [l.strip() for l in proxies if isinstance(l, str) and l.strip()]
    try:
        _PROXY_FILE.write_text(
            "\n".join(lines) + ("\n" if lines else ""), encoding="utf-8"
        )
        return JSONResponse({"ok": True, "count": len(lines)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)




@app.get("/api/server/proxies", response_class=JSONResponse)
async def api_server_proxies_get():
    """Return server proxy list and enabled state."""
    try:
        if _PROXY_FILE.exists():
            text = _PROXY_FILE.read_text(encoding="utf-8").strip()
            lines = [l.strip() for l in text.splitlines() if l.strip()]
        else:
            lines = []
        enabled = (db.get_config("ENABLE_SERVER_PROXIES", "") or "").strip().lower() in (
            "1", "true", "yes",
        )
        return JSONResponse({"ok": True, "proxies": lines, "enabled": enabled})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/server/proxies", response_class=JSONResponse)
async def api_server_proxies_put(request: Request):
    """Save server proxy list (shared file with scraper)."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    proxies = payload.get("proxies", [])
    if not isinstance(proxies, list):
        raise HTTPException(400, detail="proxies must be a list")
    lines = [l.strip() for l in proxies if isinstance(l, str) and l.strip()]
    try:
        _PROXY_FILE.write_text(
            "\n".join(lines) + ("\n" if lines else ""), encoding="utf-8"
        )
        return JSONResponse({"ok": True, "count": len(lines)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/server/proxies/toggle", response_class=JSONResponse)
async def api_server_proxies_toggle(request: Request):
    """Enable or disable server proxy rotation."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    enabled = bool(payload.get("enabled", False))
    try:
        db.set_config("ENABLE_SERVER_PROXIES", "true" if enabled else "false")
        return JSONResponse({"ok": True, "enabled": enabled})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/scraper/start", response_class=JSONResponse)
async def api_scraper_start(request: Request):
    """Start standalone scraper with multiple tokens (single guild, immediate start)."""
    global scraper_state

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")

    try:
        guild_id = int(payload.get("guild_id"))
    except (ValueError, TypeError):
        raise HTTPException(400, detail="Invalid guild_id")

    include_username = bool(payload.get("include_username", True))
    include_avatar_url = bool(payload.get("include_avatar_url", True))
    include_bio = bool(payload.get("include_bio", False))
    include_roles = bool(payload.get("include_roles", False))
    proxy_list = [
        p.strip()
        for p in (payload.get("proxies") or [])
        if isinstance(p, str) and p.strip()
    ]

    tokens = db.get_valid_scraper_tokens()
    if not tokens:
        return JSONResponse(
            {"ok": False, "error": "No valid tokens configured"}, status_code=400
        )

    accessible_tokens = []
    for token in tokens:
        if await _selfbot_in_guild(token["token_value"], guild_id):
            accessible_tokens.append(token)
            db.increment_scraper_token_usage(token["token_id"])

    if not accessible_tokens:
        return JSONResponse(
            {
                "ok": False,
                "error": "None of the configured tokens have access to this guild",
            },
            status_code=400,
        )

    # Build a queue item and add it
    item = {
        "id": _next_queue_id(),
        "guild_id": guild_id,
        "include_username": include_username,
        "include_avatar_url": include_avatar_url,
        "include_bio": include_bio,
        "include_roles": include_roles,
        "proxies": proxy_list,
        "status": "pending",
        "added_at": time.time(),
    }

    async with scraper_lock:
        scraper_state["queue"].append(item)

    # Ensure the queue processor is running
    await _ensure_queue_processor()

    await _broadcast_queue_update()

    return JSONResponse(
        {
            "ok": True,
            "message": "Scraper queued" if scraper_state["running"] else "Scraper started",
            "tokens_used": len(accessible_tokens),
            "proxies_used": len(proxy_list),
            "guild_id": guild_id,
            "queue_id": item["id"],
            "queue_position": len(scraper_state["queue"]),
        }
    )


@app.post("/api/scraper/queue/add", response_class=JSONResponse)
async def api_scraper_queue_add(request: Request):
    """Add a guild to the scraper queue."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")

    try:
        guild_id = int(payload.get("guild_id"))
    except (ValueError, TypeError):
        raise HTTPException(400, detail="Invalid guild_id")

    include_username = bool(payload.get("include_username", True))
    include_avatar_url = bool(payload.get("include_avatar_url", True))
    include_bio = bool(payload.get("include_bio", False))
    include_roles = bool(payload.get("include_roles", False))
    proxy_list = [
        p.strip()
        for p in (payload.get("proxies") or [])
        if isinstance(p, str) and p.strip()
    ]

    item = {
        "id": _next_queue_id(),
        "guild_id": guild_id,
        "include_username": include_username,
        "include_avatar_url": include_avatar_url,
        "include_bio": include_bio,
        "include_roles": include_roles,
        "proxies": proxy_list,
        "status": "pending",
        "added_at": time.time(),
    }

    async with scraper_lock:
        scraper_state["queue"].append(item)

    await _broadcast_queue_update()

    return JSONResponse(
        {
            "ok": True,
            "queue_id": item["id"],
            "queue_position": len(scraper_state["queue"]),
        }
    )


@app.get("/api/scraper/queue", response_class=JSONResponse)
async def api_scraper_queue_list():
    """Get the current scraper queue."""
    async with scraper_lock:
        queue_items = [
            {
                "id": item["id"],
                "guild_id": item["guild_id"],
                "status": item["status"],
                "include_username": item.get("include_username", True),
                "include_avatar_url": item.get("include_avatar_url", True),
                "include_bio": item.get("include_bio", False),
                "include_roles": item.get("include_roles", False),
                "added_at": item.get("added_at"),
            }
            for item in scraper_state["queue"]
        ]
        return JSONResponse({"ok": True, "queue": queue_items})


@app.delete("/api/scraper/queue/{queue_id}", response_class=JSONResponse)
async def api_scraper_queue_remove(queue_id: int):
    """Remove an item from the scraper queue (only if not currently running)."""
    async with scraper_lock:
        for i, item in enumerate(scraper_state["queue"]):
            if item["id"] == queue_id:
                if item["status"] == "running":
                    return JSONResponse(
                        {"ok": False, "error": "Cannot remove a running scrape. Use cancel instead."},
                        status_code=409,
                    )
                scraper_state["queue"].pop(i)
                break
        else:
            return JSONResponse(
                {"ok": False, "error": "Queue item not found"}, status_code=404
            )

    await _broadcast_queue_update()
    return JSONResponse({"ok": True})


@app.post("/api/scraper/queue/clear", response_class=JSONResponse)
async def api_scraper_queue_clear():
    """Clear all pending items from the queue (does not cancel the running one)."""
    async with scraper_lock:
        scraper_state["queue"] = [
            item for item in scraper_state["queue"] if item["status"] == "running"
        ]

    await _broadcast_queue_update()
    return JSONResponse({"ok": True})


@app.post("/api/scraper/queue/start", response_class=JSONResponse)
async def api_scraper_queue_start():
    """Start processing the queue."""
    async with scraper_lock:
        if not scraper_state["queue"]:
            return JSONResponse(
                {"ok": False, "error": "Queue is empty"}, status_code=400
            )

    await _ensure_queue_processor()
    return JSONResponse({"ok": True})


async def _broadcast_queue_update():
    """Broadcast the current queue state to all websocket clients."""
    async with scraper_lock:
        queue_items = [
            {
                "id": item["id"],
                "guild_id": item["guild_id"],
                "status": item["status"],
                "added_at": item.get("added_at"),
            }
            for item in scraper_state["queue"]
        ]
    await hub.broadcast(
        {
            "kind": "scraper",
            "role": "standalone",
            "payload": {
                "type": "queue_update",
                "queue": queue_items,
                "running": scraper_state["running"],
            },
        }
    )


async def _ensure_queue_processor():
    """Start the queue processor task if not already running."""
    global scraper_state
    async with scraper_lock:
        if scraper_state.get("queue_running"):
            return
        scraper_state["queue_running"] = True

    task = asyncio.create_task(_queue_processor())
    async with scraper_lock:
        scraper_state["queue_task"] = task


async def _queue_processor():
    """Process the scraper queue sequentially — one guild at a time."""
    global scraper_state

    try:
        while True:
            # Find the next pending item
            next_item = None
            async with scraper_lock:
                for item in scraper_state["queue"]:
                    if item["status"] == "pending":
                        next_item = item
                        break

            if next_item is None:
                # No more pending items — stop the processor
                break

            await _run_single_scrape(next_item)

    except asyncio.CancelledError:
        LOGGER.info("Queue processor cancelled")
    except Exception as e:
        LOGGER.exception("Queue processor error: %s", e)
    finally:
        async with scraper_lock:
            scraper_state["queue_running"] = False
            scraper_state["queue_task"] = None
        await _broadcast_queue_update()


async def _run_single_scrape(queue_item: dict):
    """Run a single scrape for one queue item."""
    global scraper_state

    guild_id = queue_item["guild_id"]
    include_username = queue_item.get("include_username", True)
    include_avatar_url = queue_item.get("include_avatar_url", True)
    include_bio = queue_item.get("include_bio", False)
    include_roles = queue_item.get("include_roles", False)
    proxy_list = queue_item.get("proxies", [])

    tokens = db.get_valid_scraper_tokens()
    if not tokens:
        queue_item["status"] = "error"
        await hub.broadcast(
            {
                "kind": "scraper",
                "role": "standalone",
                "payload": {
                    "type": "complete",
                    "success": False,
                    "total_count": 0,
                    "error": "No valid tokens configured",
                    "elapsed_seconds": 0,
                    "guild_id": guild_id,
                    "queue_id": queue_item["id"],
                },
            }
        )
        await _broadcast_queue_update()
        return

    accessible_tokens = []
    for token in tokens:
        if await _selfbot_in_guild(token["token_value"], guild_id):
            accessible_tokens.append(token)
            db.increment_scraper_token_usage(token["token_id"])

    if not accessible_tokens:
        queue_item["status"] = "error"
        await hub.broadcast(
            {
                "kind": "scraper",
                "role": "standalone",
                "payload": {
                    "type": "complete",
                    "success": False,
                    "total_count": 0,
                    "error": f"No tokens have access to guild {guild_id}",
                    "elapsed_seconds": 0,
                    "guild_id": guild_id,
                    "queue_id": queue_item["id"],
                },
            }
        )
        await _broadcast_queue_update()
        return

    token_values = [t["token_value"] for t in accessible_tokens]

    def progress_callback(current: int, total: Optional[int], message: str):
        """Broadcast progress to websocket clients."""
        asyncio.create_task(
            hub.broadcast(
                {
                    "kind": "scraper",
                    "role": "standalone",
                    "payload": {
                        "type": "progress",
                        "current": current,
                        "total": total,
                        "message": message,
                        "guild_id": guild_id,
                        "queue_id": queue_item["id"],
                    },
                }
            )
        )

    _scraper_log_path = DATA_DIR / "scraper.out"

    def log_callback(message: str, level: str):
        """Write log line to scraper.out AND broadcast to websocket clients."""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{level.upper()}] [Guild {guild_id}] {message}\n"
        try:
            with open(_scraper_log_path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
        asyncio.create_task(
            hub.broadcast(
                {
                    "kind": "scraper",
                    "role": "standalone",
                    "payload": {
                        "type": "log",
                        "message": message,
                        "level": level,
                        "guild_id": guild_id,
                    },
                }
            )
        )

    try:
        _scraper_log_path.write_text("", encoding="utf-8")
    except Exception:
        pass

    config = ScraperConfig(
        guild_id=guild_id,
        tokens=token_values,
        proxies=proxy_list,
        include_username=include_username,
        include_avatar_url=include_avatar_url,
        include_bio=include_bio,
        include_roles=include_roles,
        progress_callback=progress_callback,
        log_callback=log_callback,
    )

    scraper = StandaloneScraper(config)

    # Mark as running
    async with scraper_lock:
        queue_item["status"] = "running"
        scraper_state["running"] = True
        scraper_state["scraper"] = scraper
        scraper_state["result"] = None
        scraper_state["guild_id"] = guild_id
        scraper_state["started_at"] = time.time()

    await _broadcast_queue_update()

    LOGGER.info(
        "Starting standalone scraper: guild=%s tokens=%d proxies=%d username=%s avatar=%s bio=%s roles=%s",
        guild_id,
        len(accessible_tokens),
        len(proxy_list),
        include_username,
        include_avatar_url,
        include_bio,
        include_roles,
    )

    def _save_results_to_disk(members, metadata, total_count, elapsed_seconds):
        """Save results to /data/scrapes/{guild_id}/ — returns filename or None."""
        if not members:
            return None
        try:
            guild_dir = DATA_DIR / "scrapes" / str(guild_id)
            guild_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            g_name = (metadata or {}).get("guild_name") or ""
            fname = f"scrape_{guild_id}_{ts}.json"
            save_data = {
                "guild_id": str(guild_id),
                "guild_name": g_name,
                "total_count": total_count,
                "elapsed_seconds": round(elapsed_seconds, 2),
                "scraped_at": datetime.now().isoformat(),
                "metadata": metadata,
                "members": members,
            }
            (guild_dir / fname).write_text(
                json.dumps(save_data, indent=2), encoding="utf-8"
            )
            LOGGER.info("Scrape results saved to %s/%s", guild_id, fname)
            return f"{guild_id}/{fname}"
        except Exception as save_err:
            LOGGER.exception("Failed to save scrape results: %s", save_err)
            return None

    try:
        result = await scraper.scrape()
        async with scraper_lock:
            scraper_state["result"] = result
            scraper_state["running"] = False
            scraper_state["scraper"] = None
            scraper_state["started_at"] = None
            queue_item["status"] = "done"

        saved_filename = _save_results_to_disk(
            result.members,
            result.metadata,
            result.total_count,
            result.elapsed_seconds,
        )

        await hub.broadcast(
            {
                "kind": "scraper",
                "role": "standalone",
                "payload": {
                    "type": "complete",
                    "success": result.success,
                    "total_count": result.total_count,
                    "error": result.error,
                    "elapsed_seconds": result.elapsed_seconds,
                    "saved_filename": saved_filename,
                    "guild_id": guild_id,
                    "queue_id": queue_item["id"],
                },
            }
        )
    except asyncio.CancelledError:
        LOGGER.info("Scraper task cancelled for guild %s", guild_id)
        members = list(scraper._members.values()) if scraper._members else []
        elapsed = 0

        saved_filename = _save_results_to_disk(members, {}, len(members), elapsed)
        async with scraper_lock:
            scraper_state["result"] = ScraperResult(
                success=False,
                error="Cancelled",
                members=members,
                total_count=len(members),
                elapsed_seconds=elapsed,
            )
            scraper_state["running"] = False
            scraper_state["scraper"] = None
            scraper_state["started_at"] = None
            queue_item["status"] = "cancelled"

        await hub.broadcast(
            {
                "kind": "scraper",
                "role": "standalone",
                "payload": {
                    "type": "complete",
                    "success": False,
                    "total_count": len(members),
                    "error": "Cancelled",
                    "elapsed_seconds": elapsed,
                    "saved_filename": saved_filename,
                    "guild_id": guild_id,
                    "queue_id": queue_item["id"],
                },
            }
        )
        # Re-raise so the queue processor knows it was cancelled
        raise
    except Exception as e:
        LOGGER.exception("Scraper task failed for guild %s: %s", guild_id, e)
        async with scraper_lock:
            scraper_state["running"] = False
            scraper_state["scraper"] = None
            scraper_state["started_at"] = None
            queue_item["status"] = "error"

        await hub.broadcast(
            {
                "kind": "scraper",
                "role": "standalone",
                "payload": {
                    "type": "complete",
                    "success": False,
                    "total_count": 0,
                    "error": str(e),
                    "elapsed_seconds": 0,
                    "guild_id": guild_id,
                    "queue_id": queue_item["id"],
                },
            }
        )
    finally:
        # Remove completed/error/cancelled item from queue
        async with scraper_lock:
            scraper_state["queue"] = [
                it for it in scraper_state["queue"] if it["id"] != queue_item["id"]
            ]
        await _broadcast_queue_update()


@app.get("/api/scraper/status", response_class=JSONResponse)
async def api_scraper_status():
    """Get current scraper status including queue."""
    async with scraper_lock:
        queue_items = [
            {
                "id": item["id"],
                "guild_id": item["guild_id"],
                "status": item["status"],
                "added_at": item.get("added_at"),
            }
            for item in scraper_state["queue"]
        ]
        return JSONResponse(
            {
                "running": scraper_state["running"],
                "guild_id": scraper_state.get("guild_id"),
                "started_at": scraper_state.get("started_at"),
                "queue": queue_items,
                "queue_running": scraper_state.get("queue_running", False),
            }
        )


@app.post("/api/scraper/cancel", response_class=JSONResponse)
async def api_scraper_cancel():
    """Cancel the currently running scrape and optionally stop the entire queue."""
    async with scraper_lock:
        if not scraper_state["running"]:
            return JSONResponse(
                {"ok": False, "error": "No scraper running"}, status_code=400
            )

        scraper = scraper_state.get("scraper")
        if scraper:
            scraper.stop()

        return JSONResponse({"ok": True})


@app.post("/api/scraper/cancel-all", response_class=JSONResponse)
async def api_scraper_cancel_all():
    """Cancel the current scrape AND clear the entire pending queue."""
    async with scraper_lock:
        # Clear all pending items
        scraper_state["queue"] = [
            item for item in scraper_state["queue"] if item["status"] == "running"
        ]

        # Stop the current scraper
        scraper = scraper_state.get("scraper")
        if scraper:
            scraper.stop()

        # Cancel the queue processor task
        qt = scraper_state.get("queue_task")
        if qt and not qt.done():
            qt.cancel()

    await _broadcast_queue_update()
    return JSONResponse({"ok": True})


@app.get("/api/scraper/results", response_class=JSONResponse)
async def api_scraper_results():
    """Get scraper results."""
    async with scraper_lock:
        result = scraper_state.get("result")
        if not result:
            return JSONResponse(
                {"ok": False, "error": "No results available"}, status_code=404
            )

        return JSONResponse(
            {
                "ok": True,
                "members": result.members,
                "total_count": result.total_count,
                "success": result.success,
                "error": result.error,
                "elapsed_seconds": result.elapsed_seconds,
                "metadata": result.metadata,
            }
        )


@app.get("/api/scraper/scrapes", response_class=JSONResponse)
async def api_scraper_scrapes_list():
    """List all saved scrape files with metadata (without the full members array).

    Walks data/scrapes/{guild_id}/ subdirectories.
    """
    scrapes_dir = DATA_DIR / "scrapes"
    if not scrapes_dir.is_dir():
        return JSONResponse({"ok": True, "scrapes": []})

    scrapes = []

    for guild_folder in sorted(scrapes_dir.iterdir(), reverse=True):
        if not guild_folder.is_dir():

            if guild_folder.suffix == ".json" and guild_folder.name.startswith(
                "scrape_"
            ):
                try:
                    raw = guild_folder.read_text(encoding="utf-8")
                    data = json.loads(raw)
                    scrapes.append(
                        {
                            "filename": guild_folder.name,
                            "path": guild_folder.name,
                            "guild_id": data.get("guild_id"),
                            "guild_name": data.get("guild_name", ""),
                            "total_count": data.get("total_count", 0),
                            "elapsed_seconds": data.get("elapsed_seconds"),
                            "scraped_at": data.get("scraped_at"),
                            "metadata": data.get("metadata", {}),
                            "file_size": guild_folder.stat().st_size,
                        }
                    )
                except Exception:
                    scrapes.append(
                        {
                            "filename": guild_folder.name,
                            "path": guild_folder.name,
                            "error": "unreadable",
                        }
                    )
            continue

        guild_id_str = guild_folder.name
        for fp in sorted(guild_folder.glob("scrape_*.json"), reverse=True):
            try:
                raw = fp.read_text(encoding="utf-8")
                data = json.loads(raw)
                scrapes.append(
                    {
                        "filename": fp.name,
                        "path": f"{guild_id_str}/{fp.name}",
                        "guild_id": data.get("guild_id", guild_id_str),
                        "guild_name": data.get("guild_name", ""),
                        "total_count": data.get("total_count", 0),
                        "elapsed_seconds": data.get("elapsed_seconds"),
                        "scraped_at": data.get("scraped_at"),
                        "metadata": data.get("metadata", {}),
                        "file_size": fp.stat().st_size,
                    }
                )
            except Exception:
                scrapes.append(
                    {
                        "filename": fp.name,
                        "path": f"{guild_id_str}/{fp.name}",
                        "error": "unreadable",
                    }
                )

    scrapes.sort(key=lambda s: s.get("scraped_at") or "", reverse=True)
    return JSONResponse({"ok": True, "scrapes": scrapes})


@app.get("/api/scraper/scrapes/{filepath:path}")
async def api_scraper_scrape_download(filepath: str):
    """Download a saved scrape file. Accepts guild_id/filename or just filename."""

    parts = Path(filepath).parts
    if len(parts) > 2 or ".." in filepath:
        raise HTTPException(400, detail="Invalid path")

    fp = DATA_DIR / "scrapes" / filepath
    if not fp.is_file():
        raise HTTPException(404, detail="File not found")

    return FileResponse(
        path=str(fp),
        filename=fp.name,
        media_type="application/json",
    )


@app.delete("/api/scraper/scrapes/{filepath:path}", response_class=JSONResponse)
async def api_scraper_scrape_delete(filepath: str):
    """Delete a saved scrape file. Accepts guild_id/filename or just filename."""
    parts = Path(filepath).parts
    if len(parts) > 2 or ".." in filepath:
        raise HTTPException(400, detail="Invalid path")

    fp = DATA_DIR / "scrapes" / filepath
    if not fp.is_file():
        raise HTTPException(404, detail="File not found")

    try:
        fp.unlink()

        parent = fp.parent
        if (
            parent != DATA_DIR / "scrapes"
            and parent.is_dir()
            and not any(parent.iterdir())
        ):
            parent.rmdir()
        return JSONResponse({"ok": True})
    except Exception as e:
        LOGGER.exception("Failed to delete scrape file %s: %s", filepath, e)
        return JSONResponse({"ok": False, "error": "delete-failed"}, status_code=500)


@app.post("/api/scraper/logs/clear", response_class=JSONResponse)
async def api_scraper_logs_clear():
    """Clear the scraper.out log file on disk."""
    log_path = DATA_DIR / "scraper.out"
    try:
        log_path.write_text("", encoding="utf-8")
        return JSONResponse({"ok": True})
    except Exception as e:
        LOGGER.exception("Failed to clear scraper log: %s", e)
        return JSONResponse({"ok": False, "error": "clear-failed"}, status_code=500)


@app.get("/api/guilds/{guild_id}", response_class=JSONResponse)
async def guild_details(guild_id: str):
    """
    Return details for a single guild.
    Shape:
      { id, name, icon_url, member_count, ... }
    """
    try:
        rows = db.get_all_guilds()
        row = next((r for r in rows if str(r.get("guild_id")) == str(guild_id)), None)
        if not row:
            return JSONResponse({"ok": False, "error": "not-found"}, status_code=404)

        out = {
            "id": str(row.get("guild_id") or ""),
            "name": row.get("name") or "Unknown guild",
            "icon_url": row.get("icon_url"),
            "member_count": row.get("member_count"),
            "owner_id": row.get("owner_id"),
            "created_at": row.get("created_at"),
            "description": row.get("description"),
        }
        return {"ok": True, "item": out}
    except Exception as e:
        LOGGER.exception("guild_details failed for id=%s: %s", guild_id, e)
        return JSONResponse({"ok": False, "error": "server-error"}, status_code=500)


def _canon(s: str | None) -> str:
    if s is None:
        return ""
    return unicodedata.normalize("NFKC", str(s)).strip()


_DASHES_RE = re.compile(r"-{2,}")


def _discordify(s: str | None) -> str | None:
    """
    Convert free text to a Discord-safe channel name:
    - Normalize NFKC
    - Lowercase A–Z
    - Whitespace -> '-'
    - Allow emojis and most Unicode symbols (no aggressive stripping)
    - Collapse multiple '-'
    - Trim leading/trailing '-'
    - Enforce max length 100
    """
    if s is None:
        return None

    t = unicodedata.normalize("NFKC", str(s)).strip()
    if not t:
        return None

    t = t.lower()

    t = re.sub(r"\s+", "-", t)

    t = _DASHES_RE.sub("-", t).strip("-")

    if len(t) > 100:
        t = t[:100].rstrip("-") or None

    return t or None


@app.post("/api/channels/customize", response_class=JSONResponse)
async def api_channels_customize(payload: dict = Body(...)):
    """
    Set or clear a channel's custom clone name, scoped to (original_channel_id, cloned_guild_id).
    """
    try:
        ocid = int(payload.get("original_channel_id"))
        cgid = int(payload.get("cloned_guild_id"))
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid-ids"}, status_code=400)

    desired = _discordify(payload.get("clone_channel_name", None))

    try:
        orig = db.get_original_channel_name(ocid)
    except Exception:
        orig = None
    if desired is not None and _canon(orig) == desired:
        desired = None

    try:
        current_raw = db.get_clone_channel_name(ocid, cgid)
    except Exception:
        current_raw = None

    needs_update = (desired is None and current_raw is not None) or (
        desired is not None and current_raw != desired
    )
    if not needs_update:
        return JSONResponse(
            {"ok": True, "changed": False, "normalized": desired is not None}
        )

    try:
        db.set_channel_clone_name(ocid, cgid, desired)
    except Exception as e:
        LOGGER.exception("Failed to set clone_channel_name: %s", e)
        return JSONResponse({"ok": False, "error": "db-failure"}, status_code=500)

    try:
        origin_gid = db.get_original_guild_id_for_channel(ocid)

        mapping_id = None
        if origin_gid is not None:
            row = db.get_mapping_by_original_and_clone(origin_gid, cgid)
            if row:
                mapping_id = row["mapping_id"]

        data = {"guild_id": origin_gid}
        if mapping_id is not None:
            data["mapping_id"] = mapping_id

        asyncio.create_task(
            _ws_cmd(
                CLIENT_AGENT_URL,
                {"type": "sitemap_request", "data": data},
                timeout=1.0,
            )
        )
    except Exception:
        LOGGER.debug("WS sitemap_request dispatch failed", exc_info=True)

    return JSONResponse(
        {"ok": True, "changed": True, "nudged": True, "normalized_name": desired}
    )


@app.post("/api/categories/customize", response_class=JSONResponse)
async def api_categories_customize(payload: dict = Body(...)):
    """
    Set or clear a category's custom display name, scoped to (original_category_id, cloned_guild_id).
    """

    def _norm_display(s):
        if s is None:
            return None
        s = unicodedata.normalize("NFKC", str(s)).strip()
        return s if s else None

    if "original_category_id" in payload:
        try:
            ocid = int(payload.get("original_category_id"))
        except Exception as e:
            LOGGER.warning(
                "Customize category | invalid original_category_id in payload=%r: %s",
                payload.get("original_category_id"),
                e,
            )
            return JSONResponse(
                {"ok": False, "error": "invalid-original_category_id"}, status_code=400
            )
    else:
        name = _norm_display(payload.get("category_name"))
        ocid = db.resolve_original_category_id_by_name(name) if name else None
        if not ocid:
            LOGGER.warning(
                "Customize category | missing/unresolvable category for name=%r, payload=%r",
                name,
                payload,
            )
            return JSONResponse(
                {"ok": False, "error": "missing-or-unresolvable-category"},
                status_code=400,
            )

    try:
        cgid_raw = payload.get("cloned_guild_id")
        cgid = int(cgid_raw)
    except Exception as e:
        LOGGER.warning(
            "Customize category | invalid cloned_guild_id in payload=%r: %s",
            payload.get("cloned_guild_id"),
            e,
        )
        return JSONResponse(
            {"ok": False, "error": "invalid-cloned_guild_id"}, status_code=400
        )

    desired_raw = payload.get(
        "custom_category_name", payload.get("clone_category_name")
    )
    desired = _norm_display(desired_raw)

    try:
        orig = db.get_original_category_name(ocid)
        LOGGER.debug(
            "Customize category | original name for ocid=%s: %r",
            ocid,
            orig,
        )
    except Exception as e:
        LOGGER.warning(
            "Customize category | failed to load original name for ocid=%s: %s",
            ocid,
            e,
        )
        orig = None

    if desired is not None and _norm_display(orig) == _norm_display(desired):
        desired = None

    try:
        current_raw = db.get_clone_category_name(ocid, cgid)
    except Exception as e:
        LOGGER.warning(
            "Customize category | failed to load current cloned name for (ocid=%s, cgid=%s): %s",
            ocid,
            cgid,
            e,
        )
        current_raw = None

    if _norm_display(current_raw) == _norm_display(desired):
        return JSONResponse(
            {"ok": True, "changed": False, "normalized": desired is not None}
        )

    try:
        db.set_category_clone_name(ocid, cgid, desired)
    except Exception as e:
        LOGGER.exception(
            "Failed to set cloned_category_name for (ocid=%s, cgid=%s): %s",
            ocid,
            cgid,
            e,
        )
        return JSONResponse({"ok": False, "error": "db-failure"}, status_code=500)

    try:
        LOGGER.debug(
            "Customize category | nudging sitemap_request via WS for (ocid=%s, cgid=%s)",
            ocid,
            cgid,
        )

        origin_gid = db.get_original_guild_id_for_category(ocid)

        mapping_id = None
        if origin_gid is not None:
            row = db.get_mapping_by_original_and_clone(origin_gid, cgid)
            if row:
                mapping_id = row["mapping_id"]

        data = {"guild_id": origin_gid}
        if mapping_id is not None:
            data["mapping_id"] = mapping_id

        asyncio.create_task(
            _ws_cmd(
                CLIENT_AGENT_URL,
                {"type": "sitemap_request", "data": data},
                timeout=1.0,
            )
        )
    except Exception:
        LOGGER.debug("WS sitemap_request dispatch failed", exc_info=True)

    return JSONResponse(
        {
            "ok": True,
            "changed": True,
            "nudged": True,
            "normalized_name": desired,
            "original_category_id": ocid,
            "cloned_guild_id": cgid,
        }
    )


@app.get("/version")
def get_version():
    current = CURRENT_VERSION or db.get_version()
    latest = db.get_config("latest_tag", "")
    url = db.get_config("latest_url", "")

    def norm(v: str):
        import re

        v = (v or "").strip()
        if v.lower().startswith("v"):
            v = v[1:]
        v = re.sub(r"[^0-9.]", "", v)
        parts = [p for p in v.split(".") if p.isdigit()]
        while len(parts) < 3:
            parts.append("0")
        return ".".join(parts[:3])

    ca = tuple(int(x) for x in norm(current).split("."))
    lb = tuple(int(x) for x in norm(latest).split(".")) if latest else (0, 0, 0)

    return {
        "current": current,
        "latest": latest or current,
        "url": url
        or f"https://github.com/Copycord/Copycord/releases/tag/{latest or current}",
        "update_available": bool(latest) and (lb > ca),
    }


async def _fetch_latest_release(session: aiohttp.ClientSession) -> dict | None:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "copycord-app",
    }

    etag = db.get_config("gh_releases_etag", "")
    if etag:
        headers["If-None-Match"] = etag

    url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
    async with session.get(url, headers=headers, timeout=20) as r:
        if r.status == 304:
            return None
        r.raise_for_status()
        data = await r.json()
        new_etag = r.headers.get("ETag") or ""
        if new_etag and new_etag != etag:
            db.set_config("gh_releases_etag", new_etag)

    tag = data.get("tag_name")
    html_url = data.get("html_url")
    published_at = data.get("published_at")
    if not tag or not html_url:
        return None
    return {"tag": tag, "url": html_url, "published_at": published_at}


async def _release_watch_loop():
    await asyncio.sleep(2)
    LOGGER.debug("Starting GitHub release watcher for %s", GITHUB_REPO)
    async with aiohttp.ClientSession() as session:
        while not shutdown_event.is_set():
            try:
                try:
                    recorded_ver = db.get_version()
                    if recorded_ver != CURRENT_VERSION:
                        db.set_version(CURRENT_VERSION)
                except AttributeError:
                    recorded_ver = db.get_config("current_version", "")
                    if recorded_ver != CURRENT_VERSION:
                        db.set_config("current_version", CURRENT_VERSION)

                rel = await _fetch_latest_release(session)
                if rel:
                    prev = db.get_config("latest_tag", "")
                    if rel["tag"] != prev:
                        db.set_config("latest_tag", rel["tag"])
                        db.set_config("latest_url", rel["url"])
                        if rel.get("published_at"):
                            db.set_config("latest_published_at", rel["published_at"])

                        LOGGER.info("Detected new release: %s", rel["tag"])
            except Exception:
                LOGGER.exception("release watcher error")

            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=RELEASE_POLL_SECONDS
                )
            except asyncio.TimeoutError:
                pass


@app.post("/api/export/messages", response_class=JSONResponse)
async def api_export_messages(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid-json"}, status_code=400)

    webhook = (payload.get("webhook_url") or "").strip() or None

    data = {
        "guild_id": payload.get("guild_id"),
        "channel_id": payload.get("channel_id"),
        "user_id": payload.get("user_id"),
        "webhook_url": webhook,
        "has_attachments": bool(payload.get("has_attachments", False)),
        "after_iso": payload.get("after_iso"),
        "before_iso": payload.get("before_iso"),
        "filters": payload.get("filters") or {},
    }

    try:
        res = await _ws_cmd(
            CLIENT_AGENT_URL,
            {"type": "export_messages", "data": data},
            timeout=CLIENT_AGENT_TIMEOUT,
        )
    except asyncio.TimeoutError:
        return JSONResponse(
            {"ok": False, "error": "client-agent-timeout"}, status_code=504
        )
    except ConnectionRefusedError:
        return JSONResponse(
            {"ok": False, "error": "client-agent-unreachable"}, status_code=502
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"client-agent-error: {type(e).__name__}: {e}"},
            status_code=502,
        )

    if not res.get("ok", True):
        return JSONResponse(
            {"ok": False, "error": res.get("error") or "client-agent-failed"},
            status_code=502,
        )
    return JSONResponse({"ok": True, "accepted": True})


@app.get("/api/guild-mappings", response_class=JSONResponse)
async def api_list_guild_mappings():
    rows = db.list_guild_mappings()
    return JSONResponse({"ok": True, "mappings": rows})


@app.post("/api/guild-mappings")
async def api_create_mapping(payload: dict = Body(...)):

    mapping_name = (payload.get("mapping_name") or "").strip()

    try:
        host_gid = int(payload.get("original_guild_id") or 0)
        clone_gid = int(payload.get("cloned_guild_id") or 0)
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "Invalid HOST_GUILD_ID or CLONE_GUILD_ID."},
            status_code=400,
        )

    if not mapping_name:
        return JSONResponse(
            {"ok": False, "error": "Missing mapping name."},
            status_code=400,
        )

    settings = payload.get("settings") or {}

    existing_clone = db.get_mapping_by_clone(clone_gid)
    if existing_clone:
        return JSONResponse(
            {
                "ok": False,
                "error": "That clone guild is already mapped to another host.",
                "which": "cloned_guild_id",
            },
            status_code=400,
        )

    client_token = db.get_config("CLIENT_TOKEN", "")
    server_token = db.get_config("SERVER_TOKEN", "")

    in_host = await _selfbot_in_guild(client_token, host_gid)
    if not in_host:
        return JSONResponse(
            {
                "ok": False,
                "error": "Your Discord account is not a member of the host server. Check your token and join the host server with your account before continuing.",
                "which": "original_guild_id",
            },
            status_code=400,
        )

    in_clone = await _bot_in_guild(server_token, clone_gid)
    if not in_clone:
        return JSONResponse(
            {
                "ok": False,
                "error": "Your Discord bot isn’t in the clone server. Check your token and invite the bot to that server with Administrator permission.",
                "which": "cloned_guild_id",
            },
            status_code=400,
        )

    try:
        new_mapping_id = db.upsert_guild_mapping(
            mapping_id=None,
            mapping_name=mapping_name,
            original_guild_id=host_gid,
            original_guild_name="",
            original_guild_icon_url="",
            cloned_guild_id=clone_gid,
            cloned_guild_name="",
            settings=settings,
        )
    except sqlite3.IntegrityError:
        return JSONResponse(
            {
                "ok": False,
                "error": "That host+clone pair already exists.",
            },
            status_code=400,
        )

    return JSONResponse({"ok": True, "mapping_id": new_mapping_id}, status_code=200)


@app.patch("/api/guild-mappings/{mapping_id}")
async def api_update_mapping(mapping_id: str, payload: dict = Body(...)):
    mapping_name = (payload.get("mapping_name") or "").strip()
    original_guild_id = int(payload.get("original_guild_id") or 0)
    cloned_guild_id = int(payload.get("cloned_guild_id") or 0)
    settings = payload.get("settings") or {}

    existing = db.get_mapping_by_id(mapping_id)
    old_host_id = int(existing["original_guild_id"] or 0) if existing else 0
    old_clone_id = int(existing["cloned_guild_id"] or 0) if existing else 0

    client_token = db.get_config("CLIENT_TOKEN", "")
    server_token = db.get_config("SERVER_TOKEN", "")

    in_host = await _selfbot_in_guild(client_token, original_guild_id)
    in_clone = await _bot_in_guild(server_token, cloned_guild_id)

    if not in_host:
        return JSONResponse(
            {
                "ok": False,
                "error": "Your Discord account is not a member of the host server. Join the host server with your account before continuing.",
                "which": "original_guild_id",
            },
            status_code=400,
        )

    if not in_clone:
        return JSONResponse(
            {
                "ok": False,
                "error": "Your Discord bot isn’t in the clone server. Invite the bot to that server and make sure it has the Administrator permission.",
                "which": "cloned_guild_id",
            },
            status_code=400,
        )

    if existing and (
        old_host_id != original_guild_id or old_clone_id != cloned_guild_id
    ):
        db.clear_mapping_pair_state(old_host_id, old_clone_id)

    db.upsert_guild_mapping(
        mapping_id=mapping_id,
        mapping_name=mapping_name,
        original_guild_id=original_guild_id,
        original_guild_name="",
        original_guild_icon_url="",
        cloned_guild_id=cloned_guild_id,
        cloned_guild_name="",
        settings=settings,
    )

    return JSONResponse({"ok": True, "mapping_id": mapping_id})


@app.delete("/api/guild-mappings/{mapping_id}", response_class=JSONResponse)
async def api_delete_mapping(mapping_id: str):
    db.delete_guild_mapping(mapping_id)
    return JSONResponse({"ok": True})


@app.get("/api/client-guilds", response_class=JSONResponse)
async def api_client_guilds():
    """
    Return list of guilds for the CLIENT (selfbot) account, using CLIENT_TOKEN.

    """

    cfg = db.get_all_config()
    client_token = (cfg.get("CLIENT_TOKEN") or "").strip()

    if not client_token:

        raise HTTPException(status_code=400, detail="client-token-missing")

    url = f"{DISCORD_API_BASE}/users/@me/guilds"
    headers = {
        "Authorization": client_token,
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, timeout=15) as resp:
                text = await resp.text()
                if resp.status != 200:
                    LOGGER.warning(
                        "Discord /users/@me/guilds failed: status=%s body=%s",
                        resp.status,
                        text[:300],
                    )
                    if resp.status == 401:

                        raise HTTPException(
                            status_code=502, detail="client-token-invalid"
                        )
                    raise HTTPException(
                        status_code=502, detail="discord-guilds-fetch-failed"
                    )
                try:
                    raw = json.loads(text)
                except Exception:
                    LOGGER.exception("Failed to decode JSON from /users/@me/guilds")
                    raise HTTPException(
                        status_code=502, detail="discord-guilds-json-error"
                    )
    except HTTPException:

        raise
    except Exception:
        LOGGER.exception("Error calling Discord /users/@me/guilds")
        raise HTTPException(status_code=502, detail="discord-guilds-error")

    items: list[dict[str, str]] = []
    for g in raw or []:
        try:
            gid = str(g.get("id"))
        except Exception:
            continue
        name = str(g.get("name") or f"Guild {gid}")
        items.append(
            {
                "id": gid,
                "name": name,
            }
        )

    items.sort(key=lambda x: x["name"].lower())

    return JSONResponse({"ok": True, "items": items})


def to_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off", ""):
            return False
    return default


def _normalize_forwarding_rule_filters(raw: dict | str | None) -> dict:
    """
    Normalize filter payload so the DB and UI always see consistent shapes.

    - keywords_*: list[str]
    - channel_ids: list[str]
    - user_ids: list[str]
    - case_sensitive: bool
    - include_embeds: bool
    - include_bots: bool (default False to preserve old behaviour)
    - has_attachments: bool (default False)
    """
    if not raw:
        raw = {}
    elif isinstance(raw, str):

        try:
            raw = json.loads(raw) or {}
        except Exception:
            raw = {}

    if not isinstance(raw, dict):
        raw = {}

    def to_str_list(value) -> list[str]:
        if not value:
            return []
        if isinstance(value, str):
            return [x.strip() for x in value.split(",") if x.strip()]
        if isinstance(value, (list, tuple, set)):
            out = []
            for v in value:
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    out.append(s)
            return out
        return []

    return {
        "keywords_any": to_str_list(raw.get("keywords_any")),
        "keywords_all": to_str_list(raw.get("keywords_all")),
        "channel_ids": to_str_list(raw.get("channel_ids")),
        "user_ids": to_str_list(raw.get("user_ids")),
        "case_sensitive": to_bool(raw.get("case_sensitive"), False),
        "include_embeds": to_bool(raw.get("include_embeds"), False),
        "include_bots": to_bool(raw.get("include_bots"), False),
        "has_attachments": to_bool(
            raw.get("has_attachments", raw.get("hasMedia")), False
        ),
    }


async def _validate_forwarding_provider_config(
    provider: str, config: Any
) -> tuple[bool, list[str], dict]:
    """Validate provider credentials/config by calling the provider API.

    Returns: (ok, errors, details)
    - ok: True if valid
    - errors: list[str] user-friendly error messages
    - details: optional metadata to help the UI (never includes secrets)
    """

    provider = (provider or "").strip().lower()

    if config is None:
        config = {}
    if isinstance(config, str):
        try:
            config = json.loads(config) or {}
        except Exception:
            config = {}
    if not isinstance(config, dict):
        config = {}

    errors: list[str] = []
    details: dict = {"provider": provider}

    if provider == "telegram":
        token = str(config.get("bot_token") or "").strip()
        chat_id = str(config.get("chat_id") or "").strip()

        if not token:
            errors.append("Telegram: bot token is required.")
        if not chat_id:
            errors.append("Telegram: chat/channel id is required.")
        if errors:
            return False, errors, details

        base = f"https://api.telegram.org/bot{token}"

        try:
            async with aiohttp.ClientSession() as sess:

                async with sess.get(f"{base}/getMe", timeout=10) as resp:
                    try:
                        j = await resp.json()
                    except Exception:
                        j = None

                    ok = (
                        resp.status == 200 and isinstance(j, dict) and bool(j.get("ok"))
                    )
                    if not ok:
                        desc = (
                            (j or {}).get("description")
                            if isinstance(j, dict)
                            else None
                        )
                        errors.append(
                            "Telegram: invalid bot token."
                            + (f" ({desc})" if desc else "")
                        )
                        return False, errors, details

                    me = (j or {}).get("result") or {}
                    details["telegram_bot_username"] = me.get("username")
                    details["telegram_bot_id"] = me.get("id")

                async with sess.get(
                    f"{base}/getChat", params={"chat_id": chat_id}, timeout=10
                ) as resp:
                    try:
                        j = await resp.json()
                    except Exception:
                        j = None

                    ok = (
                        resp.status == 200 and isinstance(j, dict) and bool(j.get("ok"))
                    )
                    if not ok:
                        desc = (
                            (j or {}).get("description")
                            if isinstance(j, dict)
                            else None
                        )
                        errors.append(
                            "Telegram: invalid chat/channel id, or the bot cannot access it."
                            + (f" ({desc})" if desc else "")
                        )
                        return False, errors, details

                    chat = (j or {}).get("result") or {}
                    details["telegram_chat_title"] = chat.get("title")
                    details["telegram_chat_type"] = chat.get("type")

        except asyncio.TimeoutError:
            errors.append("Telegram: validation timed out (check network/DNS).")
            return False, errors, details
        except Exception as e:
            errors.append(f"Telegram: validation failed ({_safe(repr(e))}).")
            return False, errors, details

        return True, [], details

    if provider == "pushover":
        app_token = str(config.get("app_token") or "").strip()
        user_key = str(config.get("user_key") or "").strip()

        if not app_token:
            errors.append("Pushover: application token is required.")
        if not user_key:
            errors.append("Pushover: user/group key is required.")
        if errors:
            return False, errors, details

        url = "https://api.pushover.net/1/users/validate.json"
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    url,
                    data={"token": app_token, "user": user_key},
                    timeout=10,
                ) as resp:
                    try:
                        j = await resp.json()
                    except Exception:
                        j = None

                    ok = (
                        resp.status == 200
                        and isinstance(j, dict)
                        and int(j.get("status") or 0) == 1
                    )
                    if not ok:
                        errs = []
                        if isinstance(j, dict):
                            raw_errs = j.get("errors")
                            if isinstance(raw_errs, list):
                                errs = [str(x) for x in raw_errs if str(x).strip()]
                        msg = "Pushover: invalid token and/or user/group key."
                        if errs:
                            msg += " (" + "; ".join(errs[:5]) + ")"
                        errors.append(msg)
                        return False, errors, details

        except asyncio.TimeoutError:
            errors.append("Pushover: validation timed out (check network/DNS).")
            return False, errors, details
        except Exception as e:
            errors.append(f"Pushover: validation failed ({_safe(repr(e))}).")
            return False, errors, details

        return True, [], details

    return True, [], details


@app.post("/api/forwarding/validate", response_class=JSONResponse)
async def api_validate_forwarding(payload: dict = Body(...)):
    """Validate provider credentials/config without saving a rule."""
    provider = str(payload.get("provider") or "").strip().lower()
    config = payload.get("config")

    if provider not in ("pushover", "telegram", "discord"):
        raise HTTPException(status_code=400, detail="invalid-provider")

    ok, errors, details = await _validate_forwarding_provider_config(provider, config)
    if not ok:
        return JSONResponse(
            {"ok": False, "errors": errors, "details": details}, status_code=400
        )
    return JSONResponse({"ok": True, "details": details})


@app.get("/api/forwarding", response_class=JSONResponse)
async def api_list_forwarding(guild_id: str | None = Query(default=None)):
    """
    List message forwarding rules, optionally filtered by guild_id.
    """
    rows = db.list_message_forwarding_rules(guild_id=guild_id) or []

    for row in rows:
        row_filters = row.get("filters")
        row["filters"] = _normalize_forwarding_rule_filters(row_filters)

    return JSONResponse({"ok": True, "items": rows})


@app.post("/api/forwarding", response_class=JSONResponse)
async def api_save_forwarding(payload: dict = Body(...)):
    rule_id = str(payload.get("rule_id") or "").strip()
    raw_guild = str(payload.get("guild_id") or "").strip()
    guild_id = int(raw_guild) if raw_guild.isdigit() else None

    label = (payload.get("label") or "").strip()
    provider = (payload.get("provider") or "").strip().lower()
    enabled = to_bool(payload.get("enabled", True), True)

    config = payload.get("config") or {}
    filters = _normalize_forwarding_rule_filters(payload.get("filters"))

    if not rule_id:
        rule_id = uuid.uuid4().hex

    if not guild_id:
        raise HTTPException(status_code=400, detail="missing-guild-id")
    if provider not in ("pushover", "telegram", "discord"):
        raise HTTPException(status_code=400, detail="invalid-provider")

    if provider in ("telegram", "pushover"):
        ok, errors, _details = await _validate_forwarding_provider_config(
            provider, config
        )
        if not ok:

            return PlainTextResponse("\n".join(errors), status_code=400)

    try:
        db.upsert_message_forwarding_rule(
            rule_id=rule_id,
            guild_id=guild_id,
            label=label,
            provider=provider,
            enabled=enabled,
            config=config,
            filters=filters,
        )
    except Exception:
        LOGGER.exception("Failed to upsert message forwarding")
        raise HTTPException(status_code=500, detail="db-failure")

    msg = {
        "type": "forwarding_reload",
        "data": {},
    }
    try:
        asyncio.create_task(_ws_cmd(CLIENT_AGENT_URL, msg, timeout=1.0))
    except Exception:
        LOGGER.warning("forwarding_reload ws send failed", exc_info=True)

    return {
        "ok": True,
        "rule_id": rule_id,
        "guild_id": str(guild_id),
    }


@app.delete("/api/forwarding/{rule_id}", response_class=JSONResponse)
async def api_delete_forwarding(rule_id: str):
    """
    Delete a message forwarding rule.
    """
    existing = db.get_message_forwarding_rule(rule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="fwd-not-found")

    db.delete_message_forward_rule(rule_id)
    return JSONResponse({"ok": True, "deleted": rule_id})


@app.get("/api/forwarding/count", response_class=JSONResponse)
async def api_forwarding_count():
    """
    Returns the total number of forwarded messages recorded so far,
    plus counts grouped by provider.
    """
    try:
        total = db.count_forwarded_messages()
        by_provider = db.count_forwarded_by_provider()
        return JSONResponse({"ok": True, "count": total, "by_provider": by_provider})
    except Exception as e:
        LOGGER.exception("Failed to fetch forwarding count: %s", e)
        return JSONResponse({"ok": False, "error": "db-error"}, status_code=500)


@app.get("/api/forwarding/count/by-rule", response_class=JSONResponse)
async def api_forwarding_count_by_rule(include_null: bool = False):
    """
    Returns counts of forwarded messages grouped by rule_id.

    Query params:
    - include_null (bool): include a bucket for missing/empty rule_id under key "".
    """
    try:
        by_rule = db.count_forwarded_by_rule(include_null=include_null)
        return JSONResponse({"ok": True, "by_rule": by_rule})
    except Exception as e:
        LOGGER.exception("Failed to fetch forwarding count by rule: %s", e)
        return JSONResponse({"ok": False, "error": "db-error"}, status_code=500)


app = ConnCloseOnShutdownASGI(app)
