# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations
from collections import deque
import contextlib
import mimetypes
import hashlib
import json
import os
import socket
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
from typing import Dict, List, Set, Optional, Any, Union
from copycord.admin import spa
from copycord.admin.auth import init_admin_auth
from copycord.admin.security import authorize_websocket, safe_child
from copycord.logging_setup import (
    get_logger,
    req_id_var,
    route_var,
    client_var,
    REDACT_KEYS,
)

LOGGER = get_logger("copycord")
from copycord.admin.notifications import notify, send_webhook, EVENTS as NOTIFICATION_EVENTS
from fastapi import (
    FastAPI,
    Request,
    WebSocket,
    WebSocketDisconnect,
    Body,
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
from starlette.types import Scope, Receive, Send
from starlette.datastructures import MutableHeaders
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from copycord.common.config import CURRENT_VERSION
from copycord.common.db import DBManager, backups_dir
from copycord.common.paths import data_dir, out_candidates, out_dir
from copycord.common.http_session import close_shared_session, shared_session
from copycord.common.timezone import configured_tz_name
from copycord.common import inprocess
from copycord.common import install_identity
from copycord.common.backup_scheduler import (
    BackupConfig,
    DailySQLiteBackupScheduler,
    record_backup_stats,
    snapshot_database,
)
from copycord.common.common_helpers import (
    discord_urls_from_config,
    is_discord_webhook_url,
)
from copycord.common.selfbot_headers import (
    build_headers as _selfbot_headers,
    refresh_build_info as _refresh_build_info,
)
import copycord.admin.tasks  # noqa: F401  task types register at import
from copycord.admin import scraper_tokens
from copycord.admin.tasks import backfill as task_backfill
from copycord.admin.tasks import export_dms as task_export_dms
from copycord.admin.tasks import export_messages as task_export_messages
from copycord.admin.tasks import sync_assets as task_sync_assets
from copycord.admin.tasks import purge as task_purge
from copycord.admin.tasks import purge_assets as task_purge_assets
from copycord.admin.tasks import avatars as task_avatars
from copycord.admin.tasks import scrape as task_scrape
from copycord.admin.tasks import routes as task_routes
from copycord.admin import messages as messages_api
from copycord.admin import role_claims as role_claims_api
from copycord.admin.tasks.repo import SqliteTaskRepo
from copycord.admin.tasks.service import TaskService
from contextlib import suppress
from time import perf_counter
import sys as _sys, json as _json, contextvars
from datetime import datetime, timezone
import aiohttp

BASE_DIR = Path(__file__).resolve().parent


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
    """Keep only the first 8 chars of a token for debug logs."""
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

DEFAULT_DATA_DIR = str(data_dir())
DATA_DIR = Path(os.getenv("DATA_DIR", DEFAULT_DATA_DIR)).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = os.getenv("DB_PATH", str(DATA_DIR / "data.db"))
db = DBManager(DB_PATH, init_schema=True)

BACKUP_DIR = backups_dir()


def _log_files(name: str) -> list:
    """Where a role log may be: <home>/logs first, then the old data dir location."""
    return list(dict.fromkeys([*out_candidates(name), DATA_DIR / name]))
BACKUP_RETAIN = int(os.getenv("BACKUP_RETAIN") or "14")
BACKUP_AT = os.getenv("BACKUP_AT") or "03:17"
BACKUP_TZ = configured_tz_name()

_backup_cfg = BackupConfig(
    db_path=DB_PATH,
    backup_dir=BACKUP_DIR,
    retain=BACKUP_RETAIN,
    run_at=BACKUP_AT,
    timezone=BACKUP_TZ,
    key_path=DATA_DIR / "secret.key",
)


async def _record_backup_stats(archive_path: Path):
    try:
        record_backup_stats(db.set_config, archive_path)
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
    "COPYCORD_AUTOSTART",
    "LOG_MAX_SIZE_MB",
]

REQUIRED = ["SERVER_TOKEN", "CLIENT_TOKEN"]

BOOL_KEYS = [
    "ENABLE_CLONING",
    "CLONE_MESSAGES",
    "ON_DEMAND_WEBHOOKS",
    "DELETE_CHANNELS",
    "DELETE_THREADS",
    "DELETE_MESSAGES",
    "EDIT_MESSAGES",
    "RESEND_EDITED_MESSAGES",
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
    "CLONE_ROLE_ICONS",
    "CLONE_ROLE_STYLES",
    "CLONE_EMOJI",
    "CLONE_STICKER",
    "MIRROR_ROLE_PERMISSIONS",
    "MIRROR_CHANNEL_PERMISSIONS",
    "USE_USER_TOKENS",
    "USER_TOKEN_FALLBACK_WEBHOOK",
    "USER_TOKEN_TYPING",
    "USER_TOKEN_LINKS_ONLY",
    "USER_TOKEN_USE_PROXIES",
    "USER_TOKEN_STICKY_NICKNAME",
    "USER_TOKEN_STICKY_AVATAR",
    "USER_TOKEN_STICKY_ROLES",
    "CLONE_GUILD_ICON",
    "CLONE_GUILD_BANNER",
    "CLONE_GUILD_SPLASH",
    "CLONE_GUILD_DISCOVERY_SPLASH",
    "CLONE_SERVER_TAG",
    "SYNC_GUILD_DESCRIPTION",
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
    "CLONE_ROLE_ICONS": False,
    "CLONE_ROLE_STYLES": False,
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
    "CLONE_SERVER_TAG": False,
    "SYNC_GUILD_DESCRIPTION": False,
    "SYNC_FORUM_PROPERTIES": False,
    "CLONE_STAGE": True,
    "CLONE_STAGE_PROPERTIES": False,
    "ANONYMIZE_USERS": False,
    "DISABLE_EVERYONE_MENTIONS": False,
    "DISABLE_ROLE_MENTIONS": False,
    "TAG_REPLY_MSG": False,
    "APPEND_TIMESTAMP": False,
    "APPEND_AUTHOR": False,
    "ON_DEMAND_WEBHOOKS": True,
    "USE_USER_TOKENS": False,
    "USER_TOKEN_FALLBACK_WEBHOOK": True,
    "USER_TOKEN_TYPING": False,
    "USER_TOKEN_LINKS_ONLY": False,
    "USER_TOKEN_USE_PROXIES": False,
    "USER_TOKEN_STICKY_NICKNAME": False,
    "USER_TOKEN_STICKY_AVATAR": False,
    "USER_TOKEN_STICKY_ROLES": False,
    "COPYCORD_AUTOSTART": "false",
    "LOG_MAX_SIZE_MB": "10",
}

# Hooks run in registration order from the lifespan handler; on_event is not used.
_startup_hooks = []
_shutdown_hooks = []


def _on_startup(fn):
    _startup_hooks.append(fn)
    return fn


def _on_shutdown(fn):
    _shutdown_hooks.append(fn)
    return fn


@contextlib.asynccontextmanager
async def _lifespan(_: FastAPI):
    for hook in _startup_hooks:
        await hook()
    yield
    for hook in _shutdown_hooks:
        await hook()


app = FastAPI(title=APP_TITLE, lifespan=_lifespan)
mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


# Dashboard pages, all rendered by the SPA (router basename "/").
SPA_PAGES = ("/", "/channels", "/messages", "/guilds", "/forwarding", "/system", "/logs", "/tasks")
# First path segments that are never SPA pages: an unknown path under one is a plain 404.
NON_PAGE_SEGMENTS = frozenset(
    {"api", "ws", "bus", "logs", "static", "assets", "version", "health", "filters", "login", "logout"}
)


async def spa_page():
    return spa.index_response()


for _page in SPA_PAGES:
    app.add_api_route(_page, spa_page, methods=["GET"], include_in_schema=False)
# Deep links such as /tasks/runs/<run id> reload into the SPA.
app.add_api_route("/tasks/{rest:path}", spa_page, methods=["GET"], include_in_schema=False)


@app.get("/assets/{name:path}", include_in_schema=False)
async def spa_asset(name: str):
    return spa.asset_response(name)


def _root_redirect(request: Request, path: str) -> RedirectResponse:
    # Leading slashes are dropped so the target can never be //other-host.
    target = "/" + path.lstrip("/\\")
    if request.url.query:
        target += "?" + request.url.query
    return RedirectResponse(target, status_code=307)


def _done_response(request: Request):
    # The SPA asks for JSON and gets {"ok": true}; a plain form post still lands back on "/".
    if "application/json" in (request.headers.get("accept") or ""):
        return JSONResponse({"ok": True})
    return RedirectResponse("/", status_code=303)


@app.get("/event-logs", include_in_schema=False)
async def event_logs_redirect(request: Request):
    return _root_redirect(request, "logs")


@app.get("/app", include_in_schema=False)
@app.get("/app/{path:path}", include_in_schema=False)
async def app_redirect(request: Request, path: str = ""):
    """Redirect /app/<path> links to the same page at the root."""
    return _root_redirect(request, path)


app.include_router(task_routes.router)
app.include_router(messages_api.router)
app.include_router(role_claims_api.router)
messages_api.configure(
    db=db, send=lambda payload, timeout: _ws_cmd(SERVER_AGENT_URL, payload, timeout=timeout)
)
shutdown_event = asyncio.Event()
init_admin_auth(app, DATA_DIR)


def _is_page_path(path: str) -> bool:
    first = path.lstrip("/").split("/", 1)[0]
    return first not in NON_PAGE_SEGMENTS


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if (
        exc.status_code == 404
        and request.method in ("GET", "HEAD")
        and "text/html" in (request.headers.get("accept") or "")
        and _is_page_path(request.url.path)
    ):
        # Unknown page: the SPA shows its NotFound page.
        return spa.index_response(404)
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
    WATCHDOG_INTERVAL = 30
    WATCHDOG_TIMEOUT = 90

    def __init__(self):
        self.status = {"server": {}, "client": {}}
        self.subscribers: Set[asyncio.Queue[str]] = set()
        self.ui_sockets: Set[WebSocket] = set()
        self.lock = asyncio.Lock()
        self.recent = deque(maxlen=200)
        self._last_seen: dict[str, float] = {}
        self._watchdog_notified: dict[str, bool] = {}
        self._watchdog_task: Optional[asyncio.Task] = None

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
        if role in ("server", "client"):
            self._last_seen[role] = time.monotonic()
            self._watchdog_notified[role] = False

        if kind == "status" and role in ("server", "client"):
            prev = self.status.get(role) or {}
            self.status[role] = payload or {}
            asyncio.ensure_future(
                self._check_notifications(kind, role, payload or {}, prev)
            )
            # Sent by the role's _shutdown, whatever tore it down (stop, crash, signal).
            if (payload or {}).get("status") == "Stopped" and not payload.get("running"):
                asyncio.ensure_future(_stop_task_runs())

        elif kind in ("proxies_dead", "token_dead"):
            asyncio.ensure_future(
                self._check_notifications(kind, role, payload or {}, {})
            )

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

    async def _check_notifications(
        self, kind: str, role: str, payload: dict, prev: dict
    ):
        try:
            if kind == "status":
                was_running = _derive_state(prev) == "running" if prev else False
                now_running = _derive_state(payload) == "running"
                if was_running and not now_running:
                    event = "CLIENT_OFFLINE" if role == "client" else "SERVER_OFFLINE"
                    meta = NOTIFICATION_EVENTS.get(event, {})
                    LOGGER.info(
                        "[notifications] %s went offline, sending %s notification",
                        role,
                        event,
                    )
                    await notify(
                        db,
                        event,
                        meta.get("title", f"{role.title()} Offline"),
                        meta.get(
                            "description", f"The Copycord {role} bot has gone offline."
                        ),
                        meta.get("color", 0xFF6B6B),
                    )
            elif kind == "proxies_dead":
                meta = NOTIFICATION_EVENTS.get("PROXIES_DEAD", {})
                total = payload.get("total", "?")
                LOGGER.info(
                    "[notifications] All %s proxies dead, sending PROXIES_DEAD notification",
                    total,
                )
                await notify(
                    db,
                    "PROXIES_DEAD",
                    meta.get("title", "All Proxies Dead"),
                    f"All {total} configured proxies have failed. The client may be exposed or unable to connect.",
                    meta.get("color", 0xFF9800),
                )
            elif kind == "token_dead":
                meta = NOTIFICATION_EVENTS.get("TOKEN_INVALID", {})
                reason = payload.get("reason") or payload.get("data", {}).get(
                    "reason", "Unknown reason"
                )
                LOGGER.info(
                    "[notifications] Token invalid (%s), sending TOKEN_INVALID notification",
                    reason,
                )
                await notify(
                    db,
                    "TOKEN_INVALID",
                    meta.get("title", "Token Invalid"),
                    f"A bot token has been invalidated: {reason}",
                    meta.get("color", 0xFF6B6B),
                )
        except Exception:
            LOGGER.warning("[notifications] Notification check failed", exc_info=True)

    def start_watchdog(self):
        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = asyncio.ensure_future(self._watchdog_loop())

    async def _watchdog_loop(self):
        while True:
            await asyncio.sleep(self.WATCHDOG_INTERVAL)
            try:
                now = time.monotonic()
                for role in ("server", "client"):
                    last = self._last_seen.get(role)
                    if last is None:
                        continue
                    was_running = (
                        _derive_state(self.status.get(role) or {}) == "running"
                    )
                    timed_out = (now - last) > self.WATCHDOG_TIMEOUT
                    already_notified = self._watchdog_notified.get(role, False)

                    if was_running and timed_out and not already_notified:
                        event = (
                            "CLIENT_OFFLINE" if role == "client" else "SERVER_OFFLINE"
                        )
                        meta = NOTIFICATION_EVENTS.get(event, {})
                        LOGGER.info(
                            "[notifications] Watchdog: no heartbeat from %s for %ds, sending %s",
                            role,
                            int(now - last),
                            event,
                        )
                        self.status[role] = {"running": False, "status": "offline"}
                        self._watchdog_notified[role] = True
                        await notify(
                            db,
                            event,
                            meta.get("title", f"{role.title()} Offline"),
                            f"The Copycord {role} has not reported in for over {self.WATCHDOG_TIMEOUT}s and is presumed offline.",
                            meta.get("color", 0xFF6B6B),
                        )
                        await self._broadcast_text(
                            json.dumps(
                                {
                                    "kind": "status",
                                    "role": role,
                                    "payload": self.status[role],
                                },
                                separators=(",", ":"),
                            )
                        )
            except Exception:
                LOGGER.warning("[notifications] Watchdog loop error", exc_info=True)

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


async def _backfill_job_listener():
    """Hand client backfill_job* frames from the bus to the Backfill channels task."""
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
        if p.get("type") in task_backfill.JOB_FRAMES:
            await task_backfill.dispatch(p)
        elif p.get("type") in task_export_dms.FRAMES:
            await task_export_dms.dispatch(p)
        elif p.get("type") in task_export_messages.FRAMES:
            await task_export_messages.dispatch(p)
        elif p.get("type") in task_sync_assets.FRAMES:
            await task_sync_assets.dispatch(p)


async def _close_ws_quietly(
    ws: WebSocket, code: int = 1001, reason: str = "server shutdown"
):
    with contextlib.suppress(RuntimeError, WebSocketDisconnect, Exception):
        await ws.close(code=code, reason=reason)


DISCORD_API_BASE = "https://discord.com/api/v9"


async def _check_client_token_valid(raw_token: str) -> bool:
    """Return True if CLIENT_TOKEN is a valid user session (bot tokens are rejected)."""
    token = (raw_token or "").strip()
    if not token:
        LOGGER.debug("_check_client_token_valid | no token provided")
        return False

    url = f"{DISCORD_API_BASE}/users/@me"
    # A bare User-Agent can 401 even a valid user token, so send the sender's headers.
    headers = _selfbot_headers(token)

    try:
        async with contextlib.nullcontext(shared_session()) as sess:
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


async def _check_server_token_valid(bot_token: str) -> tuple[bool, Optional[str]]:
    """Return (valid, bot_user_id) for SERVER_TOKEN; the bot id doubles as the client id for invite URLs."""
    token = (bot_token or "").strip()
    if not token:
        LOGGER.debug("_check_server_token_valid | no token provided")
        return False, None

    url = f"{DISCORD_API_BASE}/users/@me"
    headers = {
        "Authorization": f"Bot {token}",
    }

    try:
        async with contextlib.nullcontext(shared_session()) as sess:
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

                if ok and uid:
                    try:
                        db.set_config("BOT_CLIENT_ID", str(uid))
                    except Exception:
                        pass

                return ok, uid if ok else None
    except Exception as e:
        LOGGER.warning(
            "_check_server_token_valid | exception=%s bot_token=%s",
            repr(e),
            _redact_token(token),
        )
        return False, None


async def _verify_tokens_for_save(values: dict[str, str]) -> list[str]:
    """Run both token checks and build user-facing error messages for /save."""
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
    server_ok, _bot_id = await _check_server_token_valid(raw_server)

    LOGGER.debug(
        "_verify_tokens_for_save | results client_ok=%s server_ok=%s bot_id=%s",
        client_ok,
        server_ok,
        _bot_id,
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


# The user account's guild list (/users/@me/guilds), shared by /api/client-guilds and
# _selfbot_in_guild. 60 s TTL: the list only changes when the account joins or leaves a
# server, a page refresh or modal reopen within a minute reuses it, and a membership miss
# forces one refresh so a server joined seconds ago is still found. Keyed by a hash of the
# token (never the raw token), so a changed CLIENT_TOKEN misses the cache by construction.
USER_GUILDS_TTL = 60.0
USER_GUILDS_SHORT_WAIT = 2.0
_USER_GUILDS_MAX_KEYS = 64
_user_guilds_cache: dict[str, tuple[float, list[dict]]] = {}
_user_guilds_inflight: dict[str, "asyncio.Task"] = {}
_user_guilds_blocked_until: dict[str, float] = {}

BOT_GUILD_TTL = 60.0
_bot_guild_cache: dict[tuple[str, int], float] = {}


class DiscordGuildsError(Exception):
    """A /users/@me/guilds lookup that produced no list; detail is the API error code."""

    def __init__(self, detail: str, status: int = 502, retry_after: float | None = None):
        super().__init__(detail)
        self.detail = detail
        self.status = status
        self.retry_after = retry_after


def _token_key(token: str) -> str:
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()[:24]


def _rate_limit_wait(resp, body: Any = None) -> float:
    """Seconds until Discord accepts this route again, from the 429 body or the headers."""
    waits: list[float] = []
    if isinstance(body, dict):
        try:
            waits.append(float(body.get("retry_after")))
        except Exception:
            pass
    hdrs = getattr(resp, "headers", None) or {}
    for name in ("Retry-After", "X-RateLimit-Reset-After"):
        try:
            waits.append(float(hdrs.get(name)))
        except Exception:
            pass
    waits = [w for w in waits if w >= 0]
    return max(waits) if waits else 1.0


def _user_guilds_invalidate(token: str | None = None) -> None:
    if token is None:
        _user_guilds_cache.clear()
        _user_guilds_blocked_until.clear()
        return
    key = _token_key(token)
    _user_guilds_cache.pop(key, None)
    _user_guilds_blocked_until.pop(key, None)


async def _fetch_user_guilds(token: str, key: str) -> list[dict]:
    """Page /users/@me/guilds once. On 429 serve the cached list if any, else wait once when short."""
    base_url = f"{DISCORD_API_BASE}/users/@me/guilds"
    headers = _selfbot_headers(token)
    sess = shared_session()
    guilds: list[dict] = []
    after = "0"
    waited = False
    try:
        while True:
            url = f"{base_url}?limit=200&after={after}"
            async with sess.get(url, headers=headers, timeout=15) as resp:
                status = resp.status
                if status == 429:
                    try:
                        body = await resp.json(content_type=None)
                    except Exception:
                        body = None
                    wait = _rate_limit_wait(resp, body)
                    _user_guilds_blocked_until[key] = time.monotonic() + wait
                    LOGGER.info(
                        "Discord /users/@me/guilds rate limited: retry_after=%.3fs global=%s token=%s",
                        wait,
                        bool(isinstance(body, dict) and body.get("global")),
                        _redact_token(token),
                    )
                    cached = _user_guilds_cache.get(key)
                    if cached:
                        return cached[1]
                    if waited or wait > USER_GUILDS_SHORT_WAIT:
                        raise DiscordGuildsError(
                            "discord-rate-limited", status=429, retry_after=wait
                        )
                    waited = True
                    await asyncio.sleep(wait)
                    continue

                if status != 200:
                    text = await resp.text()
                    LOGGER.warning(
                        "Discord /users/@me/guilds failed: status=%s body=%s",
                        status,
                        text[:300],
                    )
                    if status == 401:
                        raise DiscordGuildsError("client-token-invalid")
                    raise DiscordGuildsError("discord-guilds-fetch-failed")

                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    LOGGER.exception("Failed to decode JSON from /users/@me/guilds")
                    raise DiscordGuildsError("discord-guilds-json-error")

                hdrs = getattr(resp, "headers", None) or {}
                if str(hdrs.get("X-RateLimit-Remaining", "")) == "0":
                    _user_guilds_blocked_until[key] = time.monotonic() + _rate_limit_wait(resp)

            if not isinstance(data, list) or not data:
                break
            for g in data:
                if isinstance(g, dict) and g.get("id") is not None:
                    guilds.append(
                        {"id": str(g.get("id")), "name": str(g.get("name") or f"Guild {g.get('id')}")}
                    )
            if len(data) < 200:
                break
            after = str(data[-1].get("id", "0"))
    except DiscordGuildsError:
        raise
    except Exception as e:
        LOGGER.warning(
            "Error calling Discord /users/@me/guilds: %s token=%s", repr(e), _redact_token(token)
        )
        raise DiscordGuildsError("discord-guilds-error")

    if len(_user_guilds_cache) >= _USER_GUILDS_MAX_KEYS and key not in _user_guilds_cache:
        oldest = min(_user_guilds_cache, key=lambda k: _user_guilds_cache[k][0])
        _user_guilds_cache.pop(oldest, None)
    _user_guilds_cache[key] = (time.monotonic(), guilds)
    return guilds


async def _user_guilds(token: str, *, refresh: bool = False) -> list[dict]:
    """The account's guilds as [{"id", "name"}], cached per token for USER_GUILDS_TTL.

    Concurrent callers share one in-flight Discord request. While Discord's rate limit for
    the token is in force the cached list is served (even past the TTL) and Discord is not
    called; with nothing cached a DiscordGuildsError("discord-rate-limited", 429) is raised.
    refresh=True skips the TTL but still honours the rate limit and the in-flight request.
    """
    key = _token_key(token)
    now = time.monotonic()
    cached = _user_guilds_cache.get(key)
    if cached and not refresh and now - cached[0] < USER_GUILDS_TTL:
        return cached[1]

    blocked = _user_guilds_blocked_until.get(key, 0.0)
    if now < blocked:
        if cached:
            return cached[1]
        raise DiscordGuildsError(
            "discord-rate-limited", status=429, retry_after=round(blocked - now, 3)
        )

    task = _user_guilds_inflight.get(key)
    if task is None:

        async def _run() -> list[dict]:
            try:
                return await _fetch_user_guilds(token, key)
            finally:
                _user_guilds_inflight.pop(key, None)

        task = asyncio.ensure_future(_run())
        _user_guilds_inflight[key] = task
    return await asyncio.shield(task)


async def _selfbot_in_guild(client_token: str, guild_id: int) -> bool:
    """Return True if the user account is a member of guild_id, from the shared guild list."""
    if not client_token or not guild_id:
        LOGGER.debug(
            "_selfbot_in_guild | missing client_token or guild_id token=%s guild_id=%s",
            _redact_token(client_token),
            guild_id,
        )
        return False

    wanted = str(guild_id)

    async def _lookup(refresh: bool) -> bool:
        try:
            guilds = await _user_guilds(client_token, refresh=refresh)
        except DiscordGuildsError as e:
            if e.status != 429 or e.retry_after is None or e.retry_after > 10:
                raise
            await asyncio.sleep(e.retry_after)
            guilds = await _user_guilds(client_token, refresh=refresh)
        return any(g["id"] == wanted for g in guilds)

    try:
        if await _lookup(False):
            return True
        # A miss may be a server joined after the list was cached: look once more, fresh,
        # unless the list was fetched moments ago.
        cached = _user_guilds_cache.get(_token_key(client_token))
        if cached and time.monotonic() - cached[0] < 5.0:
            return False
        found = await _lookup(True)
        LOGGER.debug(
            "_selfbot_in_guild | guild_id=%s found=%s token=%s",
            guild_id,
            found,
            _redact_token(client_token),
        )
        return found
    except DiscordGuildsError as e:
        LOGGER.warning(
            "_selfbot_in_guild | %s token=%s guild_id=%s -> not_member",
            e.detail,
            _redact_token(client_token),
            guild_id,
        )
        return False


async def _selfbot_identity(token: str) -> dict | None:
    """Return {"id", "username"} for a user token, or None if invalid."""
    if not token:
        return None

    url = f"{DISCORD_API_BASE}/users/@me"

    headers = _selfbot_headers(token)
    try:
        async with contextlib.nullcontext(shared_session()) as sess:
            async with sess.get(url, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                uid = str(data.get("id", "") or "")
                if not uid:
                    return None
                username = data.get("username") or ""
                discriminator = str(data.get("discriminator") or "0")
                if discriminator and discriminator not in ("0", "0000"):
                    username = f"{username}#{discriminator}"
                return {"id": uid, "username": username}
    except Exception as e:
        LOGGER.debug(
            "_selfbot_identity | exception=%s token=%s",
            repr(e),
            _redact_token(token),
        )
        return None


async def _bot_in_guild(server_token: str, guild_id: int) -> bool:
    """Return True if the bot is in guild_id (Discord answers 403 when it is not)."""
    if not server_token or not guild_id:
        LOGGER.debug(
            "_bot_in_guild | missing server_token or guild_id bot=%s guild_id=%s",
            _redact_token(server_token),
            guild_id,
        )
        return False

    # Only a yes is cached, so a bot invited after a failed check is seen at once.
    cache_key = (_token_key(server_token), int(guild_id))
    seen_at = _bot_guild_cache.get(cache_key)
    if seen_at is not None and time.monotonic() - seen_at < BOT_GUILD_TTL:
        return True

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

                if ok:
                    if len(_bot_guild_cache) >= 256:
                        _bot_guild_cache.clear()
                    _bot_guild_cache[cache_key] = time.monotonic()
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
    if not await authorize_websocket(ws, internal=True):
        return
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


@app.websocket("/ws/ui")
async def ws_ui(ws: WebSocket):
    if not await authorize_websocket(ws, browser=True):
        return
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
    if not await authorize_websocket(websocket, browser=True, internal=True):
        return
    await websocket.accept()
    _set_ws_context("/ws/out", websocket)
    await hub.add_ui(websocket)

    socket_id = id(websocket)
    local_log = get_logger("copycord.ws.out", socket_id=socket_id)
    route_var.set("/ws/out")
    client_var.set("-")

    local_log.debug("Client connected", extra={"events_sent": 0})
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
    if not await authorize_websocket(websocket, browser=True):
        return
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
                res = await inprocess.admin_request(url, payload, timeout)
                if res is not None:
                    return res
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
    """Normalize legacy string booleans ("true", "1", "yes") to bool."""
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _bootstrap_legacy_mapping_if_needed() -> dict:
    """Migrate a legacy single-guild install: backfill guild ids into legacy rows, create the first guild_mappings row, then drop the legacy config keys.

    Returns a dict describing what was created, backfilled or skipped."""

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


@app.get("/api/config", response_class=JSONResponse)
async def api_config():
    """The saved global config values the dashboard form shows, same source as GET /."""
    env = _read_env()
    values = {k: env.get(k, "") for k in ALLOWED_ENV}
    values["LOG_LEVEL"] = (values["LOG_LEVEL"] or "INFO").upper()
    return JSONResponse(values)


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

    return _done_response(request)


async def _check_controller(
    name: str, url: str, timeout: float = 0.7
) -> tuple[bool, str, dict]:
    """Ping a controller with a status command; return (reachable, reason, raw_response)."""
    res = await _ws_cmd(url, {"cmd": "status"}, timeout=timeout)

    if not res.get("ok") or res.get("error"):
        reason = (res.get("error") or "unreachable").strip()
        return False, reason, res

    running = bool(res.get("running"))
    reason = "running" if running else "stopped"
    return True, reason, res


@app.post("/start")
async def start_all(request: Request):

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
    return _done_response(request)


@app.post("/stop")
async def stop_all(request: Request):
    LOGGER.info("POST /stop requested")
    await _stop_task_runs()
    await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "stop"})
    await _ws_cmd(SERVER_CTRL_URL, {"cmd": "stop"})

    return _done_response(request)


@_on_shutdown
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
        for p in _log_files(name):
            try:
                if p.exists():
                    with open(p, "w", encoding="utf-8"):
                        pass
                    cleared.append(str(p))
            except Exception:
                pass
    LOGGER.info("POST /logs/clear done | cleared=%s", cleared)
    return RedirectResponse("/", status_code=303)


@app.post("/logs/clear/{which}")
async def clear_logs_which(which: str):
    if which == "server":
        names = ("server.log", "server.out")
    elif which == "client":
        names = ("client.log", "client.out")
    else:
        return PlainTextResponse("invalid", status_code=400)
    cleared = []
    for name in names:
        for p in _log_files(name):
            try:
                if p.exists():
                    with open(p, "w", encoding="utf-8"):
                        pass
                    cleared.append(str(p))
            except Exception:
                pass
    LOGGER.info("POST /logs/clear/%s done | cleared=%s", which, cleared)
    return PlainTextResponse("ok")


@app.get("/logs/{which}", response_class=PlainTextResponse)
async def logs(which: str, tail: int = 20000):
    if which == "server":
        candidates = ["server.out", "server.log"]
    elif which == "client":
        candidates = ["client.out", "client.log"]
    else:
        return PlainTextResponse("invalid", status_code=400)

    for p in [q for n in candidates for q in _log_files(n)]:
        try:
            if p.exists() and p.stat().st_size > 0:
                text = p.read_text(encoding="utf-8", errors="ignore")
                if tail and tail > 0 and len(text) > tail:
                    text = text[-tail:]
                return PlainTextResponse(text)
        except Exception:
            continue

    return PlainTextResponse("No logs yet.", status_code=404)


async def _publish_task_event(event: dict) -> None:
    """task.run.updated on /ws/out; BusHub._normalize makes it
    {kind: "task.run.updated", role: "unknown", payload: {type, run, steps}}."""
    await hub.broadcast({
        "type": "task.run.updated",
        "run": event.get("run"),
        "steps": event.get("steps") or [],
    })


@_on_startup
async def _startup_tasks():
    service = TaskService(SqliteTaskRepo(db), publish=_publish_task_event)
    task_routes.set_service(service)
    task_backfill.configure(db=db, send=lambda frame: _ws_cmd(CLIENT_AGENT_URL, frame))
    task_export_dms.configure(send=lambda frame: _ws_cmd(CLIENT_AGENT_URL, frame, timeout=5.0), data_dir=DATA_DIR)
    task_export_messages.configure(send=lambda frame: _ws_cmd(CLIENT_AGENT_URL, frame, timeout=5.0),
                                   data_dir=DATA_DIR, db=db)
    task_sync_assets.configure(db=db, send=lambda frame: _ws_cmd(SERVER_AGENT_URL, frame, timeout=5.0))
    task_purge.configure(db=db)
    task_purge_assets.configure(db=db, send=lambda frame, timeout: _ws_cmd(SERVER_AGENT_URL, frame, timeout=timeout))
    task_avatars.configure(db=db, send=lambda frame: _ws_cmd(CLIENT_AGENT_URL, frame, timeout=task_avatars.SEND_TIMEOUT),
                           user_guilds=_user_guilds, data_dir=DATA_DIR, bus=hub)
    try:
        scraper_tokens.migrate_from_db(db, DATA_DIR)
    except Exception:
        LOGGER.exception("Scraper tokens: migration to the text file failed")
    task_scrape.configure(db=db, data_dir=DATA_DIR, in_guild=_selfbot_in_guild,
                          user_guilds=_user_guilds, log_path=out_dir() / "scraper.out")
    try:
        recovered = service.recover()
        if recovered:
            LOGGER.info("Tasks: %d interrupted runs marked failed", recovered)
    except Exception:
        LOGGER.exception("Tasks: recover failed")


async def _stop_task_runs() -> None:
    """The bots are going down: active task runs end cancelled ("Bots stopped") now. Their jobs
    die with the client and server, so no frame would ever end them."""
    service = task_routes.get_service()
    if service is None:
        return
    try:
        stopped = await service.stop_all()
    except Exception:
        LOGGER.exception("Tasks: cancelling runs for the bot stop failed")
        return
    if stopped:
        LOGGER.info("Tasks: bots stopped, %d run(s) cancelled", len(stopped))


@_on_shutdown
async def _shutdown_tasks():
    service = task_routes.get_service()
    if service is not None:
        await service.shutdown()


@_on_startup
async def _migrate_legacy_single_mapping():
    """Upgrade a legacy single-guild install to the multi-guild model, once at startup."""
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


@_on_startup
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


@_on_startup
async def _load_install_key():
    """Create the install key on the first start that has none (fresh or upgraded)."""
    try:
        install_identity.load_or_create(DATA_DIR)
    except Exception:
        LOGGER.exception("Install key: load failed")


@_on_startup
async def _start_bg_tasks():
    asyncio.create_task(_backfill_job_listener())


@_on_startup
async def _maybe_autostart():
    asyncio.create_task(_autostart_if_ready())


async def _autostart_if_ready():
    """Auto-start server and client if enabled, tokens are valid, and mappings exist."""
    await asyncio.sleep(1)

    try:
        autostart = db.get_config("COPYCORD_AUTOSTART", "false")
        if autostart.lower() not in ("1", "true", "yes", "on"):
            return

        env = _read_env()
        server_token = (env.get("SERVER_TOKEN") or "").strip()
        client_token = (env.get("CLIENT_TOKEN") or "").strip()
        if not server_token or not client_token:
            LOGGER.info("Auto-start skipped: missing tokens")
            return

        token_errs = await _verify_tokens_for_save(
            {"CLIENT_TOKEN": client_token, "SERVER_TOKEN": server_token}
        )
        if token_errs:
            LOGGER.info(
                "Auto-start skipped: invalid tokens (%s)", "; ".join(token_errs)
            )
            return

        srv_ok, srv_reason, _ = await _check_controller("Server", SERVER_CTRL_URL)
        cli_ok, cli_reason, _ = await _check_controller("Client", CLIENT_CTRL_URL)
        if not srv_ok or not cli_ok:
            reasons = []
            if not srv_ok:
                reasons.append(f"Server: {srv_reason}")
            if not cli_ok:
                reasons.append(f"Client: {cli_reason}")
            LOGGER.info(
                "Auto-start skipped: controllers not reachable (%s)", "; ".join(reasons)
            )
            return

        srv = await _ws_cmd(SERVER_CTRL_URL, {"cmd": "start"})
        cli = await _ws_cmd(CLIENT_CTRL_URL, {"cmd": "start"})

        LOGGER.info(
            "Auto-start complete: server=%s, client=%s",
            srv.get("status", "unknown"),
            cli.get("status", "unknown"),
        )

        for role in ("server", "client"):
            await hub.broadcast(
                {
                    "type": "status",
                    "source": role,
                    "data": {"running": True},
                }
            )
    except Exception:
        LOGGER.exception("Auto-start failed")


@_on_startup
async def _start_release_watcher():
    asyncio.create_task(_release_watch_loop())


@_on_startup
async def _start_backup_scheduler():
    backup_scheduler.start()


@_on_startup
async def _start_log_pruner():
    asyncio.create_task(_log_prune_loop())


@_on_startup
async def _start_watchdog():
    hub.start_watchdog()


async def _inprocess_bus_event(ev: dict) -> None:
    """In-process twin of the /bus websocket handler."""
    if not isinstance(ev, dict):
        ev = {"kind": "log", "role": "unknown", "payload": {"raw": _safe(ev)}}
    await hub.publish(
        ev.get("kind") or "log", ev.get("role") or "unknown", ev.get("payload") or {}
    )


@_on_startup
async def _register_inprocess_bus():
    # AdminBus publish/subscribe reach the hub directly.
    reg = inprocess.active()
    if reg is not None:
        reg.register(inprocess.BUS_PATH, _inprocess_bus_event)
        reg.register_feed(hub.add_ui, hub.remove_ui)


@_on_shutdown
async def _unregister_inprocess_bus():
    reg = inprocess.active()
    if reg is not None:
        reg.unregister(inprocess.BUS_PATH)
        reg.unregister_feed()


@_on_startup
async def _suppress_orphaned_dns_errors():

    loop = asyncio.get_running_loop()
    _default_handler = loop.get_exception_handler()

    def _handler(loop, context):
        exc = context.get("exception")
        if isinstance(exc, socket.gaierror):
            LOGGER.debug("Suppressed orphaned DNS future: %s", exc)
            return
        if _default_handler:
            _default_handler(loop, context)
        else:
            loop.default_exception_handler(context)

    loop.set_exception_handler(_handler)


async def _log_prune_loop():
    """Periodically truncate log files to the configured max, reading only the tail."""
    LOG_FILES = ("server.out", "client.out")
    LOGGER.info("Log pruning task started (checks every 5 minutes)")
    while True:
        await asyncio.sleep(300)
        try:
            max_mb_str = db.get_config("LOG_MAX_SIZE_MB", "10")
            max_bytes = int(max_mb_str) * 1024 * 1024
            if max_bytes <= 0:
                continue

            for p in [q for n in LOG_FILES for q in _log_files(n)]:
                name = str(p)
                try:
                    if not p.exists():
                        continue
                    size = p.stat().st_size
                    if size <= max_bytes:
                        continue

                    with open(p, "rb") as f:
                        f.seek(size - max_bytes)
                        tail = f.read()

                    text = tail.decode("utf-8", errors="ignore")
                    nl = text.find("\n")
                    if nl != -1:
                        text = text[nl + 1 :]

                    tmp = p.with_suffix(".tmp")
                    tmp.write_text(text, encoding="utf-8")
                    tmp.replace(p)

                    new_size = p.stat().st_size
                    LOGGER.info(
                        "Pruned %s: %.1fMB -> %.1fMB",
                        name,
                        size / (1024 * 1024),
                        new_size / (1024 * 1024),
                    )
                except Exception:
                    LOGGER.debug("Log prune failed for %s", name, exc_info=True)
        except Exception:
            LOGGER.debug("Log prune loop error", exc_info=True)


@_on_startup
async def _cleanup_stale_mapping_pairs_on_boot():
    """Purge per-(host, clone) state that no longer has a row in guild_mappings."""
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


@_on_startup
async def _refresh_discord_build_on_boot():
    """Refresh the shared user-token fingerprint to the current Discord build; falls back to the baked build number if unreachable."""
    try:
        await _refresh_build_info()
    except Exception:
        LOGGER.debug("Startup: Discord build refresh failed", exc_info=True)


@_on_shutdown
async def _stop_backup_scheduler():
    await backup_scheduler.stop()


@_on_startup
async def _open_http_session():
    shared_session()


@_on_shutdown
async def _close_http_session():
    await close_shared_session()


@app.get("/api/validate-tokens", response_class=JSONResponse)
async def api_validate_tokens():
    """Check whether the saved CLIENT_TOKEN and SERVER_TOKEN are valid."""
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

    bot_client_id = None
    try:
        bot_client_id = (db.get_config("BOT_CLIENT_ID", "") or "").strip() or None
    except Exception:
        pass

    return JSONResponse(
        {
            "ok": ok,
            "has_tokens": True,
            "errors": errs,
            "bot_client_id": bot_client_id,
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
    p = safe_child(BACKUP_DIR, name)
    if p is None:
        return PlainTextResponse("invalid name", status_code=400)
    if not p.exists() or not p.is_file():
        return PlainTextResponse("not found", status_code=404)
    return FileResponse(str(p), filename=name, media_type="application/gzip")


@app.post("/api/backup/delete")
async def backup_delete(name: str = Form(...)):
    """Permanently delete a backup archive from BACKUP_DIR."""
    p = safe_child(BACKUP_DIR, name)
    if p is None:
        return PlainTextResponse("invalid name", status_code=400)
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
    """Stop the agents and restore the DB from an uploaded .tar.gz or an archive in BACKUP_DIR."""
    if source not in ("upload", "existing"):
        return PlainTextResponse("bad source", status_code=400)

    if source == "existing":
        if not name:
            return PlainTextResponse("name required", status_code=400)
        arc = safe_child(BACKUP_DIR, name)
        if arc is None:
            return PlainTextResponse("invalid name", status_code=400)
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
        await _stop_task_runs()
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
                bak.unlink(missing_ok=True)
                snapshot_database(live, bak)

            # Copy through the open connection: with WAL a file copy over
            # data.db would leave the live -wal/-shm pointing at old pages.
            src = sqlite3.connect(str(extracted))
            try:
                with db.lock:
                    src.backup(db.conn)
            finally:
                src.close()
        except Exception as e:
            return PlainTextResponse(f"restore failed: {e}", status_code=500)

    db.set_config("DB_LAST_RESTORE_AT", datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z")
    return {"ok": True, "restored_from": arc.name}


@app.get("/api/event-log-types")
async def api_get_event_log_types():
    """Return the distinct event log types."""
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

    await hub.broadcast(
        {
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
        }
    )
    return {"ok": True, "log_id": log_id}


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
                for p in _log_files(n):
                    if p.exists():
                        return p
            return None

        HEARTBEAT_EVERY = 15.0
        # Sent once, after the backlog (or straight away when there is no log
        # file yet), so the log modal knows the tail is complete even when it is
        # empty. A client that ignores "ready" sees an empty batch.
        READY = f"data: {json.dumps({'lines': [], 'ready': True})}\n\n"
        sent_ready = False

        while not shutdown_event.is_set():
            if await request.is_disconnected():
                break

            path = pick_path()
            if not path:
                if not sent_ready:
                    sent_ready = True
                    yield READY
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
                    if not sent_ready:
                        sent_ready = True
                        yield READY

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
    """Normalize a status payload to 'running' or 'stopped', or pass it through."""
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
    """True if the status payload reports the bot ready (discord.ready/connected/online or state, gateway, discord_status)."""
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


async def _bots_running() -> bool:
    """Both bots running, as /api/status reports it; task types with requires_bots wait for it."""
    return bool((await _collect_status()).get("both_running"))


task_routes.set_bots_check(_bots_running)


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
    """Toggle a mapping between 'active' and 'paused'."""
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
    """Fetch the categories and channels of the mapping's original guild with CLIENT_TOKEN."""
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
    headers = _selfbot_headers(client_token)

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

    threads = await _fetch_active_threads(orig_id, client_token)
    channel_names = {c["id"]: c["name"] for c in channels}
    for th in threads:
        th["parent_name"] = channel_names.get(th.get("parent_id") or "")
    channels.extend(threads)

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


async def _fetch_active_threads(guild_id: int, client_token: str) -> list[dict]:
    """Currently-active threads in a guild, shaped like the channel rows.

    Best effort: a failure just means the picker offers none. Archived threads are not listed, since Discord pages them per channel."""
    url = f"{DISCORD_API_BASE}/guilds/{guild_id}/threads/active"
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                url, headers=_selfbot_headers(client_token), timeout=10
            ) as resp:
                if resp.status != 200:
                    LOGGER.debug(
                        "Active threads fetch for %s: HTTP %s", guild_id, resp.status
                    )
                    return []
                payload = json.loads(await resp.text())
    except Exception:
        LOGGER.debug("Active threads fetch failed for %s", guild_id, exc_info=True)
        return []

    out: list[dict] = []
    for th in (payload or {}).get("threads") or []:
        try:
            tid = str(th["id"])
        except Exception:
            continue
        parent = th.get("parent_id")
        out.append(
            {
                "id": tid,
                "name": str(th.get("name") or f"ID {tid}"),
                "type": int(th.get("type") or 0),
                "position": 0,
                "parent_id": str(parent) if parent else None,
                "is_thread": True,
            }
        )
    return out


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
    headers = _selfbot_headers(client_token)

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
        if k == "COPYCORD_AUTOSTART":
            v = "true" if str(v).lower() in ("1", "true", "yes", "on") else "false"
            os.environ["COPYCORD_AUTOSTART"] = v
        if k == "LOG_MAX_SIZE_MB":
            try:
                v = str(max(0, int(v)))
            except (ValueError, TypeError):
                v = "10"
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

    if errs:
        LOGGER.warning("Config validation failed | errs=%s", errs)
    else:
        LOGGER.debug("Config validation ok")

    return errs


def _norm_bool_str(v: str) -> str:
    return "True" if str(v).strip().lower() in ("true", "1", "yes", "on") else "False"


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


@app.get("/api/guilds", response_class=JSONResponse)
async def guilds_api():
    """Return the known guilds as {items: [{id, name, icon_url, member_count}]}."""
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


@app.get("/api/scraper/tokens", response_class=JSONResponse)
async def api_scraper_tokens_get():
    """Return the saved scraper tokens (data/scraper_tokens.txt)."""
    return JSONResponse({"ok": True, "tokens": scraper_tokens.read_tokens(DATA_DIR)})


@app.put("/api/scraper/tokens", response_class=JSONResponse)
async def api_scraper_tokens_put(request: Request):
    """Save the scraper tokens: {"tokens": [...]} or {"text": "one per line"}."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    if not isinstance(payload, dict):
        raise HTTPException(400, detail="Invalid JSON")
    if "tokens" in payload:
        values = payload["tokens"]
        if not isinstance(values, list):
            raise HTTPException(400, detail="tokens must be a list")
    elif isinstance(payload.get("text"), str):
        values = payload["text"].splitlines()
    else:
        raise HTTPException(400, detail="tokens or text is required")
    try:
        tokens = scraper_tokens.write_tokens(DATA_DIR, values)
    except Exception as e:
        LOGGER.exception("Failed to save scraper tokens: %s", e)
        return JSONResponse({"ok": False, "error": "server-error"}, status_code=500)
    return JSONResponse({"ok": True, "tokens": tokens})


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


def _get_favorite_proxies() -> list:
    """Starred proxies, stored as a JSON list of the raw lines the user typed."""
    raw = (db.get_config("CLIENT_FAVORITE_PROXIES", "") or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    return [x for x in data if isinstance(x, str)] if isinstance(data, list) else []


def _set_favorite_proxies(values: list) -> None:
    db.set_config("CLIENT_FAVORITE_PROXIES", json.dumps(list(values)))


def _get_favorite_recheck_interval() -> int:
    raw = (db.get_config("PROXY_FAVORITE_RECHECK_INTERVAL", "") or "").strip()
    try:
        return int(raw) if raw else 300
    except (ValueError, TypeError):
        return 300


@app.get("/api/server/proxies", response_class=JSONResponse)
async def api_server_proxies_get():
    """Return client proxy list, enabled state, and rotation interval."""
    try:
        if _PROXY_FILE.exists():
            text = _PROXY_FILE.read_text(encoding="utf-8").strip()
            lines = [l.strip() for l in text.splitlines() if l.strip()]
        else:
            lines = []
        enabled = (
            db.get_config("ENABLE_CLIENT_PROXIES", "") or ""
        ).strip().lower() in (
            "1",
            "true",
            "yes",
        )
        interval_raw = (db.get_config("PROXY_ROTATION_INTERVAL", "") or "").strip()
        try:
            rotation_interval = int(interval_raw) if interval_raw else 0
        except (ValueError, TypeError):
            rotation_interval = 0
        return JSONResponse(
            {
                "ok": True,
                "proxies": lines,
                "enabled": enabled,
                "rotation_interval": rotation_interval,
                "favorites": _get_favorite_proxies(),
                "favorite_recheck": (
                    db.get_config("PROXY_FAVORITE_RECHECK", "") or ""
                ).strip().lower() in ("1", "true", "yes"),
                "favorite_recheck_interval": _get_favorite_recheck_interval(),
            }
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/server/proxies", response_class=JSONResponse)
async def api_server_proxies_put(request: Request):
    """Save client proxy list (shared file with scraper)."""
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
        # A favorite naming a proxy that no longer exists would sit in
        # the config forever and still count as a favorite to the rotator.
        _set_favorite_proxies(
            [f for f in _get_favorite_proxies() if f in set(lines)]
        )
        return JSONResponse({"ok": True, "count": len(lines)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/server/proxies/favorites", response_class=JSONResponse)
async def api_server_proxies_favorites(request: Request):
    """Set which proxies are preferred for the client's gateway connection."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    favorites = payload.get("favorites", [])
    if not isinstance(favorites, list):
        raise HTTPException(400, detail="favorites must be a list")

    known = set()
    if _PROXY_FILE.exists():
        text = _PROXY_FILE.read_text(encoding="utf-8").strip()
        known = {ln.strip() for ln in text.splitlines() if ln.strip()}

    # Only keep favorites naming a proxy we actually have, so a stale UI
    # cannot leave the client preferring something not in the list.
    cleaned = [
        f for f in favorites if isinstance(f, str) and f.strip() and f.strip() in known
    ]
    try:
        _set_favorite_proxies(cleaned)
        return JSONResponse({"ok": True, "favorites": cleaned})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/server/proxies/favorite-recheck", response_class=JSONResponse)
async def api_server_proxies_favorite_recheck(request: Request):
    """Toggle (and time) the periodic re-check that returns to a favorite."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    enabled = bool(payload.get("enabled", False))
    try:
        db.set_config("PROXY_FAVORITE_RECHECK", "true" if enabled else "false")
        if "interval" in payload:
            interval = max(30, int(payload.get("interval") or 300))
            db.set_config("PROXY_FAVORITE_RECHECK_INTERVAL", str(interval))
    except (ValueError, TypeError):
        raise HTTPException(400, detail="interval must be an integer")
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    return JSONResponse(
        {"ok": True, "enabled": enabled, "interval": _get_favorite_recheck_interval()}
    )


@app.put("/api/server/proxies/toggle", response_class=JSONResponse)
async def api_server_proxies_toggle(request: Request):
    """Enable or disable client proxy rotation."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    enabled = bool(payload.get("enabled", False))
    try:
        db.set_config("ENABLE_CLIENT_PROXIES", "true" if enabled else "false")
        return JSONResponse({"ok": True, "enabled": enabled})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/server/proxies/rotation-interval", response_class=JSONResponse)
async def api_server_proxies_rotation_interval(request: Request):
    """Set the proxy rotation interval in seconds (0 = per-request)."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    try:
        interval = max(0, int(payload.get("interval", 0)))
    except (ValueError, TypeError):
        raise HTTPException(400, detail="interval must be an integer")
    try:
        db.set_config("PROXY_ROTATION_INTERVAL", str(interval))
        return JSONResponse({"ok": True, "rotation_interval": interval})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


_PROXY_SETTINGS_KEYS = {
    "PROXY_SUSPEND_DURATION": 300,
    "PROXY_TEST_BATCH_SIZE": 50,
    "PROXY_SLOW_THRESHOLD": 3,
}


@app.get("/api/server/proxies/settings", response_class=JSONResponse)
async def api_proxy_settings_get():
    """Return proxy settings."""
    try:
        settings = {}
        for key, default in _PROXY_SETTINGS_KEYS.items():
            raw = (db.get_config(key, "") or "").strip()
            try:
                settings[key] = int(raw) if raw else default
            except (ValueError, TypeError):
                settings[key] = default
        return JSONResponse({"ok": True, "settings": settings})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/server/proxies/settings", response_class=JSONResponse)
async def api_proxy_settings_put(request: Request):
    """Save proxy settings."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    settings = payload.get("settings", {})
    if not isinstance(settings, dict):
        raise HTTPException(400, detail="settings must be an object")
    try:
        for key, default in _PROXY_SETTINGS_KEYS.items():
            if key in settings:
                val = max(1, int(settings[key]))
                db.set_config(key, str(val))
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


_proxy_test_task: Optional[asyncio.Task] = None


@app.post("/api/server/proxies/test", response_class=JSONResponse)
async def api_server_proxies_test(request: Request):
    """Start a background proxy test whose results stream over the hub broadcast."""
    global _proxy_test_task
    import re as _re

    if _proxy_test_task and not _proxy_test_task.done():
        return JSONResponse(
            {"ok": False, "error": "Test already running"}, status_code=409
        )

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    proxies = payload.get("proxies", [])
    if not isinstance(proxies, list) or not proxies:
        raise HTTPException(400, detail="proxies must be a non-empty list")

    _HP_UP = _re.compile(
        r"^(?P<host>[^:]+):(?P<port>\d+):(?P<user>[^:]+):(?P<pass>.+)$"
    )
    _UP_HP = _re.compile(
        r"^(?P<user>[^:@]+):(?P<pass>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)$"
    )

    def _normalise(raw: str):
        raw = raw.strip()
        if not raw:
            return None
        scheme = "http"
        if "://" in raw:
            scheme, _, raw = raw.partition("://")
        m = _HP_UP.match(raw)
        if m:
            return f"{scheme}://{m.group('user')}:{m.group('pass')}@{m.group('host')}:{m.group('port')}"
        m = _UP_HP.match(raw)
        if m:
            return f"{scheme}://{raw}"
        if ":" in raw:
            return f"{scheme}://{raw}"
        return None

    def _is_socks(url: str) -> bool:
        return url.lower().startswith(
            ("socks4://", "socks5://", "socks4a://", "socks5h://")
        )

    try:
        from aiohttp_socks import ProxyConnector
    except ImportError:
        ProxyConnector = None

    test_url = "https://discord.com/api/v9/gateway"
    _batch_raw = (db.get_config("PROXY_TEST_BATCH_SIZE", "") or "").strip()
    batch_size = int(_batch_raw) if _batch_raw else 50
    _timeout = aiohttp.ClientTimeout(total=5)

    async def _test_one(raw_proxy: str):
        url = _normalise(raw_proxy)
        if not url:
            return {
                "proxy": raw_proxy,
                "ok": False,
                "error": "Invalid format",
                "ms": None,
            }
        import time as _time

        t0 = _time.monotonic()
        try:
            if _is_socks(url):
                if ProxyConnector is None:
                    return {
                        "proxy": raw_proxy,
                        "ok": False,
                        "error": "aiohttp_socks not installed",
                        "ms": None,
                    }
                connector = ProxyConnector.from_url(url)
                async with aiohttp.ClientSession(connector=connector) as sess:
                    async with sess.get(test_url, timeout=_timeout) as resp:
                        ms = round((_time.monotonic() - t0) * 1000)
                        return {
                            "proxy": raw_proxy,
                            "ok": resp.status == 200,
                            "status": resp.status,
                            "ms": ms,
                        }
            else:
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(test_url, proxy=url, timeout=_timeout) as resp:
                        ms = round((_time.monotonic() - t0) * 1000)
                        return {
                            "proxy": raw_proxy,
                            "ok": resp.status == 200,
                            "status": resp.status,
                            "ms": ms,
                        }
        except Exception as e:
            ms = round((_time.monotonic() - t0) * 1000)
            return {
                "proxy": raw_proxy,
                "ok": False,
                "error": type(e).__name__,
                "ms": ms,
            }

    async def _run_test(proxy_list: list):
        import time as _time

        total = len(proxy_list)
        all_results = []
        completed = 0
        t_start = _time.monotonic()

        LOGGER.info(
            "[🔀] Proxy test started: %d proxies (batch=%d, timeout=%ds)",
            total,
            batch_size,
            5,
        )

        await hub.broadcast(
            {
                "kind": "proxy_test",
                "role": "admin",
                "payload": {"type": "started", "total": total},
            }
        )

        try:
            for i in range(0, total, batch_size):
                batch = proxy_list[i : i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total + batch_size - 1) // batch_size
                LOGGER.info(
                    "[🔀] Proxy test batch %d/%d (%d proxies)",
                    batch_num,
                    total_batches,
                    len(batch),
                )

                batch_results = await asyncio.gather(*[_test_one(p) for p in batch])
                all_results.extend(batch_results)
                completed += len(batch_results)

                batch_passed = sum(1 for r in batch_results if r.get("ok"))
                batch_failed = len(batch_results) - batch_passed
                LOGGER.info(
                    "[🔀] Proxy test batch %d/%d done: %d passed, %d failed",
                    batch_num,
                    total_batches,
                    batch_passed,
                    batch_failed,
                )

                await hub.broadcast(
                    {
                        "kind": "proxy_test",
                        "role": "admin",
                        "payload": {
                            "type": "progress",
                            "current": completed,
                            "total": total,
                            "results": list(batch_results),
                        },
                    }
                )

            elapsed = round(_time.monotonic() - t_start, 1)
            total_passed = sum(1 for r in all_results if r.get("ok"))
            total_failed = total - total_passed
            LOGGER.info(
                "[🔀] Proxy test complete: %d/%d passed, %d failed (%.1fs)",
                total_passed,
                total,
                total_failed,
                elapsed,
            )

            await hub.broadcast(
                {
                    "kind": "proxy_test",
                    "role": "admin",
                    "payload": {
                        "type": "complete",
                        "total": total,
                        "results": all_results,
                    },
                }
            )
        except asyncio.CancelledError:
            elapsed = round(_time.monotonic() - t_start, 1)
            LOGGER.info(
                "[🔀] Proxy test stopped after %d/%d proxies (%.1fs)",
                completed,
                total,
                elapsed,
            )

    _proxy_test_task = asyncio.create_task(_run_test(proxies))
    return JSONResponse({"ok": True, "total": len(proxies)})


@app.get("/api/server/proxies/test/status", response_class=JSONResponse)
async def api_server_proxies_test_status():
    """Check if a proxy test is currently running."""
    running = _proxy_test_task is not None and not _proxy_test_task.done()
    return JSONResponse({"ok": True, "running": running})


@app.post("/api/server/proxies/test/stop", response_class=JSONResponse)
async def api_server_proxies_test_stop():
    """Stop a running proxy test."""
    global _proxy_test_task
    if _proxy_test_task and not _proxy_test_task.done():
        _proxy_test_task.cancel()
        LOGGER.info("[🔀] Proxy test stopped by user")
        await hub.broadcast(
            {
                "kind": "proxy_test",
                "role": "admin",
                "payload": {"type": "stopped"},
            }
        )
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": False, "error": "No test running"})


_SYNC_SETTINGS_KEYS = {
    "SYNC_STARTUP_DELAY": 15,
    "SYNC_INTER_GUILD_DELAY": 3,
    "SYNC_RANDOMIZE_ORDER": 1,
}


@app.get("/api/sync/settings", response_class=JSONResponse)
async def api_sync_settings_get():
    """Return sync timing settings."""
    try:
        settings = {}
        for key, default in _SYNC_SETTINGS_KEYS.items():
            raw = (db.get_config(key, "") or "").strip()
            try:
                settings[key] = int(raw) if raw else default
            except (ValueError, TypeError):
                settings[key] = default
        return JSONResponse({"ok": True, "settings": settings})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/sync/settings", response_class=JSONResponse)
async def api_sync_settings_put(request: Request):
    """Save sync timing settings."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    settings = payload.get("settings", {})
    if not isinstance(settings, dict):
        raise HTTPException(400, detail="settings must be an object")
    try:
        for key, default in _SYNC_SETTINGS_KEYS.items():
            if key in settings:
                val = max(1, int(settings[key]))
                db.set_config(key, str(val))
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/notifications/settings", response_class=JSONResponse)
async def api_notifications_settings_get():
    """Return webhook notification settings."""
    try:
        webhook_url = (db.get_config("NOTIFICATION_WEBHOOK_URL", "") or "").strip()
        events = {}
        for key, meta in NOTIFICATION_EVENTS.items():
            raw = (db.get_config(f"NOTIFY_{key}", "") or "").strip().lower()
            if raw:
                events[key] = raw not in ("0", "false", "no")
            else:
                events[key] = meta.get("default", True)
        return JSONResponse({"ok": True, "webhook_url": webhook_url, "events": events})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.put("/api/notifications/settings", response_class=JSONResponse)
async def api_notifications_settings_put(request: Request):
    """Save webhook notification settings."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid JSON")
    try:
        if "webhook_url" in payload:
            db.set_config(
                "NOTIFICATION_WEBHOOK_URL", (payload["webhook_url"] or "").strip()
            )
        events = payload.get("events", {})
        for key in NOTIFICATION_EVENTS:
            if key in events:
                db.set_config(f"NOTIFY_{key}", "true" if events[key] else "false")
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/notifications/test", response_class=JSONResponse)
async def api_notifications_test():
    """Send a test notification."""
    webhook_url = (db.get_config("NOTIFICATION_WEBHOOK_URL", "") or "").strip()
    if not webhook_url:
        return JSONResponse({"ok": False, "error": "No webhook URL configured"})
    ok = await send_webhook(
        webhook_url,
        "Test Notification",
        "If you see this, your Copycord webhook notifications are working!",
        color=0x8B5CF6,
    )
    return JSONResponse({"ok": ok, "error": None if ok else "Failed to send"})


@app.get("/api/scraper/scrapes", response_class=JSONResponse)
async def api_scraper_scrapes_list():
    """List the scrape files under data/scrapes/<guild_id>/ with their metadata, without the members."""
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

    fp = safe_child(DATA_DIR / "scrapes", filepath, max_depth=2)
    if fp is None:
        raise HTTPException(400, detail="Invalid path")
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
    fp = safe_child(DATA_DIR / "scrapes", filepath, max_depth=2)
    if fp is None:
        raise HTTPException(400, detail="Invalid path")
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


@app.get("/api/guilds/{guild_id}", response_class=JSONResponse)
async def guild_details(guild_id: str):
    """Return one guild's details: {id, name, icon_url, member_count, ...}."""
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
    """Convert free text to a Discord-safe channel name (NFKC, lowercase, dashes for whitespace, max 100)."""
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
    """Set or clear a channel's custom clone name, scoped to (original_channel_id, cloned_guild_id)."""
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
    """Set or clear a category's custom display name, scoped to (original_category_id, cloned_guild_id)."""

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

    in_host, in_clone = await asyncio.gather(
        _selfbot_in_guild(client_token, host_gid),
        _bot_in_guild(server_token, clone_gid),
    )
    if not in_host:
        return JSONResponse(
            {
                "ok": False,
                "error": "Your Discord account is not a member of the host server. Check your token and join the host server with your account before continuing.",
                "which": "original_guild_id",
            },
            status_code=400,
        )

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

    if (
        existing
        and old_host_id == original_guild_id
        and old_clone_id == cloned_guild_id
    ):
        # Same pair as stored: it was checked when saved, so a settings save skips Discord.
        in_host, in_clone = True, True
    else:
        in_host, in_clone = await asyncio.gather(
            _selfbot_in_guild(client_token, original_guild_id),
            _bot_in_guild(server_token, cloned_guild_id),
        )

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


def _mask_token(tok: str) -> str:
    """Mask a user token for display."""
    tok = str(tok or "")
    if not tok:
        return ""
    if len(tok) <= 10:
        return tok[:2] + "…"
    return f"{tok[:6]}…{tok[-4:]}"


def _serialize_mapping_token(row: dict) -> dict:
    return {
        "id": row.get("token_id"),
        "label": row.get("label") or "",
        "username": row.get("username") or "",
        "user_id": row.get("user_id") or "",
        "enabled": bool(row.get("enabled")),
        "token_masked": _mask_token(row.get("token_value") or ""),
        "use_count": row.get("use_count") or 0,
        "last_used": row.get("last_used"),
    }


@app.get("/api/guild-mappings/{mapping_id}/user-tokens", response_class=JSONResponse)
async def api_list_mapping_tokens(mapping_id: str):
    m = db.get_mapping_by_id(mapping_id)
    if not m:
        return JSONResponse(
            {"ok": False, "error": "Mapping not found."}, status_code=404
        )
    rows = db.list_mapping_tokens(mapping_id)
    return JSONResponse(
        {"ok": True, "tokens": [_serialize_mapping_token(r) for r in rows]}
    )


@app.post("/api/guild-mappings/{mapping_id}/user-tokens", response_class=JSONResponse)
async def api_add_mapping_token(mapping_id: str, payload: dict = Body(...)):
    m = db.get_mapping_by_id(mapping_id)
    if not m:
        return JSONResponse(
            {"ok": False, "error": "Mapping not found."}, status_code=404
        )

    token = (payload.get("token") or "").strip()
    label = (payload.get("label") or "").strip() or None
    if not token:
        return JSONResponse({"ok": False, "error": "Missing token."}, status_code=400)

    try:
        clone_gid = int(m.get("cloned_guild_id") or 0)
    except Exception:
        clone_gid = 0

    in_clone = await _selfbot_in_guild(token, clone_gid)
    if not in_clone:
        return JSONResponse(
            {
                "ok": False,
                "error": "That token is invalid or the account is not a member of the clone server. Join the clone server with this account first.",
            },
            status_code=400,
        )

    ident = await _selfbot_identity(token)
    username = (ident or {}).get("username") or None
    user_id = (ident or {}).get("id") or None

    try:
        token_id = db.add_mapping_token(
            mapping_id, token, label=label, username=username, user_id=user_id
        )
    except sqlite3.IntegrityError:
        return JSONResponse(
            {"ok": False, "error": "That token is already attached to this mapping."},
            status_code=400,
        )

    row = db.get_mapping_token(token_id)
    return JSONResponse(
        {"ok": True, "token": _serialize_mapping_token(row)}, status_code=200
    )


@app.post(
    "/api/guild-mappings/{mapping_id}/user-tokens/bulk",
    response_class=JSONResponse,
)
async def api_bulk_add_mapping_tokens(mapping_id: str, payload: dict = Body(...)):
    m = db.get_mapping_by_id(mapping_id)
    if not m:
        return JSONResponse(
            {"ok": False, "error": "Mapping not found."}, status_code=404
        )

    raw = payload.get("tokens")
    if isinstance(raw, str):
        candidates = raw.splitlines()
    elif isinstance(raw, list):
        candidates = [str(t) for t in raw]
    else:
        candidates = []

    seen_input: set[str] = set()
    tokens: list[str] = []
    for c in candidates:
        t = c.strip()
        if t and t not in seen_input:
            seen_input.add(t)
            tokens.append(t)

    try:
        clone_gid = int(m.get("cloned_guild_id") or 0)
    except Exception:
        clone_gid = 0

    existing = {r["token_value"] for r in db.list_mapping_tokens(mapping_id)}

    results = []
    added = 0
    # Validate sequentially to avoid hammering Discord's rate limits.
    for t in tokens:
        masked = _mask_token(t)
        if t in existing:
            results.append({"masked": masked, "status": "duplicate"})
            continue

        in_clone = await _selfbot_in_guild(t, clone_gid)
        if not in_clone:
            results.append({"masked": masked, "status": "invalid"})
            continue

        ident = await _selfbot_identity(t)
        username = (ident or {}).get("username") or None
        user_id = (ident or {}).get("id") or None
        try:
            db.add_mapping_token(mapping_id, t, username=username, user_id=user_id)
            existing.add(t)
            added += 1
            results.append({"masked": masked, "status": "added", "username": username})
        except sqlite3.IntegrityError:
            results.append({"masked": masked, "status": "duplicate"})
        except Exception:
            results.append({"masked": masked, "status": "error"})

    return JSONResponse(
        {
            "ok": True,
            "added": added,
            "total": len(tokens),
            "results": results,
        }
    )


@app.post(
    "/api/guild-mappings/{mapping_id}/user-tokens/verify",
    response_class=JSONResponse,
)
async def api_verify_mapping_tokens(mapping_id: str):
    """Check each attached token is valid and in the clone guild, returning per-token results with ids."""
    m = db.get_mapping_by_id(mapping_id)
    if not m:
        return JSONResponse(
            {"ok": False, "error": "Mapping not found."}, status_code=404
        )

    try:
        clone_gid = int(m.get("cloned_guild_id") or 0)
    except Exception:
        clone_gid = 0

    rows = db.list_mapping_tokens(mapping_id)

    results = []
    working = 0
    broken = 0
    # Check sequentially to avoid hammering Discord's rate limits.
    for r in rows:
        token_value = r.get("token_value") or ""
        ok = bool(await _selfbot_in_guild(token_value, clone_gid))
        if ok:
            working += 1
        else:
            broken += 1
        results.append(
            {
                "id": r.get("token_id"),
                "masked": _mask_token(token_value),
                "username": r.get("username") or "",
                "ok": ok,
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "total": len(rows),
            "working": working,
            "broken": broken,
            "results": results,
        }
    )


@app.post(
    "/api/guild-mappings/{mapping_id}/user-tokens/delete",
    response_class=JSONResponse,
)
async def api_bulk_delete_mapping_tokens(mapping_id: str, payload: dict = Body(...)):
    """Remove every attached token with {"scope": "all"}, or the ones listed in {"ids": [...]}."""
    m = db.get_mapping_by_id(mapping_id)
    if not m:
        return JSONResponse(
            {"ok": False, "error": "Mapping not found."}, status_code=404
        )

    scope = str(payload.get("scope") or "").strip().lower()
    if scope == "all":
        deleted = db.delete_all_mapping_tokens(mapping_id)
        return JSONResponse({"ok": True, "deleted": deleted})

    ids = payload.get("ids")
    if not isinstance(ids, list) or not ids:
        return JSONResponse(
            {"ok": False, "error": "Provide scope='all' or a non-empty ids list."},
            status_code=400,
        )

    deleted = 0
    for tid in ids:
        row = db.get_mapping_token(tid)
        if row and str(row.get("mapping_id")) == str(mapping_id):
            if db.delete_mapping_token(tid):
                deleted += 1

    return JSONResponse({"ok": True, "deleted": deleted})


@app.patch(
    "/api/guild-mappings/{mapping_id}/user-tokens/{token_id}",
    response_class=JSONResponse,
)
async def api_update_mapping_token(
    mapping_id: str, token_id: str, payload: dict = Body(...)
):
    row = db.get_mapping_token(token_id)
    if not row or str(row.get("mapping_id")) != str(mapping_id):
        return JSONResponse({"ok": False, "error": "Token not found."}, status_code=404)

    if "enabled" in payload:
        db.set_mapping_token_enabled(token_id, bool(payload.get("enabled")))

    updated = db.get_mapping_token(token_id)
    return JSONResponse({"ok": True, "token": _serialize_mapping_token(updated)})


@app.delete(
    "/api/guild-mappings/{mapping_id}/user-tokens/{token_id}",
    response_class=JSONResponse,
)
async def api_delete_mapping_token(mapping_id: str, token_id: str):
    row = db.get_mapping_token(token_id)
    if not row or str(row.get("mapping_id")) != str(mapping_id):
        return JSONResponse({"ok": False, "error": "Token not found."}, status_code=404)
    db.delete_mapping_token(token_id)
    return JSONResponse({"ok": True})


@app.get("/api/client-guilds", response_class=JSONResponse)
async def api_client_guilds():
    """Return the guilds of the client (selfbot) account behind CLIENT_TOKEN."""

    cfg = db.get_all_config()
    client_token = (cfg.get("CLIENT_TOKEN") or "").strip()

    if not client_token:

        raise HTTPException(status_code=400, detail="client-token-missing")

    try:
        guilds = await _user_guilds(client_token)
    except DiscordGuildsError as e:
        # The same {"detail"} body the HTTPException handler writes, plus Retry-After.
        headers = None
        if e.retry_after is not None:
            headers = {"Retry-After": str(max(1, int(e.retry_after + 0.999)))}
        return JSONResponse({"detail": e.detail}, status_code=e.status, headers=headers)

    items: list[dict[str, str]] = [{"id": g["id"], "name": g["name"]} for g in guilds]
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
    """Normalize a filter payload to consistent shapes for the DB and UI."""
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
        "include_system": to_bool(raw.get("include_system"), False),
        "has_attachments": to_bool(
            raw.get("has_attachments", raw.get("hasMedia")), False
        ),
    }


async def _validate_forwarding_provider_config(
    provider: str, config: Any
) -> tuple[bool, list[str], dict]:
    """Validate provider credentials by calling the provider API; returns (ok, errors, details)."""

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
    """List message forwarding rules, optionally filtered by guild_id."""
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

    # A disabled rule is stored as given, so a rule with a dead token can still
    # be switched off. Turning it back on validates again.
    if enabled and provider in ("telegram", "pushover"):
        ok, errors, _details = await _validate_forwarding_provider_config(
            provider, config
        )
        if not ok:

            return PlainTextResponse("\n".join(errors), status_code=400)

    if enabled and provider == "discord":
        raw_urls = discord_urls_from_config(config)
        if not raw_urls:
            return PlainTextResponse(
                "Discord: at least one webhook URL is required.", status_code=400
            )
        invalid = [u for u in raw_urls if not is_discord_webhook_url(u)]
        if invalid:
            return PlainTextResponse(
                "Discord: invalid webhook URL(s):\n" + "\n".join(invalid),
                status_code=400,
            )
        config = dict(config)
        config["urls"] = raw_urls
        config.pop("url", None)

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
    """Delete a message forwarding rule."""
    existing = db.get_message_forwarding_rule(rule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="fwd-not-found")

    db.delete_message_forward_rule(rule_id)
    return JSONResponse({"ok": True, "deleted": rule_id})


@app.get("/api/forwarding/count", response_class=JSONResponse)
async def api_forwarding_count():
    """Return the number of forwarded messages recorded, in total and per provider."""
    try:
        total = db.count_forwarded_messages()
        by_provider = db.count_forwarded_by_provider()
        return JSONResponse({"ok": True, "count": total, "by_provider": by_provider})
    except Exception as e:
        LOGGER.exception("Failed to fetch forwarding count: %s", e)
        return JSONResponse({"ok": False, "error": "db-error"}, status_code=500)


@app.get("/api/forwarding/count/by-rule", response_class=JSONResponse)
async def api_forwarding_count_by_rule(include_null: bool = False):
    """Return forwarded message counts per rule_id; include_null adds a "" bucket for rows without one."""
    try:
        by_rule = db.count_forwarded_by_rule(include_null=include_null)
        return JSONResponse({"ok": True, "by_rule": by_rule})
    except Exception as e:
        LOGGER.exception("Failed to fetch forwarding count by rule: %s", e)
        return JSONResponse({"ok": False, "error": "db-error"}, status_code=500)


app = ConnCloseOnShutdownASGI(app)
