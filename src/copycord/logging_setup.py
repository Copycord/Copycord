# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Process-wide logging for `copycord run`.

The console shows every record tagged by role, with server and client records
only at WARNING and above. Every logger (admin app, backup scheduler, roles)
goes through this one setup; LOG_FORMAT=JSON swaps the console formatter, and
SERVER_TOKEN / CLIENT_TOKEN values are redacted everywhere. Server and client records also go to
<home>/logs/<role>.out (<DATA_DIR>/<role>.out in Docker), which the admin
log tabs read.
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional

FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"
CONSOLE_FORMAT = "%(asctime)s | %(levelname)-5s | %(role_tag)s %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"

# Below DEBUG: per-frame and per-item lines (heartbeat acks, science posts,
# repair skips) that would bury everything else at LOG_LEVEL=DEBUG. Nothing
# sets a level this low, so they stay hidden unless a logger is set to it.
TRACE = 5

# Marks console handlers so server.py and client.py do not add a second one.
CONSOLE_MARK = "_copycord_console"

FILE_ROLES = ("server", "client")
# Set when Copycord runs without a console (copycordw.exe, the Windows
# service): what the console would show goes to <logs>/copycord.out instead.
WINDOWLESS = False
CONSOLE_FILE = "copycord.out"
ROLE_CONSOLE_LEVEL = logging.WARNING
MAX_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 1


def has_console_handler(root: Optional[logging.Logger] = None) -> bool:
    root = root or logging.getLogger()
    return any(getattr(h, CONSOLE_MARK, False) for h in root.handlers)


def mark_console(handler: logging.Handler) -> logging.Handler:
    setattr(handler, CONSOLE_MARK, True)
    return handler


def _level() -> int:
    return getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)


def logs_dir() -> Path:
    from copycord.common.paths import out_dir

    return out_dir()


class ThreadRoleFilter(logging.Filter):
    """Passes only records emitted on the role's copycord-<role> thread."""

    def __init__(self, role: str):
        super().__init__()
        self.thread_name = f"copycord-{role}"

    def filter(self, record: logging.LogRecord) -> bool:
        return record.threadName == self.thread_name


def thread_role(thread_name: str) -> str:
    """The role a record belongs to; anything off the role threads is admin."""
    for role in FILE_ROLES:
        if thread_name == f"copycord-{role}":
            return role
    return "admin"


class ConsoleRoleFilter(logging.Filter):
    """Holds server and client records below ROLE_CONSOLE_LEVEL off the console."""

    def filter(self, record: logging.LogRecord) -> bool:
        if thread_role(record.threadName) in FILE_ROLES:
            return record.levelno >= ROLE_CONSOLE_LEVEL
        return True


REDACT_KEYS = {"SERVER_TOKEN", "CLIENT_TOKEN"}
REDACTED = "***REDACTED***"

# Request context the admin app sets per request or websocket.
req_id_var = contextvars.ContextVar("req_id", default="-")
route_var = contextvars.ContextVar("route", default="-")
client_var = contextvars.ContextVar("client", default="-")

# Fields callers pass through `extra=` (or get_logger) that the formatters show.
EXTRA_KEYS = (
    "conn_id", "socket_id", "channel_id", "guild_id",
    "events_sent", "forwarded", "heartbeats", "took_ms",
)


def _redact_value(val):
    try:
        s = str(val)
        for k in REDACT_KEYS:
            envv = os.getenv(k)
            if envv and envv in s:
                s = s.replace(envv, REDACTED)
        return s
    except Exception:
        return "<unprintable>"


def _redact_obj(obj):
    if isinstance(obj, dict):
        return {k: (REDACTED if str(k) in REDACT_KEYS and v else v) for k, v in obj.items()}
    return obj


def _redact_arg(a):
    if isinstance(a, dict):
        return _redact_obj(a)
    if isinstance(a, str):
        return _redact_value(a)
    return a


class RedactFilter(logging.Filter):
    """Adds the request context to the record and redacts tokens in msg and args."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.req_id = req_id_var.get()
        record.scope = route_var.get()
        record.client = client_var.get()
        try:
            if isinstance(record.args, dict):
                # A lone dict argument lands here, so its token keys count too.
                args = _redact_obj(record.args)
                record.args = {k: _redact_arg(v) for k, v in args.items()}
            elif isinstance(record.args, (tuple, list)):
                new = [_redact_arg(a) for a in record.args]
                record.args = tuple(new) if isinstance(record.args, tuple) else new
            if isinstance(record.msg, str):
                record.msg = _redact_value(record.msg)
        except Exception:
            pass
        return True


def _context(record: logging.LogRecord) -> Dict[str, object]:
    """Request context and extras that are actually set on the record."""
    out: Dict[str, object] = {}
    for k in ("req_id", "scope", "client"):
        v = getattr(record, k, "-")
        if v not in (None, "", "-"):
            out[k] = v
    for k in EXTRA_KEYS:
        v = getattr(record, k, None)
        if v not in (None, "", []):
            out[k] = v
    return out


class ConsoleFormatter(logging.Formatter):
    """Console format with the [role] tag derived from the thread name.

    Extras passed by the caller follow the message; the request context is
    added only at DEBUG, where it helps tie lines together.
    """

    def format(self, record: logging.LogRecord) -> str:
        record.role_tag = f"[{thread_role(record.threadName)}]"
        line = super().format(record)
        ctx = _context(record)
        if record.levelno > logging.DEBUG:
            for k in ("req_id", "scope", "client"):
                ctx.pop(k, None)
        if not ctx:
            return line
        head, sep, tail = line.partition("\n")
        extras = " ".join(f"{k}={v}" for k, v in ctx.items())
        return f"{head} | {extras}{sep}{tail}"


class JSONFormatter(logging.Formatter):
    """One JSON object per line (LOG_FORMAT=JSON), same local timestamp as the console."""

    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, object] = {
            "time": self.formatTime(record, DATEFMT),
            "lvl": record.levelname,
            "role": thread_role(record.threadName),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        base.update(_context(record))
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, separators=(",", ":"), default=str)


def json_mode() -> bool:
    return os.getenv("LOG_FORMAT", "HUMAN").strip().upper() == "JSON"


class ContextAdapter(logging.LoggerAdapter):
    """Adds fixed extras (socket_id, conn_id, ...) to every record."""

    def process(self, msg, kwargs):
        extra = kwargs.setdefault("extra", {})
        for k, v in self.extra.items():
            extra.setdefault(k, v)
        return msg, kwargs


def get_logger(name: str = "copycord", **ctx) -> ContextAdapter:
    return ContextAdapter(logging.getLogger(name), dict(ctx))


def _quiet_libraries(level: int) -> None:
    # websockets logs "server listening on ..." at INFO for every control port,
    # repeating the supervisor's own line; uvicorn has its own access log.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(logging.WARNING)
    # asyncio's DEBUG is "Using selector: ..." for every role loop; keep its
    # slow-callback warnings and real errors.
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(
        logging.DEBUG if level <= logging.DEBUG else logging.WARNING
    )
    # LOG_LEVEL=DEBUG shows each gateway frame sent and each science post.
    debug = level <= logging.DEBUG
    logging.getLogger("selfclient.gateway").setLevel(
        logging.DEBUG if debug else logging.WARNING
    )
    logging.getLogger("selfclient.science").setLevel(
        logging.DEBUG if debug else logging.NOTSET
    )


def apply_level(level_name: Optional[str] = None) -> int:
    """Re-level root, the console and the role file handlers; idempotent, any thread.

    Without an argument it reads LOG_LEVEL from os.environ. Returns the level.
    """
    if level_name is None:
        level = _level()
    else:
        level = getattr(logging, str(level_name).strip().upper(), logging.INFO)
        if not isinstance(level, int):
            level = logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        if getattr(h, CONSOLE_MARK, False) or getattr(h, "_copycord_role", None):
            h.setLevel(level)
    _quiet_libraries(level)
    return level


class TruncateSafeRotatingFileHandler(RotatingFileHandler):
    """Rotates on the real file size, since the admin may truncate the file in place."""

    def shouldRollover(self, record: logging.LogRecord) -> bool:
        if self.maxBytes <= 0:
            return False
        if self.stream is None:
            self.stream = self._open()
        try:
            size = os.fstat(self.stream.fileno()).st_size
        except OSError:
            return False
        return size + len(self.format(record)) + 1 >= self.maxBytes


def _console_handler(directory: Optional[Path], max_bytes: int,
                     backup_count: int) -> logging.Handler:
    if WINDOWLESS:
        path = (Path(directory) if directory is not None else logs_dir()) / CONSOLE_FILE
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            return TruncateSafeRotatingFileHandler(
                str(path), maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
            )
        except OSError:
            pass
    return logging.StreamHandler()


def setup_logging(
    directory: Optional[Path] = None,
    *,
    roles=FILE_ROLES,
    max_bytes: int = MAX_BYTES,
    backup_count: int = BACKUP_COUNT,
) -> Dict[str, logging.Handler]:
    """Install the console handler and one file handler per role; safe to call twice."""
    root = logging.getLogger()
    level = _level()
    formatter = logging.Formatter(FORMAT, datefmt=DATEFMT)
    if root.level == logging.NOTSET or root.level > level:
        root.setLevel(level)

    _quiet_libraries(level)

    if not has_console_handler(root):
        ch = mark_console(_console_handler(directory, max_bytes, backup_count))
        if json_mode():
            ch.setFormatter(JSONFormatter())
        else:
            ch.setFormatter(ConsoleFormatter(CONSOLE_FORMAT, datefmt=DATEFMT))
        ch.setLevel(level)
        ch.addFilter(ConsoleRoleFilter())
        ch.addFilter(RedactFilter())
        root.addHandler(ch)

    directory = Path(directory) if directory is not None else logs_dir()
    handlers: Dict[str, logging.Handler] = {}
    for role in roles:
        path = str((directory / f"{role}.out").resolve())
        existing = next(
            (
                h
                for h in root.handlers
                if getattr(h, "_copycord_role", None) == role
                and getattr(h, "baseFilename", None) == path
            ),
            None,
        )
        if existing is not None:
            handlers[role] = existing
            continue
        try:
            directory.mkdir(parents=True, exist_ok=True)
            fh = TruncateSafeRotatingFileHandler(
                path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
            )
        except OSError:
            logging.getLogger("copycord.supervisor").warning(
                "cannot write %s; the %s log tab will stay empty", path, role,
                exc_info=True,
            )
            continue
        fh._copycord_role = role
        fh.setFormatter(formatter)
        fh.setLevel(level)
        fh.addFilter(ThreadRoleFilter(role))
        fh.addFilter(RedactFilter())
        root.addHandler(fh)
        handlers[role] = fh
    return handlers
