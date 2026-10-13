# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Process-wide logging for the single-process run.

One console handler on the root logger prints every record once, tagged
[admin], [server] or [client] by the thread it was emitted on. Server and
client records are shown on the console only at WARNING and above; all their
records (routed by the ThreadedRole thread) are written to <DATA_DIR>/<role>.out,
the files the admin's log tabs read. Admin/supervisor records go to the console
only.
"""

from __future__ import annotations

import logging
import os
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional

FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"
CONSOLE_FORMAT = "%(asctime)s | %(levelname)-5s | %(role_tag)s %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"

# Set on every handler that prints to the console, so server.py/client.py
# (which add their own at import time) can tell one is already installed.
CONSOLE_MARK = "_copycord_console"

FILE_ROLES = ("server", "client")
# Server and client INFO stays in their log files; the console only shows
# their records at this level and above.
ROLE_CONSOLE_LEVEL = logging.WARNING
# The v3 log files were unbounded; keep the last ~10 MB (+ one backup).
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
    """Where the admin reads <role>.out: the data dir (copycord.common.paths)."""
    from copycord.common.paths import data_dir

    return data_dir()


class ThreadRoleFilter(logging.Filter):
    """Passes only records emitted on the given role's thread (copycord-<role>)."""

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
    """
    Console routing: records from the server/client role threads pass only at
    ROLE_CONSOLE_LEVEL and above, everything else passes (the handler level
    still applies).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if thread_role(record.threadName) in FILE_ROLES:
            return record.levelno >= ROLE_CONSOLE_LEVEL
        return True


class ConsoleFormatter(logging.Formatter):
    """Console format with the [role] tag derived from the thread name."""

    def format(self, record: logging.LogRecord) -> str:
        record.role_tag = f"[{thread_role(record.threadName)}]"
        return super().format(record)


class TruncateSafeRotatingFileHandler(RotatingFileHandler):
    """
    RotatingFileHandler that survives the admin emptying the file in place
    (open(path, "w")): the stream is append-mode, so writes land at the new
    end, and the size check uses the real on-disk size, not the stale offset.
    """

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


def setup_logging(
    directory: Optional[Path] = None,
    *,
    roles=FILE_ROLES,
    max_bytes: int = MAX_BYTES,
    backup_count: int = BACKUP_COUNT,
) -> Dict[str, logging.Handler]:
    """
    Idempotent. Installs the console handler (unless one is marked already) and
    one file handler per role. Returns the file handlers keyed by role.
    """
    root = logging.getLogger()
    level = _level()
    formatter = logging.Formatter(FORMAT, datefmt=DATEFMT)
    if root.level == logging.NOTSET or root.level > level:
        root.setLevel(level)

    if not has_console_handler(root):
        ch = mark_console(logging.StreamHandler())
        ch.setFormatter(ConsoleFormatter(CONSOLE_FORMAT, datefmt=DATEFMT))
        ch.setLevel(level)
        ch.addFilter(ConsoleRoleFilter())
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
        root.addHandler(fh)
        handlers[role] = fh
    return handlers
