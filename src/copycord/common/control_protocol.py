# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Wire protocol for the server/client control ports (9101/9102), shared by
control/control.py and supervisor.py. The admin sends {"cmd": ...} and gets
one JSON reply per message.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

# Config keys copied from DB.app_config into a role's environment on start.
ALLOWED_ENV = frozenset(
    {
        "SERVER_TOKEN",
        "CLONE_GUILD_ID",
        "COMMAND_USERS",
        "DELETE_CHANNELS",
        "DELETE_THREADS",
        "DELETE_ROLES",
        "CLONE_EMOJI",
        "CLONE_STICKER",
        "CLONE_ROLES",
        "MIRROR_ROLE_PERMISSIONS",
        "CLIENT_TOKEN",
        "HOST_GUILD_ID",
        "ENABLE_CLONING",
        "LOG_LEVEL",
    }
)


# The server's shutdown cap on closing the token pool. close_all closes
# CLOSE_CONCURRENCY sessions at once and cuts each off at CLOSE_TIMEOUT, so a
# normal close of a thousand sessions takes a few seconds; this leaves room for
# several waves of stuck ones.
TOKEN_POOL_CLOSE_CAP = 30.0
# What the server's shutdown does besides the pool close (bus, backfills, HTTP and bot close).
ROLE_STOP_MARGIN = 15.0
# How long a stop waits for the server role before it says it is over budget.
SERVER_STOP_BUDGET = TOKEN_POOL_CLOSE_CAP + ROLE_STOP_MARGIN
# The status a role reports from a stop request until its task has really ended.
STOPPING = "stopping"


def status_payload(
    running: bool,
    pid: Optional[int],
    started_wall: Optional[float],
    started_mono: Optional[float],
) -> dict:
    """Reply to {"cmd": "status"}."""
    started_at = (
        datetime.fromtimestamp(started_wall, tz=timezone.utc).isoformat()
        if running and started_wall
        else None
    )
    uptime_sec = (
        max(0.0, time.monotonic() - started_mono)
        if running and started_mono
        else None
    )
    return {
        "ok": True,
        "running": running,
        "pid": (pid if running else None),
        "started_at": started_at,
        "uptime_sec": uptime_sec,
    }


async def serve_json(ws, handle: Callable[[dict], Awaitable[Any]]) -> None:
    """Answer each JSON message on a control websocket with handle(msg)."""
    try:
        async for raw in ws:
            try:
                data = json.loads(raw) if isinstance(raw, (str, bytes)) else {}
            except Exception:
                data = {}
            resp = await handle(data)
            await ws.send(json.dumps(resp))
    except Exception as e:
        try:
            await ws.send(json.dumps({"ok": False, "error": str(e)}))
        except Exception:
            pass
