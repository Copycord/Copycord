"""One aiohttp ClientSession per event loop (each role runs its own loop).

Roles call close_shared_session() on shutdown; the next shared_session() call
after that opens a fresh one, so a restarted role works.
"""

from __future__ import annotations

import asyncio

import aiohttp

DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)

_sessions: dict[asyncio.AbstractEventLoop, aiohttp.ClientSession] = {}


def shared_session() -> aiohttp.ClientSession:
    """The running loop's session, opened on first use."""
    loop = asyncio.get_running_loop()
    sess = _sessions.get(loop)
    if sess is None or sess.closed:
        sess = aiohttp.ClientSession(timeout=DEFAULT_TIMEOUT)
        _sessions[loop] = sess
    return sess


async def close_shared_session() -> None:
    """Close the running loop's session, if one was opened."""
    sess = _sessions.pop(asyncio.get_running_loop(), None)
    if sess is not None and not sess.closed:
        await sess.close()
