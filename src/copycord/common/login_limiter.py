# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Paces login calls per exit IP so many sessions on one IP do not trip a 429.

The key is the session's proxy URL, or ``"direct"`` for sessions with no proxy;
sessions that share a key share an exit IP. Each key hands out one login slot
per interval, first come first served. A 429 doubles the key's interval and
pauses it; each success shrinks the interval back toward the floor. Keys never
wait on each other.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import Awaitable, Callable, Dict, Optional

log = logging.getLogger(__name__)

# Patched by tests; every limiter wait goes through it unless an instance has its own.
_sleep = asyncio.sleep
_clock = time.monotonic

DIRECT = "direct"

# Tuning values. A key starts at INITIAL_INTERVAL seconds between logins
# (4 per second per IP), never goes faster than FLOOR_INTERVAL and never slower
# than CAP_INTERVAL.
INITIAL_INTERVAL = 0.25
FLOOR_INTERVAL = 0.1
CAP_INTERVAL = 8.0
# Tuning value: each success multiplies the interval by this, so the cap
# (8 s) is back at the initial 0.25 s within about 33 successes.
RECOVERY_FACTOR = 0.9
# Tuning value: a 429 pauses its key at least this long, even when Discord
# sent no retry-after or 0.
MIN_PAUSE = 1.0


def key_hash(key: str) -> str:
    """A short id for a key that is safe to log; the proxy URL holds credentials."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:8]


class _Key:
    __slots__ = ("interval", "next_at", "paused_until", "lock", "loop")

    def __init__(self) -> None:
        self.interval = INITIAL_INTERVAL
        # When the next slot opens and when a 429 pause ends, on the limiter clock.
        self.next_at = 0.0
        self.paused_until = 0.0
        # asyncio.Lock wakes waiters in arrival order, so slots go out FIFO.
        self.lock: Optional[asyncio.Lock] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    def lock_for(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        # A lock binds to the loop that first waits on it; a new loop gets a new one.
        if self.lock is None or self.loop is not loop:
            self.lock = asyncio.Lock()
            self.loop = loop
        return self.lock


class LoginLimiter:
    """One login slot per key per interval; see the module docstring."""

    def __init__(
        self,
        *,
        clock: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
    ) -> None:
        self._clock = clock
        self._sleep = sleep
        self._keys: Dict[str, _Key] = {}

    def _now(self) -> float:
        return (self._clock or _clock)()

    def _state(self, key: str) -> _Key:
        state = self._keys.get(key)
        if state is None:
            state = self._keys[key] = _Key()
        return state

    def interval(self, key: str) -> float:
        """The key's current gap between logins, in seconds."""
        state = self._keys.get(key)
        return state.interval if state is not None else INITIAL_INTERVAL

    async def acquire(self, key: str) -> None:
        """Wait for the key's next slot. A waiter cancelled here takes no slot."""
        state = self._state(key)
        sleep = self._sleep or _sleep
        async with state.lock_for(asyncio.get_running_loop()):
            # Re-checked after every wait: a 429 seen meanwhile pauses this waiter too.
            while True:
                now = self._now()
                wait = max(state.next_at, state.paused_until) - now
                if wait <= 0:
                    break
                await sleep(wait)
            state.next_at = now + state.interval

    def on_rate_limited(self, key: str, retry_after: Optional[float] = None) -> None:
        """Double the key's interval (up to the cap) and pause it."""
        state = self._state(key)
        now = self._now()
        pause = max(float(retry_after or 0.0), MIN_PAUSE)
        # Logins already in flight when the first 429 came back report theirs
        # too; one pause window slows the key once, not once per session.
        if now >= state.paused_until:
            state.interval = min(CAP_INTERVAL, state.interval * 2)
        state.paused_until = max(state.paused_until, now + pause)
        log.debug(
            "Login limiter: key %s rate limited; interval %.2fs, paused %.2fs",
            key_hash(key),
            state.interval,
            pause,
        )

    def on_success(self, key: str) -> None:
        """Shrink the key's interval a step toward the floor."""
        state = self._state(key)
        state.interval = max(FLOOR_INTERVAL, state.interval * RECOVERY_FACTOR)


LIMITER = LoginLimiter()


def get_limiter() -> LoginLimiter:
    """The process-wide limiter; tests replace ``LIMITER``."""
    return LIMITER
