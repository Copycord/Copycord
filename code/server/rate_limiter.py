# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import asyncio, time
from enum import Enum
from typing import Tuple, Dict, Optional


class ActionType(Enum):
    WEBHOOK_MESSAGE = "webhook_message"
    WEBHOOK_CREATE = "webhook_create"
    WEBHOOK_DELETE = "webhook_delete"
    CREATE_CHANNEL = "create_channel"
    EDIT_CHANNEL = "edit_channel"
    DELETE_CHANNEL = "delete_channel"
    THREAD = "thread"
    EMOJI = "emoji"
    ROLE = "role"
    STICKER_CREATE = "sticker_create"


class RateLimiter:
    def __init__(self, max_rate: int, time_window: float):
        self._max_rate = max_rate
        self._time_window = time_window
        self._allowance = max_rate
        self._last_check = time.monotonic()
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()

            if now < self._cooldown_until:
                await asyncio.sleep(self._cooldown_until - now)
                now = time.monotonic()

            elapsed = now - self._last_check
            self._last_check = now

            self._allowance = min(
                self._max_rate,
                self._allowance + elapsed * (self._max_rate / self._time_window),
            )

            if self._allowance < 1.0:
                wait = (1.0 - self._allowance) * (self._time_window / self._max_rate)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_check = time.monotonic()
                self._allowance = 0.0
            else:
                self._allowance -= 1.0

    def backoff(self, seconds: float):
        now = time.monotonic()
        candidate_end = now + max(0.0, seconds)
        if candidate_end > self._cooldown_until:
            self._cooldown_until = candidate_end

    def reset(self):
        self._cooldown_until = 0.0

    def relax(self, factor: float = 0.5):
        if factor <= 0:
            self._cooldown_until = 0.0
            return
        now = time.monotonic()
        if self._cooldown_until > now:
            remaining = self._cooldown_until - now
            self._cooldown_until = now + remaining * max(0.0, min(1.0, factor))

    def remaining_cooldown(self) -> float:
        return max(0.0, self._cooldown_until - time.monotonic())


class RateLimitManager:
    def __init__(self, config: Dict[ActionType, Tuple[int, float]] = None):
        cfg = config or {
            ActionType.WEBHOOK_MESSAGE: (5, 2.5),
            ActionType.CREATE_CHANNEL: (2, 15.0),
            ActionType.WEBHOOK_CREATE: (1, 30.0),
            ActionType.WEBHOOK_DELETE: (1, 10.0),
            ActionType.EDIT_CHANNEL: (3, 15.0),
            ActionType.DELETE_CHANNEL: (3, 15.0),
            ActionType.ROLE: (1, 10.0),
            ActionType.THREAD: (2, 5.0),
            ActionType.EMOJI: (1, 60.0),
            ActionType.STICKER_CREATE: (1, 60.0),
        }
        self._cfg = cfg

        self._webhook_config = cfg[ActionType.WEBHOOK_MESSAGE]
        self._webhook_limiters: Dict[str, RateLimiter] = {}

        self._scoped_limiters: Dict[ActionType, Dict[str, RateLimiter]] = {
            a: {} for a in cfg if a is not ActionType.WEBHOOK_MESSAGE
        }

    @staticmethod
    def _scope_key(key: Optional[str]) -> str:

        return str(key) if key is not None else "GLOBAL"

    def _get(
        self, action: ActionType, key: Optional[str] = None
    ) -> Optional[RateLimiter]:
        if action is ActionType.WEBHOOK_MESSAGE:
            if key is None:
                return None
            lim = self._webhook_limiters.get(key)
            if not lim:
                rate, window = self._webhook_config
                lim = RateLimiter(rate, window)
                self._webhook_limiters[key] = lim
            return lim

        scope = self._scope_key(key)
        bucket = self._scoped_limiters.get(action)
        if bucket is None:
            return None
        lim = bucket.get(scope)
        if not lim:
            rate, window = self._cfg[action]
            lim = RateLimiter(rate, window)
            bucket[scope] = lim
        return lim

    async def acquire(self, action: ActionType, key: str | None = None):
        lim = self._get(action, key)
        if lim:
            await lim.acquire()

    async def acquire_for_guild(self, action: ActionType, clone_guild_id: int):
        await self.acquire(action, key=str(int(clone_guild_id)))

    def penalize(self, action: ActionType, seconds: float, key: str | None = None):
        lim = self._get(action, key)
        if lim:
            lim.backoff(seconds)

    def penalize_for_guild(
        self, action: ActionType, seconds: float, clone_guild_id: int
    ):
        self.penalize(action, seconds, key=str(int(clone_guild_id)))

    def relax(self, action: ActionType, factor: float = 0.5, key: str | None = None):
        lim = self._get(action, key)
        if lim:
            lim.relax(factor)

    def relax_for_guild(self, action: ActionType, factor: float, clone_guild_id: int):
        self.relax(action, factor, key=str(int(clone_guild_id)))

    def reset(self, action: ActionType, key: str | None = None):
        lim = self._get(action, key)
        if lim:
            lim.reset()

    def reset_for_guild(self, action: ActionType, clone_guild_id: int):
        self.reset(action, key=str(int(clone_guild_id)))

    def remaining(self, action: ActionType, key: str | None = None) -> float:
        lim = self._get(action, key)
        return lim.remaining_cooldown() if lim else 0.0

    def remaining_for_guild(self, action: ActionType, clone_guild_id: int) -> float:
        return self.remaining(action, key=str(int(clone_guild_id)))
