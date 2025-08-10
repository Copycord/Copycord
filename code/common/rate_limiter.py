import asyncio, time
from enum import Enum
from typing import Tuple, Dict, Optional

class ActionType(Enum):
    WEBHOOK_MESSAGE = "webhook_message"
    WEBHOOK_CREATE = "webhook_create"
    CREATE_CHANNEL = "create_channel"
    EDIT_CHANNEL = "edit_channel"
    DELETE_CHANNEL = "delete_channel"
    THREAD = "thread"
    EMOJI = "emoji"
    STICKER_CREATE = "sticker_create"

class RateLimiter:
    def __init__(self, max_rate: int, time_window: float):
        self._max_rate = max_rate
        self._time_window = time_window
        self._allowance = max_rate
        self._last_check = time.monotonic()
        self._lock = asyncio.Lock()
        self._cooldown_until = 0.0  # NEW

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()

            # Respect adaptive cooldowns
            if now < self._cooldown_until:
                await asyncio.sleep(self._cooldown_until - now)
                now = time.monotonic()

            elapsed = now - self._last_check
            self._last_check = now

            # Refill tokens
            self._allowance = min(
                self._max_rate,
                self._allowance + elapsed * (self._max_rate / self._time_window),
            )

            if self._allowance < 1.0:
                # Sleep until we have 1 token
                wait = (1.0 - self._allowance) * (self._time_window / self._max_rate)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_check = time.monotonic()
                self._allowance = 0.0  # we’ll consume the token below by setting to 0
            else:
                # Consume one token
                self._allowance -= 1.0


    def backoff(self, seconds: float):
        now = time.monotonic()
        candidate_end = now + max(0.0, seconds)
        if candidate_end > self._cooldown_until:
            self._cooldown_until = candidate_end

    def reset(self):
        self._cooldown_until = 0.0

    def relax(self, factor: float = 0.5):
        """
        Reduce the remaining cooldown by 'factor' (50% by default).
        factor=0 clears it immediately, factor=1 leaves it unchanged.
        """
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
            ActionType.EDIT_CHANNEL: (3, 15.0),
            ActionType.DELETE_CHANNEL: (3, 15.0),
            ActionType.THREAD: (2, 5.0),
            ActionType.EMOJI: (2, 60.0),
            ActionType.STICKER_CREATE: (2, 60.0),
        }
        self._limiters: Dict[ActionType, RateLimiter] = {
            a: RateLimiter(*cfg[a]) for a in cfg if a is not ActionType.WEBHOOK_MESSAGE
        }
        self._webhook_config = cfg[ActionType.WEBHOOK_MESSAGE]
        self._webhook_limiters: Dict[str, RateLimiter] = {}

    def _get(self, action: ActionType, key: str | None = None) -> Optional[RateLimiter]:
        if action is ActionType.WEBHOOK_MESSAGE:
            # keyed per-webhook/channel
            if key is None:
                return None
            lim = self._webhook_limiters.get(key)
            if not lim:
                rate, window = self._webhook_config
                lim = RateLimiter(rate, window)
                self._webhook_limiters[key] = lim
            return lim

        return self._limiters.get(action)

    async def acquire(self, action: ActionType, key: str = None):
        lim = self._get(action, key)
        if lim:
            await lim.acquire()

    def penalize(self, action: ActionType, seconds: float, key: str | None = None):
        lim = self._get(action, key)
        if lim:
            lim.backoff(seconds)

    def relax(self, action: ActionType, factor: float = 0.5, key: str | None = None):
        lim = self._get(action, key)
        if lim:
            lim.relax(factor)

    def reset(self, action: ActionType, key: str | None = None):
        lim = self._get(action, key)
        if lim:
            lim.reset()

    def remaining(self, action: ActionType, key: str | None = None) -> float:
        lim = self._get(action, key)
        return lim.remaining_cooldown() if lim else 0.0
