import asyncio
import time
from enum import Enum
from typing import Tuple, Dict


class ActionType(Enum):
    WEBHOOK = "webhook"
    NEW_WEBHOOK = "new_webhook"  # For creating new webhooks
    CREATE = "create_channel"
    EDIT = "edit_channel"
    DELETE = "delete_channel"
    THREAD = "thread"
    EMOJI = "emoji"


class RateLimiter:
    def __init__(self, max_rate: int, time_window: float):
        self._max_rate = max_rate
        self._time_window = time_window
        self._allowance = max_rate
        self._last_check = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_check
            self._last_check = now

            # refill tokens
            self._allowance = min(
                self._max_rate,
                self._allowance + elapsed * (self._max_rate / self._time_window),
            )

            if self._allowance < 1.0:
                wait = (1.0 - self._allowance) * (self._time_window / self._max_rate)
                await asyncio.sleep(wait)
                self._last_check = time.monotonic()
                self._allowance = 0.0
            else:
                self._allowance -= 1.0


class RateLimitManager:
    def __init__(self, config: Dict[ActionType, Tuple[int, float]] = None):
        cfg = {
            ActionType.WEBHOOK: (5, 2), # Webhook messages: 5 per 2 seconds
            ActionType.CREATE: (2, 10), # Channel creation: 2 per 10 seconds
            ActionType.NEW_WEBHOOK: (1, 15), # Webhook creation: 1 per 15 seconds
            ActionType.EDIT: (3, 15), # Channel edits: 3 per 15 seconds
            ActionType.DELETE: (3, 15), # Channel deletions: 3 per 15 seconds
            ActionType.THREAD: (2, 5.0), # Thread operations: 2 per 5 seconds
            ActionType.EMOJI: (3, 60.0), # Emoji operations: 3 per 60 seconds
        }
        # For non-webhook actions, one bucket each:
        self._limiters: Dict[ActionType, RateLimiter] = {
            action: RateLimiter(rate, window)
            for action, (rate, window) in cfg.items()
            if action is not ActionType.WEBHOOK
        }

        # For webhooks: a dict of buckets keyed by webhook URL
        self._webhook_config = cfg[ActionType.WEBHOOK]
        self._webhook_limiters: Dict[str, RateLimiter] = {}

    async def acquire(self, action: ActionType, key: str = None):
        """
        Wait for a token:
          - for WEBHOOK: key must be the webhook URL (or unique ID).
          - for others: key is ignored.
        """
        if action is ActionType.WEBHOOK:
            # Must pass the webhook URL so each webhook gets its own bucket:
            if key is None:
                raise ValueError("Must provide `key` (webhook URL) for WEBHOOK rate limiting")

            limiter = self._webhook_limiters.get(key)
            if limiter is None:
                rate, window = self._webhook_config
                limiter = RateLimiter(rate, window)
                self._webhook_limiters[key] = limiter

        else:
            limiter = self._limiters.get(action)
            if limiter is None:
                # unknown action: no rate limiting
                return

        await limiter.acquire()
