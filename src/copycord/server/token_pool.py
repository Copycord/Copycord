# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""A warm pool of always-online gateway sessions for the sending tokens.

Each session is a selfclient ``Client`` running in this process, on the same
event loop as ``UserTokenSender``, so the token keeps one TLS identity for
REST and the gateway. Sessions connect one at a time with a jittered gap,
because parsing READY is the CPU spike.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import time
from typing import Any, Callable, Dict, List, Optional

from copycord.common.proxy_pool import get_pool as get_proxy_pool
from copycord.common.token_proxy import NoProxyAvailable
from copycord.selfclient import Client
from copycord.selfclient.errors import ConnectionClosed, HTTPException, LoginFailure

# Patched by tests; every stagger and backoff wait goes through it.
_sleep = asyncio.sleep

STATE_CONNECTING = "connecting"
STATE_ONLINE = "online"
STATE_OFFLINE = "offline"

# The setting's values and the Discord status each one sends; any other value is ``random``.
PRESENCE_STATUS = {
    "online": "online",
    "idle": "idle",
    "busy": "dnd",
    "invisible": "invisible",
}
# ``random`` picks from these, weighted like a real person's day.
RANDOM_STATUSES = ("online", "idle", "dnd", "invisible")
RANDOM_WEIGHTS = (50, 25, 10, 15)

STAGGER_MIN = 2.0
STAGGER_MAX = 6.0
# A connect not READY by then frees the gate so one stuck token holds no one up.
READY_TIMEOUT = 60.0

BACKOFF_BASE = 5.0
BACKOFF_CAP = 300.0

LAG_INTERVAL = 1.0
LAG_WARN_MS = 250.0
# The next stagger gap is multiplied by this after a lag warning.
LAG_GAP_FACTOR = 2.0


def _token_key(token_value: str) -> str:
    """A stable id for a token value that is safe to log."""
    return hashlib.sha256(token_value.encode("utf-8")).hexdigest()[:16]


def _identity_of(client: Any) -> Optional[dict]:
    """The account behind a ready session: username, global name, avatar URL (or the default one), user id."""
    user = getattr(client, "user", None)
    if user is None:
        return None
    try:
        asset = user.avatar or user.default_avatar
        avatar_url = asset.url if asset is not None else None
    except Exception:
        avatar_url = None
    user_id = getattr(user, "id", None)
    return {
        "username": getattr(user, "name", None) or None,
        "display_name": getattr(user, "global_name", None) or None,
        "avatar_url": avatar_url,
        "user_id": str(user_id) if user_id else None,
    }


def _pick_status(rng: random.Random, exclude: Optional[str] = None) -> str:
    """A weighted pick from ``RANDOM_STATUSES``, never ``exclude``."""
    pairs = [(s, w) for s, w in zip(RANDOM_STATUSES, RANDOM_WEIGHTS) if s != exclude]
    statuses, weights = zip(*pairs)
    return rng.choices(statuses, weights=weights)[0]


def _is_auth_failure(exc: BaseException) -> bool:
    """A 401 on login or a 4004 gateway close: the token is revoked."""
    if isinstance(exc, LoginFailure):
        return True
    if isinstance(exc, HTTPException) and exc.status == 401:
        return True
    if isinstance(exc, ConnectionClosed) and exc.code == 4004:
        return True
    return False


class _Session:
    """One live (or retrying) gateway session for one token value."""

    def __init__(self, key: str, token_value: str, status: str):
        self.key = key
        self.token_value = token_value
        self.status = status
        self.state = STATE_OFFLINE
        self.since = time.time()
        self.client: Any = None
        self.runner: Optional[asyncio.Task] = None
        self.task: Optional[asyncio.Task] = None
        # Set on a socket drop, cleared on READY or RESUMED.
        self.dropped = False
        self.failures = 0

    def set_state(self, state: str) -> None:
        if state != self.state:
            self.state = state
            self.since = time.time()

    def is_online(self) -> bool:
        client = self.client
        return bool(
            client is not None
            and not self.dropped
            and client.is_ready()
            and not client.is_closed()
        )

    def refresh(self) -> None:
        """Bring ``state`` in line with the client after a drop or a late READY."""
        if self.client is None:
            return
        if self.is_online():
            self.set_state(STATE_ONLINE)
        elif self.state == STATE_ONLINE:
            self.set_state(STATE_CONNECTING)


class TokenSessionPool:
    """Keeps up to ``keep_online`` sending tokens connected to the gateway.

    ``proxy_for`` is called with the token value before every connect attempt
    and returns the proxy URL to use. None means the token has no proxy, so it
    does not connect: proxies are mandatory, never a direct connection. A None
    or a raise counts as a failed attempt and is retried with backoff.
    """

    def __init__(
        self,
        *,
        logger: Any,
        proxy_for: Callable[[str], Optional[str]],
        client_factory: Callable[..., Any] = Client,
        proxy_pool: Any = None,
    ):
        self._log = logger
        self._proxy_for = proxy_for
        # The pool REST leases from: the gateway rotates in it on a dial failure,
        # so after a rotation both still share the token's one lease.
        self._proxy_pool = proxy_pool if proxy_pool is not None else get_proxy_pool()
        self._factory = client_factory
        self._keep_online = 0
        # key -> token value, in the order tokens were first ensured.
        self._tokens: Dict[str, str] = {}
        self._key_by_id: Dict[str, str] = {}
        self._presence_by_key: Dict[str, str] = {}
        self._sessions: Dict[str, _Session] = {}
        self._dead: set = set()
        # The ``random`` pick per token, kept for the life of the process.
        self._random_status: Dict[str, str] = {}
        self._last_seen: Dict[str, float] = {}
        self._gate = asyncio.Lock()
        self._lock = asyncio.Lock()
        self._rng = random.Random()
        self._connecting = 0
        self._lag_warned = False
        self._widen_next_gap = False
        self._max_lag_ms = 0.0
        self._lag_task: Optional[asyncio.Task] = None
        self._bg: set = set()

    # public API

    async def ensure(
        self, token_id: str, token_value: str, *, keep_online: int, presence: str
    ) -> None:
        """Register ``token_id`` and reconcile so at most ``keep_online`` sessions are live.

        Sessions already live keep their slot; free slots go to tokens in the
        order they were first ensured. ``keep_online <= 0`` closes them all.
        """
        token_id = str(token_id)
        token_value = (token_value or "").strip()
        async with self._lock:
            self._keep_online = max(0, int(keep_online or 0))
            if token_value:
                key = _token_key(token_value)
                old = self._key_by_id.get(token_id)
                self._key_by_id[token_id] = key
                self._tokens.setdefault(key, token_value)
                self._last_seen[key] = time.monotonic()
                if old is not None and old != key:
                    self._forget_if_unused(old)
                await self._apply_presence(key, presence)
            await self._reconcile()
            if self._keep_online > 0:
                self._start_lag_monitor()

    def online_ids(self) -> set:
        """Token ids whose session is READY with its socket up."""
        out = set()
        for token_id, key in self._key_by_id.items():
            sess = self._sessions.get(key)
            if sess is not None and sess.is_online():
                out.add(token_id)
        return out

    def dead_ids(self) -> set:
        """Token ids whose token got a 401 or a 4004 close; never retried."""
        return {tid for tid, key in self._key_by_id.items() if key in self._dead}

    def snapshot(self) -> dict:
        """Per-token session state for the settings UI; ``identity`` is set while the session is ready."""
        tokens = {}
        for token_id, key in self._key_by_id.items():
            sess = self._sessions.get(key)
            identity = None
            if sess is not None:
                sess.refresh()
                state, since, status = sess.state, sess.since, sess.status
                if sess.is_online():
                    try:
                        identity = _identity_of(sess.client)
                    except Exception:
                        identity = None
            else:
                state, since = STATE_OFFLINE, None
                status = self._status_for(key, self._presence_by_key.get(key, "random"))
            tokens[token_id] = {
                "state": state,
                "presence": status,
                "since": since,
                "dead": key in self._dead,
                "session": key,
                "identity": identity,
            }
        return {
            "keep_online": self._keep_online,
            "live": sum(1 for s in self._sessions.values() if s.is_online()),
            "max_loop_lag_ms": round(self._max_lag_ms, 1),
            "tokens": tokens,
        }

    async def acquire_online(self, candidates: List[str]) -> Optional[str]:
        """The first candidate with a ready session, or None so the caller uses REST."""
        for token_id in candidates:
            key = self._key_by_id.get(str(token_id))
            sess = self._sessions.get(key) if key else None
            if sess is not None and sess.is_online():
                self._last_seen[key] = time.monotonic()
                return token_id
        return None

    async def close(self, token_id: str) -> None:
        """Forget ``token_id``; its session closes unless another id shares the token."""
        async with self._lock:
            key = self._key_by_id.pop(str(token_id), None)
            if key is None:
                return
            self._forget_if_unused(key)
            if key not in self._tokens:
                await self._stop(key)
            await self._reconcile()

    async def close_all(self) -> None:
        """Close every session and the lag monitor; ``ensure`` starts them again."""
        async with self._lock:
            self._keep_online = 0
            for key in list(self._sessions):
                await self._stop(key)
        task, self._lag_task = self._lag_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def reap_idle(self, idle_ttl: float = 3600) -> int:
        """Drop tokens not ensured or acquired for ``idle_ttl`` seconds and close their sessions.

        A reaped token is registered again by its next ``ensure``. Returns how many were reaped.
        """
        now = time.monotonic()
        async with self._lock:
            stale = [k for k, t in list(self._last_seen.items()) if now - t >= idle_ttl]
            for key in stale:
                sess = self._sessions.get(key)
                if sess is not None and sess.state == STATE_CONNECTING:
                    continue
                await self._stop(key)
                for token_id in [t for t, k in self._key_by_id.items() if k == key]:
                    self._key_by_id.pop(token_id, None)
                self._forget_if_unused(key)
            if stale:
                await self._reconcile()
        reaped = sum(1 for k in stale if k not in self._tokens)
        if reaped:
            self._log.debug("[token-pool] Reaped %d idle token(s)", reaped)
        return reaped

    # reconcile

    def _forget_if_unused(self, key: str) -> None:
        if key in self._key_by_id.values():
            return
        self._tokens.pop(key, None)
        self._presence_by_key.pop(key, None)
        self._last_seen.pop(key, None)

    async def _reconcile(self) -> None:
        keep = self._keep_online
        wanted: List[str] = []
        if keep > 0:
            live = [k for k in self._sessions if k in self._tokens and k not in self._dead]
            rest = [k for k in self._tokens if k not in self._sessions and k not in self._dead]
            wanted = (live + rest)[:keep]
        for key in [k for k in self._sessions if k not in wanted]:
            await self._stop(key)
        for key in wanted:
            if key not in self._sessions:
                self._start(key)

    def _status_for(self, key: str, presence: str) -> str:
        presence = (presence or "").strip().lower()
        if presence in PRESENCE_STATUS:
            return PRESENCE_STATUS[presence]
        status = self._random_status.get(key)
        if status is None:
            status = _pick_status(self._rng)
            self._random_status[key] = status
        return status

    async def _apply_presence(self, key: str, presence: str) -> None:
        self._presence_by_key[key] = presence
        sess = self._sessions.get(key)
        if sess is None:
            return
        status = self._status_for(key, presence)
        if status == sess.status:
            return
        sess.status = status
        if sess.is_online():
            try:
                await sess.client.change_presence(status=status)
            except Exception as exc:
                self._log.debug("[token-pool] presence change failed for %s: %r", key, exc)

    def _start(self, key: str) -> None:
        status = self._status_for(key, self._presence_by_key.get(key, "random"))
        sess = _Session(key, self._tokens[key], status)
        self._sessions[key] = sess
        sess.task = asyncio.ensure_future(self._run(sess))

    async def _stop(self, key: str) -> None:
        sess = self._sessions.pop(key, None)
        if sess is None:
            return
        task = sess.task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._teardown(sess)
        sess.set_state(STATE_OFFLINE)
        self._log.debug("[token-pool] Session %s closed", key)

    # one session

    async def _run(self, sess: _Session) -> None:
        """Connect, wait for the session to end, back off, repeat until stopped or dead."""
        try:
            while True:
                exc: Optional[BaseException] = None
                try:
                    await self._connect_once(sess)
                    runner = sess.runner
                    if runner is not None:
                        await asyncio.wait({runner})
                        if not runner.cancelled():
                            exc = runner.exception()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    exc = e
                await self._teardown(sess)
                sess.set_state(STATE_OFFLINE)
                if exc is not None and _is_auth_failure(exc):
                    self._mark_dead(sess)
                    return
                sess.failures += 1
                delay = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (sess.failures - 1)))
                delay = min(BACKOFF_CAP, delay * self._rng.uniform(1.0, 1.25))
                self._log.warning(
                    "[token-pool] Session %s ended (%s); retrying in %.0fs",
                    sess.key,
                    type(exc).__name__ if exc is not None else "closed",
                    delay,
                )
                await _sleep(delay)
        finally:
            await self._teardown(sess)

    async def _connect_once(self, sess: _Session) -> None:
        """Start one connect under the gate, wait for READY, then hold the gate for the stagger gap."""
        async with self._gate:
            sess.set_state(STATE_CONNECTING)
            sess.dropped = False
            self._connecting += 1
            self._lag_warned = False
            cancelled = False
            try:
                try:
                    proxy = self._proxy_for(sess.token_value)
                    if not proxy:
                        # Proxies are mandatory: no lease, no connect. The session
                        # stays offline and retries with the normal backoff.
                        raise NoProxyAvailable("no proxy leased for this token")
                    client = self._factory(
                        proxy=proxy,
                        proxy_pool=self._proxy_pool,
                        max_messages=0,
                        status=sess.status,
                        refresh_build=False,
                    )
                except Exception as exc:
                    sess.runner = asyncio.ensure_future(_raise(exc))
                    return
                self._watch(sess, client)
                sess.client = client
                sess.runner = asyncio.ensure_future(self._drive(client, sess.token_value))
                ready = asyncio.ensure_future(client.wait_until_ready())
                try:
                    await asyncio.wait(
                        {sess.runner, ready},
                        timeout=READY_TIMEOUT,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    ready.cancel()
                if sess.is_online():
                    sess.failures = 0
                    sess.set_state(STATE_ONLINE)
                    self._log.info(
                        "[token-pool] Session %s online as %s", sess.key, sess.status
                    )
            except asyncio.CancelledError:
                # A stopped session frees the gate at once.
                cancelled = True
                raise
            finally:
                self._connecting -= 1
                if not cancelled:
                    await _sleep(self._next_gap())

    @staticmethod
    async def _drive(client: Any, token_value: str) -> None:
        await client.login(token_value)
        await client.connect(reconnect=True)

    def _watch(self, sess: _Session, client: Any) -> None:
        """Track socket drops, which the client's READY flag does not show."""
        add = getattr(client, "add_listener", None)
        if add is None:
            return

        async def on_disconnect(*_: Any) -> None:
            if sess.client is client:
                sess.dropped = True
                sess.refresh()

        async def on_up(*_: Any) -> None:
            if sess.client is client:
                sess.dropped = False
                sess.failures = 0
                sess.refresh()

        add(on_disconnect, "on_disconnect")
        add(on_up, "on_ready")
        add(on_up, "on_resumed")

    async def _teardown(self, sess: _Session) -> None:
        client, runner = sess.client, sess.runner
        sess.client = None
        sess.runner = None
        if client is not None:
            try:
                await client.close()
            except Exception as exc:
                self._log.debug("[token-pool] close failed for %s: %r", sess.key, exc)
        if runner is not None and not runner.done():
            runner.cancel()
        if runner is not None:
            await asyncio.gather(runner, return_exceptions=True)

    def _mark_dead(self, sess: _Session) -> None:
        self._dead.add(sess.key)
        if self._sessions.get(sess.key) is sess:
            self._sessions.pop(sess.key, None)
        ids = sorted(t for t, k in self._key_by_id.items() if k == sess.key)
        self._log.warning(
            "[token-pool] Token %s rejected (401 or 4004); not retrying",
            ", ".join(ids) or sess.key,
        )
        # Hand the freed slot to the next token.
        task = asyncio.ensure_future(self._refill())
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    async def _refill(self) -> None:
        async with self._lock:
            await self._reconcile()

    # stagger and loop lag

    def _next_gap(self) -> float:
        gap = self._rng.uniform(STAGGER_MIN, STAGGER_MAX)
        if self._widen_next_gap:
            self._widen_next_gap = False
            gap *= LAG_GAP_FACTOR
        return gap

    def _start_lag_monitor(self) -> None:
        if self._lag_task is None or self._lag_task.done():
            self._lag_task = asyncio.ensure_future(self._lag_loop())

    async def _lag_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            start = loop.time()
            await asyncio.sleep(LAG_INTERVAL)
            self._note_lag(max(0.0, loop.time() - start - LAG_INTERVAL))

    def _note_lag(self, lag: float) -> None:
        ms = lag * 1000.0
        if ms > self._max_lag_ms:
            self._max_lag_ms = ms
        if ms > LAG_WARN_MS and self._connecting > 0 and not self._lag_warned:
            self._lag_warned = True
            self._widen_next_gap = True
            self._log.warning(
                "[token-pool] Event loop lagged %.0f ms during a gateway connect; "
                "widening the next stagger gap",
                ms,
            )


async def _raise(exc: BaseException) -> None:
    raise exc


__all__ = ["TokenSessionPool", "PRESENCE_STATUS", "RANDOM_STATUSES", "RANDOM_WEIGHTS"]
