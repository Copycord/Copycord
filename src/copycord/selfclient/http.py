# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""User-token REST over curl_cffi.

User-token requests go through curl_cffi impersonating the browser the headers
describe, never aiohttp, whose TLS handshake identifies itself as Python.
Responses are returned as decoded JSON.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import random
import re
import time
from typing import Any, AsyncIterator, Callable, Dict, Optional, Tuple

from copycord.common import traffic_capture
from copycord.common.exit_timezone import note_token_proxy
from copycord.common.proxy_pool import get_pool as get_proxy_pool
from copycord.common.token_proxy import NoProxyAvailable, lease_with_timezone

from .enums import API_VERSION
from .errors import HTTPException, LoginFailure, RateLimited, for_status
from .fingerprint import (
    SUPPRESSED_PROFILE_HEADERS,
    TLS_IMPERSONATE,
    build_headers,
    channel_referer,
    context_properties,
)

try:
    from curl_cffi.requests import AsyncSession as _CurlAsyncSession
except ImportError:  # the tests drive a fake session and do not need it
    _CurlAsyncSession = None

log = logging.getLogger("selfclient.http")

API_BASE = f"https://discord.com/api/v{API_VERSION}"

MESSAGE_PAGE = 100
# The archived thread lists will not serve a limit below two.
THREAD_PAGE = 100

REQUEST_TIMEOUT = 30

MAX_RETRIES = 5
FALLBACK_RETRY_AFTER = 1.0
# A 429 with no retry_after (or 0) still waits, so retries do not fire back to back.
EMPTY_RETRY_FLOOR = 1.0
EMPTY_RETRY_CAP = 8.0

# Pause between pages, on top of the bucket pacing.
PAGE_DELAY = 0.2

ERROR_BODY_CHARS = 400
FULL_BODY_CHARS = 20_000

DISCORD_EPOCH = 1420070400000

# Response headers whose value is a live credential. Names compared lowercased.
_SECRET_HEADERS = frozenset(
    {"set-cookie", "cookie", "authorization", "proxy-authorization", "x-super-properties"}
)

# Missing Access and Missing Permissions: routine on a source guild, logged at INFO.
_ROUTINE_CODES = frozenset({50001, 50013})

# Account-level refusals, explained once per run.
_ACCOUNT_CODES = {
    40002: (
        "The account behind the token is not fully verified: Discord wants both "
        "a phone number and a confirmed email on it before it will serve this. "
        "Verify both on that account or use one that is already verified. No "
        "header, TLS fingerprint or proxy change fixes this."
    ),
}

# Browsers send Origin only on non-GET/HEAD requests.
_METHODS_WITHOUT_ORIGIN = frozenset({"GET", "HEAD"})

# Major-parameter ids keep their own bucket; other ids collapse, as Discord's do.
_MAJOR_RE = re.compile(r"(?<!channels/)(?<!guilds/)(?<!webhooks/)\b\d{15,21}\b")


def snowflake_at(milliseconds: float) -> int:
    """The smallest snowflake that could have been minted at this unix time."""
    return (int(milliseconds) - DISCORD_EPOCH) << 22


def message_nonce() -> str:
    """A snowflake-shaped message nonce, like the web client's."""
    ms = int(time.time() * 1000) - DISCORD_EPOCH
    return str((ms << 22) | random.getrandbits(22))


def _route_key(method: str, path: str) -> str:
    path = path.split("?", 1)[0]
    return f"{method} {_MAJOR_RE.sub('{id}', path)}"


def _cursor_id(value: Any) -> Optional[str]:
    """An id, an Object-like with ``.id``, or None, as the string Discord wants."""
    if value is None:
        return None
    value = getattr(value, "id", value)
    return str(value) if value != "" else None


# response reading and logging


def _header(resp: Any, name: str) -> Optional[str]:
    try:
        return resp.headers.get(name)
    except Exception:
        return None


def _decode(resp: Any) -> Any:
    """The body as JSON, or None if it was not JSON."""
    try:
        return resp.json()
    except Exception:
        return None


def _text(resp: Any) -> str:
    try:
        return resp.text or ""
    except Exception:
        return f"<{len(getattr(resp, 'content', b'') or b'')} bytes that are not text>"


def _snippet(resp: Any) -> str:
    flat = " ".join(_text(resp).split())
    return flat[:ERROR_BODY_CHARS] + ("..." if len(flat) > ERROR_BODY_CHARS else "")


def _full_body(resp: Any) -> str:
    flat = " ".join(_text(resp).split())
    if len(flat) <= FULL_BODY_CHARS:
        return flat
    return f"{flat[:FULL_BODY_CHARS]}... [truncated, {len(flat)} chars total]"


def _looks_like_html(resp: Any) -> bool:
    lowered = _snippet(resp).lower()
    return "<html" in lowered or "<!doctype" in lowered or "cloudflare" in lowered


def _safe_headers(resp: Any) -> str:
    """Every response header, with credentials redacted to their length."""
    parts = []
    try:
        items = list(resp.headers.items())
    except Exception:
        return "<headers unreadable>"
    for name, value in items:
        if str(name).lower() in _SECRET_HEADERS:
            parts.append(f"{name}: <redacted, {len(value or '')} chars>")
        else:
            parts.append(f"{name}: {value}")
    return " | ".join(parts) or "<none>"


def redact_request_headers(headers: Dict[str, Any]) -> Dict[str, Any]:
    """A copy of outgoing headers safe to log."""
    out: Dict[str, Any] = {}
    for name, value in (headers or {}).items():
        if str(name).lower() in _SECRET_HEADERS and value is not None:
            out[name] = f"<redacted, {len(str(value))} chars>"
        else:
            out[name] = value
    return out


def _retry_after(resp: Any, default: Optional[float] = FALLBACK_RETRY_AFTER) -> Optional[float]:
    """Seconds Discord asked us to wait, from the 429 body; ``default`` when it gave none."""
    body = _decode(resp)
    if isinstance(body, dict):
        try:
            return float(body.get("retry_after", default))
        except (TypeError, ValueError):
            pass
    try:
        return float(_header(resp, "Retry-After") or default)
    except (TypeError, ValueError):
        return default


def _empty_retry_wait(attempt: int) -> float:
    """The wait after a 429 that gave no wait (or 0): EMPTY_RETRY_FLOOR doubling per attempt, jittered, capped."""
    base = EMPTY_RETRY_FLOOR * (2 ** (max(1, attempt) - 1))
    return min(EMPTY_RETRY_CAP, base * random.uniform(1.0, 1.25))


# client


class HTTPClient:
    """REST on one user token, through one curl_cffi session.

    Keep ``proxy`` equal to the gateway's so REST and socket share one IP.
    """

    def __init__(
        self,
        token: Optional[str] = None,
        *,
        session: Any = None,
        proxy: Optional[str] = None,
        guild_id: Optional[int] = None,
        headers_factory: Optional[Callable[[Optional[str]], Dict[str, Any]]] = None,
        impersonate: Optional[str] = None,
        max_retries: int = MAX_RETRIES,
        page_delay: float = PAGE_DELAY,
        timeout: float = REQUEST_TIMEOUT,
        sleep: Callable[[float], Any] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.token = token
        self.proxy = proxy
        self.guild_id = str(guild_id) if guild_id else None
        self._session = session
        self._owns_session = session is None
        self._headers_factory = headers_factory
        self._impersonate = impersonate or TLS_IMPERSONATE
        self.max_retries = max(1, int(max_retries))
        self.page_delay = page_delay
        self.timeout = timeout
        self._sleep = sleep
        self._clock = clock
        # route key -> bucket hash, bucket hash -> monotonic time it reopens.
        self._route_buckets: Dict[str, str] = {}
        self._bucket_reset: Dict[str, float] = {}
        self._global_reset: float = 0.0
        self._explained: set = set()

    # lifecycle

    @property
    def session(self) -> Any:
        if self._session is None:
            if _CurlAsyncSession is None:
                raise RuntimeError("curl_cffi is not installed")
            self._session = _CurlAsyncSession(impersonate=self._impersonate)
        return self._session

    def set_proxy(self, proxy: Optional[str]) -> None:
        self.proxy = proxy or None

    def reset_diagnostics(self) -> None:
        """Let the once-per-run account diagnoses log again."""
        self._explained.clear()

    async def close(self) -> None:
        if self._session is None or not self._owns_session:
            return
        closing = self._session.close()
        if inspect.isawaitable(closing):
            await closing
        self._session = None

    # headers

    def referer(self, channel_id: Any, guild_id: Any = None) -> Optional[str]:
        """The channel page a browser would be on, or None without a guild id."""
        gid = guild_id or self.guild_id
        if not gid:
            return None
        return channel_referer(channel_id, gid)

    def headers_for(
        self,
        method: str,
        *,
        has_body: bool,
        referer: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if self._headers_factory is not None:
            headers = dict(self._headers_factory(referer))
        else:
            if not self.token:
                raise RuntimeError("HTTPClient has no token")
            headers = build_headers(self.token, referer=referer)
        if method in _METHODS_WITHOUT_ORIGIN:
            headers.pop("Origin", None)
        if not has_body:
            headers.pop("Content-Type", None)
        # None tells curl_cffi to drop these from its impersonation defaults.
        headers.update(SUPPRESSED_PROFILE_HEADERS)
        if extra:
            headers.update(extra)
        return headers

    # rate limits

    async def _wait_for_bucket(self, route: str) -> None:
        now = self._clock()
        wait = self._global_reset - now
        bucket = self._route_buckets.get(route)
        if bucket is not None:
            wait = max(wait, self._bucket_reset.get(bucket, 0.0) - now)
        if wait > 0:
            log.debug("bucket %s for %s is empty, waiting %.2fs", bucket, route, wait)
            await self._sleep(wait)

    def _record_bucket(self, route: str, resp: Any) -> None:
        """Remember when this route's bucket reopens, if it is now empty."""
        bucket = _header(resp, "X-RateLimit-Bucket") or route
        self._route_buckets[route] = bucket
        if _header(resp, "X-RateLimit-Remaining") != "0":
            self._bucket_reset.pop(bucket, None)
            return
        try:
            reset_after = float(_header(resp, "X-RateLimit-Reset-After") or 0.0)
        except (TypeError, ValueError):
            return
        if reset_after > 0:
            self._bucket_reset[bucket] = self._clock() + reset_after

    # failure logging

    def _log_failure(self, method: str, path: str, resp: Any, *, quiet: bool) -> None:
        """Log one line naming the likely cause, and the full response at debug."""
        status = getattr(resp, "status_code", 0)
        body = _decode(resp)
        code = body.get("code") if isinstance(body, dict) else None
        message = (body.get("message") if isinstance(body, dict) else None) or ""
        where = f"{method} {path}"

        if code in _ACCOUNT_CODES and code in self._explained:
            log.debug("%s HTTP %s code %s again, already reported this run", where, status, code)
        elif code in _ACCOUNT_CODES:
            self._explained.add(code)
            log.error(
                "%s Discord refused it: HTTP %s, code %s, %r. %s Repeats go to debug, "
                "full response at debug.",
                where, status, code, message, _ACCOUNT_CODES[code],
            )
        elif code in _ROUTINE_CODES:
            (log.debug if quiet else log.info)(
                "%s Discord refused it: HTTP %s, code %s, %r", where, status, code, message
            )
        elif status == 404 and quiet:
            log.debug("%s HTTP 404, code %s, %r", where, code, message)
        elif code is not None:
            log.error(
                "%s Discord refused it: HTTP %s, code %s, %r, full response at debug.",
                where, status, code, message,
            )
        elif isinstance(body, dict) and "captcha_key" in body:
            log.error(
                "%s HTTP %s carrying captcha_key: Discord wants a captcha solved and "
                "will refuse until it is, full response at debug.",
                where, status,
            )
        elif _looks_like_html(resp):
            log.error(
                "%s HTTP %s with an html body (cf-ray %s): refused before the API saw "
                "it, so no permission or account change helps, full response at debug.",
                where, status, _header(resp, "cf-ray") or "?",
            )
        else:
            log.warning("%s HTTP %s with an unrecognised body, full response at debug.", where, status)

        log.debug(
            "%s full response: HTTP %s, body: %s, headers: %s",
            where, status, _full_body(resp), _safe_headers(resp),
        )

    # requests

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Any = None,
        referer: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        quiet_refusal: bool = False,
    ) -> Any:
        """One REST call. Returns the decoded body, or None for an empty one."""
        method = method.upper()
        url = f"{API_BASE}{path}"
        # curl_cffi builds the query string itself and wants strings for it.
        query = {k: str(v) for k, v in (params or {}).items() if v is not None} or None
        has_body = json is not None
        if self.token and self._headers_factory is None:
            pool = get_proxy_pool()
            if self.proxy and pool.current(self.token) == self.proxy:
                # The pool's lease: rotate past proxies whose exit timezone lookup fails.
                self.proxy = await lease_with_timezone(pool, self.token)
            elif not await note_token_proxy(self.token, self.proxy):
                raise NoProxyAvailable("exit timezone lookup failed for the token's proxy")
        send_headers = self.headers_for(method, has_body=has_body, referer=referer, extra=headers)
        route = _route_key(method, path)

        kwargs: Dict[str, Any] = {
            "params": query,
            "headers": send_headers,
            "timeout": self.timeout,
        }
        if has_body:
            kwargs["json"] = json
        if self.proxy:
            kwargs["proxy"] = self.proxy
        # Dev mode only: None otherwise.
        capture = traffic_capture.recorder_for(self.token or send_headers.get("Authorization"))

        last_retry_after = FALLBACK_RETRY_AFTER
        last_body: Any = None
        for attempt in range(1, self.max_retries + 1):
            await self._wait_for_bucket(route)
            rid = None
            if capture is not None:
                rid = capture.request(
                    method, url, send_headers, params=query, body=json, has_body=has_body
                )
            try:
                resp = await self.session.request(method, url, **kwargs)
            except Exception as e:
                if capture is not None:
                    capture.failed(rid, e)
                log.warning("%s %s raised %r", method, path, e)
                raise HTTPException(0, None, method=method, path=path, body=repr(e)) from e

            status = int(getattr(resp, "status_code", 0) or 0)
            if capture is not None:
                capture.response(rid, status)
            self._record_bucket(route, resp)

            if status == 429:
                given = _retry_after(resp, None)
                last_retry_after = given if given and given > 0 else _empty_retry_wait(attempt)
                last_body = _decode(resp)
                scope = _header(resp, "X-RateLimit-Scope") or "?"
                is_global = bool(isinstance(last_body, dict) and last_body.get("global")) or (
                    _header(resp, "X-RateLimit-Global") == "true"
                )
                # The body's retry_after overrides the bucket headers. The wait
                # happens in _wait_for_bucket, so other callers on it wait too.
                reopen = self._clock() + last_retry_after
                if is_global:
                    self._global_reset = reopen
                else:
                    self._bucket_reset[self._route_buckets.get(route, route)] = reopen
                log.warning(
                    "%s %s rate limited (%s scope%s), %.2fs, attempt %d/%d",
                    method, path, scope, ", global" if is_global else "",
                    last_retry_after, attempt, self.max_retries,
                )
                continue

            if 200 <= status < 300:
                if status == 204:
                    return None
                payload = _decode(resp)
                if payload is None:
                    if not _text(resp).strip():
                        return None
                    log.error("%s %s HTTP %s that is not JSON: %s", method, path, status, _snippet(resp))
                    raise HTTPException(0, None, method=method, path=path, body=_snippet(resp))
                return payload

            self._log_failure(method, path, resp, quiet=quiet_refusal)
            raise for_status(status, _decode(resp), method=method, path=path, body=_snippet(resp))

        log.error("%s %s kept being rate limited, giving up", method, path)
        raise RateLimited(last_retry_after, last_body, method=method, path=path)

    async def get(self, path: str, **kw: Any) -> Any:
        return await self.request("GET", path, **kw)

    async def post(self, path: str, **kw: Any) -> Any:
        return await self.request("POST", path, **kw)

    async def patch(self, path: str, **kw: Any) -> Any:
        return await self.request("PATCH", path, **kw)

    async def put(self, path: str, **kw: Any) -> Any:
        return await self.request("PUT", path, **kw)

    async def delete(self, path: str, **kw: Any) -> Any:
        return await self.request("DELETE", path, **kw)

    # paginators

    async def messages(
        self,
        channel_id: Any,
        *,
        limit: Optional[int] = None,
        before: Any = None,
        after_id: Optional[int] = None,
        guild_id: Any = None,
        raise_on_error: bool = False,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield a channel's or thread's messages, newest first.

        ``after_id`` is a floor, not a cursor: paging stops once it is reached.
        A refusal ends the walk quietly unless ``raise_on_error``.
        """
        cursor = _cursor_id(before)
        floor = int(getattr(after_id, "id", after_id)) if after_id is not None else None
        remaining = limit
        referer = self.referer(channel_id, guild_id)

        while True:
            page_size = MESSAGE_PAGE if remaining is None else min(MESSAGE_PAGE, remaining)
            if page_size <= 0:
                return
            params: Dict[str, Any] = {"limit": page_size}
            if cursor:
                params["before"] = cursor
            try:
                page = await self.get(f"/channels/{channel_id}/messages", params=params, referer=referer)
            except HTTPException:
                if raise_on_error:
                    raise
                return
            if not isinstance(page, list) or not page:
                return

            for message in page:
                if floor is not None and int(message.get("id") or 0) <= floor:
                    return
                yield message
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            # A short page is the end; otherwise the oldest entry is the next `before`.
            if len(page) < page_size:
                return
            cursor = str(page[-1].get("id") or "")
            if not cursor:
                return
            await self._sleep(self.page_delay)

    async def messages_after(
        self,
        channel_id: Any,
        *,
        limit: Optional[int] = None,
        after_id: Any = None,
        before_id: Any = None,
        guild_id: Any = None,
        raise_on_error: bool = False,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield a channel's messages oldest first, starting just after ``after_id``.

        ``before_id`` is an exclusive ceiling.
        """
        cursor = int(getattr(after_id, "id", after_id) or 0)
        ceiling = int(getattr(before_id, "id", before_id)) if before_id is not None else None
        remaining = limit
        referer = self.referer(channel_id, guild_id)

        while True:
            page_size = MESSAGE_PAGE if remaining is None else min(MESSAGE_PAGE, remaining)
            if page_size <= 0:
                return
            params: Dict[str, Any] = {"limit": page_size, "after": str(cursor)}
            try:
                page = await self.get(f"/channels/{channel_id}/messages", params=params, referer=referer)
            except HTTPException:
                if raise_on_error:
                    raise
                return
            if not isinstance(page, list) or not page:
                return

            # The API returns the page newest first even when paging on after.
            page = sorted(page, key=lambda m: int(m.get("id") or 0))
            for message in page:
                mid = int(message.get("id") or 0)
                if ceiling is not None and mid >= ceiling:
                    return
                yield message
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            if len(page) < page_size:
                return
            cursor = int(page[-1].get("id") or 0)
            if not cursor:
                return
            await self._sleep(self.page_delay)

    async def archived_threads(
        self,
        channel_id: Any,
        *,
        private: bool = False,
        joined: bool = False,
        limit: Optional[int] = None,
        before: Any = None,
        guild_id: Any = None,
        raise_on_error: bool = False,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Yield a channel's archived threads, newest archive first.

        The private list needs MANAGE_THREADS and usually 403s, so its refusal
        logs at debug; ``joined`` lists the private threads the account is in.
        The cursor is archive_timestamp, except the joined list pages on id.
        """
        if joined:
            path = f"/channels/{channel_id}/users/@me/threads/archived/private"
        elif private:
            path = f"/channels/{channel_id}/threads/archived/private"
        else:
            path = f"/channels/{channel_id}/threads/archived/public"

        cursor: Optional[str] = None
        if before is not None:
            iso = getattr(before, "isoformat", None)
            cursor = iso() if callable(iso) and not joined else _cursor_id(before)
        remaining = limit
        referer = self.referer(channel_id, guild_id)

        while True:
            page_size = THREAD_PAGE if remaining is None else max(2, min(THREAD_PAGE, remaining))
            params: Dict[str, Any] = {"limit": page_size}
            if cursor:
                params["before"] = cursor
            try:
                payload = await self.get(
                    path, params=params, referer=referer, quiet_refusal=private or joined
                )
            except HTTPException:
                if raise_on_error:
                    raise
                return
            if not isinstance(payload, dict):
                return

            batch = payload.get("threads")
            if not isinstance(batch, list) or not batch:
                return
            for thread in batch:
                yield thread
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            # These lists honour the limit loosely, so stop on has_more, not page size.
            if not payload.get("has_more"):
                return

            last = batch[-1]
            if joined:
                cursor = str(last.get("id") or "")
            else:
                cursor = str((last.get("thread_metadata") or {}).get("archive_timestamp") or "")
            if not cursor:
                log.debug("%s gave no cursor to continue from, stopping", path)
                return
            await self._sleep(self.page_delay)

    # raw-dict endpoints the client role uses

    async def login(self) -> Dict[str, Any]:
        """GET /users/@me, turning a 401 into LoginFailure."""
        try:
            return await self.get("/users/@me")
        except HTTPException as e:
            if e.status == 401:
                raise LoginFailure("Improper token has been passed.") from e
            raise

    async def get_user(self, user_id: Any) -> Dict[str, Any]:
        return await self.get(f"/users/{user_id}")

    async def get_user_profile(self, user_id: Any, *, guild_id: Any = None) -> Dict[str, Any]:
        params = {"with_mutual_guilds": "false"}
        if guild_id:
            params["guild_id"] = str(guild_id)
        return await self.get(f"/users/{user_id}/profile", params=params)

    async def get_channel(self, channel_id: Any) -> Dict[str, Any]:
        return await self.get(f"/channels/{channel_id}")

    async def get_message(self, channel_id: Any, message_id: Any, *, guild_id: Any = None) -> Optional[Dict[str, Any]]:
        """One message by id, via an ``around`` window (the single-message route is bot-only)."""
        page = await self.get(
            f"/channels/{channel_id}/messages",
            params={"limit": 1, "around": str(message_id)},
            referer=self.referer(channel_id, guild_id),
        )
        if isinstance(page, list):
            for msg in page:
                if str(msg.get("id")) == str(message_id):
                    return msg
        return None

    async def get_guild(self, guild_id: Any, *, with_counts: bool = True) -> Dict[str, Any]:
        return await self.get(f"/guilds/{guild_id}", params={"with_counts": str(with_counts).lower()})

    async def get_guild_channels(self, guild_id: Any) -> Any:
        return await self.get(f"/guilds/{guild_id}/channels")

    async def get_roles(self, guild_id: Any) -> Any:
        return await self.get(f"/guilds/{guild_id}/roles")

    async def get_emojis(self, guild_id: Any) -> Any:
        return await self.get(f"/guilds/{guild_id}/emojis")

    async def get_stickers(self, guild_id: Any) -> Any:
        return await self.get(f"/guilds/{guild_id}/stickers")

    async def get_member(self, guild_id: Any, user_id: Any) -> Dict[str, Any]:
        return await self.get(f"/guilds/{guild_id}/members/{user_id}")

    async def get_private_channels(self) -> Any:
        return await self.get("/users/@me/channels")

    async def start_private_message(
        self, user_id: Any, *, context_location: Optional[str] = None
    ) -> Dict[str, Any]:
        """Open or fetch the DM channel with a user."""
        headers = (
            {"X-Context-Properties": context_properties(context_location)}
            if context_location
            else None
        )
        return await self.post(
            "/users/@me/channels",
            json={"recipients": [str(user_id)]},
            headers=headers,
        )

    async def send_message(
        self,
        channel_id: Any,
        content: str = "",
        *,
        guild_id: Any = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """POST a message with the body shape the web client sends."""
        payload: Dict[str, Any] = {
            "mobile_network_type": "unknown",
            "content": content or "",
            "nonce": message_nonce(),
            "tts": False,
        }
        extra = dict(extra or {})
        for key in ("message_reference", "allowed_mentions"):
            if key in extra:
                payload[key] = extra.pop(key)
        payload["flags"] = 0
        payload.update(extra)
        gid = guild_id or self.guild_id
        return await self.post(
            f"/channels/{channel_id}/messages",
            json=payload,
            referer=channel_referer(channel_id, gid),
            headers={"X-Context-Properties": context_properties("chat_input")},
        )

    async def send_dm(self, user_id: Any, content: str, **kw: Any) -> Dict[str, Any]:
        dm = await self.start_private_message(user_id)
        return await self.send_message(dm["id"], content, **kw)

    async def edit_message(self, channel_id: Any, message_id: Any, **fields: Any) -> Dict[str, Any]:
        return await self.patch(f"/channels/{channel_id}/messages/{message_id}", json=fields)

    async def delete_message(self, channel_id: Any, message_id: Any) -> None:
        await self.delete(f"/channels/{channel_id}/messages/{message_id}")


def rate_limit_state(http: HTTPClient) -> Tuple[Dict[str, str], Dict[str, float], float]:
    """(route->bucket, bucket->reset, global reset), for diagnostics and tests."""
    return dict(http._route_buckets), dict(http._bucket_reset), http._global_reset


__all__ = [
    "API_BASE",
    "HTTPClient",
    "MESSAGE_PAGE",
    "THREAD_PAGE",
    "MAX_RETRIES",
    "message_nonce",
    "snowflake_at",
    "redact_request_headers",
    "rate_limit_state",
]
