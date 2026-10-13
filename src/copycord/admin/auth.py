# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations

import hmac
import math
import os
import secrets
import time
from pathlib import Path
from typing import Optional, Dict

from fastapi import FastAPI, Request, Form, status
from fastapi.responses import RedirectResponse, PlainTextResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from copycord.admin.logging_setup import LOGGER
from copycord.common.config import CURRENT_VERSION
from copycord.common.private_file import write_private_file

ADMIN_COOKIE_NAME = "cc_admin"
SESSION_MAX_AGE = 60 * 60 * 12


ADMIN_PASSWORD: str = ""
_session_signer: URLSafeTimedSerializer | None = None


def _client_ip(request: Request) -> str:
    """Best-effort client IP for logging, honoring reverse proxy and Cloudflare headers."""
    try:
        hdrs = request.headers

        cf_ip = hdrs.get("cf-connecting-ip")
        if cf_ip:
            return cf_ip.strip()

        real_ip = hdrs.get("x-real-ip")
        if real_ip:
            return real_ip.strip()

        xff = hdrs.get("x-forwarded-for")
        if xff:
            first = xff.split(",")[0].strip()
            if first:
                return first

        if request.client:
            return request.client.host

    except Exception:
        pass

    return "unknown"


def _limiter_key(request: Request) -> str:
    """Socket peer address for the login limiter.

    Forwarding headers are not read here: any client can set them. Uvicorn
    already replaces the peer with X-Forwarded-For when the connection comes
    from a proxy listed in FORWARDED_ALLOW_IPS (default 127.0.0.1), so a local
    reverse proxy still gets per-client limits; behind any other proxy all
    clients share the proxy's address.
    """
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def password_matches(given: str, expected: str) -> bool:
    """Constant-time compare of the submitted and configured passwords."""
    return hmac.compare_digest(
        (given or "").encode("utf-8"), (expected or "").encode("utf-8")
    )


class LoginLimiter:
    """In-memory failed-login limiter keyed by client address.

    After max_failures failures in a row the key is locked for base_delay
    seconds, doubling with each further failure up to max_delay. A success
    clears the key; so does a quiet window with no failures and no lock.
    """

    def __init__(
        self,
        max_failures: int = 5,
        window: float = 300.0,
        base_delay: float = 30.0,
        max_delay: float = 900.0,
        max_keys: int = 10000,
        clock=time.monotonic,
    ) -> None:
        self.max_failures = max_failures
        self.window = window
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_keys = max_keys
        self._clock = clock
        # key -> [failures, last_failure, locked_until]
        self._state: Dict[str, list] = {}

    def _expired(self, entry: list, now: float) -> bool:
        return now >= entry[2] and now - entry[1] > self.window

    def retry_after(self, key: str) -> int:
        """Seconds until key may try again; 0 when it is not locked."""
        entry = self._state.get(key)
        if not entry:
            return 0
        now = self._clock()
        if self._expired(entry, now):
            del self._state[key]
            return 0
        return max(0, math.ceil(entry[2] - now))

    def failure(self, key: str) -> int:
        """Record a failed attempt; returns the lock length in seconds (0 if none)."""
        now = self._clock()
        entry = self._state.get(key)
        if entry is None or self._expired(entry, now):
            if len(self._state) >= self.max_keys:
                self._prune(now)
            entry = [0, now, 0.0]
            self._state[key] = entry
        entry[0] += 1
        entry[1] = now
        over = entry[0] - self.max_failures
        if over < 0:
            return 0
        delay = min(self.max_delay, self.base_delay * (2 ** min(over, 32)))
        entry[2] = now + delay
        return math.ceil(delay)

    def success(self, key: str) -> None:
        self._state.pop(key, None)

    def reset(self) -> None:
        self._state.clear()

    def _prune(self, now: float) -> None:
        for k in [k for k, e in self._state.items() if self._expired(e, now)]:
            del self._state[k]
        if len(self._state) >= self.max_keys:
            oldest = sorted(self._state, key=lambda k: self._state[k][1])
            for k in oldest[: len(self._state) - self.max_keys + 1]:
                del self._state[k]


LOGIN_LIMITER = LoginLimiter()


def _load_or_create_secret_key(
    data_dir: Path,
    secret_env: str,
    filename: str = "secret.key",
) -> str:
    """Load the key for signing session cookies: SECRET_KEY env, then <data dir>/secret.key, else a generated key (not persisted if the write fails).

    A secret.key that exists but cannot be read raises instead of being
    replaced, since a new key would silently sign everyone out.
    """
    if secret_env:
        return secret_env

    secret_file = data_dir / filename

    if secret_file.exists():
        try:
            key = secret_file.read_text(encoding="utf-8").strip()
        except Exception as e:
            raise RuntimeError(
                f"Cannot read the login key file {secret_file}: {e}. "
                "Fix its permissions or delete it to create a new one "
                "(deleting it signs everyone out)."
            ) from e
        if key:
            return key

    key = secrets.token_urlsafe(32)
    try:
        write_private_file(secret_file, key)
    except Exception:

        LOGGER.warning(
            "Failed writing %s; key will rotate on restart", secret_file, exc_info=True
        )
    return key


def _get_signer() -> URLSafeTimedSerializer:
    if _session_signer is None:
        raise RuntimeError("admin auth not initialized: call init_admin_auth() first")
    return _session_signer


def decode_admin_session(token: str | None) -> Optional[Dict]:
    """Return the session payload if the token is valid and unexpired, else None."""
    if not token:
        return None
    try:
        return _get_signer().loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


class PasswordGuardMiddleware(BaseHTTPMiddleware):
    """Single-password gate: with PASSWORD set, everything except /login, /health and static assets needs a valid session cookie.
    Unauthenticated GET/HEAD requests redirect to /login; others get a 401."""

    async def dispatch(self, request: Request, call_next):

        if not ADMIN_PASSWORD:
            return await call_next(request)

        path = request.url.path or "/"

        if (
            path.startswith("/login")
            or path.startswith("/health")
            or path.startswith("/static")
        ):
            return await call_next(request)

        token = request.cookies.get(ADMIN_COOKIE_NAME)
        sess = decode_admin_session(token)
        if sess and sess.get("ok") is True:
            return await call_next(request)

        if request.method in ("GET", "HEAD"):
            return RedirectResponse(url="/login")
        return PlainTextResponse(
            "Unauthorized",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )


def init_admin_auth(app: FastAPI, templates: Jinja2Templates, data_dir: Path) -> None:
    """Set up admin login: read PASSWORD and SECRET_KEY, add PasswordGuardMiddleware, CSRFMiddleware and the /login routes."""
    global ADMIN_PASSWORD, _session_signer

    ADMIN_PASSWORD = os.getenv("PASSWORD", "").strip()
    secret_env = os.getenv("SECRET_KEY", "").strip()

    secret_key = _load_or_create_secret_key(data_dir, secret_env)
    _session_signer = URLSafeTimedSerializer(secret_key, salt="copycord-admin")

    app.add_middleware(PasswordGuardMiddleware)
    # Added last so it runs first; security imports this module.
    from copycord.admin.security import CSRFMiddleware

    app.add_middleware(CSRFMiddleware)

    def _too_many(request: Request, wait: int):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "version": CURRENT_VERSION,
                "error": f"Too many failed attempts. Try again in {wait} seconds.",
            },
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            headers={"Retry-After": str(wait)},
        )

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):

        if not ADMIN_PASSWORD:
            return RedirectResponse("/", status_code=status.HTTP_302_FOUND)

        sess = decode_admin_session(request.cookies.get(ADMIN_COOKIE_NAME))
        if sess and sess.get("ok") is True:
            LOGGER.info(
                "Admin login page hit by already-authenticated client %s; redirecting to dashboard",
                _client_ip(request),
            )
            return RedirectResponse("/", status_code=status.HTTP_302_FOUND)

        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "version": CURRENT_VERSION,
                "error": None,
            },
        )

    @app.post("/login", response_class=HTMLResponse)
    async def login_submit(
        request: Request,
        password: str = Form(...),
    ):

        if not ADMIN_PASSWORD:
            return RedirectResponse("/", status_code=status.HTTP_302_FOUND)

        key = _limiter_key(request)
        wait = LOGIN_LIMITER.retry_after(key)
        if wait:
            LOGGER.warning(
                "Admin login THROTTLED from %s (%ss left)", _client_ip(request), wait
            )
            return _too_many(request, wait)

        if password_matches(password, ADMIN_PASSWORD):
            LOGIN_LIMITER.success(key)

            LOGGER.info(
                "Admin login SUCCESS from %s",
                _client_ip(request),
            )

            token = _get_signer().dumps({"ok": True})

            resp = RedirectResponse("/", status_code=status.HTTP_302_FOUND)
            secure = request.url.scheme == "https"
            resp.set_cookie(
                ADMIN_COOKIE_NAME,
                token,
                httponly=True,
                samesite="lax",
                max_age=SESSION_MAX_AGE,
                secure=secure,
            )
            return resp

        LOGGER.warning(
            "Admin login FAILED from %s",
            _client_ip(request),
        )

        wait = LOGIN_LIMITER.failure(key)
        if wait:
            return _too_many(request, wait)

        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "version": CURRENT_VERSION,
                "error": "Incorrect password. Please try again.",
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
