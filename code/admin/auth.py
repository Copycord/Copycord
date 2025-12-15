# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations

import os
import secrets
from pathlib import Path
from typing import Optional, Dict
from dotenv import load_dotenv

from fastapi import FastAPI, Request, Form, status
from fastapi.responses import RedirectResponse, PlainTextResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from admin.logging_setup import LOGGER
from common.config import CURRENT_VERSION

BASE_DIR = Path(__file__).resolve().parent        
PROJECT_ROOT = BASE_DIR.parent  
load_dotenv(PROJECT_ROOT / ".env")

ADMIN_COOKIE_NAME = "cc_admin"
SESSION_MAX_AGE = 60 * 60 * 12


ADMIN_PASSWORD: str = ""
_session_signer: URLSafeTimedSerializer | None = None


def _client_ip(request: Request) -> str:
    """
    Best-effort client IP for logging, aware of reverse proxies / Cloudflare.
    """
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


def _load_or_create_secret_key(
    data_dir: Path,
    secret_env: str,
    filename: str = "secret.key",
) -> str:
    """
    Load a stable secret key for signing admin session cookies.

    Priority:
      1) SECRET_KEY env (advanced users)
      2) /data/admin_secret.key (auto-persisted)
      3) freshly generated (non-persisted if write fails)
    """
    if secret_env:
        return secret_env

    secret_file = data_dir / filename

    if secret_file.exists():
        try:
            key = secret_file.read_text(encoding="utf-8").strip()
            if key:
                return key
        except Exception:
            LOGGER.warning(
                "Failed reading %s; regenerating", secret_file, exc_info=True
            )

    key = secrets.token_urlsafe(32)
    try:
        secret_file.write_text(key, encoding="utf-8")
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
    """
    Return session payload if token is valid & not expired, else None.
    """
    if not token:
        return None
    try:
        return _get_signer().loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


class PasswordGuardMiddleware(BaseHTTPMiddleware):
    """
    Very simple 'one password for everything' gate.

    - If PASSWORD is not set, it does nothing.
    - If PASSWORD is set:
        * /login and /health (and static assets) are always allowed
        * every other request must have a valid signed session cookie,
          otherwise you get redirected to /login (for GET/HEAD)
          or a 401 for other methods.
    """

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
    """
    Initialize admin login / cookie auth:

    - Reads PASSWORD & SECRET_KEY from env
    - Sets up the session signer
    - Adds PasswordGuardMiddleware
    - Registers /login GET+POST routes
    """
    global ADMIN_PASSWORD, _session_signer

    ADMIN_PASSWORD = os.getenv("PASSWORD", "").strip()
    secret_env = os.getenv("SECRET_KEY", "").strip()

    secret_key = _load_or_create_secret_key(data_dir, secret_env)
    _session_signer = URLSafeTimedSerializer(secret_key, salt="copycord-admin")

    app.add_middleware(PasswordGuardMiddleware)

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

        if password == ADMIN_PASSWORD:

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

        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "version": CURRENT_VERSION,
                "error": "Incorrect password. Please try again.",
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
