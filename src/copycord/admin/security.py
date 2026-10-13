# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Request hardening helpers shared by the admin routes."""

from __future__ import annotations

import hmac
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocket

from copycord.admin import auth
from copycord.common.websockets import INTERNAL_TOKEN_ENV, INTERNAL_TOKEN_HEADER

# Policy violation; sent before accept, so the upgrade is refused.
WS_REJECT_CODE = 1008

# The dashboard sends this on every state-changing request. Cross-site pages
# cannot add a custom header without a CORS preflight, which this app never
# grants.
CSRF_HEADER = "X-Copycord-CSRF"
CSRF_HEADER_VALUE = "1"
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})
# Plain HTML form posts that cannot carry the header; Origin is still checked.
CSRF_HEADER_EXEMPT = frozenset({"/login"})

_SAFE_PART_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._ -]*$")


def _safe_part(part: str) -> bool:
    if not part or part in (".", ".."):
        return False
    if ".." in part:
        return False
    return bool(_SAFE_PART_RE.match(part))


def safe_child(base: Path, name: str, *, max_depth: int = 1) -> Optional[Path]:
    """Return base/name only if name is a plain relative name inside base.

    Rejects separators other than "/" (between up to max_depth parts),
    "..", drive letters, absolute paths and anything that resolves outside
    base. Returns None when the name is not acceptable.
    """
    if not isinstance(name, str) or not name:
        return None
    if "\\" in name or "\x00" in name or ":" in name:
        return None
    parts = name.split("/")
    if len(parts) > max_depth or not all(_safe_part(p) for p in parts):
        return None
    try:
        root = Path(base).resolve()
        p = (root / Path(*parts)).resolve()
    except (OSError, ValueError):
        return None
    if p == root or root not in p.parents:
        return None
    return p


def _host_key(host: str) -> str:
    host = (host or "").strip().lower()
    for default in (":80", ":443"):
        if host.endswith(default):
            host = host[: -len(default)]
    return host


def origin_matches_host(origin: Optional[str], headers: Headers) -> bool:
    """True when origin (an Origin or Referer URL) names the request's host.

    A reverse proxy that rewrites Host can pass the public name in
    X-Forwarded-Host. Browsers do not let pages set either header.
    """
    if not origin:
        return False
    try:
        origin_host = _host_key(urlsplit(origin).netloc)
    except ValueError:
        return False
    if not origin_host:
        return False
    hosts = [headers.get("host", "")]
    fwd = headers.get("x-forwarded-host")
    if fwd:
        hosts.extend(h for h in fwd.split(","))
    return any(origin_host == _host_key(h) for h in hosts if h.strip())


def websocket_origin_ok(ws: WebSocket) -> bool:
    """True when the Origin header names the host the socket was opened on.

    Browsers always send Origin on websocket upgrades and pages cannot forge
    it, so this blocks cross-site websocket hijacking.
    """
    return origin_matches_host(ws.headers.get("origin"), ws.headers)


def websocket_session_ok(ws: WebSocket) -> bool:
    """True when the cc_admin cookie is a valid session."""
    sess = auth.decode_admin_session(ws.cookies.get(auth.ADMIN_COOKIE_NAME))
    return bool(sess and sess.get("ok") is True)


def internal_token_ok(headers: Headers) -> bool:
    """True when the headers carry this run's internal token."""
    expected = os.getenv(INTERNAL_TOKEN_ENV, "")
    sent = headers.get(INTERNAL_TOKEN_HEADER, "")
    if not expected or not sent:
        return False
    return hmac.compare_digest(sent.encode("utf-8"), expected.encode("utf-8"))


def websocket_internal_ok(ws: WebSocket) -> bool:
    """True when the socket carries this run's internal token."""
    return internal_token_ok(ws.headers)


async def authorize_websocket(
    ws: WebSocket, *, browser: bool = False, internal: bool = False
) -> bool:
    """Gate a websocket before accept().

    browser: dashboard pages; Origin must match Host and, with PASSWORD set,
    the cc_admin session cookie must be valid.
    internal: Copycord's own agents; must send the per-run internal token.
    Returns False after closing the socket when neither credential passes.
    """
    if internal and websocket_internal_ok(ws):
        return True
    if browser and websocket_origin_ok(ws) and websocket_session_ok(ws):
        return True
    await ws.close(code=WS_REJECT_CODE)
    return False


def csrf_rejection(scope: Scope) -> Optional[str]:
    """Why an HTTP request fails the CSRF check, or None when it passes.

    Requests with a safe method, or with this run's internal token, pass.
    Everything else needs an Origin (or, without one, a Referer) naming this
    host, plus the CSRF header unless the path is a plain form post such as
    /login. This applies with or without PASSWORD.
    """
    if scope.get("method", "GET").upper() in SAFE_METHODS:
        return None
    headers = Headers(scope=scope)
    if internal_token_ok(headers):
        return None
    source = headers.get("origin") or headers.get("referer")
    if not origin_matches_host(source, headers):
        return "cross-origin request"
    path = (scope.get("path") or "/").rstrip("/") or "/"
    if path in CSRF_HEADER_EXEMPT:
        return None
    if headers.get(CSRF_HEADER) != CSRF_HEADER_VALUE:
        return f"missing {CSRF_HEADER} header"
    return None


class CSRFMiddleware:
    """Pure ASGI CSRF gate for state-changing HTTP requests; see csrf_rejection."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            reason = csrf_rejection(scope)
            if reason:
                resp = PlainTextResponse(f"Forbidden: {reason}", status_code=403)
                await resp(scope, receive, send)
                return
        await self.app(scope, receive, send)
