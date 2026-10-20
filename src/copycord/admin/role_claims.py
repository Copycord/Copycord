"""/api/redeem: the "Redeem code" modal's proxy to the Copycord helper.

The browser never talks to the helper's public API. /start signs its call with
the install key (common/install_identity.py, common/helper_client.py) and passes
the answer on. Both routes sit behind the app wide session guard and CSRF
middleware like every other /api route, so a logged out call is 401.

The helper base URL comes from COPYCORD_HELPER_URL, default https://copycord.xyz.
An empty value turns role claims off.

The modal is one code box. A member gets a personal code from the /redeem
slash command in the Copycord Discord (/redeem code:<event code>); it is bound
to their Discord account, single use and good for 24 hours. The helper grants
the role as soon as the code is redeemed, so there is no Discord login window.
Nothing here says what a code redeems: the helper alone knows.

Contract for the SPA
====================

Errors are JSON {"detail": "<message to show>"}.

GET /api/redeem/config
----------------------

Local only; never calls the helper.

200: {"configured": true}; {"configured": false} when COPYCORD_HELPER_URL is
set to "".

POST /api/redeem/start
----------------------

Body: {"code": "<personal code>"}. The code is stripped and must then be 1 to
64 characters. It is never logged. The helper gets {"code": ...} only.

200: {"status": "claimed", "message": "Role claimed", "reward": "role"}

The role has been added. status and reward are passed on from the helper
("claimed" and "role" when it omits them); message is its text, "Role
claimed" when it sends none. The SPA ignores reward.

- 400 {"detail": ...}: body is not JSON, or the code is missing, empty or
  longer than 64 characters.
- 503 {"detail": "Role claims are not configured"}: COPYCORD_HELPER_URL is "".
- Helper errors: see below.

Errors from the helper
----------------------

The helper's own {"error": "..."} text becomes detail. Its status passes
through when it is one of these:

- 400: bad request, every invalid, used, expired or wrong version code
  ("Invalid code"), and a member who is not in the Copycord Discord server
  ("Join the Copycord Discord server first")
- 403: refused, for example a banned install
- 404: passed through as sent
- 409: the account already has the role ("Role already claimed")
- 429: rate limited, including too many wrong codes

Anything else (401 bad signature, 5xx such as "Could not add the role, try
again later", a non JSON answer) is 502 with the helper's text when it sent
one. A helper that cannot be reached (DNS, refused, TLS, 10 s timeout) is 502
{"detail": "Server is down"}.
"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from copycord.common import helper_client
from copycord.common.helper_client import HelperError

logger = logging.getLogger("admin.role_claims")

router = APIRouter(prefix="/api/redeem", tags=["redeem"])

CODE_MAX = 64
PASSTHROUGH = frozenset({400, 403, 404, 409, 429})
NOT_CONFIGURED = "Role claims are not configured"
CLAIMED = "Role claimed"


def _text(value: object, default: str) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else default


def _error(status: int, detail: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=status)


def _helper_error(e: HelperError) -> JSONResponse:
    status = e.status if e.status in PASSTHROUGH else 502
    return _error(status, e.message)


@router.get("/config")
async def config():
    return {"configured": bool(helper_client.helper_url())}


@router.post("/start")
async def start_claim(request: Request):
    try:
        body = json.loads(await request.body() or b"null")
    except ValueError:
        return _error(400, "Expected JSON {code}")
    if not isinstance(body, dict):
        return _error(400, "Expected JSON {code}")
    code = body.get("code")
    code = code.strip() if isinstance(code, str) else ""
    if not 1 <= len(code) <= CODE_MAX:
        return _error(400, f"Enter a code of 1 to {CODE_MAX} characters")
    if not helper_client.helper_url():
        return _error(503, NOT_CONFIGURED)
    try:
        data = await helper_client.start_claim(code)
    except HelperError as e:
        logger.info("Role claims: start failed (%s): %s", e.status, e.message)
        return _helper_error(e)
    return {
        "status": _text(data.get("status"), "claimed"),
        "message": _text(data.get("message"), CLAIMED),
        "reward": _text(data.get("reward"), "role"),
    }
