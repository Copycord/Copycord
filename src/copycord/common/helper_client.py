"""Signed calls from this install to the Copycord helper's public redeem API.

Every request carries the five X-Copycord-* headers the helper verifies (see
src/copycord/admin/role_claims.py for the contract). The signed message is,
joined by "\\n" with no trailing newline:

    METHOD, PATH, TIMESTAMP, NONCE, VERSION, hex(sha256(body))

PATH is the path exactly as the helper sees it (request.raw_path), so it
includes any path in the configured base URL. The version travels only in the
X-Copycord-Version header; no request has a query string.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import time
from typing import Mapping, Optional
from urllib.parse import urlsplit

import aiohttp

from copycord.common import install_identity
from copycord.common.http_session import shared_session
from copycord.version import package_version

# The helper takes [0-9A-Za-z.+_-]{1,32}: v4.0.0.3, or v4.0.0.4.dev5+g1a2b3c4 for a
# dev build (without the date or dirty mark a local build may add).
VERSION = "v" + re.sub(r"\.(?:d\d{8}|dirty)$", "", package_version())

DEFAULT_HELPER_URL = "https://copycord.xyz"
HELPER_URL_ENV = "COPYCORD_HELPER_URL"

START_PATH = "/api/public/redeem/start"

TIMEOUT = aiohttp.ClientTimeout(total=10)
UNREACHABLE = "Server is down"
BAD_RESPONSE = "Server sent an unexpected response"


class HelperError(Exception):
    """A failed helper call. str() is safe to show; status is the helper's HTTP
    status, or None when it was never reached or answered nonsense."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.message = message
        self.status = status


def helper_url(env: Optional[Mapping[str, str]] = None) -> str:
    """The helper base URL without a trailing slash; "" means not configured."""
    env = os.environ if env is None else env
    raw = env.get(HELPER_URL_ENV)
    if raw is None:
        return DEFAULT_HELPER_URL
    return raw.strip().rstrip("/")


def signing_message(method: str, path: str, timestamp: str, nonce: str,
                    version: str, body: bytes) -> bytes:
    return "\n".join((
        method.upper(), path, timestamp, nonce, version,
        hashlib.sha256(body or b"").hexdigest(),
    )).encode("utf-8")


def signed_headers(method: str, path: str, body: bytes = b"", *,
                   version: str = VERSION, now: Optional[float] = None,
                   nonce: Optional[str] = None) -> dict[str, str]:
    timestamp = str(int(time.time() if now is None else now))
    nonce = nonce or secrets.token_urlsafe(18)
    msg = signing_message(method, path, timestamp, nonce, version, body)
    return {
        "X-Copycord-Install": install_identity.public_key_b64url(),
        "X-Copycord-Timestamp": timestamp,
        "X-Copycord-Nonce": nonce,
        "X-Copycord-Version": version,
        "X-Copycord-Signature": install_identity.b64url(install_identity.sign(msg)),
    }


async def _request(method: str, path: str, payload: Optional[dict] = None) -> dict:
    base = helper_url()
    if not base:
        raise HelperError("Server is not configured")
    body = b"" if payload is None else json.dumps(payload, separators=(",", ":")).encode("utf-8")
    full_path = urlsplit(base).path.rstrip("/") + path
    headers = signed_headers(method, full_path, body)
    if payload is not None:
        headers["Content-Type"] = "application/json"
    try:
        async with shared_session().request(
            method, base + path, data=body if payload is not None else None,
            headers=headers, timeout=TIMEOUT, allow_redirects=False,
        ) as resp:
            status = resp.status
            text = await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
        raise HelperError(UNREACHABLE) from e
    try:
        data = json.loads(text) if text else None
    except ValueError:
        data = None
    if status >= 400:
        error = data.get("error") if isinstance(data, dict) else None
        if not isinstance(error, str) or not error.strip():
            error = f"Server error (HTTP {status})"
        raise HelperError(error, status)
    if not isinstance(data, dict):
        raise HelperError(BAD_RESPONSE)
    return data


async def start_claim(code: str) -> dict:
    """Redeems a personal code; the helper's success JSON, {status, message, reward}."""
    return await _request("POST", START_PATH, {"code": code})
