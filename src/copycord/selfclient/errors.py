# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Exceptions raised by selfclient.

Names and attributes match discord.py-self's, so callers catch them the same way.
"""

from __future__ import annotations

from typing import Any, Optional


class SelfClientException(Exception):
    """Base for every exception this package raises."""


class ClientException(SelfClientException):
    """A misuse of the client rather than a refusal from Discord."""


class LoginFailure(ClientException):
    """The token was rejected (HTTP 401 on login, or gateway close 4004)."""


class ConnectionClosed(SelfClientException):
    """The gateway socket closed with a code we do not resume from."""

    def __init__(
        self, code: Optional[int], reason: str = "", *, shard_id: Optional[int] = None
    ):
        self.code: Optional[int] = code
        self.reason: str = reason or ""
        self.shard_id: Optional[int] = shard_id
        super().__init__(f"gateway closed with code {code}: {self.reason}".rstrip(": "))


class HTTPException(SelfClientException):
    """A REST call came back with a non-2xx status."""

    def __init__(
        self,
        status: int,
        json: Any = None,
        *,
        method: str = "",
        path: str = "",
        retry_after: Optional[float] = None,
        body: str = "",
    ):
        self.status: int = int(status or 0)
        self.json: Any = json
        self.method: str = method
        self.path: str = path
        self.retry_after: Optional[float] = retry_after
        if isinstance(json, dict):
            self.code: int = int(json.get("code") or 0)
            self.text: str = str(json.get("message") or "")
        else:
            self.code = 0
            self.text = body or ""
        where = f"{method} {path} ".lstrip()
        detail = f" (error code: {self.code})" if self.code else ""
        msg = f"{where}{self.status}{detail}"
        if self.text:
            msg += f": {self.text}"
        super().__init__(msg)

    @property
    def response_status(self) -> int:
        return self.status


class Forbidden(HTTPException):
    """HTTP 403."""


class NotFound(HTTPException):
    """HTTP 404."""


class DiscordServerError(HTTPException):
    """HTTP 5xx."""


class RateLimited(HTTPException):
    """Still 429 after the retry budget ran out."""

    def __init__(
        self,
        retry_after: float,
        json: Any = None,
        *,
        method: str = "",
        path: str = "",
        body: str = "",
    ):
        super().__init__(
            429, json, method=method, path=path, retry_after=float(retry_after), body=body
        )
        self.retry_after: float = float(retry_after)


def for_status(
    status: int,
    json: Any = None,
    *,
    method: str = "",
    path: str = "",
    body: str = "",
) -> HTTPException:
    """The exception class discord.py would raise for this status."""
    if status == 403:
        cls = Forbidden
    elif status == 404:
        cls = NotFound
    elif status >= 500:
        cls = DiscordServerError
    else:
        cls = HTTPException
    return cls(status, json, method=method, path=path, body=body)


__all__ = [
    "SelfClientException",
    "ClientException",
    "LoginFailure",
    "ConnectionClosed",
    "HTTPException",
    "Forbidden",
    "NotFound",
    "DiscordServerError",
    "RateLimited",
    "for_status",
]
