# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""The ``qos_token`` READY hands out, sent back on the next fresh IDENTIFY.

READY's ``qos_token`` is base64 protobuf: f1 a message {f1 fixed64 user id,
f2 fixed64 issued ms, f4 32 bytes, f5 fixed64 expiry ms (issue + 14 days)},
f2 a 32-byte signature, f3 varint 1. Chrome keeps the newest one and its next
fresh IDENTIFY carries {f1: empty, f2: <the READY token bytes>}; with nothing
stored it sends NO_TOKEN. RESUME carries none.

Stored per account (the short token hash) in the DB's gateway_qos_tokens
table. Not an auth secret, but never logged: debug lines give its expiry and a
short hash of it.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from copycord.common.selfbot_headers import _token_log_id

log = logging.getLogger("selfclient.qos")

# {f1: {f1: 1}}: IDENTIFY's qos_token with no usable READY token stored.
NO_TOKEN = "CgIIAQ=="

_VARINT, _FIXED64, _BYTES, _FIXED32 = 0, 1, 2, 5


def _read_varint(buf: bytes, pos: int) -> Tuple[int, int]:
    value = shift = 0
    while True:
        if pos >= len(buf) or shift > 63:
            raise ValueError("truncated varint")
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if not value:
            out.append(byte)
            return bytes(out)
        out.append(byte | 0x80)


def read_fields(buf: bytes) -> Dict[int, List[Any]]:
    """Field number -> values of one protobuf message: ints for varint and
    fixed fields, bytes for length-delimited ones. ValueError when it is
    truncated or uses a wire type a token never has."""
    fields: Dict[int, List[Any]] = {}
    pos = 0
    while pos < len(buf):
        key, pos = _read_varint(buf, pos)
        number, wire = key >> 3, key & 7
        if number == 0:
            raise ValueError("field number 0")
        if wire == _VARINT:
            value, pos = _read_varint(buf, pos)
        elif wire in (_FIXED64, _FIXED32):
            size = 8 if wire == _FIXED64 else 4
            if pos + size > len(buf):
                raise ValueError("truncated fixed field")
            value = int.from_bytes(buf[pos : pos + size], "little")
            pos += size
        elif wire == _BYTES:
            size, pos = _read_varint(buf, pos)
            if pos + size > len(buf):
                raise ValueError("truncated bytes field")
            value = bytes(buf[pos : pos + size])
            pos += size
        else:
            raise ValueError(f"wire type {wire}")
        fields.setdefault(number, []).append(value)
    return fields


def _first(fields: Dict[int, List[Any]], number: int, kind: type) -> Any:
    for value in fields.get(number, ()):
        if isinstance(value, kind):
            return value
    return None


def _b64decode(value: str) -> bytes:
    text = value.strip()
    return base64.b64decode(text + "=" * (-len(text) % 4), validate=True)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iso(ms: Optional[int]) -> str:
    if ms is None:
        return "unknown"
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))


@dataclass(frozen=True)
class ReadyToken:
    """A READY qos_token, decoded."""

    raw: bytes
    user_id: Optional[int]
    issued_ms: Optional[int]
    expires_ms: int

    @property
    def ref(self) -> str:
        """A short hash of the token, for logs."""
        return hashlib.sha256(self.raw).hexdigest()[:10]

    def expired(self, now_ms: Optional[int] = None) -> bool:
        return self.expires_ms <= (_now_ms() if now_ms is None else now_ms)


def decode_raw(raw: bytes) -> Optional[ReadyToken]:
    """READY token bytes decoded; None when they do not parse or carry no expiry."""
    try:
        body = _first(read_fields(raw), 1, bytes)
        if body is None:
            return None
        inner = read_fields(body)
    except ValueError:
        return None
    expires = _first(inner, 5, int)
    if expires is None:
        return None
    return ReadyToken(bytes(raw), _first(inner, 1, int), _first(inner, 2, int), expires)


def decode_ready_token(value: Any) -> Optional[ReadyToken]:
    """READY's qos_token decoded; None when it is not base64 protobuf with an expiry."""
    if not isinstance(value, str) or not value:
        return None
    try:
        raw = _b64decode(value)
    except ValueError:
        return None
    return decode_raw(raw)


def wrap(raw: bytes) -> str:
    """IDENTIFY's qos_token carrying a READY token: {f1: empty, f2: raw}."""
    return base64.b64encode(b"\x0a\x00\x12" + _varint(len(raw)) + raw).decode("ascii")


def unwrap(value: Any) -> Optional[bytes]:
    """The READY token bytes inside an IDENTIFY qos_token; None for NO_TOKEN or junk."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return _first(read_fields(_b64decode(value)), 2, bytes)
    except ValueError:
        return None


def choose(
    stored: Optional[str], account_id: Any = None, *, now_ms: Optional[int] = None
) -> Tuple[str, str]:
    """(IDENTIFY's qos_token, why) for a stored READY qos_token: it wrapped, or
    NO_TOKEN when nothing is stored, it does not parse, it expired, or it was
    issued to another user than ``account_id`` (when that is known)."""
    if not stored:
        return NO_TOKEN, "none stored"
    token = decode_ready_token(stored)
    if token is None:
        return NO_TOKEN, "stored token does not parse"
    if token.expired(now_ms):
        return NO_TOKEN, f"stored token {token.ref} expired {_iso(token.expires_ms)}"
    if account_id and token.user_id is not None and str(token.user_id) != str(account_id):
        return NO_TOKEN, f"stored token {token.ref} is another user's"
    return wrap(token.raw), f"stored token {token.ref} expires {_iso(token.expires_ms)}"


class QosTokenStore:
    """The last READY qos_token per account, keyed by the short token hash: in
    the DB when one is given and reachable, else in memory."""

    def __init__(self, db: Any = None) -> None:
        self.db = db
        self._memory: Dict[str, Tuple[str, Optional[str]]] = {}

    def load(self, token: str) -> Tuple[Optional[str], Optional[str]]:
        """(READY qos_token, the user id that READY named) last stored for token."""
        key = _token_log_id(token)
        if self.db is not None:
            try:
                row = self.db.get_gateway_qos_token(key)
            except sqlite3.Error as exc:
                log.debug("qos_token read failed: %r", exc)
            else:
                if row:
                    return row["qos_token"], row["user_id"]
        return self._memory.get(key, (None, None))

    def save(
        self, token: str, value: str, user_id: Optional[str], expires_ms: Optional[int]
    ) -> None:
        key = _token_log_id(token)
        self._memory[key] = (value, user_id)
        if self.db is not None:
            try:
                self.db.set_gateway_qos_token(key, value, user_id, expires_ms)
            except sqlite3.Error as exc:
                log.debug("qos_token write failed: %r", exc)


_store: Optional[QosTokenStore] = None


def _open_db() -> Any:
    """The data dir's DB, or None when it does not exist yet or will not open."""
    from copycord.common.db import DBManager
    from copycord.common.paths import db_path

    path = db_path()
    if not path.exists():
        return None
    try:
        return DBManager(str(path))
    except Exception as exc:
        log.debug("qos_token store has no DB: %r", exc)
        return None


def store() -> QosTokenStore:
    """The process's store, on the data dir's DB from its first use."""
    global _store
    if _store is None:
        _store = QosTokenStore(_open_db())
    return _store


def set_store(new: Optional[QosTokenStore]) -> None:
    """Use ``new`` from now on; None reopens the data dir's DB on next use."""
    global _store
    _store = new


def remember_ready(token: str, ready: Any) -> Optional[ReadyToken]:
    """Store READY's qos_token for token's account, replacing the last one.

    A READY without one keeps what is stored. Returns it decoded, or None.
    """
    d = ready if isinstance(ready, dict) else {}
    value = d.get("qos_token")
    if not isinstance(value, str) or not value:
        return None
    user = d.get("user")
    user_id = user.get("id") if isinstance(user, dict) else None
    decoded = decode_ready_token(value)
    store().save(
        token,
        value,
        str(user_id) if user_id else None,
        decoded.expires_ms if decoded is not None else None,
    )
    if decoded is None:
        log.debug("READY qos_token does not parse; IDENTIFY will send the default")
    else:
        log.debug("READY qos_token %s expires %s", decoded.ref, _iso(decoded.expires_ms))
    return decoded


def identify_token(token: str, account_id: Any = None) -> str:
    """IDENTIFY's qos_token for token's account: its stored READY token
    wrapped, else NO_TOKEN. ``account_id`` is the user id the last READY of
    this connection named; without one, the id stored with the token."""
    value, stored_user = store().load(token)
    qos_token, why = choose(value, account_id or stored_user)
    log.debug(
        "IDENTIFY qos_token %s (%s)", "stored" if qos_token != NO_TOKEN else "default", why
    )
    return qos_token


__all__ = [
    "NO_TOKEN",
    "QosTokenStore",
    "ReadyToken",
    "choose",
    "decode_raw",
    "decode_ready_token",
    "identify_token",
    "read_fields",
    "remember_ready",
    "set_store",
    "store",
    "unwrap",
    "wrap",
]
