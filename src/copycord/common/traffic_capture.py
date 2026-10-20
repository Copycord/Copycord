# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""A user token's Discord traffic, recorded in discord-capture's events.jsonl format.

discord-capture records what real Chrome sends to and receives from Discord; this
writes the same records for Copycord's own client, so its report.mjs compares
the two. One recorder per token, per process, writing
<data dir>/captures/<YYYYMMDD-HHMMSS>-<token id>/events.jsonl, one JSON object per
line, appended and flushed as it happens.

What is recorded is what Copycord hands its transports: the header dict given
to curl_cffi (its impersonation profile adds defaults and fixes the order on the
wire; wire_probe.py records that side), the JSON bodies, every gateway frame
sent and every one received after zlib-stream decoding. X-Super-Properties stays
base64 in the headers, as Chrome sends it and discord-capture records it, and is
also given decoded as superProperties.

Secrets are redacted before anything is written, with discord-capture's rules
and its "<redacted len=N>" form: Authorization and cookie headers, token keys
anywhere in bodies and frames (op 2, op 6, the science body), passwords, codes
and logins in /auth and /mfa bodies, secret query parameters. Only Discord hosts
are recorded.

Dev mode only (COPYCORD_ENV=dev). Otherwise recorder_for() returns None after
one is_dev() check and nothing is created or written. Nothing here raises into
a caller; a failure costs the record and logs at debug.
"""

from __future__ import annotations

import atexit
import base64
import itertools
import json
import logging
import platform
import re
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import unquote_plus, urlencode, urlsplit

from copycord.common.env import is_dev
from copycord.common.paths import data_dir

logger = logging.getLogger("common.traffic_capture")

CAPTURE_DIR = "captures"
EVENTS_FILE = "events.jsonl"
TOOL = "copycord"

# discord-capture lib/util.mjs
DISCORD_DOMAINS = ("discord.com", "discord.gg", "discordapp.com", "discordapp.net", "discord.media")
_GATEWAY_HOST = re.compile(r"^gateway(-[a-z0-9-]+)?\.discord\.gg$", re.IGNORECASE)

# discord-capture lib/redact.mjs
SECRET_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-captcha-key",
        "x-captcha-rqtoken",
        "x-captcha-session-id",
    }
)
SECRET_KEYS = frozenset(
    {
        "token",
        "password",
        "new_password",
        "ticket",
        "captcha_key",
        "captcha_rqtoken",
        "captcha_session_id",
        "analytics_token",
        "auth_token",
        "handoff_token",
        "encrypted_token",
        "access_token",
        "refresh_token",
        "secret",
        "secret_key",
        "totp_secret",
    }
)
AUTH_KEYS = frozenset({"code", "login"})
SECRET_QUERY = frozenset(
    {
        "token",
        "handoff_token",
        "code",
        "ticket",
        "password",
        "captcha_key",
        "access_token",
        "refresh_token",
    }
)
_AUTH_PATH = re.compile(r"/(auth|mfa)(/|$)", re.IGNORECASE)

# discord-capture lib/recorder.mjs: received frames whose d is kept.
KEEP_DISPATCH = frozenset({"READY", "RESUMED", "SESSIONS_REPLACE"})
KEEP_OPS = frozenset({7, 9, 10, 11})

BODY_METHODS = frozenset({"POST", "PATCH", "PUT"})

# A body string longer than this (an avatar data URI) is kept as its length only.
MAX_BODY_STRING = 4096

# Credentials in a proxy URL, should one end up in an error message.
_URL_USERINFO = re.compile(r"(://)[^/@\s]+@")

_LOCK = threading.Lock()
_RECORDERS: Dict[str, "TrafficRecorder"] = {}
_ATEXIT_REGISTERED = False


# helpers shared with discord-capture


class _Null:
    """JSON null for a field that is written even when its value is None (op 11's d)."""


_NULL = _Null()


def _default(obj: Any) -> Any:
    if obj is _NULL:
        return None
    raise TypeError(f"{type(obj).__name__} is not JSON serializable")


def _dumps(value: Any) -> str:
    """JSON.stringify's output: compact, non-ASCII kept."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=_default)


def local_time(ts: int) -> str:
    """Local wall-clock time with its UTC offset, e.g. 2026-10-20T11:49:51.123-04:00."""
    return datetime.fromtimestamp(ts / 1000).astimezone().isoformat(timespec="milliseconds")


def dir_stamp(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%Y%m%d-%H%M%S")


def is_discord_host(hostname: Any) -> bool:
    h = str(hostname or "").lower().rstrip(".")
    return any(h == d or h.endswith("." + d) for d in DISCORD_DOMAINS)


def is_discord_url(url: Any) -> bool:
    try:
        parts = urlsplit(str(url))
    except ValueError:
        return False
    return parts.scheme in ("http", "https", "ws", "wss") and is_discord_host(parts.hostname)


def is_gateway_url(url: Any) -> bool:
    try:
        return bool(_GATEWAY_HOST.match(urlsplit(str(url)).hostname or ""))
    except ValueError:
        return False


def is_auth_path(url: Any) -> bool:
    try:
        return bool(_AUTH_PATH.search(urlsplit(str(url)).path))
    except ValueError:
        return False


def redact_value(value: Any) -> str:
    s = value if isinstance(value, str) else _dumps(value)
    return f"<redacted len={len(s)}>"


def redact_json(value: Any, *, auth: bool = False, max_string: Optional[int] = None) -> Any:
    """Deep copy with secret keys redacted, key order kept."""
    if isinstance(value, dict):
        out = {}
        for k, x in value.items():
            lk = str(k).lower()
            secret = lk in SECRET_KEYS or (auth and lk in AUTH_KEYS)
            if secret and x is not None:
                out[k] = redact_value(x)
            else:
                out[k] = redact_json(x, auth=auth, max_string=max_string)
        return out
    if isinstance(value, (list, tuple)):
        return [redact_json(x, auth=auth, max_string=max_string) for x in value]
    if max_string is not None and isinstance(value, str) and len(value) > max_string:
        return f"<string len={len(value)}>"
    return value


def redact_headers(headers: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for name, value in (headers or {}).items():
        out[name] = redact_value(str(value)) if str(name).lower() in SECRET_HEADERS else value
    return out


def _redact_params(params: str, extra: frozenset = frozenset()) -> str:
    parts = []
    for p in params.split("&"):
        name, eq, value = p.partition("=")
        if eq and (unquote_plus(name).lower() in SECRET_QUERY | SECRET_KEYS | extra):
            p = f"{name}={redact_value(unquote_plus(value))}"
        parts.append(p)
    return "&".join(parts)


def redact_url(url: Any) -> str:
    """Secret query and fragment parameters redacted, the rest byte for byte."""
    s = str(url or "")
    main, hashmark, frag = s.partition("#")
    base, q, query = main.partition("?")
    if q:
        main = f"{base}?{_redact_params(query)}"
    if "=" in frag:
        frag = _redact_params(frag)
    return f"{main}#{frag}" if hashmark else main


def summarize_ready(d: Any) -> Any:
    """READY reduced to its key order, its scalars and the sizes of everything else."""
    if not isinstance(d, dict):
        return d
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if str(k).lower() in SECRET_KEYS and v is not None:
            out[k] = redact_value(v)
        elif v is None or isinstance(v, (bool, int, float)):
            out[k] = v
        elif isinstance(v, str):
            out[k] = v if len(v) <= 200 else f"<string len={len(v)}>"
        elif isinstance(v, list):
            out[k] = f"<array len={len(v)}>"
        else:
            out[k] = f"<object keys={len(v) if isinstance(v, dict) else 0}>"
    return out


def summarize_received_d(op: Any, t: Any, d: Any) -> Any:
    if op == 0 and t == "READY":
        return summarize_ready(d)
    if op == 0 and t == "SESSIONS_REPLACE" and isinstance(d, list):
        out = []
        for s in d:
            s = s if isinstance(s, dict) else {}
            entry = {
                "session_id": s.get("session_id"),
                "status": s.get("status"),
                "active": s.get("active"),
                "client_info": redact_json(s.get("client_info")),
            }
            if isinstance(s.get("activities"), list):
                entry["activities"] = len(s["activities"])
            out.append({k: v for k, v in entry.items() if v is not None})
        return out
    return redact_json(d)


def keep_received_d(op: Any, t: Any) -> bool:
    return (op == 0 and t in KEEP_DISPATCH) or op in KEEP_OPS


def _decode_super_properties(value: Any) -> Optional[Any]:
    try:
        return json.loads(base64.b64decode(value))
    except Exception:
        return None


def _scrub_error(err: Any, token: Optional[str]) -> str:
    text = err if isinstance(err, str) else f"{type(err).__name__}: {err}"
    if token:
        text = text.replace(token, redact_value(token))
    return _URL_USERINFO.sub(r"\1***@", text)[:500]


# the recorder


class TrafficRecorder:
    """One token's events.jsonl. Every public method swallows its own failures."""

    def __init__(self, token: str, token_id: str, *, now=time.time):
        self._token = token
        self.id = token_id
        self._now = now
        self.started = self._ms()
        self.path = data_dir() / CAPTURE_DIR / f"{dir_stamp(self.started)}-{token_id}" / EVENTS_FILE
        self._file = None
        self._lock = threading.Lock()
        self._request_ids = itertools.count(1)
        self._socket_ids = itertools.count(1)
        self.stopped = False
        self.counts: Dict[str, int] = {
            "requests": 0,
            "bodies": 0,
            "wsSent": 0,
            "wsRecv": 0,
            "identify": 0,
            "resume": 0,
        }
        self._emit("capture_start", self._start_fields(), self.started)

    def _ms(self) -> int:
        return int(self._now() * 1000)

    def _start_fields(self) -> Dict[str, Any]:
        fields: Dict[str, Any] = {
            "tool": TOOL,
            "account": self.id,
            "python": platform.python_version(),
            "platform": sys.platform,
        }
        try:
            from copycord.common.config import CURRENT_VERSION

            fields["version"] = CURRENT_VERSION
        except Exception:
            fields["version"] = None
        try:
            from copycord.common import selfbot_headers as sh

            fields["impersonate"] = sh.TLS_IMPERSONATE
            fields["userAgent"] = sh.get_build_info().get("browser_user_agent")
        except Exception:
            fields["impersonate"] = None
        try:
            import curl_cffi

            fields["curl_cffi"] = getattr(curl_cffi, "__version__", None)
        except Exception:
            fields["curl_cffi"] = None
        # report.mjs prints this as the browser.
        fields["browser"] = (
            f"Copycord {fields.get('version') or '?'}, curl_cffi {fields.get('curl_cffi') or '?'}"
            f" impersonating {fields.get('impersonate') or '?'}"
        )
        return fields

    def _emit(self, type_: str, fields: Optional[Dict[str, Any]] = None, ts: Optional[int] = None) -> None:
        try:
            ts = self._ms() if ts is None else ts
            rec: Dict[str, Any] = {"ts": ts, "time": local_time(ts), "type": type_}
            for k, v in (fields or {}).items():
                if v is not None:
                    rec[k] = v
            line = _dumps(rec) + "\n"
            with self._lock:
                if self._file is None:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    self._file = open(self.path, "a", encoding="utf-8")
                self._file.write(line)
                self._file.flush()
        except Exception as e:
            logger.debug("traffic capture write failed for %s: %r", self.id, e)

    # HTTP

    def request(
        self,
        method: str,
        url: str,
        headers: Any,
        *,
        params: Any = None,
        body: Any = None,
        has_body: bool = False,
    ) -> Optional[str]:
        """request, request_headers and, for POST, PATCH and PUT, request_body.
        Returns the id to pass to response() or failed(), or None if not recorded."""
        try:
            if params:
                query = urlencode(params)
                url = f"{url}{'&' if '?' in url else '?'}{query}" if query else url
            if not is_discord_url(url):
                return None
            method = str(method).upper()
            rid = f"R{next(self._request_ids)}"
            ts = self._ms()
            sent = {k: v for k, v in (headers or {}).items() if v is not None}
            suppressed = [k for k, v in (headers or {}).items() if v is None]
            redacted = redact_headers(sent)
            self.counts["requests"] += 1
            self._emit(
                "request",
                {
                    "requestId": rid,
                    "method": method,
                    "url": redact_url(url),
                    "resourceType": "curl_cffi",
                    "headers": redacted,
                },
                ts,
            )
            sp = next((v for k, v in sent.items() if str(k).lower() == "x-super-properties"), None)
            self._emit(
                "request_headers",
                {
                    "requestId": rid,
                    "headers": redacted,
                    "suppressed": suppressed or None,
                    "superProperties": _decode_super_properties(sp) if sp else None,
                },
                ts,
            )
            if method in BODY_METHODS:
                if has_body:
                    self.counts["bodies"] += 1
                    self._emit(
                        "request_body",
                        {
                            "requestId": rid,
                            "size": len(_dumps(body).encode("utf-8")),
                            "json": redact_json(body, auth=is_auth_path(url), max_string=MAX_BODY_STRING),
                        },
                        ts,
                    )
                else:
                    self._emit("request_body", {"requestId": rid, "unavailable": True}, ts)
            return rid
        except Exception as e:
            logger.debug("traffic capture request failed for %s: %r", self.id, e)
            return None

    def response(self, rid: Optional[str], status: Any) -> None:
        if rid is None:
            return
        try:
            self._emit("response", {"requestId": rid, "status": int(status or 0)})
        except Exception as e:
            logger.debug("traffic capture response failed for %s: %r", self.id, e)

    def failed(self, rid: Optional[str], error: Any) -> None:
        if rid is None:
            return
        self._emit("request_failed", {"requestId": rid, "errorText": _scrub_error(error, self._token)})

    # websockets

    def ws_open(self, url: str, headers: Any, *, zlib_stream: Optional[bool] = None) -> Optional[str]:
        """ws_created and ws_handshake_request; returns the socket id, or None."""
        try:
            if not is_discord_url(url):
                return None
            wid = f"W{next(self._socket_ids)}"
            if zlib_stream is None:
                zlib_stream = "compress=zlib-stream" in str(url)
            self._emit(
                "ws_created",
                {
                    "requestId": wid,
                    "url": redact_url(url),
                    "gateway": is_gateway_url(url),
                    "compression": "zlib-stream" if zlib_stream else "none",
                },
            )
            self._emit("ws_handshake_request", {"requestId": wid, "headers": redact_headers(headers)})
            return wid
        except Exception as e:
            logger.debug("traffic capture ws_open failed for %s: %r", self.id, e)
            return None

    def ws_sent(self, wid: Optional[str], payload: Any, text: Optional[str] = None) -> None:
        if wid is None:
            return
        try:
            text = _dumps(payload) if text is None else text
            fields: Dict[str, Any] = {"requestId": wid, "opcode": 1, "size": len(text.encode("utf-8"))}
            if isinstance(payload, dict):
                op = payload.get("op")
                fields["op"] = op
                if "d" in payload:
                    fields["d"] = redact_json(payload["d"])
                if op == 2:
                    self.counts["identify"] += 1
                elif op == 6:
                    self.counts["resume"] += 1
            self.counts["wsSent"] += 1
            self._emit("ws_sent", fields)
        except Exception as e:
            logger.debug("traffic capture ws_sent failed for %s: %r", self.id, e)

    def ws_recv(
        self, wid: Optional[str], msg: Any, *, size: int, decoded_size: Optional[int] = None, binary: bool = True
    ) -> None:
        """One received message; decoded_size when it arrived compressed."""
        if wid is None:
            return
        try:
            fields: Dict[str, Any] = {"requestId": wid, "opcode": 2 if binary else 1, "size": int(size)}
            if decoded_size is not None:
                fields["decodedSize"] = int(decoded_size)
            if isinstance(msg, dict):
                op, t = msg.get("op"), msg.get("t")
                fields.update({"op": op, "t": t, "s": msg.get("s")})
                if keep_received_d(op, t):
                    fields["d"] = summarize_received_d(op, t, msg.get("d"))
                    # None is dropped by _emit; op 11 and op 9 keep theirs.
                    if fields["d"] is None:
                        fields["d"] = _NULL
                    if op == 0 and t == "READY":
                        fields["dSummarized"] = True
            else:
                fields["json"] = False
            self.counts["wsRecv"] += 1
            self._emit("ws_recv", fields)
        except Exception as e:
            logger.debug("traffic capture ws_recv failed for %s: %r", self.id, e)

    def ws_closed(self, wid: Optional[str], code: Any = None, reason: Any = None, *, error: Any = None) -> None:
        """The close frame received (opcode 8, where report.mjs reads close codes),
        then ws_closed; error for a socket that never opened."""
        if wid is None:
            return
        try:
            reason = str(reason or "")[:200] or None
            if code is not None:
                self._emit(
                    "ws_recv",
                    {
                        "requestId": wid,
                        "opcode": 8,
                        "size": 2 + len((reason or "").encode("utf-8")),
                        "closeCode": int(code),
                        "closeReason": reason,
                    },
                )
            if error is not None:
                self._emit("ws_error", {"requestId": wid, "errorMessage": _scrub_error(error, self._token)})
            self._emit(
                "ws_closed",
                {"requestId": wid, "closeCode": int(code) if code is not None else None, "closeReason": reason},
            )
        except Exception as e:
            logger.debug("traffic capture ws_closed failed for %s: %r", self.id, e)

    # lifecycle

    def stop(self, reason: str) -> None:
        """capture_stop, then close the file. A later record reopens it and appends."""
        try:
            self._emit("capture_stop", {"reason": reason, "counts": dict(self.counts)})
            self.stopped = True
            with self._lock:
                if self._file is not None:
                    self._file.close()
                    self._file = None
        except Exception as e:
            logger.debug("traffic capture stop failed for %s: %r", self.id, e)


# entry points


def _token_id(token: str) -> str:
    from copycord.common.selfbot_headers import _token_log_id

    return _token_log_id(token)


def recorder_for(token: Optional[str]) -> Optional[TrafficRecorder]:
    """This token's recorder, created on first use. None outside dev mode or without a token."""
    if not token or not is_dev():
        return None
    try:
        token = str(token)
        key = _token_id(token)
        with _LOCK:
            rec = _RECORDERS.get(key)
            if rec is None:
                rec = TrafficRecorder(token, key)
                _RECORDERS[key] = rec
                _register_atexit()
            elif rec.stopped:
                rec.stopped = False
        return rec
    except Exception as e:
        logger.debug("traffic capture unavailable: %r", e)
        return None


def stop(token: Optional[str], reason: str = "client closed") -> None:
    """capture_stop for this token's recorder, if one is open. Never creates one."""
    if not token or not is_dev():
        return
    try:
        rec = _RECORDERS.get(_token_id(str(token)))
        if rec is not None and not rec.stopped:
            rec.stop(reason)
    except Exception as e:
        logger.debug("traffic capture stop failed: %r", e)


def stop_all(reason: str = "process exit") -> None:
    for rec in list(_RECORDERS.values()):
        if not rec.stopped:
            rec.stop(reason)


def forget_all() -> None:
    """Drop every recorder, closing its file; for tests."""
    with _LOCK:
        recs = list(_RECORDERS.values())
        _RECORDERS.clear()
    for rec in recs:
        with rec._lock:
            if rec._file is not None:
                rec._file.close()
                rec._file = None


def _register_atexit() -> None:
    global _ATEXIT_REGISTERED
    if not _ATEXIT_REGISTERED:
        _ATEXIT_REGISTERED = True
        atexit.register(stop_all)


__all__ = [
    "CAPTURE_DIR",
    "EVENTS_FILE",
    "TrafficRecorder",
    "forget_all",
    "recorder_for",
    "redact_headers",
    "redact_json",
    "redact_url",
    "stop",
    "stop_all",
    "summarize_ready",
]
