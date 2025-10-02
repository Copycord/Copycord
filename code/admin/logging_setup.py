# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
import logging
import os
import sys as _sys
import json as _json
import contextvars
from datetime import datetime


REDACT_KEYS = {"SERVER_TOKEN", "CLIENT_TOKEN"}


req_id_var = contextvars.ContextVar("req_id", default="-")
route_var = contextvars.ContextVar("route", default="-")
client_var = contextvars.ContextVar("client", default="-")


def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _redact_value(val):
    try:
        s = str(val)
        for k in REDACT_KEYS:
            envv = os.getenv(k)
            if envv and envv in s:
                s = s.replace(envv, "***REDACTED***")
        return s
    except Exception:
        return "<unprintable>"


def _redact_obj(obj):
    try:
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                if str(k) in REDACT_KEYS and v:
                    out[k] = "***REDACTED***"
                else:
                    out[k] = v
            return out
        return obj
    except Exception:
        return {"_redact_error": True}


class RedactFilter(logging.Filter):
    """Injects context + redacts secrets appearing in args/msg."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.req_id = req_id_var.get()
        record.scope = route_var.get()
        record.client = client_var.get()
        try:
            if isinstance(record.args, dict):
                new_args = {}
                for k, v in record.args.items():
                    if isinstance(v, dict):
                        new_args[k] = _redact_obj(v)
                    elif isinstance(v, (int, float, bool, type(None))):
                        new_args[k] = v
                    elif isinstance(v, str):
                        new_args[k] = _redact_value(v)
                    else:
                        new_args[k] = v
                record.args = new_args
            elif isinstance(record.args, (tuple, list)):
                new_list = []
                for a in record.args:
                    if isinstance(a, dict):
                        new_list.append(_redact_obj(a))
                    elif isinstance(a, (int, float, bool, type(None))):
                        new_list.append(a)
                    elif isinstance(a, str):
                        new_list.append(_redact_value(a))
                    else:
                        new_list.append(a)
                record.args = (
                    tuple(new_list) if isinstance(record.args, tuple) else new_list
                )

            if isinstance(record.msg, str):
                record.msg = _redact_value(record.msg)
        except Exception:
            pass
        return True


LEVEL_MARK = {
    logging.DEBUG: "🧩",
    logging.INFO: "✅",
    logging.WARNING: "⚠️",
    logging.ERROR: "❌",
    logging.CRITICAL: "💥",
}


class HumanFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        mark = LEVEL_MARK.get(record.levelno, "•")
        ts = _now_iso()
        scope = getattr(record, "scope", "-")
        rid = getattr(record, "req_id", "-")
        cli = getattr(record, "client", "-")
        msg = super().format(record)
        extras = []
        for k in (
            "conn_id",
            "socket_id",
            "channel_id",
            "guild_id",
            "events_sent",
            "forwarded",
            "heartbeats",
            "took_ms",
        ):
            v = getattr(record, k, None)
            if v not in (None, "", []):
                extras.append(f"{k}={v}")
        extras_s = f" | {' '.join(extras)}" if extras else ""
        return f"{ts} {mark} {record.levelname:<8} [{scope}] (rid={rid} cli={cli}) {msg}{extras_s}"


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "time": _now_iso(),
            "lvl": record.levelname,
            "msg": super().format(record),
            "scope": getattr(record, "scope", "-"),
            "req_id": getattr(record, "req_id", "-"),
            "client": getattr(record, "client", "-"),
            "logger": record.name,
        }
        for k in (
            "conn_id",
            "socket_id",
            "channel_id",
            "guild_id",
            "events_sent",
            "forwarded",
            "heartbeats",
            "took_ms",
        ):
            v = getattr(record, k, None)
            if v not in (None, "", []):
                base[k] = v
        return _json.dumps(base, separators=(",", ":"))


class ContextAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.setdefault("extra", {})
        for k, v in self.extra.items():
            extra.setdefault(k, v)
        return msg, kwargs


def get_logger(name="copycord", **ctx):
    logger = logging.getLogger(name)
    return ContextAdapter(logger, dict(ctx))


def configure_app_logging():
    """
    Unified logging config with:
    - LOG_FORMAT: HUMAN (default) or JSON
    - LOG_LEVEL: DEBUG/INFO/etc.
    - redaction + context
    - reuses uvicorn.error handlers when present
    """
    fmt = os.getenv("LOG_FORMAT", "HUMAN").strip().upper()
    lvl = os.getenv("LOG_LEVEL", "INFO").strip().upper()

    root = logging.getLogger("copycord")
    uvicorn_err = logging.getLogger("uvicorn.error")

    def _apply(h: logging.Handler):
        if fmt == "JSON":
            h.setFormatter(JSONFormatter("%(message)s"))
        else:
            h.setFormatter(HumanFormatter("%(message)s"))
        h.addFilter(RedactFilter())

    if uvicorn_err.handlers:
        root.handlers = uvicorn_err.handlers[:]
        for h in root.handlers:
            _apply(h)
    else:
        root.handlers.clear()
        h = logging.StreamHandler(stream=_sys.stdout)
        _apply(h)
        root.addHandler(h)

    root.propagate = False
    root.setLevel(getattr(logging, lvl, logging.INFO))

    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(
        logging.WARNING if root.level > logging.DEBUG else logging.DEBUG
    )
    return get_logger("copycord")


LOGGER = configure_app_logging()
