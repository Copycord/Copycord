"""The one time zone setting: TZ in .env."""

import logging
import os
from zoneinfo import ZoneInfo

logger = logging.getLogger("common.timezone")

_warned = set()


def _warn_once(key: str, message: str, *args) -> None:
    if key not in _warned:
        _warned.add(key)
        logger.warning(message, *args)


def configured_tz_name() -> str:
    """Name of the configured time zone: TZ, else UTC. An invalid name gives UTC."""
    name = (os.getenv("TZ") or "").strip()
    if not name:
        old = (os.getenv("UI_TZ") or "").strip()
        if old:
            _warn_once(
                "ui_tz",
                "UI_TZ is no longer used. Rename it to TZ in your .env file.",
            )
            name = old
    if not name:
        return "UTC"
    try:
        ZoneInfo(name)
    except Exception:
        _warn_once(
            "bad:" + name,
            "Time zone %r is not valid, using UTC. Set TZ to a name like America/New_York.",
            name,
        )
        return "UTC"
    return name


def configured_tz() -> ZoneInfo:
    return ZoneInfo(configured_tz_name())
