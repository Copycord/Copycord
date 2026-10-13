# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Update check on start and the optional auto-update.

The check is a read-only hint: it asks GitHub about the saved channel (newest
release for stable and beta, the branch head for dev), caches the answer in
<data dir>/update-check.json for 24 hours and prints one line. It runs in a
background thread with a short timeout and never fails or delays startup.

Opt out with the app config UPDATE_CHECK_ON_START=false or the environment
variable COPYCORD_NO_UPDATE_CHECK=1.

AUTO_UPDATE=true (app config, off by default) makes the running background
service check every 6 hours and run the same flow as `copycord update`, which
restarts the service. A foreground `copycord run` in a terminal only hints.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import NamedTuple, Optional

from copycord import ui, update

logger = logging.getLogger("copycord.update_check")

CACHE_FILE = "update-check.json"
CACHE_TTL = 24 * 3600
AUTO_UPDATE_INTERVAL = 6 * 3600
CHECK_TIMEOUT = 3.0
ENV_OPT_OUT = "COPYCORD_NO_UPDATE_CHECK"
KEY_ON_START = "UPDATE_CHECK_ON_START"
KEY_AUTO_UPDATE = "AUTO_UPDATE"

_FALSE = ("0", "false", "no", "off")
_TRUE = ("1", "true", "yes", "on")


class Result(NamedTuple):
    available: bool
    current: str  # installed version, or the installed commit for dev
    latest: str  # newest version, or the branch head commit for dev
    checked_at: float
    dev: bool = False
    ref: str = ""


# --------------------------------------------------------------------------
# settings
# --------------------------------------------------------------------------


def config_value(key: str, default: str = "") -> str:
    """A stored app config value, read-only; default when there is no DB."""
    from copycord.common.paths import db_path

    path = db_path()
    if not path.exists():
        return default
    try:
        conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=1.0)
        try:
            row = conn.execute("SELECT value FROM app_config WHERE key=?", (key,)).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return default
    return str(row[0]) if row and row[0] is not None else default


def _flag(key: str, default: bool) -> bool:
    value = config_value(key).strip().lower()
    if value in _FALSE:
        return False
    if value in _TRUE:
        return True
    return default


def check_enabled() -> bool:
    if os.environ.get(ENV_OPT_OUT, "").strip().lower() in _TRUE:
        return False
    return _flag(KEY_ON_START, True)


def auto_update_enabled() -> bool:
    return _flag(KEY_AUTO_UPDATE, False)


# --------------------------------------------------------------------------
# cache
# --------------------------------------------------------------------------


def cache_path() -> Path:
    from copycord.common.paths import data_dir

    return data_dir() / CACHE_FILE


def _identity(channel: update.Channel) -> dict:
    """What a cached answer is only valid for."""
    ident = {"channel": channel.describe()}
    ident["installed"] = (update.installed_commit() or "") if channel.channel == update.DEV else update.current_version()
    return ident


def load_cache(channel: update.Channel, now: Optional[float] = None, ignore_age: bool = False) -> Optional[Result]:
    now = time.time() if now is None else now
    try:
        raw = json.loads(cache_path().read_text(encoding="utf-8"))
        checked = float(raw["checked_at"])
        if raw["identity"] != _identity(channel):
            return None
        if not ignore_age and not (0 <= now - checked < CACHE_TTL):
            return None
        return Result(bool(raw["available"]), str(raw["current"]), str(raw["latest"]),
                      checked, channel.channel == update.DEV, channel.ref)
    except (OSError, ValueError, KeyError, TypeError):
        return None


def save_cache(channel: update.Channel, result: Result) -> None:
    data = {
        "checked_at": result.checked_at,
        "identity": _identity(channel),
        "available": result.available,
        "current": result.current,
        "latest": result.latest,
    }
    try:
        path = cache_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    except OSError:
        logger.debug("could not write %s", CACHE_FILE, exc_info=True)


# --------------------------------------------------------------------------
# check
# --------------------------------------------------------------------------


def fetch(channel: update.Channel, timeout: float = CHECK_TIMEOUT, now: Optional[float] = None) -> Result:
    """Ask GitHub; raises on network errors and on a channel with no release."""
    now = time.time() if now is None else now
    if channel.channel == update.DEV:
        head = update.fetch_branch_head(channel.ref, timeout=timeout, repo=channel.repo_name)
        installed = update.installed_commit()
        # An unknown installed commit is not reported as an update.
        return Result(installed is not None and installed != head.sha,
                      installed or "", head.sha, now, True, channel.ref)
    release = update.fetch_latest_release(prerelease=channel.channel == update.BETA, timeout=timeout)
    current = update.current_version()
    return Result(update.is_newer(release.version, current), current, release.version, now)


def check_cached(channel: Optional[update.Channel] = None, force: bool = False,
                 timeout: float = CHECK_TIMEOUT) -> Result:
    """The cached answer when it is under 24 hours old, else a fresh one (saved)."""
    channel = update.load_channel() if channel is None else channel
    if not force:
        cached = load_cache(channel)
        if cached is not None:
            return cached
    result = fetch(channel, timeout=timeout)
    save_cache(channel, result)
    return result


def hint_text(result: Result) -> str:
    if result.dev:
        return (f"Update available: {result.ref} is at {result.latest[:12]} "
                f"(you have {result.current[:12]}). Run copycord update")
    return (f"Update available: v{result.latest.lstrip('v')} "
            f"(you have v{result.current.lstrip('v')}). Run copycord update")


def print_hint(result: Result, stream=None) -> None:
    stream = sys.stdout if stream is None else stream
    if result.available:
        ui.warn(hint_text(result), stream=stream)


def start(stream=None, wait: float = 0.0) -> Optional[threading.Thread]:
    """Check in a background thread and print the hint when there is one.

    Does nothing when opted out. wait > 0 joins the thread for at most that
    many seconds (for short commands that would exit before the answer).
    """
    try:
        if not check_enabled():
            return None
    except Exception:
        return None

    def work() -> None:
        try:
            result = check_cached()
            logger.info("update check: %s", "update available" if result.available else "up to date")
            print_hint(result, stream)
        except Exception as e:
            logger.debug("update check failed: %s", e)

    t = threading.Thread(target=work, name="update-check", daemon=True)
    t.start()
    if wait > 0:
        t.join(wait)
    return t


def last_check_line() -> str:
    """For `copycord update status`: when the last check ran."""
    try:
        raw = json.loads(cache_path().read_text(encoding="utf-8"))
        ts = float(raw["checked_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return "never"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


# --------------------------------------------------------------------------
# auto-update
# --------------------------------------------------------------------------


def auto_update_once() -> bool:
    """One auto-update cycle; True when an update was applied."""
    if not auto_update_enabled():
        return False
    if ui.is_interactive():
        logger.info("auto-update: skipped in a terminal, hint only")
        return False
    if update.detect_installer() in (update.DOCKER, update.SOURCE):
        logger.info("auto-update: skipped, Copycord is not installed with a tool that can update it")
        return False
    channel = update.load_channel()
    try:
        result = check_cached(channel, force=True)
    except Exception as e:
        logger.warning("auto-update: check failed: %s", e)
        return False
    logger.info("auto-update: check done, %s",
                hint_text(result) if result.available else "up to date")
    if not result.available:
        return False
    logger.info("auto-update: applying %s", result.latest[:12] if result.dev else result.latest)
    rc = update.update()
    if rc != 0:
        logger.error("auto-update: failed (exit %s)", rc)
        return False
    logger.info("auto-update: applied")
    return True


async def auto_update_loop(interval: float = AUTO_UPDATE_INTERVAL) -> None:
    """Runs for the life of the service; the update restarts the service."""
    while True:
        try:
            await asyncio.to_thread(auto_update_once)
        except Exception:
            logger.exception("auto-update: cycle crashed")
        await asyncio.sleep(interval)
