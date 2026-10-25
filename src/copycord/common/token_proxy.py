# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
"""Proxies are mandatory for user tokens.

A user token never talks to Discord (REST or gateway) from the server's own
address. With no proxy lease for a token, no request goes out for it.
``USER_TOKEN_USE_PROXIES`` stays readable in stored settings so old databases
load, but its value is ignored.
"""
from __future__ import annotations

import time

# Always on; kept as a name so every call site reads the same rule.
USE_PROXY = True

PROXIES_REQUIRED_ERROR = "Proxies are required for user tokens. Add proxies first."

_WARN_INTERVAL = 60.0
# total time lease_with_timezone spends rotating before the token counts as unavailable
ROTATE_TIME_CAP = 30.0
_last_warned: dict[str, float] = {}


class NoProxyAvailable(RuntimeError):
    """A user token has no proxy lease, so it must not make a request."""


async def lease_with_timezone(pool, token: str, *, time_cap: float = ROTATE_TIME_CAP) -> str:
    """The token's proxy once its exit timezone resolves, rotating past proxies whose lookup fails.

    Each failed proxy goes to ``pool.report_failure`` (its health logic, then a
    lease that excludes it). Tries at most one proxy per pool entry and stops
    after ``time_cap`` seconds; then the lease is released and NoProxyAvailable
    is raised, so nothing is sent with a fallback timezone.
    """
    from copycord.common.exit_timezone import note_token_proxy

    proxy = pool.current(token)
    if not proxy:
        if not getattr(pool, "enabled", False):
            raise NoProxyAvailable("no proxies configured")
        proxy = await pool.lease(token)
    deadline = time.monotonic() + time_cap
    tried: set = set()
    for _ in range(max(1, int(getattr(pool, "total", 1) or 1))):
        if not proxy:
            raise NoProxyAvailable("no free proxy for this token")
        if await note_token_proxy(token, proxy):
            return proxy
        tried.add(proxy)
        proxy = await pool.report_failure(token, exclude=tried)
        if time.monotonic() >= deadline:
            break
    if proxy:
        await pool.release(token)
    raise NoProxyAvailable("no proxy with a resolvable exit timezone for this token")


def token_use_proxy(settings=None) -> bool:
    """Whether token requests go through a proxy: always, whatever the stored setting says."""
    return USE_PROXY


def proxies_configured(pool=None) -> bool:
    """True when ``pool`` (the shared proxy pool by default) has at least one proxy loaded."""
    try:
        if pool is None:
            from copycord.common.proxy_pool import get_pool

            pool = get_pool()
        return bool(pool.enabled)
    except Exception:
        return False


TOKENS_DISABLED_NOTICE = (
    "Token message cloning is disabled: no proxies are configured. "
    "Add proxies in the Proxy settings to enable it."
)
TOKENS_ENABLED_NOTICE = "Proxies found: token message cloning is enabled."
# Set once the disabled notice is logged; cleared when proxies come back.
_tokens_disabled_noticed = False


def notice_tokens_disabled(logger) -> bool:
    """Log the disabled notice once until proxies come back. True when it logged."""
    global _tokens_disabled_noticed
    if _tokens_disabled_noticed:
        return False
    _tokens_disabled_noticed = True
    logger.warning(TOKENS_DISABLED_NOTICE)
    return True


def notice_tokens_enabled(logger) -> bool:
    """Log the enabled line once if the disabled notice was logged, and clear it. True when it logged."""
    global _tokens_disabled_noticed
    if not _tokens_disabled_noticed:
        return False
    _tokens_disabled_noticed = False
    logger.info(TOKENS_ENABLED_NOTICE)
    return True


def warn_no_proxy(logger, token_ref: str, ctx: str = "", *, pool=None) -> bool:
    """Log one warning per masked token id per minute. True when it logged.

    With no proxies configured at all this logs the one disabled notice
    instead, never a line per token.
    """
    if not proxies_configured(pool):
        return notice_tokens_disabled(logger)
    now = time.monotonic()
    last = _last_warned.get(token_ref)
    if last is not None and now - last < _WARN_INTERVAL:
        return False
    _last_warned[token_ref] = now
    logger.warning(
        "[user-send] No proxy available for token %s%s; not sending with it",
        token_ref,
        f" ({ctx})" if ctx else "",
    )
    return True
