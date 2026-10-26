# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
"""Proxies for user tokens: recommended, off by default, chosen per mapping.

``USER_TOKEN_USE_PROXIES`` (default off) decides it per mapping. A token used by
several mappings goes through a proxy if any of them has it on. A proxied token
never falls back to the server's own address: with no proxy lease, no request
goes out for it. With the setting off, that mapping's tokens talk to Discord
(REST and gateway) directly and never ask the proxy pool for a lease.
"""
from __future__ import annotations

import logging
import time

_logger = logging.getLogger("common.token_proxy")

# The default use_proxy for internal callers that are not told otherwise.
USE_PROXY = True
# The default when a mapping has no stored USER_TOKEN_USE_PROXIES value.
USE_PROXIES_DEFAULT = False

PROXIES_REQUIRED_ERROR = (
    "No proxies are configured. Add proxies, or turn off Send through proxies for this mapping."
)

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
    """Whether a mapping's token requests go through a proxy.

    ``USER_TOKEN_USE_PROXIES`` from ``settings``; missing or None means off.
    """
    if not settings:
        return USE_PROXIES_DEFAULT
    value = settings.get("USER_TOKEN_USE_PROXIES", USE_PROXIES_DEFAULT)
    if value is None:
        return USE_PROXIES_DEFAULT
    if isinstance(value, str):
        return value.strip().lower() not in ("0", "false", "no", "off", "")
    return bool(value)


# Token values every mapping using them has proxies off, from the server's last
# plan. Empty until the first plan, so until then every token is proxied.
_direct_tokens: frozenset = frozenset()


def set_direct_tokens(values) -> None:
    """Publish the token values allowed to go direct (no mapping using them wants proxies)."""
    global _direct_tokens
    _direct_tokens = frozenset(v for v in (values or ()) if v)


def resolve_use_proxy(token: str, use_proxy: bool = USE_PROXY) -> bool:
    """Whether this request for ``token`` goes through a proxy.

    A token goes direct only when the caller's mapping has proxies off and so
    does every other mapping using the token: if any of them wants proxies, the
    token uses one.
    """
    return bool(use_proxy) or token not in _direct_tokens


def proxies_configured(pool=None) -> bool:
    """True when ``pool`` (the shared proxy pool by default) has at least one proxy loaded."""
    try:
        if pool is None:
            from copycord.common.proxy_pool import get_pool

            pool = get_pool()
        return bool(pool.enabled)
    except Exception:
        _logger.debug("could not read the proxy pool state", exc_info=True)
        return False


TOKENS_DISABLED_NOTICE = (
    "Token message cloning is paused for mappings with Send through proxies on: no proxies "
    "are configured. Proxies are recommended: add them in the Proxy settings, or turn off "
    "Send through proxies on a mapping to send from this server's own IP."
)
TOKENS_ENABLED_NOTICE = (
    "Proxies found: token message cloning is enabled for mappings with Send through proxies on."
)
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
