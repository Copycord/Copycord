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
_last_warned: dict[str, float] = {}


class NoProxyAvailable(RuntimeError):
    """A user token has no proxy lease, so it must not make a request."""


def token_use_proxy(settings=None) -> bool:
    """Whether token requests go through a proxy: always, whatever the stored setting says."""
    return USE_PROXY


def proxies_configured() -> bool:
    """True when the proxy pool has at least one proxy loaded."""
    from copycord.common.proxy_pool import get_pool

    try:
        return bool(get_pool().enabled)
    except Exception:
        return False


def warn_no_proxy(logger, token_ref: str, ctx: str = "") -> bool:
    """Log one warning per masked token id per minute. True when it logged."""
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
