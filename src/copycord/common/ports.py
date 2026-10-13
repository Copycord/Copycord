# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Default ports and the dashboard listen address, in one place."""

from __future__ import annotations

import os
from typing import Mapping, Optional

DEFAULT_ADMIN_PORT = 8080
DEFAULT_SERVER_WS_PORT = 8765
DEFAULT_CLIENT_WS_PORT = 8766
DEFAULT_SERVER_CONTROL_PORT = 9101
DEFAULT_CLIENT_CONTROL_PORT = 9102

LOOPBACK = "127.0.0.1"

# The address the dashboard listens on: 127.0.0.1 is this computer only,
# 0.0.0.0 every network, or one IP of this computer.
DASHBOARD_HOST_ENV = "DASHBOARD_HOST"
DEFAULT_DASHBOARD_HOST = LOOPBACK

# Set to 1 to listen beyond this computer with no or the default password.
ALLOW_INSECURE_ENV = "COPYCORD_ALLOW_INSECURE_DASHBOARD"

_WILDCARDS = ("0.0.0.0", "::", "[::]")


def dashboard_host(env: Optional[Mapping[str, str]] = None) -> str:
    env = os.environ if env is None else env
    return (env.get(DASHBOARD_HOST_ENV) or "").strip() or DEFAULT_DASHBOARD_HOST


def is_loopback(host: str) -> bool:
    return host in ("localhost", "::1", "[::1]") or host.startswith("127.")


def agent_host(listen: str) -> str:
    """Where the in-process roles reach the dashboard: loopback unless it listens on one other address."""
    if listen in _WILDCARDS or is_loopback(listen):
        return LOOPBACK
    return listen


def browser_host(listen: str) -> str:
    """The host to print in the dashboard link."""
    if listen in _WILDCARDS or is_loopback(listen):
        return "localhost"
    return f"[{listen}]" if ":" in listen and not listen.startswith("[") else listen


def insecure_dashboard(env: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """A message when the dashboard would be reachable from the network with a weak password, else None."""
    from copycord.envfile import DEFAULT_PASSWORD

    env = os.environ if env is None else env
    host = dashboard_host(env)
    if is_loopback(host):
        return None
    if (env.get(ALLOW_INSECURE_ENV) or "").strip() == "1":
        return None
    password = (env.get("PASSWORD") or "").strip()
    if password and password != DEFAULT_PASSWORD:
        return None
    why = "no password" if not password else "the default password"
    return (
        f"{DASHBOARD_HOST_ENV}={host} makes the dashboard reachable from other computers, "
        f"but it has {why}. Set your own PASSWORD, or remove {DASHBOARD_HOST_ENV} so the "
        f"dashboard stays on this computer only (http://localhost)."
    )
