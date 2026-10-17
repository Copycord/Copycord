# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


import contextvars
from typing import Optional

sync_host_name = contextvars.ContextVar("sync_host_name", default=None)
sync_display_id = contextvars.ContextVar("sync_display_id", default=None)
guild_name = contextvars.ContextVar("guild_name", default=None)


def format_prefix() -> str:
    """Log prefix "[<mapping>][<task>]", or "[<host>][<task>]" when no mapping label is set."""
    mapping = guild_name.get()
    host = sync_host_name.get()
    disp = sync_display_id.get()

    parts = []
    if mapping:
        parts.append(f"[{mapping}]")
    elif host:
        parts.append(f"[{host}]")
    if disp:
        parts.append(f"[{disp}]")

    return " ".join(parts) + " " if parts else ""

