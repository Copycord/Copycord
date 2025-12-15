# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
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
    """
    Build a prefix like:
      - "[<mapping>][<task>]" when a per-clone mapping label is set, else
      - "[<host>][<task>]" as a fallback.
    """
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


def guild_prefix() -> str:
    """
    Returns something like "[Guild A] " if a guild_name is set
    for this task/context, else "".
    """
    g = guild_name.get()
    if g:
        return f"[{g}] "
    return ""
