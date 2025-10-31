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
    Keep your existing sync prefix helper. We are NOT touching it here.
    This is still used for sync/structure stuff.
    """
    host = sync_host_name.get()
    disp = sync_display_id.get()

    parts = []
    if host:
        parts.append(f"[{host}]")
    if disp:
        parts.append(f"[{disp}]")

    if parts:
        return " ".join(parts) + " "
    return ""

def guild_prefix() -> str:
    """
    Returns something like "[Guild A] " if a guild_name is set
    for this task/context, else "".
    """
    g = guild_name.get()
    if g:
        return f"[{g}] "
    return ""
