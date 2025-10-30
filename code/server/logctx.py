# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import contextvars

# Who/what is being synced
sync_host_name = contextvars.ContextVar("sync_host_name", default=None)
sync_display_id = contextvars.ContextVar("sync_display_id", default=None)

def format_prefix() -> str:
    """
    Returns a standard prefix like:
    "[🛠️] [CC-Testing-Client] [khp6n] "
    or falls back gracefully if we only have one of them.
    """
    host = sync_host_name.get()
    tag = sync_display_id.get()

    if host and tag:
        return f"[🛠️] [{host}] [{tag}] "
    if host:
        return f"[{host}] "
    if tag:
        return f"[{tag}] "
    return ""
