# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
"""How each bot appears on Discord: the SERVER_STATUS and CLIENT_STATUS keys."""

import sqlite3

STATUS_KEYS = ("SERVER_STATUS", "CLIENT_STATUS")
STATUSES = ("online", "idle", "dnd", "invisible")
DEFAULT_STATUS = "online"


def normalize_status(value) -> str:
    """A known status, lowercased; anything else is online."""
    v = str(value or "").strip().lower()
    return v if v in STATUSES else DEFAULT_STATUS


def read_status(db, key: str) -> str:
    """The saved status for ``key``, online when unset or unreadable."""
    if db is None:
        return DEFAULT_STATUS
    try:
        return normalize_status(db.get_config(key, DEFAULT_STATUS))
    except (OSError, ValueError, RuntimeError, sqlite3.Error):
        return DEFAULT_STATUS
