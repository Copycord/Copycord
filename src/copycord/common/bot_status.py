# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
"""How each bot appears on Discord: the SERVER_STATUS and CLIENT_STATUS keys."""

STATUS_KEYS = ("SERVER_STATUS", "CLIENT_STATUS")
STATUSES = ("online", "idle", "dnd", "invisible")
DEFAULT_STATUS = "online"


def normalize_status(value) -> str:
    """A known status, lowercased; anything else is online."""
    v = str(value or "").strip().lower()
    return v if v in STATUSES else DEFAULT_STATUS


def read_status(db, key: str) -> str:
    """The saved status for ``key``, online when unset or unreadable."""
    try:
        return normalize_status(db.get_config(key, DEFAULT_STATUS))
    except Exception:
        return DEFAULT_STATUS
