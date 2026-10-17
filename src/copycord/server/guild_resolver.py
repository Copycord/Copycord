# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations
from typing import Optional, Set


class GuildResolver:
    def __init__(self, db, config=None):
        self.db = db
        self.config = config

    def clones_for_host(self, host_guild_id: int) -> set[int]:
        try:
            rows = self.db.list_mappings_by_origin(int(host_guild_id)) or []
            active_ids: set[int] = set()

            for r in rows:
                st = str(r.get("status", "active") or "active").strip().lower()
                if st == "paused":
                    continue
                if r.get("cloned_guild_id"):
                    active_ids.add(int(r["cloned_guild_id"]))

            return active_ids
        except Exception:
            return set()

    def is_clone(self, gid: int) -> bool:

        if self.db.is_clone_guild_id(int(gid)):
            return True
        fallback = getattr(self.config, "CLONE_GUILD_ID", None) if self.config else None
        return bool(fallback and int(fallback) == int(gid))

    def resolve_target_clone(
        self,
        *,
        host_guild_id: Optional[int],
        explicit_clone_id: Optional[int] = None,
    ) -> Optional[int]:
        """The clone guild to target: explicit_clone_id when it is one of ours (else None),
        otherwise the active clone of host_guild_id, else CLONE_GUILD_ID if configured."""
        if explicit_clone_id is not None:
            return (
                int(explicit_clone_id)
                if self.is_clone(int(explicit_clone_id))
                else None
            )

        if host_guild_id is not None:
            row = self.db.get_mapping_by_original(int(host_guild_id))
            if row and row.get("cloned_guild_id"):
                st = str(row.get("status", "active") or "active").strip().lower()
                if st != "paused":
                    return int(row["cloned_guild_id"])

        fallback = getattr(self.config, "CLONE_GUILD_ID", None) if self.config else None
        return int(fallback) if fallback else None
