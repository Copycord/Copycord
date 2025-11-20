# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
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

    def all_clone_guild_ids(self) -> Set[int]:
        ids = set(int(x) for x in (self.db.get_all_clone_guild_ids() or []))

        fallback = getattr(self.config, "CLONE_GUILD_ID", None) if self.config else None
        if not ids and fallback:
            ids.add(int(fallback))
        return ids

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

    def originals_for_clone(self, clone_guild_id: int) -> Set[int]:
        row = self.db.get_mapping_by_clone(int(clone_guild_id))
        return (
            {int(row["original_guild_id"])}
            if row and row.get("original_guild_id")
            else set()
        )

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
        """
        Prefer an explicit clone id if it's one of our clones, otherwise map host->clone.
        Finally, fall back to single-guild CLONE_GUILD_ID if configured.
        """
        if explicit_clone_id is not None:
            return (
                int(explicit_clone_id)
                if self.is_clone(int(explicit_clone_id))
                else None
            )

        if host_guild_id is not None:
            # Prefer an active mapping
            row = self.db.get_mapping_by_original(int(host_guild_id))
            if row and row.get("cloned_guild_id"):
                st = str(row.get("status", "active") or "active").strip().lower()
                if st != "paused":
                    return int(row["cloned_guild_id"])

        fallback = getattr(self.config, "CLONE_GUILD_ID", None) if self.config else None
        return int(fallback) if fallback else None
