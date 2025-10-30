# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


def resolve_mapping_settings(
    db,
    config,
    *,
    original_guild_id: int | None = None,
    cloned_guild_id: int | None = None
) -> dict:
    base = config.default_mapping_settings()
    m = None
    if original_guild_id:
        m = db.get_mapping_by_original(int(original_guild_id))
    elif cloned_guild_id:
        m = db.get_mapping_by_clone(int(cloned_guild_id))
    if m and isinstance(m.get("settings"), dict):
        base.update(m["settings"])
    return base
