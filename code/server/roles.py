# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================


from __future__ import annotations
import asyncio, logging, discord
from typing import List, Dict, Tuple, Optional
from server.rate_limiter import RateLimitManager, ActionType

logger = logging.getLogger("server.roles")


class RoleManager:
    def __init__(
        self,
        bot: discord.Bot,
        db,
        ratelimit: RateLimitManager,
        clone_guild_id: int,
        delete_roles: bool = True,
        mirror_permissions: bool = True,
    ):
        self.bot = bot
        self.db = db
        self.ratelimit = ratelimit
        self.clone_guild_id = clone_guild_id
        self.delete_roles = delete_roles
        self.mirror_permissions = mirror_permissions
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._last_roles: List[Dict] = []
        self.MAX_ROLES = 250

    def set_last_sitemap(self, roles: List[Dict] | None):
        self._last_roles = roles or []

    def kickoff_sync(self, roles: List[Dict] | None) -> None:
        """Schedule a background role sync if not running."""
        if self._task and not self._task.done():
            logger.debug("Role sync already running; skip kickoff.")
            return
        g = self.bot.get_guild(self.clone_guild_id)
        if not g:
            logger.debug("Role kickoff: clone guild not ready.")
            return
        self._last_roles = roles or []
        logger.debug("[🧩] Role sync task scheduled.")
        self._task = asyncio.create_task(self._run_sync(g, self._last_roles))

    async def _run_sync(self, guild: discord.Guild, incoming: List[Dict]) -> None:
        async with self._lock:
            try:
                deleted, updated, created = await self._sync(guild, incoming)
                parts = []
                if deleted:
                    parts.append(f"Deleted {deleted} roles")
                if updated:
                    parts.append(f"Updated {updated} roles")
                if created:
                    parts.append(f"Created {created} roles")
                if parts:
                    logger.info("[🧩] Role sync changes: " + "; ".join(parts))
                else:
                    logger.debug("[🧩] Role sync: no changes needed")
            except asyncio.CancelledError:
                logger.debug("[🧩] Role sync canceled.")
            except Exception:
                logger.exception("[🧩] Role sync failed")
            finally:
                self._task = None

    async def _recreate_missing_role(
        self,
        *,
        guild: discord.Guild,
        orig_id: int,
        want_name: str,
        want_perms: discord.Permissions,
        want_color: discord.Color,
        want_hoist: bool,
        want_mention: bool,
        can_create: bool,
        create_suppressed_logged: bool,
        clone_by_id: Dict[int, discord.Role],
    ) -> Tuple[Optional[discord.Role], int, bool, bool]:
        """
        Recreate a missing cloned role when a DB mapping exists but the role was deleted.
        Returns (cloned_role, created_delta, can_create_updated, create_suppressed_logged).
        """
        if not can_create:
            if not create_suppressed_logged:
                logger.warning(
                    "[🧩] Can't recreate role %r — guild at max role count (%d).",
                    want_name,
                    self.MAX_ROLES,
                )
                create_suppressed_logged = True
            return None, 0, can_create, create_suppressed_logged

        try:
            await self.ratelimit.acquire(ActionType.ROLE)
            kwargs = dict(
                name=want_name,
                colour=want_color,
                hoist=want_hoist,
                mentionable=want_mention,
                reason="Copycord role sync (recreate missing clone)",
            )
            if self.mirror_permissions:
                kwargs["permissions"] = want_perms

            cloned = await guild.create_role(**kwargs)

            self.db.upsert_role_mapping(orig_id, want_name, cloned.id, cloned.name)
            clone_by_id[cloned.id] = cloned

            logger.info(
                "[🧩] Recreated missing cloned role for upstream %r → %s (%d)",
                want_name,
                cloned.name,
                cloned.id,
            )

            can_create = len(guild.roles) < self.MAX_ROLES
            return cloned, 1, can_create, create_suppressed_logged

        except Exception as e:
            logger.warning(
                "[⚠️] Failed recreating missing cloned role for %r: %s", want_name, e
            )
            return None, 0, can_create, create_suppressed_logged

    async def _sync(
        self, guild: discord.Guild, incoming: List[Dict]
    ) -> Tuple[int, int, int]:
        """
        Mirror roles (name/color/hoist/mentionable + permissions if enabled).
        Skip managed roles and @everyone.
        """
        me = guild.me
        bot_top = me.top_role.position if me and me.top_role else 0

        current = {
            int(r["original_role_id"]): dict(r) for r in self.db.get_all_role_mappings()
        }
        incoming_filtered = {
            int(r["id"]): r
            for r in incoming
            if not r.get("managed") and not r.get("everyone")
        }

        clone_by_id = {r.id: r for r in guild.roles}
        blocked = {int(x) for x in self.db.get_blocked_role_ids()}

        can_create = len(guild.roles) < self.MAX_ROLES
        create_suppressed_logged = False

        deleted = updated = created = 0

        for orig_id in list(current.keys()):
            if orig_id not in incoming_filtered:
                row = current[orig_id]
                cloned_id = row.get("cloned_role_id")
                cloned = clone_by_id.get(int(cloned_id)) if cloned_id else None

                if not self.delete_roles:
                    self.db.delete_role_mapping(orig_id)
                    if cloned:
                        logger.info(
                            "[🧩] Host role deleted; kept cloned role %s (%d), removed mapping.",
                            cloned.name,
                            cloned.id,
                        )
                    else:
                        logger.info(
                            "[🧩] Host role deleted; cloned missing, removed mapping only."
                        )
                    continue

                if (
                    not cloned
                    or cloned.is_default()
                    or cloned.managed
                    or cloned.position >= bot_top
                ):
                    self.db.delete_role_mapping(orig_id)
                    if cloned:
                        logger.info(
                            "[🧩] Skipped deleting role %s (%d); removed mapping.",
                            cloned.name,
                            cloned.id,
                        )
                    else:
                        logger.info("[🧩] Cloned role missing; removed mapping.")
                    continue

                try:
                    await self.ratelimit.acquire(ActionType.ROLE)
                    await cloned.delete()
                    deleted += 1
                    logger.info("[🧩] Deleted role %s (%d)", cloned.name, cloned.id)
                except Exception as e:
                    logger.warning(
                        "[⚠️] Failed deleting role %s (%s); removing mapping anyway: %s",
                        getattr(cloned, "name", "?"),
                        cloned_id,
                        e,
                    )
                finally:
                    self.db.delete_role_mapping(orig_id)

        current = {
            int(r["original_role_id"]): dict(r) for r in self.db.get_all_role_mappings()
        }
        clone_by_id = {r.id: r for r in guild.roles}

        for orig_id, info in incoming_filtered.items():
            mapping = current.get(orig_id)

            cloned = None
            if mapping:
                cloned_id = mapping.get("cloned_role_id")
                if cloned_id:
                    try:
                        cloned = clone_by_id.get(int(cloned_id))
                    except Exception:
                        cloned = None

            if orig_id in blocked:
                if (
                    cloned
                    and (not cloned.is_default())
                    and (not cloned.managed)
                    and cloned.position < bot_top
                ):
                    try:
                        await self.ratelimit.acquire(ActionType.ROLE)
                        await cloned.delete(reason="Blocked by Copycord role blocklist")
                        logger.info(
                            "[🧩] Deleted blocked role %s (%d)", cloned.name, cloned.id
                        )
                    except Exception as e:
                        logger.warning(
                            "[⚠️] Failed deleting blocked role %s: %s",
                            getattr(cloned, "name", "?"),
                            e,
                        )
                if mapping:
                    self.db.delete_role_mapping(orig_id)
                continue

            want_name = info["name"]
            want_perms = discord.Permissions(info.get("permissions", 0))
            want_color = discord.Color(info.get("color", 0))
            want_hoist = bool(info.get("hoist", False))
            want_mention = bool(info.get("mentionable", False))

            if mapping and not cloned:
                cloned, add, can_create, create_suppressed_logged = (
                    await self._recreate_missing_role(
                        guild=guild,
                        orig_id=orig_id,
                        want_name=want_name,
                        want_perms=want_perms,
                        want_color=want_color,
                        want_hoist=want_hoist,
                        want_mention=want_mention,
                        can_create=can_create,
                        create_suppressed_logged=create_suppressed_logged,
                        clone_by_id=clone_by_id,
                    )
                )
                created += add
                if not cloned:

                    continue

            if not mapping:
                if not can_create:
                    if not create_suppressed_logged:
                        logger.warning(
                            "[🧩] Can't create more roles. Guild is at max role count (%d).",
                            self.MAX_ROLES,
                        )
                        create_suppressed_logged = True
                    continue

                try:
                    await self.ratelimit.acquire(ActionType.ROLE)
                    kwargs = dict(
                        name=want_name,
                        colour=want_color,
                        hoist=want_hoist,
                        mentionable=want_mention,
                        reason="Copycord role sync",
                    )
                    if self.mirror_permissions:
                        kwargs["permissions"] = want_perms

                    cloned = await guild.create_role(**kwargs)
                    created += 1

                    self.db.upsert_role_mapping(
                        orig_id, want_name, cloned.id, cloned.name
                    )
                    clone_by_id[cloned.id] = cloned
                    logger.info("[🧩] Created role %s", cloned.name)

                    can_create = len(guild.roles) < self.MAX_ROLES

                    continue
                except Exception as e:
                    logger.warning("[⚠️] Failed creating role %s: %s", want_name, e)
                    continue

            if (
                cloned
                and (not cloned.is_default())
                and (not cloned.managed)
                and cloned.position < bot_top
            ):
                changes: list[str] = []

                if cloned.name != want_name:
                    changes.append(f"name: {cloned.name!r} -> {want_name!r}")

                if self.mirror_permissions and (
                    cloned.permissions.value != want_perms.value
                ):
                    added, removed = self._perm_diff(cloned.permissions, want_perms)
                    parts = []
                    if added:
                        parts.append("+" + ",".join(added))
                    if removed:
                        parts.append("-" + ",".join(removed))
                    changes.append(
                        f"perms: {' '.join(parts) if parts else '(bitfield change)'} "
                        f"({cloned.permissions.value} -> {want_perms.value})"
                    )
                elif (not self.mirror_permissions) and (
                    cloned.permissions.value != want_perms.value
                ):
                    logger.debug(
                        "[🧩] permissions differ for %s (%d) but MIRROR_ROLE_PERMISSIONS=False; skipping perms update.",
                        cloned.name,
                        cloned.id,
                    )

                old_color = self._color_int(cloned.color)
                new_color = self._color_int(want_color)
                if old_color != new_color:
                    changes.append(f"color: #{old_color:06X} -> #{new_color:06X}")

                if cloned.hoist != want_hoist:
                    changes.append(f"hoist: {cloned.hoist} -> {want_hoist}")

                if cloned.mentionable != want_mention:
                    changes.append(
                        f"mentionable: {cloned.mentionable} -> {want_mention}"
                    )

                if changes:
                    logger.debug(
                        "[🧩] update details for %s (%d): %s",
                        cloned.name,
                        cloned.id,
                        "; ".join(changes),
                    )
                    try:
                        await self.ratelimit.acquire(ActionType.ROLE)
                        kwargs = dict(
                            name=want_name,
                            colour=want_color,
                            hoist=want_hoist,
                            mentionable=want_mention,
                            reason="Copycord role sync",
                        )
                        if self.mirror_permissions:
                            kwargs["permissions"] = want_perms

                        await cloned.edit(**kwargs)
                        updated += 1
                        self.db.upsert_role_mapping(
                            orig_id, want_name, cloned.id, cloned.name
                        )
                        logger.info("[🧩] Updated role %s", cloned.name)
                    except Exception as e:
                        logger.warning(
                            "[⚠️] Failed updating role %s: %s", cloned.name, e
                        )

        return deleted, updated, created

    def _color_int(self, c) -> int:
        try:
            return int(c.value)
        except Exception:
            return int(c)

    def _perm_diff(
        self, before_perm: discord.Permissions, after_perm: discord.Permissions
    ) -> tuple[list[str], list[str]]:
        """Return (added_flags, removed_flags) between two Permissions."""
        added, removed = [], []

        for name, new in after_perm:
            old = getattr(before_perm, name)
            if new and not old:
                added.append(name)
            elif old and not new:
                removed.append(name)
        return added, removed
