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


import asyncio, logging, discord
from typing import List, Dict, Tuple, Optional
from server.rate_limiter import RateLimitManager, ActionType
from server import logctx

logger = logging.getLogger("server.roles")


class RoleManager:
    def __init__(
        self,
        bot,
        db,
        guild_resolver,
        ratelimit,
        clone_guild_id: int | None = None,
        delete_roles: bool | None = None,
        mirror_permissions: bool | None = None,
    ):
        self.bot = bot
        self.db = db
        self.guild_resolver = guild_resolver
        self.ratelimit = ratelimit
        self.clone_guild_id = int(clone_guild_id or 0)
        self.delete_roles = bool(delete_roles) if delete_roles is not None else False
        self.mirror_permissions = (
            bool(mirror_permissions) if mirror_permissions is not None else False
        )

        self._tasks: dict[int, asyncio.Task] = {}
        self._locks: dict[int, asyncio.Lock] = {}

        self.MAX_ROLES = 250

    def _log(self, level: str, msg: str, *args) -> None:
        """
        Role sync logging with sync task context.
        Adds the same prefix that structure sync uses so every
        role create/update/delete line is tied to the sync task id.
        """
        prefix = logctx.format_prefix()

        if level == "info":
            logger.info(prefix + msg, *args)
        elif level == "warning":
            logger.warning(prefix + msg, *args)
        elif level == "error":
            logger.error(prefix + msg, *args)
        else:
            logger.debug(prefix + msg, *args)

    def _get_lock_for_clone(self, clone_gid: int) -> asyncio.Lock:
        """
        Get/create a lock dedicated to this clone guild.
        Prevents overlapping role modifications *within the same* clone guild,
        but allows different clone guilds to sync in parallel.
        """
        if clone_gid not in self._locks:
            self._locks[clone_gid] = asyncio.Lock()
        return self._locks[clone_gid]

    def kickoff_sync(
        self,
        roles,
        host_guild_id,
        target_clone_guild_id,
        *,
        validate_mapping: bool = True,
        delete_roles: bool | None = None,
        mirror_permissions: bool | None = None,
    ) -> None:
        clone_gid = int(target_clone_guild_id or self.clone_guild_id)

        existing_task = self._tasks.get(clone_gid)
        if existing_task and not existing_task.done():
            logger.debug(
                "[roles] Sync already running for clone %s; skip kickoff.", clone_gid
            )
            return

        clone_guild = self.bot.get_guild(clone_gid)
        if not clone_guild:
            logger.debug("[roles] kickoff: clone guild %s not ready.", clone_gid)
            return

        if validate_mapping and self.guild_resolver and host_guild_id:
            try:
                clones = set(self.guild_resolver.clones_for_host(int(host_guild_id)))
                if clones and clone_gid not in clones:
                    self._log(
                        "warning",
                        "[roles] host %s is not mapped to clone %s; proceeding anyway",
                        host_guild_id,
                        clone_gid,
                    )
            except Exception:
                pass

        incoming_roles = roles or []
        host_id_int = int(host_guild_id) if host_guild_id else None

        eff_delete_roles = (
            self.delete_roles if delete_roles is None else bool(delete_roles)
        )
        eff_mirror_perms = (
            self.mirror_permissions
            if mirror_permissions is None
            else bool(mirror_permissions)
        )

        logger.debug(
            "[🧩] Scheduling role sync task host=%s → clone=%s (delete_roles=%s mirror_perms=%s)",
            host_id_int,
            clone_gid,
            eff_delete_roles,
            eff_mirror_perms,
        )

        task = asyncio.create_task(
            self._run_sync(
                guild=clone_guild,
                incoming=incoming_roles,
                host_id=host_id_int,
                clone_id=clone_gid,
                delete_roles=eff_delete_roles,
                mirror_permissions=eff_mirror_perms,
            )
        )

        self._tasks[clone_gid] = task
        return task

    async def _run_sync(
        self,
        *,
        guild: discord.Guild,
        incoming: List[Dict],
        host_id: int | None,
        clone_id: int,
        delete_roles: bool,
        mirror_permissions: bool,
    ) -> None:
        lock = self._get_lock_for_clone(clone_id)
        async with lock:
            try:
                deleted, updated, created = await self._sync(
                    guild=guild,
                    incoming=incoming,
                    host_id=host_id,
                    clone_id=clone_id,
                    delete_roles=delete_roles,
                    mirror_permissions=mirror_permissions,
                )

                parts = []
                if deleted:
                    parts.append(f"Deleted {deleted} roles")
                if updated:
                    parts.append(f"Updated {updated} roles")
                if created:
                    parts.append(f"Created {created} roles")

                if parts:
                    self._log(
                        "info",
                        "[🧩] Role sync complete: %s",
                        "; ".join(parts),
                    )
                else:
                    self._log(
                        "info",
                        "[🧩] Role sync complete: no changes needed",
                    )

            except asyncio.CancelledError:
                self._log("warning", "[🧩] Role sync canceled.")
            except Exception as e:
                self._log("error", "[🧩] Role sync failed: %s", e)
            finally:
                t = self._tasks.get(clone_id)
                if t and t.done():

                    pass
                else:

                    self._tasks.pop(clone_id, None)

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
        original_guild_id: int | None,
        cloned_guild_id: int | None,
        mirror_permissions: bool,
    ) -> Tuple[Optional[discord.Role], int, bool, bool]:
        """
        (unchanged logic, still per-clone safe)
        Recreate a missing cloned role when DB mapping exists but the role was deleted.
        """
        if not can_create:
            if not create_suppressed_logged:
                self._log(
                    "warning",
                    "[🧩] Can't recreate role %r — guild at max role count (%d).",
                    want_name,
                    self.MAX_ROLES,
                )
                create_suppressed_logged = True
            return None, 0, can_create, create_suppressed_logged

        try:
            await self.ratelimit.acquire_for_guild(ActionType.ROLE, cloned_guild_id)
            kwargs = dict(
                name=want_name,
                colour=want_color,
                hoist=want_hoist,
                mentionable=want_mention,
                reason="Copycord role sync (recreate missing clone)",
            )
            if mirror_permissions:
                kwargs["permissions"] = want_perms

            cloned = await guild.create_role(**kwargs)

            self.db.upsert_role_mapping(
                orig_id,
                want_name,
                cloned.id,
                cloned.name,
                original_guild_id=original_guild_id,
                cloned_guild_id=cloned_guild_id,
            )

            clone_by_id[cloned.id] = cloned

            self._log(
                "info",
                "[🧩] Recreated missing cloned role for upstream %r → %s (%d)",
                want_name,
                cloned.name,
                cloned.id,
            )

            can_create = len(guild.roles) < self.MAX_ROLES
            return cloned, 1, can_create, create_suppressed_logged

        except Exception as e:
            self._log(
                "warning",
                "[⚠️] Failed recreating missing cloned role for %r: %s",
                want_name,
                e,
            )
            return None, 0, can_create, create_suppressed_logged

    async def _sync(
        self,
        *,
        guild: discord.Guild,
        incoming: List[Dict],
        host_id: int | None,
        clone_id: int,
        delete_roles: bool,
        mirror_permissions: bool,
    ) -> Tuple[int, int, int]:
        """
        Mirror roles (name/color/hoist/mentionable + permissions if enabled)
        from a single host guild into a single clone guild.

        This version is multi-guild safe:
        - it only reads mappings for (host_id, clone_id)
        - it does not rely on shared state on self
        """
        me = guild.me
        bot_top = me.top_role.position if me and me.top_role else 0

        rows = self.db.get_all_role_mappings()
        current: dict[int, dict] = {}
        for r in rows:
            row = dict(r)

            if (
                host_id is None
                or int(row.get("original_guild_id") or 0) == int(host_id)
            ) and int(row.get("cloned_guild_id") or 0) == int(clone_id):
                current[int(row["original_role_id"])] = row

        incoming_filtered = {
            int(r["id"]): r
            for r in incoming
            if not r.get("managed") and not r.get("everyone")
        }

        clone_by_id = {r.id: r for r in guild.roles}
        blocked = {int(x) for x in self.db.get_blocked_role_ids(clone_id)}

        can_create = len(guild.roles) < self.MAX_ROLES
        create_suppressed_logged = False

        deleted = updated = created = 0

        for orig_id in list(current.keys()):
            if orig_id not in incoming_filtered:
                row = current[orig_id]
                cloned_id = row.get("cloned_role_id")
                cloned_role = clone_by_id.get(int(cloned_id)) if cloned_id else None

                if not delete_roles:

                    self.db.delete_role_mapping_for_clone(orig_id, clone_id)
                    if cloned_role:
                        self._log(
                            "info",
                            "[🧩] Host role deleted; kept cloned role %s (%d), removed mapping.",
                            cloned_role.name,
                            cloned_role.id,
                        )
                    else:
                        self._log(
                            "info",
                            "[🧩] Host role deleted; cloned missing, removed mapping only."
                        )
                    continue

                if (
                    not cloned_role
                    or cloned_role.is_default()
                    or cloned_role.managed
                    or cloned_role.position >= bot_top
                ):

                    self.db.delete_role_mapping_for_clone(orig_id, clone_id)
                    if cloned_role:
                        self._log(
                            "info",
                            "[🧩] Skipped deleting role %s (%d); removed mapping.",
                            cloned_role.name,
                            cloned_role.id,
                        )
                    else:
                        self._log(
                            "info",
                            "[🧩] Cloned role missing; removed mapping."
                        )
                    continue

                try:
                    await self.ratelimit.acquire_for_guild(ActionType.ROLE, clone_id)
                    await cloned_role.delete()
                    deleted += 1
                    self._log(
                        "info",
                        "[🧩] Deleted role %s (%d)",
                        cloned_role.name,
                        cloned_role.id,
                    )

                except Exception as e:
                    self._log(
                        "warning",
                        "[⚠️] Failed deleting role %s (%s); removing mapping anyway: %s",
                        getattr(cloned_role, "name", "?"),
                        cloned_id,
                        e,
                    )
                finally:
                    self.db.delete_role_mapping_for_clone(orig_id, clone_id)

        rows = self.db.get_all_role_mappings()
        current: dict[int, dict] = {}
        for r in rows:
            row = dict(r)

            if (
                host_id is None
                or int(row.get("original_guild_id") or 0) == int(host_id)
            ) and int(row.get("cloned_guild_id") or 0) == int(clone_id):
                current[int(row["original_role_id"])] = row

        clone_by_id = {r.id: r for r in guild.roles}

        for orig_id, info in incoming_filtered.items():
            mapping = current.get(orig_id)

            cloned_role = None
            if mapping:
                cloned_id = mapping.get("cloned_role_id")
                if cloned_id:
                    try:
                        cloned_role = clone_by_id.get(int(cloned_id))
                    except Exception:
                        cloned_role = None

            if orig_id in blocked:
                if (
                    cloned_role
                    and (not cloned_role.is_default())
                    and (not cloned_role.managed)
                    and cloned_role.position < bot_top
                ):
                    try:
                        await self.ratelimit.acquire_for_guild(
                            ActionType.ROLE, clone_id
                        )
                        await cloned_role.delete(
                            reason="Blocked by Copycord role blocklist"
                        )
                        self._log(
                            "info",
                            "[🧩] Deleted blocked role %s (%d)",
                            cloned_role.name,
                            cloned_role.id,
                        )
                    except Exception as e:
                        self._log(
                            "warning",
                            "[⚠️] Failed deleting blocked role %s: %s",
                            getattr(cloned_role, "name", "?"),
                            e,
                        )
                if mapping:
                    self.db.delete_role_mapping_for_clone(orig_id, clone_id)
                continue

            want_name = info["name"]
            want_perms = discord.Permissions(info.get("permissions", 0))
            want_color = discord.Color(info.get("color", 0))
            want_hoist = bool(info.get("hoist", False))
            want_mention = bool(info.get("mentionable", False))

            if mapping and not cloned_role:
                cloned_role, add, can_create, create_suppressed_logged = (
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
                        original_guild_id=host_id,
                        cloned_guild_id=clone_id,
                        mirror_permissions=mirror_permissions,
                    )
                )
                created += add
                if not cloned_role:
                    continue

            if not mapping:
                if not can_create:
                    if not create_suppressed_logged:
                        self._log(
                            "warning",
                            "[🧩] Can't create more roles. Guild is at max role count (%d).",
                            self.MAX_ROLES,
                        )
                        create_suppressed_logged = True
                    continue

                try:
                    await self.ratelimit.acquire_for_guild(ActionType.ROLE, clone_id)
                    kwargs = dict(
                        name=want_name,
                        colour=want_color,
                        hoist=want_hoist,
                        mentionable=want_mention,
                        reason="Copycord role sync",
                    )
                    if mirror_permissions:
                        kwargs["permissions"] = want_perms

                    new_role = await guild.create_role(**kwargs)
                    created += 1

                    self.db.upsert_role_mapping(
                        orig_id,
                        want_name,
                        new_role.id,
                        new_role.name,
                        original_guild_id=host_id,
                        cloned_guild_id=clone_id,
                    )

                    clone_by_id[new_role.id] = new_role
                    self._log(
                        "info",
                        "[🧩] Created role %s",
                        new_role.name,
                    )

                    can_create = len(guild.roles) < self.MAX_ROLES
                    continue

                except Exception as e:
                    self._log(
                        "warning",
                        "[⚠️] Failed creating role %s: %s",
                        want_name,
                        e,
                    )
                    continue

            if (
                cloned_role
                and (not cloned_role.is_default())
                and (not cloned_role.managed)
                and cloned_role.position < bot_top
            ):
                changes: list[str] = []

                if cloned_role.name != want_name:
                    changes.append(f"name: {cloned_role.name!r} -> {want_name!r}")

                if mirror_permissions and (
                    cloned_role.permissions.value != want_perms.value
                ):
                    added_flags, removed_flags = self._perm_diff(
                        cloned_role.permissions,
                        want_perms,
                    )
                    parts = []
                    if added_flags:
                        parts.append("+" + ",".join(added_flags))
                    if removed_flags:
                        parts.append("-" + ",".join(removed_flags))
                    changes.append(
                        "perms: "
                        + (" ".join(parts) if parts else "(bitfield change)")
                        + f" ({cloned_role.permissions.value} -> {want_perms.value})"
                    )
                elif (not self.mirror_permissions) and (
                    cloned_role.permissions.value != want_perms.value
                ):
                    logger.debug(
                        "[🧩] permissions differ for %s (%d) but MIRROR_ROLE_PERMISSIONS=False; skipping perms update.",
                        cloned_role.name,
                        cloned_role.id,
                    )

                old_color = self._color_int(cloned_role.color)
                new_color = self._color_int(want_color)
                if old_color != new_color:
                    changes.append(f"color: #{old_color:06X} -> #{new_color:06X}")

                if cloned_role.hoist != want_hoist:
                    changes.append(f"hoist: {cloned_role.hoist} -> {want_hoist}")

                if cloned_role.mentionable != want_mention:
                    changes.append(
                        f"mentionable: {cloned_role.mentionable} -> {want_mention}"
                    )

                if changes:
                    self._log(
                        "debug",
                        "[🧩] update details for %s (%d): %s",
                        cloned_role.name,
                        cloned_role.id,
                        "; ".join(changes),
                    )
                    try:
                        await self.ratelimit.acquire_for_guild(
                            ActionType.ROLE, clone_id
                        )
                        kwargs = dict(
                            name=want_name,
                            colour=want_color,
                            hoist=want_hoist,
                            mentionable=want_mention,
                            reason="Copycord role sync",
                        )
                        if mirror_permissions:
                            kwargs["permissions"] = want_perms

                        await cloned_role.edit(**kwargs)
                        updated += 1

                        self.db.upsert_role_mapping(
                            orig_id,
                            want_name,
                            cloned_role.id,
                            cloned_role.name,
                            original_guild_id=host_id,
                            cloned_guild_id=clone_id,
                        )
                        self._log(
                            "info",
                            "[🧩] Updated role %s",
                            cloned_role.name,
                        )

                    except Exception as e:
                        self._log(
                            "warning",
                            "[⚠️] Failed updating role %s: %s",
                            getattr(cloned_role, "name", "?"),
                            e,
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
