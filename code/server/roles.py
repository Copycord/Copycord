from __future__ import annotations
import asyncio, logging, discord
from typing import List, Dict, Tuple
from server.rate_limiter import RateLimitManager, ActionType

logger = logging.getLogger("server.roles")


class RoleManager:
    def __init__(
        self,
        bot: discord.Bot,
        db,
        ratelimit: RateLimitManager,
        clone_guild_id: int,
    ):
        self.bot = bot
        self.db = db
        self.ratelimit = ratelimit
        self.clone_guild_id = clone_guild_id
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._last_roles: List[Dict] = []

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
                d, u, c, reord = await self._sync(guild, incoming)
                parts = []
                if d:
                    parts.append(f"Deleted {d} roles")
                if u:
                    parts.append(f"Updated {u} roles")
                if c:
                    parts.append(f"Created {c} roles")
                if reord:
                    parts.append("Reordered roles")
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

    async def _sync(
        self, guild: discord.Guild, incoming: List[Dict]
    ) -> Tuple[int, int, int, bool]:
        """
        Mirror roles (name/perms/color/hoist/mentionable + order).
        Skip managed roles and @everyone.
        """
        me = guild.me
        bot_top = me.top_role.position if me and me.top_role else 0

        # Map in DB
        current = {r["original_role_id"]: r for r in self.db.get_all_role_mappings()}
        incoming_filtered = {
            r["id"]: r
            for r in incoming
            if not r.get("managed") and not r.get("everyone")
        }

        clone_by_id = {r.id: r for r in guild.roles}

        deleted = updated = created = 0

        for orig_id in list(current.keys()):
            if orig_id not in incoming_filtered:
                row = current[orig_id]
                cloned_id = row.get("cloned_role_id")
                cloned = clone_by_id.get(int(cloned_id), None) if cloned_id else None
                if (
                    not cloned
                    or cloned.is_default()
                    or cloned.managed
                    or cloned.position >= bot_top
                ):
                    self.db.delete_role_mapping(orig_id)
                    continue
                try:
                    await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                    await cloned.delete()
                    deleted += 1
                    logger.info("[🧩] Deleted role %s (%d)", cloned.name, cloned.id)
                except Exception as e:
                    logger.warning(
                        "[⚠️] Failed deleting role %s: %s",
                        getattr(cloned, "name", cloned_id),
                        e,
                    )
                finally:
                    self.db.delete_role_mapping(orig_id)

        current = {r["original_role_id"]: r for r in self.db.get_all_role_mappings()}
        clone_by_id = {r.id: r for r in guild.roles}

        for orig_id, info in incoming_filtered.items():
            want_name = info["name"]
            want_perms = discord.Permissions(info.get("permissions", 0))
            want_color = discord.Color(info.get("color", 0))
            want_hoist = bool(info.get("hoist", False))
            want_mention = bool(info.get("mentionable", False))

            mapping = current.get(orig_id)
            cloned = None
            if mapping:
                cloned = clone_by_id.get(int(mapping["cloned_role_id"]))

            if mapping and not cloned:
                self.db.delete_role_mapping(orig_id)
                mapping = None

            if not mapping:
                try:
                    await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                    cloned = await guild.create_role(
                        name=want_name,
                        permissions=want_perms,
                        colour=want_color,
                        hoist=want_hoist,
                        mentionable=want_mention,
                        reason="Copycord role sync",
                    )
                    created += 1
                    self.db.upsert_role_mapping(
                        orig_id, want_name, cloned.id, cloned.name
                    )
                    clone_by_id[cloned.id] = cloned
                    logger.info("[🧩] Created role %s", cloned.name)
                except Exception as e:
                    logger.warning("[⚠️] Failed creating role %s: %s", want_name, e)
                continue

            if (
                cloned
                and (not cloned.is_default())
                and (not cloned.managed)
                and cloned.position < bot_top
            ):
                need_edit = (
                    cloned.name != want_name
                    or cloned.permissions.value != want_perms.value
                    or cloned.color.value != want_color.value
                    or cloned.hoist != want_hoist
                    or cloned.mentionable != want_mention
                )
                if need_edit:
                    try:
                        await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                        await cloned.edit(
                            name=want_name,
                            permissions=want_perms,
                            colour=want_color,
                            hoist=want_hoist,
                            mentionable=want_mention,
                            reason="Copycord role sync",
                        )
                        updated += 1
                        self.db.upsert_role_mapping(
                            orig_id, want_name, cloned.id, cloned.name
                        )
                        logger.info("[🧩] Updated role %s", cloned.name)
                    except Exception as e:
                        logger.warning(
                            "[⚠️] Failed updating role %s: %s", cloned.name, e
                        )

        try:
            mapping = {
                r["original_role_id"]: r["cloned_role_id"]
                for r in self.db.get_all_role_mappings()
            }
            desired = [
                mapping[r["id"]]
                for r in sorted(incoming_filtered.values(), key=lambda x: x["position"])
                if r["id"] in mapping
            ]
            positions = {}
            movable = [guild.get_role(cid) for cid in desired]
            movable = [
                r for r in movable if r and not r.is_default() and r.position < bot_top
            ]
            base = 1
            for i, role in enumerate(movable, start=base):
                positions[role] = i
            if positions:
                await self.ratelimit.acquire(ActionType.EDIT_CHANNEL)
                await guild.edit_role_positions(positions=positions)
                return deleted, updated, created, True
        except Exception as e:
            logger.debug("[ℹ️] Role reordering skipped/failed: %s", e)

        return deleted, updated, created, False
