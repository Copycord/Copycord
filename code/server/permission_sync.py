# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
import asyncio
import inspect
from typing import Dict, List, Optional, Tuple, Any
import discord
from discord.channel import CategoryChannel, TextChannel


class ChannelPermissionSync:
    """
    Permission applier for category & channel role overwrites using cloned roles.
    """

    def __init__(
        self,
        *,
        config,
        db,
        bot: discord.Client | discord.AutoShardedClient,
        clone_guild_id: int,
        cat_map: Dict[int, dict],
        chan_map: Dict[int, dict],
        logger,
        ratelimit=None,
        rate_limiter_action=None,
    ) -> None:
        self.config = config
        self.db = db
        self.bot = bot
        self.clone_guild_id = int(clone_guild_id)
        self.cat_map = cat_map
        self.chan_map = chan_map
        self.log = logger
        self.ratelimit = ratelimit
        self.rate_limiter_action = rate_limiter_action

    def schedule_after_role_sync(
        self,
        roles_manager,
        roles_handle_or_none,
        guild: discord.Guild,
        sitemap: dict,
        *,
        task_name: str = "perm_sync_after_roles",
        await_timeout: float = 120.0,
    ) -> None:
        if not getattr(self.config, "MIRROR_CHANNEL_PERMISSIONS", False):
            return
        if not getattr(self.config, "CLONE_ROLES", False):
            return
        if guild is None:
            return

        src_everyone_id: Optional[int] = None
        try:
            for r in sitemap.get("roles", []) or []:
                if r.get("everyone"):
                    src_everyone_id = int(r["id"])
                    break
        except Exception:
            src_everyone_id = None

        async def _runner():
            try:
                await self._await_roles_done(
                    roles_manager, roles_handle_or_none, await_timeout
                )
                parts = await self._sync_permissions(guild, sitemap, src_everyone_id)
                if parts:
                    self.log.info("[perm-sync] %s", "; ".join(parts))
                else:
                    self.log.debug("[perm-sync] No permission changes needed")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.warning("[perm-sync] Background sync failed: %s", e)

        asyncio.create_task(_runner(), name=task_name)

    async def _await_roles_done(self, roles_manager, handle, timeout: float) -> None:
        try:
            if inspect.isawaitable(handle):
                await asyncio.wait_for(handle, timeout=timeout)
                return
        except Exception:
            pass

        wai = getattr(roles_manager, "wait_until_idle", None)
        if callable(wai):
            try:
                await asyncio.wait_for(wai(), timeout=timeout)
                return
            except Exception:
                pass

        evt = getattr(roles_manager, "sync_done_event", None)
        if isinstance(evt, asyncio.Event):
            try:
                await asyncio.wait_for(evt.wait(), timeout=timeout)
                return
            except Exception:
                pass

        await asyncio.sleep(2.0)

    def _reload_maps_from_db(self) -> None:
        try:
            self.cat_map.clear()
            self.cat_map.update(
                {
                    r["original_category_id"]: dict(r)
                    for r in self.db.get_all_category_mappings()
                }
            )
            self.chan_map.clear()
            self.chan_map.update(
                {
                    r["original_channel_id"]: dict(r)
                    for r in self.db.get_all_channel_mappings()
                }
            )
        except Exception as e:
            self.log.warning("[perm-sync] failed to reload maps from DB: %s", e)

    async def _sync_permissions(
        self,
        guild: discord.Guild,
        sitemap: dict,
        src_everyone_id: Optional[int],
    ) -> List[str]:
        self._reload_maps_from_db()
        changed_cat = changed_ch = 0

        for cat in sitemap.get("categories", []) or []:
            row = self.cat_map.get(int(cat["id"]))
            if not row:
                self.log.info("[perm-sync] skip category %s: no cat_map", cat.get("id"))
                continue
            cc = guild.get_channel(int(row.get("cloned_category_id") or 0))
            if isinstance(cc, CategoryChannel):
                if await self._apply_overwrites_to_channel(
                    cc, cat.get("overwrites", []), src_everyone_id
                ):
                    changed_cat += 1

            for ch in cat.get("channels", []) or []:
                crow = self.chan_map.get(int(ch["id"]))
                if not crow:
                    self.log.info(
                        "[perm-sync] skip channel %s: no chan_map", ch.get("id")
                    )
                    continue
                cch = guild.get_channel(int(crow.get("cloned_channel_id") or 0))
                if isinstance(cch, TextChannel):
                    if await self._apply_overwrites_to_channel(
                        cch, ch.get("overwrites", []), src_everyone_id
                    ):
                        changed_ch += 1

        for ch in sitemap.get("standalone_channels", []) or []:
            crow = self.chan_map.get(int(ch["id"]))
            if not crow:
                self.log.info(
                    "[perm-sync] skip channel %s: no chan_map (standalone)",
                    ch.get("id"),
                )
                continue
            cch = guild.get_channel(int(crow.get("cloned_channel_id") or 0))
            if isinstance(cch, TextChannel):
                if await self._apply_overwrites_to_channel(
                    cch, ch.get("overwrites", []), src_everyone_id
                ):
                    changed_ch += 1

        parts: List[str] = []
        if changed_cat:
            parts.append(f"Updated {changed_cat} category permission sets")
        if changed_ch:
            parts.append(f"Updated {changed_ch} channel permission sets")
        return parts

    @staticmethod
    def _extract_cloned_role_id(row: Any) -> Optional[int]:
        if row is None:
            return None
        if isinstance(row, dict):
            for k in ("cloned_role_id", "clone_role_id", "target_role_id", "cloned_id"):
                if k in row and row[k]:
                    try:
                        return int(row[k])
                    except Exception:
                        pass
        for attr in ("cloned_role_id", "clone_role_id", "target_role_id", "cloned_id"):
            if hasattr(row, attr):
                try:
                    val = getattr(row, attr)
                    if val:
                        return int(val)
                except Exception:
                    pass
        try:
            val = row["cloned_role_id"]
            if val:
                return int(val)
        except Exception:
            pass
        return None

    def _raw_role_bits_map_from_channel(
        self, ch: discord.abc.GuildChannel
    ) -> Dict[int, Tuple[int, int]]:
        """
        Exact ROLE-overwrite map using raw allow/deny ints when available.
        Ignores member overwrites. Keys are role IDs (including @everyone).
        """
        out: Dict[int, Tuple[int, int]] = {}

        raw = (
            getattr(ch, "permission_overwrites", None)
            or getattr(ch, "_permission_overwrites", None)
            or getattr(ch, "_overwrites", None)
        )
        try:
            for ow in raw or []:
                t = getattr(ow, "type", None)
                if t in (0, "role", "ROLE"):
                    rid = int(getattr(ow, "id"))
                    a = int(getattr(ow, "allow", 0))
                    d = int(getattr(ow, "deny", 0))
                    out[rid] = (a, d)
        except Exception:

            pass

        return out

    @staticmethod
    def _normalize_role_map(
        role_map: Dict[int, Tuple[int, int]],
    ) -> Dict[int, Tuple[int, int]]:
        """Drop fully-neutral (0/0) entries so '/' means 'no overwrite'."""
        return {rid: (a, d) for rid, (a, d) in role_map.items() if (a | d) != 0}

    async def _apply_overwrites_to_channel(
        self,
        ch: discord.abc.GuildChannel,
        role_items: List[dict],
        src_everyone_id: Optional[int],
    ) -> bool:
        if not role_items:
            return False

        guild = ch.guild

        desired_role_map: Dict[int, Tuple[int, int]] = {}

        for item in role_items:
            if item.get("type") != "role":
                continue
            orig_role_id = int(item.get("id") or 0)
            if not orig_role_id:
                continue

            if src_everyone_id is not None and orig_role_id == src_everyone_id:
                clone_role_id = int(guild.default_role.id)
            else:
                row = self.db.get_role_mapping(orig_role_id)
                clone_role_id = self._extract_cloned_role_id(row) or 0
                if not clone_role_id:
                    self.log.info(
                        "[perm-sync] #%s skip role %s: no cloned mapping",
                        getattr(ch, "id", "?"),
                        orig_role_id,
                    )
                    continue

            allow_bits = int(item.get("allow_bits", 0))
            deny_bits = int(item.get("deny_bits", 0))

            if (allow_bits | deny_bits) == 0:
                continue

            desired_role_map[clone_role_id] = (allow_bits, deny_bits)

        if not desired_role_map:

            current = self._normalize_role_map(self._raw_role_bits_map_from_channel(ch))
            if not current:
                self.log.debug(
                    "[perm-sync] #%s equal: no role overwrites desired or present",
                    getattr(ch, "id", "?"),
                )
                return False

        else:

            current = self._normalize_role_map(self._raw_role_bits_map_from_channel(ch))
            if current == desired_role_map:
                self.log.debug(
                    "[perm-sync] #%s equal: role overwrites already match",
                    getattr(ch, "id", "?"),
                )
                return False

        member_payload: List[dict] = []
        try:
            for tgt, ow in getattr(ch, "overwrites", {}).items():
                if isinstance(tgt, discord.Member):
                    a, d = ow.pair()
                    member_payload.append(
                        {
                            "id": str(int(tgt.id)),
                            "type": 1,
                            "allow": str(int(getattr(a, "value", a))),
                            "deny": str(int(getattr(d, "value", d))),
                        }
                    )
        except Exception:
            pass

        payload_overwrites: List[dict] = list(member_payload)
        for rid, (a_bits, d_bits) in desired_role_map.items():
            payload_overwrites.append(
                {
                    "id": str(int(rid)),
                    "type": 0,
                    "allow": str(int(a_bits)),
                    "deny": str(int(d_bits)),
                }
            )

        try:
            if self.ratelimit and self.rate_limiter_action is not None:
                await self.ratelimit.acquire(self.rate_limiter_action)
            await ch._state.http.edit_channel(
                ch.id,
                permission_overwrites=payload_overwrites,
                reason="Mirror role permissions",
            )
            self.log.info(
                "[🔐] Applied permissions on #%s (%s) (roles=%d)",
                getattr(ch, "id", "?"),
                getattr(ch, "name", "?"),
                len(desired_role_map),
            )
            return True
        except Exception as e:
            self.log.warning(
                "[perm-sync] Failed to apply permissions on #%s: %s",
                getattr(ch, "id", "?"),
                e,
            )
            return False
