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
import logging
from typing import Any, List, Dict, Optional, Set
import discord
from common.common_helpers import resolve_mapping_settings


class SitemapService:
    def __init__(self, bot, config, db, ws, logger=None):
        self.bot = bot
        self.config = config
        self.db = db
        self.ws = ws
        self.logger = logger or logging.getLogger("client.sitemap")
        self._debounce_task = None
        self._dirty_guild_ids: Set[int] = set()
        self._dirty_lock = asyncio.Lock()

    def _pick_guild(self) -> Optional[discord.Guild]:
        return self.bot.guilds[0] if self.bot.guilds else None

    def _mapped_original_ids(self) -> list[int]:
        """All original (host) guild IDs from guild_mappings."""
        try:

            return list(self.db.get_all_original_guild_ids())
        except Exception:
            return []

    def _iter_mapped_guilds(self):
        """Yield discord.Guild objects for each mapped origin the bot can see."""
        ids = self._mapped_original_ids()
        if not ids:

            g = self._pick_guild()
            if g:
                yield g
            return
        for gid in ids:
            g = self.bot.get_guild(int(gid))
            if g:
                yield g

    def schedule_sync(self, guild_id: int | None, delay: float = 1.0) -> None:
        """
        Mark a guild as needing a sitemap resend, and debounce the actual send.
        guild_id may be None (fallback: send all mapped guilds once).
        """

        if guild_id is not None:
            try:
                self._dirty_guild_ids.add(int(guild_id))
            except Exception:
                pass
        else:
            # None means "we don't know which one", so mark all mapped origins
            for gid in self._mapped_original_ids():
                self._dirty_guild_ids.add(int(gid))

        if self._debounce_task is None:
            self._debounce_task = asyncio.create_task(self._debounced(delay))

    async def _build_and_send_selected(self, origin_ids: list[int]) -> None:
        """
        Build/send sitemap for just the given original guild ids.
        """
        for ogid in origin_ids:
            g = self.bot.get_guild(int(ogid))
            if not g:
                continue
            try:
                sm = await self.build_for_guild(g)
                if sm:
                    await self.ws.send({"type": "sitemap", "data": sm})
                    self.logger.info(
                        "[📩] Sitemap sent to Server (guild=%s/%s)",
                        g.name,
                        g.id,
                    )
            except Exception as e:
                self.logger.exception(
                    "[sitemap] failed to send for guild %s (%s): %s",
                    getattr(g, "name", "?"),
                    getattr(g, "id", "?"),
                    e,
                )

    async def build_and_send_all(self) -> None:
        """Build and send a sitemap for each mapped host guild."""
        sent = 0

        async def _send_one(g: "discord.Guild"):
            sm = await self.build_for_guild(g)
            if sm:
                await self.ws.send({"type": "sitemap", "data": sm})
                self.logger.info(
                    "[📩] Sitemap sent to Server (guild=%s/%s)", g.name, g.id
                )

        for g in self._iter_mapped_guilds():
            try:
                await _send_one(g)
                sent += 1
            except Exception as e:
                self.logger.exception(
                    "[sitemap] failed to send for guild %s (%s): %s", g.name, g.id, e
                )

    async def build_for_guild(self, guild: "discord.Guild") -> Dict:
        """Build the raw sitemap for a specific guild, then filter it per config."""
        settings = resolve_mapping_settings(
            self.db, self.config, original_guild_id=guild.id
        )
        if not guild:
            self.logger.warning("[⛔] No accessible guild found to build a sitemap.")
            return {
                "guild": {"id": None, "name": None},
                "categories": [],
                "standalone_channels": [],
                "forums": [],
                "threads": [],
                "emojis": [],
                "stickers": [],
                "roles": [],
                "community": {
                    "enabled": False,
                    "rules_channel_id": None,
                    "public_updates_channel_id": None,
                },
            }

        def _enum_int(val, default=0):
            if val is None:
                return default
            v = getattr(val, "value", val)
            try:
                return int(v)
            except Exception:
                return default

        def _sticker_url(s):
            u = getattr(s, "url", None)
            if not u:
                asset = getattr(s, "asset", None)
                u = getattr(asset, "url", None) if asset else None
            return str(u) if u else ""

        if settings.get("CLONE_STICKER", True):
            try:
                fetched_stickers = await guild.fetch_stickers()
            except Exception as e:
                self.logger.warning("[🎟️] Could not fetch stickers: %s", e)
                fetched_stickers = list(getattr(guild, "stickers", []))
        else:
            fetched_stickers = []

        try:
            guild_sticker_type_val = getattr(discord.StickerType, "guild").value
        except Exception:
            guild_sticker_type_val = 1

        stickers_payload = []
        for s in fetched_stickers:
            stype = _enum_int(getattr(s, "type", None), default=guild_sticker_type_val)
            if stype != guild_sticker_type_val:
                continue
            stickers_payload.append(
                {
                    "id": s.id,
                    "name": s.name,
                    "format_type": _enum_int(
                        getattr(s, "format", None) or getattr(s, "format_type", None), 0
                    ),
                    "url": _sticker_url(s),
                    "tags": getattr(s, "tags", "") or "",
                    "description": getattr(s, "description", "") or "",
                    "available": bool(getattr(s, "available", True)),
                }
            )

        sitemap: Dict = {
            "guild": {
                "id": guild.id,
                "name": guild.name,
                "owner_id": getattr(getattr(guild, "owner", None), "id", None),
                "icon": (
                    str(getattr(guild, "icon", ""))
                    if getattr(guild, "icon", None)
                    else None
                ),
            },
            "categories": [],
            "standalone_channels": [],
            "forums": [],
            "threads": [],
            "emojis": (
                []
                if not settings.get("CLONE_EMOJI", True)
                else [
                    {
                        "id": e.id,
                        "name": e.name,
                        "url": str(e.url),
                        "animated": e.animated,
                    }
                    for e in guild.emojis
                ]
            ),
            "stickers": stickers_payload,
            "roles": (
                []
                if not settings.get("CLONE_ROLES", True)
                else [
                    {
                        "id": r.id,
                        "name": r.name,
                        "permissions": r.permissions.value,
                        "color": (
                            r.color.value if hasattr(r.color, "value") else int(r.color)
                        ),
                        "hoist": r.hoist,
                        "mentionable": r.mentionable,
                        "managed": r.managed,
                        "everyone": (r == r.guild.default_role),
                        "position": r.position,
                    }
                    for r in guild.roles
                ]
            ),
            "community": {
                "enabled": "COMMUNITY" in guild.features,
                "rules_channel_id": (
                    guild.rules_channel.id if guild.rules_channel else None
                ),
                "public_updates_channel_id": (
                    guild.public_updates_channel.id
                    if guild.public_updates_channel
                    else None
                ),
            },
        }

        include_overwrites = settings.get("MIRROR_CHANNEL_PERMISSIONS", False)

        for cat in guild.categories:
            channels = []
            for ch in cat.channels:
                if isinstance(ch, discord.TextChannel):
                    channels.append(
                        {
                            "id": ch.id,
                            "name": ch.name,
                            "type": ch.type.value,
                            **(
                                {"overwrites": self._serialize_role_overwrites(ch)}
                                if include_overwrites
                                else {}
                            ),
                        }
                    )

            sitemap["categories"].append(
                {
                    "id": cat.id,
                    "name": cat.name,
                    "channels": channels,
                    **(
                        {"overwrites": self._serialize_role_overwrites(cat)}
                        if include_overwrites
                        else {}
                    ),
                }
            )

        sitemap["standalone_channels"] = [
            {
                "id": ch.id,
                "name": ch.name,
                "type": ch.type.value,
                **(
                    {"overwrites": self._serialize_role_overwrites(ch)}
                    if include_overwrites
                    else {}
                ),
            }
            for ch in guild.text_channels
            if ch.category is None
        ]

        for forum in getattr(guild, "forums", []):
            sitemap["forums"].append(
                {
                    "id": forum.id,
                    "name": forum.name,
                    "category_id": forum.category.id if forum.category else None,
                }
            )

        seen = {t["id"] for t in sitemap["threads"]}
        for row in self.db.get_all_threads():
            try:
                orig_tid = int(row["original_thread_id"])
                forum_orig = (
                    int(row["forum_original_id"])
                    if row["forum_original_id"] is not None
                    else None
                )
            except (TypeError, ValueError):
                continue

            thr = guild.get_channel(orig_tid)
            if not thr:
                try:
                    thr = await self.bot.fetch_channel(orig_tid)
                except Exception:
                    continue
            if not isinstance(thr, discord.Thread):
                continue

            sitemap["threads"].append(
                {
                    "id": thr.id,
                    "forum_id": forum_orig,
                    "name": thr.name,
                    "archived": thr.archived,
                }
            )

        filter_view = self._build_filter_view_for_guild(int(guild.id))

        sitemap = self._filter_sitemap(sitemap, filter_view)
        return sitemap

    async def build(self) -> Dict:
        """(Legacy) Build for a single guild using _pick_guild()."""
        guild = self._pick_guild()
        if not guild:
            self.logger.warning("[⛔] No accessible guild found to build a sitemap.")
            return {
                "guild": {"id": None, "name": None},
                "categories": [],
                "standalone_channels": [],
                "forums": [],
                "threads": [],
                "emojis": [],
                "stickers": [],
                "roles": [],
                "community": {
                    "enabled": False,
                    "rules_channel_id": None,
                    "public_updates_channel_id": None,
                },
            }
        return await self.build_for_guild(guild)

    def _build_filter_view_for_guild(self, origin_guild_id: int) -> Dict[str, object]:
        """
        Build a 'filter view' for a specific source/original guild.

        This merges:
        - global filters (NULL/NULL rows)
        - filters scoped to this origin_guild_id
        and returns sets plus whitelist_enabled just like Config.
        """
        f = self.db.get_filters(original_guild_id=origin_guild_id)

        include_category_ids = set(f["whitelist"]["category"])
        include_channel_ids = set(f["whitelist"]["channel"])
        excluded_category_ids = set(f["exclude"]["category"])
        excluded_channel_ids = set(f["exclude"]["channel"])

        whitelist_enabled = bool(include_category_ids or include_channel_ids)

        return {
            "include_category_ids": include_category_ids,
            "include_channel_ids": include_channel_ids,
            "excluded_category_ids": excluded_category_ids,
            "excluded_channel_ids": excluded_channel_ids,
            "whitelist_enabled": whitelist_enabled,
        }

    def reload_filters_and_resend(self):
        """
        Called when config filter sets were reloaded from DB.
        Rebuild and resend the sitemap so the server sees the new scope.
        """

        self.schedule_sync(delay=0.2)

    def _is_filtered_out_view(
        self,
        channel_id: int | None,
        category_id: int | None,
        view: Dict[str, object],
    ) -> bool:
        """
        Return True if this channel/category should be filtered out (hidden),
        using the *per-origin-guild* filter view rather than the old global config.
        This is the same logic we use when trimming the sitemap.
        """
        include_category_ids = view["include_category_ids"]
        include_channel_ids = view["include_channel_ids"]
        excluded_category_ids = view["excluded_category_ids"]
        excluded_channel_ids = view["excluded_channel_ids"]

        wl_on = bool(view["whitelist_enabled"])

        wl_ch = bool(channel_id and channel_id in include_channel_ids)
        wl_cat = bool(category_id and category_id in include_category_ids)

        ex_ch = bool(channel_id and channel_id in excluded_channel_ids)
        ex_cat = bool(category_id and category_id in excluded_category_ids)

        # Block if whitelist mode is on and this channel/category isn't whitelisted.
        if wl_on and not (wl_ch or wl_cat):
            return True

        if ex_ch and not wl_ch:
            return True

        if ex_cat and not (wl_cat or wl_ch):
            return True

        return False

    def in_scope_channel(self, ch) -> bool:
        """
        True if this channel/category/thread should be visible for ITS ORIGIN
        GUILD'S sitemap. This now respects per-guild / per-mapping filters,
        not the legacy global self.config lists.
        """
        try:

            g = getattr(ch, "guild", None)
            if g is None and isinstance(ch, discord.Thread):
                parent = getattr(ch, "parent", None)
                g = getattr(parent, "guild", None)

            origin_gid = int(getattr(g, "id", 0) or 0)
            view = self._build_filter_view_for_guild(origin_gid)

            if isinstance(ch, discord.CategoryChannel):
                return not self._is_filtered_out_view(
                    None,
                    getattr(ch, "id", None),
                    view,
                )

            if isinstance(ch, discord.Thread):
                parent = getattr(ch, "parent", None)
                if parent is None:
                    return False
                cat_id = getattr(parent, "category_id", None)
                return not self._is_filtered_out_view(
                    getattr(parent, "id", None),
                    cat_id,
                    view,
                )

            cat_id = getattr(ch, "category_id", None)
            return not self._is_filtered_out_view(
                getattr(ch, "id", None),
                cat_id,
                view,
            )
        except Exception:

            return True

    def _serialize_role_overwrites(self, obj: discord.abc.GuildChannel) -> list[dict]:
        out: list[dict] = []

        raw = (
            getattr(obj, "permission_overwrites", None)
            or getattr(obj, "_permission_overwrites", None)
            or getattr(obj, "_overwrites", None)
        )
        try:
            for ow in raw or []:
                t = getattr(ow, "type", None)
                if t in (0, "role", "ROLE"):
                    rid = int(getattr(ow, "id"))
                    allow_bits = int(getattr(ow, "allow", 0))
                    deny_bits = int(getattr(ow, "deny", 0))
                    out.append(
                        {
                            "type": "role",
                            "id": rid,
                            "allow_bits": allow_bits,
                            "deny_bits": deny_bits,
                        }
                    )
            if out:
                return out
        except Exception:
            pass

        return out

    def in_scope_thread(self, thr: discord.Thread) -> bool:
        """
        True if this thread's parent channel is allowed for that origin guild.
        """
        try:
            parent = getattr(thr, "parent", None)
            if parent is None:
                return False

            g = getattr(parent, "guild", None)
            origin_gid = int(getattr(g, "id", 0) or 0)
            view = self._build_filter_view_for_guild(origin_gid)

            cat_id = getattr(parent, "category_id", None)
            return not self._is_filtered_out_view(
                getattr(parent, "id", None),
                cat_id,
                view,
            )
        except Exception:
            return True

    def role_change_is_relevant(
        self, before: discord.Role, after: discord.Role
    ) -> bool:
        """Ignore @everyone and managed roles; ignore position changes."""
        try:
            if after.is_default() or after.managed:
                return False
            if before.name != after.name:
                return True
            if getattr(before.permissions, "value", 0) != getattr(
                after.permissions, "value", 0
            ):
                return True

            def _colval(c):
                try:
                    return c.value
                except Exception:
                    return int(c)

            if _colval(before.color) != _colval(after.color):
                return True
            if before.hoist != after.hoist:
                return True
            if before.mentionable != after.mentionable:
                return True
        except Exception:
            return True
        return False

    async def _debounced(self, delay: float):
        try:
            await asyncio.sleep(delay)

            dirty: list[int] = list(self._dirty_guild_ids)
            self._dirty_guild_ids.clear()

            if not dirty:
                dirty = list(self._mapped_original_ids())

            await self._build_and_send_selected(dirty)

        finally:
            self._debounce_task = None

    def _log_filter_settings(self):
        cfg = self.config
        self.logger.debug(
            "[filter] settings: wl_enabled=%s | inc_cats=%d inc_chs=%d | exc_cats=%d exc_chs=%d",
            bool(cfg.whitelist_enabled),
            len(getattr(cfg, "include_category_ids", set())),
            len(getattr(cfg, "include_channel_ids", set())),
            len(getattr(cfg, "excluded_category_ids", set())),
            len(getattr(cfg, "excluded_channel_ids", set())),
        )

    def _filter_reason(self, channel_id: int | None, category_id: int | None) -> str:
        cfg = self.config
        wl_on = bool(cfg.whitelist_enabled)
        allowed_ch = bool(channel_id and channel_id in cfg.include_channel_ids)
        allowed_cat = bool(category_id and category_id in cfg.include_category_ids)
        ex_ch = bool(channel_id and channel_id in cfg.excluded_channel_ids)
        ex_cat = bool(category_id and category_id in cfg.excluded_category_ids)

        if wl_on and not (allowed_ch or allowed_cat):
            return "blocked by whitelist (not listed)"
        if wl_on and allowed_cat and not allowed_ch and ex_ch:
            return "carve-out: excluded channel under whitelisted category"
        if ex_ch and not allowed_ch:
            return "excluded channel"
        if ex_cat and not (allowed_cat or allowed_ch):
            return "excluded category"
        return "allowed"

    def _filter_sitemap(
        self, sitemap: Dict[str, Any], view: Dict[str, object]
    ) -> Dict[str, Any]:
        """
        Apply whitelist / blacklist rules (now guild-scoped) to a sitemap dict.

        view = {
            "include_category_ids": set[int],
            "include_channel_ids": set[int],
            "excluded_category_ids": set[int],
            "excluded_channel_ids": set[int],
            "whitelist_enabled": bool,
        }
        """

        include_category_ids: Set[int] = view["include_category_ids"]
        include_channel_ids: Set[int] = view["include_channel_ids"]
        excluded_category_ids: Set[int] = view["excluded_category_ids"]
        excluded_channel_ids: Set[int] = view["excluded_channel_ids"]

        wl_on_global = bool(
            view["whitelist_enabled"] and (include_category_ids or include_channel_ids)
        )

        categories = sitemap.get("categories", [])
        standalone_channels = sitemap.get("channels", [])
        thread_entries = sitemap.get("threads", [])

        kept_categories: List[Dict[str, Any]] = []
        kept_standalones: List[Dict[str, Any]] = []
        kept_threads: List[Dict[str, Any]] = []
        dropped_channels: List[Dict[str, Any]] = []

        def _why_drop(cat_id: Optional[int], ch_id: Optional[int]) -> str:
            in_wl_cat = cat_id in include_category_ids if cat_id else False
            in_wl_ch = ch_id in include_channel_ids if ch_id else False
            in_ex_cat = cat_id in excluded_category_ids if cat_id else False
            in_ex_ch = ch_id in excluded_channel_ids if ch_id else False

            reasons: List[str] = []

            if wl_on_global:
                if in_wl_cat or in_wl_ch:
                    reasons.append("whitelist-in")
                else:
                    reasons.append("whitelist-out")

            if in_ex_cat or in_ex_ch:
                reasons.append("blacklisted")

            return ", ".join(reasons) or "none"

        for cat in categories:
            cat_id = int(cat["id"])
            channels = cat.get("channels", [])

            valid_channels: List[Dict[str, Any]] = []

            for ch in channels:
                ch_id = int(ch["id"])

                blacklisted = (cat_id in excluded_category_ids) or (
                    ch_id in excluded_channel_ids
                )
                whitelisted = (cat_id in include_category_ids) or (
                    ch_id in include_channel_ids
                )

                keep_ch = True
                if wl_on_global and not whitelisted:
                    keep_ch = False
                if blacklisted:
                    keep_ch = False

                if keep_ch:
                    valid_channels.append(ch)
                else:
                    dropped_channels.append(
                        {
                            "category_id": str(cat_id),
                            "channel_id": str(ch_id),
                            "name": ch.get("name"),
                            "reason": _why_drop(cat_id, ch_id),
                        }
                    )

            if valid_channels:
                kept_categories.append(
                    {
                        **cat,
                        "channels": valid_channels,
                    }
                )
            else:

                dropped_channels.append(
                    {
                        "category_id": str(cat_id),
                        "channel_id": None,
                        "name": cat.get("name"),
                        "reason": _why_drop(cat_id, None),
                    }
                )

        for ch in standalone_channels:
            ch_id = int(ch["id"])

            blacklisted = ch_id in excluded_channel_ids
            whitelisted = ch_id in include_channel_ids

            keep_ch = True
            if wl_on_global and not whitelisted:
                keep_ch = False
            if blacklisted:
                keep_ch = False

            if keep_ch:
                kept_standalones.append(ch)
            else:
                dropped_channels.append(
                    {
                        "category_id": None,
                        "channel_id": str(ch_id),
                        "name": ch.get("name"),
                        "reason": _why_drop(None, ch_id),
                    }
                )

        for th in thread_entries:
            parent_id = int(th.get("parent_channel_id") or 0)
            th_id = int(th["id"])

            blacklisted = (
                parent_id in excluded_channel_ids or th_id in excluded_channel_ids
            )
            whitelisted = (
                parent_id in include_channel_ids or th_id in include_channel_ids
            )

            keep_th = True
            if wl_on_global and not whitelisted:
                keep_th = False
            if blacklisted:
                keep_th = False

            if keep_th:
                kept_threads.append(th)
            else:
                dropped_channels.append(
                    {
                        "category_id": None,
                        "channel_id": str(th_id),
                        "name": th.get("name"),
                        "reason": _why_drop(None, th_id),
                    }
                )

        sitemap["categories"] = kept_categories
        sitemap["channels"] = kept_standalones
        sitemap["threads"] = kept_threads

        self.logger.debug(
            "[filter] settings (guild=%s) | wl_enabled=%s "
            "| inc_cats=%d inc_chs=%d | ex_cats=%d ex_chs=%d",
            sitemap.get("guild", {}).get("id"),
            wl_on_global,
            len(include_category_ids),
            len(include_channel_ids),
            len(excluded_category_ids),
            len(excluded_channel_ids),
        )

        sitemap["dropped"] = dropped_channels

        return sitemap

    def _is_filtered_out(self, channel_id: int | None, category_id: int | None) -> bool:
        cfg = self.config

        wl_ch = bool(channel_id and channel_id in cfg.include_channel_ids)
        wl_cat = bool(category_id and category_id in cfg.include_category_ids)
        ex_ch = bool(channel_id and channel_id in cfg.excluded_channel_ids)
        ex_cat = bool(category_id and category_id in cfg.excluded_category_ids)

        wl_on = bool(
            cfg.whitelist_enabled
            and (cfg.include_channel_ids or cfg.include_category_ids)
        )

        if wl_on and not (wl_ch or wl_cat):
            return True

        if ex_ch and not wl_ch:
            return True

        if ex_cat and not (wl_cat or wl_ch):
            return True

        return False

    def is_excluded_ids(self, channel_id: int | None, category_id: int | None) -> bool:
        return self._is_filtered_out(channel_id, category_id)
