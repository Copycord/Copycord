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
        self._send_lock = asyncio.Lock()

    def _pick_guild(self) -> Optional[discord.Guild]:
        return self.bot.guilds[0] if self.bot.guilds else None

    def _mapped_original_ids(self) -> list[int]:
        """All original (host) guild IDs from guild_mappings (unique)."""
        try:
            ids = list(self.db.get_all_original_guild_ids())

            seen = {}
            for i in ids:
                seen.setdefault(int(i), True)
            return list(seen.keys())
        except Exception:
            return []

    def _cancel_pending_debounce(self) -> None:
        try:
            if self._debounce_task and not self._debounce_task.done():
                self._debounce_task.cancel()
        except Exception:
            pass
        self._debounce_task = None

    def _mapping_label(
        self,
        origin_guild_id: int,
        guild_name_fallback: str,
        cloned_guild_id: int | None = None,
    ) -> str:
        try:
            mappings = self.db.list_guild_mappings() or []
        except Exception:
            mappings = []

        def _build_label(m: dict) -> str:
            mapping_id = (m.get("mapping_id") or "").strip() or str(origin_guild_id)

            name = (m.get("mapping_name") or "").strip()
            if not name and cloned_guild_id is not None:
                name = (m.get("cloned_guild_name") or "").strip()
            if not name:
                name = (m.get("original_guild_name") or "").strip()
            if not name:
                name = guild_name_fallback

            return f"{name}"

        if cloned_guild_id is not None:
            for m in mappings:
                try:
                    ogid = int(m.get("original_guild_id", 0) or 0)
                    cgid = int(m.get("cloned_guild_id", 0) or 0)
                except Exception:
                    continue
                if ogid == int(origin_guild_id) and cgid == int(cloned_guild_id):
                    return _build_label(m)

        for m in mappings:
            try:
                ogid = int(m.get("original_guild_id", 0) or 0)
            except Exception:
                ogid = 0
            if ogid == int(origin_guild_id):
                return _build_label(m)

        return f"{guild_name_fallback} ({origin_guild_id})"

    def _iter_mapped_guilds(self):
        ids = self._mapped_original_ids()
        for gid in ids:
            g = self.bot.get_guild(int(gid))
            if g:
                yield g

    def schedule_sync(self, guild_id: int | None, delay: float = 1.0) -> None:
        mapped = set(self._mapped_original_ids())

        if guild_id is not None:
            gid = int(guild_id)
            if gid not in mapped:
                self.logger.info(
                    "[sitemap] Ignoring sync request for unmapped origin %s",
                    gid,
                )
                return
            self._dirty_guild_ids.add(gid)
        else:
            for gid in mapped:
                self._dirty_guild_ids.add(int(gid))

        if self._debounce_task is None:
            self._debounce_task = asyncio.create_task(self._debounced(delay))

    def _build_filter_view_for_mapping(
        self,
        origin_guild_id: int,
        cloned_guild_id: int | None,
    ) -> Dict[str, object]:
        """
        Build filter view for a specific (origin, clone) mapping.
        Merges global rows + origin-only rows + (origin,clone) rows with proper precedence.
        """
        f = self.db.get_filters(
            original_guild_id=int(origin_guild_id),
            cloned_guild_id=(
                int(cloned_guild_id) if cloned_guild_id is not None else None
            ),
        )

        def _to_int_set(xs):
            out = set()
            for x in xs or []:
                try:
                    out.add(int(x))
                except Exception:
                    pass
            return out

        include_category_ids = _to_int_set(f["whitelist"]["category"])
        include_channel_ids = _to_int_set(f["whitelist"]["channel"])
        excluded_category_ids = _to_int_set(f["exclude"]["category"])
        excluded_channel_ids = _to_int_set(f["exclude"]["channel"])

        return {
            "include_category_ids": include_category_ids,
            "include_channel_ids": include_channel_ids,
            "excluded_category_ids": excluded_category_ids,
            "excluded_channel_ids": excluded_channel_ids,
            "whitelist_enabled": bool(include_category_ids or include_channel_ids),
        }

    async def _build_and_send_selected(
        self,
        origin_ids: list[int],
        max_concurrency: int = 10,
    ) -> None:
        """
        Build/send sitemap for just the given original guild ids.
        Fans out to each configured clone for the origin (legacy-compatible).

        Uses a limited async concurrency so that many mappings can be sent quickly
        without blocking on each other.
        """
        if not origin_ids:
            return

        sem = asyncio.Semaphore(max_concurrency)
        tasks: list[asyncio.Future] = []

        async def _handle_mapping(
            g: discord.Guild,
            clone_id: int | None,
        ) -> None:
            async with sem:
                try:

                    if clone_id is None:
                        sm = await self.build_for_guild(g)
                    else:
                        sm = await self.build_for_guild_and_clone(g, int(clone_id))

                    if not sm:
                        return

                    await self.ws.send({"type": "sitemap", "data": sm})

                    label = self._mapping_label(
                        g.id,
                        g.name,
                        int(clone_id) if clone_id is not None else None,
                    )
                    self.logger.info(
                        "[📩] Sitemap sent for %s",
                        label,
                    )
                except Exception as e:
                    self.logger.exception(
                        "[sitemap] failed to send for guild %s (%s) clone %s: %s",
                        getattr(g, "name", "?"),
                        getattr(g, "id", "?"),
                        clone_id,
                        e,
                    )

        for ogid in origin_ids:
            g = self.bot.get_guild(int(ogid))
            if not g:
                continue

            try:
                clones = self.db.get_clone_guild_ids_for_origin(int(g.id)) or [None]
            except Exception as e:
                self.logger.exception(
                    "[sitemap] get_clone_guild_ids_for_origin failed for guild %s (%s): %s",
                    getattr(g, "name", "?"),
                    getattr(g, "id", "?"),
                    e,
                )
                continue

            for cg in clones:
                clone_id = int(cg) if cg is not None else None
                tasks.append(_handle_mapping(g, clone_id))

        if tasks:

            await asyncio.gather(*tasks)

    async def send_for_mapping_id(self, mapping_id: str) -> None:
        """
        Build and send a sitemap for exactly one mapping row
        (original_guild_id + cloned_guild_id) identified by mapping_id.
        """
        mapping_id = (mapping_id or "").strip()
        if not mapping_id:
            self.logger.warning("[sitemap] send_for_mapping_id called with empty id")
            return

        try:
            m = self.db.get_mapping_by_id(mapping_id)
        except Exception:
            self.logger.exception(
                "[sitemap] get_mapping_by_id failed for %r", mapping_id
            )
            return

        if not m:
            self.logger.warning(
                "[sitemap] No guild_mappings row found for mapping_id=%r", mapping_id
            )
            return

        try:
            ogid = int(m["original_guild_id"] or 0)
            cgid = int(m["cloned_guild_id"] or 0)
        except Exception:
            self.logger.warning(
                "[sitemap] Bad mapping row for %r: %r", mapping_id, dict(m)
            )
            return

        g = self.bot.get_guild(ogid)
        if not g:
            self.logger.warning(
                "[sitemap] Host guild %s not found in bot for mapping_id=%r",
                ogid,
                mapping_id,
            )
            return

        async with self._send_lock:
            try:
                sitemap = await self.build_for_guild_and_clone(g, cgid)
                if not sitemap:
                    self.logger.info(
                        "[sitemap] Empty sitemap for mapping_id=%r (%s -> %s)",
                        mapping_id,
                        ogid,
                        cgid,
                    )
                    return

                await self.ws.send({"type": "sitemap", "data": sitemap})

                label = self._mapping_label(ogid, g.name, cgid)
                self.logger.info(
                    "[📩] Sent targeted sitemap for %s -> clone %s (mapping_id=%s)",
                    label,
                    cgid,
                    mapping_id,
                )
            except Exception:
                self.logger.exception(
                    "[sitemap] Failed sending targeted sitemap for mapping_id=%r",
                    mapping_id,
                )

    async def build_and_send_all(self, max_concurrency: int = 10) -> None:
        async with self._send_lock:

            self._cancel_pending_debounce()
            self._dirty_guild_ids.clear()

            origin_ids = self._mapped_original_ids()

            if not origin_ids:
                self.logger.info("[⚠️] No active guild mappings to clone. Please check your configuration.")
                return

            await self._build_and_send_selected(
                origin_ids,
                max_concurrency=max_concurrency,
            )

    async def build_for_guild(
        self, guild: "discord.Guild", cloned_guild_id: int | None = None
    ) -> Dict:
        """Build the raw sitemap for a specific guild, then filter it per config."""
        if not guild:
            self.logger.warning("[⛔] No accessible guild found to build a sitemap.")
            return {
                "guild": {
                    "id": None,
                    "name": None,
                    "owner_id": None,
                    "icon": None,
                    "banner": None,
                    "splash": None,
                    "discovery_splash": None,
                    "description": None,
                    "vanity_url_code": None,
                    "preferred_locale": None,
                    "features": [],
                    "afk_channel_id": None,
                    "afk_timeout": None,
                    "system_channel_id": None,
                    "system_channel_flags": None,
                    "verification_level": None,
                    "explicit_content_filter": None,
                    "default_notifications": None,
                    "widget_enabled": None,
                    "widget_channel_id": None,
                    "premium_tier": None,
                    "premium_subscription_count": None,
                    "max_members": None,
                    "max_presences": None,
                    "max_video_channel_users": None,
                    "nsfw_level": None,
                    "mfa_level": None,
                },
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

        def _serialize_forum_default_reaction(forum) -> Optional[Dict[str, object]]:
            """
            Serialize the default reaction emoji on a ForumChannel (if present).
            """
            try:
                em = getattr(forum, "default_reaction_emoji", None)
            except Exception:
                em = None
            if not em:
                return None

            try:
                emoji_id = getattr(em, "id", None)
                emoji_name = getattr(em, "name", None)
                emoji_animated = bool(getattr(em, "animated", False))
            except Exception:
                emoji_id = None
                emoji_name = None
                emoji_animated = False

            if emoji_id is None and not emoji_name:
                return None

            return {
                "id": int(emoji_id) if emoji_id else None,
                "name": emoji_name,
                "animated": emoji_animated,
            }

        def _serialize_forum_tags(forum) -> List[Dict[str, object]]:
            """
            Serialize available forum tags into a simple JSON-friendly structure.
            """
            tags = []
            try:
                available = list(getattr(forum, "available_tags", []) or [])
            except Exception:
                available = []

            for t in available:
                try:
                    tag_id = getattr(t, "id", None)
                    name = getattr(t, "name", None)
                    moderated = bool(getattr(t, "moderated", False))
                except Exception:
                    continue

                emoji_obj = getattr(t, "emoji", None)
                if emoji_obj:
                    emoji_id = getattr(emoji_obj, "id", None)
                    emoji_name = getattr(emoji_obj, "name", None)
                    emoji_animated = bool(getattr(emoji_obj, "animated", False))
                else:
                    emoji_id = None
                    emoji_name = None
                    emoji_animated = False

                tags.append(
                    {
                        "id": int(tag_id) if tag_id is not None else None,
                        "name": name,
                        "moderated": moderated,
                        "emoji_id": int(emoji_id) if emoji_id is not None else None,
                        "emoji_name": emoji_name,
                        "emoji_animated": emoji_animated,
                    }
                )
            return tags

        def _serialize_video_quality(ch):
            """
            Normalize discord.VideoQualityMode → 'auto' / 'full' / None
            so it can safely go into JSON.
            """
            vqm = getattr(ch, "video_quality_mode", None)
            if not vqm:
                return None

            name = getattr(vqm, "name", None)
            if isinstance(name, str):
                return name.lower()

            try:
                val = int(getattr(vqm, "value", 0))
            except Exception:
                return None

            if val == 2:
                return "full"
            if val == 1:
                return "auto"
            return None

        def _sticker_url(s):
            u = getattr(s, "url", None)
            if not u:
                asset = getattr(s, "asset", None)
                u = getattr(asset, "url", None) if asset else None
            return str(u) if u else ""

        def _asset_str(obj, attr_name: str) -> Optional[str]:
            """
            Safely stringify a guild Asset-style attribute (icon, banner, splash, etc.).
            Returns None if not present.
            """
            try:
                asset = getattr(obj, attr_name, None)
            except Exception:
                asset = None
            if not asset:
                return None
            try:
                return str(asset)
            except Exception:
                return None

        try:
            fetched_stickers = await guild.fetch_stickers()
        except Exception as e:
            self.logger.warning("[🎟️] Could not fetch stickers: %s", e)
            fetched_stickers = list(getattr(guild, "stickers", []))

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

        afk_channel_id = getattr(getattr(guild, "afk_channel", None), "id", None)
        system_channel_id = getattr(getattr(guild, "system_channel", None), "id", None)
        pref_locale = getattr(guild, "preferred_locale", None)
        features = list(getattr(guild, "features", []) or [])

        try:
            scf = getattr(guild, "system_channel_flags", None)
            system_flags_val = (
                int(getattr(scf, "value", int(scf))) if scf is not None else None
            )
        except Exception:
            system_flags_val = None

        try:
            widget_enabled = getattr(guild, "widget_enabled", None)
        except Exception:
            widget_enabled = None

        try:
            widget_channel = getattr(guild, "widget_channel", None)
            widget_channel_id = (
                getattr(widget_channel, "id", None) if widget_channel else None
            )
        except Exception:
            widget_channel_id = None

        sitemap: Dict = {
            "guild": {
                "id": guild.id,
                "name": guild.name,
                "owner_id": getattr(getattr(guild, "owner", None), "id", None),
                "icon": _asset_str(guild, "icon"),
                "banner": _asset_str(guild, "banner"),
                "splash": _asset_str(guild, "splash"),
                "discovery_splash": _asset_str(guild, "discovery_splash"),
                "description": getattr(guild, "description", None),
                "vanity_url_code": getattr(guild, "vanity_url_code", None),
                "preferred_locale": (
                    str(pref_locale) if pref_locale is not None else None
                ),
                "features": features,
                "afk_channel_id": afk_channel_id,
                "afk_timeout": getattr(guild, "afk_timeout", None),
                "system_channel_id": system_channel_id,
                "system_channel_flags": system_flags_val,
                "widget_enabled": widget_enabled,
                "widget_channel_id": widget_channel_id,
                "verification_level": _enum_int(
                    getattr(guild, "verification_level", None), 0
                ),
                "explicit_content_filter": _enum_int(
                    getattr(guild, "explicit_content_filter", None), 0
                ),
                "default_notifications": _enum_int(
                    getattr(guild, "default_notifications", None), 0
                ),
                "premium_tier": _enum_int(getattr(guild, "premium_tier", None), 0),
                "premium_subscription_count": getattr(
                    guild, "premium_subscription_count", None
                ),
                "max_members": getattr(guild, "max_members", None),
                "max_presences": getattr(guild, "max_presences", None),
                "max_video_channel_users": getattr(
                    guild, "max_video_channel_users", None
                ),
                "nsfw_level": _enum_int(getattr(guild, "nsfw_level", None), 0),
                "mfa_level": _enum_int(getattr(guild, "mfa_level", None), 0),
            },
            "categories": [],
            "standalone_channels": [],
            "forums": [],
            "threads": [],
            "emojis": [
                {"id": e.id, "name": e.name, "url": str(e.url), "animated": e.animated}
                for e in guild.emojis
            ],
            "stickers": stickers_payload,
            "roles": sorted(
                [
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
                ],
                key=lambda x: x["position"],
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

        include_overwrites = True

        for cat in guild.categories:
            channels = []
            for ch in cat.channels:
                if isinstance(ch, discord.TextChannel):
                    channels.append(
                        {
                            "id": ch.id,
                            "name": ch.name,
                            "type": ch.type.value,
                            "nsfw": getattr(ch, "nsfw", False),
                            "topic": getattr(ch, "topic", None),
                            "slowmode_delay": getattr(ch, "slowmode_delay", 0),
                            **(
                                {"overwrites": self._serialize_role_overwrites(ch)}
                                if include_overwrites
                                else {}
                            ),
                        }
                    )
                elif isinstance(ch, discord.VoiceChannel):
                    channels.append(
                        {
                            "id": ch.id,
                            "name": ch.name,
                            "type": ch.type.value,
                            "nsfw": getattr(ch, "nsfw", False),
                            "slowmode_delay": getattr(ch, "slowmode_delay", 0),
                            "bitrate": getattr(ch, "bitrate", 64000),
                            "user_limit": getattr(ch, "user_limit", 0),
                            "rtc_region": getattr(ch, "rtc_region", None),
                            "video_quality": _serialize_video_quality(ch),
                            **(
                                {"overwrites": self._serialize_role_overwrites(ch)}
                                if include_overwrites
                                else {}
                            ),
                        }
                    )
                elif isinstance(ch, discord.StageChannel):
                    channels.append(
                        {
                            "id": ch.id,
                            "name": ch.name,
                            "type": ch.type.value,
                            "nsfw": getattr(ch, "nsfw", False),
                            "slowmode_delay": getattr(ch, "slowmode_delay", 0),
                            "bitrate": getattr(ch, "bitrate", 64000),
                            "user_limit": getattr(ch, "user_limit", 0),
                            "rtc_region": getattr(ch, "rtc_region", None),
                            "topic": getattr(ch, "topic", None),
                            "video_quality": _serialize_video_quality(ch),
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

        sitemap["standalone_channels"] = []
        for ch in guild.channels:
            if ch.category is None:
                if isinstance(ch, discord.TextChannel):
                    sitemap["standalone_channels"].append(
                        {
                            "id": ch.id,
                            "name": ch.name,
                            "type": ch.type.value,
                            "nsfw": getattr(ch, "nsfw", False),
                            "topic": getattr(ch, "topic", None),
                            "slowmode_delay": getattr(ch, "slowmode_delay", 0),
                            **(
                                {"overwrites": self._serialize_role_overwrites(ch)}
                                if include_overwrites
                                else {}
                            ),
                        }
                    )
                elif isinstance(ch, discord.VoiceChannel):
                    sitemap["standalone_channels"].append(
                        {
                            "id": ch.id,
                            "name": ch.name,
                            "type": ch.type.value,
                            "nsfw": getattr(ch, "nsfw", False),
                            "slowmode_delay": getattr(ch, "slowmode_delay", 0),
                            "bitrate": getattr(ch, "bitrate", 64000),
                            "user_limit": getattr(ch, "user_limit", 0),
                            "rtc_region": getattr(ch, "rtc_region", None),
                            "video_quality": _serialize_video_quality(ch),
                            **(
                                {"overwrites": self._serialize_role_overwrites(ch)}
                                if include_overwrites
                                else {}
                            ),
                        }
                    )
                elif isinstance(ch, discord.StageChannel):
                    sitemap["standalone_channels"].append(
                        {
                            "id": ch.id,
                            "name": ch.name,
                            "type": ch.type.value,
                            "nsfw": getattr(ch, "nsfw", False),
                            "slowmode_delay": getattr(ch, "slowmode_delay", 0),
                            "bitrate": getattr(ch, "bitrate", 64000),
                            "user_limit": getattr(ch, "user_limit", 0),
                            "rtc_region": getattr(ch, "rtc_region", None),
                            "topic": getattr(ch, "topic", None),
                            "video_quality": _serialize_video_quality(ch),
                            **(
                                {"overwrites": self._serialize_role_overwrites(ch)}
                                if include_overwrites
                                else {}
                            ),
                        }
                    )

        for forum in getattr(guild, "forums", []):
            entry = {
                "id": forum.id,
                "type": forum.type.value,
                "name": forum.name,
                "category_id": forum.category.id if forum.category else None,
                "nsfw": getattr(forum, "nsfw", False),
                "post_guidelines": getattr(forum, "topic", None),
                "topic": getattr(forum, "topic", None),
                "slowmode_delay": getattr(forum, "slowmode_delay", 0),
                "message_limit_per_interval": int(
                    getattr(forum, "default_thread_slowmode_delay", 0) or 0
                ),
                "default_layout": _enum_int(getattr(forum, "default_layout", None), 0),
                "default_sort_order": _enum_int(
                    getattr(forum, "default_sort_order", None), 0
                ),
                "hide_after_inactivity": getattr(
                    forum, "default_auto_archive_duration", None
                ),
                "require_tag": bool(
                    getattr(forum, "require_tag", False)
                    or getattr(forum, "require_tags", False)
                    or bool(
                        getattr(
                            getattr(forum, "flags", None),
                            "require_tag",
                            False,
                        )
                    )
                ),
                "default_reaction": _serialize_forum_default_reaction(forum),
                "available_tags": _serialize_forum_tags(forum),
            }
            if include_overwrites:
                entry["overwrites"] = self._serialize_role_overwrites(forum)
            sitemap["forums"].append(entry)

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

        if cloned_guild_id is not None:
            filter_view = self._build_filter_view_for_mapping(
                int(guild.id), int(cloned_guild_id)
            )
        else:
            filter_view = self._build_filter_view_for_guild(int(guild.id))

        sitemap = self._filter_sitemap(sitemap, filter_view)
        return sitemap

    async def build_for_guild_and_clone(
        self, guild: "discord.Guild", cloned_guild_id: int
    ) -> Dict:
        sm = await self.build_for_guild(guild, cloned_guild_id=int(cloned_guild_id))
        if not sm:
            return sm
        sm["target"] = {
            "original_guild_id": int(guild.id),
            "cloned_guild_id": int(cloned_guild_id),
        }
        return sm

    async def build(self) -> Dict:
        """(Legacy) Build for a single guild using _pick_guild()."""
        guild = self._pick_guild()
        if not guild:
            self.logger.warning("[⛔] No accessible guild found to build a sitemap.")
            return {
                "guild": {
                    "id": None,
                    "name": None,
                    "owner_id": None,
                    "icon": None,
                    "banner": None,
                    "splash": None,
                    "discovery_splash": None,
                    "description": None,
                    "vanity_url_code": None,
                    "preferred_locale": None,
                    "features": [],
                    "afk_channel_id": None,
                    "afk_timeout": None,
                    "system_channel_id": None,
                    "system_channel_flags": None,
                    "verification_level": None,
                    "explicit_content_filter": None,
                    "default_notifications": None,
                    "widget_enabled": None,
                    "widget_channel_id": None,
                    "premium_tier": None,
                    "premium_subscription_count": None,
                    "max_members": None,
                    "max_presences": None,
                    "max_video_channel_users": None,
                    "nsfw_level": None,
                    "mfa_level": None,
                },
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
        f = self.db.get_filters(original_guild_id=int(origin_guild_id))

        def _to_int_set(xs):
            out = set()
            for x in xs or []:
                try:
                    out.add(int(x))
                except Exception:
                    pass
            return out

        include_category_ids = _to_int_set(f["whitelist"]["category"])
        include_channel_ids = _to_int_set(f["whitelist"]["channel"])
        excluded_category_ids = _to_int_set(f["exclude"]["category"])
        excluded_channel_ids = _to_int_set(f["exclude"]["channel"])

        return {
            "include_category_ids": include_category_ids,
            "include_channel_ids": include_channel_ids,
            "excluded_category_ids": excluded_category_ids,
            "excluded_channel_ids": excluded_channel_ids,
            "whitelist_enabled": bool(include_category_ids or include_channel_ids),
        }

    def reload_filters_and_resend(self, guild_id: int | None):
        """
        Called when config filter sets were reloaded from DB.
        Rebuild and resend the sitemap so the server sees the new scope.
        If guild_id is provided, only mark that guild dirty.
        """
        self.schedule_sync(guild_id, delay=0.2)

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

        if wl_on and not (wl_ch or wl_cat):
            return True

        if ex_ch and not wl_ch:
            return True

        if ex_cat and not (wl_cat or wl_ch):
            return True

        return False

    def in_scope_channel(self, ch, cloned_guild_id: int | None = None) -> bool:
        """
        True if this channel/category/thread should be visible for its ORIGIN guild
        *and* (optionally) a specific clone mapping when cloned_guild_id is provided.
        """
        try:
            g = getattr(ch, "guild", None)
            if g is None and isinstance(ch, discord.Thread):
                parent = getattr(ch, "parent", None)
                g = getattr(parent, "guild", None)

            origin_gid = int(getattr(g, "id", 0) or 0)

            if cloned_guild_id is not None:
                view = self._build_filter_view_for_mapping(
                    origin_gid, int(cloned_guild_id)
                )
            else:
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

    def in_scope_thread(
        self, thr: discord.Thread, cloned_guild_id: int | None = None
    ) -> bool:
        """
        True if this thread's parent channel is allowed for its origin guild,
        using per-mapping filters when a clone is specified.
        """
        try:
            parent = getattr(thr, "parent", None)
            if parent is None:
                return False

            g = getattr(parent, "guild", None)
            origin_gid = int(getattr(g, "id", 0) or 0)

            if cloned_guild_id is not None:
                view = self._build_filter_view_for_mapping(
                    origin_gid, int(cloned_guild_id)
                )
            else:
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
        """Ignore @everyone and managed roles; check for meaningful changes."""
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
            if before.position != after.position:
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

            async with self._send_lock:
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
        standalone_channels = sitemap.get("standalone_channels", [])
        forum_entries = sitemap.get("forums", [])
        thread_entries = sitemap.get("threads", [])

        kept_categories: List[Dict[str, Any]] = []
        kept_standalones: List[Dict[str, Any]] = []
        kept_forums: List[Dict[str, Any]] = []
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
            chan_list = cat.get("channels", [])

            valid_channels: List[Dict[str, Any]] = []

            for ch in chan_list:
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
                kept_categories.append({**cat, "channels": valid_channels})
            else:

                cat_excluded = cat_id in excluded_category_ids
                cat_whitelisted = (
                    (cat_id in include_category_ids) if wl_on_global else True
                )

                if cat_excluded or not cat_whitelisted:

                    dropped_channels.append(
                        {
                            "category_id": str(cat_id),
                            "channel_id": None,
                            "name": cat.get("name"),
                            "reason": _why_drop(cat_id, None),
                        }
                    )
                else:
                    kept_categories.append({**cat, "channels": []})
                    dropped_channels.append(
                        {
                            "category_id": str(cat_id),
                            "channel_id": None,
                            "name": cat.get("name"),
                            "reason": "children-filtered",
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

        for fm in forum_entries:
            fm_id = int(fm["id"])
            parent_cat_id_raw = fm.get("category_id")
            parent_cat_id = int(parent_cat_id_raw) if parent_cat_id_raw else None

            blacklisted = (
                (parent_cat_id in excluded_category_ids) if parent_cat_id else False
            ) or (fm_id in excluded_channel_ids)
            whitelisted = (
                (parent_cat_id in include_category_ids) if parent_cat_id else False
            ) or (fm_id in include_channel_ids)

            keep_forum = True
            if wl_on_global and not whitelisted:
                keep_forum = False
            if blacklisted:
                keep_forum = False

            if keep_forum:
                kept_forums.append(fm)
            else:
                dropped_channels.append(
                    {
                        "category_id": (
                            str(parent_cat_id) if parent_cat_id is not None else None
                        ),
                        "channel_id": str(fm_id),
                        "name": fm.get("name"),
                        "reason": _why_drop(parent_cat_id, fm_id),
                    }
                )

        for th in thread_entries:
            parent_id_raw = th.get("forum_id")
            parent_id = int(parent_id_raw) if parent_id_raw else 0
            th_id = int(th["id"])

            blacklisted = (parent_id in excluded_channel_ids) or (
                th_id in excluded_channel_ids
            )
            whitelisted = (parent_id in include_channel_ids) or (
                th_id in include_channel_ids
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
        sitemap["standalone_channels"] = kept_standalones
        sitemap["forums"] = kept_forums
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
