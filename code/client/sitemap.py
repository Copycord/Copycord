# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
import asyncio
import logging
from typing import List, Dict, Optional
import discord


class SitemapService:
    """
    Builds the guild sitemap and applies whitelist/exclude filtering.
    Also exposes helpers used by the client:
      - in_scope_channel / in_scope_thread
      - role_change_is_relevant
      - schedule_sync (debounced)
    """

    def __init__(
        self,
        bot: discord.Bot,
        config,
        db,
        ws,
        host_guild_id: int,
        logger: Optional[logging.Logger] = None,
    ):
        self.bot = bot
        self.config = config
        self.db = db
        self.ws = ws
        self.host_guild_id = int(host_guild_id)
        self.logger = logger or logging.getLogger("client.sitemap")
        self._debounce_task: asyncio.Task | None = None

    # ----------------------------
    # Public API
    # ----------------------------

    def schedule_sync(self, delay: float = 1.0) -> None:
        """Debounced sitemap send."""
        if self._debounce_task is None:
            self._debounce_task = asyncio.create_task(self._debounced(delay))

    async def build_and_send(self) -> None:
        """Build, filter, and send the sitemap via websocket."""
        sitemap = await self.build()
        if not sitemap:
            return
        await self.ws.send({"type": "sitemap", "data": sitemap})
        self.logger.info("[📩] Sitemap sent to Server")

    async def build(self) -> Dict:
        """Build the raw sitemap, then filter it per config."""
        guild = self.bot.get_guild(self.host_guild_id)
        if not guild:
            self.logger.error("[⛔] Guild %s not found", self.host_guild_id)
            return {}

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

        # Stickers
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

        # Base structure
        sitemap: Dict = {
            "categories": [],
            "standalone_channels": [],
            "forums": [],
            "threads": [],
            "emojis": [
                {"id": e.id, "name": e.name, "url": str(e.url), "animated": e.animated}
                for e in guild.emojis
            ],
            "stickers": stickers_payload,
            "roles": [
                {
                    "id": r.id,
                    "name": r.name,
                    "permissions": r.permissions.value,
                    "color": r.color.value if hasattr(r.color, "value") else int(r.color),
                    "hoist": r.hoist,
                    "mentionable": r.mentionable,
                    "managed": r.managed,
                    "everyone": (r == r.guild.default_role),
                    "position": r.position,
                }
                for r in guild.roles
            ],
            "community": {
                "enabled": "COMMUNITY" in guild.features,
                "rules_channel_id": (guild.rules_channel.id if guild.rules_channel else None),
                "public_updates_channel_id": (
                    guild.public_updates_channel.id if guild.public_updates_channel else None
                ),
            },
        }

        # Categories + text channels
        for cat in guild.categories:
            channels = [
                {"id": ch.id, "name": ch.name, "type": ch.type.value}
                for ch in cat.channels
                if isinstance(ch, discord.TextChannel)
            ]
            sitemap["categories"].append(
                {"id": cat.id, "name": cat.name, "channels": channels}
            )

        # Standalone text channels (no category)
        sitemap["standalone_channels"] = [
            {"id": ch.id, "name": ch.name, "type": ch.type.value}
            for ch in guild.text_channels
            if ch.category is None
        ]

        # Forums
        for forum in getattr(guild, "forums", []):
            sitemap["forums"].append(
                {
                    "id": forum.id,
                    "name": forum.name,
                    "category_id": forum.category.id if forum.category else None,
                }
            )

        # Threads (from DB so we know forums we cloned)
        seen = {t["id"] for t in sitemap["threads"]}
        for row in self.db.get_all_threads():
            orig_tid = row["original_thread_id"]
            forum_orig = row["forum_original_id"]
            if orig_tid in seen:
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
                {"id": thr.id, "forum_id": forum_orig, "name": thr.name, "archived": thr.archived}
            )

        # Apply filters
        sitemap = self._filter_sitemap(sitemap)
        return sitemap

    # ----------------------------
    # Event helpers used by client
    # ----------------------------

    def in_scope_channel(self, ch) -> bool:
        """True if channel/category belongs in filtered sitemap."""
        try:
            if isinstance(ch, discord.CategoryChannel):
                return not self._is_filtered_out(None, ch.id)

            if isinstance(ch, discord.Thread):
                parent = getattr(ch, "parent", None)
                if parent is None:
                    return False
                cat_id = getattr(parent, "category_id", None)
                return not self._is_filtered_out(parent.id, cat_id)

            cat_id = getattr(ch, "category_id", None)
            return not self._is_filtered_out(getattr(ch, "id", None), cat_id)
        except Exception:
            return True

    def in_scope_thread(self, thr: discord.Thread) -> bool:
        """True if thread's parent survives filtering."""
        try:
            parent = getattr(thr, "parent", None)
            if parent is None:
                return False
            cat_id = getattr(parent, "category_id", None)
            return not self._is_filtered_out(getattr(parent, "id", None), cat_id)
        except Exception:
            return True

    def role_change_is_relevant(self, before: discord.Role, after: discord.Role) -> bool:
        """Ignore @everyone and managed roles; ignore position changes."""
        try:
            if after.is_default() or after.managed:
                return False
            if before.name != after.name:
                return True
            if getattr(before.permissions, "value", 0) != getattr(after.permissions, "value", 0):
                return True
            def _colval(c):
                try: return c.value
                except Exception: return int(c)
            if _colval(before.color) != _colval(after.color):
                return True
            if before.hoist != after.hoist:
                return True
            if before.mentionable != after.mentionable:
                return True
        except Exception:
            return True
        return False

    # ----------------------------
    # Internal debouncer
    # ----------------------------

    async def _debounced(self, delay: float):
        try:
            await asyncio.sleep(delay)
            await self.build_and_send()
        finally:
            self._debounce_task = None

    # ----------------------------
    # Filtering
    # ----------------------------

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
        allowed_ch  = bool(channel_id and channel_id in cfg.include_channel_ids)
        allowed_cat = bool(category_id and category_id in cfg.include_category_ids)
        ex_ch       = bool(channel_id and channel_id in cfg.excluded_channel_ids)
        ex_cat      = bool(category_id and category_id in cfg.excluded_category_ids)

        if wl_on and not (allowed_ch or allowed_cat):
            return "blocked by whitelist (not listed)"
        if wl_on and allowed_cat and not allowed_ch and ex_ch:
            return "carve-out: excluded channel under whitelisted category"
        if ex_ch and not allowed_ch:
            return "excluded channel"
        if ex_cat and not (allowed_cat or allowed_ch):
            return "excluded category"
        return "allowed"

    def _filter_sitemap(self, sitemap: dict) -> dict:
        self._log_filter_settings()
        wl_on   = bool(self.config.whitelist_enabled)
        inc_cats = getattr(self.config, "include_category_ids", set())

        kept_cat_cnt = kept_chan_cnt = kept_standalone_cnt = kept_forum_cnt = kept_thread_cnt = 0
        drop_cat_cnt = drop_chan_cnt = drop_standalone_cnt = drop_forum_cnt = drop_thread_cnt = 0

        # Categories (+ text channels)
        new_categories = []
        for cat in sitemap.get("categories", []):
            cat_id = int(cat["id"])
            cat_name = cat.get("name", str(cat_id))

            kept_children = []
            for ch in cat.get("channels", []):
                ch_id = int(ch["id"])
                ch_name = ch.get("name", str(ch_id))
                if self._is_filtered_out(ch_id, cat_id):
                    drop_chan_cnt += 1
                    self.logger.debug(
                        "[filter] drop channel %s (%d) in category %s (%d): %s",
                        ch_name, ch_id, cat_name, cat_id, self._filter_reason(ch_id, cat_id)
                    )
                else:
                    kept_children.append(ch)
                    kept_chan_cnt += 1

            if kept_children:
                new_categories.append({**cat, "channels": kept_children})
                kept_cat_cnt += 1
                continue

            if self._is_filtered_out(None, cat_id):
                drop_cat_cnt += 1
                self.logger.debug(
                    "[filter] drop category %s (%d): %s",
                    cat_name, cat_id, self._filter_reason(None, cat_id)
                )
                continue

            keep_empty = (not wl_on) or (cat_id in inc_cats)
            if keep_empty:
                new_categories.append({**cat, "channels": []})
                kept_cat_cnt += 1
                self.logger.debug(
                    "[filter] keep empty category shell %s (%d) [keep_empty=%s wl_on=%s cat_in_wl=%s]",
                    cat_name, cat_id, keep_empty, wl_on, (cat_id in inc_cats)
                )
            else:
                drop_cat_cnt += 1
                self.logger.debug(
                    "[filter] drop empty category %s (%d): no kept children and keep_empty=False",
                    cat_name, cat_id
                )

        # Standalone channels
        standalones = []
        for ch in sitemap.get("standalone_channels", []):
            ch_id = int(ch["id"])
            ch_name = ch.get("name", str(ch_id))
            if self._is_filtered_out(ch_id, None):
                drop_standalone_cnt += 1
                self.logger.debug(
                    "[filter] drop standalone channel %s (%d): %s",
                    ch_name, ch_id, self._filter_reason(ch_id, None)
                )
            else:
                standalones.append(ch)
                kept_standalone_cnt += 1

        # Forums
        forums = []
        for f in sitemap.get("forums", []):
            f_id = int(f["id"])
            f_name = f.get("name", str(f_id))
            f_cat_id = int(f.get("category_id") or 0) or None
            if self._is_filtered_out(f_id, f_cat_id):
                drop_forum_cnt += 1
                self.logger.debug(
                    "[filter] drop forum %s (%d) under cat_id=%s: %s",
                    f_name, f_id, f_cat_id, self._filter_reason(f_id, f_cat_id)
                )
            else:
                forums.append(f)
                kept_forum_cnt += 1

        # Threads only for kept forums
        keep_forum_ids = {int(f["id"]) for f in forums}
        threads = []
        for t in sitemap.get("threads", []):
            t_id = int(t["id"])
            forum_id = int(t.get("forum_id", 0)) or 0
            if forum_id in keep_forum_ids:
                threads.append(t)
                kept_thread_cnt += 1
            else:
                drop_thread_cnt += 1
                self.logger.debug(
                    "[filter] drop thread %s (%d): parent forum %d not kept",
                    t.get("name", str(t_id)), t_id, forum_id
                )

        self.logger.debug(
            "[filter] kept: categories=%d channels=%d standalones=%d forums=%d threads=%d | "
            "dropped: categories=%d channels=%d standalones=%d forums=%d threads=%d",
            kept_cat_cnt, kept_chan_cnt, kept_standalone_cnt, kept_forum_cnt, kept_thread_cnt,
            drop_cat_cnt, drop_chan_cnt, drop_standalone_cnt, drop_forum_cnt, drop_thread_cnt
        )

        out = dict(sitemap)
        out["categories"] = new_categories
        out["standalone_channels"] = standalones
        out["forums"] = forums
        out["threads"] = threads
        return out

    # Core predicate used everywhere
    def _is_filtered_out(self, channel_id: int | None, category_id: int | None) -> bool:
        """
        True if this (channel, category) should be dropped.
        Whitelist (if enabled) gates inclusion; excludes still apply, but practical
        precedence is:
          1) Channel whitelist > channel exclude
          2) Channel exclude > category whitelist  (carve-out)
          3) Category whitelist > category exclude
        """
        cfg = self.config

        # Whitelist gate
        if cfg.whitelist_enabled:
            allowed = False
            if channel_id and channel_id in cfg.include_channel_ids:
                allowed = True
            elif category_id and category_id in cfg.include_category_ids:
                allowed = True
            if not allowed:
                return True

        # Channel exclude (whitelist beats it if channel explicitly whitelisted)
        if channel_id and channel_id in cfg.excluded_channel_ids:
            if channel_id in cfg.include_channel_ids:
                return False
            if category_id and category_id in cfg.include_category_ids:
                # carve-out: category WL does NOT override channel exclude
                return True
            return True

        # Category exclude (overridden by either category WL or channel WL)
        if category_id and category_id in cfg.excluded_category_ids:
            if category_id in cfg.include_category_ids:
                return False
            if channel_id and channel_id in cfg.include_channel_ids:
                return False
            return True

        return False

    # Back-compat alias
    def is_excluded_ids(self, channel_id: int | None, category_id: int | None) -> bool:
        return self._is_filtered_out(channel_id, category_id)
