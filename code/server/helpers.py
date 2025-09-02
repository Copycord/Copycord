# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
import base64
import contextlib
import gzip
import asyncio
import io
import json
import logging
import time
import random
from discord.ext import commands
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import uuid
import discord


class OnJoinService:
    DEFAULT_COLORS = [
        discord.Color.blurple(),
        discord.Color.blue(),
        discord.Color.teal(),
        discord.Color.green(),
        discord.Color.gold(),
        discord.Color.orange(),
        discord.Color.purple(),
        discord.Color.red(),
        discord.Color.fuchsia(),
    ]

    def __init__(
        self,
        bot: discord.Client,
        db,
        logger: Optional[logging.Logger] = None,
        *,
        colors: Optional[Iterable[discord.Color]] = None,
        color_strategy: str = "random",  # "random" | "seed_guild" | "seed_user"
    ) -> None:
        self.bot = bot
        self.db = db
        self.log = (logger or logging.getLogger(__name__)).getChild(self.__class__.__name__)
        self._palette = list(colors) if colors else list(self.DEFAULT_COLORS)
        self._color_strategy = color_strategy

    # ----------------------------- public entrypoint -----------------------------

    async def handle_member_joined(self, data: dict) -> None:
        try:
            guild_id = int(data.get("guild_id") or 0)
            guild_name = data.get("guild_name") or str(guild_id)
            user_id = int(data.get("user_id") or 0)
            display = data.get("display_name") or data.get("username") or str(user_id)
            avatar = data.get("avatar_url")
            joined_iso = data.get("joined_at")

            ts = int(
                (
                    datetime.fromisoformat(joined_iso).timestamp()
                    if joined_iso
                    else datetime.now(timezone.utc).timestamp()
                )
            )

            targets = self.db.get_onjoin_users(guild_id)
            if not targets:
                return

            embed = self.build_embed(
                display_name=display,
                user_id=user_id,
                guild_name=guild_name,
                when_unix=ts,
                avatar_url=avatar,
            )

            await self._fanout_dm(targets, embed, guild_id)
        except Exception:
            self.log.exception("handle_member_joined failed")

    # --------------------------------- helpers ----------------------------------

    async def _fanout_dm(self, user_ids: Iterable[int], embed: discord.Embed, guild_id: int) -> None:
        for uid in user_ids:
            try:
                u = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
                await u.send(embed=embed)
                self.log.info("[🔔] On-join DM sent to %s for guild %s", uid, guild_id)
            except Exception as ex:
                self.log.warning("[⚠️] Failed DM to %s for guild %s: %s", uid, guild_id, ex)

    # ------------------------------ embed building ------------------------------

    def build_embed(
        self,
        *,
        display_name: str,
        user_id: int,
        guild_name: str,
        when_unix: int,
        avatar_url: Optional[str] = None,
        color: Optional[discord.Color] = None,
    ) -> discord.Embed:
        color = color or self._pick_color(guild_id=None, user_id=user_id)

        desc = f"**{display_name}** just joined **{guild_name}**\n\n"
        desc += f"> **User**: <@{user_id}> (`{user_id}`)\n"
        desc += f"> **When**: <t:{when_unix}:R>"

        e = discord.Embed(
            description=desc,
            timestamp=datetime.fromtimestamp(when_unix, tz=timezone.utc),
            color=color,
        )

        if avatar_url:
            e.set_thumbnail(url=avatar_url)

        return e

    def _pick_color(self, *, guild_id: Optional[int], user_id: Optional[int]) -> discord.Color:
        if not self._palette:
            return discord.Color(random.randint(0, 0xFFFFFF))

        strat = (self._color_strategy or "random").lower()
        if strat == "random":
            return random.choice(self._palette)

        if strat == "seed_guild" and guild_id:
            idx = hash(guild_id) % len(self._palette)
            return self._palette[idx]

        if strat == "seed_user" and user_id:
            idx = hash(user_id) % len(self._palette)
            return self._palette[idx]

        return random.choice(self._palette)
    
        
class VerifyController:
    """
    Observability upgrades:
      • Start/stop logs with task state
      • Bus subscribe/publish success/failure + payload preview
      • Per-request request_id to tie UI action → results
      • Timings for list/delete operations (ms)
      • Counts for orphans found / deleted
      • Ratellimiter waits surfaced
      • Clear logs on 'guild not found' / unknown actions
      • Robust exception logs with action context
    """

    def __init__(
        self,
        *,
        bus,                                 # AdminBus
        admin_base_url: str,                 # e.g. ws://host:8000
        bot,                                 # discord.Client | discord.AutoShardedClient | commands.Bot
        guild_id: int,
        db,                                  # your DBManager
        ratelimit,                           # rate limiter with acquire(ActionType.DELETE_CHANNEL)
        get_protected_channel_ids: Callable[[discord.Guild], Iterable[int]],
        action_type_delete_channel,          # pass in ActionType.DELETE_CHANNEL
        logger: Optional[logging.Logger] = None,
    ):
        self.bus = bus
        self.admin_base_url = admin_base_url.rstrip("/")
        self.bot = bot
        self.guild_id = int(guild_id)
        self.db = db
        self.ratelimit = ratelimit
        self._get_protected = get_protected_channel_ids
        self._AT_DELETE = action_type_delete_channel
        self.log = logger or logging.getLogger("VerifyController")
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    # -------- lifecycle --------
    def start(self) -> None:
        if self._task and not self._task.done():
            self.log.debug("start() called but listener already running (task=%s)", self._task.get_name())
            return
        self._stopping = False
        self._task = asyncio.create_task(self._listen_loop(), name="verify-listen")
        self.log.debug("VerifyController started | task=%s guild_id=%s admin_base_url=%s",
                      self._task.get_name(), self.guild_id, self.admin_base_url)

    async def stop(self) -> None:
        self._stopping = True
        if not self._task:
            self.log.debug("stop() called but no listener task present")
            return
        self.log.debug("VerifyController stopping | task=%s", self._task.get_name())
        self._task.cancel()
        with contextlib.suppress(Exception):
            await self._task
        self._task = None
        self.log.debug("VerifyController stopped")

    # -------- internals --------
    @staticmethod
    def _ms_since(t0: float) -> float:
        return (time.perf_counter() - t0) * 1000.0

    def _new_req_id(self) -> str:
        # short request id to correlate logs
        return uuid.uuid4().hex[:8]

    # -------- bus I/O --------
    async def _listen_loop(self):
        async def _handler(ev: dict):
            # Only handle verify requests coming from UI
            if ev.get("kind") != "verify" or ev.get("role") != "ui":
                return
            payload = ev.get("payload") or {}
            req_id = payload.get("req_id") or self._new_req_id()
            try:
                self.log.debug("RX verify UI event | req_id=%s payload=%s", req_id, _safe_preview(payload))
                await self._handle(payload, req_id=req_id)
            except Exception as e:
                self.log.exception("Unhandled error while handling verify payload | req_id=%s err=%s payload=%s",
                                   req_id, e, _safe_preview(payload))
                # surface error to UI (non-fatal)
                await self._publish({"type": "error", "req_id": req_id, "message": str(e)})

        # Reconnect-with-backoff handled by AdminBus.subscribe()
        try:
            self.log.debug("Subscribing to admin bus | url=%s path=/bus", self.admin_base_url)
            await self.bus.subscribe(self.admin_base_url, _handler)
        except asyncio.CancelledError:
            self.log.debug("_listen_loop cancelled")
            raise
        except Exception as e:
            self.log.exception("Fatal error in _listen_loop subscribe | err=%s", e)
            # graceful stop; outer supervisor may restart us
            await asyncio.sleep(0.5)

    async def _publish(self, payload: dict):
        # attach a default req_id if missing so UI can correlate
        payload = dict(payload or {})
        payload.setdefault("req_id", self._new_req_id())
        t0 = time.perf_counter()
        try:
            await self.bus.publish("verify", payload)
            self.log.debug("TX verify event -> bus | ok req_id=%s took=%.1fms payload=%s",
                           payload.get("req_id"), self._ms_since(t0), _safe_preview(payload))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.warning("TX verify event -> bus FAILED | req_id=%s took=%.1fms err=%s payload=%s",
                             payload.get("req_id"), self._ms_since(t0), e, _safe_preview(payload))

    # -------- actions --------
    async def _handle(self, payload: dict, *, req_id: str):
        act = (payload.get("action") or "").lower()
        t0 = time.perf_counter()
        guild = self.bot.get_guild(self.guild_id)

        if not guild:
            self.log.warning("Guild not found | req_id=%s guild_id=%s action=%s", req_id, self.guild_id, act)
            await self._publish({"type": "orphans", "req_id": req_id, "categories": [], "channels": []})
            return

        if act == "list":
            ct0 = time.perf_counter()
            cats, chs = self._compute_orphans(guild)
            self.log.debug("Orphans listed | req_id=%s guild=%s cats=%d chs=%d took=%.1fms",
                          req_id, guild.id, len(cats), len(chs), self._ms_since(ct0))
            await self._publish({"type": "orphans", "req_id": req_id, "categories": cats, "channels": chs})
            self.log.debug("Action complete | req_id=%s action=%s total=%.1fms", req_id, act, self._ms_since(t0))
            return

        if act == "delete_one":
            kind = (payload.get("kind") or "").lower()
            _id = int(payload.get("id") or 0)
            self.log.debug("Delete one requested | req_id=%s kind=%s id=%s", req_id, kind, _id)
            results = await self._delete_ids(guild, [(_id, kind)], req_id=req_id)
            await self._publish({
                "type": "deleted",
                "req_id": req_id,
                "ok": True,
                "results": results,    # << includes name + reason
            })
            self.log.debug("Action complete | req_id=%s action=%s deleted=%d total=%.1fms",
                        req_id, act, sum(1 for r in results if r["deleted"]), self._ms_since(t0))
            return

        if act == "delete_all":
            raw_ids = payload.get("ids") or []
            ids = [int(x) for x in raw_ids if str(x).isdigit()]
            self.log.info("Delete all requested | req_id=%s ids=%d", req_id, len(ids))

            kind_map = {int(c.id): "category" for c in guild.categories}
            for ch in guild.channels:
                if not isinstance(ch, discord.CategoryChannel):
                    kind_map[int(ch.id)] = "channel"

            targets = [(i, kind_map.get(i, "channel")) for i in ids]
            results = await self._delete_ids(guild, targets, req_id=req_id)
            await self._publish({
                "type": "deleted",
                "req_id": req_id,
                "ok": True,
                "results": results,    # << includes name + reason
            })
            self.log.debug("Action complete | req_id=%s action=%s requested=%d deleted=%d total=%.1fms",
                        req_id, act, len(ids), sum(1 for r in results if r["deleted"]), self._ms_since(t0))
            return

        # unknown action
        self.log.warning("Unknown verify action | req_id=%s action=%r payload=%s",
                         req_id, act, _safe_preview(payload))

    # -------- helpers --------
    async def _resolve_channel_like(self, _id: int, guild: discord.Guild):
        # Prefer cached object first
        obj = getattr(guild, "get_channel_or_thread", guild.get_channel)(int(_id))
        if obj is not None:
            return obj

        # Fallback to REST (works for channels and threads)
        try:
            return await self.bot.fetch_channel(int(_id))  # may return TextChannel/VoiceChannel/Thread
        except discord.NotFound:
            self.log.info("Target not found (404) | id=%s", _id)
        except discord.Forbidden:
            self.log.info("Forbidden fetching target | id=%s (missing permissions?)", _id)
        except Exception as e:
            self.log.warning("Error fetching target | id=%s err=%s", _id, e)
        return None

    def _compute_orphans(self, guild: discord.Guild) -> tuple[list[dict], list[dict]]:
        # timings per phase (DB scan vs guild snapshot)
        t0 = time.perf_counter()
        mapped_cats = {
            int(r["cloned_category_id"])
            for r in self.db.get_all_category_mappings()
            if r["cloned_category_id"] is not None
        }
        mapped_chs = {
            int(r["cloned_channel_id"])
            for r in self.db.get_all_channel_mappings()
            if r["cloned_channel_id"] is not None
        }
        db_ms = self._ms_since(t0)

        g0 = time.perf_counter()
        orphan_categories = [
            {"id": str(int(c.id)), "name": c.name}
            for c in guild.categories
            if int(c.id) not in mapped_cats
        ]

        orphan_channels: list[dict] = []
        for ch in guild.channels:
            if isinstance(ch, discord.CategoryChannel):
                continue
            if int(ch.id) in mapped_chs:
                continue

            cat_id = self._category_id_of(ch)       # int or None
            cat_name = self._category_name_of(ch)   # str or None

            orphan_channels.append({
                "id": str(int(ch.id)),
                "name": getattr(ch, "name", f"#{ch.id}"),
                # provide BOTH id & name so UI can group correctly
                "category_id": str(int(cat_id)) if cat_id is not None else None,
                "category_name": cat_name,
                # optional: pass a channel "type" if you want nicer labels on UI
                "type": getattr(ch, "type", None).value if getattr(getattr(ch, "type", None), "value", None) is not None else None,
            })

        guild_ms = self._ms_since(g0)

        self.log.debug(
            "Computed orphans | guild=%s db_ms=%.1f guild_ms=%.1f cats=%d chs=%d",
            guild.id, db_ms, guild_ms, len(orphan_categories), len(orphan_channels)
        )
        return orphan_categories, orphan_channels


    async def _delete_ids(self, guild: discord.Guild, targets: list[tuple[int, str]], *, req_id: str):
        """
        Returns a list of dicts:
        { id:int, kind:str, name:str, deleted:bool, reason:str }  # reason when deleted == False
            reason ∈ {"protected","not_found","not_category","not_channel","error"}
        """
        results: list[dict] = []
        try:
            protected = set(self._get_protected(guild))
        except Exception as e:
            protected = set()
            self.log.warning("get_protected_channel_ids failed | req_id=%s err=%s", req_id, e)

        for _id, kind in targets:
            try:
                ch = guild.get_channel(int(_id))  # category OR channel OR None
                if not ch:
                    results.append({"id": int(_id), "kind": kind or "channel",
                                    "name": f"#{_id}", "deleted": False, "reason": "not_found"})
                    self.log.debug("Skip (not found) | req_id=%s id=%s", req_id, _id)
                    continue

                # Typed branches
                if kind == "category":
                    if not isinstance(ch, discord.CategoryChannel):
                        results.append({"id": int(_id), "kind": "category",
                                        "name": getattr(ch, "name", f"#{_id}"),
                                        "deleted": False, "reason": "not_category"})
                        self.log.debug("Skip (not category) | req_id=%s id=%s", req_id, _id)
                        continue

                    wait_t0 = time.perf_counter()
                    await self.ratelimit.acquire(self._AT_DELETE)
                    wait_ms = self._ms_since(wait_t0)
                    op_t0 = time.perf_counter()
                    await ch.delete()
                    self.log.info("[🗑️] Deleted orphan Category | req_id=%s name=%s id=%d wait_ms=%.1f op_ms=%.1f",
                                req_id, ch.name, ch.id, wait_ms, self._ms_since(op_t0))
                    results.append({"id": int(_id), "kind": "category",
                                    "name": ch.name, "deleted": True})

                else:
                    # treat everything else as “channel”
                    if isinstance(ch, discord.CategoryChannel):
                        results.append({"id": int(_id), "kind": "channel",
                                        "name": getattr(ch, "name", f"#{_id}"),
                                        "deleted": False, "reason": "not_channel"})
                        self.log.debug("Skip (is category not channel) | req_id=%s id=%s", req_id, _id)
                        continue

                    if ch.id in protected:
                        self.log.info("[🛡️] Skipping protected channel | req_id=%s name=%s id=%d",
                                    req_id, getattr(ch, "name", "?"), ch.id)
                        results.append({"id": int(_id), "kind": "channel",
                                        "name": getattr(ch, "name", f"#{_id}"),
                                        "deleted": False, "reason": "protected"})
                        continue

                    wait_t0 = time.perf_counter()
                    await self.ratelimit.acquire(self._AT_DELETE)
                    wait_ms = self._ms_since(wait_t0)
                    op_t0 = time.perf_counter()
                    await ch.delete()
                    self.log.info("[🗑️] Deleted orphan %s | req_id=%s name=%s id=%d wait_ms=%.1f op_ms=%.1f",
                                type(ch).__name__, req_id, getattr(ch, "name", "?"),
                                ch.id, wait_ms, self._ms_since(op_t0))
                    results.append({"id": int(_id), "kind": "channel",
                                    "name": getattr(ch, "name", f"#{_id}"), "deleted": True})

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.warning("[⚠️] Failed to delete %s | req_id=%s id=%d err=%s",
                                kind, req_id, _id, e)
                results.append({"id": int(_id), "kind": kind or "channel",
                                "name": f"#{_id}", "deleted": False, "reason": "error"})

        # Final tally
        deleted_count = sum(1 for r in results if r["deleted"])
        self.log.debug("Delete finished | req_id=%s requested=%d deleted=%d",
                    req_id, len(targets), deleted_count)
        return results
    
    def _category_id_of(self, ch) -> Optional[int]:
        """
        Return the category ID for a discord channel/thread, if any.
        - Text/Voice/Forum/Stage: ch.category_id
        - Threads: ch.parent.category_id
        """
        # direct attribute for most channels
        cid = getattr(ch, "category_id", None)
        if cid is not None:
            try:
                return int(cid)
            except Exception:
                return None

        # threads: look at parent channel's category_id
        parent = getattr(ch, "parent", None)
        if parent is not None:
            try:
                pcid = getattr(parent, "category_id", None)
                return int(pcid) if pcid is not None else None
            except Exception:
                return None
        return None


    def _category_name_of(self, ch) -> Optional[str]:
        """
        Return the category name for a discord channel/thread, if any.
        """
        cat = getattr(ch, "category", None)
        if cat is not None and getattr(cat, "name", None):
            return cat.name

        # threads: derive from parent channel's category
        parent = getattr(ch, "parent", None)
        if parent is not None:
            pcat = getattr(parent, "category", None)
            if pcat is not None and getattr(pcat, "name", None):
                return pcat.name
        return None


class PurgeAssetHelper:
    def __init__(self, cog: commands.Cog):
        self.cog = cog
        self.logger = getattr(cog, "logger", None)

    def _log_purge_event(
        self,
        *,
        kind: str,
        outcome: str,
        guild_id: int | None = None,
        user_id: int | None = None,
        obj_id: int | None = None,
        name: str | None = None,
        reason: str | None = None,
        counts: dict | None = None,
    ) -> None:
        """Structured purge event logging."""
        lg = self.logger or __import__("logging").getLogger(__name__)
        msg = f"[purge:{kind}] {outcome}"
        extras = {
            "guild": guild_id,
            "by": user_id,
            "id": obj_id,
            "name": name,
            "reason": reason,
            **(counts or {}),
        }
        if outcome in ("begin", "db_clear", "done"):
            lg.warning(f"{msg} {extras}")
        elif outcome == "deleted":
            lg.info(f"{msg} {extras}")
        elif outcome == "skipped":
            lg.debug(f"{msg} {extras}")
        elif outcome == "failed":
            lg.error(f"{msg} {extras}")
        else:
            lg.debug(f"{msg} {extras}")

    async def _resolve_me_and_top(self, guild: discord.Guild):
        """Resolve bot’s Member, its top role, and all roles."""
        me = getattr(guild, "me", None) or guild.get_member(self.cog.bot.user.id)
        if me is None:
            try:
                me = await guild.fetch_member(self.cog.bot.user.id)
            except discord.HTTPException:
                me = None
        try:
            roles = await guild.fetch_roles()
        except Exception:
            roles = list(guild.roles)
        top_role = None
        if me and me.top_role:
            for r in roles:
                if r.id == me.top_role.id:
                    top_role = r
                    break
        return me, top_role, roles

# -------- standalone helpers --------
def _safe_preview(obj) -> str:
    """Shorten & sanitize dicts for logs."""
    try:
        s = json.dumps(obj, ensure_ascii=False) if isinstance(obj, (dict, list)) else str(obj)
    except Exception:
        s = str(obj)
    if len(s) > 500:
        return s[:500] + "…"
    return s