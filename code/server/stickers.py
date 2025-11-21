# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
from typing import Tuple, List, Optional
import io
import asyncio
import discord
import aiohttp
import logging
from server.rate_limiter import ActionType
from server import logctx

logger = logging.getLogger("server.stickers")


class StickerManager:
    def __init__(
        self,
        bot,
        db,
        guild_resolver,
        ratelimit,
        clone_guild_id: int | None = None,
        session=None,
    ):
        self.bot = bot
        self.db = db
        self.ratelimit = ratelimit
        self.clone_guild_id = int(clone_guild_id or 0)
        self.session = session
        self.guild_resolver = guild_resolver
        self._state: dict[int, dict] = {}
        self._tasks: dict[int, asyncio.Task] = {}
        self._locks: dict[int, asyncio.Lock] = {}
        self._std_ok: set[int] = set()
        self._std_bad: set[int] = set()

    def _log(self, level: str, msg: str, *args):
        """
        Sticker sync logging with sync context.
        """
        prefix = logctx.format_prefix()
        line = prefix + msg

        if level == "debug":
            logger.debug(line, *args)
        elif level == "info":
            logger.info(line, *args)
        elif level == "warning":
            logger.warning(line, *args)
        elif level == "error":
            logger.error(line, *args)
        elif level == "exception":
            logger.exception(line, *args)
        else:
            logger.log(logging.INFO, line, *args)

    def set_session(self, session: aiohttp.ClientSession | None):
        self.session = session

    def _ensure_state(self, clone_gid: int) -> dict:
        """
        Get or create the state bucket for this clone guild.
        """
        if clone_gid not in self._state:
            self._state[clone_gid] = {
                "host_id": None,
                "sitemap": [],
                "cache": {},
                "cache_ts": None,
            }
        return self._state[clone_gid]

    def _get_lock_for_clone(self, clone_gid: int) -> asyncio.Lock:
        """
        Get/create the lock dedicated to this clone guild.
        We serialize sticker writes per clone guild, but allow
        different clone guilds to sync in parallel.
        """
        if clone_gid not in self._locks:
            self._locks[clone_gid] = asyncio.Lock()
        return self._locks[clone_gid]

    def set_last_sitemap(
        self,
        clone_guild_id: int,
        stickers: list[dict] | None,
        host_guild_id: int | None,
    ):
        """
        Record the latest upstream sticker sitemap for *this* clone guild.
        """
        st = self._ensure_state(int(clone_guild_id))
        st["sitemap"] = stickers or []
        st["host_id"] = int(host_guild_id) if host_guild_id else None

    async def refresh_cache(self, clone_gid: int) -> None:
        """
        Refresh the cached stickers list for a specific clone guild.
        """
        st = self._ensure_state(clone_gid)

        guild = self.bot.get_guild(clone_gid)
        if not guild:
            st["cache"] = {}
            st["cache_ts"] = None
            return

        try:
            stickers = await guild.fetch_stickers()
        except Exception:
            stickers = []

        st["cache"] = {int(s.id): s for s in stickers}

        st["cache_ts"] = None

    def kickoff_sync(
        self,
        target_clone_guild_id: int,
        *,
        validate_mapping: bool = True,
    ) -> None:
        """
        Start (or skip) a sync task for this specific clone guild.
        Multiple clone guilds can sync in parallel.
        """
        clone_gid = int(target_clone_guild_id)
        st = self._ensure_state(clone_gid)

        existing = self._tasks.get(clone_gid)
        if existing and not existing.done():
            self._log(
                "debug",
                "[🎟️] Sticker sync already running; skip kickoff.",
            )
            return

        host_id = st["host_id"]

        if validate_mapping and host_id:
            try:
                clones = set(self.guild_resolver.clones_for_host(host_id))
                if clones and clone_gid not in clones:
                    self._log(
                        "warning",
                        "[sticker] host %s is not mapped to clone %s; proceeding anyway",
                        host_id,
                        clone_gid,
                    )
            except Exception:
                self._log(
                    "exception",
                    "[sticker] mapping validation threw, continuing anyway",
                )

        guild = self.bot.get_guild(clone_gid)
        if not guild:
            self._log("debug", "[🎟️] Clone guild unavailable; aborting")
            return

        stickers = st["sitemap"] or []

        async def _run_one():
            await self._run_sync(
                guild=guild,
                stickers=stickers,
                host_id=host_id,
                clone_gid=clone_gid,
            )

        self._log(
            "debug",
            "[🎟️] Sticker sync scheduled",
        )
        task = asyncio.create_task(_run_one())
        self._tasks[clone_gid] = task

    async def _run_sync(
        self,
        guild: discord.Guild,
        stickers: list[dict],
        host_id: int | None,
        clone_gid: int,
    ) -> None:
        """
        Run the actual sync for a single clone guild.
        """
        lock = self._get_lock_for_clone(clone_gid)

        async with lock:
            try:
                upstream = len(stickers or [])
                try:
                    clone_list = await guild.fetch_stickers()
                except Exception:
                    clone_list = []
                mappings = len(list(self.db.get_all_sticker_mappings()))

                self._log(
                    "debug",
                    "[🎟️] Sticker sync start: upstream=%d, clone=%d, mappings=%d",
                    upstream,
                    len(clone_list),
                    mappings,
                )

                d, r, c = await self._sync(
                    guild=guild,
                    stickers=stickers or [],
                    host_id=host_id,
                )

                summary_parts = [
                    f"{label} {n}"
                    for (label, n) in (
                        ("Deleted", d),
                        ("Renamed", r),
                        ("Created", c),
                    )
                    if n
                ]
                if summary_parts:
                    await self.refresh_cache(clone_gid)
                    summary_text = ", ".join(summary_parts)
                else:
                    summary_text = "no changes needed"

                self._log(
                    "info",
                    "[🎟️] Sticker sync complete: %s",
                    summary_text,
                )

            except asyncio.CancelledError:
                self._log("debug", "[🎟️] Sticker sync canceled before completion.")
            except Exception as e:
                self._log("exception", "[🎟️] Sticker sync failed: %s", e)
            finally:

                t = self._tasks.get(clone_gid)
                if t and t.done():

                    pass
                else:
                    self._tasks.pop(clone_gid, None)

    async def _sync(
        self,
        guild: discord.Guild,
        stickers: list[dict],
        host_id: int | None,
    ) -> Tuple[int, int, int]:
        """
        Sync stickers for (host_id -> guild.id).
        Only touch mappings where original_guild_id == host_id
        AND cloned_guild_id == guild.id.
        """
        deleted = renamed = created = 0
        skipped_limit = size_failed = 0

        limit = getattr(guild, "sticker_limit", None)
        if not isinstance(limit, int):
            limit = 5

        try:
            clone_stickers = await guild.fetch_stickers()
        except Exception:
            clone_stickers = []
        current_count = len(clone_stickers)
        clone_by_id = {s.id: s for s in clone_stickers}

        rows = self.db.get_all_sticker_mappings()
        current: dict[int, dict] = {}
        for r in rows:
            row = dict(r)
            if (
                host_id is None
                or int(row.get("original_guild_id") or 0) == int(host_id)
            ) and int(row.get("cloned_guild_id") or 0) == int(guild.id):
                current[int(row["original_sticker_id"])] = row

        incoming = {int(s["id"]): s for s in stickers if s.get("id")}

        for orig_id in set(current) - set(incoming):
            row = current[orig_id]
            cloned = clone_by_id.get(row["cloned_sticker_id"])
            if cloned:
                try:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.STICKER_CREATE, guild.id
                    )
                    await cloned.delete()
                    deleted += 1
                    current_count = max(0, current_count - 1)
                    self._log(
                        "info",
                        "[🎟️] Deleted sticker %s",
                        row["cloned_sticker_name"],
                    )

                except discord.Forbidden:
                    self._log(
                        "warning",
                        "[⚠️] No permission to delete sticker %s",
                        row["cloned_sticker_name"],
                    )
                except discord.HTTPException as e:
                    self._log(
                        "error",
                        "[⛔] Error deleting sticker %s: %s",
                        row["cloned_sticker_name"],
                        e,
                    )

            self.db.delete_sticker_mapping_for_clone(orig_id, cloned_guild_id=guild.id)

        for orig_id, info in incoming.items():
            name = info.get("name") or f"sticker_{orig_id}"
            url = info.get("url") or ""
            mapping = current.get(orig_id)

            cloned = None
            if mapping:
                cloned = clone_by_id.get(mapping["cloned_sticker_id"])
                if mapping and not cloned:
                    self._log(
                        "warning",
                        "[⚠️] Sticker %s missing in clone; will recreate",
                        mapping["original_sticker_name"],
                    )

                    self.db.delete_sticker_mapping_for_clone(
                        orig_id, cloned_guild_id=guild.id
                    )
                    mapping = None

            if mapping and cloned and mapping["original_sticker_name"] != name:
                try:
                    await self.ratelimit.acquire_for_guild(
                        ActionType.STICKER_CREATE, guild.id
                    )
                    await cloned.edit(name=name)
                    renamed += 1
                    self.db.upsert_sticker_mapping(
                        orig_id,
                        name,
                        cloned.id,
                        cloned.name,
                        original_guild_id=host_id,
                        cloned_guild_id=guild.id,
                    )
                    self._log(
                        "info",
                        "[🎟️] Renamed sticker %s → %s",
                        mapping["original_sticker_name"],
                        name,
                    )

                except discord.HTTPException as e:
                    self._log(
                        "error",
                        "[⛔] Failed renaming sticker %s: %s",
                        cloned.name,
                        e,
                    )
                continue

            if mapping:
                continue

            if not url:
                self._log(
                    "warning",
                    "[⚠️] Sticker %s has no URL; skipping",
                    name,
                )
                continue

            if current_count >= limit:
                skipped_limit += 1
                continue

            raw = None
            try:
                if self.session is None or self.session.closed:
                    self.session = aiohttp.ClientSession()
                async with self.session.get(url) as resp:
                    raw = await resp.read()
            except Exception as e:
                self._log(
                    "error",
                    "[⛔] Failed fetching sticker %s at %s: %s",
                    name,
                    url,
                    e,
                )

                continue

            if raw and len(raw) > 512 * 1024:
                self._log(
                    "info",
                    "[🎟️] Skipping %s: exceeds size limit",
                    name,
                )
                size_failed += 1
                continue

            fmt = int((info.get("format_type") or 0))
            fname = f"{name}.json" if fmt == 3 else f"{name}.png"
            file = discord.File(io.BytesIO(raw), filename=fname)
            tag = (info.get("tags") or "🙂")[:50]
            desc = (info.get("description") or "")[:100]

            try:
                await self.ratelimit.acquire_for_guild(
                    ActionType.STICKER_CREATE, guild.id
                )
                created_stk = await guild.create_sticker(
                    name=name,
                    description=desc,
                    emoji=tag,
                    file=file,
                    reason="Copycord sync",
                )
                created += 1
                current_count += 1

                self.db.upsert_sticker_mapping(
                    orig_id,
                    name,
                    created_stk.id,
                    created_stk.name,
                    original_guild_id=host_id,
                    cloned_guild_id=guild.id,
                )

                self._log(
                    "info",
                    "[🎟️] Created sticker %s",
                    name,
                )

            except discord.HTTPException as e:

                if getattr(e, "code", None) == 30039 or "30039" in str(e):
                    skipped_limit += 1
                    self._log(
                        "info",
                        "[🎟️] Skipped creating sticker due to clone guild sticker limit.",
                    )

                else:
                    self._log(
                        "error",
                        "[⛔] Failed creating sticker %s: %s",
                        name,
                        e,
                    )

        if skipped_limit:
            self._log(
                "info",
                "[🎟️] Skipped %d stickers due to clone guild limit (%d).",
                skipped_limit,
                limit,
            )
        if size_failed:
            self._log(
                "info",
                "[🎟️] Skipped %d stickers because they exceed 512 KiB.",
                size_failed,
            )

        return deleted, renamed, created

    def resolve_cloned(
        self,
        clone_gid: int,
        stickers: list[dict],
    ) -> Tuple[List[discord.StickerItem], List[str]]:
        """
        For this clone guild only, return StickerItem objects for the given upstream stickers.
        """
        raw_rows = self.db.get_all_sticker_mappings()
        rows: dict[int, dict] = {}
        for r in raw_rows:
            row = dict(r)
            if int(row.get("cloned_guild_id") or 0) == int(clone_gid):
                rows[int(row["original_sticker_id"])] = row

        st = self._ensure_state(clone_gid)
        guild = self.bot.get_guild(clone_gid)

        items: List[discord.StickerItem] = []
        names: List[str] = []

        for s in stickers or []:
            try:
                orig_id = int(s.get("id"))
            except Exception:
                continue
            row = rows.get(orig_id)
            if not row:
                continue

            clone_id = int(row["cloned_sticker_id"])
            stk = st["cache"].get(clone_id)
            if not stk and guild:
                stk = next(
                    (
                        cs
                        for cs in getattr(guild, "stickers", [])
                        if int(cs.id) == clone_id
                    ),
                    None,
                )
            if not stk:
                continue

            items.append(discord.Object(id=stk.id))
            names.append(getattr(stk, "name", s.get("name", "sticker")))

        return items, names

    def _compose_content(self, author: str, base_content: str | None) -> str:
        """Always prefix with From {author}:, even if there is no content."""
        base = (base_content or "").strip()
        return f"From {author}: {base}" if base else f"From {author}:"

    async def try_send_standard(
        self,
        channel: discord.abc.Messageable,
        author: str,
        stickers: list[dict],
        base_content: str | None = None,
    ) -> bool:
        """Attempt to send default (global) stickers by original ID."""
        cand_ids: list[int] = []
        for s in (stickers or [])[:3]:
            try:
                sid = int(s.get("id"))
            except Exception:
                continue
            if sid in self._std_bad:
                continue
            cand_ids.append(sid)

        if not cand_ids:
            return False

        content = self._compose_content(author, base_content)

        try:
            await channel.send(
                content=content, stickers=[discord.Object(id=i) for i in cand_ids]
            )
            self._std_ok.update(cand_ids)
            return True
        except discord.HTTPException:
            self._std_bad.update(cand_ids)
            return False
        except Exception:
            self._std_bad.update(cand_ids)
            return False

    def _is_image_url(self, u: str | None) -> bool:
        """
        Checks if the given URL corresponds to an image file based on its extension.
        """
        if not u:
            return False
        u = u.lower()
        return u.endswith((".png", ".webp", ".gif", ".apng", ".jpg", ".jpeg"))

    def lookup_original_urls(
        self,
        clone_gid: int,
        stickers: list[dict],
    ) -> list[tuple[str, str]]:
        """
        For each incoming sticker (up to 3), try to find original CDN URLs
        that we saw in that clone guild's last sitemap from its host.
        """
        st = self._ensure_state(clone_gid)
        last_map = st["sitemap"] or []
        if not last_map:
            return []

        by_id: dict[int, dict] = {}
        for row in last_map:
            rid = row.get("id")
            url = row.get("url")
            if rid is None or not url:
                continue
            try:
                rid_int = int(rid)
            except Exception:
                continue
            by_id[rid_int] = row

        out: list[tuple[str, str]] = []
        for s in (stickers or [])[:3]:
            sid = s.get("id")
            try:
                sid_int = int(sid)
            except Exception:
                continue
            row = by_id.get(sid_int)
            if not row:
                continue
            url = row.get("url")
            if not url or not self._is_image_url(url):
                continue

            name = row.get("name") or s.get("name") or "sticker"
            out.append((name, url))

        return out

    def _backfill_suffix(self, receiver, source_id: int, msg: dict) -> str:
        """
        Format a '[N left]' or '[N sent]' suffix for sticker logs when running
        under backfill. This does NOT mutate progress; it just reads it.
        """
        try:
            bf = getattr(receiver, "backfill", None)
            if not bf or not msg.get("__backfill__"):
                return ""

            delivered, total = bf.get_progress(int(source_id))
            if total is not None:
                left = max(int(total) - int(delivered), 0)
                return f" [{left} left]"
            else:
                return f" [{delivered} sent]"
        except Exception:

            return ""

    async def send_with_fallback(
        self,
        receiver,
        ch,
        stickers: list[dict],
        mapping: dict,
        msg: dict,
        source_id: int,
    ) -> bool:
        """
        Try to send stickers via cloned mapping -> standard/global -> webhook image-embed fallback.
        Returns True if the message was sent or queued (no further action needed by caller).
        Returns False if we prepared embeds in `msg` and the caller should continue with webhook send.
        """

        suppress_text = bool(msg.get("__stickers_no_text__"))
        prefer_embeds = bool(msg.get("__stickers_prefer_embeds__"))
        clone_gid = getattr(getattr(ch, "guild", None), "id", None)

        if msg.get("__stickers_embeds_added__"):
            return False

        def _is_custom_sticker_dict(s: dict) -> bool:
            try:
                if int(s.get("type", 0)) == 2:
                    return True
            except Exception:
                pass
            return (
                bool(s.get("guild_id"))
                or bool(s.get("custom"))
                or bool(s.get("is_custom"))
            )

        def _collect_pairs(sts: list[dict]) -> tuple[list[tuple[str, str]], bool]:
            """
            Returns (pairs, all_custom)
            pairs: [(name, url)]
            all_custom: True if every pair came from a custom/guild sticker
            """
            pairs: list[tuple[str, str]] = []
            all_custom = True
            for s in (sts or [])[:3]:
                url = s.get("url")
                if not url:
                    continue
                name = s.get("name") or "sticker"
                pairs.append((name, url))
                all_custom = all_custom and _is_custom_sticker_dict(s)

            if not pairs:

                extra = self.lookup_original_urls(clone_gid, sts)
                if extra:
                    pairs.extend(extra[: max(0, 3 - len(pairs))])

                    if extra:
                        all_custom = False

            return pairs, all_custom

        if prefer_embeds:
            pairs, all_custom = _collect_pairs(stickers)
            if pairs:
                msg["embeds"] = (msg.get("embeds") or []) + [
                    {"type": "rich", "image": {"url": url}} for (_n, url) in pairs
                ]
                msg["__stickers_embeds_added__"] = True
                msg["__stickers_embeds_custom__"] = all_custom
                msg["__stickers_embeds_count__"] = len(pairs)
            return False

        if (not mapping) or (not ch):
            msg["__buffered__"] = True
            receiver._pending_msgs.setdefault(source_id, []).append(msg)
            logger.info(
                "[⏳] Queued sticker message for later: %s%s",
                "mapping missing" if not mapping else "",
                " and cloned channel not found" if mapping and not ch else "",
            )
            return True

        objs, _ = self.resolve_cloned(clone_gid, stickers)

        author = msg.get("author")
        base_content = (msg.get("content") or "").strip()
        all_custom = all(_is_custom_sticker_dict(s) for s in (stickers or []))
        auth_disp = author or "Unknown"

        if suppress_text and all_custom and not base_content:
            content = None
        elif base_content:
            content = f"From {auth_disp}: {base_content}"
        else:
            content = f"From {auth_disp}:"

        if objs:
            rl_key = f"channel:{mapping.get('cloned_channel_id') or source_id}"
            await receiver.ratelimit.acquire(ActionType.WEBHOOK_MESSAGE, key=rl_key)
            try:

                await ch.send(stickers=objs, content=content)

                suffix = ""
                if msg.get("__backfill__"):
                    suffix = self._backfill_suffix(receiver, source_id, msg)

                if msg.get("__backfill__"):
                    logger.info(
                        "[💬] [backfill] Forwarded cloned-sticker message to #%s from %s (%s)%s",
                        msg.get("channel_name"),
                        msg.get("author"),
                        msg.get("author_id"),
                        suffix,
                    )
                else:
                    logger.info(
                        "[💬] Forwarded cloned-sticker message to #%s from %s (%s)",
                        msg.get("channel_name"),
                        msg.get("author"),
                        msg.get("author_id"),
                    )
                return True

            except discord.HTTPException:
                logger.debug(
                    "[⚠️] Cloned sticker send failed; will try standard or embed fallback."
                )
            finally:
                receiver.ratelimit.relax(ActionType.WEBHOOK_MESSAGE, key=rl_key)

        base_content = (
            None if suppress_text else ((msg.get("content") or "").strip() or None)
        )
        rl_key_std = f"channel:{mapping.get('cloned_channel_id') or source_id}"
        await receiver.ratelimit.acquire(ActionType.WEBHOOK_MESSAGE, key=rl_key_std)
        try:
            sent_std = await self.try_send_standard(
                channel=ch,
                author=msg.get("author"),
                stickers=stickers,
                base_content=base_content,
            )
        finally:
            receiver.ratelimit.relax(ActionType.WEBHOOK_MESSAGE, key=rl_key_std)

        if sent_std:
            suffix = ""
            if msg.get("__backfill__"):
                suffix = self._backfill_suffix(receiver, source_id, msg)

            if msg.get("__backfill__"):
                logger.info(
                    "[💬] [backfill] Forwarded standard sticker message to #%s from %s (%s)%s",
                    msg.get("channel_name"),
                    msg.get("author"),
                    msg.get("author_id"),
                    suffix,
                )
            else:
                logger.info(
                    "[💬] Forwarded standard sticker message to #%s from %s (%s)",
                    msg.get("channel_name"),
                    msg.get("author"),
                    msg.get("author_id"),
                )
            return True

        pairs, all_custom = _collect_pairs(stickers)
        if pairs:
            msg["embeds"] = (msg.get("embeds") or []) + [
                {"type": "rich", "image": {"url": url}} for (_n, url) in pairs
            ]

            msg["__stickers_embeds_added__"] = True
            msg["__stickers_embeds_custom__"] = all_custom
            msg["__stickers_embeds_count__"] = len(pairs)
            logger.info(
                "[💬] Sticker-embed fallback message sent from %s in #%s",
                msg.get("author", "Unknown"),
                msg.get("channel_name", "Unknown"),
            )
            return False

        failed_info = (
            ", ".join(
                f"{s.get('name','unknown')} ({s.get('id','no-id')}) [{s.get('url','no-url')}]"
                for s in (stickers or [])
            )
            or "no stickers in payload"
        )
        logger.warning(
            "[⚠️] Sticker(s) not embeddable for #%s — %s",
            msg.get("channel_name"),
            failed_info,
        )
        await ch.send(content=f"From {msg.get('author')}: sticker unavailable in clone")
        return True
