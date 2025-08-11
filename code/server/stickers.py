from __future__ import annotations
from typing import Tuple, List, Optional
import io
import asyncio
import discord
import aiohttp
import logging
from common.rate_limiter import RateLimitManager, ActionType

logger = logging.getLogger("server.stickers")

class StickerManager:
    def __init__(
        self,
        bot: discord.Bot,
        db,
        ratelimit: RateLimitManager,
        clone_guild_id: int,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.bot = bot
        self.db = db
        self.ratelimit = ratelimit
        self.clone_guild_id = clone_guild_id
        self.session = session

        # caches + state
        self._cache: dict[int, discord.GuildSticker] = {}
        self._cache_ts: float | None = None
        self._last_sitemap: list[dict] = []
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

        # standard sticker heuristics
        self._std_ok: set[int] = set()
        self._std_bad: set[int] = set()

    def set_session(self, session: aiohttp.ClientSession | None):
        self.session = session

    def set_last_sitemap(self, stickers: list[dict] | None):
        self._last_sitemap = stickers or []

    async def refresh_cache(self) -> None:
        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            return
        try:
            stickers = await guild.fetch_stickers()
        except Exception:
            stickers = []
        self._cache = {int(s.id): s for s in stickers}

    def kickoff_sync(self) -> None:
        """Schedule a background sticker sync if not running."""
        if self._task and not self._task.done():
            logger.debug("Sticker sync already running; skip kickoff.")
            return
        guild = self.bot.get_guild(self.clone_guild_id)
        if not guild:
            logger.debug("kickoff_sync: clone guild not available yet; skip.")
            return
        logger.debug("[🎟️] Sticker sync task scheduled.")
        self._task = asyncio.create_task(self._run_sync(guild, self._last_sitemap))

    async def _run_sync(self, guild: discord.Guild, stickers: list[dict]) -> None:
        """
        Synchronizes stickers for a given Discord guild with the provided list of sticker data.
        This method ensures that the stickers in the guild are consistent with the provided
        list by performing necessary operations such as deletion, renaming, or creation of stickers.
        It also updates the cache if any changes are made.
        """
        async with self._lock:
            try:
                upstream = len(stickers or [])
                try:
                    clone_list = await guild.fetch_stickers()
                except Exception:
                    clone_list = []
                mappings = len(list(self.db.get_all_sticker_mappings()))
                logger.debug("[🎟️] Sticker sync start: upstream=%d, clone=%d, mappings=%d",
                            upstream, len(clone_list), mappings)

                d, r, c = await self._sync(guild, stickers or [])
                if any((d, r, c)):
                    await self.refresh_cache()
                    logger.debug("[🎟️] Sticker sync: %s", ", ".join(
                        f"{label} {n}" for label, n in (("Deleted", d), ("Renamed", r), ("Created", c)) if n
                    ))
                else:
                    logger.debug("[🎟️] Sticker sync: no changes needed")
            except Exception:
                logger.exception("[🎟️] Sticker sync failed")
            finally:
                self._task = None

    async def _sync(self, guild: discord.Guild, stickers: list[dict]) -> Tuple[int,int,int]:
        """
        Synchronizes stickers between the provided guild and the given list of stickers.
        This method performs the following operations:
        - Deletes stickers in the guild that are no longer present in the incoming list.
        - Renames stickers in the guild if their names differ from the incoming list.
        - Creates new stickers in the guild based on the incoming list, respecting guild limits.
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

        current = {r["original_sticker_id"]: r for r in self.db.get_all_sticker_mappings()}
        incoming = {int(s["id"]): s for s in stickers if s.get("id")}

        # deletions
        for orig_id in set(current) - set(incoming):
            row = current[orig_id]
            cloned = clone_by_id.get(row["cloned_sticker_id"])
            if cloned:
                try:
                    await self.ratelimit.acquire(ActionType.STICKER_CREATE)
                    await cloned.delete()
                    deleted += 1
                    current_count = max(0, current_count - 1)
                    logger.info(f"[🎟️] Deleted sticker {row['cloned_sticker_name']}")
                except discord.Forbidden:
                    logger.warning(f"[⚠️] No permission to delete sticker {row['cloned_sticker_name']}")
                except discord.HTTPException as e:
                    logger.error(f"[⛔] Error deleting sticker {row['cloned_sticker_name']}: {e}")
            self.db.delete_sticker_mapping(orig_id)

        # upserts
        for orig_id, info in incoming.items():
            name = info.get("name") or f"sticker_{orig_id}"
            url  = info.get("url") or ""
            mapping = current.get(orig_id)
            cloned = None
            if mapping:
                cloned = clone_by_id.get(mapping["cloned_sticker_id"])
                if mapping and not cloned:
                    logger.warning(f"[⚠️] Sticker {mapping['original_sticker_name']} missing in clone; will recreate")
                    self.db.delete_sticker_mapping(orig_id)
                    mapping = None

            # rename
            if mapping and cloned and mapping["original_sticker_name"] != name:
                try:
                    await self.ratelimit.acquire(ActionType.STICKER_CREATE)
                    await cloned.edit(name=name)
                    renamed += 1
                    self.db.upsert_sticker_mapping(orig_id, name, cloned.id, cloned.name)
                    logger.info(f"[🎟️] Renamed sticker {mapping['original_sticker_name']} → {name}")
                except discord.HTTPException as e:
                    logger.error(f"[⛔] Failed renaming sticker {cloned.name}: {e}")
                continue

            if mapping:
                continue

            if not url:
                logger.warning(f"[⚠️] Sticker {name} has no URL; skipping")
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
                logger.error(f"[⛔] Failed fetching sticker {name} at {url}: {e}")
                continue

            if raw and len(raw) > 512 * 1024:
                logger.info(f"[🎟️] Skipping {name}: exceeds size limit")
                size_failed += 1
                continue

            fmt = int((info.get("format_type") or 0))
            fname = f"{name}.json" if fmt == 3 else f"{name}.png"
            file = discord.File(io.BytesIO(raw), filename=fname)
            tag = (info.get("tags") or "🙂")[:50]
            desc = (info.get("description") or "")[:100]

            try:
                await self.ratelimit.acquire(ActionType.STICKER_CREATE)
                created_stk = await guild.create_sticker(
                    name=name, description=desc, emoji=tag, file=file, reason="Clonecord sync",
                )
                created += 1
                current_count += 1
                self.db.upsert_sticker_mapping(orig_id, name, created_stk.id, created_stk.name)
                logger.info(f"[🎟️] Created sticker {name}")
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 30039 or "30039" in str(e):
                    skipped_limit += 1
                    logger.info("[🎟️] Skipped creating sticker due to clone guild sticker limit.")
                else:
                    logger.error(f"[⛔] Failed creating sticker {name}: {e}")

        if skipped_limit:
            logger.info(f"[🎟️] Skipped {skipped_limit} stickers due to clone guild limit ({limit}).")
        if size_failed:
            logger.info(f"[🎟️] Skipped {size_failed} stickers because they exceed 512 KiB.")

        return deleted, renamed, created

    def resolve_cloned(self, stickers: list[dict]) -> Tuple[List[discord.StickerItem], List[str]]:
        """
        Resolves cloned stickers based on a provided list of sticker dictionaries.
        This method matches stickers from the input list with their corresponding
        cloned stickers using a preloaded database mapping. It returns a tuple
        containing a list of `discord.StickerItem` objects and a list of sticker names.
        """
        # Use preloaded DB mapping
        rows = {r["original_sticker_id"]: r for r in self.db.get_all_sticker_mappings()}
        guild = self.bot.get_guild(self.clone_guild_id)

        items: List[discord.StickerItem] = []
        names: List[str] = []
        for s in stickers:
            try:
                orig_id = int(s.get("id"))
            except Exception:
                continue
            row = rows.get(orig_id)
            if not row:
                continue
            clone_id = int(row["cloned_sticker_id"])
            stk = self._cache.get(clone_id)
            if not stk and guild:
                stk = next((cs for cs in getattr(guild, "stickers", []) if int(cs.id) == clone_id), None)
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
            await channel.send(content=content, stickers=[discord.Object(id=i) for i in cand_ids])
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
    
    def lookup_original_urls(self, stickers: list[dict]) -> list[tuple[str, str]]:
        """
        For each incoming sticker (up to 3), try to find an original CDN URL
        from the most recent sitemap the server received from the client.
        Returns a list of (name, url) pairs, filtered to image URLs only.
        """
        if not self._last_sitemap:
            return []

        by_id: dict[int, dict] = {}
        for row in self._last_sitemap:
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
                # Skip non-image formats (e.g., lottie .json) for pure image-embed fallback
                continue

            name = row.get("name") or s.get("name") or "sticker"
            out.append((name, url))

        return out

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
        if (not mapping) or (not ch):
            msg["__buffered__"] = True
            receiver._pending_msgs.setdefault(source_id, []).append(msg)
            logger.info(
                "[⏳] Queued sticker message for later: %s%s",
                "mapping missing" if not mapping else "",
                " and cloned channel not found" if mapping and not ch else "",
            )
            return True

        # Try mapped/cloned stickers
        objs, _ = self.resolve_cloned(stickers)
        if objs:
            author = msg.get("author")
            base_content = (msg.get("content") or "").strip()
            content = f"From {author}: {base_content}" if base_content else f"From {author}:"
            try:
                await receiver.ratelimit.acquire(ActionType.WEBHOOK_MESSAGE, key=str(mapping["cloned_channel_id"]))
                await ch.send(content=content, stickers=objs[:3])
                logger.info("[💬] Forwarded cloned-sticker message to #%s from %s (%s)",
                        msg["channel_name"], msg["author"], msg["author_id"])
                return True
            except discord.HTTPException:
                logger.debug("[⚠️] Cloned sticker send failed; will try standard or embed fallback.")

        # Helper: collect image URLs for embed fallback
        def _collect_pairs(sts: list[dict]) -> list[tuple[str, str]]:
            pairs: list[tuple[str, str]] = []
            for s in (sts or [])[:3]:
                url = s.get("url")
                if url and self._is_image_url(url):
                    pairs.append((s.get("name") or "sticker", url))
            if not pairs:
                pairs.extend(self.lookup_original_urls(sts))
            return pairs

        # Try standard/global stickers
        sent_std = await self.try_send_standard(
            channel=ch,
            author=msg.get("author"),
            stickers=stickers,
            base_content=(msg.get("content") or "").strip() or None,
        )
        if sent_std:
            logger.info("[💬] Forwarded standard-sticker message to #%s from %s (%s)",
                    msg["channel_name"], msg["author"], msg["author_id"])
            return True

        # Webhook image-embed fallback (pure image: no content, no titles)
        pairs = _collect_pairs(stickers)
        if pairs:
            msg["content"] = None
            msg["embeds"] = (msg.get("embeds") or []) + [
                {"type": "rich", "image": {"url": url}} for (_, url) in pairs
            ]
            logger.info(
                "[💬] Image-embed fallback — %d sticker(s) from %s in #%s",
                len(pairs),
                msg.get("author", "Unknown"),
                msg.get("channel_name", "Unknown")
            )
            return False

        # Final degrade
        failed_info = ", ".join(
            f"{s.get('name','unknown')} ({s.get('id','no-id')}) [{s.get('url','no-url')}]"
            for s in stickers
        ) or "no stickers in payload"
        logger.warning("[⚠️] Sticker(s) not embeddable for #%s — %s", msg["channel_name"], failed_info)
        await ch.send(content=f"From {msg.get('author')}: sticker unavailable in clone")
        return True