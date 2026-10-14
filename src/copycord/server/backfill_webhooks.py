# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Temp webhook pool for backfills: create, adopt, rotate and delete the temporary webhooks a clone channel sends through.

Moved unchanged from server/backfill.py; BackfillManager inherits it and owns the state it reads.
"""

import asyncio
import contextlib
import json
import logging
import random
import aiohttp
from copycord.server.rate_limiter import ActionType

logger = logging.getLogger("server")


class BackfillWebhookPool:
    def _is_retryable_http(self, e: Exception) -> bool:
        status = getattr(e, "status", None)
        if status is None and isinstance(e, aiohttp.ClientResponseError):
            status = e.status

        if isinstance(e, (aiohttp.ClientError, asyncio.TimeoutError)):
            return True

        return status in (429, 500, 502, 503, 504)

    def _retry_after_seconds_from_exc(self, e: Exception) -> float | None:
        """Extract a server-provided delay from the exception: Retry-After header first, then body retry_after."""
        resp = getattr(e, "response", None)

        if resp is not None:
            try:

                ra = resp.headers.get("Retry-After") or resp.headers.get(
                    "X-RateLimit-Reset-After"
                )
                if ra is not None:
                    return float(ra)
            except Exception:
                pass

            try:

                raw = getattr(e, "text", None)
                if raw:
                    data = json.loads(raw)
                    if isinstance(data, dict) and "retry_after" in data:
                        return float(data["retry_after"])
            except Exception:
                pass
        return None

    async def _api_with_retry(
        self, *, what: str, call, action=None, base: float = 0.75, cap: float = 60.0
    ) -> any:
        """Retry call until it succeeds or we shut down, honoring Discord's hints, else exponential backoff with jitter."""
        attempt = 0
        while True:
            if getattr(self, "_shutting_down", False):
                raise asyncio.CancelledError(f"Aborted {what}: shutting down")

            try:
                if action is not None:

                    await self.ratelimit.acquire(action)

                return await call()
            except Exception as e:
                if not self._is_retryable_http(e):

                    raise

                hint = self._retry_after_seconds_from_exc(e)
                if hint is not None:
                    delay = max(0.0, float(hint))
                else:
                    delay = min(cap, (base * (2**attempt))) + random.random() * 0.333

                logger.warning(
                    "[retry] %s failed (%s). sleeping %.2fs before retry #%d",
                    what,
                    getattr(e, "status", type(e).__name__),
                    delay,
                    attempt + 1,
                )
                await asyncio.sleep(delay)
                attempt += 1

    async def _ensure_temp_webhooks(
        self, clone_channel_id: int
    ) -> tuple[list[int], list[str]]:
        try:
            ch = self.bot.get_channel(clone_channel_id) or await self.bot.fetch_channel(
                clone_channel_id
            )
            if not ch:
                logger.debug("[temps] Channel not found for #%s", clone_channel_id)
                return [], []

            orig = self._by_clone.get(clone_channel_id)
            row = self.r.chan_map.get(orig) or {}
            primary_url = row.get("channel_webhook_url") or row.get("webhook_url")

            st = self._progress.get(orig) if orig is not None else None
            N = max(0, int(getattr(self, "temp_webhook_max", 1)))

            primary_name = None
            primary_avatar_url = None
            customized = False

            if primary_url and orig is not None:
                try:
                    wid = int(primary_url.rstrip("/").split("/")[-2])

                    async def _do_fetch_primary():
                        return await self.bot.fetch_webhook(wid)

                    whp = await self._api_with_retry(
                        what=f"fetch primary webhook {wid}",
                        call=_do_fetch_primary,
                        action=None,
                    )
                    canonical = self._canonical_temp_name()
                    primary_name = (whp.name or "").strip() or None
                    customized = bool(primary_name and primary_name != canonical)

                    av_asset = getattr(whp, "avatar", None)
                    if av_asset:
                        try:
                            primary_avatar_url = (
                                str(getattr(av_asset, "url", None)) or None
                            )
                        except Exception:
                            primary_avatar_url = None
                except Exception as e:
                    logger.debug(
                        "[temps] fetch primary webhook failed for %s: %s",
                        primary_url,
                        e,
                    )

            if st is not None:
                st.setdefault("primary_identity", {})
                st["primary_identity"] = {
                    "name": primary_name,
                    "avatar_url": primary_avatar_url,
                }

            async def _fetch_wh(url: str):
                if not url:
                    return None
                try:
                    wid = int(url.rstrip("/").split("/")[-2])

                    async def _do_fetch():
                        return await self.bot.fetch_webhook(wid)

                    return await self._api_with_retry(
                        what=f"fetch webhook {wid}", call=_do_fetch, action=None
                    )
                except Exception:
                    return None

            seen_urls: set[str] = set()
            pairs: list[tuple[int, str]] = []

            created_ids: list[int] = []
            if st is not None:
                created_ids = list(st.get("temp_created_ids") or [])

            async def _adopt(url: str):
                if not url:
                    return
                if primary_url and url == primary_url:
                    return
                if url in seen_urls:
                    return
                wh = await _fetch_wh(url)
                if wh is not None:
                    seen_urls.add(url)
                    pairs.append((int(wh.id), url))

            if st:
                for u in list(st.get("temp_webhook_urls") or []):
                    await _adopt(u)

            try:

                async def _do_list():
                    return await ch.webhooks()

                hooks = await self._api_with_retry(
                    what=f"list webhooks in #{clone_channel_id}",
                    call=_do_list,
                    action=None,
                )
                for wh in hooks:
                    try:
                        made_by_us = (
                            getattr(wh, "user", None)
                            and getattr(wh.user, "id", None) == self.bot.user.id
                        )
                    except Exception:
                        made_by_us = False
                    if made_by_us and (not primary_url or wh.url != primary_url):
                        await _adopt(wh.url)
            except Exception as e:
                logger.debug(
                    "[temps] list hooks failed for #%s: %s", clone_channel_id, e
                )

            if N <= 0:
                ids, urls = [], []
                if st is not None:
                    st["temp_webhook_ids"] = ids
                    st["temp_webhook_urls"] = urls
                    st["temp_created_ids"] = []
                self._rotate_pool.pop(clone_channel_id, None)
                self._rotate_idx.pop(clone_channel_id, None)
                return ids, urls

            while len(pairs) < N:
                logger.debug(
                    "[🧹] Creating temp webhook for rotation in #%s...",
                    clone_channel_id,
                )

                async def _do_create():
                    return await ch.create_webhook(
                        name=self._canonical_temp_name(),
                        reason="Backfill rotation",
                    )

                wh_new = await self._api_with_retry(
                    what=f"create webhook in #{clone_channel_id}",
                    call=_do_create,
                    action=ActionType.WEBHOOK_CREATE,
                )
                pairs.append((int(wh_new.id), wh_new.url))
                seen_urls.add(wh_new.url)
                created_ids.append(int(wh_new.id))

            pairs.sort(key=lambda t: t[0])
            pairs = pairs[:N]

            ids = [p[0] for p in pairs]
            urls = [p[1] for p in pairs]

            if st is not None:
                st["temp_webhook_ids"] = list(ids)
                st["temp_webhook_urls"] = list(urls)
                st["temp_created_ids"] = [i for i in created_ids if i in ids]
            else:
                kept = [i for i in created_ids if i in ids]
                if kept:
                    self._created_cache[int(clone_channel_id)].extend(kept)

            self._rotate_pool.pop(clone_channel_id, None)
            self._rotate_idx.pop(clone_channel_id, None)
            return ids, urls

        except Exception as e:
            logger.warning(
                "[⚠️] Could not ensure temp webhooks in #%s: %s", clone_channel_id, e
            )
            return [], []

    async def _list_temp_webhook_urls(self, clone_channel_id: int) -> list[str]:
        """Return temp webhook URLs for rotation, from the backfill sink cache or else this bot's non-primary webhooks in the channel."""
        try:
            sink_key = self._by_clone.get(clone_channel_id)
            if sink_key is not None:
                st = self._progress.get(sink_key) or {}
                urls = list(st.get("temp_webhook_urls") or [])
                if urls:
                    pairs = [(int(u.rstrip("/").split("/")[-2]), u) for u in urls]
                    pairs.sort(key=lambda t: t[0])
                    return [u for (_id, u) in pairs]

            ch = self.bot.get_channel(clone_channel_id) or await self.bot.fetch_channel(
                clone_channel_id
            )
            hooks = await ch.webhooks()

            primary_url = None
            orig = self._by_clone.get(clone_channel_id)
            if orig is not None:
                row = self.r.chan_map.get(orig) or {}
                primary_url = row.get("channel_webhook_url")

            pairs = []
            for wh in hooks:
                try:

                    made_by_us = (
                        getattr(wh, "user", None)
                        and getattr(wh.user, "id", None) == self.bot.user.id
                    )
                except Exception:
                    made_by_us = False
                if made_by_us and (not primary_url or wh.url != primary_url):
                    pairs.append((int(wh.id), wh.url))

            pairs.sort(key=lambda t: t[0])
            return [u for (_id, u) in pairs]
        except Exception as e:
            logger.debug("[temps] list failed for #%s: %s", clone_channel_id, e)
            return []

    async def pick_url_for_send(
        self, clone_channel_id: int, primary_url: str, create_missing: bool
    ):
        lock = self._rot_locks.get(clone_channel_id)
        if lock is None:
            lock = self._rot_locks[clone_channel_id] = asyncio.Lock()
        async with lock:
            pool = self._rotate_pool.get(clone_channel_id)
            if pool is None:
                temps = (
                    (await self._ensure_temp_webhooks(clone_channel_id))[1]
                    if create_missing
                    else await self._list_temp_webhook_urls(clone_channel_id)
                )

                temps = [u for u in temps if u != primary_url]

                if not temps:
                    return primary_url, False

                N = max(0, int(self.temp_webhook_max))
                pool = [primary_url] + temps[:N]
                self._rotate_pool[clone_channel_id] = pool
                self._rotate_idx.setdefault(clone_channel_id, -1)

            idx = (self._rotate_idx.get(clone_channel_id, -1) + 1) % len(pool)
            self._rotate_idx[clone_channel_id] = idx
            return pool[idx], True

    async def ensure_temps_ready(self, clone_id: int):
        ev = self._temp_ready.setdefault(clone_id, asyncio.Event())
        if ev.is_set():
            return
        lock = self._temp_locks.setdefault(clone_id, asyncio.Lock())
        async with lock:
            if ev.is_set():
                return
            ids, urls = await self._ensure_temp_webhooks(clone_id)
            sink_key = self._by_clone.get(clone_id)
            if sink_key is not None:
                st = self._progress.get(sink_key) or {}
                st["temp_webhook_ids"] = ids
                st["temp_webhook_urls"] = urls
            ev.set()

    def _canonical_temp_name(self) -> str:
        return self._temp_prefix_canon

    def invalidate_rotation(self, clone_channel_id: int) -> None:
        cid = int(clone_channel_id)
        self._rotate_pool.pop(cid, None)
        self._rotate_idx.pop(cid, None)
        self._temps_cache.pop(cid, None)

        if hasattr(self, "_temp_ready"):
            self._temp_ready.pop(cid, None)
        self._temp_locks.pop(cid, None)
        self._rot_locks.pop(cid, None)

        logger.debug("[rotate] invalidated pool for #%s", clone_channel_id)

    async def delete_created_temps_for(
        self,
        clone_channel_id: int,
        *,
        created_ids: list[int] | None = None,
        dry_run: bool = False,
    ) -> dict:
        """Delete only temp webhooks created during this backfill run (or the given IDs). Returns {'deleted': int}."""
        stats = {"deleted": 0}
        try:

            if created_ids is None:

                orig = self._by_clone.get(int(clone_channel_id))
                st = self._progress.get(int(orig)) if orig is not None else None
                if not st:
                    return stats

                row = self.r.chan_map.get(int(orig)) or {}
                primary_url = row.get("channel_webhook_url") or row.get("webhook_url")
                primary_id = None
                if primary_url:
                    with contextlib.suppress(Exception):
                        primary_id = int(primary_url.rstrip("/").split("/")[-2])

                created_ids = []
                created_ids.extend(list(st.get("temp_created_ids") or []))
                created_ids.extend(self._created_cache.get(int(clone_channel_id), []))
                created_ids = list({int(x) for x in created_ids})
            else:

                row = None
                orig = self._by_clone.get(int(clone_channel_id))
                if orig is not None:
                    row = self.r.chan_map.get(int(orig)) or {}
                primary_url = (row or {}).get("channel_webhook_url") or (row or {}).get(
                    "webhook_url"
                )
                primary_id = None
                if primary_url:
                    with contextlib.suppress(Exception):
                        primary_id = int(primary_url.rstrip("/").split("/")[-2])

                created_ids = [int(x) for x in set(created_ids or [])]

            if not created_ids:
                return stats

            async def _delete_one(wid: int) -> bool:
                if primary_id and int(wid) == int(primary_id):
                    return False
                try:
                    wh = await self.bot.fetch_webhook(int(wid))

                    acq = getattr(self.ratelimit, "acquire", None)
                    if acq is not None:
                        try:
                            await acq(
                                getattr(
                                    ActionType,
                                    "WEBHOOK_DELETE",
                                    ActionType.WEBHOOK_CREATE,
                                )
                            )
                        except Exception:
                            pass
                    await wh.delete(reason="Backfill complete: remove temp webhook")
                    return True
                except Exception as e:
                    logger.debug(
                        "[cleanup] Could not delete webhook %s in #%s: %s",
                        wid,
                        clone_channel_id,
                        e,
                    )
                    return False

            for wid in created_ids:
                if dry_run:
                    logger.info(
                        "[🧹 DRY RUN] Would delete temp webhook %s in #%s",
                        wid,
                        clone_channel_id,
                    )
                else:
                    if await _delete_one(int(wid)):
                        stats["deleted"] += 1

            sink_key = self._by_clone.get(int(clone_channel_id))
            if sink_key is not None:
                st2 = self._progress.get(int(sink_key)) or {}
                st2["temp_created_ids"] = []
                st2["temp_webhook_ids"] = []
                st2["temp_webhook_urls"] = []
            self._created_cache.pop(int(clone_channel_id), None)
            self.invalidate_rotation(int(clone_channel_id))
        except Exception:
            logger.debug(
                "[cleanup] delete_created_temps_for failed for #%s",
                clone_channel_id,
                exc_info=True,
            )
        return stats

    async def cleanup_non_primary_webhooks(
        self,
        *,
        channel_ids: list[int] | None = None,
        only_ours: bool = True,
        dry_run: bool = False,
    ) -> dict:
        """Delete webhooks in clone channels that are not the mapped primary.

        only_ours limits it to webhooks this bot created; channel_ids limits the channels;
        dry_run only logs. Returns counts of channels checked, deleted and skipped.
        """
        stats = {
            "channels_checked": 0,
            "deleted": 0,
            "skipped_no_primary": 0,
            "skipped_missing_channel": 0,
        }
        try:
            if getattr(self.r, "_shutting_down", False):
                logger.debug("[cleanup] Skipping cleanup: shutting down")
                return stats

            with contextlib.suppress(Exception):
                self.r._load_mappings()

            def _wid_from_url(u: str | None) -> int | None:
                if not u:
                    return None
                try:
                    return int(u.rstrip("/").split("/")[-2])
                except Exception:
                    return None

            clone_to_primary: dict[int, int] = {}
            for row in (self.r.chan_map or {}).values():
                try:
                    clone_id = int(row.get("cloned_channel_id") or 0)
                except Exception:
                    clone_id = 0
                if not clone_id:
                    continue
                wid = _wid_from_url(
                    row.get("channel_webhook_url") or row.get("webhook_url")
                )
                if wid:
                    clone_to_primary[clone_id] = wid

            targets = list(clone_to_primary.keys())
            if channel_ids:
                targets = [int(x) for x in channel_ids if int(x) in clone_to_primary]

            if not targets:
                logger.debug("[cleanup] No clone channels to check; nothing to do")
                return stats

            for clone_id in targets:
                if getattr(self.r, "_shutting_down", False):
                    break

                ch = self.bot.get_channel(int(clone_id))
                if not ch:
                    try:
                        ch = await self.bot.fetch_channel(int(clone_id))
                    except Exception as e:
                        stats["skipped_missing_channel"] += 1
                        logger.debug(
                            "[cleanup] Clone channel #%s not found/inaccessible (%s); skipping",
                            clone_id,
                            e,
                        )
                        continue

                if not ch:
                    stats["skipped_missing_channel"] += 1
                    logger.debug(
                        "[cleanup] Clone channel #%s not found; skipping", clone_id
                    )
                    continue

                stats["channels_checked"] += 1
                primary_id = int(clone_to_primary.get(clone_id) or 0)
                if not primary_id:
                    stats["skipped_no_primary"] += 1
                    logger.debug(
                        "[cleanup] #%s has no primary webhook id; skipping", clone_id
                    )
                    continue

                try:
                    hooks = await ch.webhooks()
                except Exception as e:

                    stats["skipped_missing_channel"] += 1
                    logger.debug(
                        "[cleanup] Could not list webhooks for #%s: %s", clone_id, e
                    )
                    continue

                for wh in hooks:
                    try:
                        if int(wh.id) == primary_id:
                            continue
                    except Exception:
                        continue

                    if only_ours:
                        try:
                            made_by_us = (
                                getattr(wh, "user", None)
                                and getattr(wh.user, "id", None) == self.bot.user.id
                            )
                        except Exception:
                            made_by_us = False
                        if not made_by_us:
                            continue

                    if dry_run:
                        logger.info(
                            "[🧹 DRY RUN] Would delete non-primary webhook %s in #%s (name=%r)",
                            wh.id,
                            clone_id,
                            wh.name,
                        )
                        continue

                    try:

                        await wh.delete(
                            reason="Cleanup: remove non-primary webhook in clone channel"
                        )
                        stats["deleted"] += 1
                        logger.debug(
                            "[🧹] Deleted extra webhook %s in #%s (name=%r)",
                            wh.id,
                            clone_id,
                            wh.name,
                        )
                    except Exception as e:
                        logger.warning(
                            "[⚠️] Failed to delete webhook %s in #%s: %s",
                            wh.id,
                            clone_id,
                            e,
                        )

                await asyncio.sleep(0)

            logger.debug(
                "[🧹] Cleanup complete: checked=%d deleted=%d skipped_no_primary=%d skipped_missing_channel=%d",
                stats["channels_checked"],
                stats["deleted"],
                stats["skipped_no_primary"],
                stats["skipped_missing_channel"],
            )
        except Exception:
            logger.exception(
                "[cleanup] Unexpected error while deleting non-primary webhooks"
            )
        return stats
