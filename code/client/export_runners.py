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
import json
import os
import aiohttp
import re
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, TypeAlias


DictLike: TypeAlias = Dict[str, Any]


class ExportMessagesRunner:
    def __init__(
        self,
        bot: Any,
        ws: Any,
        msg_serializer,
        logger: Optional[logging.Logger] = None,
        scan_sleep: float = 0.0,
        send_sleep: float = 2.0,
        out_root: str = "/data/exports",
        download_concurrency: int = 4,
    ) -> None:
        self.bot = bot
        self.ws = ws
        self.serialize = msg_serializer
        self.log = logger or logging.getLogger("export")
        self.scan_sleep = float(scan_sleep)
        self.send_sleep = float(send_sleep)
        self.out_root = out_root
        self.download_concurrency = max(1, int(download_concurrency))

        self._link_re = re.compile(r"https?://\S+", re.I)
        self._emoji_re = re.compile(r"(<a?:\w+:\d+>)|([\U0001F300-\U0001FAFF])")
        self._active_guilds: set[int] = set()
        self._active_lock = asyncio.Lock()
        
    async def try_begin(self, guild_id: int) -> bool:
        """
        Attempt to mark a guild as 'running'. Returns False if already running.
        """
        async with self._active_lock:
            if guild_id in self._active_guilds:
                return False
            self._active_guilds.add(guild_id)
            return True

    async def _end(self, guild_id: int) -> None:
        async with self._active_lock:
            self._active_guilds.discard(guild_id)

    async def run(self, d: DictLike, guild: Any, acquired: bool = False) -> None:
        """
        Execute the export with the provided `data` and resolved `guild`.
        Emits progress logs and a final 'export_messages_done' ws event.
        """
        gid_log = getattr(guild, "id", None)
        gname = getattr(guild, "name", "") or "Unknown"

        # Concurrency guard
        if not acquired:
            ok = await self.try_begin(gid_log)
            if not ok:
                self.log.warning(f"[export] Another export is already running for guild={gid_log}. Aborting run().")
                try:
                    await self._ws_send({
                        "type": "export_messages_done",
                        "data": {"guild_id": gid_log, "error": "export-already-running"},
                    })
                except Exception:
                    pass
                return

        try:
            chan_id_raw = (d.get("channel_id") or "").strip() or None
            user_id_raw = (d.get("user_id") or "").strip() or None
            webhook_url = (d.get("webhook_url") or "").strip() or None
            only_with_attachments = bool(d.get("has_attachments", False))
            after_dt = self._parse_iso(d.get("after_iso") or None)
            before_dt = self._parse_iso(d.get("before_iso") or None)
            user_id = int(user_id_raw) if (user_id_raw and user_id_raw.isdigit()) else None

            wh_tail = webhook_url[-6:] if webhook_url else "None"
            do_forward = bool(webhook_url)

            filters = d.get("filters") or {}
            F: DictLike = {
                "embeds": filters.get("embeds", True),
                "attachments": filters.get("attachments", True),
                "att_types": (
                    filters.get("att_types")
                    or {"images": True, "videos": True, "audio": True, "other": True}
                ),
                "links": filters.get("links", True),
                "emojis": filters.get("emojis", True),
                "has_content": bool(filters.get("has_content", True)),
                "word_on": filters.get("word_on", False),
                "word": (filters.get("word") or "").strip(),
                "replies": filters.get("replies", True),
                "bots": filters.get("bots", True),
                "system": filters.get("system", True),
                "min_length": int(filters.get("min_length", 0) or 0),
                "min_reactions": int(filters.get("min_reactions", 0) or 0),
                "pinned": filters.get("pinned", True),
                "stickers": filters.get("stickers", True),
                "mentions": filters.get("mentions", True),
                "download_media": (
                    filters.get("download_media")
                    or {"images": False, "videos": False, "audio": False, "other": False}
                ),
            }

            if not F["attachments"]:
                F["att_types"] = {
                    "images": False,
                    "videos": False,
                    "audio": False,
                    "other": False,
                }
            else:
                kinds = F["att_types"]
                if not any(bool(kinds.get(k)) for k in ("images", "videos", "audio", "other")):
                    F["attachments"] = False

            if after_dt and before_dt and after_dt > before_dt:
                after_dt, before_dt = before_dt, after_dt

            me = None
            try:
                me = guild.get_member(getattr(self.bot.user, "id", None))
            except Exception:
                me = getattr(guild, "me", None)

            channels: List[Any] = []
            if chan_id_raw:
                ch = None
                try:
                    ch = await self.bot.fetch_channel(int(chan_id_raw))
                except Exception as e:
                    self.log.debug(
                        f"[export] fetch_channel({chan_id_raw}) failed: {e}; falling back to cache"
                    )
                    if chan_id_raw.isdigit():
                        ch = self.bot.get_channel(int(chan_id_raw))
                if ch:
                    channels = [ch]

            if not channels:
                if me:
                    channels = [
                        c
                        for c in getattr(guild, "text_channels", [])
                        if c.permissions_for(me).read_message_history
                    ]
                else:
                    channels = list(getattr(guild, "text_channels", []))

            t0 = time.perf_counter()
            self.log.info(
                f"[export] Start task guild={gid_log} ({gname}) "
                f"filters: chan={chan_id_raw or 'ALL'}, user={user_id or 'ANY'}, "
                f"attachments_only={only_with_attachments}, after={after_dt}, before={before_dt}, "
                f"forward_webhook={do_forward}({('…'+wh_tail) if do_forward else '—'}), "
                f"scan_sleep={self.scan_sleep}, send_sleep={self.send_sleep}, ui_filters={filters}"
            )

            if not channels:
                self.log.warning(f"[export] No readable text channels in guild {gid_log}. Aborting.")
                await self._ws_send({
                    "type": "export_messages_done",
                    "data": {"guild_id": gid_log, "forwarded": 0, "scanned": 0},
                })
                return

            ch_ids_preview = [getattr(c, "id", None) for c in channels[:8]]
            more_note = "" if len(channels) <= 8 else f" (+{len(channels)-8} more)"
            self.log.info(f"[export] Channels to scan: {len(channels)} -> {ch_ids_preview}{more_note}")

            total_scanned = 0
            total_matched = 0
            forwarded = 0
            buffer_rows: List[DictLike] = []

            for ch in channels:
                cid = getattr(ch, "id", None)
                cname = getattr(ch, "name", "") or "unknown"
                self.log.info(f"[export] Scanning channel #{cname} ({cid}) …")
                ch_scanned = 0
                ch_matched = 0

                try:
                    kwargs: DictLike = {"limit": None, "oldest_first": True}
                    if after_dt:
                        kwargs["after"] = after_dt
                    if before_dt:
                        kwargs["before"] = before_dt

                    async for msg in ch.history(**kwargs):
                        ch_scanned += 1
                        total_scanned += 1

                        if user_id and getattr(msg.author, "id", None) != user_id:
                            if self.scan_sleep:
                                await asyncio.sleep(self.scan_sleep)
                            continue

                        if only_with_attachments and not getattr(msg, "attachments", []):
                            if self.scan_sleep:
                                await asyncio.sleep(self.scan_sleep)
                            continue

                        if not self._passes_filters(msg, F):
                            if self.scan_sleep:
                                await asyncio.sleep(self.scan_sleep)
                            continue

                        ch_matched += 1
                        total_matched += 1

                        serialized = self.serialize(msg)

                        buffer_rows.append(
                            {
                                "guild_id": getattr(guild, "id", None),
                                "channel_id": getattr(ch, "id", None),
                                "message": serialized,
                            }
                        )

                        if ch_scanned % 200 == 0:
                            self.log.info(
                                f"[export] Progress ch={cid}: scanned={ch_scanned}, matched={ch_matched}, "
                                f"total_scanned={total_scanned}, total_matched={total_matched}, buffered={len(buffer_rows)}"
                            )

                        if self.scan_sleep:
                            await asyncio.sleep(self.scan_sleep)
                except Exception as e:
                    self.log.warning(f"[export] Channel {cid} failed: {e}")
                    continue

                self.log.info(
                    f"[export] Done channel #{cname} ({cid}): scanned={ch_scanned}, matched={ch_matched}"
                )

            json_file: Optional[str] = None
            try:
                ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                gid_str = str(getattr(guild, "id", "unknown"))
                subdir = os.path.join(self.out_root, gid_str, ts)
                os.makedirs(subdir, exist_ok=True)

                json_file = os.path.join(subdir, "messages.json")
                self.log.info(f"[export] Writing JSON snapshot ({len(buffer_rows)} messages) → {json_file}")
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "guild_id": gid_str,
                            "exported_at": ts + "Z",
                            "count": len(buffer_rows),
                            "messages": [row["message"] for row in buffer_rows],
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
                self.log.info(f"[export] JSON saved: {json_file}")

            except Exception as e:
                self.log.warning(f"[export] Failed to write JSON: {e}")
                json_file = None

            dl_cfg = F.get("download_media") or {}
            want_any_download = any(dl_cfg.get(k, False) for k in ("images", "videos", "audio", "other"))
            media_report = {"images": 0, "videos": 0, "audio": 0, "other": 0, "errors": 0}

            if want_any_download and aiohttp is None:
                self.log.warning("[export] Download requested but aiohttp not available. Skipping downloads.")
            elif want_any_download:
                from urllib.parse import urlparse

                def _ext_from(att: DictLike, url: str) -> str:
                    name = (att.get("filename") or "").strip()
                    ext = os.path.splitext(name)[1]
                    if not ext:
                        path = urlparse(url).path
                        ext = os.path.splitext(path)[1]
                    if not ext:
                        ct = (att.get("content_type") or "").lower()
                        if "png" in ct:
                            ext = ".png"
                        elif "jpeg" in ct or "jpg" in ct:
                            ext = ".jpg"
                        elif "gif" in ct:
                            ext = ".gif"
                        elif "webp" in ct:
                            ext = ".webp"
                        elif "mp4" in ct:
                            ext = ".mp4"
                        elif "webm" in ct:
                            ext = ".webm"
                        elif "mp3" in ct:
                            ext = ".mp3"
                        elif "wav" in ct:
                            ext = ".wav"
                    return ext or ".bin"

                candidates_by_kind: Dict[str, List[Tuple[str, str, str, str]]] = {
                    "images": [],
                    "videos": [],
                    "audio": [],
                    "other": [],
                }

                for row in buffer_rows:
                    msg_obj = row.get("message") or {}
                    msg_id = str(msg_obj.get("id") or "")
                    if not msg_id:
                        continue

                    per_msg_items: List[Tuple[str, DictLike]] = []
                    for att in msg_obj.get("attachments") or []:
                        url = att.get("url") or att.get("proxy_url")
                        if (
                            not url
                            or not isinstance(url, str)
                            or not url.lower().startswith(("http://", "https://"))
                        ):
                            continue
                        kind = self._att_kind(att)
                        if not dl_cfg.get(kind, False):
                            continue
                        per_msg_items.append((kind, att))

                    if not per_msg_items:
                        continue

                    total_count = 0
                    for kind, att in per_msg_items:
                        url = att.get("url") or att.get("proxy_url")
                        ext = _ext_from(att, url)

                        total_count += 1
                        if total_count == 1:
                            fname = f"{msg_id}{ext}"
                        else:
                            fname = f"{msg_id}-{total_count}{ext}"

                        candidates_by_kind[kind].append((url, fname, "", msg_id))

                used_kinds = [k for k, items in candidates_by_kind.items() if items]
                if not used_kinds:
                    self.log.info("[export] No media matches selection; skipping download step.")
                else:
                    media_root = os.path.join(os.path.dirname(json_file or ""), "media")
                    os.makedirs(media_root, exist_ok=True)
                    kind_dirs: Dict[str, str] = {k: os.path.join(media_root, k) for k in used_kinds}
                    for d in kind_dirs.values():
                        os.makedirs(d, exist_ok=True)

                    def _ensure_unique(dest_dir: str, filename: str) -> str:
                        base, ext = os.path.splitext(filename)
                        i = 0
                        candidate = filename
                        while os.path.exists(os.path.join(dest_dir, candidate)):
                            i += 1
                            candidate = f"{base}-dup{i}{ext}"
                        return candidate

                    sem = asyncio.Semaphore(self.download_concurrency)
                    tasks: List[Tuple[str, str, str, str]] = []
                    for kind in used_kinds:
                        dest_dir = kind_dirs[kind]
                        for url, fname, _unused, _msgid in candidates_by_kind[kind]:
                            final_name = _ensure_unique(dest_dir, fname)
                            tasks.append((kind, url, final_name, dest_dir))

                    self.log.info(
                        f"[export] Downloading media per selection: "
                        f"{ {k: dl_cfg.get(k, False) for k in ('images','videos','audio','other')} } "
                        f"→ kinds_used={used_kinds} count={len(tasks)}"
                    )

                    async with aiohttp.ClientSession() as session:
                        results = await asyncio.gather(
                            *[
                                self._download_one(session, sem, kind, url, fname, dest)
                                for (kind, url, fname, dest) in tasks
                            ],
                            return_exceptions=False,
                        )

                    for (kind, url, *_), res in zip(tasks, results):
                        ok, path, err = res
                        if ok:
                            media_report[kind] += 1
                        else:
                            media_report["errors"] += 1
                            self.log.warning(
                                f"[export] Download failed kind={kind} url={self._safe(url)} err={err}"
                            )

                    self.log.info(f"[export] Media download complete: {media_report}")

            if do_forward and buffer_rows:
                self.log.info(
                    f"[export] Forwarding buffered messages: {len(buffer_rows)} → webhook(…{wh_tail})"
                )
                for idx, row in enumerate(buffer_rows, 1):
                    try:
                        await self._ws_send(
                            {
                                "type": "export_message",
                                "data": {
                                    "guild_id": row.get("guild_id"),
                                    "channel_id": row.get("channel_id"),
                                    "webhook_url": webhook_url,
                                    "message": row.get("message"),
                                },
                            }
                        )
                        forwarded += 1
                        if self.send_sleep:
                            await asyncio.sleep(self.send_sleep)
                    except Exception as e:
                        self.log.warning(f"[📤] export_message send failed (post-JSON) idx={idx}: {e}")
                        break

                    if idx % 200 == 0:
                        self.log.info(
                            f"[export] Forwarded {idx}/{len(buffer_rows)} (total_forwarded={forwarded})"
                        )
            else:
                self.log.info("[export] No webhook URL provided — skipping forwarding step.")

            dur = time.perf_counter() - t0
            self.log.info(
                f"[export] Complete guild={gid_log} ({gname}) scanned={total_scanned}, matched={total_matched}, "
                f"forwarded={forwarded}, json={'saved '+json_file if json_file else 'none'}, "
                f"media_dl={media_report if want_any_download else 'skipped'}, elapsed={dur:.1f}s"
            )

            try:
                done_payload: DictLike = {
                    "guild_id": getattr(guild, "id", None),
                    "forwarded": forwarded,
                    "scanned": total_scanned,
                    "matched": total_matched,
                    **({"json_path": json_file} if json_file else {}),
                }
                if want_any_download:
                    done_payload["media_download"] = media_report
                await self._ws_send({"type": "export_messages_done", "data": done_payload})
            except Exception as e:
                self.log.debug(f"[export] emit export_messages_done failed: {e}")

        finally:
            await self._end(gid_log)


    async def _ws_send(self, payload: DictLike) -> None:
        await self.ws.send(payload)

    @staticmethod
    def _parse_iso(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(
                timezone.utc
            )
        except Exception:
            return None

    @staticmethod
    def _safe(s: str, n: int = 64) -> str:
        try:
            return s if len(s) <= n else (s[:n] + "…")
        except Exception:
            return "<str>"

    def _att_kind(self, att: Union[DictLike, Any]) -> str:
        if hasattr(att, "content_type"):
            ct = (getattr(att, "content_type", "") or "").lower()
            name = (getattr(att, "filename", "") or "").lower()
        else:
            ct = (att.get("content_type") or "").lower()
            name = (att.get("filename") or "").lower()

        def has_any(kw: Iterable[str]) -> bool:
            return any(k in ct or name.endswith(k) for k in kw)

        if has_any(
            ("image/", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff")
        ):
            return "images"
        if has_any(("video/", ".mp4", ".mov", ".webm", ".mkv", ".avi")):
            return "videos"
        if has_any(("audio/", ".mp3", ".wav", ".ogg", ".flac", ".m4a", ".aac")):
            return "audio"
        return "other"

    def _has_any_attachment_type(self, msg: Union[DictLike, Any], F: DictLike) -> bool:
        atts = getattr(msg, "attachments", None)
        if atts is None and isinstance(msg, dict):
            atts = msg.get("attachments") or []
        if not atts:
            return False
        for a in atts:
            k = self._att_kind(a)
            if F["att_types"].get(k, True):
                return True
        return False

    def _passes_filters(self, msg: Union[DictLike, Any], F: DictLike) -> bool:

        if isinstance(msg, dict):
            is_bot = bool(((msg.get("author") or {}).get("bot")) or False)
        else:
            is_bot = bool(getattr(getattr(msg, "author", None), "bot", False))
        if not F["bots"] and is_bot:
            return False

        if not F["system"]:
            if isinstance(msg, dict):
                t = str(msg.get("type") or "").lower()
                if t and "default" not in t:
                    return False
            else:
                mtype = getattr(msg, "type", None)
                if mtype and getattr(mtype, "name", "").lower() != "default":
                    return False

        content = (
            msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
        ) or ""
        if not F["has_content"] and content.strip():
            return False
        if F["min_length"] > 0 and len(content) < F["min_length"]:
            return False

        reacts = (
            msg.get("reactions")
            if isinstance(msg, dict)
            else getattr(msg, "reactions", [])
        ) or []
        if F["min_reactions"] > 0:

            def _rcount(r: Union[DictLike, Any]) -> int:
                return int(
                    (r.get("count") if isinstance(r, dict) else getattr(r, "count", 0))
                    or 0
                )

            total_reacts = sum(_rcount(r) for r in reacts)
            if total_reacts < F["min_reactions"]:
                return False

        pinned = bool(
            (
                msg.get("pinned")
                if isinstance(msg, dict)
                else getattr(msg, "pinned", False)
            )
            or False
        )
        if not F["pinned"] and pinned:
            return False

        stickers = (
            msg.get("stickers")
            if isinstance(msg, dict)
            else getattr(msg, "stickers", [])
        ) or []
        if not F["stickers"] and stickers:
            return False

        if not F["mentions"]:
            if isinstance(msg, dict):
                if (
                    (msg.get("mentions") or [])
                    or (msg.get("role_mentions") or [])
                    or (msg.get("channel_mentions") or [])
                ):
                    return False
            else:
                if (
                    (getattr(msg, "mentions", []) or [])
                    or (getattr(msg, "role_mentions", []) or [])
                    or (getattr(msg, "channel_mentions", []) or [])
                ):
                    return False

        if isinstance(msg, dict):
            ref = msg.get("reference")
            has_ref = bool(ref and ref.get("resolved"))
        else:
            ref = getattr(msg, "reference", None)
            has_ref = bool(ref and getattr(ref, "resolved", None))
        if not F["replies"] and has_ref:
            return False

        embeds = (
            msg.get("embeds") if isinstance(msg, dict) else getattr(msg, "embeds", [])
        ) or []
        if not F["embeds"] and embeds:
            return False

        atts = (
            msg.get("attachments")
            if isinstance(msg, dict)
            else getattr(msg, "attachments", [])
        ) or []
        if not F["attachments"]:
            if atts:
                return False
        else:
            if atts and not self._has_any_attachment_type(msg, F):
                return False

        if not F["links"]:
            if self._link_re.search(content):
                return False
            for e in embeds:
                if isinstance(e, dict):
                    vals = (e.get("url"), e.get("title"), e.get("description"))
                else:
                    vals = (
                        getattr(e, "url", None),
                        getattr(e, "title", None),
                        getattr(e, "description", None),
                    )
                if any(v and self._link_re.search(str(v)) for v in vals):
                    return False

        if not F["emojis"] and self._emoji_re.search(content):
            return False

        if F["word_on"] and F["word"]:
            word_re = re.compile(re.escape(F["word"]), re.I)
            if not word_re.search(content):
                return False

        return True

    def _iter_attachment_links(
        self, msg_obj: DictLike
    ) -> Iterable[Tuple[str, str, str]]:
        """
        Yield (kind, url, filename) from a serialized message object (dict).
        Expects message['attachments'] list with items having 'url' and 'filename' (and optionally 'content_type').
        """
        atts = msg_obj.get("attachments") or []
        for a in atts:
            url = a.get("url") or a.get("proxy_url")
            if (
                not url
                or not isinstance(url, str)
                or not url.lower().startswith(("http://", "https://"))
            ):
                continue
            kind = self._att_kind(a)
            filename = (
                a.get("filename")
                or os.path.basename(url.split("?", 1)[0])
                or f"{kind}-{uuid.uuid4().hex}"
            )
            yield (kind, url, filename)

    async def _download_one(
        self,
        session: Any,
        sem: asyncio.Semaphore,
        kind: str,
        url: str,
        filename: str,
        dest_dir: str,
    ) -> Tuple[bool, str, Optional[str]]:
        base, ext = os.path.splitext(filename)
        if not base:
            base = kind
        out_path = os.path.join(dest_dir, f"{base}{ext}")
        i = 0
        while os.path.exists(out_path):
            i += 1
            out_path = os.path.join(dest_dir, f"{base}-{i}{ext}")

        try:
            async with sem:
                timeout = aiohttp.ClientTimeout(total=180)
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    with open(out_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(1 << 14):
                            if chunk:
                                f.write(chunk)
            return True, out_path, None
        except Exception as e:
            return False, out_path, str(e)


class DmHistoryExporter:
    """
    Export a user's DM history.
    Enforces: only 1 export per user_id at a time (class-level registry).
    """

    # ---------- class-level registry ----------
    _lock: asyncio.Lock = asyncio.Lock()
    _active_users: set[int] = set()
    _tasks: dict[int, asyncio.Task] = {}
    # -----------------------------------------

    @classmethod
    async def try_begin(cls, user_id: int) -> bool:
        async with cls._lock:
            if user_id in cls._active_users:
                return False
            cls._active_users.add(user_id)
            return True

    @classmethod
    async def end(cls, user_id: int) -> None:
        async with cls._lock:
            cls._active_users.discard(user_id)
            cls._tasks.pop(user_id, None)

    @classmethod
    async def register_task(cls, user_id: int, task: asyncio.Task) -> None:
        async with cls._lock:
            cls._tasks[user_id] = task

    @classmethod
    async def cancel(cls, user_id: int) -> bool:
        async with cls._lock:
            t = cls._tasks.get(user_id)
        if t and not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            await cls.end(user_id)
            return True
        return False

    def __init__(
        self,
        bot: Any,
        ws: Any,
        msg_serializer,
        logger: Optional[logging.Logger] = None,
        send_sleep: float = 2.0,
        do_precache_count: bool = True,
        out_root: str = "/data/exports",
        save_json: bool = True,
    ) -> None:
        self.bot = bot
        self.ws = ws
        self.serialize = msg_serializer
        self.log = logger or logging.getLogger("dm_export")
        self.send_sleep = float(send_sleep)
        self.do_precache_count = bool(do_precache_count)
        self.out_root = out_root
        self.save_json = bool(save_json)

    async def run(self, user_id: int, webhook_url: Optional[str]) -> None:
        json_path: Optional[str] = None
        buffered: List[DictLike] = []
        try:
            dm, user = await self._find_dm_channel(user_id)
            if dm is None or user is None:
                self.log.warning(
                    f"[📥] DM export aborted: DM not in cache for user_id={user_id}"
                )
                await self._ws_send(
                    {
                        "type": "export_dm_done",
                        "data": {
                            "user_id": user_id,
                            "username": None,
                            "webhook_url": webhook_url,
                            "error": "dm-not-in-cache",
                        },
                    }
                )
                return

            uname = (
                getattr(user, "global_name", None)
                or getattr(user, "display_name", None)
                or user.name
            )

            if self.do_precache_count:
                found = 0
                async for _ in dm.history(limit=None, oldest_first=True):
                    found += 1
                self.log.info(
                    f"[📥] Starting DM export for {uname} ({user_id}) — messages found: {found}"
                )

            async for msg in dm.history(limit=None, oldest_first=True):
                serialized = self.serialize(msg)
                buffered.append(serialized)

            if self.save_json:
                try:
                    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                    subdir = os.path.join(self.out_root, "dm", str(user_id), ts)
                    os.makedirs(subdir, exist_ok=True)
                    json_path = os.path.join(subdir, "messages.json")
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(
                            {
                                "user_id": str(user_id),
                                "username": uname,
                                "exported_at": ts + "Z",
                                "count": len(buffered),
                                "messages": buffered,
                            },
                            f,
                            ensure_ascii=False,
                            indent=2,
                        )
                    self.log.info(f"[📥] DM JSON saved: {json_path}")
                except Exception as e:
                    self.log.warning(f"[📥] Failed to write DM JSON: {e}")
                    json_path = None

            sent = 0
            if webhook_url:
                for serialized in buffered:
                    payload: DictLike = {
                        "type": "export_dm_message",
                        "data": {
                            "user_id": user_id,
                            "username": uname,
                            "webhook_url": webhook_url,
                            "message": serialized,
                        },
                    }
                    try:
                        await self._ws_send(payload)
                        sent += 1
                    except Exception as send_err:
                        self.log.warning(
                            f"[📥] DM Export: websocket send failed (likely closed) after {sent} messages: {send_err}"
                        )
                        break
                    if self.send_sleep:
                        await asyncio.sleep(self.send_sleep)
            else:
                self.log.info("[📥] No webhook provided — skipping forwarding step.")

            if self.do_precache_count:
                self.log.info(
                    f"[📥] DM export finished for {uname} ({user_id}) — total sent: {sent}/{len(buffered)}"
                )
            else:
                self.log.info(
                    f"[📥] DM export finished for {uname} ({user_id}) — total sent: {sent}"
                )

            await self._ws_send(
                {
                    "type": "export_dm_done",
                    "data": {
                        "user_id": user_id,
                        "username": uname,
                        "webhook_url": webhook_url,
                        **({"json_path": json_path} if json_path else {}),
                    },
                }
            )

        except Exception as e:
            self.log.exception(f"[📥] DM export error for user_id={user_id}: {e}")
            await self._ws_send(
                {
                    "type": "export_dm_done",
                    "data": {
                        "user_id": user_id,
                        "username": None,
                        "webhook_url": webhook_url,
                        "error": str(e),
                        **({"json_path": json_path} if json_path else {}),
                    },
                }
            )

    async def _ws_send(self, payload: DictLike) -> None:
        await self.ws.send(payload)

    async def _find_dm_channel(
        self, user_id: int
    ) -> Tuple[Optional[Any], Optional[Any]]:
        user = self.bot.get_user(user_id)
        dm = None

        if user and getattr(user, "dm_channel", None):
            dm = user.dm_channel

        if dm is None:
            try:
                import discord
            except Exception:
                discord = None
            for ch in getattr(self.bot, "private_channels", []):
                if discord and isinstance(ch, getattr(discord, "DMChannel", ())):
                    if (
                        getattr(ch, "recipient", None)
                        and getattr(ch.recipient, "id", None) == user_id
                    ):
                        dm = ch
                        user = ch.recipient
                        break

        return dm, user