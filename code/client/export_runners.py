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
import random
import logging
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    TypeAlias,
    AsyncIterator,
)
from discord import ChannelType, MessageType, Object as DiscordObject
from discord.errors import HTTPException, Forbidden


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

    async def _collect_threads_for_channel(
        self,
        ch: Any,
        me: Optional[Any],
        *,
        include_forum_threads: bool,
        include_private_threads: bool,
    ) -> list[Any]:
        """
        Returns a list of Thread objects (active + archived) under the parent channel `ch`,
        respecting permissions if `me` is provided.
        Works for TextChannel and ForumChannel. Best-effort if API varies.
        """
        threads: list[Any] = []

        try:
            ch_name = getattr(ch, "name", "") or "unknown"
            ch_id = getattr(ch, "id", None)
        except Exception:
            ch_name, ch_id = "unknown", None

        is_forum = (
            getattr(ch, "__class__", type(ch)).__name__.lower().find("forum") != -1
        )
        if is_forum and not include_forum_threads:
            return threads

        try:
            active = list(getattr(ch, "threads", []) or [])
            for th in active:
                try:
                    if (
                        me is None
                        or getattr(th, "permissions_for")(me).read_message_history
                    ):
                        threads.append(th)
                except Exception:

                    threads.append(th)
        except Exception:
            pass

        try:
            if hasattr(ch, "archived_threads"):
                async for th in ch.archived_threads(limit=None, private=False):
                    try:
                        if (
                            me is None
                            or getattr(th, "permissions_for")(me).read_message_history
                        ):
                            threads.append(th)
                    except Exception:
                        threads.append(th)
        except Exception:

            pass

        if include_private_threads:
            try:
                if hasattr(ch, "archived_threads"):
                    async for th in ch.archived_threads(limit=None, private=True):
                        try:
                            if (
                                me is None
                                or getattr(th, "permissions_for")(
                                    me
                                ).read_message_history
                            ):
                                threads.append(th)
                        except Exception:
                            threads.append(th)
            except Exception:
                pass

        self.log.debug(
            f"[export] Collected {len(threads)} threads from #{ch_name} ({ch_id})"
        )
        return threads

    async def run(self, d: DictLike, guild: Any, acquired: bool = False) -> None:
        """
        Execute the export with the provided `data` and resolved `guild`.
        Emits progress logs and a final 'export_messages_done' ws event.
        """
        gid_log = getattr(guild, "id", None)
        gname = getattr(guild, "name", "") or "Unknown"

        if not acquired:
            ok = await self.try_begin(gid_log)
            if not ok:
                self.log.warning(
                    f"[export] Another export is already running for guild={gid_log}. Aborting run()."
                )
                try:
                    await self._ws_send(
                        {
                            "type": "export_messages_done",
                            "data": {
                                "guild_id": gid_log,
                                "error": "export-already-running",
                            },
                        }
                    )
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
            user_id = (
                int(user_id_raw) if (user_id_raw and user_id_raw.isdigit()) else None
            )

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
                    or {
                        "images": False,
                        "videos": False,
                        "audio": False,
                        "other": False,
                    }
                ),
                "threads": filters.get("threads", True),
                "forum_threads": filters.get("forum_threads", True),
                "private_threads": filters.get("private_threads", True),
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
                if not any(
                    bool(kinds.get(k)) for k in ("images", "videos", "audio", "other")
                ):
                    F["attachments"] = False

            if after_dt and before_dt and after_dt > before_dt:
                after_dt, before_dt = before_dt, after_dt

            me = None
            try:
                me = guild.get_member(getattr(self.bot.user, "id", None))
            except Exception:
                me = getattr(guild, "me", None)

            def _is_thread(obj: Any) -> bool:
                cls = getattr(obj, "__class__", type(obj)).__name__.lower()
                return "thread" in cls

            def _is_forum(obj: Any) -> bool:
                cls = getattr(obj, "__class__", type(obj)).__name__.lower()

                return "forum" in cls

            async def _collect_threads_for_parent(parent: Any) -> list[Any]:
                """
                Collect active + archived threads for a parent channel, respecting toggles and perms.
                """
                out: list[Any] = []

                if _is_forum(parent) and not F["forum_threads"]:
                    return out

                try:
                    for th in list(getattr(parent, "threads", []) or []):
                        try:
                            if (me is None) or getattr(th, "permissions_for")(
                                me
                            ).read_message_history:
                                out.append(th)
                        except Exception:
                            out.append(th)
                except Exception:
                    pass

                try:
                    if hasattr(parent, "archived_threads"):
                        async for th in parent.archived_threads(
                            limit=None, private=False
                        ):
                            try:
                                if (me is None) or getattr(th, "permissions_for")(
                                    me
                                ).read_message_history:
                                    out.append(th)
                            except Exception:
                                out.append(th)
                except Exception:
                    pass

                if F["private_threads"]:
                    try:
                        if hasattr(parent, "archived_threads"):
                            async for th in parent.archived_threads(
                                limit=None, private=True
                            ):
                                try:
                                    if (me is None) or getattr(th, "permissions_for")(
                                        me
                                    ).read_message_history:
                                        out.append(th)
                                except Exception:
                                    out.append(th)
                    except Exception:
                        pass

                return out

            scan_targets: List[Any] = []

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
                    if _is_thread(ch):

                        try:
                            if (me is None) or getattr(ch, "permissions_for")(
                                me
                            ).read_message_history:
                                scan_targets.append(ch)
                        except Exception:
                            scan_targets.append(ch)
                    else:

                        try:
                            if (me is None) or getattr(ch, "permissions_for")(
                                me
                            ).read_message_history:
                                scan_targets.append(ch)
                        except Exception:
                            scan_targets.append(ch)

                        if F["threads"]:
                            scan_targets.extend(await _collect_threads_for_parent(ch))

            if not scan_targets:
                text_chs = list(getattr(guild, "text_channels", []) or [])
                forum_chs = list(
                    (
                        getattr(guild, "forum_channels", None)
                        or getattr(guild, "forums", [])
                        or []
                    )
                )
                base_chs: List[Any] = []

                if me:
                    for c in text_chs + forum_chs:
                        try:
                            if c.permissions_for(me).read_message_history:
                                base_chs.append(c)
                        except Exception:
                            base_chs.append(c)
                else:
                    base_chs = text_chs + forum_chs

                scan_targets.extend(base_chs)

                if F["threads"]:
                    total_threads = 0
                    for parent in base_chs:
                        ths = await _collect_threads_for_parent(parent)
                        total_threads += len(ths)
                        scan_targets.extend(ths)
                    self.log.info(
                        f"[export] Thread discovery complete: parents={len(base_chs)}, threads={total_threads}"
                    )

            t0 = time.perf_counter()
            self.log.info(
                f"[export] Start task guild={gid_log} ({gname}) "
                f"filters: chan={chan_id_raw or 'ALL'}, user={user_id or 'ANY'}, "
                f"attachments_only={only_with_attachments}, after={after_dt}, before={before_dt}, "
                f"forward_webhook={do_forward}({('…'+wh_tail) if do_forward else '—'}), "
                f"scan_sleep={self.scan_sleep}, send_sleep={self.send_sleep}, ui_filters={filters}"
            )

            if not scan_targets:
                self.log.warning(
                    f"[export] No readable channels/threads in guild {gid_log}. Aborting."
                )
                await self._ws_send(
                    {
                        "type": "export_messages_done",
                        "data": {"guild_id": gid_log, "forwarded": 0, "scanned": 0},
                    }
                )
                return

            preview = []
            for c in scan_targets[:8]:
                cid = getattr(c, "id", None)
                tag = "thread" if _is_thread(c) else "chan"
                preview.append(f"{tag}:{cid}")
            more_note = (
                "" if len(scan_targets) <= 8 else f" (+{len(scan_targets)-8} more)"
            )
            self.log.info(
                f"[export] Scan targets: {len(scan_targets)} -> {preview}{more_note}"
            )

            total_scanned = 0
            total_matched = 0
            forwarded = 0
            buffer_rows: List[DictLike] = []

            for ch in scan_targets:
                cid = getattr(ch, "id", None)
                cname = getattr(ch, "name", "") or "unknown"
                is_thread = _is_thread(ch)
                label = f"{'#' if not is_thread else ''}{cname}{' [thread]' if is_thread else ''}"

                self.log.info(f"[export] Scanning {label} ({cid}) …")
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

                        if only_with_attachments and not getattr(
                            msg, "attachments", []
                        ):
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

                        serialized = self.serialize(msg)

                        is_thread = _is_thread(ch)
                        parent = getattr(ch, "parent", None)

                        serialized["_export_ctx"] = {
                            "channel_id": getattr(ch, "id", None),
                            "channel_name": getattr(ch, "name", None),
                            "is_thread": is_thread,
                            "parent_channel_id": getattr(ch, "parent_id", None),
                            "parent_channel_name": getattr(parent, "name", None),
                            "channel_kind": (
                                "thread"
                                if is_thread
                                else ("forum" if _is_forum(ch) else "text")
                            ),
                            "forum_parent_id": (
                                getattr(parent, "id", None)
                                if _is_forum(parent or ch)
                                else None
                            ),
                            "forum_parent_name": (
                                getattr(parent, "name", None)
                                if _is_forum(parent or ch)
                                else None
                            ),
                        }

                        buffer_rows.append(
                            {
                                "guild_id": getattr(guild, "id", None),
                                "channel_id": getattr(ch, "id", None),
                                "message": serialized,
                            }
                        )

                        if ch_scanned % 200 == 0:
                            self.log.info(
                                f"[export] Progress {('thread' if is_thread else 'ch')}={cid}: "
                                f"scanned={ch_scanned}, matched={ch_matched}, "
                                f"total_scanned={total_scanned}, total_matched={total_matched}, buffered={len(buffer_rows)}"
                            )

                        if self.scan_sleep:
                            await asyncio.sleep(self.scan_sleep)
                except Exception as e:
                    self.log.warning(f"[export] Target {cid} failed: {e}")
                    continue

                self.log.info(
                    f"[export] Done {label} ({cid}): scanned={ch_scanned}, matched={ch_matched}"
                )

            json_file: Optional[str] = None
            try:
                ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                gid_str = str(getattr(guild, "id", "unknown"))
                subdir = os.path.join(self.out_root, gid_str, ts)
                os.makedirs(subdir, exist_ok=True)

                json_file = os.path.join(subdir, "messages.json")
                self.log.info(
                    f"[export] Writing JSON snapshot ({len(buffer_rows)} messages) → {json_file}"
                )
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
            want_any_download = any(
                dl_cfg.get(k, False) for k in ("images", "videos", "audio", "other")
            )
            media_report = {
                "images": 0,
                "videos": 0,
                "audio": 0,
                "other": 0,
                "errors": 0,
            }

            if want_any_download and aiohttp is None:
                self.log.warning(
                    "[export] Download requested but aiohttp not available. Skipping downloads."
                )
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
                    self.log.info(
                        "[export] No media matches selection; skipping download step."
                    )
                else:
                    media_root = os.path.join(os.path.dirname(json_file or ""), "media")
                    os.makedirs(media_root, exist_ok=True)
                    kind_dirs: Dict[str, str] = {
                        k: os.path.join(media_root, k) for k in used_kinds
                    }
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
                        self.log.warning(
                            f"[📤] export_message send failed (post-JSON) idx={idx}: {e}"
                        )
                        break

                    if idx % 200 == 0:
                        self.log.info(
                            f"[export] Forwarded {idx}/{len(buffer_rows)} (total_forwarded={forwarded})"
                        )
            else:
                self.log.info(
                    "[export] No webhook URL provided — skipping forwarding step."
                )

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
                await self._ws_send(
                    {"type": "export_messages_done", "data": done_payload}
                )
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

    _lock: asyncio.Lock = asyncio.Lock()
    _active_users: set[int] = set()
    _tasks: dict[int, asyncio.Task] = {}

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


class BackfillEngine:
    """
    Message backfill engine for for pulling a channels message history.
    """

    MAX_RETRIES = 6
    BASE_DELAY = 0.75
    JITTER = 0.40
    PROGRESS_EVERY = 50
    LOG_EVERY_SEC = 5.0

    def __init__(self, receiver, *, logger: Optional[logging.Logger] = None):
        self.r = receiver
        self.bot = receiver.bot
        self.ws = receiver.ws
        self.msg = receiver.msg
        self.host_guild_id = getattr(receiver, "host_guild_id", None)
        self.logger = logger or logging.getLogger("backfill")

        try:
            from zoneinfo import ZoneInfo

            _DEFAULT_UI_TZ = os.getenv("UI_TZ", "America/New_York")
            self.LOCAL_TZ = ZoneInfo(_DEFAULT_UI_TZ)
        except Exception:
            self.LOCAL_TZ = datetime.now().astimezone().tzinfo or timezone.utc

    async def run_channel(
        self,
        original_channel_id: int,
        *,
        after_iso: Optional[str] = None,
        before_iso: Optional[str] = None,
        last_n: Optional[int] = None,
    ) -> None:
        """
        Primary entry point used by client code to backfill a single channel.
        """
        loop = asyncio.get_event_loop()
        t0 = loop.time()
        sent = 0
        skipped = 0
        last_ping = 0.0
        last_log = 0.0

        def _coerce_int(x):
            try:
                return int(x) if x is not None else None
            except Exception:
                return None

        after_dt = self._parse_iso(after_iso)
        before_dt = self._parse_iso(before_iso)

        mode = (
            "last_n"
            if _coerce_int(last_n)
            else (
                "between"
                if (after_iso and before_iso)
                else ("since" if after_iso else "all")
            )
        )

        self.logger.info(
            "[backfill] ▶ START requested | channel_id=%s mode=%s after=%r before=%r last_n=%r — large channels can take a while to pull history",
            original_channel_id,
            mode,
            after_iso,
            before_iso,
            last_n,
        )

        self.logger.debug(
            "[backfill] INIT | channel=%s mode=%s after_iso=%r before_iso=%r last_n=%r",
            original_channel_id,
            mode,
            after_iso,
            before_iso,
            last_n,
        )

        await self._safe_ws_send(
            {
                "type": "backfill_started",
                "data": {
                    "channel_id": original_channel_id,
                    "range": {
                        "after": after_iso,
                        "before": before_iso,
                        "last_n": last_n,
                    },
                },
            }
        )

        guild = await self._resolve_guild(original_channel_id)
        if not guild:
            self.logger.error(
                "[backfill] ⛔ no accessible guild found for channel=%s",
                original_channel_id,
            )
            await self._finish_progress_only(original_channel_id, sent)
            return

        ch = guild.get_channel(original_channel_id)
        if not ch:
            try:
                ch = await self._retry(
                    "fetch_channel", lambda: self.bot.fetch_channel(original_channel_id)
                )
                self.logger.debug(
                    "[backfill] channel fetched | id=%s name=%s type=%s",
                    getattr(ch, "id", None),
                    getattr(ch, "name", None),
                    getattr(getattr(ch, "type", None), "value", None),
                )
            except Exception as e:
                self.logger.error(
                    "[backfill] ⛔ cannot fetch channel | id=%s err=%s",
                    original_channel_id,
                    e,
                )
                await self._finish_progress_only(original_channel_id, sent)
                return
        else:
            self.logger.debug(
                "[backfill] channel OK | id=%s name=%s type=%s",
                getattr(ch, "id", None),
                getattr(ch, "name", None),
                getattr(getattr(ch, "type", None), "value", None),
            )

        if (
            self.host_guild_id
            and getattr(getattr(ch, "guild", None), "id", None) != self.host_guild_id
        ):
            self.logger.warning(
                "[backfill] channel is not in host guild | got_guild=%s expected=%s (was a clone id passed?)",
                getattr(getattr(ch, "guild", None), "id", None),
                self.host_guild_id,
            )

        ALLOWED_TYPES = {MessageType.default}
        for _name in ("reply", "application_command", "context_menu_command"):
            _t = getattr(MessageType, _name, None)
            if _t is not None:
                ALLOWED_TYPES.add(_t)
        self.logger.debug(
            "[backfill] allowed message types: %s",
            sorted(getattr(t, "value", str(t)) for t in ALLOWED_TYPES),
        )

        def _is_normal(msg) -> bool:
            try:
                if callable(getattr(msg, "is_system", None)) and msg.is_system():
                    return False
            except Exception:
                pass
            if getattr(getattr(msg, "author", None), "system", False):
                return False
            if getattr(msg, "type", None) not in ALLOWED_TYPES:
                return False
            return True

        if after_iso and after_dt:
            self.logger.debug(
                "[backfill] since filter parsed | utc=%s", after_dt.isoformat()
            )
        elif after_iso and not after_dt:
            self.logger.warning(
                "[backfill] since filter parse failed | after_iso=%r (ignoring)",
                after_iso,
            )

        if before_iso and before_dt:
            self.logger.debug(
                "[backfill] before filter parsed | utc=%s", before_dt.isoformat()
            )
        elif before_iso and not before_dt:
            self.logger.warning(
                "[backfill] before filter parse failed | before_iso=%r (ignoring)",
                before_iso,
            )

        async def _emit_msg(m):
            nonlocal sent, skipped, last_ping, last_log
            if not _is_normal(m):
                skipped += 1
                return

            raw = m.content or ""
            system = getattr(m, "system_content", "") or ""
            content = system if (not raw and system) else raw
            author_name = (
                "System"
                if (not raw and system)
                else getattr(m.author, "name", "Unknown")
            )

            raw_embeds = [e.to_dict() for e in m.embeds]
            mention_map = await self.msg.build_mention_map(m, raw_embeds)
            embeds = [
                self.msg.sanitize_embed_dict(e, m, mention_map) for e in raw_embeds
            ]
            content = self.msg.sanitize_inline(content, m, mention_map)
            stickers_payload = self.msg.stickers_payload(getattr(m, "stickers", []))

            is_thread = getattr(m.channel, "type", None) in (
                ChannelType.public_thread,
                ChannelType.private_thread,
            )
            payload = {
                "type": "thread_message" if is_thread else "message",
                "data": {
                    "guild_id": getattr(m.guild, "id", None),
                    "message_id": getattr(m, "id", None),
                    "channel_id": m.channel.id,
                    "channel_name": getattr(m.channel, "name", None),
                    "channel_type": (
                        getattr(m.channel, "type", None).value
                        if getattr(m.channel, "type", None)
                        else None
                    ),
                    "author": author_name,
                    "author_id": getattr(m.author, "id", None),
                    "avatar_url": (
                        str(m.author.display_avatar.url)
                        if getattr(m.author, "display_avatar", None)
                        else None
                    ),
                    "content": content,
                    "attachments": [
                        {"url": a.url, "filename": a.filename, "size": a.size}
                        for a in m.attachments
                    ],
                    "stickers": stickers_payload,
                    "embeds": embeds,
                    "__backfill__": True,
                    **(
                        {
                            "thread_parent_id": getattr(
                                getattr(m.channel, "parent", None), "id", None
                            ),
                            "thread_parent_name": getattr(
                                getattr(m.channel, "parent", None), "name", None
                            ),
                            "thread_id": getattr(m.channel, "id", None),
                            "thread_name": getattr(m.channel, "name", None),
                        }
                        if is_thread
                        else {}
                    ),
                },
            }

            await self._safe_ws_send(payload)
            sent += 1
            await asyncio.sleep(0.02)

            now = loop.time()
            if sent % self.PROGRESS_EVERY == 0 or (now - last_ping) >= 2.0:
                await self._safe_ws_send(
                    {
                        "type": "backfill_progress",
                        "data": {"channel_id": original_channel_id, "sent": sent},
                    }
                )
                last_ping = now

            if (now - last_log) >= self.LOG_EVERY_SEC:
                dt = now - t0
                rate = (sent / dt) if dt > 0 else 0.0
                self.logger.debug(
                    "[backfill] progress | channel=%s sent=%d skipped=%d rate=%.1f msg/s",
                    original_channel_id,
                    sent,
                    skipped,
                    rate,
                )
                last_log = now

        async def _count_history(obj, *, n: Optional[int], after_dt, before_dt) -> int:
            total = 0
            oldest_first = not (n and n > 0)
            try:
                if n and n > 0:
                    cnt = 0
                    async for m in self._iter_history_resumable(
                        obj,
                        n=n,
                        after_dt=after_dt,
                        before_dt=before_dt,
                        oldest_first=False,
                    ):
                        if _is_normal(m):
                            cnt += 1
                    total = cnt
                else:
                    async for m in self._iter_history_resumable(
                        obj,
                        n=None,
                        after_dt=after_dt,
                        before_dt=before_dt,
                        oldest_first=True,
                    ):
                        if _is_normal(m):
                            total += 1
            except Forbidden:
                pass
            return total

        async def _stream_history(obj, *, n: Optional[int], after_dt, before_dt):

            if n and n > 0:
                tmp = []
                async for m in self._iter_history_resumable(
                    obj, n=n, after_dt=after_dt, before_dt=before_dt, oldest_first=False
                ):
                    tmp.append(m)
                for m in reversed(tmp):
                    await _emit_msg(m)
            else:
                async for m in self._iter_history_resumable(
                    obj,
                    n=None,
                    after_dt=after_dt,
                    before_dt=before_dt,
                    oldest_first=True,
                ):
                    await _emit_msg(m)

        try:
            n = _coerce_int(last_n)

            if getattr(ch, "type", None) == ChannelType.forum:
                self.logger.debug(
                    "[backfill] forum detected | id=%s name=%s",
                    getattr(ch, "id", None),
                    getattr(ch, "name", None),
                )

                total = 0
                async for th in self._iter_all_threads(ch):
                    try:
                        total += await _count_history(
                            th, n=n, after_dt=after_dt, before_dt=before_dt
                        )
                    except Forbidden:
                        self.logger.debug(
                            "[backfill] thread history forbidden | thread=%s",
                            getattr(th, "id", None),
                        )
                    except HTTPException as e:
                        self.logger.warning(
                            "[backfill] HTTP while counting thread=%s err=%s",
                            getattr(th, "id", None),
                            e,
                        )

                await self._safe_ws_send(
                    {
                        "type": "backfill_progress",
                        "data": {"channel_id": original_channel_id, "total": total},
                    }
                )

                async for th in self._iter_all_threads(ch):
                    try:
                        await _stream_history(
                            th, n=n, after_dt=after_dt, before_dt=before_dt
                        )
                    except Forbidden:
                        self.logger.debug(
                            "[backfill] thread history forbidden | thread=%s",
                            getattr(th, "id", None),
                        )
                    except HTTPException as e:
                        self.logger.warning(
                            "[backfill] HTTP while streaming thread=%s err=%s",
                            getattr(th, "id", None),
                            e,
                        )

            else:
                if n and n > 0:
                    buf = []
                    self.logger.debug(
                        "[backfill] mode=last_n | n=%d %s",
                        n,
                        (
                            f"(after {after_dt.isoformat()})"
                            if after_dt
                            else "(no since bound)"
                        ),
                    )
                    async for m in self._iter_history_resumable(
                        ch,
                        n=n,
                        after_dt=after_dt,
                        before_dt=before_dt,
                        oldest_first=False,
                    ):
                        buf.append(m)

                    parent_total = sum(1 for m in buf if _is_normal(m))
                    self.logger.debug(
                        "[backfill] fetched buffer | size=%d, total_normal=%d",
                        len(buf),
                        parent_total,
                    )

                    thread_total = 0
                    async for th in self._iter_all_threads(ch):
                        try:
                            thread_total += await _count_history(
                                th, n=n, after_dt=after_dt, before_dt=before_dt
                            )
                        except Forbidden:
                            self.logger.debug(
                                "[backfill] thread history forbidden | thread=%s",
                                getattr(th, "id", None),
                            )
                        except HTTPException as e:
                            self.logger.warning(
                                "[backfill] HTTP while counting thread=%s err=%s",
                                getattr(th, "id", None),
                                e,
                            )

                    combined_total = parent_total + thread_total
                    await self._safe_ws_send(
                        {
                            "type": "backfill_progress",
                            "data": {
                                "channel_id": original_channel_id,
                                "total": combined_total,
                            },
                        }
                    )

                    for m in reversed(buf):
                        await _emit_msg(m)

                    async for th in self._iter_all_threads(ch):
                        try:
                            await _stream_history(
                                th, n=n, after_dt=after_dt, before_dt=before_dt
                            )
                        except Forbidden:
                            self.logger.debug(
                                "[backfill] thread history forbidden | thread=%s",
                                getattr(th, "id", None),
                            )
                        except HTTPException as e:
                            self.logger.warning(
                                "[backfill] HTTP while streaming thread=%s err=%s",
                                getattr(th, "id", None),
                                e,
                            )

                else:
                    self.logger.debug(
                        "[backfill] mode=%s | streaming oldest→newest%s",
                        "since" if after_dt else "all",
                        f" (after {after_dt.isoformat()})" if after_dt else "",
                    )

                    parent_buf = []
                    async for m in self._iter_history_resumable(
                        ch,
                        n=None,
                        after_dt=after_dt,
                        before_dt=before_dt,
                        oldest_first=True,
                    ):
                        parent_buf.append(m)
                    parent_total = sum(1 for m in parent_buf if _is_normal(m))
                    self.logger.debug(
                        "[backfill] pre-count complete | fetched=%d total_normal=%d",
                        len(parent_buf),
                        parent_total,
                    )

                    thread_total = 0
                    async for th in self._iter_all_threads(ch):
                        try:
                            thread_total += await _count_history(
                                th, n=None, after_dt=after_dt, before_dt=before_dt
                            )
                        except Forbidden:
                            self.logger.debug(
                                "[backfill] thread history forbidden | thread=%s",
                                getattr(th, "id", None),
                            )
                        except HTTPException as e:
                            self.logger.warning(
                                "[backfill] HTTP while counting thread=%s err=%s",
                                getattr(th, "id", None),
                                e,
                            )

                    combined_total = parent_total + thread_total
                    await self._safe_ws_send(
                        {
                            "type": "backfill_progress",
                            "data": {
                                "channel_id": original_channel_id,
                                "total": combined_total,
                            },
                        }
                    )

                    for m in parent_buf:
                        await _emit_msg(m)

                    async for th in self._iter_all_threads(ch):
                        try:
                            await _stream_history(
                                th, n=None, after_dt=after_dt, before_dt=before_dt
                            )
                        except Forbidden:
                            self.logger.debug(
                                "[backfill] thread history forbidden | thread=%s",
                                getattr(th, "id", None),
                            )
                        except HTTPException as e:
                            self.logger.warning(
                                "[backfill] HTTP while streaming thread=%s err=%s",
                                getattr(th, "id", None),
                                e,
                            )

        except Forbidden as e:
            self.logger.debug(
                "[backfill] history forbidden | channel=%s err=%s",
                original_channel_id,
                e,
            )
        except HTTPException as e:
            if self._should_retry_http(e):
                self.logger.warning(
                    "[backfill] HTTP 5xx bubbled | channel=%s err=%s",
                    original_channel_id,
                    e,
                )
            else:
                self.logger.warning(
                    "[backfill] HTTP non-retryable | channel=%s err=%s",
                    original_channel_id,
                    e,
                )
        except Exception as e:
            self.logger.exception(
                "[backfill] unexpected error | channel=%s err=%s",
                original_channel_id,
                e,
            )
        finally:
            try:
                await self._safe_ws_send(
                    {
                        "type": "backfill_progress",
                        "data": {"channel_id": original_channel_id, "sent": sent},
                    }
                )
            except Exception:
                pass

            dur = loop.time() - t0
            self.logger.debug(
                "[backfill] STREAM END | channel=%s mode=%s sent=%d skipped=%d dur=%.1fs",
                original_channel_id,
                mode,
                sent,
                skipped,
                dur,
            )
            await self._safe_ws_send(
                {
                    "type": "backfill_stream_end",
                    "data": {"channel_id": original_channel_id},
                }
            )

    def _parse_iso(self, s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            try:
                dt = datetime.fromisoformat(s + ":00")
            except Exception:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self.LOCAL_TZ).astimezone(timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt

    async def _resolve_guild(self, original_channel_id: int):
        guild = None
        if self.host_guild_id:
            guild = self.bot.get_guild(self.host_guild_id)

        if not guild:
            ch_probe = None
            try:
                ch_probe = self.bot.get_channel(
                    original_channel_id
                ) or await self._retry(
                    "fetch_channel(probe)",
                    lambda: self.bot.fetch_channel(original_channel_id),
                )
            except Exception:
                ch_probe = None
            if ch_probe and getattr(ch_probe, "guild", None):
                guild = ch_probe.guild

        if not guild and self.bot.guilds:
            guild = self.bot.guilds[0]

        if guild:
            self.logger.debug(
                "[backfill] guild OK | id=%s name=%s",
                getattr(guild, "id", None),
                getattr(guild, "name", None),
            )
        return guild

    def _should_retry_http(self, e: Exception) -> bool:
        status = getattr(e, "status", None)
        return status in {500, 502, 503, 504, 520, 522, 524}

    async def _retry(self, desc: str, op):
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                return await op()
            except HTTPException as e:
                if not self._should_retry_http(e) or attempt >= self.MAX_RETRIES:
                    raise
                delay = (
                    self.BASE_DELAY
                    * (2**attempt)
                    * (1.0 + random.random() * self.JITTER)
                )
                self.logger.warning(
                    "[backfill] retry %s after HTTP %s attempt=%d sleep=%.2fs",
                    desc,
                    getattr(e, "status", "?"),
                    attempt + 1,
                    delay,
                )
                await asyncio.sleep(delay)

    async def _safe_ws_send(self, payload: dict) -> None:
        try:
            await self.ws.send(payload)
        except Exception:

            pass

    async def _iter_all_threads(self, parent) -> AsyncIterator:
        """
        Yield all threads (active + archived) under a TextChannel or ForumChannel.

        Rules:
        - ForumChannel: only public archived threads (no `private=`/`joined=`).
        - TextChannel: public archived, then private archived, then joined private archived.
        """
        seen: set[int] = set()

        for th in getattr(parent, "threads", None) or []:
            tid = getattr(th, "id", None)
            if th and tid is not None and tid not in seen:
                seen.add(tid)
                yield th

        async def _drain(iter_factory, label: str):
            """
            Run an iterator factory once, with a retry on transient HTTP 5xx.
            iter_factory() must return an async iterator (NOT a coroutine).
            """

            async def _once():
                it = iter_factory()
                async for th in it:
                    tid = getattr(th, "id", None)
                    if th and tid is not None and tid not in seen:
                        seen.add(tid)
                        yield th

            try:
                async for th in _once():
                    yield th
            except Forbidden:
                self.logger.debug(
                    "[backfill] %s threads forbidden | channel=%s",
                    label,
                    getattr(parent, "id", None),
                )
            except ValueError as e:

                self.logger.debug(
                    "[backfill] %s unavailable: %s | channel=%s",
                    label,
                    e,
                    getattr(parent, "id", None),
                )
            except HTTPException as e:
                if self._should_retry_http(e):
                    self.logger.warning(
                        "[backfill] transient HTTP %s on %s; retrying",
                        getattr(e, "status", "?"),
                        label,
                    )
                    try:
                        async for th in _once():
                            yield th
                    except Exception:
                        self.logger.warning(
                            "[backfill] %s threads failed after retry", label
                        )
                else:
                    raise

        def _public_iter():
            return parent.archived_threads(limit=None)

        async for th in _drain(_public_iter, "archived public"):
            yield th

        # If it's a ForumChannel, stop here (no private/joined on forums).
        if getattr(parent, "type", None) == ChannelType.forum:
            return

        def _private_iter():
            return parent.archived_threads(private=True, limit=None)

        try:
            async for th in _drain(_private_iter, "archived private"):
                yield th
        except TypeError:

            pass

        def _joined_private_iter():
            return parent.archived_threads(private=True, joined=True, limit=None)

        try:
            async for th in _drain(_joined_private_iter, "archived private (joined)"):
                yield th
        except (TypeError, Forbidden):

            pass

    async def _iter_history_resumable(
        self,
        obj,
        *,
        n: Optional[int],
        after_dt: Optional[datetime],
        before_dt: Optional[datetime],
        oldest_first: bool,
    ) -> AsyncIterator:
        """
        Yield messages with a cursor that survives transient 5xx errors.
        Streams all history when n is None (no artificial 100-msg cap).
        """
        remaining = n if (n and n > 0) else None
        last_id = None

        while True:
            limit = 100 if remaining is None else min(remaining, 100)
            kw = {
                "limit": limit,
                "oldest_first": oldest_first,
                "after": after_dt,
                "before": before_dt,
            }

            if last_id is not None:
                if oldest_first:
                    kw["after"] = DiscordObject(id=last_id)
                else:
                    kw["before"] = DiscordObject(id=last_id)

            try:
                fetched_any = False
                async for m in obj.history(**kw):
                    fetched_any = True
                    last_id = m.id
                    yield m
                    if remaining is not None:
                        remaining -= 1
                        if remaining <= 0:
                            return

                if not fetched_any:
                    return

                continue

            except HTTPException as e:
                if self._should_retry_http(e):
                    delay = self.BASE_DELAY * (1.0 + random.random() * self.JITTER)
                    self.logger.warning(
                        "[backfill] transient HTTP %s on history; resuming after sleep=%.2fs",
                        getattr(e, "status", "?"),
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

    async def _finish_progress_only(self, channel_id: int, sent: int) -> None:
        try:
            await self._safe_ws_send(
                {
                    "type": "backfill_progress",
                    "data": {"channel_id": channel_id, "sent": sent},
                }
            )
        finally:
            await self._safe_ws_send(
                {"type": "backfill_stream_end", "data": {"channel_id": channel_id}}
            )
