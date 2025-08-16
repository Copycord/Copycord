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
import gzip
import asyncio
import io
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import discord


@dataclass
class ExportJob:
    task: asyncio.Task
    user_id: int
    include_names: bool
    started_at: float


class MemberExportService:
    """
    Helper service for slash-command driven member exports
    """

    def __init__(
        self,
        bot: discord.Client,
        ws_manager,
        *,
        logger: Optional[logging.Logger] = None,
        request_timeout: Optional[float] = None,
        stream_timeout: Optional[float] = None,
        default_chunk_size: int = 512 * 1024,
    ) -> None:
        self.bot = bot
        self.ws = ws_manager
        self.logger = logger.getChild(self.__class__.__name__)
        self.request_timeout = request_timeout
        self.stream_timeout = stream_timeout
        self.default_chunk_size = int(default_chunk_size)

    def _fmt_duration(self, seconds: float) -> str:
        """
        Format a duration given in seconds into a human-readable string.
        """
        seconds = int(seconds)
        h, r = divmod(seconds, 3600)
        m, s = divmod(r, 60)
        if h:
            return f"{h}h {m}m {s}s"
        if m:
            return f"{m}m {s}s"
        return f"{s}s"

    async def request_or_stream(
        self,
        include_names: bool,
        *,
        request_timeout: Optional[float] = None,
        stream_timeout: Optional[float] = None,
        num_sessions: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Issues the 'scrape_members' RPC and (if needed) consumes the stream to return final `data`.
        Returns {"ok": True, "data": {...}} or {"ok": False, "error": "..."}.
        """
        req_t = self.request_timeout if request_timeout is None else request_timeout
        st_t = self.stream_timeout if stream_timeout is None else stream_timeout

        try:
            req_kwargs = {}

            if req_t is not None:
                req_kwargs["timeout"] = float(req_t)

            data = {"include_names": bool(include_names)}

            if num_sessions is not None:
                data["num_sessions"] = int(num_sessions)

            resp = await self.ws.request(
                {"type": "scrape_members", "data": data},
                **req_kwargs,
            )
        except Exception as e:
            self.logger.warning("request_or_stream: initial request failed: %r", e)
            return {"ok": False, "error": f"request-failed: {e!r}"}

        if not resp or not resp.get("ok"):
            return {"ok": False, "error": (resp or {}).get("error", "no-response")}

        if "data" in resp:
            return {"ok": True, "data": resp["data"]}

        stream = resp.get("stream")
        if not stream:
            return {"ok": False, "error": "missing-data-and-stream"}

        sid = stream["id"]
        chunk_size = int(stream.get("chunk_size", self.default_chunk_size))
        try:
            data = await self.consume_stream(sid, 0, chunk_size, timeout=st_t)
            return {"ok": True, "data": data}
        except Exception as e:
            try:
                await self.ws.request({"type": "stream_abort", "data": {"id": sid}})
            except Exception:
                pass
            self.logger.info("request_or_stream: stream failed: %r", e)
            return {"ok": False, "error": f"stream-failed: {e!r}"}

    async def consume_stream(
        self,
        sid: str,
        start_offset: int = 0,
        chunk_size: Optional[int] = None,
        *,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Pulls all chunks for a given stream ID and returns the decoded JSON object.
        Raises on error.
        """
        cs = int(chunk_size or self.default_chunk_size)
        parts: List[bytes] = []
        offset = int(start_offset)

        while True:
            req_kwargs = {}
            if timeout is not None:
                req_kwargs["timeout"] = float(timeout)

            nxt = await self.ws.request(
                {
                    "type": "stream_next",
                    "data": {"id": sid, "offset": offset, "length": cs},
                },
                **req_kwargs,
            )
            if not nxt or not nxt.get("ok"):
                raise RuntimeError((nxt or {}).get("error", "stream-next-failure"))

            if nxt.get("eof"):
                break

            parts.append(base64.b64decode(nxt["data_b64"]))
            offset = int(nxt["next"])

        gz = b"".join(parts)
        try:
            return json.loads(gzip.decompress(gz).decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"decompress/parse-failed: {e!r}")

    @staticmethod
    def build_member_export_payload(
        raw_members: List[Any],
        include_names: bool,
        *,
        timestamp: Optional[str] = None,
    ) -> Tuple[str, bytes, str]:
        """
        Shapes the export into a file payload.
        Returns (filename, payload_bytes, summary_text).
        """
        if not timestamp:
            timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

        if not include_names:
            ids: List[int] = []
            for m in raw_members:
                if isinstance(m, str):
                    ids.append(int(m))
                elif isinstance(m, dict) and m.get("id"):
                    ids.append(int(m["id"]))
            filename = f"members_ids_{timestamp}.json"
            payload = json.dumps(ids, indent=2).encode("utf-8")
            summary = f"Found {len(ids)} members"
            return filename, payload, summary

        shaped: List[Dict[str, Any]] = []
        for m in raw_members:
            if isinstance(m, dict) and m.get("id"):
                shaped.append(
                    {
                        "id": str(m.get("id")),
                        "username": m.get("username"),
                        "bot": bool(m.get("bot", False)),
                    }
                )
            elif isinstance(m, str):
                shaped.append({"id": m, "username": None, "bot": False})
        filename = f"members_named_{timestamp}.json"
        payload = json.dumps(shaped, indent=2).encode("utf-8")
        summary = f"Found {len(shaped)} members"
        return filename, payload, summary

    async def dm_export_file(
        self,
        user_id: int,
        *,
        filename: str,
        payload: bytes,
        summary: str,
        duration_secs: float | None = None,
        total: int | None = None,
        include_names: bool | None = None,
    ) -> None:
        user: Optional[discord.User] = self.bot.get_user(int(user_id))
        try:
            if user is None:
                user = await self.bot.fetch_user(int(user_id))
        except Exception:
            user = None
        if user is None:
            self.logger.warning("dm_export_file: cannot resolve user_id=%s", user_id)
            return

        embed = discord.Embed(
            title="Member export complete",
            timestamp=datetime.now(timezone.utc),
            color=discord.Color.blurple(),
        )
        if total is not None:
            embed.add_field(name="Members Found", value=str(total), inline=True)
        if duration_secs is not None:
            embed.add_field(
                name="Duration", value=self._fmt_duration(duration_secs), inline=True
            )

        try:
            await user.send(
                embed=embed,
                file=discord.File(io.BytesIO(payload), filename=filename),
            )
            return
        except discord.HTTPException as e:
            if e.code == 40005:  # too large → gzip and retry
                gz = gzip.compress(payload)
                try:
                    await user.send(
                        embed=embed,
                        file=discord.File(io.BytesIO(gz), filename=f"{filename}.gz"),
                    )
                except Exception as e2:
                    self.logger.warning(
                        "dm_export_file: gzip send failed for user=%s: %r",
                        getattr(user, "id", user_id),
                        e2,
                    )
            elif e.code == 50007:
                self.logger.info(
                    "dm_export_file: DMs closed for user=%s",
                    getattr(user, "id", user_id),
                )
            else:
                self.logger.warning(
                    "dm_export_file: HTTP error for user=%s: %r",
                    getattr(user, "id", user_id),
                    e,
                )
        except Exception as e:
            self.logger.warning(
                "dm_export_file: generic failure for user=%s: %r", user_id, e
            )

    async def background_finish_and_dm(
        self,
        user_id: int,
        include_names: bool,
        *,
        request_timeout: Optional[float] = None,
        stream_timeout: Optional[float] = None,
        num_sessions: Optional[int] = None,
    ) -> None:
        """
        Start a fresh scrape in the background and DM the result.
        """
        t0 = time.monotonic()
        try:
            result = await self.request_or_stream(
                include_names,
                request_timeout=request_timeout,
                stream_timeout=stream_timeout,
                num_sessions=num_sessions,
            )
            if not result.get("ok"):
                self.logger.debug(
                    "background_finish_and_dm: scrape did not finish: %s",
                    result.get("error"),
                )
                return
            elapsed = time.monotonic() - t0
            data = result["data"]
            raw_members = data.get("members") or []
            filename, payload, summary = self.build_member_export_payload(
                raw_members, include_names
            )

            await self.dm_export_file(
                user_id,
                filename=filename,
                payload=payload,
                summary=summary,
                duration_secs=elapsed,
                total=len(raw_members),
                include_names=include_names,
            )
        except asyncio.CancelledError:
            self.logger.info(
                "[🛑] Scrape canceled early — collected: %d members", len(raw_members)
            )
            raise
        except Exception as e:
            self.logger.exception("background_finish_and_dm: unexpected error: %r", e)

    async def resume_stream_and_dm(
        self,
        user_id: int,
        include_names: bool,
        *,
        sid: str,
        offset: int,
        chunk_size: Optional[int] = None,
        stream_timeout: Optional[float] = None,
    ) -> None:
        """
        Resume an existing partial stream (sid, offset). If the stream is gone/expired,
        fall back to a full background scrape, then DM the result.
        """
        t0 = time.monotonic()
        cs = int(chunk_size or self.default_chunk_size)
        try:
            try:
                data = await self.consume_stream(
                    sid, offset, cs, timeout=stream_timeout
                )
            except Exception as e:
                self.logger.info(
                    "resume_stream_and_dm: resume failed (%r); starting fresh", e
                )
                await self.background_finish_and_dm(
                    user_id,
                    include_names,
                    request_timeout=None,
                    stream_timeout=None,
                )
                return

            elapsed = time.monotonic() - t0
            raw_members = data.get("members") or []
            filename, payload, summary = self.build_member_export_payload(
                raw_members, include_names
            )
            summary_with_time = f"{summary}\n🕒 Duration: {self._fmt_duration(elapsed)}"

            await self.dm_export_file(
                user_id,
                filename=filename,
                payload=payload,
                summary=summary_with_time,
                duration_secs=elapsed,
                total=len(raw_members),
                include_names=include_names,
            )
        except Exception as e:
            self.logger.exception("resume_stream_and_dm: unexpected error: %r", e)
