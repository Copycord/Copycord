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
import time
import uuid
from typing import Optional, Any, Dict
import discord
from common.config import CURRENT_VERSION


class ClientUiController:
    def __init__(
        self,
        *,
        bus,
        admin_base_url: str,
        bot: discord.Client,
        guild_id: Optional[int] = None,
        listener,
        logger: Optional[logging.Logger] = None,
        topic: str = "client",
    ):
        self.bus = bus
        self.admin_base_url = admin_base_url.rstrip("/")
        self.bot = bot
        try:
            self.guild_id = int(guild_id) if guild_id is not None else None
        except (TypeError, ValueError):
            self.guild_id = None
        self.listener = listener
        self.topic = topic
        self.log = logger or logging.getLogger("ClientUiController")
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    def start(self) -> None:
        if self._task and not self._task.done():
            self.log.debug(
                "start() ignored; task already running: %s", self._task.get_name()
            )
            return
        self._stopping = False
        self._task = asyncio.create_task(self._listen_loop(), name="ui-listen")
        gid_dbg = self.guild_id if self.guild_id is not None else "(none)"
        self.log.debug(
            "ClientUiController started | task=%s guild_id=%s admin_base_url=%s topic=%s",
            self._task.get_name(),
            gid_dbg,
            self.admin_base_url,
            self.topic,
        )

    async def stop(self) -> None:
        self._stopping = True
        if not self._task:
            return
        self.log.debug("ClientUiController stopping | task=%s", self._task.get_name())
        self._task.cancel()
        with asyncio.CancelledError.__enter__ if False else None:
            pass
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        self.log.debug("ClientUiController stopped")

    @staticmethod
    def _ms_since(t0: float) -> float:
        return (time.perf_counter() - t0) * 1000.0

    @staticmethod
    def _new_req_id() -> str:
        return uuid.uuid4().hex[:8]

    async def _publish(self, payload: Dict[str, Any]) -> None:
        payload = dict(payload or {})
        payload.setdefault("req_id", self._new_req_id())
        t0 = time.perf_counter()
        try:
            await self.bus.publish(self.topic, payload)
            self.log.debug(
                "TX client event -> bus | ok req_id=%s took=%.1fms payload=%s",
                payload.get("req_id"),
                self._ms_since(t0),
                _safe_preview(payload),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.warning(
                "TX client event -> bus FAILED | req_id=%s took=%.1fms err=%s payload=%s",
                payload.get("req_id"),
                self._ms_since(t0),
                e,
                _safe_preview(payload),
            )

    async def _listen_loop(self):
        async def _handler(ev: dict):

            kind = (ev.get("kind") or ev.get("topic") or "").lower()
            role = (ev.get("role") or ev.get("source") or "").lower()

            self.log.debug("Bus event RX | kind=%r role=%r", kind, role)
            self.log.debug("Bus event payload = %s", _safe_preview(ev))

            if kind != "client":
                self.log.debug("Ignoring non-client event.")
                return
            if role and role != "ui":
                self.log.debug("Ignoring client event from non-UI role=%r", role)
                return

            p = ev.get("payload") or ev.get("data") or {}
            action = (ev.get("action") or p.get("action") or "").lower().strip()
            req_id = ev.get("req_id") or self._new_req_id()

            self.log.debug(
                "Client command received | action=%r req_id=%s",
                action or "(none)",
                req_id,
            )

            if not action:
                self.log.debug("Ignoring client event without action.")
                return

            try:
                if action == "status":
                    self.log.debug("Status requested by UI | req_id=%s", req_id)
                    await self._act_status(req_id=req_id, data=p)
                    self.log.debug("Status response published | req_id=%s", req_id)
                    return

                if action == "backfill":
                    self.log.debug(
                        "Backfill request received | req_id=%s raw=%s",
                        req_id,
                        _safe_preview(p),
                    )

                    ui_raw = p.get("clone_channel_id")
                    if (
                        ui_raw is None
                        or str(ui_raw).strip() == ""
                        or str(ui_raw).lower() == "null"
                    ):
                        raise ValueError("channel_id missing")

                    any_id = int(ui_raw)

                    orig_id, clone_id, src = (
                        self.listener.db.resolve_original_from_any_id(any_id)
                    )
                    if orig_id is None:
                        raise ValueError(f"Could not resolve original id from {any_id}")

                    self.log.debug(
                        "Backfill id resolved | src=%s ui_id=%s → original_id=%s clone_id=%s",
                        src,
                        any_id,
                        orig_id,
                        clone_id,
                    )

                    def _coerce_int(x):
                        try:
                            n = int(x)
                            return n if n > 0 else None
                        except Exception:
                            return None

                    rng = p.get("range") or {}

                    after_iso = p.get("after_iso")
                    if after_iso is None or (
                        isinstance(after_iso, str) and after_iso.strip() == ""
                    ):
                        after_iso = rng.get("after")
                        if (
                            after_iso is None
                            or (isinstance(after_iso, str) and after_iso.strip() == "")
                        ) and (str(rng.get("mode") or "").lower() == "since"):
                            after_iso = rng.get("value")
                    if isinstance(after_iso, str) and after_iso.strip() == "":
                        after_iso = None

                    last_n = _coerce_int(p.get("last_n"))
                    if last_n is None:
                        if str(rng.get("mode") or "").lower() == "last":
                            last_n = _coerce_int(rng.get("last_n") or rng.get("value"))
                        else:
                            last_n = _coerce_int(rng.get("last_n"))

                    mode = "last_n" if last_n else ("since" if after_iso else "all")
                    self.log.debug(
                        "Backfill params | mode=%s after_iso=%r last_n=%r",
                        mode,
                        after_iso,
                        last_n,
                    )

                    self.log.debug("Scheduling backfill task | original_id=%s", orig_id)
                    asyncio.create_task(
                        self.listener._backfill_channel(
                            original_channel_id=orig_id,
                            after_iso=after_iso,
                            last_n=last_n,
                        ),
                        name=f"backfill:{orig_id}",
                    )
                    self.log.debug("Backfill task scheduled | original_id=%s", orig_id)

                    await self._publish(
                        {
                            "kind": "client",
                            "role": "client",
                            "payload": {
                                "type": "backfill_ack",
                                "ok": True,
                                "original_id": orig_id,
                                "clone_id": clone_id,
                                "after_iso": after_iso,
                                "last_n": last_n,
                            },
                        }
                    )
                    self.log.debug(
                        "ACK published to UI | original_id=%s clone_id=%s after_iso=%r last_n=%r",
                        orig_id,
                        clone_id,
                        after_iso,
                        last_n,
                    )
                    return

                self.log.debug(
                    "Unknown client action from UI | action=%r req_id=%s",
                    action,
                    req_id,
                )
                await self._publish(
                    {
                        "kind": "client",
                        "role": "client",
                        "payload": {
                            "type": "error",
                            "ok": False,
                            "message": f"unknown action: {action}",
                        },
                    }
                )

            except Exception as e:
                self.log.exception(
                    "Client command failed | req_id=%s action=%s", req_id, action
                )
                await self._publish(
                    {
                        "kind": "client",
                        "role": "client",
                        "payload": {
                            "type": "backfill_ack" if action == "backfill" else "error",
                            "ok": False,
                            "error": str(e),
                        },
                    }
                )
                self.log.debug(
                    "Error/NACK published | req_id=%s action=%s error=%s",
                    req_id,
                    action,
                    e,
                )

        try:
            self.log.debug(
                "Subscribing to admin bus | url=%s path=/bus", self.admin_base_url
            )
            await self.bus.subscribe(self.admin_base_url, _handler)
            self.log.debug("ClientUiController subscribed to bus.")
        except asyncio.CancelledError:
            self.log.debug("_listen_loop cancelled")
            raise
        except Exception as e:
            self.log.exception("Fatal error in _listen_loop subscribe | err=%s", e)
            await asyncio.sleep(0.5)

    def _pick_guild(self) -> Optional["discord.Guild"]:
        """Return the configured guild if available, otherwise the first guild (fallback)."""
        g = None
        if self.guild_id:
            g = self.bot.get_guild(self.guild_id)
        if not g and self.bot.guilds:
            g = self.bot.guilds[0]
        return g

    async def _act_status(self, *, req_id: str, data: Dict[str, Any]):
        guild = self._pick_guild()
        discord_ready = getattr(self.bot, "is_ready", lambda: False)()
        info = {
            "type": "status",
            "ok": True,
            "req_id": req_id,
            "server": {
                "version": CURRENT_VERSION,
                "status": "ready" if discord_ready and guild else "starting",
            },
            "discord": {
                "ready": discord_ready,
                "user_id": getattr(getattr(self.bot, "user", None), "id", None),
                "user": (
                    str(getattr(self.bot, "user", None))
                    if getattr(self.bot, "user", None)
                    else None
                ),
                "latency_s": float(getattr(self.bot, "latency", 0.0) or 0.0),
            },
            "guild": {
                "id": int(guild.id) if guild else None,
                "name": getattr(guild, "name", None),
            },
        }
        await self._publish(info)


def _safe_preview(obj: Any, limit: int = 400) -> str:
    try:
        s = str(obj)
        return s if len(s) <= limit else (s[:limit] + "…")
    except Exception:
        return "<unprintable>"


async def dm_member_by_id(bot, member_id: int, message: str) -> bool:
    """
    DM a member by their ID.
    """
    try:
        member = bot.get_user(member_id)
        if not member:
            member = await bot.fetch_user(member_id)

        if not member:
            return False

        await member.send(message)
        return True

    except discord.Forbidden:
        return False
    except discord.NotFound:
        return False
    except Exception as e:
        return False
