# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
import re
from typing import Dict, List, Optional
import discord
import aiohttp
from discord import Member

class MessageUtils:
    """
    Formatting & extraction helpers used by the client:
    - mention expansion
    - embed sanitizing
    - sticker payload shaping
    - safe attribute extraction
    """
    _MENTION_RE = re.compile(r"<@!?(\d+)>")

    def __init__(self, bot: discord.Client):
        self.bot = bot

    async def build_mention_map(self, message: discord.Message, embed_dicts: List[dict]) -> Dict[str, str]:
        ids: set[str] = set()

        def _collect(s: Optional[str]):
            if s:
                ids.update(self._MENTION_RE.findall(s))

        _collect(message.content)
        for e in embed_dicts:
            _collect(e.get("title"))
            _collect(e.get("description"))
            a = e.get("author") or {}
            _collect(a.get("name"))
            f = e.get("footer") or {}
            _collect(f.get("text"))
            for fld in e.get("fields") or []:
                _collect(fld.get("name"))
                _collect(fld.get("value"))

        if not ids:
            return {}

        g = message.guild
        id_to_name: Dict[str, str] = {}
        for sid in ids:
            uid = int(sid)
            mem = g.get_member(uid) if g else None
            if mem:
                id_to_name[sid] = f"@{mem.display_name or mem.name}"
                continue
            try:
                if g:
                    mem = await g.fetch_member(uid)
                    id_to_name[sid] = f"@{mem.display_name or mem.name}"
                    continue
            except Exception:
                pass
            try:
                u = await self.bot.fetch_user(uid)
                id_to_name[sid] = f"@{u.name}"
            except Exception:
                pass
        return id_to_name

    def humanize_user_mentions(
        self,
        content: str,
        message: discord.Message,
        id_to_name_override: Optional[Dict[str, str]] = None,
    ) -> str:
        if not content:
            return content

        id_to_name = dict(id_to_name_override or {})
        for m in getattr(message, "mentions", []):
            name = f"@{(m.display_name if isinstance(m, Member) else m.name) or m.name}"
            id_to_name[str(m.id)] = name

        def repl(match: re.Match) -> str:
            uid = match.group(1)
            if uid in id_to_name:
                return id_to_name[uid]
            g = message.guild
            mem = g.get_member(int(uid)) if g else None
            if mem:
                nm = f"@{mem.display_name or mem.name}"
                id_to_name[uid] = nm
                return nm
            return match.group(0)

        return self._MENTION_RE.sub(repl, content)

    def sanitize_inline(self, s: Optional[str], message: Optional[discord.Message] = None, id_map=None):
        if not s:
            return s
        if message and "{mention}" in s:
            s = s.replace("{mention}", f"@{message.author.display_name}")
        if message:
            s = self.humanize_user_mentions(s, message, id_map)
        return s

    def sanitize_embed_dict(
        self,
        d: dict,
        message: discord.Message,
        id_map: Optional[Dict[str, str]] = None,
    ) -> dict:
        e = dict(d)
        if "title" in e:
            e["title"] = self.sanitize_inline(e.get("title"), message, id_map)
        if "description" in e:
            e["description"] = self.sanitize_inline(e.get("description"), message, id_map)

        if isinstance(e.get("author"), dict) and "name" in e["author"]:
            e["author"] = dict(e["author"])
            e["author"]["name"] = self.sanitize_inline(e["author"].get("name"), message, id_map)

        if isinstance(e.get("footer"), dict) and "text" in e["footer"]:
            e["footer"] = dict(e["footer"])
            e["footer"]["text"] = self.sanitize_inline(e["footer"].get("text"), message, id_map)

        if isinstance(e.get("fields"), list):
            new_fields = []
            for f in e["fields"]:
                if not isinstance(f, dict):
                    new_fields.append(f)
                    continue
                f2 = dict(f)
                if "name" in f2:
                    f2["name"] = self.sanitize_inline(f2.get("name"), message, id_map)
                if "value" in f2:
                    f2["value"] = self.sanitize_inline(f2.get("value"), message, id_map)
                new_fields.append(f2)
            e["fields"] = new_fields

        return e

    def stickers_payload(self, stickers) -> list[dict]:
        def _enum_int(val, default=0):
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

        out = []
        for s in stickers or []:
            out.append(
                {
                    "id": s.id,
                    "name": s.name,
                    "format_type": _enum_int(getattr(s, "format", None), 0),
                    "url": _sticker_url(s),
                }
            )
        return out

    def extract_public_message_attrs(self, message: discord.Message) -> dict:
        attrs = {}
        for name in dir(message):
            if name.startswith("_"):
                continue
            try:
                value = getattr(message, name)
            except Exception:
                continue
            if callable(value):
                continue
            attrs[name] = value
        return attrs
    
    def serialize(self, message: discord.Message) -> dict:
        """Convert a Discord message into a serializable dict for dm export."""
        data = {
            "id": str(message.id),
            "timestamp": message.created_at.isoformat(),
            "author": {
                "id": str(message.author.id),
                "name": message.author.name,
                "discriminator": message.author.discriminator,
                "bot": message.author.bot,
                "avatar_url": str(message.author.avatar.url) if message.author.avatar else None,
            },
            "content": self.humanize_user_mentions(message.content, message),
            "type": str(message.type),
            "edited_timestamp": message.edited_at.isoformat() if message.edited_at else None,
        }

        if message.attachments:
            data["attachments"] = [
                {
                    "id": str(att.id),
                    "filename": att.filename,
                    "url": att.url,
                    "size": att.size,
                    "content_type": att.content_type,
                }
                for att in message.attachments
            ]

        if message.embeds:
            embed_dicts = [e.to_dict() for e in message.embeds]
            id_map = {}
            data["embeds"] = [
                self.sanitize_embed_dict(e, message, id_map) for e in embed_dicts
            ]

        if message.stickers:
            data["stickers"] = self.stickers_payload(message.stickers)

        return data
    

class Snapshot:
    """All snapshot-related helpers, shims, and the REST fallback."""

    @staticmethod
    def _is_http_url(u: str | None) -> bool:
        return isinstance(u, str) and (
            u.startswith("http://") or u.startswith("https://")
        )

    @staticmethod
    def _default_avatar_url_from_discriminator(discriminator: str | int | None) -> str:
        try:
            i = int(discriminator) % 5
        except Exception:
            i = 0
        return f"https://cdn.discordapp.com/embed/avatars/{i}.png"

    @staticmethod
    def _avatar_cdn_url(
        author_id: int, avatar_hash: str | None, discriminator: str | int | None
    ) -> str:
        if avatar_hash:
            ext = "gif" if str(avatar_hash).startswith("a_") else "png"
            return f"https://cdn.discordapp.com/avatars/{author_id}/{avatar_hash}.{ext}?size=128"
        return Snapshot._default_avatar_url_from_discriminator(discriminator)

    class Avatar:
        def __init__(self, url: str | None):
            self.url = url

    class Author:
        def __init__(self, d: dict):
            self.id = int(d.get("id", 0) or 0)
            self.name = d.get("username") or d.get("name") or "Unknown"
            self.discriminator = str(d.get("discriminator", "0"))
            self.bot = bool(d.get("bot", False))

            avatar_hash = d.get("avatar")
            cdn = Snapshot._avatar_cdn_url(self.id, avatar_hash, self.discriminator)
            self.avatar = Snapshot.Avatar(cdn)
            self.display_avatar = Snapshot.Avatar(cdn)

    class Attachment:
        def __init__(self, d: dict):
            self.id = int(d.get("id", 0) or 0)
            self.filename = d.get("filename") or ""
            self.url = d.get("url") or d.get("proxy_url") or ""
            self.proxy_url = d.get("proxy_url") or ""
            self.size = int(d.get("size", 0) or 0)
            self.content_type = d.get("content_type")

    class EmbedWrapper:
        def __init__(self, d: dict):
            self._d = dict(d or {})

        def to_dict(self):
            return dict(self._d)

    class Message:
        """
        Minimal message-like object built from a REST message_snapshot.
        Exposes attributes your pipeline already reads (content, attachments,
        embeds with .to_dict(), stickers, author, components, channel, guild, type).
        """

        __is_snapshot__ = True

        def __init__(self, d: dict, wrapper):
            from datetime import datetime, timezone

            self.id = int(d.get("id") or getattr(wrapper, "id", 0) or 0)

            ts = d.get("timestamp")
            try:
                self.created_at = (
                    datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(
                        timezone.utc
                    )
                    if ts
                    else getattr(wrapper, "created_at", None)
                )
            except Exception:
                self.created_at = getattr(wrapper, "created_at", None)
            self.edited_at = None

            inner_author = d.get("author")
            if inner_author:
                self.author = Snapshot.Author(inner_author)
            else:
                wa = getattr(wrapper, "author", None)
                if wa is not None:
                    borrowed = {
                        "id": getattr(wa, "id", 0) or 0,
                        "username": (
                            getattr(wa, "name", None)
                            or getattr(wa, "global_name", None)
                            or getattr(wa, "display_name", None)
                            or getattr(wa, "nick", None)
                            or getattr(wa, "username", None)
                            or "Unknown"
                        ),
                        "discriminator": getattr(wa, "discriminator", "0"),
                        "bot": bool(getattr(wa, "bot", False)),
                        "avatar": None,
                    }
                    shim_author = Snapshot.Author(borrowed)
                    try:
                        w_url = str(
                            getattr(getattr(wa, "display_avatar", None), "url", "")
                            or ""
                        )
                        if Snapshot._is_http_url(w_url):
                            shim_author.avatar = Snapshot.Avatar(w_url)
                            shim_author.display_avatar = Snapshot.Avatar(w_url)
                    except Exception:
                        pass
                    self.author = shim_author
                else:
                    self.author = Snapshot.Author({})

            self.content = d.get("content") or ""
            self.system_content = d.get("system_content") or ""
            self.attachments = [
                Snapshot.Attachment(a) for a in (d.get("attachments") or [])
            ]
            self.embeds = [Snapshot.EmbedWrapper(e) for e in (d.get("embeds") or [])]
            self.stickers = d.get("stickers") or []
            self.components = d.get("components") or []

            self.channel = getattr(wrapper, "channel", None)
            self.guild = getattr(wrapper, "guild", None)
            self.type = getattr(wrapper, "type", None)

        @staticmethod
        async def resolve_via_snapshot(
            bot, wrapper_msg, *, limit: int = 50, logger=None
        ):
            """
            Fetch recent messages in the wrapper's channel and unwrap the first usable
            `message_snapshots[].message` if reference chaining fails.
            """
            try:
                chan = getattr(wrapper_msg, "channel", None)
                chan_id = int(getattr(chan, "id", 0) or 0)
                msg_id = int(getattr(wrapper_msg, "id", 0) or 0)
                if not chan_id or not msg_id:
                    return None

                http = getattr(bot, "http", None)
                token = getattr(http, "token", None)
                if not token:
                    if logger:
                        logger.debug(
                            "[forward-snapshot] no token on bot.http; skipping fallback"
                        )
                    return None


                url = f"https://discord.com/api/v9/channels/{chan_id}/messages?limit={int(limit)}"
                headers = {"Authorization": token}

                async with aiohttp.ClientSession() as sess:
                    async with sess.get(url, headers=headers) as resp:
                        if resp.status != 200:
                            if logger:
                                logger.debug(
                                    "[forward-snapshot] GET %s -> %s", url, resp.status
                                )
                            return None
                        data = await resp.json()

                target = None
                for m in data or []:
                    try:
                        if int(m.get("id", 0)) == msg_id:
                            target = m
                            break
                    except Exception:
                        continue
                if not target:
                    return None

                snaps = target.get("message_snapshots") or []
                for s in snaps:
                    inner = (s or {}).get("message") or {}
                    has_text = bool(
                        (
                            inner.get("content") or inner.get("system_content") or ""
                        ).strip()
                    )
                    has_atts = bool(inner.get("attachments"))
                    has_stickers = bool(inner.get("stickers"))
                    if has_text or has_atts or has_stickers:
                        return Snapshot.Message(inner, wrapper_msg)

                return None
            except Exception as e:
                if logger:
                    logger.debug("[forward-snapshot] error: %r", e)
                return None


async def _resolve_forward(bot, wrapper_msg, max_depth: int = 4):
    """
    Follow .reference / forward wrappers to find the original message that actually
    has real content we can forward. We ignore "embeds-only" shells.
    """
    current = wrapper_msg
    seen = 0

    while seen < max_depth and current is not None:
        raw_txt = (getattr(current, "content", "") or "").strip()
        sys_txt = (getattr(current, "system_content", "") or "").strip()
        has_text = bool(raw_txt or sys_txt)
        has_atts = bool(getattr(current, "attachments", None))
        has_stks = bool(getattr(current, "stickers", None))

        if has_text or has_atts or has_stks:
            return current

        ref = getattr(current, "reference", None)
        if not ref:
            break

        ch = None
        try:
            ch = bot.get_channel(int(ref.channel_id))
        except Exception:
            ch = None

        if ch is None:
            try:
                ch = await bot.fetch_channel(int(ref.channel_id))
            except Exception:
                ch = None
        if ch is None:
            break

        next_msg = None
        try:
            next_msg = await ch.fetch_message(int(ref.message_id))
        except Exception:
            next_msg = None

        current = next_msg
        seen += 1

    if current is not None:
        raw_txt = (getattr(current, "content", "") or "").strip()
        sys_txt = (getattr(current, "system_content", "") or "").strip()
        if (
            raw_txt
            or sys_txt
            or getattr(current, "attachments", None)
            or getattr(current, "stickers", None)
        ):
            return current

    return None


async def _resolve_forward_via_snapshot(
    bot, wrapper_msg, *, limit: int = 50, logger=None
):
    return await Snapshot.Message.resolve_via_snapshot(
        bot, wrapper_msg, limit=limit, logger=logger
    )
