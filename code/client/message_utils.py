# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations
import re
from typing import Dict, List, Optional
import discord
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
            # 1) guild cache
            mem = g.get_member(uid) if g else None
            if mem:
                id_to_name[sid] = f"@{mem.display_name or mem.name}"
                continue
            # 2) guild fetch
            try:
                if g:
                    mem = await g.fetch_member(uid)
                    id_to_name[sid] = f"@{mem.display_name or mem.name}"
                    continue
            except Exception:
                pass
            # 3) global user
            try:
                u = await self.bot.fetch_user(uid)
                id_to_name[sid] = f"@{u.name}"
            except Exception:
                # leave unresolved; original token will remain
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
        # seed from message.mentions (faster path)
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
        # Replace {mention} with actual mention text
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

        # author
        if isinstance(e.get("author"), dict) and "name" in e["author"]:
            e["author"] = dict(e["author"])
            e["author"]["name"] = self.sanitize_inline(e["author"].get("name"), message, id_map)

        # footer
        if isinstance(e.get("footer"), dict) and "text" in e["footer"]:
            e["footer"] = dict(e["footer"])
            e["footer"]["text"] = self.sanitize_inline(e["footer"].get("text"), message, id_map)

        # fields
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