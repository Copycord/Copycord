# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Attribute-compatible wrappers over raw gateway/REST dicts.

src/copycord/client reads a few hundred attributes off discord.py-self model objects
(``g.name``, ``role.color.value``, ``ch.type.value``, ``msg.author.display_avatar.url``,
``th.permissions_for(me).read_message_history`` ...). Every wrapper here keeps
the raw payload in ``_data`` and exposes those same names as properties, so the
client can switch imports without its serialization code changing.

Lookups that need state outside the payload (the guild a message belongs to,
the channel a thread hangs off, the logged-in user) go through an optional
``state`` object. Anything with ``get_guild``/``get_channel``/``get_user`` and a
``user`` attribute works; the real one is the Client that lands with
v4/selfclient-gateway. With no state the wrappers still read their own payload.

No ``discord`` import anywhere in this module.
"""

from __future__ import annotations

import copy
import datetime as _dt
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from .enums import (
    FORUM_LIKE_TYPES,
    NON_SYSTEM_MESSAGE_TYPES,
    TEXT_LIKE_TYPES,
    THREAD_TYPES,
    VOICE_LIKE_TYPES,
    ChannelType,
    MessageType,
    OverwriteType,
    StickerFormatType,
    StickerType,
    VideoQualityMode,
    try_enum,
)

DISCORD_EPOCH = 1420070400000
CDN = "https://cdn.discordapp.com"
MEDIA = "https://media.discordapp.net"
WEB = "https://discord.com"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _int(value: Any) -> Optional[int]:
    """Snowflakes arrive as strings; None and junk stay None."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def snowflake_time(snowflake: int) -> _dt.datetime:
    """The creation time encoded in a snowflake, as an aware UTC datetime."""
    ms = (int(snowflake) >> 22) + DISCORD_EPOCH
    return _dt.datetime.fromtimestamp(ms / 1000, tz=_dt.timezone.utc)


def time_snowflake(when: _dt.datetime, *, high: bool = False) -> int:
    """The smallest (or largest, with ``high``) snowflake minted at ``when``."""
    ms = int(when.timestamp() * 1000 - DISCORD_EPOCH)
    return (ms << 22) + (2**22 - 1 if high else 0)


def parse_time(value: Optional[str]) -> Optional[_dt.datetime]:
    """An ISO8601 timestamp from a payload, as an aware datetime."""
    if not value:
        return None
    try:
        parsed = _dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return parsed


def _state_call(state: Any, name: str, *args: Any) -> Any:
    fn = getattr(state, name, None) if state is not None else None
    if not callable(fn):
        return None
    try:
        return fn(*args)
    except Exception:
        return None


def _sorted_by_position(items: Sequence[Any]) -> List[Any]:
    return sorted(items, key=lambda c: (getattr(c, "position", 0) or 0, c.id))


class _Hashable:
    """Equality and hashing by id, like discord.py's Hashable mixin."""

    __slots__ = ()
    id: int

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and other.id == self.id

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return self.id >> 22

    @property
    def created_at(self) -> _dt.datetime:
        return snowflake_time(self.id)


class _Wrapper(_Hashable):
    """A raw payload dict plus the id it is keyed on."""

    __slots__ = ("_data", "_state", "id")

    def __init__(self, data: Dict[str, Any], state: Any = None):
        self._data: Dict[str, Any] = data or {}
        self._state = state
        self.id: int = _int(self._data.get("id")) or 0

    @property
    def raw(self) -> Dict[str, Any]:
        return self._data

    def __repr__(self) -> str:
        name = self._data.get("name") or self._data.get("username")
        extra = f" name={name!r}" if name else ""
        return f"<{self.__class__.__name__} id={self.id}{extra}>"


# ---------------------------------------------------------------------------
# small value types
# ---------------------------------------------------------------------------


class Object(_Hashable):
    """An id with nothing else known about it (discord.Object)."""

    __slots__ = ("id", "type")

    def __init__(self, id: Union[int, str], *, type: Any = None):  # noqa: A002
        self.id = int(id)
        self.type = type

    def __eq__(self, other: object) -> bool:
        return getattr(other, "id", None) == self.id

    def __hash__(self) -> int:
        return self.id >> 22

    def __repr__(self) -> str:
        return f"<Object id={self.id}>"


class Asset:
    """A CDN asset. ``str(asset)`` is its URL, ``asset.key`` the hash."""

    __slots__ = ("_url", "_key", "_animated")

    def __init__(self, url: str, *, key: str, animated: bool = False):
        self._url = url
        self._key = key
        self._animated = animated

    @property
    def url(self) -> str:
        return self._url

    @property
    def key(self) -> str:
        return self._key

    def is_animated(self) -> bool:
        return self._animated

    def __str__(self) -> str:
        return self._url

    def __len__(self) -> int:
        return len(self._url)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Asset) and other._url == self._url

    def __hash__(self) -> int:
        return hash(self._url)

    def __repr__(self) -> str:
        return f"<Asset url={self._url!r}>"

    @staticmethod
    def _fmt(key: str) -> Tuple[str, bool]:
        animated = key.startswith("a_")
        return ("gif" if animated else "png"), animated

    @classmethod
    def from_avatar(cls, user_id: int, key: str) -> "Asset":
        fmt, animated = cls._fmt(key)
        return cls(
            f"{CDN}/avatars/{user_id}/{key}.{fmt}?size=1024", key=key, animated=animated
        )

    @classmethod
    def from_guild_avatar(cls, guild_id: int, user_id: int, key: str) -> "Asset":
        fmt, animated = cls._fmt(key)
        return cls(
            f"{CDN}/guilds/{guild_id}/users/{user_id}/avatars/{key}.{fmt}?size=1024",
            key=key,
            animated=animated,
        )

    @classmethod
    def from_default_avatar(cls, index: int) -> "Asset":
        return cls(f"{CDN}/embed/avatars/{index}.png", key=str(index))

    @classmethod
    def from_guild_image(cls, guild_id: int, key: str, path: str) -> "Asset":
        """icons / banners / splashes / discovery-splashes."""
        if path == "icons":
            fmt, animated = cls._fmt(key)
            size = 1024
        elif path == "banners":
            fmt, animated = cls._fmt(key)
            size = 512
        else:
            fmt, animated, size = "png", False, 2048
        return cls(
            f"{CDN}/{path}/{guild_id}/{key}.{fmt}?size={size}",
            key=key,
            animated=animated,
        )

    @classmethod
    def from_role_icon(cls, role_id: int, key: str) -> "Asset":
        return cls(f"{CDN}/role-icons/{role_id}/{key}.png?size=1024", key=key)

    @classmethod
    def from_emoji(cls, emoji_id: int, animated: bool) -> "Asset":
        fmt = "gif" if animated else "png"
        return cls(f"{CDN}/emojis/{emoji_id}.{fmt}", key=str(emoji_id), animated=animated)


class _Flags:
    """An int bitfield with named boolean attributes."""

    FLAGS: Dict[str, int] = {}
    __slots__ = ("value",)

    def __init__(self, value: Any = 0, **kwargs: bool):
        self.value: int = int(value or 0)
        for name, on in kwargs.items():
            setattr(self, name, on)

    def __getattr__(self, name: str) -> bool:
        flags = type(self).FLAGS
        if name in flags:
            return bool(self.value & (1 << flags[name]))
        raise AttributeError(f"{type(self).__name__} has no flag {name!r}")

    def __setattr__(self, name: str, on: Any) -> None:
        if name == "value":
            object.__setattr__(self, name, on)
            return
        flags = type(self).FLAGS
        if name not in flags:
            raise AttributeError(f"{type(self).__name__} has no flag {name!r}")
        bit = 1 << flags[name]
        object.__setattr__(self, "value", (self.value | bit) if on else (self.value & ~bit))

    def __int__(self) -> int:
        return self.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and other.value == self.value

    def __hash__(self) -> int:
        return hash((type(self).__name__, self.value))

    def __iter__(self) -> Iterator[Tuple[str, bool]]:
        for name in type(self).FLAGS:
            yield name, getattr(self, name)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} value={self.value}>"


# Bit positions from Discord's permission table.
_PERMISSION_BITS: Dict[str, int] = {
    "create_instant_invite": 0,
    "kick_members": 1,
    "ban_members": 2,
    "administrator": 3,
    "manage_channels": 4,
    "manage_guild": 5,
    "add_reactions": 6,
    "view_audit_log": 7,
    "priority_speaker": 8,
    "stream": 9,
    "view_channel": 10,
    "read_messages": 10,
    "send_messages": 11,
    "send_tts_messages": 12,
    "manage_messages": 13,
    "embed_links": 14,
    "attach_files": 15,
    "read_message_history": 16,
    "mention_everyone": 17,
    "use_external_emojis": 18,
    "external_emojis": 18,
    "view_guild_insights": 19,
    "connect": 20,
    "speak": 21,
    "mute_members": 22,
    "deafen_members": 23,
    "move_members": 24,
    "use_voice_activation": 25,
    "change_nickname": 26,
    "manage_nicknames": 27,
    "manage_roles": 28,
    "manage_permissions": 28,
    "manage_webhooks": 29,
    "manage_emojis": 30,
    "manage_expressions": 30,
    "manage_emojis_and_stickers": 30,
    "use_application_commands": 31,
    "request_to_speak": 32,
    "manage_events": 33,
    "manage_threads": 34,
    "create_public_threads": 35,
    "create_private_threads": 36,
    "use_external_stickers": 37,
    "external_stickers": 37,
    "send_messages_in_threads": 38,
    "use_embedded_activities": 39,
    "moderate_members": 40,
    "view_creator_monetization_analytics": 41,
    "use_soundboard": 42,
    "create_expressions": 43,
    "create_events": 44,
    "use_external_sounds": 45,
    "send_voice_messages": 46,
    "send_polls": 49,
    "use_external_apps": 50,
}

# Permissions that only mean something at guild level; everything else is
# channel-scoped and is what a hidden channel strips.
_GUILD_ONLY_BITS = frozenset(
    {"kick_members", "ban_members", "administrator", "manage_guild", "view_audit_log",
     "view_guild_insights", "change_nickname", "manage_nicknames", "manage_emojis",
     "moderate_members", "view_creator_monetization_analytics", "create_expressions"}
)
_VOICE_BITS = frozenset(
    {"priority_speaker", "stream", "connect", "speak", "mute_members", "deafen_members",
     "move_members", "use_voice_activation", "request_to_speak",
     "use_embedded_activities", "use_soundboard", "use_external_sounds"}
)


class Permissions(_Flags):
    FLAGS = _PERMISSION_BITS
    __slots__ = ()

    @classmethod
    def none(cls) -> "Permissions":
        return cls(0)

    @classmethod
    def all(cls) -> "Permissions":
        value = 0
        for bit in _PERMISSION_BITS.values():
            value |= 1 << bit
        return cls(value)

    @classmethod
    def _of(cls, names: Any) -> "Permissions":
        value = 0
        for name in names:
            value |= 1 << _PERMISSION_BITS[name]
        return cls(value)

    @classmethod
    def all_channel(cls) -> "Permissions":
        return cls._of(n for n in _PERMISSION_BITS if n not in _GUILD_ONLY_BITS)

    @classmethod
    def voice(cls) -> "Permissions":
        return cls._of(_VOICE_BITS)

    def handle_overwrite(self, allow: int, deny: int) -> None:
        """Apply one overwrite: clear the denied bits, then set the allowed ones."""
        self.value = (self.value & ~int(deny)) | int(allow)

    def __or__(self, other: "Permissions") -> "Permissions":
        return Permissions(self.value | int(other))

    def __and__(self, other: "Permissions") -> "Permissions":
        return Permissions(self.value & int(other))

    def is_subset(self, other: "Permissions") -> bool:
        return (self.value & int(other)) == self.value

    def is_superset(self, other: "Permissions") -> bool:
        return (self.value | int(other)) == self.value


class MessageFlags(_Flags):
    FLAGS = {
        "crossposted": 0,
        "is_crossposted": 1,
        "suppress_embeds": 2,
        "source_message_deleted": 3,
        "urgent": 4,
        "has_thread": 5,
        "ephemeral": 6,
        "loading": 7,
        "failed_to_mention_some_roles_in_thread": 8,
        "suppress_notifications": 12,
        "voice": 13,
        "forwarded": 14,
        "has_snapshot": 14,
    }
    __slots__ = ()


class ChannelFlags(_Flags):
    FLAGS = {"pinned": 1, "require_tag": 4, "hide_media_download_options": 15}
    __slots__ = ()


class SystemChannelFlags(_Flags):
    # Discord's bits are "suppress" bits; discord.py inverts them for its
    # attribute names, but the client only ever reads .value.
    FLAGS = {
        "suppress_join_notifications": 0,
        "suppress_premium_subscriptions": 1,
        "suppress_guild_reminder_notifications": 2,
        "suppress_join_notification_replies": 3,
    }
    __slots__ = ()


class Colour:
    __slots__ = ("value",)

    def __init__(self, value: Any = 0):
        self.value: int = int(value or 0)

    def __int__(self) -> int:
        return self.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Colour) and other.value == self.value

    def __hash__(self) -> int:
        return hash(self.value)

    @property
    def r(self) -> int:
        return (self.value >> 16) & 0xFF

    @property
    def g(self) -> int:
        return (self.value >> 8) & 0xFF

    @property
    def b(self) -> int:
        return self.value & 0xFF

    def __str__(self) -> str:
        return f"#{self.value:0>6x}"

    def __repr__(self) -> str:
        return f"<Colour value={self.value}>"


Color = Colour


# ---------------------------------------------------------------------------
# users, members, roles
# ---------------------------------------------------------------------------


class User(_Wrapper):
    """A Discord user from any payload carrying a user object."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return self._data.get("username") or ""

    @property
    def global_name(self) -> Optional[str]:
        return self._data.get("global_name")

    @property
    def discriminator(self) -> str:
        return str(self._data.get("discriminator") or "0")

    @property
    def bot(self) -> bool:
        return bool(self._data.get("bot", False))

    @property
    def system(self) -> bool:
        return bool(self._data.get("system", False))

    @property
    def public_flags(self) -> int:
        return int(self._data.get("public_flags") or 0)

    @property
    def avatar(self) -> Optional[Asset]:
        key = self._data.get("avatar")
        return Asset.from_avatar(self.id, key) if key else None

    @property
    def default_avatar(self) -> Asset:
        if self.discriminator in ("0", "0000"):
            index = (self.id >> 22) % 6
        else:
            index = int(self.discriminator) % 5
        return Asset.from_default_avatar(index)

    @property
    def display_avatar(self) -> Asset:
        return self.avatar or self.default_avatar

    @property
    def banner(self) -> Optional[Asset]:
        key = self._data.get("banner")
        if not key:
            return None
        fmt, animated = Asset._fmt(key)
        return Asset(f"{CDN}/banners/{self.id}/{key}.{fmt}?size=512", key=key, animated=animated)

    @property
    def display_name(self) -> str:
        return self.global_name or self.name

    @property
    def mention(self) -> str:
        return f"<@{self.id}>"

    @property
    def dm_channel(self) -> Optional["DMChannel"]:
        return _state_call(self._state, "_get_private_channel_by_user", self.id)

    def __str__(self) -> str:
        if self.discriminator in ("0", "0000"):
            return self.name
        return f"{self.name}#{self.discriminator}"


ClientUser = User


class Member(User):
    """A guild member: a user plus the guild-scoped member object.

    ``data`` is the member payload with its ``user`` object inside it, which is
    what GUILD_MEMBER_ADD and REST return. MESSAGE_CREATE sends ``author`` and
    ``member`` separately; ``Member.from_message`` joins them.
    """

    __slots__ = ("_member", "guild")

    def __init__(self, data: Dict[str, Any], guild: "Guild", state: Any = None):
        self._member: Dict[str, Any] = data or {}
        super().__init__(self._member.get("user") or {}, state)
        self.guild: Guild = guild

    @classmethod
    def from_message(
        cls, author: Dict[str, Any], member: Dict[str, Any], guild: "Guild", state: Any = None
    ) -> "Member":
        return cls({**(member or {}), "user": author}, guild, state)

    @property
    def raw(self) -> Dict[str, Any]:
        return self._member

    @property
    def nick(self) -> Optional[str]:
        return self._member.get("nick")

    @property
    def display_name(self) -> str:
        return self.nick or self.global_name or self.name

    @property
    def guild_avatar(self) -> Optional[Asset]:
        key = self._member.get("avatar")
        if not key or self.guild is None:
            return None
        return Asset.from_guild_avatar(self.guild.id, self.id, key)

    @property
    def display_avatar(self) -> Asset:
        return self.guild_avatar or super().display_avatar

    @property
    def joined_at(self) -> Optional[_dt.datetime]:
        return parse_time(self._member.get("joined_at"))

    @property
    def premium_since(self) -> Optional[_dt.datetime]:
        return parse_time(self._member.get("premium_since"))

    @property
    def pending(self) -> bool:
        return bool(self._member.get("pending", False))

    @property
    def timed_out_until(self) -> Optional[_dt.datetime]:
        return parse_time(self._member.get("communication_disabled_until"))

    def is_timed_out(self) -> bool:
        until = self.timed_out_until
        return bool(until and until > _dt.datetime.now(_dt.timezone.utc))

    @property
    def status(self) -> str:
        return str(self._member.get("status") or "offline")

    @property
    def _roles(self) -> List[int]:
        return [r for r in (_int(x) for x in self._member.get("roles") or []) if r]

    @property
    def roles(self) -> List["Role"]:
        """@everyone first, then the member's roles lowest to highest."""
        if self.guild is None:
            return []
        out: List[Role] = []
        for rid in self._roles:
            role = self.guild.get_role(rid)
            if role is not None:
                out.append(role)
        out.sort()
        default = self.guild.default_role
        if default is not None:
            out.insert(0, default)
        return out

    @property
    def top_role(self) -> Optional["Role"]:
        roles = self.roles
        return roles[-1] if roles else None

    @property
    def guild_permissions(self) -> Permissions:
        if self.guild is None:
            return Permissions.none()
        if self.guild.owner_id == self.id:
            return Permissions.all()
        base = Permissions.none()
        for role in self.roles:
            base.value |= role.permissions.value
        if base.administrator:
            return Permissions.all()
        return base

    def __repr__(self) -> str:
        return f"<Member id={self.id} name={self.name!r} guild={getattr(self.guild, 'id', None)}>"


class Role(_Wrapper):
    __slots__ = ("guild",)

    def __init__(self, data: Dict[str, Any], guild: "Guild", state: Any = None):
        super().__init__(data, state)
        self.guild: Guild = guild

    @property
    def name(self) -> str:
        return self._data.get("name") or ""

    @property
    def colour(self) -> Colour:
        return Colour(self._data.get("color") or 0)

    color = colour

    @property
    def colors(self) -> Optional[Dict[str, Any]]:
        """The raw gradient/holographic colors object, if Discord sent one."""
        cols = self._data.get("colors")
        return cols if isinstance(cols, dict) else None

    @property
    def hoist(self) -> bool:
        return bool(self._data.get("hoist", False))

    @property
    def mentionable(self) -> bool:
        return bool(self._data.get("mentionable", False))

    @property
    def managed(self) -> bool:
        return bool(self._data.get("managed", False))

    @property
    def position(self) -> int:
        return int(self._data.get("position") or 0)

    @property
    def permissions(self) -> Permissions:
        return Permissions(_int(self._data.get("permissions")) or 0)

    @property
    def icon(self) -> Optional[Asset]:
        key = self._data.get("icon")
        return Asset.from_role_icon(self.id, key) if key else None

    @property
    def unicode_emoji(self) -> Optional[str]:
        return self._data.get("unicode_emoji")

    @property
    def tags(self) -> Optional[Dict[str, Any]]:
        return self._data.get("tags")

    @property
    def flags(self) -> int:
        return int(self._data.get("flags") or 0)

    @property
    def mention(self) -> str:
        return f"<@&{self.id}>"

    def is_default(self) -> bool:
        return self.guild is not None and self.id == self.guild.id

    def __lt__(self, other: "Role") -> bool:
        if not isinstance(other, Role):
            return NotImplemented
        return (self.position, self.id) < (other.position, other.id)

    def __le__(self, other: "Role") -> bool:
        return self == other or self < other

    def __gt__(self, other: "Role") -> bool:
        if not isinstance(other, Role):
            return NotImplemented
        return other < self

    def __ge__(self, other: "Role") -> bool:
        return self == other or self > other

    __hash__ = _Wrapper.__hash__

    def __str__(self) -> str:
        return self.name


# ---------------------------------------------------------------------------
# emoji, stickers
# ---------------------------------------------------------------------------


class PartialEmoji:
    """An emoji known only by id/name (reactions, forum tags, default reaction)."""

    __slots__ = ("id", "name", "animated")

    def __init__(self, *, name: Optional[str], id: Optional[int] = None, animated: bool = False):  # noqa: A002
        self.id: Optional[int] = _int(id)
        self.name: Optional[str] = name
        self.animated: bool = bool(animated)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> Optional["PartialEmoji"]:
        """From a reaction ``emoji`` object or the ``emoji_id``/``emoji_name`` pair
        forum tags and default reactions use. None if both are empty."""
        if not data:
            return None
        eid = data.get("id", data.get("emoji_id"))
        name = data.get("name", data.get("emoji_name"))
        if not eid and not name:
            return None
        return cls(name=name, id=eid, animated=bool(data.get("animated", False)))

    def is_custom_emoji(self) -> bool:
        return self.id is not None

    def is_unicode_emoji(self) -> bool:
        return self.id is None

    @property
    def url(self) -> str:
        if self.id is None:
            return ""
        return Asset.from_emoji(self.id, self.animated).url

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"name": self.name}
        if self.id is not None:
            out["id"] = self.id
        if self.animated:
            out["animated"] = True
        return out

    def __eq__(self, other: object) -> bool:
        if self.is_unicode_emoji():
            return isinstance(other, PartialEmoji) and other.name == self.name
        return getattr(other, "id", None) == self.id

    def __hash__(self) -> int:
        return hash((self.id, self.name))

    def __str__(self) -> str:
        if self.id is None:
            return self.name or ""
        prefix = "a" if self.animated else ""
        return f"<{prefix}:{self.name}:{self.id}>"

    def __repr__(self) -> str:
        return f"<PartialEmoji id={self.id} name={self.name!r} animated={self.animated}>"


class Emoji(_Wrapper):
    """A guild custom emoji."""

    __slots__ = ("guild",)

    def __init__(self, data: Dict[str, Any], guild: Optional["Guild"] = None, state: Any = None):
        super().__init__(data, state)
        self.guild = guild

    @property
    def name(self) -> str:
        return self._data.get("name") or ""

    @property
    def animated(self) -> bool:
        return bool(self._data.get("animated", False))

    @property
    def managed(self) -> bool:
        return bool(self._data.get("managed", False))

    @property
    def available(self) -> bool:
        return bool(self._data.get("available", True))

    @property
    def require_colons(self) -> bool:
        return bool(self._data.get("require_colons", True))

    @property
    def roles(self) -> List[int]:
        return [r for r in (_int(x) for x in self._data.get("roles") or []) if r]

    @property
    def guild_id(self) -> Optional[int]:
        return self.guild.id if self.guild is not None else _int(self._data.get("guild_id"))

    @property
    def url(self) -> str:
        return Asset.from_emoji(self.id, self.animated).url

    def __str__(self) -> str:
        prefix = "a" if self.animated else ""
        return f"<{prefix}:{self.name}:{self.id}>"


class Sticker(_Wrapper):
    """A guild sticker, a standard sticker, or a message's sticker item."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return self._data.get("name") or ""

    @property
    def format(self) -> Any:
        return try_enum(StickerFormatType, self._data.get("format_type", 1))

    format_type = format

    @property
    def type(self) -> Any:
        raw = self._data.get("type")
        if raw is None:
            return StickerType.guild if self._data.get("guild_id") else None
        return try_enum(StickerType, raw)

    @property
    def tags(self) -> str:
        return self._data.get("tags") or ""

    @property
    def description(self) -> str:
        return self._data.get("description") or ""

    @property
    def available(self) -> bool:
        return bool(self._data.get("available", True))

    @property
    def guild_id(self) -> Optional[int]:
        return _int(self._data.get("guild_id"))

    @property
    def url(self) -> str:
        fmt = _int(self._data.get("format_type")) or 1
        if fmt == StickerFormatType.gif:
            return f"{MEDIA}/stickers/{self.id}.gif"
        ext = "json" if fmt == StickerFormatType.lottie else "png"
        return f"{CDN}/stickers/{self.id}.{ext}"


StickerItem = Sticker
GuildSticker = Sticker


# ---------------------------------------------------------------------------
# permission overwrites
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PermissionOverwrite:
    """One raw channel overwrite: target id, target type, allow/deny bits.

    ``type`` stays the raw int (0 role, 1 member), which is what
    sitemap._serialize_role_overwrites compares against.
    """

    id: int
    type: int
    allow: int
    deny: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PermissionOverwrite":
        raw_type = data.get("type", 0)
        if isinstance(raw_type, str):
            raw_type = 1 if raw_type.lower() == "member" else 0
        return cls(
            id=_int(data.get("id")) or 0,
            type=int(raw_type or 0),
            allow=_int(data.get("allow")) or 0,
            deny=_int(data.get("deny")) or 0,
        )

    def is_role(self) -> bool:
        return self.type == OverwriteType.role

    def is_member(self) -> bool:
        return self.type == OverwriteType.member

    def pair(self) -> Tuple[Permissions, Permissions]:
        return Permissions(self.allow), Permissions(self.deny)


# ---------------------------------------------------------------------------
# channels
# ---------------------------------------------------------------------------


class _Channel(_Wrapper):
    __slots__ = ()

    @property
    def name(self) -> str:
        return self._data.get("name") or ""

    @property
    def type(self) -> Any:
        return try_enum(ChannelType, self._data.get("type", 0))

    @property
    def mention(self) -> str:
        return f"<#{self.id}>"

    def __str__(self) -> str:
        return self.name


class PartialMessageable(_Hashable):
    """A channel known only by id (discord.PartialMessageable)."""

    __slots__ = ("id", "guild_id", "type")

    def __init__(self, id: int, *, guild_id: Optional[int] = None, type: Any = None):  # noqa: A002
        self.id = int(id)
        self.guild_id = guild_id
        self.type = type

    @property
    def guild(self) -> None:
        return None

    @property
    def jump_url(self) -> str:
        return f"{WEB}/channels/{self.guild_id or '@me'}/{self.id}"

    def __repr__(self) -> str:
        return f"<PartialMessageable id={self.id}>"


class GuildChannel(_Channel):
    """Shared surface of every guild channel, including permission math."""

    __slots__ = ("guild",)

    def __init__(self, data: Dict[str, Any], guild: Optional["Guild"], state: Any = None):
        super().__init__(data, state)
        self.guild = guild

    @property
    def guild_id(self) -> Optional[int]:
        return self.guild.id if self.guild is not None else _int(self._data.get("guild_id"))

    @property
    def position(self) -> int:
        return int(self._data.get("position") or 0)

    @property
    def category_id(self) -> Optional[int]:
        return _int(self._data.get("parent_id"))

    @property
    def category(self) -> Optional["CategoryChannel"]:
        cid = self.category_id
        if cid is None or self.guild is None:
            return None
        found = self.guild.get_channel(cid)
        return found if isinstance(found, CategoryChannel) else None

    @property
    def nsfw(self) -> bool:
        return bool(self._data.get("nsfw", False))

    def is_nsfw(self) -> bool:
        return self.nsfw

    @property
    def flags(self) -> ChannelFlags:
        return ChannelFlags(self._data.get("flags") or 0)

    @property
    def jump_url(self) -> str:
        return f"{WEB}/channels/{self.guild_id or '@me'}/{self.id}"

    @property
    def permission_overwrites(self) -> List[PermissionOverwrite]:
        return [
            PermissionOverwrite.from_dict(o)
            for o in self._data.get("permission_overwrites") or []
            if isinstance(o, dict)
        ]

    _overwrites = permission_overwrites

    @property
    def overwrites(self) -> Dict[Any, PermissionOverwrite]:
        """Target (Role, Member, or Object when not cached) -> overwrite."""
        out: Dict[Any, PermissionOverwrite] = {}
        for ow in self.permission_overwrites:
            target: Any = None
            if self.guild is not None:
                target = (
                    self.guild.get_role(ow.id) if ow.is_role() else self.guild.get_member(ow.id)
                )
            out[target if target is not None else Object(ow.id)] = ow
        return out

    def overwrites_for(self, obj: Any) -> Optional[PermissionOverwrite]:
        oid = getattr(obj, "id", obj)
        for ow in self.permission_overwrites:
            if ow.id == oid:
                return ow
        return None

    def permissions_for(self, obj: Any) -> Permissions:
        """Effective permissions of a member or role in this channel.

        discord.py's GuildChannel.permissions_for order:
          1. guild owner gets everything;
          2. @everyone's role permissions OR every role the member has;
          3. administrator short-circuits to everything;
          4. @everyone channel overwrite;
          5. all role overwrites for the member's roles, denies then allows,
             combined so no single role order wins;
          6. the member-specific overwrite;
          7. implicit removals: timed-out members keep only view/read history,
             no send_messages strips the send-dependent bits, no view_channel
             strips every channel permission, no connect in voice strips voice.
        """
        guild = self.guild
        if guild is None:
            return Permissions.none()

        oid = getattr(obj, "id", None)
        if oid is not None and guild.owner_id == oid:
            return Permissions.all()

        default = guild.default_role
        base = Permissions(default.permissions.value if default is not None else 0)
        overwrites = self.permission_overwrites

        if isinstance(obj, Role):
            role_ids = [] if obj.id == guild.id else [obj.id]
            member_id = None
        else:
            role_ids = list(getattr(obj, "_roles", None) or [])
            member_id = oid

        for rid in role_ids:
            role = guild.get_role(rid)
            if role is not None:
                base.value |= role.permissions.value

        if base.administrator:
            return Permissions.all()

        for ow in overwrites:
            if ow.is_role() and ow.id == guild.id:
                base.handle_overwrite(allow=ow.allow, deny=ow.deny)
                break

        allows = denies = 0
        wanted = set(role_ids)
        for ow in overwrites:
            if ow.is_role() and ow.id in wanted and ow.id != guild.id:
                allows |= ow.allow
                denies |= ow.deny
        base.handle_overwrite(allow=allows, deny=denies)

        if member_id is not None:
            for ow in overwrites:
                if ow.is_member() and ow.id == member_id:
                    base.handle_overwrite(allow=ow.allow, deny=ow.deny)
                    break

        if isinstance(obj, Member) and obj.is_timed_out():
            base.value &= Permissions._of(("view_channel", "read_message_history")).value

        if not base.send_messages:
            base.send_tts_messages = False
            base.mention_everyone = False
            base.embed_links = False
            base.attach_files = False

        if not base.view_channel:
            base.value &= ~Permissions.all_channel().value

        ctype = self._data.get("type")
        if ctype in VOICE_LIKE_TYPES:
            if not base.connect:
                denied = Permissions.voice()
                denied.manage_channels = True
                denied.manage_roles = True
                base.value &= ~denied.value
        elif ctype in TEXT_LIKE_TYPES or ctype in FORUM_LIKE_TYPES:
            # Text channels carry no voice permissions (discord.py
            # TextChannel/ForumChannel.permissions_for).
            base.value &= ~Permissions.voice().value

        return base


class TextChannel(GuildChannel):
    __slots__ = ()

    @property
    def topic(self) -> Optional[str]:
        return self._data.get("topic")

    @property
    def slowmode_delay(self) -> int:
        return int(self._data.get("rate_limit_per_user") or 0)

    @property
    def last_message_id(self) -> Optional[int]:
        return _int(self._data.get("last_message_id"))

    @property
    def default_auto_archive_duration(self) -> int:
        return int(self._data.get("default_auto_archive_duration") or 1440)

    @property
    def default_thread_slowmode_delay(self) -> int:
        return int(self._data.get("default_thread_rate_limit_per_user") or 0)

    def is_news(self) -> bool:
        return self._data.get("type") == ChannelType.news

    @property
    def threads(self) -> List["Thread"]:
        if self.guild is None:
            return []
        return [t for t in self.guild.threads if t.parent_id == self.id]


NewsChannel = TextChannel


class VoiceChannel(GuildChannel):
    __slots__ = ()

    @property
    def bitrate(self) -> int:
        return int(self._data.get("bitrate") or 64000)

    @property
    def user_limit(self) -> int:
        return int(self._data.get("user_limit") or 0)

    @property
    def rtc_region(self) -> Optional[str]:
        return self._data.get("rtc_region")

    @property
    def video_quality_mode(self) -> Any:
        return try_enum(VideoQualityMode, self._data.get("video_quality_mode", 1))

    @property
    def slowmode_delay(self) -> int:
        return int(self._data.get("rate_limit_per_user") or 0)

    @property
    def last_message_id(self) -> Optional[int]:
        return _int(self._data.get("last_message_id"))


class StageChannel(VoiceChannel):
    __slots__ = ()

    @property
    def topic(self) -> Optional[str]:
        return self._data.get("topic")


class CategoryChannel(GuildChannel):
    __slots__ = ()

    @property
    def channels(self) -> List[GuildChannel]:
        if self.guild is None:
            return []
        return _sorted_by_position(
            [c for c in self.guild.channels if c.category_id == self.id]
        )

    @property
    def text_channels(self) -> List[TextChannel]:
        return [c for c in self.channels if isinstance(c, TextChannel)]

    @property
    def voice_channels(self) -> List[VoiceChannel]:
        return [c for c in self.channels if type(c) is VoiceChannel]

    @property
    def stage_channels(self) -> List[StageChannel]:
        return [c for c in self.channels if isinstance(c, StageChannel)]

    @property
    def forums(self) -> List["ForumChannel"]:
        return [c for c in self.channels if isinstance(c, ForumChannel)]


class ForumTag(_Hashable):
    __slots__ = ("id", "name", "moderated", "emoji")

    def __init__(self, data: Dict[str, Any]):
        self.id: int = _int(data.get("id")) or 0
        self.name: str = data.get("name") or ""
        self.moderated: bool = bool(data.get("moderated", False))
        self.emoji: Optional[PartialEmoji] = PartialEmoji.from_dict(
            {"emoji_id": data.get("emoji_id"), "emoji_name": data.get("emoji_name")}
        )

    def __repr__(self) -> str:
        return f"<ForumTag id={self.id} name={self.name!r}>"


class ForumChannel(GuildChannel):
    __slots__ = ()

    @property
    def topic(self) -> Optional[str]:
        return self._data.get("topic")

    @property
    def slowmode_delay(self) -> int:
        return int(self._data.get("rate_limit_per_user") or 0)

    @property
    def default_thread_slowmode_delay(self) -> int:
        return int(self._data.get("default_thread_rate_limit_per_user") or 0)

    @property
    def default_auto_archive_duration(self) -> int:
        return int(self._data.get("default_auto_archive_duration") or 1440)

    @property
    def default_layout(self) -> int:
        return int(self._data.get("default_forum_layout") or 0)

    @property
    def default_sort_order(self) -> Optional[int]:
        return _int(self._data.get("default_sort_order"))

    @property
    def default_reaction_emoji(self) -> Optional[PartialEmoji]:
        return PartialEmoji.from_dict(self._data.get("default_reaction_emoji"))

    @property
    def available_tags(self) -> List[ForumTag]:
        return [ForumTag(t) for t in self._data.get("available_tags") or [] if isinstance(t, dict)]

    def get_tag(self, tag_id: int) -> Optional[ForumTag]:
        for tag in self.available_tags:
            if tag.id == tag_id:
                return tag
        return None

    @property
    def require_tag(self) -> bool:
        return bool(self.flags.require_tag)

    def is_media(self) -> bool:
        return self._data.get("type") == ChannelType.media

    @property
    def threads(self) -> List["Thread"]:
        if self.guild is None:
            return []
        return [t for t in self.guild.threads if t.parent_id == self.id]


MediaChannel = ForumChannel


class Thread(GuildChannel):
    __slots__ = ()

    @property
    def parent_id(self) -> Optional[int]:
        return _int(self._data.get("parent_id"))

    @property
    def parent(self) -> Optional[GuildChannel]:
        pid = self.parent_id
        if pid is None:
            return None
        if self.guild is not None:
            found = self.guild.get_channel(pid)
            if found is not None:
                return found
        return _state_call(self._state, "get_channel", pid)

    @property
    def category_id(self) -> Optional[int]:
        parent = self.parent
        return getattr(parent, "category_id", None) if parent is not None else None

    @property
    def category(self) -> Optional[CategoryChannel]:
        parent = self.parent
        return getattr(parent, "category", None) if parent is not None else None

    @property
    def owner_id(self) -> Optional[int]:
        return _int(self._data.get("owner_id"))

    @property
    def _metadata(self) -> Dict[str, Any]:
        return self._data.get("thread_metadata") or {}

    @property
    def archived(self) -> bool:
        return bool(self._metadata.get("archived", False))

    @property
    def locked(self) -> bool:
        return bool(self._metadata.get("locked", False))

    @property
    def invitable(self) -> bool:
        return bool(self._metadata.get("invitable", True))

    @property
    def auto_archive_duration(self) -> int:
        return int(self._metadata.get("auto_archive_duration") or 1440)

    @property
    def archive_timestamp(self) -> Optional[_dt.datetime]:
        return parse_time(self._metadata.get("archive_timestamp"))

    @property
    def created_at(self) -> _dt.datetime:
        return parse_time(self._metadata.get("create_timestamp")) or snowflake_time(self.id)

    @property
    def slowmode_delay(self) -> int:
        return int(self._data.get("rate_limit_per_user") or 0)

    @property
    def message_count(self) -> int:
        return int(self._data.get("message_count") or 0)

    @property
    def member_count(self) -> int:
        return int(self._data.get("member_count") or 0)

    @property
    def last_message_id(self) -> Optional[int]:
        return _int(self._data.get("last_message_id"))

    @property
    def nsfw(self) -> bool:
        parent = self.parent
        return bool(getattr(parent, "nsfw", False)) if parent is not None else False

    @property
    def _applied_tags(self) -> List[int]:
        return [t for t in (_int(x) for x in self._data.get("applied_tags") or []) if t]

    @property
    def applied_tags(self) -> List[ForumTag]:
        parent = self.parent
        if not isinstance(parent, ForumChannel):
            return []
        tags = {t.id: t for t in parent.available_tags}
        return [tags[t] for t in self._applied_tags if t in tags]

    def is_private(self) -> bool:
        return self._data.get("type") == ChannelType.private_thread

    def is_news(self) -> bool:
        return self._data.get("type") == ChannelType.news_thread

    @property
    def permission_overwrites(self) -> List[PermissionOverwrite]:
        return []

    _overwrites = permission_overwrites

    def permissions_for(self, obj: Any) -> Permissions:
        """The parent's permissions, with sending governed by
        send_messages_in_threads (discord.py Thread.permissions_for)."""
        parent = self.parent
        if not isinstance(parent, GuildChannel):
            return Permissions.none()
        base = parent.permissions_for(obj)
        if base.administrator:
            return base
        base.send_messages = base.send_messages_in_threads
        if not base.send_messages:
            base.send_tts_messages = False
            base.mention_everyone = False
            base.embed_links = False
            base.attach_files = False
        return base


class DMChannel(_Channel):
    __slots__ = ()

    @property
    def recipients(self) -> List[User]:
        return [User(u, self._state) for u in self._data.get("recipients") or [] if isinstance(u, dict)]

    @property
    def recipient(self) -> Optional[User]:
        recipients = self.recipients
        return recipients[0] if recipients else None

    @property
    def me(self) -> Optional[User]:
        return getattr(self._state, "user", None)

    @property
    def guild(self) -> None:
        return None

    @property
    def last_message_id(self) -> Optional[int]:
        return _int(self._data.get("last_message_id"))

    @property
    def jump_url(self) -> str:
        return f"{WEB}/channels/@me/{self.id}"

    def permissions_for(self, obj: Any = None) -> Permissions:
        perms = Permissions._of(
            ("view_channel", "send_messages", "send_tts_messages", "embed_links",
             "attach_files", "read_message_history", "mention_everyone",
             "use_external_emojis", "add_reactions")
        )
        return perms

    @property
    def name(self) -> str:
        r = self.recipient
        return f"Direct Message with {r}" if r is not None else "Direct Message"


class GroupChannel(DMChannel):
    __slots__ = ()

    @property
    def name(self) -> str:
        return self._data.get("name") or ", ".join(u.name for u in self.recipients)

    @property
    def owner_id(self) -> Optional[int]:
        return _int(self._data.get("owner_id"))


_GUILD_CHANNEL_CLASSES: Dict[int, type] = {
    ChannelType.text: TextChannel,
    ChannelType.news: TextChannel,
    ChannelType.voice: VoiceChannel,
    ChannelType.stage_voice: StageChannel,
    ChannelType.category: CategoryChannel,
    ChannelType.forum: ForumChannel,
    ChannelType.media: ForumChannel,
    ChannelType.news_thread: Thread,
    ChannelType.public_thread: Thread,
    ChannelType.private_thread: Thread,
    ChannelType.directory: GuildChannel,
}


def channel_from_dict(
    data: Dict[str, Any], guild: Optional["Guild"] = None, state: Any = None
) -> Union[GuildChannel, DMChannel]:
    """The wrapper class matching a channel payload's ``type``."""
    ctype = _int(data.get("type"))
    if ctype == ChannelType.private:
        return DMChannel(data, state)
    if ctype == ChannelType.group:
        return GroupChannel(data, state)
    cls = _GUILD_CHANNEL_CLASSES.get(ctype if ctype is not None else -1, GuildChannel)
    return cls(data, guild, state)


def is_thread_type(value: Any) -> bool:
    return _int(getattr(value, "value", value)) in THREAD_TYPES


def is_forum_type(value: Any) -> bool:
    return _int(getattr(value, "value", value)) in FORUM_LIKE_TYPES


# ---------------------------------------------------------------------------
# guild
# ---------------------------------------------------------------------------


class Guild(_Wrapper):
    """A guild from GUILD_CREATE, READY, or REST.

    User-account READY nests most scalar fields under ``properties``; reads fall
    back to it so either shape works. Channels, threads, roles, emojis,
    stickers and members are wrapped once at construction and indexed by id.
    """

    __slots__ = ("_channels", "_threads", "_roles", "_members", "_emojis", "_stickers")

    def __init__(self, data: Dict[str, Any], state: Any = None):
        super().__init__(data, state)
        if not self.id:
            self.id = _int(self._props.get("id")) or 0
        self._channels: Dict[int, GuildChannel] = {}
        self._threads: Dict[int, Thread] = {}
        self._roles: Dict[int, Role] = {}
        self._members: Dict[int, Member] = {}
        for raw in data.get("channels") or []:
            self._add_channel(raw)
        for raw in data.get("threads") or []:
            self._add_channel(raw)
        for raw in data.get("roles") or []:
            role = Role(raw, self, state)
            self._roles[role.id] = role
        for raw in data.get("members") or []:
            member = Member(raw, self, state)
            if member.id:
                self._members[member.id] = member
        self._emojis: List[Emoji] = [Emoji(e, self, state) for e in data.get("emojis") or []]
        self._stickers: List[Sticker] = [Sticker(s, state) for s in data.get("stickers") or []]

    # -- cache upkeep (used by the gateway branch; harmless here) ----------

    def _add_channel(self, raw: Dict[str, Any]) -> Optional[GuildChannel]:
        if not isinstance(raw, dict):
            return None
        ch = channel_from_dict(raw, self, self._state)
        if not isinstance(ch, GuildChannel) or not ch.id:
            return None
        if isinstance(ch, Thread):
            self._threads[ch.id] = ch
        else:
            self._channels[ch.id] = ch
        return ch

    def _remove_channel(self, channel_id: int) -> None:
        self._channels.pop(channel_id, None)
        self._threads.pop(channel_id, None)

    def _add_member(self, member: Member) -> None:
        if member.id:
            self._members[member.id] = member

    # -- scalar fields -----------------------------------------------------

    @property
    def _props(self) -> Dict[str, Any]:
        props = self._data.get("properties")
        return props if isinstance(props, dict) else {}

    def _get(self, key: str, default: Any = None) -> Any:
        if key in self._data:
            return self._data[key]
        return self._props.get(key, default)

    @property
    def name(self) -> str:
        return self._get("name") or ""

    def _image(self, key: str, path: str) -> Optional[Asset]:
        value = self._get(key)
        return Asset.from_guild_image(self.id, value, path) if value else None

    @property
    def icon(self) -> Optional[Asset]:
        return self._image("icon", "icons")

    @property
    def banner(self) -> Optional[Asset]:
        return self._image("banner", "banners")

    @property
    def splash(self) -> Optional[Asset]:
        return self._image("splash", "splashes")

    @property
    def discovery_splash(self) -> Optional[Asset]:
        return self._image("discovery_splash", "discovery-splashes")

    @property
    def description(self) -> Optional[str]:
        return self._get("description")

    @property
    def vanity_url_code(self) -> Optional[str]:
        return self._get("vanity_url_code")

    @property
    def preferred_locale(self) -> Optional[str]:
        return self._get("preferred_locale")

    @property
    def features(self) -> List[str]:
        return list(self._get("features") or [])

    @property
    def owner_id(self) -> Optional[int]:
        return _int(self._get("owner_id"))

    @property
    def owner(self) -> Optional[Member]:
        oid = self.owner_id
        return self.get_member(oid) if oid else None

    @property
    def afk_timeout(self) -> int:
        return int(self._get("afk_timeout") or 0)

    def _channel_field(self, key: str) -> Optional[GuildChannel]:
        cid = _int(self._get(key))
        return self.get_channel(cid) if cid else None

    @property
    def afk_channel(self) -> Optional[GuildChannel]:
        return self._channel_field("afk_channel_id")

    @property
    def system_channel(self) -> Optional[GuildChannel]:
        return self._channel_field("system_channel_id")

    @property
    def rules_channel(self) -> Optional[GuildChannel]:
        return self._channel_field("rules_channel_id")

    @property
    def public_updates_channel(self) -> Optional[GuildChannel]:
        return self._channel_field("public_updates_channel_id")

    @property
    def widget_channel(self) -> Optional[GuildChannel]:
        return self._channel_field("widget_channel_id")

    @property
    def system_channel_flags(self) -> SystemChannelFlags:
        return SystemChannelFlags(self._get("system_channel_flags") or 0)

    @property
    def widget_enabled(self) -> bool:
        return bool(self._get("widget_enabled", False))

    @property
    def verification_level(self) -> int:
        return int(self._get("verification_level") or 0)

    @property
    def explicit_content_filter(self) -> int:
        return int(self._get("explicit_content_filter") or 0)

    @property
    def default_notifications(self) -> int:
        return int(self._get("default_message_notifications") or 0)

    @property
    def premium_tier(self) -> int:
        return int(self._get("premium_tier") or 0)

    @property
    def premium_subscription_count(self) -> int:
        return int(self._get("premium_subscription_count") or 0)

    @property
    def max_members(self) -> Optional[int]:
        return _int(self._get("max_members"))

    @property
    def max_presences(self) -> Optional[int]:
        return _int(self._get("max_presences"))

    @property
    def max_video_channel_users(self) -> Optional[int]:
        return _int(self._get("max_video_channel_users"))

    @property
    def nsfw_level(self) -> int:
        return int(self._get("nsfw_level") or 0)

    @property
    def mfa_level(self) -> int:
        return int(self._get("mfa_level") or 0)

    @property
    def member_count(self) -> Optional[int]:
        return _int(self._get("member_count"))

    @property
    def unavailable(self) -> bool:
        return bool(self._data.get("unavailable", False))

    @property
    def large(self) -> bool:
        return bool(self._get("large", False))

    @property
    def joined_at(self) -> Optional[_dt.datetime]:
        return parse_time(self._get("joined_at"))

    # -- collections -------------------------------------------------------

    @property
    def roles(self) -> List[Role]:
        """Lowest to highest, @everyone first."""
        return sorted(self._roles.values())

    @property
    def default_role(self) -> Optional[Role]:
        return self._roles.get(self.id)

    def get_role(self, role_id: Optional[int]) -> Optional[Role]:
        return self._roles.get(_int(role_id) or 0)

    @property
    def emojis(self) -> List[Emoji]:
        return list(self._emojis)

    @property
    def stickers(self) -> List[Sticker]:
        return list(self._stickers)

    def get_emoji(self, emoji_id: int) -> Optional[Emoji]:
        for e in self._emojis:
            if e.id == emoji_id:
                return e
        return None

    @property
    def channels(self) -> List[GuildChannel]:
        """Every non-thread channel."""
        return list(self._channels.values())

    @property
    def categories(self) -> List[CategoryChannel]:
        return _sorted_by_position([c for c in self._channels.values() if isinstance(c, CategoryChannel)])

    @property
    def text_channels(self) -> List[TextChannel]:
        return _sorted_by_position([c for c in self._channels.values() if isinstance(c, TextChannel)])

    @property
    def voice_channels(self) -> List[VoiceChannel]:
        return _sorted_by_position([c for c in self._channels.values() if type(c) is VoiceChannel])

    @property
    def stage_channels(self) -> List[StageChannel]:
        return _sorted_by_position([c for c in self._channels.values() if isinstance(c, StageChannel)])

    @property
    def forums(self) -> List[ForumChannel]:
        return _sorted_by_position([c for c in self._channels.values() if isinstance(c, ForumChannel)])

    @property
    def threads(self) -> List[Thread]:
        return list(self._threads.values())

    def get_channel(self, channel_id: Optional[int]) -> Optional[GuildChannel]:
        """A channel or a thread, like discord.py's get_channel_or_thread."""
        cid = _int(channel_id) or 0
        return self._channels.get(cid) or self._threads.get(cid)

    get_channel_or_thread = get_channel

    def get_thread(self, thread_id: Optional[int]) -> Optional[Thread]:
        return self._threads.get(_int(thread_id) or 0)

    @property
    def members(self) -> List[Member]:
        return list(self._members.values())

    def get_member(self, user_id: Optional[int]) -> Optional[Member]:
        return self._members.get(_int(user_id) or 0)

    @property
    def me(self) -> Optional[Member]:
        user = getattr(self._state, "user", None)
        uid = getattr(user, "id", None)
        return self.get_member(uid) if uid else None

    def __str__(self) -> str:
        return self.name


# ---------------------------------------------------------------------------
# messages
# ---------------------------------------------------------------------------


class Attachment(_Wrapper):
    __slots__ = ()

    @property
    def filename(self) -> str:
        return self._data.get("filename") or ""

    @property
    def url(self) -> str:
        return self._data.get("url") or ""

    @property
    def proxy_url(self) -> str:
        return self._data.get("proxy_url") or ""

    @property
    def size(self) -> int:
        return int(self._data.get("size") or 0)

    @property
    def content_type(self) -> Optional[str]:
        return self._data.get("content_type")

    @property
    def width(self) -> Optional[int]:
        return _int(self._data.get("width"))

    @property
    def height(self) -> Optional[int]:
        return _int(self._data.get("height"))

    @property
    def description(self) -> Optional[str]:
        return self._data.get("description")

    @property
    def title(self) -> Optional[str]:
        return self._data.get("title")

    @property
    def ephemeral(self) -> bool:
        return bool(self._data.get("ephemeral", False))

    @property
    def duration(self) -> Optional[float]:
        return self._data.get("duration_secs")

    @property
    def waveform(self) -> Optional[str]:
        return self._data.get("waveform")

    @property
    def flags(self) -> int:
        return int(self._data.get("flags") or 0)

    def is_spoiler(self) -> bool:
        return self.filename.startswith("SPOILER_")

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._data)

    def __str__(self) -> str:
        return self.url


class Embed:
    """An embed payload. ``to_dict()`` is what every client call site uses."""

    __slots__ = ("_data",)

    def __init__(self, data: Optional[Dict[str, Any]] = None):
        self._data: Dict[str, Any] = data or {}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Embed":
        return cls(copy.deepcopy(data))

    def to_dict(self) -> Dict[str, Any]:
        return copy.deepcopy(self._data)

    def __getattr__(self, name: str) -> Any:
        if name in ("title", "description", "url", "type", "timestamp", "fields",
                    "footer", "image", "thumbnail", "video", "provider", "author"):
            return self._data.get(name)
        if name in ("color", "colour"):
            value = self._data.get("color")
            return Colour(value) if value is not None else None
        raise AttributeError(name)

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return bool(self._data)


class Reaction:
    __slots__ = ("_data", "message")

    def __init__(self, data: Dict[str, Any], message: "Message"):
        self._data = data or {}
        self.message = message

    @property
    def emoji(self) -> Optional[PartialEmoji]:
        return PartialEmoji.from_dict(self._data.get("emoji"))

    @property
    def count(self) -> int:
        return int(self._data.get("count") or 0)

    @property
    def me(self) -> bool:
        return bool(self._data.get("me", False))


class DeletedReferencedMessage:
    """The referenced message no longer exists (Discord sent null)."""

    __slots__ = ("_parent",)

    def __init__(self, parent: "MessageReference"):
        self._parent = parent

    @property
    def id(self) -> Optional[int]:
        return self._parent.message_id

    @property
    def channel_id(self) -> Optional[int]:
        return self._parent.channel_id

    @property
    def guild_id(self) -> Optional[int]:
        return self._parent.guild_id


class MessageReference:
    __slots__ = ("_data", "resolved")

    def __init__(self, data: Dict[str, Any], resolved: Any = None):
        self._data = data or {}
        self.resolved: Any = resolved

    @property
    def message_id(self) -> Optional[int]:
        return _int(self._data.get("message_id"))

    @property
    def channel_id(self) -> Optional[int]:
        return _int(self._data.get("channel_id"))

    @property
    def guild_id(self) -> Optional[int]:
        return _int(self._data.get("guild_id"))

    @property
    def type(self) -> int:
        """0 default (reply), 1 forward."""
        return int(self._data.get("type") or 0)

    @property
    def fail_if_not_exists(self) -> bool:
        return bool(self._data.get("fail_if_not_exists", True))

    @property
    def cached_message(self) -> Any:
        return self.resolved if isinstance(self.resolved, Message) else None

    @property
    def jump_url(self) -> str:
        return f"{WEB}/channels/{self.guild_id or '@me'}/{self.channel_id}/{self.message_id}"

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._data)


_MENTION_RE = re.compile(r"<@!?([0-9]{15,21})>")
_ROLE_MENTION_RE = re.compile(r"<@&([0-9]{15,21})>")
_CHANNEL_MENTION_RE = re.compile(r"<#([0-9]{15,21})>")


class Message(_Wrapper):
    """A MESSAGE_CREATE/UPDATE payload or a REST message object."""

    __slots__ = ("_guild", "_channel")

    def __init__(
        self,
        data: Dict[str, Any],
        state: Any = None,
        *,
        guild: Optional[Guild] = None,
        channel: Any = None,
    ):
        super().__init__(data, state)
        self._guild = guild
        self._channel = channel

    # -- location ----------------------------------------------------------

    @property
    def channel_id(self) -> int:
        return _int(self._data.get("channel_id")) or 0

    @property
    def guild_id(self) -> Optional[int]:
        if self._guild is not None:
            return self._guild.id
        return _int(self._data.get("guild_id"))

    @property
    def guild(self) -> Optional[Guild]:
        if self._guild is not None:
            return self._guild
        gid = _int(self._data.get("guild_id"))
        if gid:
            found = _state_call(self._state, "get_guild", gid)
            if found is not None:
                return found
        return getattr(self._channel, "guild", None)

    @property
    def channel(self) -> Any:
        if self._channel is not None:
            return self._channel
        cid = self.channel_id
        guild = self.guild
        found = guild.get_channel(cid) if guild is not None else None
        if found is None:
            found = _state_call(self._state, "get_channel", cid)
        if found is None:
            found = PartialMessageable(cid, guild_id=self.guild_id)
        return found

    @property
    def jump_url(self) -> str:
        return f"{WEB}/channels/{self.guild_id or '@me'}/{self.channel_id}/{self.id}"

    # -- people ------------------------------------------------------------

    @property
    def author(self) -> User:
        author = self._data.get("author") or {}
        guild = self.guild
        member = self._data.get("member")
        if guild is not None and isinstance(member, dict) and not self.webhook_id:
            return Member.from_message(author, member, guild, self._state)
        if guild is not None and not self.webhook_id:
            cached = guild.get_member(_int(author.get("id")))
            if cached is not None:
                return cached
        return User(author, self._state)

    @property
    def mentions(self) -> List[User]:
        guild = self.guild
        out: List[User] = []
        for raw in self._data.get("mentions") or []:
            if not isinstance(raw, dict):
                continue
            member = raw.get("member")
            if guild is not None and isinstance(member, dict):
                user = {k: v for k, v in raw.items() if k != "member"}
                out.append(Member.from_message(user, member, guild, self._state))
            elif guild is not None and guild.get_member(_int(raw.get("id"))) is not None:
                out.append(guild.get_member(_int(raw.get("id"))))
            else:
                out.append(User(raw, self._state))
        return out

    @property
    def raw_mentions(self) -> List[int]:
        return [int(x) for x in _MENTION_RE.findall(self.content)]

    @property
    def raw_role_mentions(self) -> List[int]:
        return [int(x) for x in _ROLE_MENTION_RE.findall(self.content)]

    @property
    def raw_channel_mentions(self) -> List[int]:
        return [int(x) for x in _CHANNEL_MENTION_RE.findall(self.content)]

    @property
    def role_mentions(self) -> List[Role]:
        guild = self.guild
        if guild is None:
            return []
        out: List[Role] = []
        for rid in self._data.get("mention_roles") or []:
            role = guild.get_role(_int(rid))
            if role is not None:
                out.append(role)
        return out

    @property
    def channel_mentions(self) -> List[GuildChannel]:
        guild = self.guild
        if guild is None:
            return []
        out: List[GuildChannel] = []
        seen: set = set()
        for cid in self.raw_channel_mentions:
            ch = guild.get_channel(cid)
            if ch is not None and cid not in seen:
                seen.add(cid)
                out.append(ch)
        return out

    @property
    def mention_everyone(self) -> bool:
        return bool(self._data.get("mention_everyone", False))

    # -- content -----------------------------------------------------------

    @property
    def content(self) -> str:
        return self._data.get("content") or ""

    @property
    def type(self) -> Any:
        return try_enum(MessageType, self._data.get("type", 0))

    @property
    def flags(self) -> MessageFlags:
        return MessageFlags(self._data.get("flags") or 0)

    @property
    def attachments(self) -> List[Attachment]:
        return [Attachment(a) for a in self._data.get("attachments") or [] if isinstance(a, dict)]

    @property
    def embeds(self) -> List[Embed]:
        return [Embed(e) for e in self._data.get("embeds") or [] if isinstance(e, dict)]

    @property
    def stickers(self) -> List[Sticker]:
        items = self._data.get("sticker_items") or self._data.get("stickers") or []
        return [Sticker(s) for s in items if isinstance(s, dict)]

    @property
    def reactions(self) -> List[Reaction]:
        return [Reaction(r, self) for r in self._data.get("reactions") or [] if isinstance(r, dict)]

    @property
    def components(self) -> List[Dict[str, Any]]:
        return list(self._data.get("components") or [])

    @property
    def message_snapshots(self) -> List[Dict[str, Any]]:
        return list(self._data.get("message_snapshots") or [])

    @property
    def interaction(self) -> Optional[Dict[str, Any]]:
        return self._data.get("interaction_metadata") or self._data.get("interaction")

    @property
    def poll(self) -> Optional[Dict[str, Any]]:
        return self._data.get("poll")

    @property
    def pinned(self) -> bool:
        return bool(self._data.get("pinned", False))

    @property
    def tts(self) -> bool:
        return bool(self._data.get("tts", False))

    @property
    def nonce(self) -> Any:
        return self._data.get("nonce")

    @property
    def position(self) -> Optional[int]:
        return _int(self._data.get("position"))

    @property
    def webhook_id(self) -> Optional[int]:
        return _int(self._data.get("webhook_id"))

    @property
    def application_id(self) -> Optional[int]:
        return _int(self._data.get("application_id"))

    @property
    def edited_at(self) -> Optional[_dt.datetime]:
        return parse_time(self._data.get("edited_timestamp"))

    @property
    def created_at(self) -> _dt.datetime:
        return parse_time(self._data.get("timestamp")) or snowflake_time(self.id)

    @property
    def thread(self) -> Optional[Thread]:
        raw = self._data.get("thread")
        if not isinstance(raw, dict):
            return None
        guild = self.guild
        if guild is not None:
            cached = guild.get_thread(_int(raw.get("id")))
            if cached is not None:
                return cached
        ch = channel_from_dict(raw, guild, self._state)
        return ch if isinstance(ch, Thread) else None

    @property
    def reference(self) -> Optional[MessageReference]:
        ref = self._data.get("message_reference")
        if not isinstance(ref, dict):
            return None
        resolved: Any = None
        if "referenced_message" in self._data:
            raw = self._data.get("referenced_message")
            if isinstance(raw, dict):
                resolved = Message(raw, self._state, guild=self._guild)
            else:
                resolved = DeletedReferencedMessage(MessageReference(ref))
        return MessageReference(ref, resolved)

    def is_system(self) -> bool:
        """Whether this is a system message rather than user content
        (discord.py Message.is_system)."""
        return self._data.get("type", 0) not in NON_SYSTEM_MESSAGE_TYPES

    @property
    def clean_content(self) -> str:
        """Content with mentions rendered as plain names and @everyone/@here
        defused, the way discord.py's clean_content renders them."""
        guild = self.guild
        names: Dict[int, str] = {}
        for user in self.mentions:
            names[user.id] = f"@{user.display_name}"

        def _user(m: "re.Match[str]") -> str:
            uid = int(m.group(1))
            if uid in names:
                return names[uid]
            member = guild.get_member(uid) if guild is not None else None
            return f"@{member.display_name}" if member is not None else "@deleted-user"

        def _role(m: "re.Match[str]") -> str:
            role = guild.get_role(int(m.group(1))) if guild is not None else None
            return f"@{role.name}" if role is not None else "@deleted-role"

        def _channel(m: "re.Match[str]") -> str:
            ch = guild.get_channel(int(m.group(1))) if guild is not None else None
            return f"#{ch.name}" if ch is not None else "#deleted-channel"

        text = _MENTION_RE.sub(_user, self.content)
        text = _ROLE_MENTION_RE.sub(_role, text)
        text = _CHANNEL_MENTION_RE.sub(_channel, text)
        return text.replace("@everyone", "@​everyone").replace("@here", "@​here")

    @property
    def system_content(self) -> str:
        """Discord's rendering of a system message, else the plain content.

        A port of discord.py's Message.system_content for the types the
        client can see; anything unrecognised falls back to ``content``.
        """
        t = self._data.get("type", 0)
        author = self.author
        name = author.name

        if t in NON_SYSTEM_MESSAGE_TYPES and t != MessageType.thread_starter_message:
            return self.content

        mentioned = self.mentions
        target = mentioned[0].name if mentioned else ""

        if t == MessageType.recipient_add:
            if self.channel_id and isinstance(self.channel, GroupChannel):
                return f"{name} added {target} to the group."
            return f"{name} added {target} to the thread."
        if t == MessageType.recipient_remove:
            if isinstance(self.channel, GroupChannel):
                return f"{name} removed {target} from the group."
            return f"{name} removed {target} from the thread."
        if t == MessageType.channel_name_change:
            if isinstance(self.channel, Thread):
                return f"{name} changed the thread name: **{self.content}**"
            return f"{name} changed the channel name: **{self.content}**"
        if t == MessageType.channel_icon_change:
            return f"{name} changed the group icon."
        if t == MessageType.pins_add:
            return f"{name} pinned a message to this channel."
        if t == MessageType.new_member:
            formats = _JOIN_FORMATS
            created_ms = int(self.created_at.timestamp() * 1000)
            return formats[created_ms % len(formats)].format(name)
        if t == MessageType.premium_guild_subscription:
            if not self.content:
                return f"{name} just boosted the server!"
            return f"{name} just boosted the server **{self.content}** times!"
        if t in (
            MessageType.premium_guild_tier_1,
            MessageType.premium_guild_tier_2,
            MessageType.premium_guild_tier_3,
        ):
            level = int(t) - int(MessageType.premium_guild_tier_1) + 1
            guild_name = getattr(self.guild, "name", "")
            if not self.content:
                return f"{name} just boosted the server! {guild_name} has achieved **Level {level}!**"
            return (
                f"{name} just boosted the server **{self.content}** times! "
                f"{guild_name} has achieved **Level {level}!**"
            )
        if t == MessageType.channel_follow_add:
            return (
                f"{name} has added {self.content} to this channel. Its most important "
                "updates will show up here."
            )
        if t == MessageType.guild_stream:
            return f"{name} is live! Now streaming {self.content}"
        if t == MessageType.guild_discovery_disqualified:
            return (
                "This server has been removed from Server Discovery because it no longer "
                "passes all the requirements. Check Server Settings for more details."
            )
        if t == MessageType.guild_discovery_requalified:
            return "This server is eligible for Server Discovery again and has been automatically relisted!"
        if t == MessageType.guild_discovery_grace_period_initial_warning:
            return (
                "This server has failed Discovery activity requirements for 1 week. If this "
                "server fails for 4 weeks in a row, it will be automatically removed from Discovery."
            )
        if t == MessageType.guild_discovery_grace_period_final_warning:
            return (
                "This server has failed Discovery activity requirements for 3 weeks in a row. "
                "If this server fails for 1 more week, it will be removed from Discovery."
            )
        if t == MessageType.thread_created:
            return f"{name} started a thread: **{self.content}**. See all **threads**."
        if t == MessageType.thread_starter_message:
            ref = self.reference
            resolved = getattr(ref, "resolved", None) if ref is not None else None
            if isinstance(resolved, Message):
                return resolved.content
            return "Sorry, we couldn't load the first message in this thread"
        if t == MessageType.guild_invite_reminder:
            return "Wondering who to invite?\nStart by inviting anyone who can help you build the server!"
        if t == MessageType.stage_start:
            return f"{name} started **{self.content}**."
        if t == MessageType.stage_end:
            return f"{name} ended **{self.content}**."
        if t == MessageType.stage_speaker:
            return f"{name} is now a speaker."
        if t == MessageType.stage_raise_hand:
            return f"{name} requested to speak."
        if t == MessageType.stage_topic:
            return f"{name} changed Stage topic: **{self.content}**."
        if t == MessageType.call:
            return f"{name} started a call."
        return self.content

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._data)


# discord.py's welcome-message rotation, picked by created_at milliseconds.
_JOIN_FORMATS = (
    "{0} joined the party.",
    "{0} is here.",
    "Welcome, {0}. We hope you brought pizza.",
    "A wild {0} appeared.",
    "{0} just landed.",
    "{0} just slid into the server.",
    "{0} just showed up!",
    "Welcome {0}. Say hi!",
    "{0} hopped into the server.",
    "Everyone welcome {0}!",
    "Glad you're here, {0}.",
    "Good to see you, {0}.",
    "Yay you made it, {0}!",
)


__all__ = [
    "Object",
    "Asset",
    "Permissions",
    "MessageFlags",
    "ChannelFlags",
    "SystemChannelFlags",
    "Colour",
    "Color",
    "User",
    "ClientUser",
    "Member",
    "Role",
    "PartialEmoji",
    "Emoji",
    "Sticker",
    "StickerItem",
    "GuildSticker",
    "PermissionOverwrite",
    "PartialMessageable",
    "GuildChannel",
    "TextChannel",
    "NewsChannel",
    "VoiceChannel",
    "StageChannel",
    "CategoryChannel",
    "ForumTag",
    "ForumChannel",
    "MediaChannel",
    "Thread",
    "DMChannel",
    "GroupChannel",
    "Guild",
    "Attachment",
    "Embed",
    "Reaction",
    "DeletedReferencedMessage",
    "MessageReference",
    "Message",
    "channel_from_dict",
    "is_thread_type",
    "is_forum_type",
    "snowflake_time",
    "time_snowflake",
    "parse_time",
]
