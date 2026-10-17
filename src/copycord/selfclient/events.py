# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Gateway dispatch table: raw event name to ``on_*`` handler calls.

Handler names and argument shapes match discord.py's. ``state`` is optional
and only read; events that need a cached "before" are skipped on a cache miss,
as in discord.py.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from .models import (
    Emoji,
    Guild,
    GuildChannel,
    Member,
    Message,
    Role,
    Sticker,
    Thread,
    User,
    _int,
    _state_call,
    channel_from_dict,
)


class Dispatch:
    """One handler call: ``on_<event>(*args)``, plus the object to cache."""

    __slots__ = ("event", "args", "payload")

    def __init__(self, event: str, args: tuple = (), payload: Any = None):
        self.event: str = event
        self.args: tuple = args
        self.payload: Any = payload

    @property
    def handler_name(self) -> str:
        return f"on_{self.event}"

    def __repr__(self) -> str:
        return f"<Dispatch {self.event} args={len(self.args)}>"


# raw event payloads (discord.py's Raw*Event attribute names)


class RawMessageUpdateEvent:
    """MESSAGE_UPDATE as ``on_raw_message_edit`` receives it."""

    __slots__ = ("message_id", "channel_id", "guild_id", "data", "cached_message", "message")

    def __init__(self, data: Dict[str, Any], cached_message: Optional[Message] = None,
                 message: Optional[Message] = None):
        self.message_id: int = _int(data.get("id")) or 0
        self.channel_id: int = _int(data.get("channel_id")) or 0
        self.guild_id: Optional[int] = _int(data.get("guild_id"))
        self.data: Dict[str, Any] = data
        self.cached_message: Optional[Message] = cached_message
        self.message: Optional[Message] = message


class RawMessageDeleteEvent:
    """MESSAGE_DELETE as ``on_raw_message_delete`` receives it."""

    __slots__ = ("message_id", "channel_id", "guild_id", "cached_message")

    def __init__(self, data: Dict[str, Any], cached_message: Optional[Message] = None):
        self.message_id: int = _int(data.get("id")) or 0
        self.channel_id: int = _int(data.get("channel_id")) or 0
        self.guild_id: Optional[int] = _int(data.get("guild_id"))
        self.cached_message: Optional[Message] = cached_message


class RawBulkMessageDeleteEvent:
    """MESSAGE_DELETE_BULK as ``on_raw_bulk_message_delete`` receives it."""

    __slots__ = ("message_ids", "channel_id", "guild_id", "cached_messages")

    def __init__(self, data: Dict[str, Any], cached_messages: Optional[List[Message]] = None):
        self.message_ids = {i for i in (_int(x) for x in data.get("ids") or []) if i}
        self.channel_id: int = _int(data.get("channel_id")) or 0
        self.guild_id: Optional[int] = _int(data.get("guild_id"))
        self.cached_messages: List[Message] = list(cached_messages or [])


class RawThreadDeleteEvent:
    """THREAD_DELETE as ``on_raw_thread_delete`` receives it."""

    __slots__ = ("thread_id", "guild_id", "parent_id", "thread_type", "thread")

    def __init__(self, data: Dict[str, Any], thread: Optional[Thread] = None):
        self.thread_id: int = _int(data.get("id")) or 0
        self.guild_id: Optional[int] = _int(data.get("guild_id"))
        self.parent_id: Optional[int] = _int(data.get("parent_id"))
        self.thread_type: Optional[int] = _int(data.get("type"))
        self.thread: Optional[Thread] = thread


class ReadyEvent:
    """READY, parsed. Carried on ``Dispatch.payload`` since ``on_ready`` takes no arguments."""

    __slots__ = ("user", "guilds", "private_channels", "session_id", "resume_gateway_url", "data")

    def __init__(self, user: Optional[User], guilds: List[Guild], private_channels: List[Any],
                 session_id: Optional[str], resume_gateway_url: Optional[str],
                 data: Dict[str, Any]):
        self.user = user
        self.guilds = guilds
        self.private_channels = private_channels
        self.session_id = session_id
        self.resume_gateway_url = resume_gateway_url
        self.data = data


class GuildMembersChunk:
    """GUILD_MEMBERS_CHUNK, the answer to an op 8 request."""

    __slots__ = ("guild_id", "members", "nonce", "chunk_index", "chunk_count", "not_found", "data")

    def __init__(self, data: Dict[str, Any], guild: Guild, state: Any = None):
        self.guild_id: int = _int(data.get("guild_id")) or 0
        self.members: List[Member] = [
            Member(m, guild, state) for m in data.get("members") or [] if isinstance(m, dict)
        ]
        self.nonce: Optional[str] = data.get("nonce")
        self.chunk_index: int = int(data.get("chunk_index") or 0)
        self.chunk_count: int = int(data.get("chunk_count") or 1)
        self.not_found: List[Any] = list(data.get("not_found") or [])
        self.data = data


# lookups


def _guild(state: Any, guild_id: Any) -> Optional[Guild]:
    gid = _int(guild_id)
    if not gid:
        return None
    return _state_call(state, "get_guild", gid)


def _guild_or_stub(state: Any, guild_id: Any) -> Optional[Guild]:
    """The cached guild, or an id-only one so wrappers still know their guild."""
    gid = _int(guild_id)
    if not gid:
        return None
    return _guild(state, gid) or Guild({"id": str(gid)}, state)


def _cached_channel(state: Any, guild: Optional[Guild], channel_id: Any) -> Any:
    cid = _int(channel_id)
    if not cid:
        return None
    if guild is not None:
        found = guild.get_channel(cid) or guild.get_thread(cid)
        if found is not None:
            return found
    return _state_call(state, "get_channel", cid)


def _cached_message(state: Any, message_id: Any) -> Optional[Message]:
    mid = _int(message_id)
    if not mid:
        return None
    return _state_call(state, "_get_message", mid)


def _resolve_recipients(channel: Dict[str, Any], users: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """READY with DEDUPE_USER_OBJECTS sends recipient ids, not user objects."""
    if channel.get("recipients") or not channel.get("recipient_ids"):
        return channel
    resolved = [users[str(u)] for u in channel["recipient_ids"] if str(u) in users]
    return {**channel, "recipients": resolved}


# parsers


def parse_ready(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    user_raw = data.get("user")
    user = User(user_raw, state) if isinstance(user_raw, dict) else None
    guilds = [Guild(g, state) for g in data.get("guilds") or [] if isinstance(g, dict)]
    users = {
        str(u.get("id")): u for u in data.get("users") or [] if isinstance(u, dict) and u.get("id")
    }
    private = [
        channel_from_dict(_resolve_recipients(c, users), None, state)
        for c in data.get("private_channels") or []
        if isinstance(c, dict)
    ]
    ready = ReadyEvent(
        user, guilds, private, data.get("session_id"), data.get("resume_gateway_url"), data
    )
    return [Dispatch("ready", (), ready)]


def parse_resumed(data: Any, state: Any = None) -> List[Dispatch]:
    return [Dispatch("resumed")]


def parse_guild_create(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = Guild(data, state)
    if data.get("unavailable"):
        return [Dispatch("guild_unavailable", (guild,), guild)]
    # A guild already cached is back from an outage, not newly joined.
    known = _guild(state, guild.id) is not None
    return [Dispatch("guild_available" if known else "guild_join", (guild,), guild)]


def parse_guild_update(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    before = _guild(state, data.get("id"))
    if before is None:
        return [Dispatch("guild_update_uncached", (), Guild(data, state))]
    # GUILD_UPDATE carries no channels/members; keep the cached ones.
    merged = {**before.raw, **data}
    after = Guild(merged, state)
    return [Dispatch("guild_update", (before, after), after)]


def parse_guild_delete(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild(state, data.get("id")) or Guild(data, state)
    if data.get("unavailable"):
        return [Dispatch("guild_unavailable", (guild,), guild)]
    return [Dispatch("guild_remove", (guild,), guild)]


def parse_channel_create(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    channel = channel_from_dict(data, guild, state)
    if guild is None:
        return [Dispatch("private_channel_create", (channel,), channel)]
    return [Dispatch("guild_channel_create", (channel,), channel)]


def parse_channel_update(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    after = channel_from_dict(data, guild, state)
    before = _cached_channel(state, guild, data.get("id"))
    if guild is None:
        if before is None:
            return [Dispatch("private_channel_update_uncached", (), after)]
        return [Dispatch("private_channel_update", (before, after), after)]
    if before is None:
        return [Dispatch("guild_channel_update_uncached", (), after)]
    return [Dispatch("guild_channel_update", (before, after), after)]


def parse_channel_delete(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    channel = _cached_channel(state, guild, data.get("id")) or channel_from_dict(data, guild, state)
    if guild is None:
        return [Dispatch("private_channel_delete", (channel,), channel)]
    return [Dispatch("guild_channel_delete", (channel,), channel)]


def _thread(data: Dict[str, Any], guild: Optional[Guild], state: Any) -> GuildChannel:
    return channel_from_dict(data, guild, state)


def parse_thread_create(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    thread = _thread(data, guild, state)
    event = "thread_create" if data.get("newly_created") else "thread_join"
    return [Dispatch(event, (thread,), thread)]


def parse_thread_update(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    after = _thread(data, guild, state)
    before = _cached_channel(state, guild, data.get("id"))
    if before is None:
        # discord.py fires thread_join here; we report it as an update instead.
        return [Dispatch("thread_update_uncached", (), after)]
    return [Dispatch("thread_update", (before, after), after)]


def parse_thread_delete(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    cached = _cached_channel(state, guild, data.get("id"))
    out = [Dispatch("raw_thread_delete", (RawThreadDeleteEvent(data, cached),), None)]
    # The payload has every field on_thread_delete reads, so a cache miss still works.
    thread = cached if cached is not None else _thread(data, guild, state)
    out.append(Dispatch("thread_delete", (thread,), thread))
    return out


def parse_thread_list_sync(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    threads = [_thread(t, guild, state) for t in data.get("threads") or [] if isinstance(t, dict)]
    return [Dispatch("thread_list_sync", (), threads)]


def _message(data: Dict[str, Any], state: Any) -> Message:
    guild = _guild(state, data.get("guild_id"))
    channel = _cached_channel(state, guild, data.get("channel_id"))
    return Message(data, state, guild=guild, channel=channel)


def parse_message_create(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    message = _message(data, state)
    return [Dispatch("message", (message,), message)]


def parse_message_update(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    before = _cached_message(state, data.get("id"))
    after: Optional[Message] = None
    if before is not None:
        # An update can be partial (an embed resolving); fill from the cache.
        after = _message({**before.raw, **data}, state)
    elif data.get("author"):
        after = _message(data, state)
    raw = RawMessageUpdateEvent(data, before, after)
    out = [Dispatch("raw_message_edit", (raw,), after)]
    if before is not None and after is not None:
        out.append(Dispatch("message_edit", (before, after), after))
    return out


def parse_message_delete(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    cached = _cached_message(state, data.get("id"))
    out = [Dispatch("raw_message_delete", (RawMessageDeleteEvent(data, cached),), None)]
    if cached is not None:
        out.append(Dispatch("message_delete", (cached,), cached))
    return out


def parse_message_delete_bulk(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    cached = [m for m in (_cached_message(state, i) for i in data.get("ids") or []) if m]
    out = [Dispatch("raw_bulk_message_delete", (RawBulkMessageDeleteEvent(data, cached),), None)]
    if cached:
        out.append(Dispatch("bulk_message_delete", (cached,), cached))
    return out


def parse_guild_role_create(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    role = Role(data.get("role") or {}, guild, state)
    return [Dispatch("guild_role_create", (role,), role)]


def parse_guild_role_update(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    after = Role(data.get("role") or {}, guild, state)
    before = guild.get_role(after.id) if guild is not None else None
    if before is None:
        return [Dispatch("guild_role_update_uncached", (), after)]
    return [Dispatch("guild_role_update", (before, after), after)]


def parse_guild_role_delete(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    role_id = _int(data.get("role_id"))
    role = guild.get_role(role_id) if guild is not None else None
    if role is None:
        # The delete carries only the id.
        role = Role({"id": str(role_id or 0)}, guild, state)
    return [Dispatch("guild_role_delete", (role,), role)]


def parse_guild_member_add(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    member = Member(data, guild, state)
    return [Dispatch("member_join", (member,), member)]


def parse_guild_member_remove(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    user = User(data.get("user") or {}, state)
    cached = guild.get_member(user.id) if guild is not None else None
    return [Dispatch("member_remove", (cached or user,), cached or user)]


def parse_guild_emojis_update(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    before = list(guild.emojis) if guild is not None else []
    after = [Emoji(e, guild, state) for e in data.get("emojis") or [] if isinstance(e, dict)]
    return [Dispatch("guild_emojis_update", (guild, before, after), after)]


def parse_guild_stickers_update(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id"))
    before = list(guild.stickers) if guild is not None else []
    after = [Sticker(s, state) for s in data.get("stickers") or [] if isinstance(s, dict)]
    return [Dispatch("guild_stickers_update", (guild, before, after), after)]


def parse_guild_members_chunk(data: Dict[str, Any], state: Any = None) -> List[Dispatch]:
    guild = _guild_or_stub(state, data.get("guild_id")) or Guild({}, state)
    chunk = GuildMembersChunk(data, guild, state)
    return [Dispatch("guild_members_chunk", (chunk,), chunk)]


Parser = Callable[[Dict[str, Any], Any], List[Dispatch]]

EVENT_PARSERS: Dict[str, Parser] = {
    "READY": parse_ready,
    "RESUMED": parse_resumed,
    "GUILD_CREATE": parse_guild_create,
    "GUILD_UPDATE": parse_guild_update,
    "GUILD_DELETE": parse_guild_delete,
    "CHANNEL_CREATE": parse_channel_create,
    "CHANNEL_UPDATE": parse_channel_update,
    "CHANNEL_DELETE": parse_channel_delete,
    "THREAD_CREATE": parse_thread_create,
    "THREAD_UPDATE": parse_thread_update,
    "THREAD_DELETE": parse_thread_delete,
    "THREAD_LIST_SYNC": parse_thread_list_sync,
    "MESSAGE_CREATE": parse_message_create,
    "MESSAGE_UPDATE": parse_message_update,
    "MESSAGE_DELETE": parse_message_delete,
    "MESSAGE_DELETE_BULK": parse_message_delete_bulk,
    "GUILD_ROLE_CREATE": parse_guild_role_create,
    "GUILD_ROLE_UPDATE": parse_guild_role_update,
    "GUILD_ROLE_DELETE": parse_guild_role_delete,
    "GUILD_MEMBER_ADD": parse_guild_member_add,
    "GUILD_MEMBER_REMOVE": parse_guild_member_remove,
    "GUILD_EMOJIS_UPDATE": parse_guild_emojis_update,
    "GUILD_STICKERS_UPDATE": parse_guild_stickers_update,
    "GUILD_MEMBERS_CHUNK": parse_guild_members_chunk,
}

# The ``on_*`` handlers the client role registers.
CLIENT_HANDLERS = frozenset(
    {
        "ready",
        "resumed",
        "message",
        "message_edit",
        "raw_message_edit",
        "message_delete",
        "raw_message_delete",
        "guild_channel_create",
        "guild_channel_delete",
        "guild_channel_update",
        "thread_delete",
        "thread_update",
        "member_join",
        "guild_role_create",
        "guild_role_delete",
        "guild_role_update",
        "guild_join",
        "guild_remove",
        "guild_update",
        "guild_emojis_update",
        "guild_stickers_update",
    }
)


def parse(event: Optional[str], data: Any, state: Any = None) -> List[Dispatch]:
    """The handler calls one raw dispatch maps to; empty for unknown events."""
    parser = EVENT_PARSERS.get(event or "")
    if parser is None:
        return []
    return parser(data if isinstance(data, dict) else {}, state)


__all__ = [
    "Dispatch",
    "RawMessageUpdateEvent",
    "RawMessageDeleteEvent",
    "RawBulkMessageDeleteEvent",
    "RawThreadDeleteEvent",
    "ReadyEvent",
    "GuildMembersChunk",
    "EVENT_PARSERS",
    "CLIENT_HANDLERS",
    "parse",
]
