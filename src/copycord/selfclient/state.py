# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""The Client's caches, kept up to date from parsed gateway dispatches.

``apply(dispatch)`` is the only write path. The Client parses before applying,
so a handler's "before" comes from the cache as it was.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Any, Callable, Dict, Iterable, List, Optional

from .events import Dispatch, GuildMembersChunk, ReadyEvent
from .models import (
    DMChannel,
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
)

log = logging.getLogger("selfclient.state")

# discord.py's default ``max_messages``.
DEFAULT_MAX_MESSAGES = 1000


class ConnectionState:
    """Caches for one user-token session."""

    def __init__(self, *, max_messages: Optional[int] = DEFAULT_MAX_MESSAGES):
        self.max_messages: Optional[int] = max_messages
        self.user: Optional[User] = None
        self.session_id: Optional[str] = None
        # The Client's HTTPClient, reached through a model's ``_state``.
        self.http: Any = None
        self._guilds: Dict[int, Guild] = {}
        self._private_channels: Dict[int, DMChannel] = {}
        self._users: Dict[int, User] = {}
        self._messages: "OrderedDict[int, Message]" = OrderedDict()
        self._appliers: Dict[str, Callable[[Dispatch], None]] = {
            "ready": self._apply_ready,
            "guild_join": self._apply_guild_create,
            "guild_available": self._apply_guild_create,
            "guild_unavailable": self._apply_guild_unavailable,
            "guild_update": self._apply_guild_update,
            "guild_update_uncached": self._apply_guild_update,
            "guild_remove": self._apply_guild_remove,
            "guild_channel_create": self._apply_channel_put,
            "guild_channel_update": self._apply_channel_put,
            "guild_channel_update_uncached": self._apply_channel_put,
            "guild_channel_delete": self._apply_channel_delete,
            "private_channel_create": self._apply_private_put,
            "private_channel_update": self._apply_private_put,
            "private_channel_update_uncached": self._apply_private_put,
            "private_channel_delete": self._apply_private_delete,
            "thread_create": self._apply_channel_put,
            "thread_join": self._apply_channel_put,
            "thread_update": self._apply_channel_put,
            "thread_update_uncached": self._apply_channel_put,
            "thread_delete": self._apply_channel_delete,
            "thread_list_sync": self._apply_thread_list_sync,
            "message": self._apply_message_put,
            "message_edit": self._apply_message_put,
            "raw_message_edit": self._apply_message_put,
            "message_delete": self._apply_message_delete,
            "raw_message_delete": self._apply_raw_message_delete,
            "bulk_message_delete": self._apply_bulk_message_delete,
            "raw_bulk_message_delete": self._apply_raw_bulk_message_delete,
            "guild_role_create": self._apply_role_put,
            "guild_role_update": self._apply_role_put,
            "guild_role_update_uncached": self._apply_role_put,
            "guild_role_delete": self._apply_role_delete,
            "member_join": self._apply_member_put,
            "member_remove": self._apply_member_remove,
            "guild_emojis_update": self._apply_emojis,
            "guild_stickers_update": self._apply_stickers,
            "guild_members_chunk": self._apply_members_chunk,
        }

    # lookups

    @property
    def guilds(self) -> List[Guild]:
        return list(self._guilds.values())

    @property
    def private_channels(self) -> List[DMChannel]:
        return list(self._private_channels.values())

    @property
    def users(self) -> List[User]:
        return list(self._users.values())

    @property
    def cached_messages(self) -> List[Message]:
        return list(self._messages.values())

    def get_guild(self, guild_id: Any) -> Optional[Guild]:
        return self._guilds.get(_int(guild_id) or 0)

    def get_channel(self, channel_id: Any) -> Any:
        """A private channel, guild channel or thread, by id."""
        cid = _int(channel_id) or 0
        if not cid:
            return None
        found = self._private_channels.get(cid)
        if found is not None:
            return found
        for guild in self._guilds.values():
            found = guild.get_channel(cid)
            if found is not None:
                return found
        return None

    def get_user(self, user_id: Any) -> Optional[User]:
        uid = _int(user_id) or 0
        if not uid:
            return None
        if self.user is not None and self.user.id == uid:
            return self.user
        return self._users.get(uid)

    def _get_message(self, message_id: Any) -> Optional[Message]:
        return self._messages.get(_int(message_id) or 0)

    def _get_private_channel_by_user(self, user_id: Any) -> Optional[DMChannel]:
        uid = _int(user_id) or 0
        for channel in self._private_channels.values():
            if type(channel) is DMChannel:
                recipient = channel.recipient
                if recipient is not None and recipient.id == uid:
                    return channel
        return None

    # writes

    def clear(self) -> None:
        """Forget everything."""
        self.user = None
        self.session_id = None
        self._guilds.clear()
        self._private_channels.clear()
        self._users.clear()
        self._messages.clear()

    def apply(self, dispatch: Dispatch) -> None:
        """Fold one parsed dispatch into the caches."""
        applier = self._appliers.get(dispatch.event)
        if applier is None:
            return
        try:
            applier(dispatch)
        except Exception:
            log.exception("cache update failed for %s", dispatch.event)

    def apply_all(self, dispatches: Iterable[Dispatch]) -> None:
        for dispatch in dispatches:
            self.apply(dispatch)

    def store_user(self, user: Optional[User]) -> Optional[User]:
        """Index a user by id. Members are stored as their plain user."""
        if user is None or not user.id:
            return user
        if isinstance(user, Member):
            raw = user.raw.get("user")
            if isinstance(raw, dict):
                self._users[user.id] = User(raw, self)
            return user
        self._users[user.id] = user
        return user

    def store_guild(self, guild: Guild) -> Guild:
        if guild.id:
            self._guilds[guild.id] = guild
            for member in guild.members:
                self.store_user(member)
        return guild

    def store_private_channel(self, channel: DMChannel) -> DMChannel:
        if channel.id:
            self._private_channels[channel.id] = channel
            for user in channel.recipients:
                self.store_user(user)
        return channel

    def store_channel(self, channel: Any) -> Any:
        """Cache a channel or thread on its guild, or as a private channel."""
        if isinstance(channel, DMChannel):
            return self.store_private_channel(channel)
        if not isinstance(channel, GuildChannel) or not channel.id:
            return channel
        guild = self.get_guild(channel.guild_id)
        if guild is None:
            return channel
        if channel.guild is not guild:
            # Rebind to the cached guild.
            channel = type(channel)(channel.raw, guild, self)
        if isinstance(channel, Thread):
            guild._channels.pop(channel.id, None)
            guild._threads[channel.id] = channel
        else:
            guild._threads.pop(channel.id, None)
            guild._channels[channel.id] = channel
        return channel

    def remove_channel(self, channel_id: Any, guild_id: Any = None) -> None:
        cid = _int(channel_id) or 0
        if not cid:
            return
        self._private_channels.pop(cid, None)
        guild = self.get_guild(guild_id)
        targets = [guild] if guild is not None else list(self._guilds.values())
        for g in targets:
            g._remove_channel(cid)

    def store_message(self, message: Message) -> Message:
        if not message.id or self.max_messages == 0:
            return message
        self._messages[message.id] = message
        self._messages.move_to_end(message.id)
        if self.max_messages is not None:
            while len(self._messages) > self.max_messages:
                self._messages.popitem(last=False)
        author = message.raw.get("author")
        if isinstance(author, dict) and author.get("id") and not message.webhook_id:
            self._users[_int(author["id"]) or 0] = User(author, self)
        return message

    # appliers

    def _apply_ready(self, d: Dispatch) -> None:
        ready = d.payload
        if not isinstance(ready, ReadyEvent):
            return
        self.clear()
        self.user = ready.user
        self.session_id = ready.session_id
        for raw in ready.data.get("users") or []:
            if isinstance(raw, dict) and raw.get("id"):
                self.store_user(User(raw, self))
        for guild in ready.guilds:
            self.store_guild(guild)
        for channel in ready.private_channels:
            if isinstance(channel, DMChannel):
                self.store_private_channel(channel)

    def _apply_guild_create(self, d: Dispatch) -> None:
        if isinstance(d.payload, Guild):
            self.store_guild(d.payload)

    def _apply_guild_unavailable(self, d: Dispatch) -> None:
        # An outage: keep the cached guild if there is one.
        guild = d.payload
        if isinstance(guild, Guild) and guild.id and guild.id not in self._guilds:
            self._guilds[guild.id] = guild

    def _apply_guild_update(self, d: Dispatch) -> None:
        after = d.payload
        if not isinstance(after, Guild) or not after.id:
            return
        before = self._guilds.get(after.id)
        if before is not None:
            # GUILD_UPDATE has no channels, threads or members, and the merged
            # raw ones may be stale, so carry the live ones over.
            after._channels.clear()
            after._threads.clear()
            for ch in before.channels + before.threads:
                after._add_channel(ch.raw)
            after._members.clear()
            for member in before.members:
                after._add_member(Member(member.raw, after, self))
            # A list the update did not send is still before.raw's, which
            # misses later role/emoji/sticker events; use the live copy.
            if after.raw.get("roles") is before.raw.get("roles"):
                after._roles = {r.id: Role(r.raw, after, self) for r in before.roles}
            if after.raw.get("emojis") is before.raw.get("emojis"):
                after._emojis = [Emoji(e.raw, after, self) for e in before.emojis]
            if after.raw.get("stickers") is before.raw.get("stickers"):
                after._stickers = list(before.stickers)
        self._guilds[after.id] = after

    def _apply_guild_remove(self, d: Dispatch) -> None:
        guild = d.payload
        gid = getattr(guild, "id", None)
        if gid:
            self._guilds.pop(gid, None)

    def _apply_channel_put(self, d: Dispatch) -> None:
        if d.payload is not None:
            self.store_channel(d.payload)

    def _apply_channel_delete(self, d: Dispatch) -> None:
        channel = d.payload
        if channel is not None:
            self.remove_channel(channel.id, getattr(channel, "guild_id", None))

    def _apply_private_put(self, d: Dispatch) -> None:
        if isinstance(d.payload, DMChannel):
            self.store_private_channel(d.payload)

    def _apply_private_delete(self, d: Dispatch) -> None:
        channel = d.payload
        if channel is not None:
            self._private_channels.pop(channel.id, None)

    def _apply_thread_list_sync(self, d: Dispatch) -> None:
        for thread in d.payload or []:
            self.store_channel(thread)

    def _apply_message_put(self, d: Dispatch) -> None:
        if isinstance(d.payload, Message):
            self.store_message(d.payload)

    def _apply_message_delete(self, d: Dispatch) -> None:
        message = d.payload
        if message is not None:
            self._messages.pop(message.id, None)

    def _apply_raw_message_delete(self, d: Dispatch) -> None:
        raw = d.args[0] if d.args else None
        mid = getattr(raw, "message_id", None)
        if mid:
            self._messages.pop(mid, None)

    def _apply_bulk_message_delete(self, d: Dispatch) -> None:
        for message in d.payload or []:
            self._messages.pop(message.id, None)

    def _apply_raw_bulk_message_delete(self, d: Dispatch) -> None:
        raw = d.args[0] if d.args else None
        for mid in getattr(raw, "message_ids", None) or ():
            self._messages.pop(mid, None)

    def _cached_guild_of(self, obj: Any) -> Optional[Guild]:
        return self.get_guild(getattr(getattr(obj, "guild", None), "id", None))

    def _apply_role_put(self, d: Dispatch) -> None:
        role = d.payload
        guild = self._cached_guild_of(role)
        if guild is None or not getattr(role, "id", None):
            return
        if role.guild is not guild:
            role = Role(role.raw, guild, self)
        guild._roles[role.id] = role

    def _apply_role_delete(self, d: Dispatch) -> None:
        role = d.payload
        guild = self._cached_guild_of(role)
        if guild is not None and role is not None:
            guild._roles.pop(role.id, None)

    def _apply_member_put(self, d: Dispatch) -> None:
        member = d.payload
        guild = self._cached_guild_of(member)
        if guild is None or not isinstance(member, Member):
            return
        if member.guild is not guild:
            member = Member(member.raw, guild, self)
        guild._add_member(member)
        self.store_user(member)

    def _apply_member_remove(self, d: Dispatch) -> None:
        member = d.payload
        if isinstance(member, Member):
            guild = self._cached_guild_of(member)
            if guild is not None:
                guild._members.pop(member.id, None)

    def _apply_emojis(self, d: Dispatch) -> None:
        guild = self.get_guild(getattr(d.args[0], "id", None)) if d.args else None
        if guild is None:
            return
        guild._emojis = [Emoji(e.raw, guild, self) for e in d.payload or []]

    def _apply_stickers(self, d: Dispatch) -> None:
        guild = self.get_guild(getattr(d.args[0], "id", None)) if d.args else None
        if guild is None:
            return
        guild._stickers = [Sticker(s.raw, self) for s in d.payload or []]

    def _apply_members_chunk(self, d: Dispatch) -> None:
        chunk = d.payload
        if not isinstance(chunk, GuildMembersChunk):
            return
        guild = self.get_guild(chunk.guild_id)
        if guild is None:
            return
        for member in chunk.members:
            if member.guild is not guild:
                member = Member(member.raw, guild, self)
            guild._add_member(member)
            self.store_user(member)


__all__ = ["ConnectionState", "DEFAULT_MAX_MESSAGES"]
