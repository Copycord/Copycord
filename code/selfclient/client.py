# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""``Client``: the gateway, REST and caches behind one discord.py-shaped object.

This is the object code/client will hold as ``self.bot`` once it drops
discord.py-self. The surface is the one that code reaches for today:

  * lifecycle: ``start(token)``, ``login``, ``connect``, ``close``, ``run``,
    ``clear``, ``is_closed()``, ``is_ready()``, ``wait_until_ready()``,
    ``latency``, ``user``, ``loop``, ``http`` (whose ``proxy`` it assigns);
  * events: ``bot.event(self.on_message)`` sets the handler by name, as
    discord.py does, ``add_listener``/``listen`` add extra ones, and every
    gateway dispatch is run through selfclient.events and then the state
    before ``on_<event>(*args)`` is scheduled. ``on_connect`` fires when a
    socket opens and ``on_disconnect`` when it closes;
  * caches: ``guilds``, ``private_channels``, ``users``, ``get_guild``,
    ``get_channel``, ``get_user``, kept by selfclient.state;
  * REST that lives on library objects today (``channel.history``,
    ``parent.archived_threads``, ``guild.fetch_member``, ``user.send`` ...)
    as Client methods taking the object or its id, returning model wrappers.

Nothing in code/client imports this yet.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import inspect
import logging
import uuid
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

from . import events
from .errors import ClientException, NotFound
from .gateway import GatewayClient, open_websocket
from .http import HTTPClient
from .models import (
    DMChannel,
    Emoji,
    Guild,
    GuildChannel,
    Member,
    Message,
    Role,
    Sticker,
    User,
    _int,
    channel_from_dict,
    time_snowflake,
)
from .state import DEFAULT_MAX_MESSAGES, ConnectionState

log = logging.getLogger(__name__)

Coro = Callable[..., Any]

# How long fetch_members waits for its GUILD_MEMBERS_CHUNKs.
MEMBER_CHUNK_TIMEOUT = 30.0


def _snowflake(value: Any) -> Optional[int]:
    """An id, an object with ``.id``, or None."""
    if value is None:
        return None
    return _int(getattr(value, "id", value))


def _cursor(value: Any, *, high: bool = False) -> Optional[int]:
    """A history bound: a datetime becomes the snowflake minted at it."""
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_dt.timezone.utc)
        return time_snowflake(value, high=high)
    return _snowflake(value)


class _TrackedSocket:
    """A gateway socket that reports, once, when it stops being one.

    The gateway always closes the socket on its way out of a connection, and
    a server-side close ends the read loop; either is a disconnect.
    """

    def __init__(self, ws: Any, on_end: Callable[[], None]):
        self._ws = ws
        self._on_end = on_end
        self._ended = False

    def _end(self) -> None:
        if not self._ended:
            self._ended = True
            self._on_end()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ws, name)

    async def __aiter__(self):
        try:
            async for raw in self._ws:
                yield raw
        finally:
            self._end()

    async def send(self, data: Any) -> Any:
        return await self._ws.send(data)

    async def close(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return await self._ws.close(*args, **kwargs)
        finally:
            self._end()


class Client:
    """A user-token Discord client: GatewayClient + HTTPClient + ConnectionState.

    ``http``, ``gateway_factory`` and ``connector`` exist for tests and for
    callers that already hold a primed session; the defaults are the real
    transports. ``command_prefix`` and ``self_bot``, which code/client passes
    to ``commands.Bot`` today, are accepted and ignored.
    """

    def __init__(
        self,
        *,
        proxy: Optional[str] = None,
        proxy_pool: Any = None,
        max_messages: Optional[int] = DEFAULT_MAX_MESSAGES,
        status: str = "online",
        custom_status: Optional[str] = None,
        activity: Optional[Dict[str, Any]] = None,
        zlib_stream: bool = True,
        refresh_build: bool = True,
        http: Optional[HTTPClient] = None,
        gateway_factory: Optional[Callable[..., Any]] = None,
        connector: Optional[Callable[..., Any]] = None,
        **options: Any,
    ):
        for name in options:
            if name not in ("command_prefix", "self_bot", "intents"):
                log.debug("Client ignoring unknown option %r", name)
        self.loop: Any = None
        self.token: Optional[str] = None
        self.proxy = proxy
        self.proxy_pool = proxy_pool
        self.status = status
        self.custom_status = custom_status
        self.activity = activity
        self.zlib_stream = zlib_stream
        self.refresh_build = refresh_build
        self.http: Optional[HTTPClient] = http
        self.gateway: Any = None
        self._gateway_factory = gateway_factory or GatewayClient
        self._connector = connector
        self._connection = ConnectionState(max_messages=max_messages)
        self._connection.http = http
        self._ready = asyncio.Event()
        self._closed = False
        self._listeners: Dict[str, List[Coro]] = {}
        self._tasks: set = set()
        self._chunk_requests: Dict[str, Tuple[List[Member], asyncio.Future]] = {}

    # ------------------------------------------------------------------
    # state passthrough
    # ------------------------------------------------------------------

    @property
    def state(self) -> ConnectionState:
        return self._connection

    @property
    def user(self) -> Optional[User]:
        """The logged-in account."""
        return self._connection.user

    @property
    def guilds(self) -> List[Guild]:
        return self._connection.guilds

    @property
    def private_channels(self) -> List[DMChannel]:
        return self._connection.private_channels

    @property
    def users(self) -> List[User]:
        return self._connection.users

    @property
    def cached_messages(self) -> List[Message]:
        return self._connection.cached_messages

    def get_guild(self, guild_id: Any) -> Optional[Guild]:
        return self._connection.get_guild(guild_id)

    def get_channel(self, channel_id: Any) -> Any:
        return self._connection.get_channel(channel_id)

    def get_user(self, user_id: Any) -> Optional[User]:
        return self._connection.get_user(user_id)

    @property
    def ws(self) -> Any:
        return self.gateway

    @property
    def latency(self) -> float:
        """Seconds between the last heartbeat and its ack; inf before one."""
        return getattr(self.gateway, "latency", float("inf"))

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def is_closed(self) -> bool:
        return self._closed

    def is_ready(self) -> bool:
        return self._ready.is_set()

    async def wait_until_ready(self) -> None:
        await self._ready.wait()

    async def login(self, token: str) -> None:
        """Check the token over REST and learn who it belongs to."""
        self.loop = asyncio.get_running_loop()
        token = (token or "").strip()
        if not token:
            raise ClientException("login() needs a token")
        self.token = token
        if self.http is None:
            self.http = HTTPClient(token, proxy=self.proxy)
        elif not self.http.token:
            self.http.token = token
        self._connection.http = self.http
        data = await self.http.login()
        self._connection.user = User(data, self._connection)

    def _make_gateway(self, reconnect: bool) -> Any:
        gateway = self._gateway_factory(
            self.token,
            status=self.status,
            custom_status=self.custom_status,
            activity=self.activity,
            reconnect=reconnect,
            zlib_stream=self.zlib_stream,
            proxy=self.proxy,
            proxy_pool=self.proxy_pool,
            connector=self._tracked_connector,
        )
        gateway.on_dispatch(self._on_gateway_dispatch)
        return gateway

    async def _tracked_connector(self, url: str, headers: Dict[str, str], proxy: Optional[str]) -> Any:
        if self._connector is not None:
            ws = await self._connector(url, headers, proxy)
        else:
            ws = await open_websocket(url, headers, proxy)
        self.dispatch("connect")
        return _TrackedSocket(ws, lambda: self.dispatch("disconnect"))

    async def connect(self, *, reconnect: bool = True) -> None:
        """Run the gateway until close(), or until Discord ends it for good.

        Raises LoginFailure on close code 4004 and ConnectionClosed on the
        other codes no reconnect can fix, as the gateway does.
        """
        if self.token is None:
            raise ClientException("login() must be called before connect()")
        self.loop = asyncio.get_running_loop()
        self.gateway = self._make_gateway(reconnect)
        await self.gateway.run(refresh_build=self.refresh_build)

    async def start(self, token: str, *, reconnect: bool = True) -> None:
        await self.login(token)
        await self.connect(reconnect=reconnect)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for _, future in self._chunk_requests.values():
            if not future.done():
                future.cancel()
        self._chunk_requests.clear()
        if self.gateway is not None:
            try:
                await self.gateway.close()
            except Exception:
                log.debug("gateway close failed", exc_info=True)
        if self.http is not None:
            try:
                await self.http.close()
            except Exception:
                log.debug("http close failed", exc_info=True)
        self._ready.clear()

    def clear(self) -> None:
        """Reset a closed client so start() can be called on it again."""
        self._closed = False
        self._ready.clear()
        self._connection.clear()
        self.gateway = None

    def run(self, token: str, *, reconnect: bool = True) -> None:
        """Blocking start(): owns the event loop until the client stops."""

        async def runner() -> None:
            async with self:
                await self.start(token, reconnect=reconnect)

        try:
            asyncio.run(runner())
        except KeyboardInterrupt:
            return

    async def __aenter__(self) -> "Client":
        self.loop = asyncio.get_running_loop()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if not self.is_closed():
            await self.close()

    def set_proxy(self, proxy: Optional[str]) -> None:
        """Use ``proxy`` for REST now and for the next gateway socket."""
        self.proxy = proxy or None
        if self.http is not None:
            self.http.set_proxy(self.proxy)
        if self.gateway is not None:
            self.gateway.proxy = self.proxy

    async def change_presence(
        self,
        *,
        status: Optional[str] = None,
        custom_status: Optional[str] = None,
        activity: Optional[Dict[str, Any]] = None,
    ) -> None:
        if status is not None:
            self.status = status
        if custom_status is not None:
            self.custom_status = custom_status or None
        if activity is not None:
            self.activity = activity or None
        if self.gateway is not None:
            await self.gateway.change_presence(
                status=status, custom_status=custom_status, activity=activity
            )

    # ------------------------------------------------------------------
    # events
    # ------------------------------------------------------------------

    def event(self, coro: Coro) -> Coro:
        """Install ``coro`` as the handler named by its ``__name__``.

        Works as a decorator and as ``client.event(self.on_message)``; one
        handler per name, a second replaces the first, as in discord.py.
        """
        if not inspect.iscoroutinefunction(coro):
            raise TypeError("event registered must be a coroutine function")
        setattr(self, coro.__name__, coro)
        log.debug("%s has been registered as an event", coro.__name__)
        return coro

    def add_listener(self, func: Coro, name: Optional[str] = None) -> None:
        """Add an extra handler; any number may share one event."""
        name = name or func.__name__
        if not inspect.iscoroutinefunction(func):
            raise TypeError("listeners must be coroutines")
        if not name.startswith("on_"):
            name = f"on_{name}"
        self._listeners.setdefault(name, []).append(func)

    def remove_listener(self, func: Coro, name: Optional[str] = None) -> None:
        name = name or func.__name__
        if not name.startswith("on_"):
            name = f"on_{name}"
        try:
            self._listeners.get(name, []).remove(func)
        except ValueError:
            pass

    def listen(self, name: Optional[str] = None) -> Callable[[Coro], Coro]:
        def decorator(func: Coro) -> Coro:
            self.add_listener(func, name)
            return func

        return decorator

    def dispatch(self, event: str, *args: Any) -> None:
        """Schedule ``on_<event>(*args)`` and its listeners as tasks."""
        method = f"on_{event}"
        for listener in list(self._listeners.get(method, [])):
            self._schedule_event(listener, method, *args)
        handler = getattr(self, method, None)
        if handler is not None and callable(handler):
            self._schedule_event(handler, method, *args)

    def _schedule_event(self, coro: Coro, name: str, *args: Any) -> None:
        task = asyncio.ensure_future(self._run_event(coro, name, *args))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_event(self, coro: Coro, name: str, *args: Any) -> None:
        try:
            await coro(*args)
        except asyncio.CancelledError:
            pass
        except Exception:
            try:
                await self.on_error(name, *args)
            except asyncio.CancelledError:
                pass

    async def on_error(self, event_method: str, *args: Any) -> None:
        log.exception("Ignoring exception in %s", event_method)

    async def _on_gateway_dispatch(self, event: str, data: Any) -> None:
        """Parse against the old cache, update the cache, then fire handlers."""
        dispatches = events.parse(event, data, self._connection)
        self._connection.apply_all(dispatches)
        for d in dispatches:
            if d.event == "ready":
                self._ready.set()
            elif d.event == "guild_members_chunk":
                self._resolve_chunk(d.payload)
            self.dispatch(d.event, *d.args)

    # ------------------------------------------------------------------
    # REST, returning model wrappers
    # ------------------------------------------------------------------

    def _http(self) -> HTTPClient:
        if self.http is None:
            raise ClientException("login() must be called before REST calls")
        return self.http

    def _guild_for(self, guild_id: Any) -> Optional[Guild]:
        gid = _snowflake(guild_id)
        if not gid:
            return None
        return self.get_guild(gid) or Guild({"id": str(gid)}, self._connection)

    def _channel_guild_id(self, channel: Any) -> Optional[int]:
        gid = _int(getattr(channel, "guild_id", None))
        if gid:
            return gid
        cached = self.get_channel(_snowflake(channel))
        return _int(getattr(cached, "guild_id", None))

    def _channel_obj(self, channel: Any) -> Any:
        if isinstance(channel, (GuildChannel, DMChannel)):
            return channel
        return self.get_channel(_snowflake(channel))

    def _wrap_channel(self, data: Dict[str, Any]) -> Any:
        return channel_from_dict(data, self._guild_for(data.get("guild_id")), self._connection)

    def _wrap_message(self, data: Dict[str, Any], channel: Any = None) -> Message:
        guild = self._guild_for(data.get("guild_id") or getattr(channel, "guild_id", None))
        return Message(data, self._connection, guild=guild, channel=channel)

    async def fetch_user(self, user_id: Any) -> User:
        data = await self._http().get_user(_snowflake(user_id))
        return self._connection.store_user(User(data, self._connection))

    async def fetch_user_profile(self, user_id: Any, *, guild: Any = None) -> Dict[str, Any]:
        """The raw profile payload; there is no wrapper for it yet."""
        return await self._http().get_user_profile(_snowflake(user_id), guild_id=_snowflake(guild))

    async def fetch_channel(self, channel_id: Any) -> Any:
        """A channel or thread over REST; not cached, as in discord.py."""
        data = await self._http().get_channel(_snowflake(channel_id))
        return self._wrap_channel(data)

    async def fetch_guild(self, guild_id: Any, *, with_counts: bool = True) -> Guild:
        data = await self._http().get_guild(_snowflake(guild_id), with_counts=with_counts)
        return Guild(data, self._connection)

    async def fetch_guild_channels(self, guild: Any) -> List[Any]:
        gid = _snowflake(guild)
        owner = self._guild_for(gid)
        data = await self._http().get_guild_channels(gid)
        return [channel_from_dict(c, owner, self._connection) for c in data or [] if isinstance(c, dict)]

    async def fetch_roles(self, guild: Any) -> List[Role]:
        gid = _snowflake(guild)
        owner = self._guild_for(gid)
        data = await self._http().get_roles(gid)
        return [Role(r, owner, self._connection) for r in data or [] if isinstance(r, dict)]

    async def fetch_emojis(self, guild: Any) -> List[Emoji]:
        gid = _snowflake(guild)
        owner = self._guild_for(gid)
        data = await self._http().get_emojis(gid)
        return [Emoji(e, owner, self._connection) for e in data or [] if isinstance(e, dict)]

    async def fetch_stickers(self, guild: Any) -> List[Sticker]:
        data = await self._http().get_stickers(_snowflake(guild))
        return [Sticker(s, self._connection) for s in data or [] if isinstance(s, dict)]

    async def fetch_member(self, guild: Any, user_id: Any) -> Member:
        gid = _snowflake(guild)
        data = await self._http().get_member(gid, _snowflake(user_id))
        return Member(data, self._guild_for(gid), self._connection)

    async def fetch_members(
        self,
        guild: Any,
        *,
        user_ids: Optional[List[Any]] = None,
        query: str = "",
        limit: int = 0,
        presences: bool = False,
        timeout: float = MEMBER_CHUNK_TIMEOUT,
    ) -> List[Member]:
        """Members over the gateway (op 8); user tokens get no REST list.

        The answer is every GUILD_MEMBERS_CHUNK carrying this request's nonce.
        """
        if self.gateway is None:
            raise ClientException("fetch_members needs a connected gateway")
        nonce = uuid.uuid4().hex
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._chunk_requests[nonce] = ([], future)
        try:
            await self.gateway.request_guild_members(
                _snowflake(guild),
                query=query,
                limit=limit,
                presences=presences,
                user_ids=[_snowflake(u) for u in user_ids] if user_ids is not None else None,
                nonce=nonce,
            )
            return await asyncio.wait_for(future, timeout)
        finally:
            self._chunk_requests.pop(nonce, None)

    def _resolve_chunk(self, chunk: Any) -> None:
        entry = self._chunk_requests.get(getattr(chunk, "nonce", None) or "")
        if entry is None:
            return
        members, future = entry
        members.extend(chunk.members)
        if chunk.chunk_index >= chunk.chunk_count - 1 and not future.done():
            future.set_result(members)

    async def fetch_message(self, channel: Any, message_id: Any) -> Message:
        cid = _snowflake(channel)
        mid = _snowflake(message_id)
        data = await self._http().get_message(cid, mid, guild_id=self._channel_guild_id(channel))
        if data is None:
            raise NotFound(404, None, method="GET", path=f"/channels/{cid}/messages/{mid}")
        return self._wrap_message(data, self._channel_obj(channel))

    async def history(
        self,
        channel: Any,
        *,
        limit: Optional[int] = 100,
        before: Any = None,
        after: Any = None,
        oldest_first: Optional[bool] = None,
        raise_on_error: bool = False,
    ) -> AsyncIterator[Message]:
        """``channel.history(...)``: newest first unless ``oldest_first``.

        ``before``/``after`` take a message, an id or a datetime. With
        ``after`` set the default order is oldest first, as in discord.py.
        A bounded oldest-first walk pages forward from ``after`` (or from the
        start of the channel), so ``limit`` picks the oldest messages, as in
        discord.py. ``raise_on_error`` raises the HTTPException a refused page
        came back with instead of ending the walk quietly.
        """
        cid = _snowflake(channel)
        obj = self._channel_obj(channel)
        if oldest_first is None:
            oldest_first = after is not None
        if oldest_first and limit is not None:
            async for raw in self._http().messages_after(
                cid,
                limit=limit,
                after_id=_cursor(after, high=True),
                before_id=_cursor(before),
                guild_id=self._channel_guild_id(channel),
                raise_on_error=raise_on_error,
            ):
                yield self._wrap_message(raw, obj)
            return
        walk = self._http().messages(
            cid,
            limit=limit,
            before=_cursor(before),
            after_id=_cursor(after, high=True),
            guild_id=self._channel_guild_id(channel),
            raise_on_error=raise_on_error,
        )
        if not oldest_first:
            async for raw in walk:
                yield self._wrap_message(raw, obj)
            return
        # The API pages newest first; the oldest-first order needs the lot.
        collected = [raw async for raw in walk]
        for raw in reversed(collected):
            yield self._wrap_message(raw, obj)

    async def archived_threads(
        self,
        channel: Any,
        *,
        private: bool = False,
        joined: bool = False,
        limit: Optional[int] = 100,
        before: Any = None,
        raise_on_error: bool = False,
    ) -> AsyncIterator[Any]:
        """``parent.archived_threads(...)``: Thread wrappers, newest archive first."""
        cid = _snowflake(channel)
        gid = self._channel_guild_id(channel)
        owner = self._guild_for(gid)
        async for raw in self._http().archived_threads(
            cid,
            private=private,
            joined=joined,
            limit=limit,
            before=before,
            guild_id=gid,
            raise_on_error=raise_on_error,
        ):
            yield channel_from_dict(raw, owner, self._connection)

    async def fetch_private_channels(self) -> List[DMChannel]:
        data = await self._http().get_private_channels()
        out: List[DMChannel] = []
        for raw in data or []:
            channel = channel_from_dict(raw, None, self._connection) if isinstance(raw, dict) else None
            if isinstance(channel, DMChannel):
                out.append(self._connection.store_private_channel(channel))
        return out

    async def create_dm(self, user: Any) -> DMChannel:
        """The DM channel with ``user``, opening it if it is not cached."""
        uid = _snowflake(user)
        found = self._connection._get_private_channel_by_user(uid)
        if found is not None:
            return found
        data = await self._http().start_private_message(uid)
        channel = channel_from_dict(data, None, self._connection)
        if isinstance(channel, DMChannel):
            self._connection.store_private_channel(channel)
        return channel

    async def send_message(self, channel: Any, content: str = "", **fields: Any) -> Message:
        """POST a message; ``fields`` go into the body beside ``content``."""
        cid = _snowflake(channel)
        data = await self._http().send_message(
            cid, content, guild_id=self._channel_guild_id(channel), extra=fields or None
        )
        return self._wrap_message(data, self._channel_obj(channel))

    async def send_dm(self, user: Any, content: str = "", **fields: Any) -> Message:
        """``user.send(...)``: open the DM if needed and post into it."""
        dm = await self.create_dm(user)
        return await self.send_message(dm, content, **fields)


__all__ = ["Client", "MEMBER_CHUNK_TIMEOUT"]
