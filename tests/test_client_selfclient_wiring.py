"""
code/client/client.py on selfclient: handler wiring and the restart lifecycle.

ClientListener builds a selfclient.Client with every ``on_*`` handler
registered by name. These build that Client the way the listener does, run it
on a fake gateway and feed it canned raw dispatches, so each gateway event is
checked to reach the matching listener method with the argument types the
handler bodies read. The listener is made with ``__new__`` (no config, DB or
sockets) and its handlers are swapped for recorders on a subclass, so what is
under test is the wiring, not the handler bodies.
"""
import asyncio
import types

import pytest

import client.client as cc
from selfclient import Client
from selfclient.events import (
    CLIENT_HANDLERS,
    RawMessageDeleteEvent,
    RawMessageUpdateEvent,
)
from selfclient.models import (
    ForumChannel,
    Message,
    Role,
    TextChannel,
    Thread,
)

TOKEN = "test-token-not-real"
PROXY = "http://proxy.example:8080"

ME = 100000000000000001
FRIEND = 100000000000000002
GUILD = 200000000000000001
GENERAL = 300000000000000001
FORUM = 300000000000000002
THREAD = 300000000000000003
MOD_ROLE = 400000000000000001
MSG = 500000000000000001


def _user(uid, name):
    return {"id": str(uid), "username": name, "discriminator": "0", "avatar": None}


def _text_channel(name="general", topic=None):
    return {
        "id": str(GENERAL),
        "type": 0,
        "name": name,
        "guild_id": str(GUILD),
        "position": 0,
        "topic": topic,
    }


def _thread(name="a thread"):
    return {
        "id": str(THREAD),
        "type": 11,
        "name": name,
        "guild_id": str(GUILD),
        "parent_id": str(FORUM),
        "thread_metadata": {"archived": False},
    }


def _role(name="mod"):
    return {"id": str(MOD_ROLE), "name": name, "permissions": "8", "position": 1}


def _ready():
    return {
        "user": _user(ME, "me"),
        "session_id": "sess-1",
        "resume_gateway_url": "wss://resume.example.gg",
        "guilds": [
            {
                "id": str(GUILD),
                "name": "Source",
                "owner_id": str(FRIEND),
                "channels": [
                    _text_channel(),
                    {
                        "id": str(FORUM),
                        "type": 15,
                        "name": "forum",
                        "guild_id": str(GUILD),
                        "position": 1,
                    },
                ],
                "threads": [_thread()],
                "roles": [
                    {"id": str(GUILD), "name": "@everyone", "permissions": "1024", "position": 0},
                    _role(),
                ],
                "members": [{"user": _user(FRIEND, "friend"), "roles": [str(MOD_ROLE)]}],
                "emojis": [],
                "stickers": [],
            }
        ],
        "users": [_user(FRIEND, "friend")],
        "private_channels": [],
    }


def _message(content="hello"):
    return {
        "id": str(MSG),
        "channel_id": str(GENERAL),
        "guild_id": str(GUILD),
        "author": _user(FRIEND, "friend"),
        "content": content,
        "type": 0,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "attachments": [],
        "embeds": [],
        "mentions": [],
    }


class _FakeGateway:
    """Stands in for GatewayClient: runs until closed, feeds dispatches."""

    def __init__(self, token, **kwargs):
        self.token = token
        self.kwargs = kwargs
        self.proxy = kwargs.get("proxy")
        self.latency = 0.042
        self.handlers = []
        self.closed = asyncio.Event()

    def on_dispatch(self, handler):
        self.handlers.append(handler)

    async def run(self, *, refresh_build=True):
        self.refresh_build = refresh_build
        await self.closed.wait()

    async def close(self):
        self.closed.set()

    async def feed(self, event, data):
        for handler in self.handlers:
            await handler(event, data)


class _FakeHTTP:
    def __init__(self):
        self.token = None
        self.proxy = None
        self.closed = False

    async def login(self):
        return _user(ME, "me")

    async def close(self):
        self.closed = True

    def set_proxy(self, proxy):
        self.proxy = proxy


def _recorder(name):
    async def handler(self, *args):
        self.calls.append((name, args))

    handler.__name__ = name
    return handler


class _Recorder(cc.ClientListener):
    """A listener whose on_* methods only record what they were called with."""


for _name in cc.ClientListener.EVENT_HANDLERS:
    setattr(_Recorder, _name, _recorder(_name))


def _listener(cls=_Recorder):
    listener = cls.__new__(cls)
    listener.calls = []
    for attr in ("msg", "sitemap", "ui_controller", "runner", "backfill"):
        setattr(listener, attr, types.SimpleNamespace(bot=None))
    return listener


@pytest.fixture()
def fake_transport(monkeypatch):
    """Make the listener's Clients run on fake HTTP and a fake gateway."""
    gateways = []
    built = []

    def factory(token, **kwargs):
        gw = _FakeGateway(token, **kwargs)
        gateways.append(gw)
        return gw

    def make_client(**kwargs):
        client = Client(http=_FakeHTTP(), gateway_factory=factory, **kwargs)
        built.append(client)
        return client

    monkeypatch.setattr(cc, "Client", make_client)
    return types.SimpleNamespace(gateways=gateways, built=built)


async def _drain(client):
    for _ in range(10):
        await asyncio.sleep(0)
        pending = list(client._tasks)
        if not pending:
            return
        await asyncio.gather(*pending, return_exceptions=True)


async def _start(listener, transport):
    listener._install_bot(listener._new_bot(PROXY))
    task = asyncio.ensure_future(listener.bot.start(TOKEN))
    for _ in range(50):
        if transport.gateways and transport.gateways[-1].handlers:
            break
        await asyncio.sleep(0)
    gw = transport.gateways[-1]
    await gw.feed("READY", _ready())
    await _drain(listener.bot)
    listener.calls.clear()
    return task, gw


async def _stop(listener, task):
    await listener.bot.close()
    await asyncio.wait_for(task, timeout=2)


def _only(listener, name):
    found = [args for n, args in listener.calls if n == name]
    assert len(found) == 1, (name, listener.calls)
    return found[0]


class TestEveryHandlerIsRegistered:
    def test_the_list_covers_every_on_method(self):
        defined = {n for n in dir(cc.ClientListener) if n.startswith("on_")}
        assert defined == set(cc.ClientListener.EVENT_HANDLERS)

    def test_the_list_matches_the_selfclient_dispatch_table(self):
        # CLIENT_HANDLERS is the set selfclient.events was built for; connect
        # and disconnect come from the Client's socket tracking instead.
        names = {f"on_{n}" for n in CLIENT_HANDLERS} | {"on_connect", "on_disconnect"}
        assert names == set(cc.ClientListener.EVENT_HANDLERS)

    def test_building_the_client_registers_the_real_methods(self):
        listener = cc.ClientListener.__new__(cc.ClientListener)
        bot = listener._new_bot(PROXY)
        assert isinstance(bot, Client)
        for name in cc.ClientListener.EVENT_HANDLERS:
            handler = bot.__dict__[name]
            assert handler.__self__ is listener
            assert handler.__func__ is getattr(cc.ClientListener, name)

    def test_it_is_built_with_the_proxy_and_no_second_build_fetch(self):
        listener = cc.ClientListener.__new__(cc.ClientListener)
        bot = listener._new_bot(PROXY)
        assert bot.proxy == PROXY
        assert bot.refresh_build is False


class TestGatewayEventsReachTheirHandlers:
    @pytest.mark.asyncio
    async def test_ready(self, fake_transport):
        listener = _listener()
        listener._install_bot(listener._new_bot(PROXY))
        task = asyncio.ensure_future(listener.bot.start(TOKEN))
        for _ in range(50):
            if fake_transport.gateways and fake_transport.gateways[0].handlers:
                break
            await asyncio.sleep(0)
        await fake_transport.gateways[0].feed("READY", _ready())
        await _drain(listener.bot)
        assert _only(listener, "on_ready") == ()
        assert listener.bot.is_ready()
        await _stop(listener, task)

    @pytest.mark.asyncio
    async def test_message_create(self, fake_transport):
        listener = _listener()
        task, gw = await _start(listener, fake_transport)
        await gw.feed("MESSAGE_CREATE", _message())
        await _drain(listener.bot)
        (message,) = _only(listener, "on_message")
        assert isinstance(message, Message)
        assert message.id == MSG and message.content == "hello"
        assert isinstance(message.channel, TextChannel)
        assert message.guild.id == GUILD
        await _stop(listener, task)

    @pytest.mark.asyncio
    async def test_message_update(self, fake_transport):
        listener = _listener()
        task, gw = await _start(listener, fake_transport)
        await gw.feed("MESSAGE_CREATE", _message())
        await _drain(listener.bot)
        listener.calls.clear()

        await gw.feed("MESSAGE_UPDATE", _message("edited"))
        await _drain(listener.bot)

        (raw,) = _only(listener, "on_raw_message_edit")
        assert isinstance(raw, RawMessageUpdateEvent)
        assert raw.message_id == MSG and raw.channel_id == GENERAL
        assert raw.guild_id == GUILD
        assert isinstance(raw.data, dict) and raw.data["content"] == "edited"
        before, after = _only(listener, "on_message_edit")
        assert isinstance(before, Message) and isinstance(after, Message)
        assert (before.content, after.content) == ("hello", "edited")
        await _stop(listener, task)

    @pytest.mark.asyncio
    async def test_uncached_message_update_is_raw_only(self, fake_transport):
        listener = _listener()
        task, gw = await _start(listener, fake_transport)
        await gw.feed("MESSAGE_UPDATE", _message("edited"))
        await _drain(listener.bot)
        (raw,) = _only(listener, "on_raw_message_edit")
        assert raw.cached_message is None
        assert raw.data["content"] == "edited"
        assert not [n for n, _ in listener.calls if n == "on_message_edit"]
        await _stop(listener, task)

    @pytest.mark.asyncio
    async def test_message_delete(self, fake_transport):
        listener = _listener()
        task, gw = await _start(listener, fake_transport)
        await gw.feed("MESSAGE_CREATE", _message())
        await _drain(listener.bot)
        listener.calls.clear()

        await gw.feed(
            "MESSAGE_DELETE",
            {"id": str(MSG), "channel_id": str(GENERAL), "guild_id": str(GUILD)},
        )
        await _drain(listener.bot)

        (raw,) = _only(listener, "on_raw_message_delete")
        assert isinstance(raw, RawMessageDeleteEvent)
        assert (raw.message_id, raw.channel_id, raw.guild_id) == (MSG, GENERAL, GUILD)
        assert isinstance(raw.cached_message, Message)
        (message,) = _only(listener, "on_message_delete")
        assert isinstance(message, Message) and message.id == MSG
        await _stop(listener, task)

    @pytest.mark.asyncio
    async def test_thread_update(self, fake_transport):
        listener = _listener()
        task, gw = await _start(listener, fake_transport)
        await gw.feed("THREAD_UPDATE", _thread("renamed"))
        await _drain(listener.bot)
        before, after = _only(listener, "on_thread_update")
        assert isinstance(before, Thread) and isinstance(after, Thread)
        assert (before.name, after.name) == ("a thread", "renamed")
        assert isinstance(after.parent, ForumChannel)
        assert after.guild.id == GUILD
        await _stop(listener, task)

    @pytest.mark.asyncio
    async def test_channel_update(self, fake_transport):
        listener = _listener()
        task, gw = await _start(listener, fake_transport)
        await gw.feed("CHANNEL_UPDATE", _text_channel("renamed", topic="new"))
        await _drain(listener.bot)
        before, after = _only(listener, "on_guild_channel_update")
        assert isinstance(before, TextChannel) and isinstance(after, TextChannel)
        assert (before.name, after.name) == ("general", "renamed")
        assert after.guild.id == GUILD
        await _stop(listener, task)

    @pytest.mark.asyncio
    async def test_guild_role_update(self, fake_transport):
        listener = _listener()
        task, gw = await _start(listener, fake_transport)
        await gw.feed(
            "GUILD_ROLE_UPDATE", {"guild_id": str(GUILD), "role": _role("moderator")}
        )
        await _drain(listener.bot)
        before, after = _only(listener, "on_guild_role_update")
        assert isinstance(before, Role) and isinstance(after, Role)
        assert (before.name, after.name) == ("mod", "moderator")
        assert after.guild.id == GUILD
        await _stop(listener, task)


class TestCloseAndRestart:
    @pytest.mark.asyncio
    async def test_a_closed_client_is_replaced_by_a_fresh_one(self, fake_transport):
        listener = _listener()
        task, _ = await _start(listener, fake_transport)
        old = listener.bot
        await _stop(listener, task)
        assert old.is_closed()

        listener._reset_bot_for_retry()

        new = listener.bot
        assert new is not old
        assert isinstance(new, Client)
        assert not new.is_closed() and not new.is_ready()
        assert new.token is None and new.guilds == []
        assert new.proxy == PROXY
        for name in cc.ClientListener.EVENT_HANDLERS:
            assert new.__dict__[name].__self__ is listener
        for attr in ("msg", "sitemap", "ui_controller", "runner", "backfill"):
            assert getattr(listener, attr).bot is new

    @pytest.mark.asyncio
    async def test_the_fresh_client_starts_and_dispatches(self, fake_transport):
        listener = _listener()
        task, _ = await _start(listener, fake_transport)
        await _stop(listener, task)
        listener._reset_bot_for_retry()

        task = asyncio.ensure_future(listener.bot.start(TOKEN))
        for _ in range(50):
            if len(fake_transport.gateways) == 2 and fake_transport.gateways[1].handlers:
                break
            await asyncio.sleep(0)
        gw = fake_transport.gateways[1]
        assert gw.proxy == PROXY
        await gw.feed("READY", _ready())
        await gw.feed("MESSAGE_CREATE", _message())
        await _drain(listener.bot)
        (message,) = _only(listener, "on_message")
        assert isinstance(message, Message)
        await _stop(listener, task)

    @pytest.mark.asyncio
    async def test_a_spent_open_client_is_closed_and_replaced(self, fake_transport):
        # start() raised (a bad token, a fatal close): the client was never
        # closed and still holds the old token. A retry must not reuse it.
        listener = _listener()
        listener._install_bot(listener._new_bot(PROXY))
        old = listener.bot
        old.token = TOKEN
        listener._reset_bot_for_retry()
        for _ in range(5):
            await asyncio.sleep(0)
        assert listener.bot is not old
        assert old.is_closed()

    @pytest.mark.asyncio
    async def test_an_unused_client_is_kept(self, fake_transport):
        listener = _listener()
        listener._install_bot(listener._new_bot(PROXY))
        first = listener.bot
        listener._reset_bot_for_retry()
        assert listener.bot is first

    @pytest.mark.asyncio
    async def test_a_stalled_connect_is_aborted_onto_a_fresh_client(self, fake_transport):
        listener = _listener()
        listener._install_bot(listener._new_bot(PROXY))
        old = listener.bot
        task = asyncio.ensure_future(old.start(TOKEN))
        for _ in range(50):
            if fake_transport.gateways and fake_transport.gateways[0].handlers:
                break
            await asyncio.sleep(0)

        await listener._abort_stalled_connect(task)

        assert old.is_closed()
        assert listener.bot is not old and not listener.bot.is_closed()
        assert listener.bot.proxy == PROXY
