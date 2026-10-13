"""selfclient.client: lifecycle, event dispatch, caches and REST helpers.

Gateway and HTTPClient are fakes with canned data. Handlers run as tasks,
so each test drains the client's pending tasks before asserting.
"""
import asyncio
import contextlib

import pytest

from copycord.selfclient import Client
from copycord.selfclient.client import _TrackedSocket
from copycord.selfclient.errors import NotFound
from copycord.selfclient.models import (
    DMChannel,
    Guild,
    Member,
    Message,
    Role,
    TextChannel,
    Thread,
    User,
)

TOKEN = "test-token-not-real"

ME = 100000000000000001
FRIEND = 100000000000000002
STRANGER = 100000000000000003
GUILD = 200000000000000001
OTHER_GUILD = 200000000000000002
GENERAL = 300000000000000001
NEWS = 300000000000000002
NEW_CHANNEL = 300000000000000003
OTHER_GENERAL = 300000000000000004
THREAD = 300000000000000005
DM = 300000000000000006
MOD_ROLE = 400000000000000001
NEW_ROLE = 400000000000000002
MSG = 500000000000000001
MSG2 = 500000000000000002
MSG3 = 500000000000000003


def user(uid, name):
    return {"id": str(uid), "username": name, "discriminator": "0", "avatar": None}


def text_channel(cid, name, guild_id=GUILD, position=0):
    return {"id": str(cid), "type": 0, "name": name, "guild_id": str(guild_id), "position": position}


def guild_payload(gid=GUILD, name="Source", channels=None, roles=None):
    return {
        "id": str(gid),
        "name": name,
        "owner_id": str(FRIEND),
        "channels": channels
        if channels is not None
        else [text_channel(GENERAL, "general"), text_channel(NEWS, "news", position=1)],
        "threads": [],
        "roles": roles
        if roles is not None
        else [
            {"id": str(gid), "name": "@everyone", "permissions": "1024", "position": 0},
            {"id": str(MOD_ROLE), "name": "mod", "permissions": "8", "position": 1},
        ],
        "members": [{"user": user(FRIEND, "friend"), "roles": [str(MOD_ROLE)]}],
        "emojis": [],
        "stickers": [],
    }


def ready_payload():
    return {
        "user": user(ME, "me"),
        "session_id": "sess-1",
        "resume_gateway_url": "wss://resume.example.gg",
        "guilds": [guild_payload()],
        "users": [user(FRIEND, "friend")],
        "private_channels": [{"id": str(DM), "type": 1, "recipient_ids": [str(FRIEND)]}],
    }


def message_payload(mid=MSG, content="hello", channel_id=GENERAL, guild_id=GUILD, author=FRIEND):
    return {
        "id": str(mid),
        "channel_id": str(channel_id),
        "guild_id": str(guild_id) if guild_id else None,
        "author": user(author, "friend"),
        "content": content,
        "type": 0,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "attachments": [],
        "embeds": [],
        "mentions": [],
    }


class FakeGateway:
    """Stands in for GatewayClient: records construction, feeds dispatches."""

    def __init__(self, token, **kwargs):
        self.token = token
        self.kwargs = kwargs
        self.proxy = kwargs.get("proxy")
        self.latency = 0.042
        self.handlers = []
        self.closed = asyncio.Event()
        self.member_requests = []

    def on_dispatch(self, handler):
        self.handlers.append(handler)

    async def run(self, *, refresh_build=True):
        await self.closed.wait()

    async def close(self):
        self.closed.set()

    async def feed(self, event, data):
        for handler in self.handlers:
            await handler(event, data)

    async def request_guild_members(self, guild_id, **kwargs):
        self.member_requests.append((guild_id, kwargs))


class FakeHTTP:
    """Stands in for HTTPClient: canned payloads, records calls."""

    def __init__(self):
        self.token = None
        self.proxy = None
        self.calls = []
        self.closed = False
        self.pages = []

    async def login(self):
        self.calls.append(("login",))
        return user(ME, "me")

    async def close(self):
        self.closed = True

    def set_proxy(self, proxy):
        self.proxy = proxy

    async def get_user(self, user_id):
        self.calls.append(("get_user", user_id))
        return user(user_id, "stranger")

    async def get_channel(self, channel_id):
        self.calls.append(("get_channel", channel_id))
        return text_channel(channel_id, "fetched")

    async def get_member(self, guild_id, user_id):
        self.calls.append(("get_member", guild_id, user_id))
        return {"user": user(user_id, "stranger"), "roles": [str(MOD_ROLE)], "nick": "nick"}

    async def get_roles(self, guild_id):
        return [{"id": str(MOD_ROLE), "name": "mod", "permissions": "8", "position": 1}]

    async def get_message(self, channel_id, message_id, *, guild_id=None):
        self.calls.append(("get_message", channel_id, message_id, guild_id))
        return None

    async def messages(self, channel_id, *, limit=None, before=None, after_id=None, guild_id=None,
                       raise_on_error=False):
        self.calls.append(("messages", channel_id, limit, before, after_id, guild_id))
        for raw in self.pages:
            yield raw

    async def archived_threads(self, channel_id, *, private=False, joined=False, limit=None,
                               before=None, guild_id=None, raise_on_error=False):
        self.calls.append(("archived_threads", channel_id, private, joined, guild_id))
        yield {
            "id": str(THREAD),
            "type": 11,
            "name": "old thread",
            "guild_id": str(GUILD),
            "parent_id": str(channel_id),
            "thread_metadata": {"archived": True, "archive_timestamp": "2026-01-01T00:00:00+00:00"},
        }

    async def start_private_message(self, user_id, *, context_location=None):
        self.calls.append(("start_private_message", user_id))
        return {"id": "300000000000000099", "type": 1, "recipients": [user(user_id, "stranger")]}

    async def send_message(self, channel_id, content="", *, guild_id=None, extra=None):
        self.calls.append(("send_message", channel_id, content, guild_id, extra))
        return message_payload(MSG3, content, channel_id=channel_id, guild_id=guild_id, author=ME)


async def drain(client):
    """Let every scheduled handler task finish."""
    for _ in range(10):
        await asyncio.sleep(0)
        pending = list(client._tasks)
        if not pending:
            return
        await asyncio.gather(*pending, return_exceptions=True)


@contextlib.asynccontextmanager
async def running(**kwargs):
    """A Client that has logged in and is running on a FakeGateway."""
    fake_http = FakeHTTP()
    gateways = []

    def factory(token, **gw_kwargs):
        gw = FakeGateway(token, **gw_kwargs)
        gateways.append(gw)
        return gw

    client = Client(
        http=fake_http, gateway_factory=factory, proxy="http://proxy.example:8080", **kwargs
    )
    task = asyncio.ensure_future(client.start(TOKEN))
    try:
        for _ in range(50):
            if gateways and gateways[0].handlers:
                break
            await asyncio.sleep(0)
        yield client, gateways[0], fake_http
    finally:
        await client.close()
        await asyncio.wait_for(task, timeout=2)


async def ready(client, gw):
    await gw.feed("READY", ready_payload())
    await drain(client)


# ---------------------------------------------------------------------------
# lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_logs_in_then_builds_gateway(self):
        async with running() as (client, gw, fake_http):
            assert fake_http.calls[0] == ("login",)
            assert fake_http.token == TOKEN
            assert isinstance(client.user, User) and client.user.id == ME
            assert gw.token == TOKEN
            assert gw.kwargs["proxy"] == "http://proxy.example:8080"
            assert client.loop is asyncio.get_running_loop()
            assert client.latency == pytest.approx(0.042)
            assert not client.is_ready()
            assert not client.is_closed()

    @pytest.mark.asyncio
    async def test_ready_sets_ready_and_close_stops(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            assert client.is_ready()
            await asyncio.wait_for(client.wait_until_ready(), timeout=1)
            await client.close()
            assert client.is_closed()
            assert gw.closed.is_set()
            assert fake_http.closed
            assert not client.is_ready()

    @pytest.mark.asyncio
    async def test_clear_resets_a_closed_client(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            await client.close()
            client.clear()
            assert not client.is_closed()
            assert client.guilds == []
            assert client.user is None

    @pytest.mark.asyncio
    async def test_set_proxy_reaches_http_and_gateway(self):
        async with running() as (client, gw, fake_http):
            client.set_proxy("http://other.example:3128")
            assert fake_http.proxy == "http://other.example:3128"
            assert gw.proxy == "http://other.example:3128"

    def test_bot_constructor_options_are_accepted(self):
        Client(command_prefix="!", self_bot=True, proxy=None)


# ---------------------------------------------------------------------------
# event registration
# ---------------------------------------------------------------------------


class TestEvents:
    def test_event_requires_a_coroutine(self):
        client = Client()
        with pytest.raises(TypeError):
            client.event(lambda: None)

    @pytest.mark.asyncio
    async def test_bound_method_registered_like_code_client_does(self):
        async with running() as (client, gw, fake_http):
            seen = []

            class Listener:
                async def on_message(self, message):
                    seen.append(message)

                async def on_ready(self):
                    seen.append("ready")

            listener = Listener()
            client.event(listener.on_ready)
            client.event(listener.on_message)
            await ready(client, gw)
            await gw.feed("MESSAGE_CREATE", message_payload())
            await drain(client)

            assert seen[0] == "ready"
            message = seen[1]
            assert isinstance(message, Message)
            assert message.id == MSG
            assert message.content == "hello"
            assert message.guild is client.get_guild(GUILD)
            assert message.channel is client.get_channel(GENERAL)
            assert isinstance(message.author, Member)
            assert message.author.id == FRIEND

    @pytest.mark.asyncio
    async def test_listeners_and_handler_both_fire(self):
        async with running() as (client, gw, fake_http):
            calls = []

            @client.event
            async def on_message(message):
                calls.append(("event", message.id))

            @client.listen("on_message")
            async def extra(message):
                calls.append(("listener", message.id))

            await ready(client, gw)
            await gw.feed("MESSAGE_CREATE", message_payload())
            await drain(client)
            assert sorted(calls) == [("event", MSG), ("listener", MSG)]

    @pytest.mark.asyncio
    async def test_handler_error_goes_to_on_error(self):
        async with running() as (client, gw, fake_http):
            errors = []

            @client.event
            async def on_message(message):
                raise RuntimeError("boom")

            @client.event
            async def on_error(event_method, *args):
                errors.append(event_method)

            await ready(client, gw)
            await gw.feed("MESSAGE_CREATE", message_payload())
            await drain(client)
            assert errors == ["on_message"]

    @pytest.mark.asyncio
    async def test_connect_and_disconnect_fire_from_the_socket(self):
        fired = []
        closed = []

        class Sock:
            async def close(self, code=1000):
                closed.append(code)

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

        async def connector(url, headers, proxy):
            return Sock()

        client = Client(connector=connector)

        @client.event
        async def on_connect():
            fired.append("connect")

        @client.event
        async def on_disconnect():
            fired.append("disconnect")

        ws = await client._tracked_connector("wss://gateway.example", {}, None)
        assert isinstance(ws, _TrackedSocket)
        async for _ in ws:
            pass
        await ws.close(code=4000)
        await drain(client)
        assert fired == ["connect", "disconnect"]
        assert closed == [4000]


# ---------------------------------------------------------------------------
# caches
# ---------------------------------------------------------------------------


class TestReadyAndGuildCreate:
    @pytest.mark.asyncio
    async def test_ready_populates_caches(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)

            assert [g.id for g in client.guilds] == [GUILD]
            guild = client.get_guild(GUILD)
            assert isinstance(guild, Guild) and guild.name == "Source"
            assert isinstance(client.get_channel(GENERAL), TextChannel)
            assert client.get_channel(GENERAL).guild is guild
            assert client.user.id == ME
            assert client.get_user(FRIEND).name == "friend"
            assert client.get_user(ME) is client.user

            assert [c.id for c in client.private_channels] == [DM]
            dm = client.get_channel(DM)
            assert isinstance(dm, DMChannel)
            assert dm.recipient.id == FRIEND
            assert client.get_user(FRIEND).dm_channel is dm

    @pytest.mark.asyncio
    async def test_guild_create_adds_guild_and_fires_guild_join(self):
        async with running() as (client, gw, fake_http):
            joined = []

            @client.event
            async def on_guild_join(guild):
                joined.append(guild)

            await ready(client, gw)
            await gw.feed(
                "GUILD_CREATE",
                guild_payload(OTHER_GUILD, "Other", channels=[text_channel(OTHER_GENERAL, "lobby", OTHER_GUILD)]),
            )
            await drain(client)

            assert {g.id for g in client.guilds} == {GUILD, OTHER_GUILD}
            assert client.get_guild(OTHER_GUILD).name == "Other"
            assert client.get_channel(OTHER_GENERAL).name == "lobby"
            assert [g.id for g in joined] == [OTHER_GUILD]

    @pytest.mark.asyncio
    async def test_guild_delete_removes_guild(self):
        async with running() as (client, gw, fake_http):
            removed = []

            @client.event
            async def on_guild_remove(guild):
                removed.append(guild.id)

            await ready(client, gw)
            await gw.feed("GUILD_DELETE", {"id": str(GUILD)})
            await drain(client)
            assert client.get_guild(GUILD) is None
            assert client.get_channel(GENERAL) is None
            assert removed == [GUILD]

    @pytest.mark.asyncio
    async def test_guild_update_keeps_live_channels(self):
        async with running() as (client, gw, fake_http):
            updates = []

            @client.event
            async def on_guild_update(before, after):
                updates.append((before.name, after.name))

            await ready(client, gw)
            await gw.feed("CHANNEL_CREATE", text_channel(NEW_CHANNEL, "added-later"))
            await gw.feed("GUILD_UPDATE", {"id": str(GUILD), "name": "Renamed"})
            await drain(client)

            guild = client.get_guild(GUILD)
            assert guild.name == "Renamed"
            assert client.get_channel(NEW_CHANNEL).guild is guild
            assert client.get_channel(GENERAL).guild is guild
            assert guild.get_role(MOD_ROLE) is not None
            assert guild.get_member(FRIEND) is not None
            assert updates == [("Source", "Renamed")]


class TestMessageCache:
    @pytest.mark.asyncio
    async def test_update_and_delete_maintain_cache(self):
        async with running() as (client, gw, fake_http):
            edits = []
            raw_edits = []
            deletes = []

            @client.event
            async def on_message_edit(before, after):
                edits.append((before.content, after.content))

            @client.event
            async def on_raw_message_edit(payload):
                raw_edits.append(payload.message_id)

            @client.event
            async def on_message_delete(message):
                deletes.append(message.id)

            await ready(client, gw)
            await gw.feed("MESSAGE_CREATE", message_payload())
            assert client.state._get_message(MSG).content == "hello"

            await gw.feed(
                "MESSAGE_UPDATE",
                {"id": str(MSG), "channel_id": str(GENERAL), "guild_id": str(GUILD), "content": "edited"},
            )
            cached = client.state._get_message(MSG)
            assert cached.content == "edited"
            assert cached.author.id == FRIEND

            await gw.feed("MESSAGE_DELETE", {"id": str(MSG), "channel_id": str(GENERAL), "guild_id": str(GUILD)})
            await drain(client)

            assert client.state._get_message(MSG) is None
            assert edits == [("hello", "edited")]
            assert raw_edits == [MSG]
            assert deletes == [MSG]

    @pytest.mark.asyncio
    async def test_uncached_delete_fires_raw_only(self):
        async with running() as (client, gw, fake_http):
            raw = []
            cooked = []

            @client.event
            async def on_raw_message_delete(payload):
                raw.append(payload.message_id)

            @client.event
            async def on_message_delete(message):
                cooked.append(message)

            await ready(client, gw)
            await gw.feed("MESSAGE_DELETE", {"id": str(MSG2), "channel_id": str(GENERAL), "guild_id": str(GUILD)})
            await drain(client)
            assert raw == [MSG2]
            assert cooked == []

    @pytest.mark.asyncio
    async def test_bulk_delete_and_max_messages(self):
        async with running(max_messages=2) as (client, gw, fake_http):
            await ready(client, gw)
            for mid in (MSG, MSG2, MSG3):
                await gw.feed("MESSAGE_CREATE", message_payload(mid))
            assert [m.id for m in client.cached_messages] == [MSG2, MSG3]

            await gw.feed(
                "MESSAGE_DELETE_BULK",
                {"ids": [str(MSG2), str(MSG3)], "channel_id": str(GENERAL), "guild_id": str(GUILD)},
            )
            assert client.cached_messages == []


class TestChannelAndRoleCache:
    @pytest.mark.asyncio
    async def test_channel_create_update_delete(self):
        async with running() as (client, gw, fake_http):
            seen = []

            @client.event
            async def on_guild_channel_create(channel):
                seen.append(("create", channel.name))

            @client.event
            async def on_guild_channel_update(before, after):
                seen.append(("update", before.name, after.name))

            @client.event
            async def on_guild_channel_delete(channel):
                seen.append(("delete", channel.name))

            await ready(client, gw)
            await gw.feed("CHANNEL_CREATE", text_channel(NEW_CHANNEL, "fresh"))
            assert client.get_channel(NEW_CHANNEL).name == "fresh"
            assert NEW_CHANNEL in {c.id for c in client.get_guild(GUILD).channels}

            await gw.feed("CHANNEL_UPDATE", text_channel(NEW_CHANNEL, "renamed"))
            assert client.get_channel(NEW_CHANNEL).name == "renamed"
            assert client.get_channel(NEW_CHANNEL).guild is client.get_guild(GUILD)

            await gw.feed("CHANNEL_DELETE", text_channel(NEW_CHANNEL, "renamed"))
            await drain(client)
            assert client.get_channel(NEW_CHANNEL) is None
            assert seen == [("create", "fresh"), ("update", "fresh", "renamed"), ("delete", "renamed")]

    @pytest.mark.asyncio
    async def test_thread_create_and_delete(self):
        async with running() as (client, gw, fake_http):
            deleted = []

            @client.event
            async def on_thread_delete(thread):
                deleted.append(thread.id)

            await ready(client, gw)
            thread = {
                "id": str(THREAD),
                "type": 11,
                "name": "a thread",
                "guild_id": str(GUILD),
                "parent_id": str(GENERAL),
                "newly_created": True,
                "thread_metadata": {"archived": False},
            }
            await gw.feed("THREAD_CREATE", thread)
            cached = client.get_channel(THREAD)
            assert isinstance(cached, Thread)
            assert cached in client.get_channel(GENERAL).threads

            await gw.feed(
                "THREAD_DELETE",
                {"id": str(THREAD), "guild_id": str(GUILD), "parent_id": str(GENERAL), "type": 11},
            )
            await drain(client)
            assert client.get_channel(THREAD) is None
            assert deleted == [THREAD]

    @pytest.mark.asyncio
    async def test_role_create_update_delete(self):
        async with running() as (client, gw, fake_http):
            seen = []

            @client.event
            async def on_guild_role_update(before, after):
                seen.append((before.name, after.name))

            @client.event
            async def on_guild_role_delete(role):
                seen.append(("deleted", role.name))

            await ready(client, gw)
            guild = client.get_guild(GUILD)
            await gw.feed(
                "GUILD_ROLE_CREATE",
                {"guild_id": str(GUILD), "role": {"id": str(NEW_ROLE), "name": "new", "permissions": "0", "position": 2}},
            )
            assert isinstance(guild.get_role(NEW_ROLE), Role)
            assert guild.get_role(NEW_ROLE).guild is guild

            await gw.feed(
                "GUILD_ROLE_UPDATE",
                {"guild_id": str(GUILD), "role": {"id": str(NEW_ROLE), "name": "newer", "permissions": "0", "position": 2}},
            )
            assert guild.get_role(NEW_ROLE).name == "newer"

            await gw.feed("GUILD_ROLE_DELETE", {"guild_id": str(GUILD), "role_id": str(NEW_ROLE)})
            await drain(client)
            assert guild.get_role(NEW_ROLE) is None
            assert seen == [("new", "newer"), ("deleted", "newer")]

    @pytest.mark.asyncio
    async def test_member_join_and_remove(self):
        async with running() as (client, gw, fake_http):
            joined = []

            @client.event
            async def on_member_join(member):
                joined.append(member.id)

            await ready(client, gw)
            guild = client.get_guild(GUILD)
            await gw.feed("GUILD_MEMBER_ADD", {"guild_id": str(GUILD), "user": user(STRANGER, "stranger"), "roles": []})
            assert guild.get_member(STRANGER) is not None
            assert client.get_user(STRANGER).name == "stranger"

            await gw.feed("GUILD_MEMBER_REMOVE", {"guild_id": str(GUILD), "user": user(STRANGER, "stranger")})
            await drain(client)
            assert guild.get_member(STRANGER) is None
            assert joined == [STRANGER]


# ---------------------------------------------------------------------------
# REST helpers
# ---------------------------------------------------------------------------


class TestRest:
    @pytest.mark.asyncio
    async def test_fetch_channel_returns_wrapper_on_cached_guild(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            channel = await client.fetch_channel(NEW_CHANNEL)
            assert isinstance(channel, TextChannel)
            assert channel.name == "fetched"
            assert channel.guild is client.get_guild(GUILD)
            assert client.get_channel(NEW_CHANNEL) is None

    @pytest.mark.asyncio
    async def test_fetch_user_and_member(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            fetched = await client.fetch_user(STRANGER)
            assert isinstance(fetched, User) and fetched.id == STRANGER
            assert client.get_user(STRANGER) is fetched

            member = await client.fetch_member(client.get_guild(GUILD), STRANGER)
            assert isinstance(member, Member)
            assert member.display_name == "nick"
            assert [r.id for r in member.roles][-1] == MOD_ROLE
            assert ("get_member", GUILD, STRANGER) in fake_http.calls

    @pytest.mark.asyncio
    async def test_fetch_roles(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            roles = await client.fetch_roles(GUILD)
            assert [type(r) for r in roles] == [Role]
            assert roles[0].guild is client.get_guild(GUILD)

    @pytest.mark.asyncio
    async def test_fetch_message_miss_raises_not_found(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            with pytest.raises(NotFound):
                await client.fetch_message(client.get_channel(GENERAL), MSG)

    @pytest.mark.asyncio
    async def test_history_wraps_messages_in_requested_order(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            fake_http.pages = [message_payload(MSG3, "c"), message_payload(MSG2, "b"), message_payload(MSG, "a")]
            channel = client.get_channel(GENERAL)

            newest = [m async for m in client.history(channel, limit=None)]
            assert [m.content for m in newest] == ["c", "b", "a"]
            assert all(isinstance(m, Message) and m.channel is channel for m in newest)

            oldest = [m async for m in client.history(channel, limit=None, after=MSG)]
            assert [m.content for m in oldest] == ["a", "b", "c"]
            assert fake_http.calls[-1] == ("messages", GENERAL, None, None, MSG, GUILD)

    @pytest.mark.asyncio
    async def test_archived_threads_yield_threads(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            threads = [t async for t in client.archived_threads(client.get_channel(GENERAL), private=True)]
            assert [type(t) for t in threads] == [Thread]
            assert threads[0].archived
            assert threads[0].guild is client.get_guild(GUILD)
            assert ("archived_threads", GENERAL, True, False, GUILD) in fake_http.calls

    @pytest.mark.asyncio
    async def test_send_dm_opens_and_caches_dm(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            sent = await client.send_dm(STRANGER, "hi there")
            assert isinstance(sent, Message)
            assert sent.content == "hi there"
            assert ("start_private_message", STRANGER) in fake_http.calls
            dm = client.state._get_private_channel_by_user(STRANGER)
            assert isinstance(dm, DMChannel)

            await client.send_dm(STRANGER, "again")
            opens = [c for c in fake_http.calls if c[0] == "start_private_message"]
            assert len(opens) == 1

    @pytest.mark.asyncio
    async def test_fetch_members_collects_chunks(self):
        async with running() as (client, gw, fake_http):
            await ready(client, gw)
            task = asyncio.ensure_future(client.fetch_members(GUILD, user_ids=[STRANGER], timeout=2))
            for _ in range(20):
                if gw.member_requests:
                    break
                await asyncio.sleep(0)
            _, kwargs = gw.member_requests[0]
            nonce = kwargs["nonce"]
            for index in range(2):
                await gw.feed(
                    "GUILD_MEMBERS_CHUNK",
                    {
                        "guild_id": str(GUILD),
                        "members": [{"user": user(STRANGER + index, "m"), "roles": []}],
                        "nonce": nonce,
                        "chunk_index": index,
                        "chunk_count": 2,
                    },
                )
            members = await task
            assert [m.id for m in members] == [STRANGER, STRANGER + 1]
            assert client.get_guild(GUILD).get_member(STRANGER) is not None
