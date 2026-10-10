"""
client.message_utils and client.helpers on selfclient objects.

Both modules used to type against discord.py-self and catch its exceptions.
They now take selfclient.models wrappers built straight from gateway payloads,
so these tests feed them a Message made from a canned MESSAGE_CREATE and check
that serialize/extract read back the same shapes, and that dm_member_by_id
maps selfclient's Forbidden/NotFound to False. Fake bots stand in for
selfclient.Client; nothing touches the network.
"""
import json

import pytest

import client.helpers as helpers
import client.message_utils as message_utils
from client.helpers import dm_member_by_id, dump_message_debug
from client.message_utils import MessageUtils, _resolve_forward
from selfclient import errors
from selfclient.errors import Forbidden, NotFound
from selfclient.models import Guild, Member, Message, User

GUILD = 200000000000000001
CHAT = 300000000000000001
OTHER_CHAT = 300000000000000002
AUTHOR = 100000000000000001
FRIEND = 100000000000000002
STRANGER = 100000000000000003
UNKNOWN = 100000000000000004
MOD_ROLE = 400000000000000001
VIP_ROLE = 400000000000000002
MSG = 500000000000000001
INNER = 500000000000000002
ATTACHMENT = 600000000000000001
GUILD_STICKER = 700000000000000001
STANDARD_STICKER = 700000000000000002


def user(uid, name, **extra):
    return {"id": str(uid), "username": name, "discriminator": "0", **extra}


def guild_payload():
    return {
        "id": str(GUILD),
        "name": "Source",
        "owner_id": str(AUTHOR),
        "roles": [
            {"id": str(GUILD), "name": "@everyone", "position": 0, "permissions": "0"},
            {"id": str(VIP_ROLE), "name": "VIP", "position": 2, "permissions": "0"},
            {"id": str(MOD_ROLE), "name": "Mod", "position": 1, "permissions": "0"},
        ],
        "channels": [
            {"id": str(CHAT), "type": 0, "name": "chat", "position": 0},
            {"id": str(OTHER_CHAT), "type": 0, "name": "elsewhere", "position": 1},
        ],
        "stickers": [{"id": str(GUILD_STICKER), "name": "wave", "format_type": 1}],
    }


def message_create(**overrides):
    """A MESSAGE_CREATE ``d`` as the gateway sends it to a user account."""
    data = {
        "id": str(MSG),
        "type": 0,
        "channel_id": str(CHAT),
        "guild_id": str(GUILD),
        "timestamp": "2026-01-02T03:04:05.000000+00:00",
        "edited_timestamp": "2026-01-02T03:05:00.000000+00:00",
        "content": f"hi <@{FRIEND}> and <@!{STRANGER}>",
        "author": user(AUTHOR, "author", avatar="abc123", global_name="Author G"),
        "member": {"nick": "Boss", "roles": [str(MOD_ROLE), str(VIP_ROLE)]},
        "mentions": [
            {**user(FRIEND, "friend", global_name="Friend G"), "member": {"nick": "Pal", "roles": []}},
            user(STRANGER, "stranger", global_name="Stranger G"),
        ],
        "mention_roles": [str(MOD_ROLE)],
        "mention_everyone": False,
        "attachments": [
            {
                "id": str(ATTACHMENT),
                "filename": "cat.png",
                "url": "https://cdn.discordapp.com/attachments/1/2/cat.png",
                "proxy_url": "https://media.discordapp.net/attachments/1/2/cat.png",
                "size": 1234,
                "content_type": "image/png",
            }
        ],
        "embeds": [
            {
                "title": f"for <@{FRIEND}>",
                "description": "{mention} welcome",
                "fields": [{"name": "who", "value": f"<@{FRIEND}>"}],
            }
        ],
        "sticker_items": [
            {"id": str(GUILD_STICKER), "name": "wave", "format_type": 1},
            {"id": str(STANDARD_STICKER), "name": "hello", "format_type": 3},
        ],
        "pinned": False,
        "tts": False,
    }
    data.update(overrides)
    return data


@pytest.fixture()
def guild():
    return Guild(guild_payload())


@pytest.fixture()
def message(guild):
    return Message(message_create(), guild=guild)


class FakeBot:
    """The slice of selfclient.Client the two modules call."""

    def __init__(self, guild=None):
        self.guild = guild
        self.users = {}
        self.channels = {}
        self.messages = {}
        self.member_result = None
        self.dm_error = None
        self.calls = []

    def get_user(self, user_id):
        self.calls.append(("get_user", user_id))
        return self.users.get(user_id)

    async def fetch_user(self, user_id):
        self.calls.append(("fetch_user", user_id))
        return User(user(user_id, f"fetched{user_id % 10}"))

    async def fetch_member(self, guild, user_id):
        self.calls.append(("fetch_member", guild.id, user_id))
        if self.member_result is None:
            raise NotFound(404, {"code": 10007, "message": "Unknown Member"})
        return self.member_result

    def get_channel(self, channel_id):
        self.calls.append(("get_channel", channel_id))
        return self.channels.get(channel_id)

    async def fetch_channel(self, channel_id):
        self.calls.append(("fetch_channel", channel_id))
        raise NotFound(404, None)

    async def fetch_message(self, channel, message_id):
        self.calls.append(("fetch_message", channel.id, message_id))
        return self.messages[message_id]

    async def send_dm(self, user_obj, content):
        self.calls.append(("send_dm", user_obj.id, content))
        if self.dm_error is not None:
            raise self.dm_error


class TestNoDiscordImport:
    def test_modules_hold_no_discord_reference(self):
        assert not hasattr(message_utils, "discord")
        assert not hasattr(helpers, "discord")

    def test_helpers_catch_selfclient_errors(self):
        assert helpers.Forbidden is errors.Forbidden
        assert helpers.NotFound is errors.NotFound

    def test_message_utils_checks_selfclient_member(self):
        assert message_utils.Member is Member


class TestSerialize:
    def test_top_level_shape(self, guild, message):
        data = MessageUtils(FakeBot(guild)).serialize(message)

        assert data["id"] == str(MSG)
        assert data["timestamp"] == "2026-01-02T03:04:05+00:00"
        assert data["edited_timestamp"] == "2026-01-02T03:05:00+00:00"
        assert data["type"] == "MessageType.default"

    def test_author(self, guild, message):
        author = MessageUtils(FakeBot(guild)).serialize(message)["author"]

        assert author == {
            "id": str(AUTHOR),
            "name": "author",
            "discriminator": "0",
            "bot": False,
            "avatar_url": f"https://cdn.discordapp.com/avatars/{AUTHOR}/abc123.png?size=1024",
        }

    def test_author_without_avatar(self, guild):
        msg = Message(message_create(author=user(AUTHOR, "author")), guild=guild)

        assert MessageUtils(FakeBot(guild)).serialize(msg)["author"]["avatar_url"] is None

    def test_content_mentions_humanized(self, guild, message):
        # A mentioned Member shows its nick; a bare User its username, not
        # its global name, as with discord.py-self's User.
        data = MessageUtils(FakeBot(guild)).serialize(message)

        assert data["content"] == "hi @Pal and @stranger"

    def test_attachments(self, guild, message):
        data = MessageUtils(FakeBot(guild)).serialize(message)

        assert data["attachments"] == [
            {
                "id": str(ATTACHMENT),
                "filename": "cat.png",
                "url": "https://cdn.discordapp.com/attachments/1/2/cat.png",
                "size": 1234,
                "content_type": "image/png",
            }
        ]

    def test_embeds_sanitized(self, guild, message):
        (embed,) = MessageUtils(FakeBot(guild)).serialize(message)["embeds"]

        assert embed["title"] == "for @Pal"
        assert embed["description"] == "@Boss welcome"
        assert embed["fields"] == [{"name": "who", "value": "@Pal"}]

    def test_embeds_leave_message_payload_untouched(self, guild, message):
        MessageUtils(FakeBot(guild)).serialize(message)

        assert message.raw["embeds"][0]["title"] == f"for <@{FRIEND}>"

    def test_stickers(self, guild, message):
        data = MessageUtils(FakeBot(guild)).serialize(message)

        assert data["stickers"] == [
            {
                "id": GUILD_STICKER,
                "name": "wave",
                "format_type": 1,
                "url": f"https://cdn.discordapp.com/stickers/{GUILD_STICKER}.png",
                "is_guild_sticker": True,
            },
            {
                "id": STANDARD_STICKER,
                "name": "hello",
                "format_type": 3,
                "url": f"https://cdn.discordapp.com/stickers/{STANDARD_STICKER}.json",
                "is_guild_sticker": False,
            },
        ]

    def test_optional_sections_absent(self, guild):
        msg = Message(
            message_create(attachments=[], embeds=[], sticker_items=[]), guild=guild
        )

        data = MessageUtils(FakeBot(guild)).serialize(msg)

        assert "attachments" not in data
        assert "embeds" not in data
        assert "stickers" not in data

    def test_dm_message_without_guild(self):
        msg = Message(message_create(guild_id=None, member=None, mentions=[]))

        data = MessageUtils(FakeBot()).serialize(msg)

        assert data["author"]["name"] == "author"
        assert data["stickers"][0]["is_guild_sticker"] is False


class TestExtract:
    def test_public_non_callable_attrs(self, guild, message):
        attrs = MessageUtils(FakeBot(guild)).extract_public_message_attrs(message)

        assert attrs["id"] == MSG
        assert attrs["content"] == message.content
        assert attrs["guild"] is guild
        assert isinstance(attrs["author"], Member)
        assert attrs["mention_everyone"] is False
        assert "is_system" not in attrs
        assert "to_dict" not in attrs
        assert not any(name.startswith("_") for name in attrs)

    def test_role_mentions_payload(self, guild, message):
        out = MessageUtils(FakeBot(guild))._build_role_mentions_payload(message)

        assert out == [{"id": MOD_ROLE, "name": "Mod"}]

    def test_author_role_ids_skip_everyone(self, message):
        assert MessageUtils.author_role_ids(message) == [MOD_ROLE, VIP_ROLE]

    def test_author_role_ids_bare_user(self):
        msg = Message(message_create(guild_id=None, member=None))

        assert MessageUtils.author_role_ids(msg) == []


class TestMentionMap:
    @pytest.mark.asyncio
    async def test_fetch_member_then_user_fallback(self, guild):
        msg = Message(message_create(content=f"<@{UNKNOWN}>", mentions=[]), guild=guild)
        bot = FakeBot(guild)

        out = await MessageUtils(bot).build_mention_map(msg, [])

        assert out == {str(UNKNOWN): "@fetched4"}
        assert ("fetch_member", GUILD, UNKNOWN) in bot.calls
        assert ("fetch_user", UNKNOWN) in bot.calls

    @pytest.mark.asyncio
    async def test_fetched_member_uses_display_name(self, guild):
        msg = Message(message_create(content="", mentions=[]), guild=guild)
        bot = FakeBot(guild)
        bot.member_result = Member({"user": user(UNKNOWN, "someone"), "nick": "Nick"}, guild)
        embeds = [{"footer": {"text": f"by <@{UNKNOWN}>"}}]

        out = await MessageUtils(bot).build_mention_map(msg, embeds)

        assert out == {str(UNKNOWN): "@Nick"}
        assert ("fetch_user", UNKNOWN) not in bot.calls


class TestResolveForward:
    @pytest.mark.asyncio
    async def test_follows_reference(self, guild):
        wrapper = Message(
            message_create(
                content="",
                attachments=[],
                embeds=[],
                sticker_items=[],
                message_reference={"type": 1, "channel_id": str(OTHER_CHAT), "message_id": str(INNER)},
            ),
            guild=guild,
        )
        inner = Message(message_create(id=str(INNER), channel_id=str(OTHER_CHAT), content="inner"), guild=guild)
        bot = FakeBot(guild)
        bot.channels[OTHER_CHAT] = guild.get_channel(OTHER_CHAT)
        bot.messages[INNER] = inner

        resolved = await _resolve_forward(bot, wrapper)

        assert resolved is inner
        assert ("fetch_message", OTHER_CHAT, INNER) in bot.calls

    @pytest.mark.asyncio
    async def test_message_with_content_returns_itself(self, message):
        assert await _resolve_forward(FakeBot(), message) is message


class TestDmMemberById:
    @pytest.mark.asyncio
    async def test_cached_user_sent(self):
        bot = FakeBot()
        bot.users[FRIEND] = User(user(FRIEND, "friend"))

        assert await dm_member_by_id(bot, FRIEND, "hello") is True
        assert bot.calls == [("get_user", FRIEND), ("send_dm", FRIEND, "hello")]

    @pytest.mark.asyncio
    async def test_uncached_user_fetched(self):
        bot = FakeBot()

        assert await dm_member_by_id(bot, STRANGER, "hello") is True
        assert ("fetch_user", STRANGER) in bot.calls
        assert ("send_dm", STRANGER, "hello") in bot.calls

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            Forbidden(403, {"code": 50007, "message": "Cannot send messages to this user"}),
            NotFound(404, {"code": 10013, "message": "Unknown User"}),
            RuntimeError("anything else"),
        ],
    )
    async def test_errors_map_to_false(self, error):
        bot = FakeBot()
        bot.dm_error = error

        assert await dm_member_by_id(bot, FRIEND, "hello") is False


class TestDumpMessageDebug:
    def test_dump_reads_selfclient_message(self, guild, message):
        data = json.loads(dump_message_debug(message))

        assert data["id"] == MSG
        assert data["type"] == "MessageType"
        assert data["author"]["display_name"] == "Boss"
        assert data["author"]["avatar_url"].endswith(f"/avatars/{AUTHOR}/abc123.png?size=1024")
        assert data["channel"]["name"] == "chat"
        assert data["channel"]["type"] == "text"
        assert data["guild"] == {"id": GUILD, "name": "Source", "_raw": data["guild"]["_raw"]}
        assert data["attachments"][0]["proxy_url"].startswith("https://media.discordapp.net/")
        assert data["mentions_everyone"] is False
        assert data["is_system"] is None
