"""selfclient.models: permissions_for() and the raw-dict wrappers.

permissions_for follows the usual Discord order: owner and administrator first,
then @everyone, role grants, then channel overwrites.
"""
import pytest

from copycord.selfclient.enums import ChannelType, MessageType, StickerFormatType, VideoQualityMode
from copycord.selfclient.models import (
    CategoryChannel,
    ForumChannel,
    Guild,
    Member,
    Message,
    Object,
    Permissions,
    StageChannel,
    TextChannel,
    Thread,
    User,
    VoiceChannel,
)

GUILD_ID = 100000000000000001
OWNER_ID = 200000000000000001
MEMBER_ID = 300000000000000001
OTHER_ID = 300000000000000002

EVERYONE = GUILD_ID
MOD = 400000000000000001
ADMIN = 400000000000000002
HELPER = 400000000000000003
QUIET = 400000000000000004

CATEGORY = 500000000000000001
TEXT = 500000000000000002
VOICE = 500000000000000003
FORUM = 500000000000000004
STAGE = 500000000000000005
TEXT_LOW = 500000000000000006
THREAD = 600000000000000001


def bits(*names):
    return str(Permissions._of(names).value)


def overwrite(target, kind, allow=(), deny=()):
    return {"id": str(target), "type": kind, "allow": bits(*allow), "deny": bits(*deny)}


def make_guild(text_overwrites=(), extra=None):
    data = {
        "id": str(GUILD_ID),
        "name": "Source",
        "icon": "abc123",
        "owner_id": str(OWNER_ID),
        "features": ["COMMUNITY"],
        "rules_channel_id": str(TEXT),
        "roles": [
            {"id": str(EVERYONE), "name": "@everyone", "position": 0,
             "permissions": bits("view_channel", "send_messages", "read_message_history",
                                 "embed_links"),
             "color": 0},
            {"id": str(MOD), "name": "Mod", "position": 3, "permissions": bits("manage_messages"),
             "color": 0x3498DB, "hoist": True, "mentionable": True},
            {"id": str(ADMIN), "name": "Admin", "position": 5, "permissions": bits("administrator"),
             "color": 0},
            {"id": str(HELPER), "name": "Helper", "position": 2, "permissions": "0", "color": 0},
            {"id": str(QUIET), "name": "Quiet", "position": 1, "permissions": "0", "color": 0},
        ],
        "channels": [
            {"id": str(CATEGORY), "type": 4, "name": "General", "position": 0},
            {"id": str(TEXT), "type": 0, "name": "chat", "position": 2, "parent_id": str(CATEGORY),
             "topic": "hello", "rate_limit_per_user": 5, "nsfw": False,
             "permission_overwrites": list(text_overwrites)},
            {"id": str(TEXT_LOW), "type": 0, "name": "rules", "position": 1,
             "parent_id": str(CATEGORY)},
            {"id": str(VOICE), "type": 2, "name": "Voice", "position": 3,
             "parent_id": str(CATEGORY), "bitrate": 96000, "user_limit": 10,
             "video_quality_mode": 2},
            {"id": str(STAGE), "type": 13, "name": "Stage", "position": 4, "topic": "talks"},
            {"id": str(FORUM), "type": 15, "name": "help", "position": 5, "flags": 16,
             "default_reaction_emoji": {"emoji_id": None, "emoji_name": "👍"},
             "available_tags": [
                 {"id": "700000000000000001", "name": "bug", "moderated": True,
                  "emoji_id": None, "emoji_name": "🐛"},
                 {"id": "700000000000000002", "name": "idea", "moderated": False,
                  "emoji_id": "800000000000000001", "emoji_name": "bulb"},
             ]},
        ],
        "threads": [
            {"id": str(THREAD), "type": 11, "name": "post", "parent_id": str(FORUM),
             "owner_id": str(MEMBER_ID), "applied_tags": ["700000000000000002"],
             "thread_metadata": {"archived": False, "archive_timestamp": "2026-01-01T00:00:00+00:00"}},
        ],
        "members": [],
        "emojis": [{"id": "800000000000000001", "name": "bulb", "animated": False}],
    }
    data.update(extra or {})
    return Guild(data)


def make_member(guild, roles=(), user_id=MEMBER_ID, **extra):
    data = {"user": {"id": str(user_id), "username": "alice", "discriminator": "0"},
            "roles": [str(r) for r in roles]}
    data.update(extra)
    return Member(data, guild)


# ---------------------------------------------------------------------------
# permissions_for
# ---------------------------------------------------------------------------


class TestPermissionsFor:
    def test_administrator_short_circuits_every_overwrite(self):
        guild = make_guild([overwrite(EVERYONE, 0, deny=("view_channel", "send_messages"))])
        member = make_member(guild, roles=[ADMIN])

        perms = guild.get_channel(TEXT).permissions_for(member)

        assert perms.administrator
        assert perms.view_channel and perms.read_message_history
        assert perms.value == Permissions.all().value

    def test_guild_owner_gets_everything(self):
        guild = make_guild([overwrite(EVERYONE, 0, deny=("view_channel",))])
        owner = make_member(guild, user_id=OWNER_ID)
        assert guild.get_channel(TEXT).permissions_for(owner).value == Permissions.all().value

    def test_everyone_base_ors_with_member_roles(self):
        guild = make_guild()
        member = make_member(guild, roles=[MOD])

        perms = guild.get_channel(TEXT).permissions_for(member)

        assert perms.view_channel and perms.send_messages and perms.read_message_history
        assert perms.manage_messages
        assert not perms.ban_members
        assert not perms.administrator

    def test_plain_member_has_only_everyone(self):
        guild = make_guild()
        member = make_member(guild)
        perms = guild.get_channel(TEXT).permissions_for(member)
        assert perms.read_message_history
        assert not perms.manage_messages

    def test_role_allow_overrides_everyone_deny(self):
        guild = make_guild(
            [
                overwrite(EVERYONE, 0, deny=("view_channel",)),
                overwrite(MOD, 0, allow=("view_channel",)),
            ]
        )
        channel = guild.get_channel(TEXT)

        assert channel.permissions_for(make_member(guild, roles=[MOD])).view_channel
        plain = channel.permissions_for(make_member(guild))
        assert not plain.view_channel
        assert not plain.read_message_history
        assert not plain.send_messages

    def test_role_allow_beats_another_roles_deny(self):
        guild = make_guild(
            [
                overwrite(HELPER, 0, deny=("send_messages",)),
                overwrite(QUIET, 0, allow=("send_messages",)),
            ]
        )
        member = make_member(guild, roles=[HELPER, QUIET])
        assert guild.get_channel(TEXT).permissions_for(member).send_messages

    def test_role_deny_applies_without_a_matching_allow(self):
        guild = make_guild([overwrite(HELPER, 0, deny=("send_messages",))])
        perms = guild.get_channel(TEXT).permissions_for(make_member(guild, roles=[HELPER]))
        assert not perms.send_messages
        assert not perms.embed_links
        assert perms.view_channel

    def test_member_overwrite_beats_role_overwrite(self):
        guild = make_guild(
            [
                overwrite(MOD, 0, allow=("send_messages",)),
                overwrite(MEMBER_ID, 1, deny=("send_messages",)),
            ]
        )
        assert not guild.get_channel(TEXT).permissions_for(make_member(guild, roles=[MOD])).send_messages
        other = make_member(guild, roles=[MOD], user_id=OTHER_ID)
        assert guild.get_channel(TEXT).permissions_for(other).send_messages

    def test_member_allow_beats_everyone_deny(self):
        guild = make_guild(
            [
                overwrite(EVERYONE, 0, deny=("view_channel",)),
                overwrite(MEMBER_ID, 1, allow=("view_channel",)),
            ]
        )
        assert guild.get_channel(TEXT).permissions_for(make_member(guild)).view_channel

    def test_role_target_uses_only_that_roles_overwrite(self):
        guild = make_guild([overwrite(MOD, 0, deny=("read_message_history",))])
        channel = guild.get_channel(TEXT)
        assert not channel.permissions_for(guild.get_role(MOD)).read_message_history
        assert channel.permissions_for(guild.default_role).read_message_history

    def test_timed_out_member_keeps_only_view_and_history(self):
        guild = make_guild()
        member = make_member(guild, roles=[MOD], communication_disabled_until="2999-01-01T00:00:00+00:00")
        perms = guild.get_channel(TEXT).permissions_for(member)
        assert perms.view_channel and perms.read_message_history
        assert not perms.send_messages
        assert not perms.manage_messages

    def test_thread_inherits_parent_permissions(self):
        guild = make_guild()
        thread = guild.get_channel(THREAD)
        assert isinstance(thread, Thread)
        assert thread.permissions_for(make_member(guild)).read_message_history

    def test_guild_permissions(self):
        guild = make_guild()
        perms = make_member(guild, roles=[MOD]).guild_permissions
        assert perms.manage_messages and perms.view_channel
        assert make_member(guild, roles=[ADMIN]).guild_permissions.value == Permissions.all().value

    def test_handle_overwrite_clears_then_sets(self):
        p = Permissions(Permissions._of(("send_messages", "embed_links")).value)
        p.handle_overwrite(allow=int(bits("attach_files")), deny=int(bits("embed_links")))
        assert p.send_messages and p.attach_files and not p.embed_links


# ---------------------------------------------------------------------------
# guild / channel / role wrappers
# ---------------------------------------------------------------------------


class TestGuildWrappers:
    def test_guild_scalars(self):
        guild = make_guild()
        assert guild.id == GUILD_ID
        assert guild.name == "Source"
        assert guild.owner_id == OWNER_ID
        assert str(guild.icon) == f"https://cdn.discordapp.com/icons/{GUILD_ID}/abc123.png?size=1024"
        assert guild.icon.key == "abc123"
        assert guild.banner is None
        assert "COMMUNITY" in guild.features
        assert guild.rules_channel.id == TEXT
        assert guild.public_updates_channel is None

    def test_ready_properties_shape_is_read_too(self):
        guild = Guild({"id": str(GUILD_ID), "properties": {"name": "Nested", "owner_id": str(OWNER_ID)},
                       "channels": [], "roles": []})
        assert guild.name == "Nested"
        assert guild.owner_id == OWNER_ID

    def test_roles(self):
        guild = make_guild()
        roles = guild.roles
        assert [r.id for r in roles] == [EVERYONE, QUIET, HELPER, MOD, ADMIN]
        everyone = roles[0]
        assert everyone.is_default() and everyone == everyone.guild.default_role
        mod = guild.get_role(MOD)
        assert mod.color.value == 0x3498DB
        assert mod.permissions.value == int(bits("manage_messages"))
        assert mod.hoist and mod.mentionable and not mod.managed
        assert mod.position == 3

    def test_member_roles_start_with_everyone(self):
        guild = make_guild()
        member = make_member(guild, roles=[MOD, HELPER])
        assert [r.id for r in member.roles] == [EVERYONE, HELPER, MOD]
        assert member.top_role.id == MOD

    def test_channel_types_and_category_listing(self):
        guild = make_guild()
        cat = guild.get_channel(CATEGORY)
        assert isinstance(cat, CategoryChannel)
        assert [c.id for c in cat.channels] == [TEXT_LOW, TEXT, VOICE]
        assert [c.id for c in guild.categories] == [CATEGORY]
        assert [c.id for c in guild.text_channels] == [TEXT_LOW, TEXT]

        text = guild.get_channel(TEXT)
        assert isinstance(text, TextChannel)
        assert text.type == ChannelType.text and text.type.value == 0
        assert text.topic == "hello" and text.slowmode_delay == 5
        assert text.category.id == CATEGORY and text.category_id == CATEGORY
        assert guild.get_channel(STAGE).category is None

    def test_voice_and_stage(self):
        guild = make_guild()
        voice = guild.get_channel(VOICE)
        assert isinstance(voice, VoiceChannel)
        assert voice.bitrate == 96000 and voice.user_limit == 10
        assert voice.video_quality_mode == VideoQualityMode.full
        assert voice.video_quality_mode.name == "full"
        stage = guild.get_channel(STAGE)
        assert isinstance(stage, StageChannel) and stage.topic == "talks"

    def test_forum_and_thread(self):
        guild = make_guild()
        forum = guild.get_channel(FORUM)
        assert isinstance(forum, ForumChannel)
        assert forum.type == ChannelType.forum
        assert forum.require_tag is True
        assert forum.default_reaction_emoji.name == "👍"
        assert forum.default_reaction_emoji.id is None
        tags = forum.available_tags
        assert [t.name for t in tags] == ["bug", "idea"]
        assert tags[0].moderated and tags[0].emoji.name == "🐛"
        assert tags[1].emoji.id == 800000000000000001
        assert [f.id for f in guild.forums] == [FORUM]

        thread = guild.get_channel(THREAD)
        assert thread.parent.id == FORUM
        assert thread.type in (ChannelType.public_thread, ChannelType.private_thread)
        assert thread.archived is False
        assert [t.name for t in thread.applied_tags] == ["idea"]
        assert [t.id for t in forum.threads] == [THREAD]

    def test_permission_overwrites_list_shape(self):
        guild = make_guild([overwrite(MOD, 0, allow=("view_channel",)), overwrite(MEMBER_ID, 1)])
        ows = guild.get_channel(TEXT).permission_overwrites
        assert [(o.id, o.type) for o in ows] == [(MOD, 0), (MEMBER_ID, 1)]
        assert ows[0].allow == int(bits("view_channel")) and ows[0].deny == 0

    def test_emojis(self):
        guild = make_guild()
        e = guild.emojis[0]
        assert e.name == "bulb" and not e.animated
        assert str(e.url) == "https://cdn.discordapp.com/emojis/800000000000000001.png"


# ---------------------------------------------------------------------------
# message / user wrappers
# ---------------------------------------------------------------------------


def _message_payload(**overrides):
    data = {
        "id": "900000000000000002",
        "channel_id": str(TEXT),
        "guild_id": str(GUILD_ID),
        "type": 19,
        "flags": 16384,
        "content": f"hi <@{OTHER_ID}> and <@&{MOD}> in <#{TEXT}> @everyone",
        "timestamp": "2026-02-01T12:00:00.000000+00:00",
        "edited_timestamp": None,
        "author": {"id": str(MEMBER_ID), "username": "alice", "global_name": "Alice",
                   "discriminator": "0", "avatar": "a_deadbeef"},
        "member": {"nick": "Ali", "roles": [str(MOD)]},
        "mentions": [{"id": str(OTHER_ID), "username": "bob", "discriminator": "0",
                      "member": {"nick": "Bobby", "roles": []}}],
        "mention_roles": [str(MOD)],
        "attachments": [{"id": "1", "filename": "a.png", "url": "https://cdn/a.png", "size": 42,
                         "content_type": "image/png"}],
        "embeds": [{"title": "T", "description": "D"}],
        "sticker_items": [{"id": "2", "name": "wave", "format_type": 1}],
        "message_reference": {"message_id": "900000000000000001", "channel_id": str(TEXT),
                              "guild_id": str(GUILD_ID)},
        "referenced_message": {"id": "900000000000000001", "channel_id": str(TEXT), "type": 0,
                               "content": "original", "author": {"id": str(OTHER_ID),
                                                                 "username": "bob"}},
    }
    data.update(overrides)
    return data


class TestMessageWrapper:
    def test_message_reads_like_discord_message(self):
        guild = make_guild()
        msg = Message(_message_payload(), guild=guild)

        assert msg.id == 900000000000000002
        assert msg.guild is guild and msg.guild_id == GUILD_ID
        assert msg.channel.id == TEXT and isinstance(msg.channel, TextChannel)
        assert msg.type == MessageType.reply and msg.type.value == 19
        assert str(msg.type) == "MessageType.reply"
        assert msg.is_system() is False
        assert msg.system_content == msg.content
        assert msg.flags.value & 16384
        assert msg.created_at.year == 2026 and msg.created_at.tzinfo is not None
        assert msg.edited_at is None
        assert msg.jump_url == f"https://discord.com/channels/{GUILD_ID}/{TEXT}/900000000000000002"

        author = msg.author
        assert isinstance(author, Member)
        assert author.display_name == "Ali" and author.name == "alice"
        assert [r.id for r in author.roles] == [EVERYONE, MOD]
        assert author.avatar.url.endswith("/avatars/300000000000000001/a_deadbeef.gif?size=1024")
        assert author.display_avatar.url == author.avatar.url

        assert [m.display_name for m in msg.mentions] == ["Bobby"]
        assert [r.name for r in msg.role_mentions] == ["Mod"]
        assert [c.id for c in msg.channel_mentions] == [TEXT]
        assert msg.clean_content == "hi @Bobby and @Mod in #chat @​everyone"

        att = msg.attachments[0]
        assert (att.filename, att.url, att.size, att.content_type) == (
            "a.png", "https://cdn/a.png", 42, "image/png")
        assert msg.embeds[0].to_dict() == {"title": "T", "description": "D"}
        assert msg.stickers[0].name == "wave"
        assert msg.stickers[0].format == StickerFormatType.png

        ref = msg.reference
        assert ref.message_id == 900000000000000001 and ref.channel_id == TEXT
        assert ref.resolved.content == "original"

    def test_system_message(self):
        guild = make_guild()
        msg = Message(_message_payload(type=18, content="new thread", member=None, flags=0),
                      guild=guild)
        assert msg.is_system()
        assert msg.type == MessageType.thread_created
        assert msg.system_content == "alice started a thread: **new thread**. See all **threads**."

    def test_message_without_guild_has_a_user_author_and_partial_channel(self):
        msg = Message(_message_payload(guild_id=None, member=None))
        assert msg.guild is None
        assert type(msg.author) is User
        assert msg.channel.id == TEXT
        assert msg.role_mentions == []

    def test_deleted_reference(self):
        msg = Message(_message_payload(referenced_message=None))
        assert msg.reference.resolved is not None
        assert msg.reference.resolved.id == 900000000000000001


class TestSmallWrappers:
    def test_user_defaults(self):
        user = User({"id": str(MEMBER_ID), "username": "alice", "discriminator": "0"})
        assert user.display_name == "alice"
        assert str(user) == "alice"
        assert user.avatar is None
        assert user.display_avatar.url.startswith("https://cdn.discordapp.com/embed/avatars/")
        assert user.mention == f"<@{MEMBER_ID}>"

    def test_object_compares_by_id(self):
        assert Object(id=5) == Object(id="5")
        assert Object(5).created_at is not None

    def test_unknown_enum_values_pass_through(self):
        guild = make_guild()
        msg = Message(_message_payload(type=999), guild=guild)
        assert msg.type == 999
        assert msg.is_system()


@pytest.mark.parametrize(
    "ctype,cls",
    [(0, TextChannel), (2, VoiceChannel), (4, CategoryChannel), (13, StageChannel),
     (15, ForumChannel), (11, Thread), (12, Thread)],
)
def test_channel_factory_picks_class_by_type(ctype, cls):
    from copycord.selfclient.models import channel_from_dict

    assert type(channel_from_dict({"id": "1", "type": ctype}, None)) is cls
