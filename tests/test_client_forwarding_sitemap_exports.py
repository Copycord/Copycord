"""client.forwarding, client.sitemap and client.export_runners on selfclient objects.

Canned payloads go through the real code paths with fake bots for Client. No network.
"""
import logging

import pytest

from copycord.client.export_runners import (
    BackfillEngine,
    DmHistoryExporter,
    _archived_threads,
    _has_archived_threads,
)
from copycord.client.forwarding import ForwardingFilters, ForwardingManager, ForwardingRule
from copycord.client.sitemap import SitemapService
from copycord.selfclient.errors import Forbidden
from copycord.selfclient.http import HTTPClient
from copycord.selfclient.models import (
    CategoryChannel,
    DMChannel,
    ForumChannel,
    GroupChannel,
    Guild,
    Message,
    Object,
    Role,
    StageChannel,
    TextChannel,
    Thread,
    VoiceChannel,
    channel_from_dict,
)

GUILD = 200000000000000001
CATEGORY = 300000000000000001
CHAT = 300000000000000002
VOICE = 300000000000000003
STAGE = 300000000000000004
FORUM = 300000000000000005
LOOSE = 300000000000000006
POST = 300000000000000007
CHAT_THREAD = 300000000000000008
AUTHOR = 100000000000000001
FRIEND = 100000000000000002
MOD_ROLE = 400000000000000001
VIP_ROLE = 400000000000000002
TAG = 450000000000000001
MSG = 500000000000000001
GUILD_STICKER = 700000000000000001
STANDARD_STICKER = 700000000000000002


def user(uid, name, **extra):
    return {"id": str(uid), "username": name, "discriminator": "0", **extra}


def guild_payload():
    """A GUILD_CREATE-shaped guild with one of every channel kind sitemap reads."""
    return {
        "id": str(GUILD),
        "name": "Source",
        "owner_id": str(AUTHOR),
        "features": ["COMMUNITY"],
        "preferred_locale": "en-US",
        "verification_level": 2,
        "premium_tier": 1,
        "system_channel_id": str(CHAT),
        "rules_channel_id": str(CHAT),
        "roles": [
            {"id": str(GUILD), "name": "@everyone", "position": 0, "permissions": "1024"},
            {
                "id": str(MOD_ROLE),
                "name": "Mod",
                "position": 1,
                "permissions": "8",
                "color": 16711680,
                "hoist": True,
            },
            {"id": str(VIP_ROLE), "name": "VIP", "position": 2, "permissions": "0", "color": 255},
        ],
        "channels": [
            {"id": str(CATEGORY), "type": 4, "name": "Main", "position": 0},
            {
                "id": str(CHAT),
                "type": 0,
                "name": "chat",
                "position": 1,
                "parent_id": str(CATEGORY),
                "topic": "talk here",
                "rate_limit_per_user": 5,
                "permission_overwrites": [
                    {"id": str(GUILD), "type": 0, "allow": "0", "deny": "1024"},
                    {"id": str(AUTHOR), "type": 1, "allow": "1024", "deny": "0"},
                ],
            },
            {
                "id": str(VOICE),
                "type": 2,
                "name": "Lounge",
                "position": 2,
                "parent_id": str(CATEGORY),
                "bitrate": 96000,
                "user_limit": 10,
                "video_quality_mode": 2,
            },
            {
                "id": str(STAGE),
                "type": 13,
                "name": "Town hall",
                "position": 3,
                "parent_id": str(CATEGORY),
                "bitrate": 64000,
                "topic": "AMA",
            },
            {
                "id": str(FORUM),
                "type": 15,
                "name": "help",
                "position": 4,
                "parent_id": str(CATEGORY),
                "topic": "be nice",
                "flags": 16,
                "available_tags": [
                    {"id": str(TAG), "name": "solved", "moderated": True, "emoji_name": "✅"}
                ],
                "default_reaction_emoji": {"emoji_id": None, "emoji_name": "👍"},
            },
            {"id": str(LOOSE), "type": 0, "name": "loose", "position": 5},
        ],
        "threads": [
            {
                "id": str(POST),
                "type": 11,
                "name": "how do I",
                "parent_id": str(FORUM),
                "applied_tags": [str(TAG)],
                "thread_metadata": {"archived": False},
            },
            {
                "id": str(CHAT_THREAD),
                "type": 11,
                "name": "side chat",
                "parent_id": str(CHAT),
                "thread_metadata": {"archived": True},
            },
        ],
        "emojis": [{"id": "800000000000000001", "name": "blob", "animated": True}],
        "stickers": [{"id": str(GUILD_STICKER), "name": "wave", "format_type": 1, "type": 2}],
    }


def message_create(**overrides):
    data = {
        "id": str(MSG),
        "type": 0,
        "channel_id": str(CHAT),
        "guild_id": str(GUILD),
        "timestamp": "2026-01-02T03:04:05.000000+00:00",
        "content": "hello there",
        "author": user(AUTHOR, "author", global_name="Author G"),
        "member": {"nick": "Boss", "roles": [str(MOD_ROLE), str(VIP_ROLE)]},
        "mentions": [],
        "mention_roles": [],
        "attachments": [
            {
                "id": "600000000000000001",
                "filename": "cat.png",
                "url": "https://cdn.discordapp.com/attachments/1/2/cat.png",
                "size": 1234,
                "content_type": "image/png",
            }
        ],
        "embeds": [{"title": "card", "description": "body"}],
    }
    data.update(overrides)
    return data


@pytest.fixture()
def guild():
    return Guild(guild_payload())


# ---------------------------------------------------------------------------
# forwarding
# ---------------------------------------------------------------------------


def _manager():
    mgr = ForwardingManager.__new__(ForwardingManager)
    mgr.log = logging.getLogger("test.forwarding")
    mgr._started = True
    return mgr


class TestForwardingOnSelfclientMessages:
    def test_message_attributes_from_a_message_create(self, guild):
        msg = Message(message_create(), guild=guild)

        attrs = ForwardingManager._get_message_attributes(None, msg)

        assert attrs["message_id"] == MSG
        assert attrs["guild_id"] == GUILD
        assert attrs["channel_id"] == CHAT
        assert attrs["channel_name"] == "chat"
        assert attrs["author_id"] == AUTHOR
        assert attrs["author_name"] == "Boss"
        assert attrs["role_ids"] == [GUILD, MOD_ROLE, VIP_ROLE]
        assert attrs["is_bot"] is False
        assert attrs["is_system"] is False
        assert attrs["message_type"] == "default"
        assert attrs["message_type_id"] == 0
        assert attrs["content"] == "hello there"
        assert attrs["attachments"] == [
            {
                "url": "https://cdn.discordapp.com/attachments/1/2/cat.png",
                "filename": "cat.png",
                "size": 1234,
                "content_type": "image/png",
            }
        ]
        assert attrs["has_attachments"] is True
        assert attrs["embeds"] == [{"title": "card", "description": "body"}]
        assert attrs["jump_url"] == f"https://discord.com/channels/{GUILD}/{CHAT}/{MSG}"

    def test_webhook_author_is_a_plain_user_with_no_roles(self, guild):
        msg = Message(
            message_create(webhook_id="900000000000000001", member=None),
            guild=guild,
        )

        attrs = ForwardingManager._get_message_attributes(None, msg)

        assert attrs["role_ids"] == []
        assert attrs["author_name"] == "Author G"

    def test_forward_wrapper_detection_reads_flags_off_the_wrapper(self, guild):
        wrapper = Message(
            message_create(content="", attachments=[], embeds=[], flags=16384),
            guild=guild,
        )
        plain = Message(message_create(), guild=guild)

        mgr = _manager()
        assert mgr._looks_like_forward_wrapper(wrapper) is True
        assert mgr._looks_like_forward_wrapper(plain) is False

    @pytest.mark.asyncio
    async def test_handle_message_dispatches_matching_rules(self, guild):
        mgr = _manager()

        def rule(rule_id, role_id):
            return ForwardingRule(
                rule_id=rule_id,
                guild_id=GUILD,
                label=rule_id,
                provider="discord",
                enabled=True,
                config={"urls": ["https://discord.com/api/webhooks/1/abc"]},
                filters=ForwardingFilters.from_dict({"include_roles": [role_id]}),
            )

        dispatched = []

        async def rules_for(gid):
            assert gid == GUILD
            return [rule("r1", VIP_ROLE), rule("r2", 999)]

        async def dispatch(r, attrs):
            dispatched.append((r.rule_id, attrs))

        mgr._get_rules_for_guild = rules_for
        mgr._dispatch_forwarding = dispatch

        await mgr.handle_new_message(
            discord_message=Message(message_create(), guild=guild), bot=None
        )

        assert [rid for rid, _ in dispatched] == ["r1"]
        assert dispatched[0][1]["content"] == "hello there"
        assert dispatched[0][1]["channel_id"] == CHAT


# ---------------------------------------------------------------------------
# sitemap
# ---------------------------------------------------------------------------


class _SitemapDB:
    def __init__(self, thread_rows=()):
        self.thread_rows = list(thread_rows)

    def get_filters(self, original_guild_id):
        empty = {"category": [], "channel": []}
        return {"whitelist": dict(empty), "exclude": dict(empty)}

    def get_all_threads(self):
        return self.thread_rows

    def list_guild_mappings(self):
        return []


class _SitemapBot:
    """The slice of selfclient.Client the sitemap calls."""

    def __init__(self, guild):
        self.guild = guild
        self.calls = []

    @property
    def guilds(self):
        return [self.guild]

    def get_guild(self, gid):
        return self.guild if int(gid) == self.guild.id else None

    async def fetch_stickers(self, guild):
        self.calls.append(("fetch_stickers", guild.id))
        from copycord.selfclient.models import Sticker

        return [
            Sticker({"id": str(GUILD_STICKER), "name": "wave", "format_type": 1, "type": 2}),
            Sticker({"id": str(STANDARD_STICKER), "name": "hello", "format_type": 3, "type": 1}),
        ]

    async def fetch_roles(self, guild):
        self.calls.append(("fetch_roles", guild.id))
        raw = [dict(r) for r in guild_payload()["roles"]]
        raw[1]["colors"] = {
            "primary_color": 16711680,
            "secondary_color": 255,
            "tertiary_color": None,
        }
        return [Role(r, guild) for r in raw]

    async def fetch_channel(self, cid):
        self.calls.append(("fetch_channel", cid))
        raise Forbidden(403, {"code": 50001, "message": "Missing Access"})


def _service(guild, thread_rows=()):
    return SitemapService(
        bot=_SitemapBot(guild),
        config=None,
        db=_SitemapDB(thread_rows),
        ws=None,
        logger=logging.getLogger("test.sitemap"),
    )


class TestSitemapClassification:
    def test_canned_payload_builds_the_expected_channel_kinds(self, guild):
        assert isinstance(guild.get_channel(CATEGORY), CategoryChannel)
        assert isinstance(guild.get_channel(CHAT), TextChannel)
        assert isinstance(guild.get_channel(VOICE), VoiceChannel)
        assert isinstance(guild.get_channel(STAGE), StageChannel)
        assert isinstance(guild.get_channel(FORUM), ForumChannel)
        assert isinstance(guild.get_channel(POST), Thread)

    @pytest.mark.asyncio
    async def test_build_for_guild_sorts_channels_by_kind(self, guild):
        svc = _service(
            guild,
            thread_rows=[
                {
                    "original_thread_id": str(POST),
                    "forum_original_id": str(FORUM),
                    "original_guild_id": str(GUILD),
                },
                {
                    "original_thread_id": str(CHAT_THREAD),
                    "forum_original_id": None,
                    "original_guild_id": str(GUILD),
                },
                {
                    "original_thread_id": "399999999999999999",
                    "forum_original_id": None,
                    "original_guild_id": str(GUILD),
                },
            ],
        )

        sm = await svc.build_for_guild(guild)

        (cat,) = sm["categories"]
        assert cat["id"] == CATEGORY
        by_id = {c["id"]: c for c in cat["channels"]}
        assert set(by_id) == {CHAT, VOICE, STAGE}

        chat = by_id[CHAT]
        assert chat["type"] == 0
        assert chat["topic"] == "talk here"
        assert chat["slowmode_delay"] == 5
        # Only role overwrites are serialized, with raw int bits.
        assert chat["overwrites"] == [
            {"type": "role", "id": GUILD, "allow_bits": 0, "deny_bits": 1024}
        ]

        voice = by_id[VOICE]
        assert voice["type"] == 2
        assert voice["bitrate"] == 96000
        assert voice["user_limit"] == 10
        assert voice["video_quality"] == "full"
        assert "topic" not in voice

        # selfclient's StageChannel subclasses VoiceChannel; it must still get
        # the stage entry, with its topic.
        stage = by_id[STAGE]
        assert stage["type"] == 13
        assert stage["topic"] == "AMA"

        assert [c["id"] for c in sm["standalone_channels"]] == [LOOSE]

        (forum,) = sm["forums"]
        assert forum["id"] == FORUM
        assert forum["type"] == 15
        assert forum["category_id"] == CATEGORY
        assert forum["require_tag"] is True
        assert forum["default_reaction"] == {"id": None, "name": "👍", "animated": False}
        assert forum["available_tags"] == [
            {
                "id": TAG,
                "name": "solved",
                "moderated": True,
                "emoji_id": None,
                "emoji_name": "✅",
                "emoji_animated": False,
            }
        ]

        threads = {t["id"]: t for t in sm["threads"]}
        assert threads[POST] == {
            "id": POST,
            "forum_id": FORUM,
            "name": "how do I",
            "archived": False,
            "applied_tag_ids": [TAG],
        }
        assert threads[CHAT_THREAD]["archived"] is True
        assert 399999999999999999 not in threads
        assert ("fetch_channel", 399999999999999999) in svc.bot.calls

    @pytest.mark.asyncio
    async def test_build_for_guild_reads_stickers_and_role_colors_through_client(self, guild):
        svc = _service(guild)

        sm = await svc.build_for_guild(guild)

        assert ("fetch_stickers", GUILD) in svc.bot.calls
        assert ("fetch_roles", GUILD) in svc.bot.calls
        assert [s["id"] for s in sm["stickers"]] == [GUILD_STICKER]
        assert sm["stickers"][0]["format_type"] == 1

        roles = {r["id"]: r for r in sm["roles"]}
        assert roles[GUILD]["everyone"] is True
        assert roles[MOD_ROLE]["color"] == 16711680
        assert roles[MOD_ROLE]["permissions"] == 8
        assert roles[MOD_ROLE]["hoist"] is True
        assert roles[MOD_ROLE]["colors"] == {
            "primary_color": 16711680,
            "secondary_color": 255,
            "tertiary_color": None,
        }
        assert roles[VIP_ROLE]["colors"] is None
        assert [r["id"] for r in sm["roles"]] == [GUILD, MOD_ROLE, VIP_ROLE]

        assert sm["guild"]["owner_id"] is None
        assert sm["guild"]["preferred_locale"] == "en-US"
        assert sm["guild"]["system_channel_id"] == CHAT
        assert sm["community"] == {
            "enabled": True,
            "rules_channel_id": CHAT,
            "public_updates_channel_id": None,
        }

    def test_in_scope_channel_classifies_threads_and_categories(self, guild):
        svc = _service(guild)
        excluded = {
            "include_category_ids": set(),
            "include_channel_ids": set(),
            "excluded_category_ids": {CATEGORY},
            "excluded_channel_ids": set(),
            "whitelist_enabled": False,
        }
        svc._build_filter_view_for_guild = lambda gid: excluded

        assert svc.in_scope_channel(guild.get_channel(CATEGORY)) is False
        assert svc.in_scope_channel(guild.get_channel(POST)) is False
        assert svc.in_scope_thread(guild.get_channel(CHAT_THREAD)) is False
        assert svc.in_scope_channel(guild.get_channel(LOOSE)) is True

    def test_role_change_is_relevant_on_role_wrappers(self, guild):
        svc = _service(guild)
        before = guild.get_role(MOD_ROLE)
        recoloured = Role({**guild_payload()["roles"][1], "color": 1}, guild)
        same = Role(dict(guild_payload()["roles"][1]), guild)

        assert svc.role_change_is_relevant(before, recoloured) is True
        assert svc.role_change_is_relevant(before, same) is False
        assert svc.role_change_is_relevant(before, guild.default_role) is False


# ---------------------------------------------------------------------------
# export runners
# ---------------------------------------------------------------------------


class _HistoryBot:
    """Records the Client.history / archived_threads calls the runners make."""

    def __init__(self, pages=None, threads=None):
        self.pages = list(pages or [])
        self.threads = threads or {}
        self.history_calls = []
        self.archived_calls = []
        self.private_channels = []

    def get_user(self, uid):
        return None

    async def history(self, channel, **kw):
        self.history_calls.append(kw)
        page = self.pages.pop(0) if self.pages else []
        for m in page:
            yield m

    async def archived_threads(self, channel, **kw):
        self.archived_calls.append((channel.id, kw))
        for th in self.threads.get((kw.get("private"), kw.get("joined")), []):
            yield th


def _engine(bot):
    eng = BackfillEngine.__new__(BackfillEngine)
    eng.bot = bot
    eng.logger = logging.getLogger("test.backfill")
    eng.PAGE_DELAY = 0
    return eng


class TestExportRunnersOnClient:
    def test_archived_threads_only_for_text_and_forum(self, guild):
        assert _has_archived_threads(guild.get_channel(CHAT)) is True
        assert _has_archived_threads(guild.get_channel(FORUM)) is True
        assert _has_archived_threads(guild.get_channel(VOICE)) is False
        assert _has_archived_threads(guild.get_channel(POST)) is False

    def test_forum_refuses_private_archived_lists(self, guild):
        with pytest.raises(TypeError):
            _archived_threads(_HistoryBot(), guild.get_channel(FORUM), private=True)
        with pytest.raises(TypeError):
            _archived_threads(_HistoryBot(), guild.get_channel(FORUM), joined=True)

    @pytest.mark.asyncio
    async def test_iter_all_threads_walks_all_three_lists_through_client(self, guild):
        old_public = channel_from_dict(
            {"id": "310000000000000001", "type": 11, "parent_id": str(CHAT)}, guild
        )
        old_private = channel_from_dict(
            {"id": "310000000000000002", "type": 12, "parent_id": str(CHAT)}, guild
        )
        bot = _HistoryBot(
            threads={(False, False): [old_public], (True, False): [old_private], (True, True): [old_private]}
        )
        eng = _engine(bot)
        chat = guild.get_channel(CHAT)

        got = [t.id async for t in eng._iter_all_threads(chat)]

        assert got == [CHAT_THREAD, 310000000000000001, 310000000000000002]
        assert [(c, kw["private"], kw["joined"], kw["raise_on_error"]) for c, kw in bot.archived_calls] == [
            (CHAT, False, False, True),
            (CHAT, True, False, True),
            (CHAT, True, True, True),
        ]

    @pytest.mark.asyncio
    async def test_iter_all_threads_on_a_forum_stops_after_public(self, guild):
        bot = _HistoryBot()
        eng = _engine(bot)

        _ = [t async for t in eng._iter_all_threads(guild.get_channel(FORUM))]

        assert [(kw["private"], kw["joined"]) for _, kw in bot.archived_calls] == [(False, False)]

    @pytest.mark.asyncio
    async def test_resumable_history_pages_on_selfclient_objects(self, guild):
        first = [Message(message_create(id=str(MSG + i)), guild=guild) for i in range(2)]
        bot = _HistoryBot(pages=[first, []])
        eng = _engine(bot)

        got = [
            m.id
            async for m in eng._iter_history_resumable(
                guild.get_channel(CHAT),
                n=None,
                after_dt=None,
                before_dt=None,
                oldest_first=True,
                after_obj_id=MSG - 1,
            )
        ]

        assert got == [MSG, MSG + 1]
        assert all(kw["raise_on_error"] is True for kw in bot.history_calls)
        first_after = bot.history_calls[0]["after"]
        assert isinstance(first_after, Object) and first_after.id == MSG - 1
        resume_after = bot.history_calls[1]["after"]
        assert isinstance(resume_after, Object) and resume_after.id == MSG + 1

    @pytest.mark.asyncio
    async def test_find_dm_channel_skips_group_dms(self):
        dm = channel_from_dict({"id": "320000000000000001", "type": 1, "recipients": [user(FRIEND, "friend")]})
        group = channel_from_dict(
            {"id": "320000000000000002", "type": 3, "recipients": [user(FRIEND, "friend")]}
        )
        assert isinstance(dm, DMChannel) and isinstance(group, GroupChannel)
        bot = _HistoryBot()
        bot.private_channels = [group, dm]
        exporter = DmHistoryExporter.__new__(DmHistoryExporter)
        exporter.bot = bot

        found, who = await exporter._find_dm_channel(FRIEND)

        assert found is dm
        assert who.id == FRIEND


# ---------------------------------------------------------------------------
# selfclient: the oldest-first bounded walk export_runners relies on
# ---------------------------------------------------------------------------


class TestMessagesAfter:
    @pytest.mark.asyncio
    async def test_pages_forward_on_after_and_stops_at_ceiling(self):
        client = HTTPClient("token-abc", page_delay=0)
        seen = []

        async def fake_get(path, params=None, referer=None, **kw):
            seen.append(dict(params))
            after = int(params["after"])
            ids = [after + i for i in range(1, params["limit"] + 1)]
            # Discord returns the page newest first even on after.
            return [{"id": str(i)} for i in reversed(ids)]

        client.get = fake_get

        got = [int(m["id"]) async for m in client.messages_after(1, limit=3, after_id=100)]
        assert got == [101, 102, 103]
        assert seen == [{"limit": 3, "after": "100"}]

        seen.clear()
        got = [int(m["id"]) async for m in client.messages_after(1, after_id=0, before_id=150)]
        assert got == list(range(1, 150))
        assert [p["after"] for p in seen] == ["0", "100"]
