"""
Forwarding Discord's own system messages (joins, boosts, pins, thread creation).

These messages carry no content of their own -- Discord renders the visible
text on the client from the message type -- so the forwarder saw them as empty
and sent them on as a bare "New message" placeholder, with no way to turn them
off. They now have a filter of their own and carry their rendered text.

The tests call the real filter and the real attribute builder.
"""

import pytest

pytest.importorskip("discord", reason="client module needs a discord library present")

import discord

from client.forwarding import ForwardingFilters, ForwardingManager

CHANNEL = 4001
GUILD = 5001
AUTHOR = 6001


class _Guild:
    def __init__(self, gid=GUILD):
        self.id = gid


class _Channel:
    def __init__(self, cid=CHANNEL, name="general"):
        self.id = cid
        self.name = name


class _Author:
    def __init__(self, uid=AUTHOR, name="Tunmise", bot=False):
        self.id = uid
        self.name = name
        self.display_name = name
        self.bot = bot
        self.roles = []


class _Message:
    """Enough of discord.Message for the attribute builder.

    is_system() is discord.py's own implementation, bound to this object, so
    the type classification under test is the library's and not a guess about
    it -- it deliberately treats replies and slash-command responses as normal
    messages, which a naive "type != default" check would get wrong.
    """

    def __init__(self, *, msg_type, content="", system_content="", bot=False):
        self.id = 90001
        self.type = msg_type
        self.content = content
        self.system_content = system_content
        self.guild = _Guild()
        self.channel = _Channel()
        self.author = _Author(bot=bot)
        self.attachments = []
        self.embeds = []
        self.jump_url = "https://discord.com/channels/1/2/3"

    def is_system(self):
        return discord.Message.is_system(self)


def _attrs(message):
    return ForwardingManager._get_message_attributes(None, message)


def _join_message():

    return _Message(
        msg_type=discord.MessageType.new_member,
        content="",
        system_content="Yay you made it, Tunmise!",
    )


def _filters(**kw):
    return ForwardingFilters.from_dict(kw)


class TestSystemMessagesAreRecognised:
    def test_a_join_is_flagged_as_a_system_message(self):
        assert _attrs(_join_message())["is_system"] is True

    def test_a_normal_message_is_not(self):
        msg = _Message(msg_type=discord.MessageType.default, content="hello")
        assert _attrs(msg)["is_system"] is False

    def test_a_reply_is_not_a_system_message(self):

        msg = _Message(msg_type=discord.MessageType.reply, content="hello")
        assert _attrs(msg)["is_system"] is False

    def test_the_message_type_is_reported(self):
        reported = _attrs(_join_message())["message_type"]
        assert reported == discord.MessageType.new_member.name
        assert isinstance(reported, str)
        assert "MessageType" not in reported
        assert not reported.isdigit()

    def test_the_wire_value_is_reported(self):
        assert _attrs(_join_message())["message_type_id"] == 7


class TestTheTextIsRecovered:
    def test_a_join_carries_its_rendered_text(self):

        assert _attrs(_join_message())["content"] == "Yay you made it, Tunmise!"

    def test_a_normal_message_keeps_its_own_content(self):
        msg = _Message(
            msg_type=discord.MessageType.default,
            content="real text",
            system_content="real text",
        )
        assert _attrs(msg)["content"] == "real text"

    def test_content_wins_when_a_system_message_has_some(self):

        msg = _Message(
            msg_type=discord.MessageType.channel_name_change,
            content="new-name",
            system_content="x changed the channel name: **new-name**",
        )
        assert _attrs(msg)["content"] == "new-name"

    def test_a_system_message_with_no_text_at_all_is_still_safe(self):
        msg = _Message(msg_type=discord.MessageType.pins_add)
        assert _attrs(msg)["content"] == ""


class TestTheFilter:
    def test_system_messages_are_off_by_default(self):

        assert _filters().apply(_attrs(_join_message())) is False

    def test_turning_it_on_lets_them_through(self):
        assert _filters(include_system=True).apply(_attrs(_join_message())) is True

    def test_normal_messages_are_unaffected_when_it_is_off(self):
        msg = _Message(msg_type=discord.MessageType.default, content="hello")
        assert _filters().apply(_attrs(msg)) is True

    def test_it_does_not_imply_bot_messages(self):
        # The two switches are independent: opting into Discord's notices must

        msg = _Message(msg_type=discord.MessageType.default, content="hi", bot=True)
        assert _filters(include_system=True).apply(_attrs(msg)) is False

    def test_bot_messages_alone_do_not_admit_system_messages(self):
        assert _filters(include_bots=True).apply(_attrs(_join_message())) is False

    def test_the_flag_is_read_from_the_stored_shape(self):

        assert _filters(include_system=True).include_system is True
        assert _filters(include_system=False).include_system is False
        assert _filters().include_system is False


class TestOtherFiltersStillApplyToSystemMessages:
    def test_keywords_match_against_the_rendered_text(self):

        attrs = _attrs(_join_message())
        assert _filters(include_system=True, include_keywords=["made it"]).apply(attrs)
        assert not _filters(include_system=True, include_keywords=["boosted"]).apply(
            attrs
        )

    def test_channel_scoping_still_applies(self):
        attrs = _attrs(_join_message())
        assert not _filters(include_system=True, include_channels=[CHANNEL + 1]).apply(
            attrs
        )
        assert _filters(include_system=True, include_channels=[CHANNEL]).apply(attrs)

    def test_an_attachment_requirement_still_excludes_them(self):
        attrs = _attrs(_join_message())
        assert not _filters(include_system=True, has_attachments=True).apply(attrs)


class TestNormalizerKeepsTheKey:
    def test_the_admin_layer_round_trips_the_flag(self):

        from admin.app import _normalize_forwarding_rule_filters as norm

        assert norm({"include_system": "on"})["include_system"] is True
        assert norm({"include_system": True})["include_system"] is True
        assert norm({})["include_system"] is False
        assert norm(None)["include_system"] is False
