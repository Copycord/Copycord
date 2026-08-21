"""
Custom emoji an account without Nitro cannot send.

Discord does not reject these - it strips the `<...>` wrapper and delivers the
bare `:name:`, so a cloned message arrives reading "Hello all :wave:". We remove
them instead, but only the ones the account genuinely cannot use.
"""
import logging
import pytest

from server.token_sender import (
    SEND_OK,
    UserTokenSender,
    strip_unusable_emoji,
)

# Emoji that were cloned into the guild being posted into.
CLONE_EMOJI = {111, 222}
# Never cloned, so it still points at the source guild: cross-guild.
FOREIGN = 999


class TestStripUnusableEmoji:
    def test_keeps_static_emoji_owned_by_this_guild(self):
        # Any member may use their own guild's static emoji without Nitro.
        assert strip_unusable_emoji("hey <:wave:111> all", CLONE_EMOJI) == (
            "hey <:wave:111> all"
        )

    def test_drops_emoji_from_another_guild(self):
        assert strip_unusable_emoji(f"hey <:wave:{FOREIGN}> all", CLONE_EMOJI) == (
            "hey all"
        )

    def test_drops_animated_even_when_cloned(self):
        # Animated emoji need Nitro anywhere, including your own guild.
        assert strip_unusable_emoji("hey <a:party:111> all", CLONE_EMOJI) == "hey all"

    def test_mixed_message_keeps_only_what_works(self):
        text = f"<:a:111> x <:b:{FOREIGN}> y <a:c:222>"
        assert strip_unusable_emoji(text, CLONE_EMOJI) == "<:a:111> x y"

    def test_leaves_no_double_space_or_trailing_space(self):
        assert strip_unusable_emoji(f"a <:x:{FOREIGN}> b", CLONE_EMOJI) == "a b"
        assert strip_unusable_emoji(f"hello <:x:{FOREIGN}>", CLONE_EMOJI) == "hello"

    def test_leaves_everything_else_alone(self):
        assert strip_unusable_emoji("hey 👋 all", CLONE_EMOJI) == "hey 👋 all"
        assert strip_unusable_emoji("nothing here", CLONE_EMOJI) == "nothing here"
        assert strip_unusable_emoji("", CLONE_EMOJI) == ""

    def test_preserves_line_structure(self):
        text = f"line1 <:x:{FOREIGN}>\nline2"
        assert strip_unusable_emoji(text, CLONE_EMOJI) == "line1\nline2"


class _DB:
    def get_enabled_mapping_tokens(self, mapping_id):
        return [{"token_id": "t1", "token_value": "TOK", "username": "acct"}]

    def increment_mapping_token_usage(self, tid):
        return None

    def get_cloned_emoji_ids(self, guild_id):
        return {111}


def _sender(premium_type, *, premium_status=SEND_OK):
    """A sender whose /users/@me reports the given premium_type."""
    s = UserTokenSender(
        db=_DB(),
        ratelimit=None,
        action_type=None,
        session_provider=lambda: None,
        logger=logging.getLogger("test"),
    )
    calls = []
    sent = {}

    async def fake_request(token, url, *, json_body=None, mime_factory=None,
                           method="POST", **kw):
        calls.append(url)
        if url.endswith("/users/@me"):
            if premium_status != SEND_OK:
                return premium_status, None
            return SEND_OK, {"premium_type": premium_type}
        sent["content"] = (json_body or {}).get("content")
        return SEND_OK, {"id": "1"}

    async def fake_prepare(session, attachments):
        return [], set()

    s._request_with_token = fake_request
    s._prepare_files = fake_prepare
    return s, calls, sent


class TestSendStripsForNonNitro:
    @pytest.mark.asyncio
    async def test_non_nitro_drops_only_the_unusable_emoji(self):
        s, _, sent = _sender(0)
        await s._send_with_token(
            "TOK", 42, f"hey <:wave:111> and <:x:{FOREIGN}>", [], guild_id=5000
        )
        assert sent["content"] == "hey <:wave:111> and"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("premium", [1, 2, 3])
    async def test_nitro_of_any_tier_leaves_the_message_alone(self, premium):
        # Classic, Nitro and Basic all include custom emoji anywhere.
        s, _, sent = _sender(premium)
        text = f"hey <:wave:111> and <:x:{FOREIGN}>"
        await s._send_with_token("TOK", 42, text, [], guild_id=5000)
        assert sent["content"] == text

    @pytest.mark.asyncio
    async def test_message_without_emoji_never_looks_up_premium(self):
        # The lookup is a live request; ordinary messages must not pay for it.
        s, calls, _ = _sender(0)
        await s._send_with_token("TOK", 42, "plain message", [], guild_id=5000)
        assert not any(u.endswith("/users/@me") for u in calls)

    @pytest.mark.asyncio
    async def test_premium_is_looked_up_once_per_token(self):
        s, calls, _ = _sender(0)
        for _ in range(3):
            await s._send_with_token(
                "TOK", 42, f"<:x:{FOREIGN}> hi", [], guild_id=5000
            )
        assert sum(1 for u in calls if u.endswith("/users/@me")) == 1

    @pytest.mark.asyncio
    async def test_emoji_only_message_is_not_emptied(self):
        # Stripping everything would turn this into an empty send, which fails.
        # Delivering ":x:" is worse than the original but better than nothing.
        s, _, sent = _sender(0)
        status, _mid = await s._send_with_token(
            "TOK", 42, f"<:x:{FOREIGN}>", [], guild_id=5000
        )
        assert sent["content"] == f"<:x:{FOREIGN}>"
        assert status == SEND_OK

    @pytest.mark.asyncio
    async def test_unresolved_premium_leaves_the_message_intact(self):
        # A wrongly-kept emoji renders as :name:; a wrongly-stripped one is
        # gone for good, so an unknown answer must not strip.
        s, _, sent = _sender(0, premium_status="transient")
        text = f"hey <:x:{FOREIGN}>"
        await s._send_with_token("TOK", 42, text, [], guild_id=5000)
        assert sent["content"] == text
