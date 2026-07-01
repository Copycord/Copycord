"""
Tests for host-message routing: which messages must go via webhook rather than
a user token (bot/webhook-authored messages and rich embeds).
"""

from server.helpers import host_message_needs_webhook


def test_plain_user_message_uses_token():
    assert host_message_needs_webhook({"content": "hi"}) is False


def test_bot_authored_message_needs_webhook():
    assert host_message_needs_webhook({"is_bot": True}) is True


def test_webhook_authored_message_needs_webhook():
    assert host_message_needs_webhook({"webhook_id": 123456}) is True


def test_rich_embed_needs_webhook():
    msg = {"content": "", "embeds": [{"type": "rich", "title": "Announcement"}]}
    assert host_message_needs_webhook(msg) is True


def test_auto_link_preview_stays_on_token():
    # A user posting a link produces an auto-embed (not "rich"); the URL is in
    # the text and the clone regenerates the preview, so this still uses a token.
    msg = {"content": "https://y", "embeds": [{"type": "video", "url": "https://y"}]}
    assert host_message_needs_webhook(msg) is False


def test_non_dict_is_safe():
    assert host_message_needs_webhook(None) is False
