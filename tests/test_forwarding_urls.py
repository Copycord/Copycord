"""
Unit tests for shared Discord webhook URL helpers in common.common_helpers.
"""
import os
import sys

CODE_DIR = os.path.join(os.path.dirname(__file__), "..", "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(CODE_DIR))

from common.common_helpers import (  # noqa: E402
    MAX_DISCORD_WEBHOOK_URLS,
    coerce_url_list,
    discord_urls_from_config,
    is_discord_webhook_url,
)

VALID_A = "https://discord.com/api/webhooks/111/aaa"
VALID_B = "https://ptb.discord.com/api/webhooks/222/bbb-_token"
VALID_C = "https://discordapp.com/api/webhooks/333/ccc"


class TestIsDiscordWebhookUrl:
    def test_accepts_canonical(self):
        assert is_discord_webhook_url(VALID_A) is True

    def test_accepts_ptb_and_canary_and_app(self):
        assert is_discord_webhook_url(VALID_B) is True
        assert is_discord_webhook_url(VALID_C) is True
        assert is_discord_webhook_url(
            "https://canary.discord.com/api/webhooks/4/d"
        ) is True

    def test_rejects_non_webhook(self):
        assert is_discord_webhook_url("https://example.com/hook") is False
        assert is_discord_webhook_url("not a url") is False
        assert is_discord_webhook_url("") is False


class TestCoerceUrlList:
    def test_csv_string(self):
        assert coerce_url_list(f"{VALID_A}, {VALID_B}") == [VALID_A, VALID_B]

    def test_whitespace_and_newlines(self):
        assert coerce_url_list(f"{VALID_A}\n{VALID_B}  {VALID_C}") == [
            VALID_A, VALID_B, VALID_C
        ]

    def test_list_input_trimmed(self):
        assert coerce_url_list([f"  {VALID_A}  ", "", VALID_B]) == [VALID_A, VALID_B]

    def test_dedup_preserves_order(self):
        assert coerce_url_list([VALID_B, VALID_A, VALID_B]) == [VALID_B, VALID_A]

    def test_non_iterable_returns_empty(self):
        assert coerce_url_list(None) == []
        assert coerce_url_list(123) == []


class TestDiscordUrlsFromConfig:
    def test_reads_urls_list(self):
        assert discord_urls_from_config({"urls": [VALID_A, VALID_B]}) == [VALID_A, VALID_B]

    def test_legacy_url_fallback(self):
        assert discord_urls_from_config({"url": VALID_A}) == [VALID_A]

    def test_urls_takes_precedence_over_legacy(self):
        assert discord_urls_from_config({"urls": [VALID_B], "url": VALID_A}) == [VALID_B]

    def test_empty_urls_falls_back_to_legacy(self):
        assert discord_urls_from_config({"urls": [], "url": VALID_A}) == [VALID_A]

    def test_missing_returns_empty(self):
        assert discord_urls_from_config({}) == []

    def test_max_constant(self):
        assert MAX_DISCORD_WEBHOOK_URLS == 10
