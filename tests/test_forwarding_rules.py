"""
Unit tests for ForwardingManager Discord multi-URL parsing and routing.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

CODE_DIR = os.path.join(os.path.dirname(__file__), "..", "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(CODE_DIR))

from client.forwarding import ForwardingManager  # noqa: E402

VALID_A = "https://discord.com/api/webhooks/111/aaa"
VALID_B = "https://discord.com/api/webhooks/222/bbb"


def _manager(db):
    return ForwardingManager(
        config=SimpleNamespace(),
        db=db,
        ws=SimpleNamespace(),
        loop=asyncio.new_event_loop(),
    )


def _item(config):
    return {
        "rule_id": "r1",
        "guild_id": 1,
        "label": "L",
        "provider": "discord",
        "enabled": True,
        "config": config,
        "filters": {},
    }


class TestParseRuleMultiUrl:
    def test_urls_list_parsed(self, db):
        rule = _manager(db)._parse_rule(_item({"urls": [VALID_A, VALID_B]}))
        assert rule is not None
        assert rule.config["urls"] == [VALID_A, VALID_B]

    def test_legacy_url_normalized_to_urls(self, db):
        rule = _manager(db)._parse_rule(_item({"url": VALID_A}))
        assert rule is not None
        assert rule.config["urls"] == [VALID_A]
        assert "url" not in rule.config

    def test_invalid_urls_dropped(self, db):
        rule = _manager(db)._parse_rule(
            _item({"urls": [VALID_A, "https://example.com/x"]})
        )
        assert rule.config["urls"] == [VALID_A]

    def test_no_valid_urls_skips_rule(self, db):
        assert _manager(db)._parse_rule(_item({"urls": ["https://example.com/x"]})) is None
        assert _manager(db)._parse_rule(_item({"urls": []})) is None

    def test_caps_at_max(self, db):
        many = [f"https://discord.com/api/webhooks/{i}/tok" for i in range(15)]
        rule = _manager(db)._parse_rule(_item({"urls": many}))
        assert len(rule.config["urls"]) == 10


class TestQueueForRule:
    def test_routes_discord_when_valid(self, db):
        rule = _manager(db)._parse_rule(_item({"urls": [VALID_A]}))
        assert _manager(db)._queue_for_rule(rule) == "discord"
