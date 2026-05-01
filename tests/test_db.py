"""
Unit tests for common.db.DBManager.

Tests schema creation, CRUD operations for core mapping tables,
config management, event logs, and edge cases.
"""
import sqlite3
import pytest


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

class TestSchemaInit:
    """Verify the schema is created correctly."""

    def test_tables_created(self, db):
        tables = {
            r[0]
            for r in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        expected = {
            "guild_mappings",
            "app_config",
            "filters",
            "category_mappings",
            "channel_mappings",
            "threads",
            "emoji_mappings",
            "sticker_mappings",
            "role_mappings",
            "settings",
            "blocked_keywords",
            "messages",
            "event_logs",
            "backfill_runs",
            "message_forwarding",
        }
        assert expected.issubset(tables), f"Missing tables: {expected - tables}"

    def test_reinit_is_idempotent(self, db):
        """Calling _init_schema a second time should not fail or lose data."""
        db.set_config("test_key", "hello")
        db._init_schema()
        assert db.get_config("test_key") == "hello"


# ---------------------------------------------------------------------------
# App config
# ---------------------------------------------------------------------------

class TestAppConfig:

    def test_set_and_get(self, db):
        db.set_config("FOO", "bar")
        assert db.get_config("FOO") == "bar"

    def test_get_default(self, db):
        assert db.get_config("MISSING", "fallback") == "fallback"

    def test_overwrite(self, db):
        db.set_config("KEY", "v1")
        db.set_config("KEY", "v2")
        assert db.get_config("KEY") == "v2"

    def test_get_all_config(self, db):
        db.set_config("A", "1")
        db.set_config("B", "2")
        cfg = db.get_all_config()
        assert cfg["A"] == "1"
        assert cfg["B"] == "2"


# ---------------------------------------------------------------------------
# Settings (version tracking)
# ---------------------------------------------------------------------------

class TestSettings:

    def test_initial_version_empty(self, db):
        assert db.get_version() == ""

    def test_set_and_get_version(self, db):
        db.set_version("v3.20.0")
        assert db.get_version() == "v3.20.0"

    def test_set_and_get_notified_version(self, db):
        db.set_notified_version("v3.21.0")
        assert db.get_notified_version() == "v3.21.0"


# ---------------------------------------------------------------------------
# Guild mappings
# ---------------------------------------------------------------------------

class TestGuildMappings:

    def _create_mapping(self, db, **overrides):
        defaults = dict(
            mapping_id="test-mapping-1",
            mapping_name="Test Mapping",
            original_guild_id=111,
            original_guild_name="Source Server",
            original_guild_icon_url=None,
            cloned_guild_id=222,
            cloned_guild_name="Clone Server",
        )
        defaults.update(overrides)
        return db.upsert_guild_mapping(**defaults)

    def test_create_and_list(self, db):
        mid = self._create_mapping(db)
        assert mid == "test-mapping-1"
        mappings = db.list_guild_mappings()
        assert len(mappings) == 1
        assert mappings[0]["mapping_name"] == "Test Mapping"
        assert mappings[0]["original_guild_id"] == "111"
        assert mappings[0]["cloned_guild_id"] == "222"

    def test_get_by_id(self, db):
        self._create_mapping(db)
        m = db.get_mapping_by_id("test-mapping-1")
        assert m is not None
        assert m["mapping_name"] == "Test Mapping"

    def test_get_by_id_missing(self, db):
        assert db.get_mapping_by_id("nonexistent") is None

    def test_get_by_original(self, db):
        self._create_mapping(db)
        m = db.get_mapping_by_original(111)
        assert m is not None
        assert int(m["cloned_guild_id"]) == 222

    def test_get_by_clone(self, db):
        self._create_mapping(db)
        m = db.get_mapping_by_clone(222)
        assert m is not None
        assert int(m["original_guild_id"]) == 111

    def test_upsert_updates_name(self, db):
        self._create_mapping(db)
        self._create_mapping(db, mapping_name="Updated Name")
        m = db.get_mapping_by_id("test-mapping-1")
        assert m["mapping_name"] == "Updated Name"

    def test_delete(self, db):
        self._create_mapping(db)
        db.delete_guild_mapping("test-mapping-1")
        assert db.list_guild_mappings() == []

    def test_delete_nonexistent(self, db):
        # Should not raise
        db.delete_guild_mapping("nope")

    def test_get_clone_guild_ids(self, db):
        self._create_mapping(db)
        self._create_mapping(
            db,
            mapping_id="m2",
            cloned_guild_id=333,
        )
        ids = db.get_clone_guild_ids(111)
        assert sorted(ids) == [222, 333]

    def test_get_host_guild_ids(self, db):
        self._create_mapping(db)
        ids = db.get_host_guild_ids(222)
        assert ids == [111]

    def test_update_mapping_status(self, db):
        self._create_mapping(db)
        db.update_mapping_status("test-mapping-1", "paused")
        mappings = db.list_guild_mappings()
        assert mappings[0]["status"] == "paused"

    def test_toggle_status_back_to_active(self, db):
        self._create_mapping(db)
        db.update_mapping_status("test-mapping-1", "paused")
        db.update_mapping_status("test-mapping-1", "active")
        mappings = db.list_guild_mappings()
        assert mappings[0]["status"] == "active"

    def test_unique_constraint(self, db):
        self._create_mapping(db)
        with pytest.raises(sqlite3.IntegrityError):
            db.upsert_guild_mapping(
                mapping_id="different-id",
                mapping_name="Dupe",
                original_guild_id=111,
                original_guild_name="",
                original_guild_icon_url=None,
                cloned_guild_id=222,
                cloned_guild_name="",
            )

    def test_settings_json(self, db):
        self._create_mapping(db, settings={"CLONE_EMOJI": True})
        # list_guild_mappings parses settings JSON into a dict
        mappings = db.list_guild_mappings()
        assert mappings[0]["settings"] == {"CLONE_EMOJI": True}

    def test_active_guild_id_queries(self, db):
        self._create_mapping(db)
        assert 111 in db.get_all_original_guild_ids()
        assert 222 in db.get_all_clone_guild_ids()
        assert db.is_clone_guild_id(222) is True
        assert db.is_clone_guild_id(999) is False


# ---------------------------------------------------------------------------
# Category mappings
# ---------------------------------------------------------------------------

class TestCategoryMappings:

    def test_upsert_and_list(self, db):
        db.upsert_category_mapping(
            orig_id=100,
            orig_name="General",
            clone_id=200,
            clone_name="General-Clone",
            original_guild_id=1,
            cloned_guild_id=2,
        )
        cats = db.get_all_category_mappings()
        assert len(cats) == 1
        assert cats[0]["original_category_name"] == "General"

    def test_count(self, db):
        assert db.count_categories() == 0
        db.upsert_category_mapping(100, "Cat", 200, "Cat-C", 1, 2)
        assert db.count_categories() == 1

    def test_delete(self, db):
        db.upsert_category_mapping(100, "Cat", 200, "Cat-C", 1, 2)
        db.delete_category_mapping(100)
        assert db.count_categories() == 0


# ---------------------------------------------------------------------------
# Channel mappings
# ---------------------------------------------------------------------------

class TestChannelMappings:

    def _add_category(self, db):
        db.upsert_category_mapping(
            orig_id=10, orig_name="Cat", clone_id=20,
            original_guild_id=1, cloned_guild_id=2,
        )

    def test_upsert_and_list(self, db):
        self._add_category(db)
        db.upsert_channel_mapping(
            original_channel_id=1000,
            original_channel_name="general",
            cloned_channel_id=2000,
            channel_webhook_url="https://discord.com/api/webhooks/...",
            original_parent_category_id=10,
            cloned_parent_category_id=20,
            channel_type=0,
            original_guild_id=1,
            cloned_guild_id=2,
        )
        channels = db.get_all_channel_mappings()
        assert len(channels) == 1
        assert channels[0]["original_channel_name"] == "general"

    def test_get_by_clone_id(self, db):
        self._add_category(db)
        db.upsert_channel_mapping(1000, "gen", 2000, None, 10, 20, 0, original_guild_id=1, cloned_guild_id=2)
        row = db.get_channel_mapping_by_clone_id(2000)
        assert row is not None
        assert row["original_channel_id"] == 1000

    def test_get_original_channel_id(self, db):
        self._add_category(db)
        db.upsert_channel_mapping(1000, "gen", 2000, None, 10, 20, 0, original_guild_id=1, cloned_guild_id=2)
        assert db.get_original_channel_id(2000) == 1000
        assert db.get_original_channel_id(9999) is None

    def test_count(self, db):
        assert db.count_channels() == 0
        self._add_category(db)
        db.upsert_channel_mapping(1000, "gen", 2000, None, 10, 20, 0, original_guild_id=1, cloned_guild_id=2)
        assert db.count_channels() == 1

    def test_delete(self, db):
        self._add_category(db)
        db.upsert_channel_mapping(1000, "gen", 2000, None, 10, 20, 0, original_guild_id=1, cloned_guild_id=2)
        db.delete_channel_mapping(1000)
        assert db.count_channels() == 0

    def test_resolve_original_from_clone(self, db):
        self._add_category(db)
        db.upsert_channel_mapping(1000, "gen", 2000, None, 10, 20, 0, original_guild_id=1, cloned_guild_id=2)
        orig, clone, source = db.resolve_original_from_any_id(2000)
        assert orig == 1000
        assert clone == 2000
        assert source == "from_clone"

    def test_resolve_original_from_original(self, db):
        self._add_category(db)
        db.upsert_channel_mapping(1000, "gen", 2000, None, 10, 20, 0, original_guild_id=1, cloned_guild_id=2)
        orig, clone, source = db.resolve_original_from_any_id(1000)
        assert orig == 1000
        assert clone == 2000
        assert source == "from_original"

    def test_resolve_assumed_original(self, db):
        orig, clone, source = db.resolve_original_from_any_id(9999)
        assert orig == 9999
        assert clone is None
        assert source == "assumed_original"


# ---------------------------------------------------------------------------
# Role mappings
# ---------------------------------------------------------------------------

class TestRoleMappings:

    def test_upsert_and_list(self, db):
        db.upsert_role_mapping(
            500, "Admin", 600, "Admin-Clone",
            original_guild_id=1, cloned_guild_id=2,
        )
        roles = db.get_all_role_mappings()
        assert len(roles) == 1
        assert roles[0]["original_role_name"] == "Admin"

    def test_get_role_mapping(self, db):
        db.upsert_role_mapping(500, "Admin", 600, "Admin-Clone", original_guild_id=1, cloned_guild_id=2)
        row = db.get_role_mapping(500)
        assert row is not None

    def test_delete(self, db):
        db.upsert_role_mapping(500, "Admin", 600, "Admin-Clone", original_guild_id=1, cloned_guild_id=2)
        db.delete_role_mapping(500)
        assert db.get_all_role_mappings() == []


# ---------------------------------------------------------------------------
# Emoji mappings
# ---------------------------------------------------------------------------

class TestEmojiMappings:

    def test_upsert_and_list(self, db):
        db.upsert_emoji_mapping(
            700, "pepe", 800, "pepe",
            original_guild_id=1, cloned_guild_id=2,
        )
        emojis = db.get_all_emoji_mappings()
        assert len(emojis) == 1

    def test_get_emoji_mapping(self, db):
        db.upsert_emoji_mapping(700, "pepe", 800, "pepe", original_guild_id=1, cloned_guild_id=2)
        row = db.get_emoji_mapping(700)
        assert row is not None
        assert row["cloned_emoji_id"] == 800


# ---------------------------------------------------------------------------
# Sticker mappings
# ---------------------------------------------------------------------------

class TestStickerMappings:

    def test_upsert_and_list(self, db):
        db.upsert_sticker_mapping(
            900, "wave", 901, "wave",
            original_guild_id=1, cloned_guild_id=2,
        )
        stickers = db.get_all_sticker_mappings()
        assert len(stickers) == 1

    def test_delete(self, db):
        db.upsert_sticker_mapping(900, "wave", 901, "wave", original_guild_id=1, cloned_guild_id=2)
        db.delete_sticker_mapping(900)
        assert db.get_all_sticker_mappings() == []


# ---------------------------------------------------------------------------
# Blocked keywords
# ---------------------------------------------------------------------------

class TestBlockedKeywords:

    def test_add_and_check(self, db):
        added = db.add_blocked_keyword("spam", original_guild_id=1, cloned_guild_id=2)
        assert added is True

    def test_add_duplicate(self, db):
        db.add_blocked_keyword("spam", original_guild_id=1, cloned_guild_id=2)
        added = db.add_blocked_keyword("spam", original_guild_id=1, cloned_guild_id=2)
        assert added is False

    def test_add_normalizes_case(self, db):
        db.add_blocked_keyword("SPAM", original_guild_id=1, cloned_guild_id=2)
        added = db.add_blocked_keyword("spam", original_guild_id=1, cloned_guild_id=2)
        assert added is False

    def test_add_empty_string(self, db):
        added = db.add_blocked_keyword("", original_guild_id=1, cloned_guild_id=2)
        assert added is False


# ---------------------------------------------------------------------------
# Event logs
# ---------------------------------------------------------------------------

class TestEventLogs:

    def test_add_and_get(self, db):
        log_id = db.add_event_log(
            event_type="channel_create",
            details="Created #general",
            guild_id=111,
            guild_name="Test Guild",
        )
        assert log_id is not None
        logs = db.get_event_logs()
        assert len(logs) == 1
        assert logs[0]["event_type"] == "channel_create"
        assert logs[0]["details"] == "Created #general"

    def test_filter_by_type(self, db):
        db.add_event_log(event_type="channel_create", details="a")
        db.add_event_log(event_type="role_create", details="b")
        logs = db.get_event_logs(event_type="role_create")
        assert len(logs) == 1
        assert logs[0]["event_type"] == "role_create"

    def test_filter_by_guild(self, db):
        db.add_event_log(event_type="x", details="a", guild_id=1)
        db.add_event_log(event_type="x", details="b", guild_id=2)
        logs = db.get_event_logs(guild_id=1)
        assert len(logs) == 1

    def test_search(self, db):
        db.add_event_log(event_type="x", details="Created #announcements")
        db.add_event_log(event_type="x", details="Deleted #general")
        logs = db.get_event_logs(search="announcements")
        assert len(logs) == 1

    def test_count(self, db):
        db.add_event_log(event_type="x", details="a")
        db.add_event_log(event_type="y", details="b")
        assert db.count_event_logs() == 2
        assert db.count_event_logs(event_type="x") == 1

    def test_get_types(self, db):
        db.add_event_log(event_type="alpha", details="")
        db.add_event_log(event_type="beta", details="")
        db.add_event_log(event_type="alpha", details="")
        types = db.get_event_log_types()
        assert types == ["alpha", "beta"]

    def test_delete_single(self, db):
        log_id = db.add_event_log(event_type="x", details="a")
        assert db.delete_event_log(log_id) is True
        assert db.count_event_logs() == 0

    def test_delete_nonexistent(self, db):
        assert db.delete_event_log("nope") is False

    def test_delete_bulk(self, db):
        id1 = db.add_event_log(event_type="x", details="a")
        id2 = db.add_event_log(event_type="x", details="b")
        db.add_event_log(event_type="x", details="c")
        deleted = db.delete_event_logs_bulk([id1, id2])
        assert deleted == 2
        assert db.count_event_logs() == 1

    def test_clear(self, db):
        db.add_event_log(event_type="x", details="a")
        db.add_event_log(event_type="x", details="b")
        deleted = db.clear_event_logs()
        assert deleted == 2
        assert db.count_event_logs() == 0

    def test_pagination(self, db):
        for i in range(5):
            db.add_event_log(event_type="x", details=f"log {i}")
        logs = db.get_event_logs(limit=2, offset=0)
        assert len(logs) == 2
        logs = db.get_event_logs(limit=2, offset=3)
        assert len(logs) == 2

    def test_extra_json(self, db):
        log_id = db.add_event_log(
            event_type="x",
            details="test",
            extra={"foo": "bar", "count": 42},
        )
        logs = db.get_event_logs()
        assert logs[0]["extra_json"] is not None
        import json
        extra = json.loads(logs[0]["extra_json"])
        assert extra["foo"] == "bar"


# ---------------------------------------------------------------------------
# Message mappings
# ---------------------------------------------------------------------------

class TestMessageMappings:

    def test_upsert_and_get(self, db):
        db.upsert_message_mapping(
            1,     # original_guild_id
            100,   # original_channel_id
            5000,  # original_message_id
            200,   # cloned_channel_id
            6000,  # cloned_message_id
            "https://example.com/webhook",
            cloned_guild_id=2,
        )
        row = db.get_mapping_by_cloned(6000)
        assert row is not None

    def test_get_by_original(self, db):
        db.upsert_message_mapping(1, 100, 5000, 200, 6000, "", cloned_guild_id=2)
        rows = db.get_message_mappings_for_original(5000)
        assert len(rows) >= 1

    def test_delete(self, db):
        db.upsert_message_mapping(1, 100, 5000, 200, 6000, "", cloned_guild_id=2)
        deleted = db.delete_message_mapping(5000)
        assert deleted >= 1


# ---------------------------------------------------------------------------
# Thread mappings
# ---------------------------------------------------------------------------

class TestThreadMappings:

    def _setup_channel(self, db):
        db.upsert_category_mapping(10, "Cat", 20, "Cat-C", 1, 2)
        db.upsert_channel_mapping(100, "forum", 200, None, 10, 20, 15, original_guild_id=1, cloned_guild_id=2)

    def test_upsert_and_list(self, db):
        self._setup_channel(db)
        db.upsert_forum_thread_mapping(
            orig_thread_id=3000,
            orig_thread_name="Thread 1",
            clone_thread_id=4000,
            forum_orig_id=100,
            forum_clone_id=200,
            original_guild_id=1,
            cloned_guild_id=2,
        )
        threads = db.get_all_threads()
        assert len(threads) == 1
        assert threads[0]["original_thread_name"] == "Thread 1"

    def test_delete(self, db):
        self._setup_channel(db)
        db.upsert_forum_thread_mapping(3000, "Thread 1", 4000, 100, 200, original_guild_id=1, cloned_guild_id=2)
        db.delete_forum_thread_mapping(3000)
        assert db.get_all_threads() == []


# ---------------------------------------------------------------------------
# Cascade: deleting a guild mapping cleans child tables
# ---------------------------------------------------------------------------

class TestGuildMappingCascade:

    def test_delete_mapping_cleans_children(self, db):
        mid = db.upsert_guild_mapping(
            mapping_id="cascade-test",
            mapping_name="Cascade",
            original_guild_id=1,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=2,
            cloned_guild_name="",
        )

        db.upsert_category_mapping(10, "Cat", 20, "Cat-C", 1, 2)
        db.upsert_channel_mapping(100, "gen", 200, None, 10, 20, 0, original_guild_id=1, cloned_guild_id=2)
        db.upsert_role_mapping(500, "Admin", 600, "Admin-Clone", original_guild_id=1, cloned_guild_id=2)
        db.upsert_emoji_mapping(700, "pepe", 800, "pepe", original_guild_id=1, cloned_guild_id=2)
        db.add_event_log(event_type="x", details="y", guild_id=1)

        db.delete_guild_mapping(mid)

        assert len(db.get_all_category_mappings()) == 0
        assert len(db.get_all_channel_mappings()) == 0
        assert len(db.get_all_role_mappings()) == 0
        assert len(db.get_all_emoji_mappings()) == 0
        # Event logs are NOT cascade-deleted (they're audit records)
        assert db.count_event_logs() == 1
