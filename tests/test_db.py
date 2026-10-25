"""Unit tests for copycord.common.db.DBManager."""
import sqlite3
import pytest


# Schema initialisation

class TestSchemaInit:
    """Verify the schema is created correctly."""

    def test_reinit_is_idempotent(self, db):
        """Calling _init_schema a second time should not fail or lose data."""
        db.set_config("test_key", "hello")
        db._init_schema()
        assert db.get_config("test_key") == "hello"


# App config

class TestAppConfig:

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


# Settings (version tracking)

class TestSettings:

    def test_set_and_get_version(self, db):
        db.set_version("v3.20.0")
        assert db.get_version() == "v3.20.0"

# Guild mappings

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

    def test_get_by_original(self, db):
        self._create_mapping(db)
        m = db.get_mapping_by_original(111)
        assert m is not None
        assert int(m["cloned_guild_id"]) == 222

    def test_upsert_updates_name(self, db):
        self._create_mapping(db)
        self._create_mapping(db, mapping_name="Updated Name")
        m = db.get_mapping_by_id("test-mapping-1")
        assert m["mapping_name"] == "Updated Name"

    def test_delete(self, db):
        self._create_mapping(db)
        db.delete_guild_mapping("test-mapping-1")
        assert db.list_guild_mappings() == []

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


# Category mappings

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

    def test_delete(self, db):
        db.upsert_category_mapping(100, "Cat", 200, "Cat-C", 1, 2)
        db.delete_category_mapping(100)
        assert db.count_categories() == 0


# Channel mappings

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

    def test_get_original_channel_id(self, db):
        self._add_category(db)
        db.upsert_channel_mapping(1000, "gen", 2000, None, 10, 20, 0, original_guild_id=1, cloned_guild_id=2)
        assert db.get_original_channel_id(2000) == 1000
        assert db.get_original_channel_id(9999) is None

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


# Role mappings

class TestRoleMappings:

    def test_upsert_and_list(self, db):
        db.upsert_role_mapping(
            500, "Admin", 600, "Admin-Clone",
            original_guild_id=1, cloned_guild_id=2,
        )
        roles = db.get_all_role_mappings()
        assert len(roles) == 1
        assert roles[0]["original_role_name"] == "Admin"

    def test_delete(self, db):
        db.upsert_role_mapping(500, "Admin", 600, "Admin-Clone", original_guild_id=1, cloned_guild_id=2)
        db.delete_role_mapping(500)
        assert db.get_all_role_mappings() == []


# Emoji mappings

class TestEmojiMappings:

    def test_get_emoji_mapping(self, db):
        db.upsert_emoji_mapping(700, "pepe", 800, "pepe", original_guild_id=1, cloned_guild_id=2)
        row = db.get_emoji_mapping(700)
        assert row is not None
        assert row["cloned_emoji_id"] == 800


# Sticker mappings

class TestStickerMappings:

    def test_delete(self, db):
        db.upsert_sticker_mapping(900, "wave", 901, "wave", original_guild_id=1, cloned_guild_id=2)
        db.delete_sticker_mapping(900)
        assert db.get_all_sticker_mappings() == []


# Blocked keywords

class TestBlockedKeywords:

    def test_add_normalizes_case(self, db):
        db.add_blocked_keyword("SPAM", original_guild_id=1, cloned_guild_id=2)
        added = db.add_blocked_keyword("spam", original_guild_id=1, cloned_guild_id=2)
        assert added is False

    def test_add_empty_string(self, db):
        added = db.add_blocked_keyword("", original_guild_id=1, cloned_guild_id=2)
        assert added is False


# Event logs

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

    def test_search_matches_metadata_values_and_ids(self, db):
        db.add_event_log(event_type="guild_metadata", details="Updated guild metadata: icon",
                         guild_id=1442034337590677500,
                         extra={"sync_task": "yi8h5", "changed_fields": ["icon"]})
        db.add_event_log(event_type="x", details="other", extra={"note": "nothing"})
        db.conn.execute("UPDATE event_logs SET extra_json = 'not json' WHERE details = 'other'")
        assert [r["details"] for r in db.get_event_logs(search="yi8h5")] == ["Updated guild metadata: icon"]
        assert db.count_event_logs(search="yi8h5") == 1
        assert len(db.get_event_logs(search="1442034337590677500")) == 1
        # Keys are not searched: "sync_task" is only a key.
        assert db.get_event_logs(search="sync_task") == []

    def test_delete_single(self, db):
        log_id = db.add_event_log(event_type="x", details="a")
        assert db.delete_event_log(log_id) is True
        assert db.count_event_logs() == 0

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

# Message mappings

class TestMessageMappings:

    def test_get_by_original(self, db):
        db.upsert_message_mapping(1, 100, 5000, 200, 6000, "", cloned_guild_id=2)
        rows = db.get_message_mappings_for_original(5000)
        assert len(rows) >= 1

    def test_delete(self, db):
        db.upsert_message_mapping(1, 100, 5000, 200, 6000, "", cloned_guild_id=2)
        deleted = db.delete_message_mapping(5000)
        assert deleted >= 1


# Thread mappings

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


# Mapping user tokens (self-bot senders)

class TestMappingUserTokens:

    def _mapping(self, db, mapping_id="ut-map", ogid=1, cgid=2):
        return db.upsert_guild_mapping(
            mapping_id=mapping_id,
            mapping_name="UT",
            original_guild_id=ogid,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=cgid,
            cloned_guild_name="",
        )

    def test_add_and_list(self, db):
        self._mapping(db)
        tid = db.add_mapping_token(
            "ut-map", "tokenA", label="acct1", username="user1", user_id="10"
        )
        assert tid
        rows = db.list_mapping_tokens("ut-map")
        assert len(rows) == 1
        assert rows[0]["token_value"] == "tokenA"
        assert rows[0]["label"] == "acct1"
        assert rows[0]["username"] == "user1"
        assert rows[0]["enabled"] == 1

    def test_enabled_tokens_only(self, db):
        self._mapping(db)
        db.add_mapping_token("ut-map", "tokA")
        t2 = db.add_mapping_token("ut-map", "tokB")
        db.set_mapping_token_enabled(t2, False)
        enabled = db.get_enabled_mapping_tokens("ut-map")
        assert {e["token_value"] for e in enabled} == {"tokA"}

    def test_unique_per_mapping(self, db):
        self._mapping(db)
        db.add_mapping_token("ut-map", "dup")
        with pytest.raises(sqlite3.IntegrityError):
            db.add_mapping_token("ut-map", "dup")

    def test_same_token_different_mappings(self, db):
        self._mapping(db)
        self._mapping(db, mapping_id="ut-map2", ogid=3, cgid=4)
        db.add_mapping_token("ut-map", "shared")
        # Same token string is allowed under a different mapping.
        db.add_mapping_token("ut-map2", "shared")
        assert len(db.list_mapping_tokens("ut-map")) == 1
        assert len(db.list_mapping_tokens("ut-map2")) == 1

    def test_delete(self, db):
        self._mapping(db)
        tid = db.add_mapping_token("ut-map", "tokA")
        assert db.delete_mapping_token(tid) is True
        assert db.list_mapping_tokens("ut-map") == []

    def test_delete_all(self, db):
        self._mapping(db)
        db.add_mapping_token("ut-map", "tokA")
        db.add_mapping_token("ut-map", "tokB")
        db.add_mapping_token("ut-map", "tokC")
        assert db.delete_all_mapping_tokens("ut-map") == 3
        assert db.list_mapping_tokens("ut-map") == []
        # A second call removes nothing.
        assert db.delete_all_mapping_tokens("ut-map") == 0

    def test_enabled_tokens_include_user_id(self, db):
        # The identity manager needs user_id to resolve the token's member.
        self._mapping(db)
        db.add_mapping_token("ut-map", "tokA", username="acc", user_id="4242")
        enabled = db.get_enabled_mapping_tokens("ut-map")
        assert len(enabled) == 1
        assert enabled[0]["user_id"] == "4242"
        assert enabled[0]["username"] == "acc"

    def test_list_all_user_tokens_across_mappings(self, db):
        self._mapping(db)
        self._mapping(db, mapping_id="ut-map2", ogid=3, cgid=4)
        db.add_mapping_token("ut-map", "shared", label="L", username="acc", user_id="7")
        db.add_mapping_token("ut-map2", "shared")
        db.add_mapping_token("ut-map2", "other")
        rows = db.list_all_user_tokens()
        assert len(rows) == 3
        assert set(rows[0]) == {
            "token_id", "token_value", "username", "display_name", "avatar_url",
            "user_id", "enabled", "last_used", "use_count",
        }
        assert rows[0]["display_name"] is None and rows[0]["avatar_url"] is None

    def test_update_token_identity_by_id_updates_every_row_of_the_value(self, db):
        self._mapping(db)
        self._mapping(db, mapping_id="ut-map2", ogid=3, cgid=4)
        t1 = db.add_mapping_token("ut-map", "shared", username="old", user_id="7")
        db.add_mapping_token("ut-map2", "shared")
        db.add_mapping_token("ut-map2", "other", username="keep")
        n = db.update_token_identity(
            token_id=t1, username="new", display_name="New Name",
            avatar_url="https://cdn.discordapp.com/avatars/7/abc.png?size=1024", user_id=7,
        )
        assert n == 2
        rows = db.list_all_user_tokens()
        shared = [r for r in rows if r["token_value"] == "shared"]
        assert all(r["username"] == "new" and r["display_name"] == "New Name" for r in shared)
        assert all(r["user_id"] == "7" for r in shared)
        other = [r for r in rows if r["token_value"] == "other"][0]
        assert other["username"] == "keep" and other["display_name"] is None

    def test_update_token_identity_by_value_keeps_username_when_none(self, db):
        self._mapping(db)
        db.add_mapping_token("ut-map", "tokA", username="acc", user_id="9")
        assert db.update_token_identity(token_value="tokA", display_name="D") == 1
        row = db.list_all_user_tokens()[0]
        assert (row["username"], row["user_id"], row["display_name"]) == ("acc", "9", "D")

    def test_update_token_identity_unknown(self, db):
        assert db.update_token_identity(token_id="nope", username="x") == 0
        with pytest.raises(ValueError):
            db.update_token_identity(username="x")

    def test_identity_columns_added_to_existing_table(self, db, tmp_db_path):
        # A table from before the identity columns keeps its rows and gains the columns.
        self._mapping(db)
        tid = db.add_mapping_token("ut-map", "tokA", username="acc")
        db.conn.execute("UPDATE mapping_user_tokens SET use_count = 5 WHERE token_id = ?", (tid,))
        db.conn.execute("ALTER TABLE mapping_user_tokens DROP COLUMN display_name")
        db.conn.execute("ALTER TABLE mapping_user_tokens DROP COLUMN avatar_url")
        db.conn.commit()
        db.conn.close()
        from copycord.common.db import DBManager

        db = DBManager(tmp_db_path, init_schema=True)
        rows = db.list_all_user_tokens()
        assert len(rows) == 1
        assert rows[0]["username"] == "acc" and rows[0]["use_count"] == 5
        assert rows[0]["display_name"] is None and rows[0]["avatar_url"] is None


class TestMappingTokenIdentities:

    def _mapping(self, db, mapping_id="id-map", ogid=1, cgid=2):
        return db.upsert_guild_mapping(
            mapping_id=mapping_id,
            mapping_name="ID",
            original_guild_id=ogid,
            original_guild_name="",
            original_guild_icon_url=None,
            cloned_guild_id=cgid,
            cloned_guild_name="",
        )

    def test_upsert_and_get(self, db):
        self._mapping(db)
        db.upsert_token_identity(
            mapping_id="id-map",
            author_id="111",
            token_id="tok1",
            cloned_guild_id=2,
            applied_nick="Alice",
            applied_role_ids=[30, 20, 10],
            assigned_at=1000,
        )
        row = db.get_token_identity("id-map", "111")
        assert row["token_id"] == "tok1"
        assert row["applied_nick"] == "Alice"
        # Role ids round-trip as a sorted int list.
        assert row["applied_role_ids"] == [10, 20, 30]
        assert row["assigned_at"] == 1000

    def test_upsert_replaces(self, db):
        self._mapping(db)
        db.upsert_token_identity(
            mapping_id="id-map", author_id="111", token_id="tok1",
            cloned_guild_id=2, applied_nick="A", applied_role_ids=[1],
            assigned_at=1000,
        )
        db.upsert_token_identity(
            mapping_id="id-map", author_id="111", token_id="tok2",
            cloned_guild_id=2, applied_nick="B", applied_role_ids=[],
            assigned_at=2000,
        )
        assert len(db.list_token_identities("id-map")) == 1
        row = db.get_token_identity("id-map", "111")
        assert row["token_id"] == "tok2"
        assert row["applied_role_ids"] == []
        assert row["assigned_at"] == 2000

    def test_list_and_delete(self, db):
        self._mapping(db)
        for aid in ("a", "b", "c"):
            db.upsert_token_identity(
                mapping_id="id-map", author_id=aid, token_id="t",
                cloned_guild_id=2, applied_nick=None, applied_role_ids=[],
                assigned_at=1,
            )
        assert len(db.list_token_identities("id-map")) == 3
        assert db.delete_token_identity("id-map", "b") is True
        assert len(db.list_token_identities("id-map")) == 2
        assert db.clear_token_identities("id-map") == 2
        assert db.list_token_identities("id-map") == []

    def test_cascade_on_mapping_delete(self, db):
        mid = self._mapping(db, mapping_id="id-cascade")
        db.upsert_token_identity(
            mapping_id=mid, author_id="1", token_id="t",
            cloned_guild_id=2, applied_nick="x", applied_role_ids=[1],
            assigned_at=1,
        )
        db.delete_guild_mapping(mid)
        assert db.list_token_identities(mid) == []


# Cascade: deleting a guild mapping cleans child tables

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
        db.add_mapping_token(mid, "tokA", label="acct")
        db.add_event_log(event_type="x", details="y", guild_id=1)

        db.delete_guild_mapping(mid)

        assert len(db.get_all_category_mappings()) == 0
        assert len(db.get_all_channel_mappings()) == 0
        assert len(db.get_all_role_mappings()) == 0
        assert len(db.get_all_emoji_mappings()) == 0
        assert db.list_mapping_tokens(mid) == []
        # Event logs are NOT cascade-deleted (they're audit records)
        assert db.count_event_logs() == 1


# WAL journal, backups and concurrent writers

class TestWal:

    def test_journal_mode_is_wal(self, db):
        assert db.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        # 1 = NORMAL
        assert db.conn.execute("PRAGMA synchronous").fetchone()[0] == 1

    def test_backup_of_wal_db_is_complete(self, db, tmp_path):
        from pathlib import Path
        from copycord.common.backup_scheduler import snapshot_database

        for i in range(50):
            db.upsert_emoji_mapping(i, f"e{i}", 1000 + i, f"c{i}", original_guild_id=1, cloned_guild_id=2)
        # the rows still sit in data.db-wal: the connection is open, no checkpoint ran
        assert Path(f"{db.path}-wal").stat().st_size > 0

        copy = tmp_path / "copy.db"
        snapshot_database(Path(db.path), copy)
        out = sqlite3.connect(str(copy))
        try:
            assert out.execute("SELECT COUNT(*) FROM emoji_mappings").fetchone()[0] == 50
            assert out.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            out.close()

    def test_restore_clears_stale_wal_and_shm(self, db, tmp_path):
        from pathlib import Path
        from copycord.common.backup_scheduler import Unpacked, put_in_place, snapshot_database

        db.upsert_emoji_mapping(1, "e1", 11, "c1", original_guild_id=1, cloned_guild_id=2)
        staged = tmp_path / "staged.db"
        snapshot_database(Path(db.path), staged)
        db.close()

        live = tmp_path / "live" / "data.db"
        live.parent.mkdir()
        live.write_bytes(b"old")
        Path(f"{live}-wal").write_bytes(b"stale wal")
        Path(f"{live}-shm").write_bytes(b"stale shm")

        put_in_place(Unpacked(staged, None, {}), live, tmp_path / "live" / "secret.key")

        assert not Path(f"{live}-wal").exists()
        assert not Path(f"{live}-shm").exists()
        conn = sqlite3.connect(str(live))
        try:
            assert conn.execute("SELECT COUNT(*) FROM emoji_mappings").fetchone()[0] == 1
        finally:
            conn.close()

    @pytest.mark.parametrize("shared", [True, False], ids=["one-connection", "two-connections"])
    def test_two_thread_write_hammer(self, db, shared):
        import threading
        from copycord.common.db import DBManager

        managers = [db, db] if shared else [db, DBManager(db.path)]
        errors = []

        def hammer(mgr, base):
            try:
                for i in range(500):
                    mgr.upsert_emoji_mapping(base + i, f"e{i}", base + i + 100000, f"c{i}",
                                             original_guild_id=1, cloned_guild_id=2)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=hammer, args=(m, n * 1000)) for n, m in enumerate(managers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)

        assert errors == []
        assert db.conn.execute("SELECT COUNT(*) FROM emoji_mappings").fetchone()[0] == 1000

    def test_tx_rolls_back_on_error(self, db):
        with pytest.raises(RuntimeError):
            with db._tx():
                db.upsert_emoji_mapping(1, "e1", 11, "c1", original_guild_id=1, cloned_guild_id=2)
                db.set_config("half_done", "1")
                raise RuntimeError("boom")
        assert db.get_emoji_mapping(1) is None
        assert db.get_config("half_done", "") == ""
        # the connection is usable afterwards
        db.set_config("after", "1")
        assert db.get_config("after", "") == "1"
