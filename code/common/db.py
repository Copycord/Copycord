# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from datetime import datetime
import json
import sqlite3, threading
from typing import Dict, List, Optional
import uuid
import secrets


class DBManager:
    def __init__(self, db_path: str, init_schema: bool = False):
        self.path = db_path
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        self.conn.execute("PRAGMA foreign_keys = ON;")

        self.conn.execute("PRAGMA wal_checkpoint(FULL);")
        self.conn.execute("PRAGMA journal_mode = DELETE;")

        self.conn.execute("PRAGMA synchronous = FULL;")
        self.conn.execute("PRAGMA busy_timeout = 5000;")
        self.lock = threading.RLock()
        if init_schema:
            self._init_schema()

    def _init_schema(self):
        """
        Initializes the database schema by creating necessary tables, adding columns if they
        do not exist, and setting up triggers for automatic timestamp updates.
        """
        c = self.conn.cursor()

        self._ensure_table(
            name="guild_mappings",
            create_sql_template="""
                CREATE TABLE {table} (
                    mapping_id            TEXT PRIMARY KEY, 
                    mapping_name          TEXT NOT NULL DEFAULT '',
                    original_guild_id     INTEGER NOT NULL,
                    original_guild_name   TEXT,
                    original_guild_icon_url TEXT,
                    cloned_guild_id       INTEGER NOT NULL,
                    cloned_guild_name     TEXT,
                    settings              TEXT NOT NULL DEFAULT '{}',
                    status                  TEXT NOT NULL DEFAULT 'active'
                                            CHECK (status IN ('active','paused')),
                    created_at            INTEGER   NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
                    last_updated          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(original_guild_id, cloned_guild_id)
                );
            """,
            required_columns={
                "mapping_id",
                "mapping_name",
                "original_guild_id",
                "original_guild_name",
                "original_guild_icon_url",
                "cloned_guild_id",
                "cloned_guild_name",
                "settings",
                "status",
                "created_at",
                "last_updated",
            },
            copy_map={
                "mapping_id": "mapping_id",
                "original_guild_id": "original_guild_id",
                "original_guild_name": "original_guild_name",
                "cloned_guild_id": "cloned_guild_id",
                "cloned_guild_name": "cloned_guild_name",
                "status": "'active'",
                "created_at": "created_at",
                "last_updated": "last_updated",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS ix_gm_clone_guild   ON guild_mappings(cloned_guild_id);",
                "CREATE INDEX IF NOT EXISTS ix_gm_uuid          ON guild_mappings(mapping_id);",
                "CREATE INDEX IF NOT EXISTS ix_gm_by_orig ON guild_mappings(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS idx_gm_status      ON guild_mappings (status);",
            ],
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS app_config(
        key           TEXT PRIMARY KEY,
        value         TEXT NOT NULL DEFAULT '',
        last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )

        self._ensure_table(
            name="filters",
            create_sql_template="""
                CREATE TABLE {table} (
                    kind TEXT NOT NULL CHECK(kind IN ('whitelist','exclude')),
                    scope TEXT NOT NULL CHECK(scope IN ('category','channel')),
                    obj_id INTEGER NOT NULL,

                    -- NEW: which mapping this filter applies to
                    original_guild_id INTEGER,
                    cloned_guild_id   INTEGER,

                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    PRIMARY KEY (
                        kind,
                        scope,
                        obj_id,
                        original_guild_id,
                        cloned_guild_id
                    )
                )
            """,
            required_columns={
                "kind",
                "scope",
                "obj_id",
                "original_guild_id",
                "cloned_guild_id",
                "last_updated",
            },
            copy_map={
                "kind": "kind",
                "scope": "scope",
                "obj_id": "obj_id",
                "original_guild_id": "NULL",
                "cloned_guild_id": "NULL",
                "last_updated": "last_updated",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_filters_orig ON filters(original_guild_id)",
                "CREATE INDEX IF NOT EXISTS idx_filters_clone ON filters(cloned_guild_id)",
            ],
        )

        self._ensure_table(
            name="category_mappings",
            create_sql_template="""
                CREATE TABLE {table} (
                original_category_id   INTEGER NOT NULL,
                original_category_name TEXT    NOT NULL,
                cloned_category_id     INTEGER UNIQUE,
                cloned_category_name   TEXT,
                original_guild_id      INTEGER,
                cloned_guild_id        INTEGER,
                last_updated           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (original_category_id, cloned_guild_id)
                );
            """,
            required_columns={
                "original_category_id",
                "original_category_name",
                "cloned_category_id",
                "cloned_category_name",
                "original_guild_id",
                "cloned_guild_id",
                "last_updated",
            },
            copy_map={
                "original_category_id": "original_category_id",
                "original_category_name": "original_category_name",
                "cloned_category_id": "cloned_category_id",
                "cloned_category_name": "cloned_category_name",
                "original_guild_id": "NULL",
                "cloned_guild_id": "NULL",
                "last_updated": "last_updated",
            },
            post_sql=[
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_category_mappings_cloned_id ON category_mappings(cloned_category_id);",
                "CREATE INDEX IF NOT EXISTS ix_category_orig_guild  ON category_mappings(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS ix_category_clone_guild ON category_mappings(cloned_guild_id);",
            ],
        )

        self._ensure_table(
            name="channel_mappings",
            create_sql_template="""
                CREATE TABLE {table} (
                original_channel_id           INTEGER NOT NULL,
                original_channel_name         TEXT    NOT NULL,
                cloned_channel_id             INTEGER UNIQUE,
                clone_channel_name            TEXT,
                channel_webhook_url           TEXT,
                original_parent_category_id   INTEGER,
                cloned_parent_category_id     INTEGER,
                channel_type                  INTEGER NOT NULL DEFAULT 0,
                original_guild_id             INTEGER,
                cloned_guild_id               INTEGER,
                last_updated                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(original_parent_category_id, cloned_guild_id)
                    REFERENCES category_mappings(original_category_id, cloned_guild_id)
                    ON DELETE SET NULL,
                FOREIGN KEY(cloned_parent_category_id)
                    REFERENCES category_mappings(cloned_category_id) ON DELETE SET NULL,
                PRIMARY KEY (original_channel_id, cloned_guild_id)
                );
            """,
            required_columns={
                "original_channel_id",
                "original_channel_name",
                "cloned_channel_id",
                "clone_channel_name",
                "channel_webhook_url",
                "original_parent_category_id",
                "cloned_parent_category_id",
                "channel_type",
                "original_guild_id",
                "cloned_guild_id",
                "last_updated",
            },
            copy_map={
                "original_channel_id": "original_channel_id",
                "original_channel_name": "original_channel_name",
                "cloned_channel_id": "cloned_channel_id",
                "clone_channel_name": "clone_channel_name",
                "channel_webhook_url": "channel_webhook_url",
                "original_parent_category_id": "original_parent_category_id",
                "cloned_parent_category_id": "cloned_parent_category_id",
                "channel_type": "channel_type",
                "original_guild_id": "NULL",
                "cloned_guild_id": "NULL",
                "last_updated": "last_updated",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS ix_channel_parent_orig  ON channel_mappings(original_parent_category_id);",
                "CREATE INDEX IF NOT EXISTS ix_channel_parent_clone ON channel_mappings(cloned_parent_category_id);",
                "CREATE INDEX IF NOT EXISTS ix_channel_orig_guild   ON channel_mappings(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS ix_channel_clone_guild  ON channel_mappings(cloned_guild_id);",
            ],
        )

        self._ensure_table(
            name="threads",
            create_sql_template="""
                CREATE TABLE {table}(
                    original_thread_id   INTEGER,
                    original_thread_name TEXT    NOT NULL,
                    cloned_thread_id     INTEGER,
                    forum_original_id    INTEGER,
                    forum_cloned_id      INTEGER,
                    original_guild_id    INTEGER,
                    cloned_guild_id      INTEGER,
                    last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (original_thread_id, cloned_guild_id),
                    FOREIGN KEY(forum_original_id, cloned_guild_id)
                    REFERENCES channel_mappings(original_channel_id, cloned_guild_id)
                    ON DELETE SET NULL,
                    FOREIGN KEY(forum_cloned_id)
                    REFERENCES channel_mappings(cloned_channel_id)   ON DELETE SET NULL
                );
            """,
            required_columns={
                "original_thread_id",
                "original_thread_name",
                "cloned_thread_id",
                "forum_original_id",
                "forum_cloned_id",
                "original_guild_id",
                "cloned_guild_id",
                "last_updated",
            },
            copy_map={
                "original_thread_id": "original_thread_id",
                "original_thread_name": "original_thread_name",
                "cloned_thread_id": "cloned_thread_id",
                "forum_original_id": "forum_original_id",
                "forum_cloned_id": "forum_cloned_id",
                "original_guild_id": "NULL",
                "cloned_guild_id": "NULL",
                "last_updated": "last_updated",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS ix_threads_orig_guild  ON threads(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS ix_threads_clone_guild ON threads(cloned_guild_id);",
            ],
        )

        self._ensure_table(
            name="emoji_mappings",
            create_sql_template="""
                CREATE TABLE {table} (
                original_emoji_id    INTEGER,
                original_emoji_name  TEXT    NOT NULL,
                cloned_emoji_id      INTEGER UNIQUE,
                cloned_emoji_name    TEXT    NOT NULL,
                original_guild_id    INTEGER,
                cloned_guild_id      INTEGER,
                last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (original_emoji_id, cloned_guild_id)
                );
            """,
            required_columns={
                "original_emoji_id",
                "original_emoji_name",
                "cloned_emoji_id",
                "cloned_emoji_name",
                "original_guild_id",
                "cloned_guild_id",
                "last_updated",
            },
            copy_map={
                "original_emoji_id": "original_emoji_id",
                "original_emoji_name": "original_emoji_name",
                "cloned_emoji_id": "cloned_emoji_id",
                "cloned_emoji_name": "cloned_emoji_name",
                "original_guild_id": "NULL",
                "cloned_guild_id": "NULL",
                "last_updated": "last_updated",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS ix_emoji_orig_guild  ON emoji_mappings(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS ix_emoji_clone_guild ON emoji_mappings(cloned_guild_id);",
            ],
        )

        self._ensure_table(
            name="sticker_mappings",
            create_sql_template="""
                CREATE TABLE {table} (
                original_sticker_id    INTEGER,
                original_sticker_name  TEXT    NOT NULL,
                cloned_sticker_id      INTEGER UNIQUE,
                cloned_sticker_name    TEXT    NOT NULL,
                original_guild_id      INTEGER,
                cloned_guild_id        INTEGER,
                last_updated           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (original_sticker_id, cloned_guild_id)
                );
            """,
            required_columns={
                "original_sticker_id",
                "original_sticker_name",
                "cloned_sticker_id",
                "cloned_sticker_name",
                "original_guild_id",
                "cloned_guild_id",
                "last_updated",
            },
            copy_map={
                "original_sticker_id": "original_sticker_id",
                "original_sticker_name": "original_sticker_name",
                "cloned_sticker_id": "cloned_sticker_id",
                "cloned_sticker_name": "cloned_sticker_name",
                "original_guild_id": "NULL",
                "cloned_guild_id": "NULL",
                "last_updated": "last_updated",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS ix_sticker_orig_guild  ON sticker_mappings(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS ix_sticker_clone_guild ON sticker_mappings(cloned_guild_id);",
            ],
        )

        self._ensure_table(
            name="role_mappings",
            create_sql_template="""
                CREATE TABLE {table} (
                original_role_id    INTEGER,
                original_role_name  TEXT    NOT NULL,
                cloned_role_id      INTEGER UNIQUE,
                cloned_role_name    TEXT    NOT NULL,
                original_guild_id   INTEGER,
                cloned_guild_id     INTEGER,
                last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (original_role_id, cloned_guild_id)
                );
            """,
            required_columns={
                "original_role_id",
                "original_role_name",
                "cloned_role_id",
                "cloned_role_name",
                "original_guild_id",
                "cloned_guild_id",
                "last_updated",
            },
            copy_map={
                "original_role_id": "original_role_id",
                "original_role_name": "original_role_name",
                "cloned_role_id": "cloned_role_id",
                "cloned_role_name": "cloned_role_name",
                "original_guild_id": "NULL",
                "cloned_guild_id": "NULL",
                "last_updated": "last_updated",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS ix_role_orig_guild  ON role_mappings(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS ix_role_clone_guild ON role_mappings(cloned_guild_id);",
            ],
        )

        self._ensure_table(
            name="settings",
            create_sql_template="""
                CREATE TABLE {table} (
                    id               INTEGER PRIMARY KEY CHECK (id=1),
                    version          TEXT DEFAULT '',
                    notified_version TEXT DEFAULT '',
                    last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            required_columns={
                "id",
                "version",
                "notified_version",
                "last_updated",
            },
            copy_map={
                "id": "id",
                "version": "version",
                "notified_version": "notified_version",
                "last_updated": "last_updated",
            },
            post_sql=[
                "INSERT OR IGNORE INTO settings (id, version, notified_version) VALUES (1, '', '')"
            ],
            forbidden_columns={"blocked_keywords"},
        )

        self._ensure_table(
            name="blocked_keywords",
            create_sql_template="""
                CREATE TABLE {table} (
                    keyword            TEXT NOT NULL,
                    original_guild_id  INTEGER,
                    cloned_guild_id    INTEGER,
                    added_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (keyword, original_guild_id, cloned_guild_id)
                )
            """,
            required_columns={
                "keyword",
                "original_guild_id",
                "cloned_guild_id",
                "added_at",
            },
            copy_map={
                "keyword": "keyword",
                "original_guild_id": "original_guild_id",
                "cloned_guild_id": "cloned_guild_id",
                "added_at": "COALESCE(added_at, CURRENT_TIMESTAMP)",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_blocked_kw_orig ON blocked_keywords(original_guild_id)",
                "CREATE INDEX IF NOT EXISTS idx_blocked_kw_clone ON blocked_keywords(cloned_guild_id)",
            ],
        )

        self._ensure_table(
            name="announcement_subscriptions",
            create_sql_template="""
                CREATE TABLE {table} (
                guild_id     INTEGER NOT NULL,
                keyword      TEXT    NOT NULL,
                user_id      INTEGER NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, keyword, user_id)
                );
            """,
            required_columns={"guild_id", "keyword", "user_id", "last_updated"},
            copy_map={
                "guild_id": "0",
                "keyword": "keyword",
                "user_id": "user_id",
                "last_updated": "COALESCE(last_updated, CURRENT_TIMESTAMP)",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_ann_sub_by_user ON announcement_subscriptions(user_id, guild_id);",
                "CREATE INDEX IF NOT EXISTS idx_ann_sub_by_guild_keyword ON announcement_subscriptions(guild_id, keyword);",
            ],
        )

        self._ensure_table(
            name="announcement_triggers",
            create_sql_template="""
                CREATE TABLE {table} (
                guild_id       INTEGER NOT NULL,
                keyword        TEXT    NOT NULL,
                filter_user_id INTEGER NOT NULL,
                channel_id     INTEGER NOT NULL DEFAULT 0,
                last_updated   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, keyword, filter_user_id, channel_id)
                );
            """,
            required_columns={
                "guild_id",
                "keyword",
                "filter_user_id",
                "channel_id",
                "last_updated",
            },
            copy_map={
                "guild_id": "0",
                "keyword": "keyword",
                "filter_user_id": "filter_user_id",
                "channel_id": "channel_id",
                "last_updated": "COALESCE(last_updated, CURRENT_TIMESTAMP)",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_ann_trig_by_guild_keyword ON announcement_triggers(guild_id, keyword);",
                "CREATE INDEX IF NOT EXISTS idx_ann_trig_by_guild_user ON announcement_triggers(guild_id, filter_user_id);",
            ],
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS join_dm_subscriptions (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, user_id)
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS guilds (
        guild_id     INTEGER PRIMARY KEY,
        name         TEXT    NOT NULL,
        icon_url     TEXT,
        owner_id     INTEGER,
        member_count INTEGER,
        description  TEXT,
        last_seen    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )
        self.conn.commit()

        self._ensure_table(
            name="role_blocks",
            create_sql_template="""
                CREATE TABLE {table} (
                    original_role_id  INTEGER NOT NULL,
                    cloned_guild_id   INTEGER NOT NULL,
                    added_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (original_role_id, cloned_guild_id)
                );
            """,
            required_columns={
                "original_role_id",
                "cloned_guild_id",
                "added_at",
            },
            copy_map={
                "original_role_id": "original_role_id",
                "cloned_guild_id": "NULL",
                "added_at": "COALESCE(added_at, CURRENT_TIMESTAMP)",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_roleblocks_clone ON role_blocks(cloned_guild_id);",
            ],
        )

        self._ensure_table(
            name="messages",
            create_sql_template="""
                CREATE TABLE {table} (
                original_message_id  INTEGER NOT NULL,
                original_guild_id    INTEGER,
                original_channel_id  INTEGER,
                cloned_guild_id      INTEGER,
                cloned_channel_id    INTEGER,
                cloned_message_id    INTEGER,
                webhook_url          TEXT,
                created_at           INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
                updated_at           INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER)),
                PRIMARY KEY (original_message_id, cloned_guild_id)
                );
            """,
            required_columns={
                "original_message_id",
                "original_guild_id",
                "original_channel_id",
                "cloned_guild_id",
                "cloned_channel_id",
                "cloned_message_id",
                "webhook_url",
                "created_at",
                "updated_at",
            },
            copy_map={
                "original_message_id": "original_message_id",
                "original_guild_id": "original_guild_id",
                "original_channel_id": "original_channel_id",
                "cloned_guild_id": "NULL",
                "cloned_channel_id": "cloned_channel_id",
                "cloned_message_id": "cloned_message_id",
                "webhook_url": "webhook_url",
                "created_at": "created_at",
                "updated_at": "updated_at",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);",
                "CREATE INDEX IF NOT EXISTS idx_messages_orig_chan ON messages(original_channel_id);",
                "CREATE INDEX IF NOT EXISTS idx_messages_clone_msg ON messages(cloned_message_id);",
                "CREATE INDEX IF NOT EXISTS idx_messages_orig_guild ON messages(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS idx_messages_clone_guild ON messages(cloned_guild_id);",
            ],
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS onjoin_roles (
                guild_id     INTEGER NOT NULL,
                role_id      INTEGER NOT NULL,
                added_by     INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, role_id)
            );
            """
        )

        self._ensure_table(
            name="backfill_runs",
            create_sql_template="""
                CREATE TABLE {table} (
                    run_id                TEXT PRIMARY KEY,
                    original_guild_id     INTEGER,
                    cloned_guild_id       INTEGER,
                    original_channel_id   INTEGER NOT NULL,
                    clone_channel_id      INTEGER,
                    status                TEXT NOT NULL DEFAULT 'running', 
                    range_json            TEXT,                             
                    started_at            TEXT NOT NULL,                 
                    updated_at            TEXT NOT NULL,                    
                    delivered             INTEGER NOT NULL DEFAULT 0,
                    expected_total        INTEGER,
                    last_orig_message_id  TEXT,                          
                    last_orig_timestamp   TEXT,                           
                    error                 TEXT
                );
            """,
            required_columns={
                "run_id",
                "original_channel_id",
                "clone_channel_id",
                "status",
                "range_json",
                "started_at",
                "updated_at",
                "delivered",
                "expected_total",
                "last_orig_message_id",
                "last_orig_timestamp",
                "error",
                "original_guild_id",
                "cloned_guild_id",
            },
            copy_map={
                "run_id": "run_id",
                "original_channel_id": "original_channel_id",
                "clone_channel_id": "clone_channel_id",
                "status": "status",
                "range_json": "range_json",
                "started_at": "started_at",
                "updated_at": "updated_at",
                "delivered": "delivered",
                "expected_total": "expected_total",
                "last_orig_message_id": "last_orig_message_id",
                "last_orig_timestamp": "last_orig_timestamp",
                "error": "error",
                "original_guild_id": "NULL",
                "cloned_guild_id": "NULL",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_bf_runs_by_orig_status ON backfill_runs(original_channel_id, status);",
            ],
        )

        self._ensure_table(
            name="user_filters",
            create_sql_template="""
                CREATE TABLE {table} (
                    user_id             INTEGER NOT NULL,
                    filter_type         TEXT NOT NULL CHECK(filter_type IN ('whitelist','blacklist')),
                    original_guild_id   INTEGER NOT NULL,
                    cloned_guild_id     INTEGER NOT NULL,
                    added_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, filter_type, original_guild_id, cloned_guild_id)
                )
            """,
            required_columns={
                "user_id",
                "filter_type",
                "original_guild_id",
                "cloned_guild_id",
                "added_at",
            },
            copy_map={
                "user_id": "user_id",
                "filter_type": "filter_type",
                "original_guild_id": "original_guild_id",
                "cloned_guild_id": "cloned_guild_id",
                "added_at": "COALESCE(added_at, CURRENT_TIMESTAMP)",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_user_filters_orig ON user_filters(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS idx_user_filters_clone ON user_filters(cloned_guild_id);",
                "CREATE INDEX IF NOT EXISTS idx_user_filters_type ON user_filters(filter_type);",
            ],
        )

        self._ensure_table(
            name="role_mentions",
            create_sql_template="""
                CREATE TABLE {table} (
                    role_mention_id    TEXT NOT NULL PRIMARY KEY,
                    original_guild_id  INTEGER NOT NULL,
                    cloned_guild_id    INTEGER NOT NULL,
                    cloned_channel_id  INTEGER,
                    cloned_role_id     INTEGER NOT NULL,
                    added_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (original_guild_id, cloned_guild_id, cloned_channel_id, cloned_role_id)
                );
            """,
            required_columns={
                "role_mention_id",
                "original_guild_id",
                "cloned_guild_id",
                "cloned_channel_id",
                "cloned_role_id",
                "added_at",
            },
            copy_map={
                "role_mention_id": "COALESCE(role_mention_id, lower(hex(randomblob(4))))",
                "original_guild_id": "original_guild_id",
                "cloned_guild_id": "cloned_guild_id",
                "cloned_channel_id": "cloned_channel_id",
                "cloned_role_id": "cloned_role_id",
                "added_at": "COALESCE(added_at, CURRENT_TIMESTAMP)",
            },
            post_sql=[
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_role_mentions_unique_scope ON role_mentions(original_guild_id, cloned_guild_id, cloned_channel_id, cloned_role_id);",
                "CREATE INDEX IF NOT EXISTS idx_role_mentions_orig ON role_mentions(original_guild_id);",
                "CREATE INDEX IF NOT EXISTS idx_role_mentions_clone ON role_mentions(cloned_guild_id);",
                "CREATE INDEX IF NOT EXISTS idx_role_mentions_chan ON role_mentions(cloned_channel_id);",
            ],
        )
        self._ensure_table(
            name="channel_webhook_profiles",
            create_sql_template="""
                CREATE TABLE {table} (
                    cloned_channel_id   INTEGER NOT NULL,
                    cloned_guild_id     INTEGER NOT NULL,
                    webhook_name        TEXT NOT NULL,
                    webhook_avatar_url  TEXT,
                    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (cloned_channel_id, cloned_guild_id)
                );
            """,
            required_columns={
                "cloned_channel_id",
                "cloned_guild_id",
                "webhook_name",
                "webhook_avatar_url",
                "created_at",
                "last_updated",
            },
            copy_map={
                "cloned_channel_id": "cloned_channel_id",
                "cloned_guild_id": "cloned_guild_id",
                "webhook_name": "webhook_name",
                "webhook_avatar_url": "webhook_avatar_url",
                "created_at": "COALESCE(created_at, CURRENT_TIMESTAMP)",
                "last_updated": "COALESCE(last_updated, CURRENT_TIMESTAMP)",
            },
            post_sql=[
                "CREATE INDEX IF NOT EXISTS idx_channel_webhook_profiles_clone ON channel_webhook_profiles(cloned_guild_id);",
            ],
        )

    def _table_exists(self, name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()
        return row is not None

    def _table_columns(self, name: str) -> set[str]:
        return {
            r[1] for r in self.conn.execute(f"PRAGMA table_info({name})").fetchall()
        }

    def _ensure_table(
        self,
        *,
        name: str,
        create_sql_template: str,
        required_columns: set[str],
        copy_map: dict[str, str],
        post_sql: list[str] | None = None,
        forbidden_columns: set[str] | None = None,
    ):
        """
        Create or rebuild table `name` to match the target schema.
        """
        post_sql = post_sql or []
        forbidden_columns = forbidden_columns or set()

        exists = self._table_exists(name)

        if not exists:
            self.conn.execute(create_sql_template.replace("{table}", name))
            for stmt in post_sql:
                self.conn.execute(stmt)
            return

        existing_cols = self._table_columns(name)

        missing_required = not required_columns.issubset(existing_cols)
        has_forbidden = bool(forbidden_columns.intersection(existing_cols))

        if (not missing_required) and (not has_forbidden):
            for stmt in post_sql:
                self.conn.execute(stmt)
            return

        temp = f"_{name}_new"

        prev_fk = self.conn.execute("PRAGMA foreign_keys").fetchone()[0]
        self.conn.execute("PRAGMA foreign_keys = OFF;")

        in_txn = self.conn.in_transaction
        sp_name = f"sp_rebuild_{name}"

        try:
            if in_txn:
                self.conn.execute(f"SAVEPOINT {sp_name};")
            else:
                self.conn.execute("BEGIN;")

            self.conn.execute(create_sql_template.replace("{table}", temp))

            new_cols = list(copy_map.keys())
            select_exprs = []
            for new_col in new_cols:
                expr = copy_map[new_col].strip()

                if expr.isidentifier() and expr not in existing_cols:
                    expr = (
                        "CURRENT_TIMESTAMP"
                        if expr.lower() == "last_updated"
                        else "NULL"
                    )

                select_exprs.append(expr)

            self.conn.execute(
                f"INSERT OR IGNORE INTO {temp} ({', '.join(new_cols)}) "
                f"SELECT {', '.join(select_exprs)} FROM {name}"
            )

            self.conn.execute(f"DROP TABLE {name};")
            self.conn.execute(f"ALTER TABLE {temp} RENAME TO {name};")

            for stmt in post_sql:
                self.conn.execute(stmt)

            if in_txn:
                self.conn.execute(f"RELEASE SAVEPOINT {sp_name};")
            else:
                self.conn.execute("COMMIT;")

        except Exception:

            if in_txn:
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name};")
                self.conn.execute(f"RELEASE SAVEPOINT {sp_name};")
            else:
                self.conn.execute("ROLLBACK;")
            raise
        finally:

            self.conn.execute(f"PRAGMA foreign_keys = {1 if prev_fk else 0};")

    def set_config(self, key: str, value: str) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO app_config(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def get_config(self, key: str, default: str = "") -> str:
        row = self.conn.execute(
            "SELECT value FROM app_config WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def get_all_config(self) -> dict[str, str]:
        return {
            r["key"]: r["value"]
            for r in self.conn.execute("SELECT key, value FROM app_config")
        }

    def get_version(self) -> str:
        """
        Retrieves the version information from the settings table in the database.
        """
        row = self.conn.execute("SELECT version FROM settings WHERE id = 1").fetchone()
        return row[0] if row else ""

    def set_version(self, version: str):
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO settings (id, version) VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE SET version = excluded.version
                """,
                (version,),
            )

    def get_notified_version(self) -> str:
        """
        Retrieves the notified version from the settings table in the database.
        """
        row = self.conn.execute(
            "SELECT notified_version FROM settings WHERE id = 1"
        ).fetchone()
        return row[0] if row else ""

    def set_notified_version(self, version: str):
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO settings (id, notified_version) VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE SET notified_version = excluded.notified_version
                """,
                (version,),
            )

    def get_all_category_mappings(self) -> List[sqlite3.Row]:
        """
        Retrieves all category mappings from the database.
        """
        return self.conn.execute("SELECT * FROM category_mappings").fetchall()

    def upsert_category_mapping(
        self,
        orig_id: int,
        orig_name: str,
        clone_id: Optional[int],
        clone_name: Optional[str] = None,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ):
        cgid = int(cloned_guild_id) if cloned_guild_id is not None else None

        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT cloned_category_id FROM category_mappings "
                "WHERE original_category_id=? AND cloned_guild_id=?",
                (orig_id, cgid),
            ).fetchone()
            old_clone = row["cloned_category_id"] if row else None

            will_change_to_new = (
                row is not None and clone_id is not None and old_clone != clone_id
            )
            if will_change_to_new and old_clone is not None:
                self.conn.execute(
                    "UPDATE channel_mappings "
                    "SET cloned_parent_category_id=NULL "
                    "WHERE cloned_parent_category_id=? AND cloned_guild_id=?",
                    (old_clone, cgid),
                )

            clearing_parent = (
                row is not None and old_clone is not None and clone_id is None
            )
            if clearing_parent:
                self.conn.execute(
                    "UPDATE channel_mappings "
                    "SET cloned_parent_category_id=NULL "
                    "WHERE cloned_parent_category_id=? AND cloned_guild_id=?",
                    (old_clone, cgid),
                )

            self.conn.execute(
                """
                INSERT INTO category_mappings (
                    original_category_id,
                    original_category_name,
                    cloned_category_id,
                    cloned_category_name,
                    original_guild_id,
                    cloned_guild_id
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(original_category_id, cloned_guild_id) DO UPDATE SET
                    original_category_name = excluded.original_category_name,
                    cloned_category_id     = excluded.cloned_category_id,
                    cloned_category_name   = CASE
                        WHEN excluded.cloned_category_id IS NULL THEN NULL
                        WHEN excluded.cloned_category_name IS NOT NULL THEN excluded.cloned_category_name
                        ELSE category_mappings.cloned_category_name
                    END,
                    original_guild_id      = COALESCE(excluded.original_guild_id, category_mappings.original_guild_id),
                    cloned_guild_id        = COALESCE(excluded.cloned_guild_id,   category_mappings.cloned_guild_id)
                """,
                (
                    orig_id,
                    orig_name,
                    clone_id,
                    clone_name,
                    int(original_guild_id) if original_guild_id is not None else None,
                    cgid,
                ),
            )

            if clone_id is not None:
                self.conn.execute(
                    "UPDATE channel_mappings "
                    "SET cloned_parent_category_id=? "
                    "WHERE original_parent_category_id=? AND cloned_guild_id=?",
                    (clone_id, orig_id, cgid),
                )

            self.conn.commit()

    def delete_category_mapping(self, orig_id: int):
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT cloned_category_id FROM category_mappings WHERE original_category_id=?",
                (orig_id,),
            ).fetchone()
            cloned_id = row["cloned_category_id"] if row else None

            self.conn.execute(
                "UPDATE channel_mappings SET original_parent_category_id=NULL WHERE original_parent_category_id=?",
                (orig_id,),
            )
            if cloned_id is not None:
                self.conn.execute(
                    "UPDATE channel_mappings SET cloned_parent_category_id=NULL WHERE cloned_parent_category_id=?",
                    (cloned_id,),
                )

            self.conn.execute(
                "DELETE FROM category_mappings WHERE original_category_id=?",
                (orig_id,),
            )

    def count_categories(self) -> int:
        """
        Counts the total number of categories in the 'category_mappings' table.
        """
        return self.conn.execute("SELECT COUNT(*) FROM category_mappings").fetchone()[0]

    def get_all_channel_mappings(self) -> List[sqlite3.Row]:
        """
        Retrieves all channel mappings from the database.
        """
        return self.conn.execute("SELECT * FROM channel_mappings").fetchall()

    def get_channel_mapping_by_clone_id(
        self, cloned_channel_id: int
    ) -> Optional[sqlite3.Row]:
        """
        Look up a single channel mapping by the cloned (destination) channel id.

        Returns:
            sqlite3.Row with columns from `channel_mappings` (e.g., original_channel_id,
            cloned_channel_id, etc.), or None if not found.
        """
        return self.conn.execute(
            "SELECT * FROM channel_mappings WHERE cloned_channel_id = ? LIMIT 1",
            (cloned_channel_id,),
        ).fetchone()

    def get_original_channel_id(self, cloned_channel_id: int) -> Optional[int]:
        row = self.get_channel_mapping_by_clone_id(cloned_channel_id)
        return int(row["original_channel_id"]) if row else None

    def get_channel_mapping_by_original_id(
        self, original_channel_id: int
    ) -> Optional[sqlite3.Row]:
        """
        Look up a single channel mapping by the original (source) channel id.
        """
        return self.conn.execute(
            "SELECT * FROM channel_mappings WHERE original_channel_id = ? LIMIT 1",
            (original_channel_id,),
        ).fetchone()

    def resolve_original_from_any_id(
        self, any_channel_id: int
    ) -> tuple[Optional[int], Optional[int], str]:
        """
        Accept either a cloned id or an original id.

        Returns:
            (original_id, cloned_id_or_none, source)
            where source is 'from_clone' | 'from_original' | 'assumed_original'
        """
        row = self.get_channel_mapping_by_clone_id(any_channel_id)
        if row:
            return (
                int(row["original_channel_id"]),
                int(row["cloned_channel_id"]),
                "from_clone",
            )

        row = self.get_channel_mapping_by_original_id(any_channel_id)
        if row:
            cloned = row["cloned_channel_id"]
            return (
                int(row["original_channel_id"]),
                (int(cloned) if cloned is not None else None),
                "from_original",
            )

        return int(any_channel_id), None, "assumed_original"

    def get_all_threads(self) -> List[sqlite3.Row]:
        """
        Retrieves all rows from the 'threads' table in the database.
        """
        return self.conn.execute("SELECT * FROM threads").fetchall()

    def upsert_forum_thread_mapping(
        self,
        orig_thread_id: int,
        orig_thread_name: str,
        clone_thread_id: Optional[int],
        forum_orig_id: int,
        forum_clone_id: Optional[int],
        *,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ):
        self.conn.execute(
            """
            INSERT INTO threads (
                original_thread_id,
                original_thread_name,
                cloned_thread_id,
                forum_original_id,
                forum_cloned_id,
                original_guild_id,
                cloned_guild_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_thread_id, cloned_guild_id) DO UPDATE SET
                original_thread_name = excluded.original_thread_name,
                cloned_thread_id     = excluded.cloned_thread_id,
                forum_original_id    = excluded.forum_original_id,
                forum_cloned_id      = excluded.forum_cloned_id,
                original_guild_id    = COALESCE(excluded.original_guild_id, threads.original_guild_id),
                cloned_guild_id      = COALESCE(excluded.cloned_guild_id,   threads.cloned_guild_id)
            """,
            (
                orig_thread_id,
                orig_thread_name,
                clone_thread_id,
                forum_orig_id,
                forum_clone_id,
                int(original_guild_id) if original_guild_id is not None else None,
                int(cloned_guild_id) if cloned_guild_id is not None else None,
            ),
        )
        self.conn.commit()

    def delete_forum_thread_mapping(self, orig_thread_id: int):
        """
        Deletes a forum thread mapping from the database.
        """
        self.conn.execute(
            "DELETE FROM threads WHERE original_thread_id = ?",
            (orig_thread_id,),
        )
        self.conn.commit()

    def upsert_channel_mapping(
        self,
        original_channel_id: int,
        original_channel_name: str,
        cloned_channel_id: int | None,
        channel_webhook_url: str | None,
        original_parent_category_id: int | None,
        cloned_parent_category_id: int | None,
        channel_type: int,
        *,
        clone_name: str | None = None,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ):
        self.conn.execute(
            """
            INSERT INTO channel_mappings (
                original_channel_id,
                original_channel_name,
                cloned_channel_id,
                channel_webhook_url,
                original_parent_category_id,
                cloned_parent_category_id,
                channel_type,
                clone_channel_name,
                original_guild_id,
                cloned_guild_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_channel_id, cloned_guild_id) DO UPDATE SET
                original_channel_name       = excluded.original_channel_name,
                -- Only adopt the incoming cloned_channel_id if it's NULL (no change) OR
                -- if that id is not used by any other row:
                cloned_channel_id = CASE
                    WHEN excluded.cloned_channel_id IS NULL THEN channel_mappings.cloned_channel_id
                    WHEN NOT EXISTS (
                        SELECT 1 FROM channel_mappings AS cm
                        WHERE cm.cloned_channel_id = excluded.cloned_channel_id
                        AND (cm.original_channel_id != channel_mappings.original_channel_id
                            OR cm.cloned_guild_id   != channel_mappings.cloned_guild_id)
                    )
                    THEN excluded.cloned_channel_id
                    ELSE channel_mappings.cloned_channel_id
                END,
                channel_webhook_url         = excluded.channel_webhook_url,
                original_parent_category_id = excluded.original_parent_category_id,
                cloned_parent_category_id   = excluded.cloned_parent_category_id,
                channel_type                = excluded.channel_type,
                clone_channel_name          = COALESCE(excluded.clone_channel_name, channel_mappings.clone_channel_name),
                original_guild_id           = COALESCE(excluded.original_guild_id, channel_mappings.original_guild_id),
                cloned_guild_id             = COALESCE(excluded.cloned_guild_id,   channel_mappings.cloned_guild_id),
                last_updated                = CURRENT_TIMESTAMP
            """,
            (
                int(original_channel_id),
                original_channel_name,
                int(cloned_channel_id) if cloned_channel_id is not None else None,
                channel_webhook_url,
                (
                    int(original_parent_category_id)
                    if original_parent_category_id is not None
                    else None
                ),
                (
                    int(cloned_parent_category_id)
                    if cloned_parent_category_id is not None
                    else None
                ),
                int(channel_type),
                (clone_name.strip() if isinstance(clone_name, str) else None),
                int(original_guild_id) if original_guild_id is not None else None,
                int(cloned_guild_id) if cloned_guild_id is not None else None,
            ),
        )
        self.conn.commit()

    def delete_channel_mapping(self, orig_id: int):
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT cloned_channel_id FROM channel_mappings WHERE original_channel_id=?",
                (orig_id,),
            ).fetchone()
            cloned_id = row["cloned_channel_id"] if row else None

            self.conn.execute(
                "UPDATE threads SET forum_original_id=NULL WHERE forum_original_id=?",
                (orig_id,),
            )
            if cloned_id is not None:
                self.conn.execute(
                    "UPDATE threads SET forum_cloned_id=NULL WHERE forum_cloned_id=?",
                    (cloned_id,),
                )

            self.conn.execute(
                "DELETE FROM channel_mappings WHERE original_channel_id=?",
                (orig_id,),
            )

    def count_channels(self) -> int:
        """
        Counts the total number of channels in the 'channel_mappings' table.
        """
        return self.conn.execute("SELECT COUNT(*) FROM channel_mappings").fetchone()[0]

    def add_blocked_keyword(
        self,
        keyword: str,
        *,
        original_guild_id: int | None,
        cloned_guild_id: int | None,
    ) -> bool:
        """
        Try to add this keyword for (original_guild_id, cloned_guild_id).
        Returns True if inserted, False if it already existed.
        """
        kw_norm = keyword.strip().lower()
        if not kw_norm:
            return False

        with self.lock, self.conn:

            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO blocked_keywords(
                    keyword, original_guild_id, cloned_guild_id
                )
                VALUES (?, ?, ?)
                """,
                (
                    kw_norm,
                    int(original_guild_id) if original_guild_id is not None else None,
                    int(cloned_guild_id) if cloned_guild_id is not None else None,
                ),
            )
            return cur.rowcount > 0

    def remove_blocked_keyword(
        self,
        keyword: str,
        *,
        original_guild_id: int | None,
        cloned_guild_id: int | None,
    ) -> bool:
        """
        Remove this keyword for (original_guild_id, cloned_guild_id).
        Returns True if something was deleted.
        """
        kw_norm = keyword.strip().lower()
        if not kw_norm:
            return False

        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                DELETE FROM blocked_keywords
                WHERE LOWER(keyword) = LOWER(?)
                AND (
                        (original_guild_id IS NULL AND ? IS NULL)
                    OR original_guild_id = ?
                    )
                AND (
                        (cloned_guild_id IS NULL AND ? IS NULL)
                    OR cloned_guild_id = ?
                    )
                """,
                (
                    kw_norm,
                    int(original_guild_id) if original_guild_id is not None else None,
                    int(original_guild_id) if original_guild_id is not None else None,
                    int(cloned_guild_id) if cloned_guild_id is not None else None,
                    int(cloned_guild_id) if cloned_guild_id is not None else None,
                ),
            )
            return cur.rowcount > 0

    def get_all_emoji_mappings(self) -> list[sqlite3.Row]:
        """
        Retrieves all emoji mappings from the database.
        """
        return self.conn.execute("SELECT * FROM emoji_mappings").fetchall()

    def upsert_emoji_mapping(
        self,
        orig_id: int,
        orig_name: str,
        clone_id: int | None,
        clone_name: str | None,
        *,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO emoji_mappings (
                original_emoji_id,
                original_emoji_name,
                cloned_emoji_id,
                cloned_emoji_name,
                original_guild_id,
                cloned_guild_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_emoji_id, cloned_guild_id) DO UPDATE SET
                original_emoji_name = excluded.original_emoji_name,
                cloned_emoji_id     = excluded.cloned_emoji_id,
                cloned_emoji_name   = excluded.cloned_emoji_name,
                original_guild_id   = COALESCE(excluded.original_guild_id, emoji_mappings.original_guild_id),
                cloned_guild_id     = COALESCE(excluded.cloned_guild_id,   emoji_mappings.cloned_guild_id)
            """,
            (
                int(orig_id),
                orig_name,
                int(clone_id) if clone_id is not None else None,
                clone_name,
                int(original_guild_id) if original_guild_id is not None else None,
                int(cloned_guild_id) if cloned_guild_id is not None else None,
            ),
        )
        self.conn.commit()

    def get_emoji_mapping(self, original_id: int) -> sqlite3.Row | None:
        """
        Returns the row for this original emoji ID, or None if we never
        cloned that emoji.
        """
        return self.conn.execute(
            "SELECT * FROM emoji_mappings WHERE original_emoji_id = ?", (original_id,)
        ).fetchone()

    def add_announcement_user(self, guild_id: int, keyword: str, user_id: int) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO announcement_subscriptions(guild_id, keyword, user_id) VALUES (?, ?, ?)",
            (guild_id, keyword, user_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def remove_announcement_user(
        self, guild_id: int, keyword: str, user_id: int
    ) -> bool:
        cur = self.conn.execute(
            "DELETE FROM announcement_subscriptions WHERE guild_id = ? AND keyword = ? AND user_id = ?",
            (guild_id, keyword, user_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_announcement_users(self, guild_id: int, keyword: str) -> list[int]:
        """
        Users subscribed to this keyword in this guild, plus:
        - '*' in this guild (all keywords), and
        - if you support cross-guild global subs: guild_id=0 records.
        """
        rows = self.conn.execute(
            "SELECT user_id FROM announcement_subscriptions "
            "WHERE (guild_id = ? AND (keyword = ? OR keyword = '*')) "
            "   OR (guild_id = 0 AND (keyword = ? OR keyword = '*'))",
            (guild_id, keyword, keyword),
        ).fetchall()
        return [r["user_id"] for r in rows]

    def add_announcement_trigger(
        self, guild_id: int, keyword: str, filter_user_id: int = 0, channel_id: int = 0
    ) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO announcement_triggers(guild_id, keyword, filter_user_id, channel_id) "
            "VALUES (?, ?, ?, ?)",
            (guild_id, keyword, filter_user_id, channel_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_announcement_keywords(self, guild_id: int) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT keyword FROM announcement_subscriptions WHERE guild_id IN (?, 0)",
            (guild_id,),
        ).fetchall()
        return [r["keyword"] for r in rows]

    def remove_announcement_trigger(
        self, guild_id: int, keyword: str, filter_user_id: int = 0, channel_id: int = 0
    ) -> bool:
        cur = self.conn.execute(
            "DELETE FROM announcement_triggers "
            "WHERE guild_id = ? AND keyword = ? AND filter_user_id = ? AND channel_id = ?",
            (guild_id, keyword, filter_user_id, channel_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_announcement_triggers(
        self, guild_id: int
    ) -> dict[str, list[tuple[int, int]]]:
        rows = self.conn.execute(
            "SELECT keyword, filter_user_id, channel_id FROM announcement_triggers WHERE guild_id = ?",
            (guild_id,),
        ).fetchall()
        d: dict[str, list[tuple[int, int]]] = {}
        for r in rows:
            d.setdefault(r["keyword"], []).append(
                (r["filter_user_id"], r["channel_id"])
            )
        return d

    def get_all_announcement_triggers_flat(self) -> list[sqlite3.Row]:
        """
        Returns every row in announcement_triggers with no grouping.
        Columns: guild_id, keyword, filter_user_id, channel_id, last_updated
        """
        return self.conn.execute(
            """
            SELECT guild_id, keyword, filter_user_id, channel_id, last_updated
            FROM announcement_triggers
            ORDER BY guild_id ASC, LOWER(keyword) ASC, filter_user_id ASC, channel_id ASC
            """
        ).fetchall()

    def get_all_announcement_subscriptions_flat(self) -> list[sqlite3.Row]:
        """
        Returns every row in announcement_subscriptions with no grouping.
        Columns: guild_id, keyword, user_id, last_updated
        """
        return self.conn.execute(
            """
            SELECT guild_id, keyword, user_id, last_updated
            FROM announcement_subscriptions
            ORDER BY guild_id ASC, LOWER(keyword) ASC, user_id ASC
            """
        ).fetchall()

    def get_effective_announcement_triggers(
        self, guild_id: int
    ) -> dict[str, list[tuple[int, int]]]:
        """
        Triggers that apply to this guild: rows where guild_id IN (guild_id, 0).
        Returns {keyword: [(filter_user_id, channel_id), ...]} with duplicates removed.
        """
        rows = self.conn.execute(
            """
            SELECT keyword, filter_user_id, channel_id
            FROM announcement_triggers
            WHERE guild_id IN (?, 0)
            """,
            (guild_id,),
        ).fetchall()

        out: dict[str, set[tuple[int, int]]] = {}
        for r in rows:
            out.setdefault(r["keyword"], set()).add(
                (int(r["filter_user_id"]), int(r["channel_id"]))
            )
        return {k: list(v) for k, v in out.items()}

    def add_onjoin_subscription(self, guild_id: int, user_id: int) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO join_dm_subscriptions(guild_id, user_id) VALUES (?, ?)",
            (guild_id, user_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def remove_onjoin_subscription(self, guild_id: int, user_id: int) -> bool:
        cur = self.conn.execute(
            "DELETE FROM join_dm_subscriptions WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def has_onjoin_subscription(self, guild_id: int, user_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM join_dm_subscriptions WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        ).fetchone()
        return bool(row)

    def get_onjoin_users(self, guild_id: int) -> list[int]:
        rows = self.conn.execute(
            "SELECT user_id FROM join_dm_subscriptions WHERE guild_id = ?",
            (guild_id,),
        ).fetchall()
        return [r["user_id"] for r in rows]

    def get_onjoin_guilds_for_user(self, user_id: int) -> list[int]:
        rows = self.conn.execute(
            "SELECT guild_id FROM join_dm_subscriptions WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        return [r["guild_id"] for r in rows]

    def get_all_sticker_mappings(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM sticker_mappings").fetchall()

    def get_sticker_mapping(self, original_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM sticker_mappings WHERE original_sticker_id = ?",
            (original_id,),
        ).fetchone()

    def upsert_sticker_mapping(
        self,
        orig_id: int,
        orig_name: str,
        clone_id: int | None,
        clone_name: str | None,
        *,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO sticker_mappings (
                original_sticker_id,
                original_sticker_name,
                cloned_sticker_id,
                cloned_sticker_name,
                original_guild_id,
                cloned_guild_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_sticker_id, cloned_guild_id) DO UPDATE SET
                original_sticker_name = excluded.original_sticker_name,
                cloned_sticker_id     = excluded.cloned_sticker_id,
                cloned_sticker_name   = excluded.cloned_sticker_name,
                original_guild_id     = COALESCE(excluded.original_guild_id, sticker_mappings.original_guild_id),
                cloned_guild_id       = COALESCE(excluded.cloned_guild_id,   sticker_mappings.cloned_guild_id)
            """,
            (
                int(orig_id),
                orig_name,
                int(clone_id) if clone_id is not None else None,
                clone_name,
                int(original_guild_id) if original_guild_id is not None else None,
                int(cloned_guild_id) if cloned_guild_id is not None else None,
            ),
        )
        self.conn.commit()

    def delete_sticker_mapping(self, orig_id: int):
        self.conn.execute(
            "DELETE FROM sticker_mappings WHERE original_sticker_id = ?", (orig_id,)
        )
        self.conn.commit()

    def get_all_role_mappings(self) -> List[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM role_mappings").fetchall()

    def upsert_role_mapping(
        self,
        orig_id: int,
        orig_name: str,
        clone_id: int | None,
        clone_name: str | None,
        *,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO role_mappings (
                original_role_id,
                original_role_name,
                cloned_role_id,
                cloned_role_name,
                original_guild_id,
                cloned_guild_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_role_id, cloned_guild_id) DO UPDATE SET
                original_role_name = excluded.original_role_name,
                cloned_role_id     = excluded.cloned_role_id,
                cloned_role_name   = excluded.cloned_role_name,
                original_guild_id  = COALESCE(excluded.original_guild_id, role_mappings.original_guild_id),
                cloned_guild_id    = COALESCE(excluded.cloned_guild_id,   role_mappings.cloned_guild_id)
            """,
            (
                int(orig_id),
                orig_name,
                int(clone_id) if clone_id is not None else None,
                clone_name,
                int(original_guild_id) if original_guild_id is not None else None,
                int(cloned_guild_id) if cloned_guild_id is not None else None,
            ),
        )
        self.conn.commit()

    def delete_role_mapping(self, orig_id: int):
        self.conn.execute(
            "DELETE FROM role_mappings WHERE original_role_id = ?", (orig_id,)
        )
        self.conn.commit()

    def get_role_mapping(self, orig_id: int):
        return self.conn.execute(
            "SELECT * FROM role_mappings WHERE original_role_id = ?", (orig_id,)
        ).fetchone()

    def get_filters(
        self,
        *,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ) -> dict:
        out = {
            "whitelist": {"category": set(), "channel": set()},
            "exclude": {"category": set(), "channel": set()},
        }

        rows = self.conn.execute(
            """
            SELECT kind, scope, obj_id, original_guild_id, cloned_guild_id
            FROM filters
            """
        ).fetchall()

        want_origin = int(original_guild_id) if original_guild_id is not None else None
        want_clone = int(cloned_guild_id) if cloned_guild_id is not None else None

        for row in rows:
            row_orig = row["original_guild_id"]
            row_clone = row["cloned_guild_id"]

            if want_origin is None and want_clone is None:
                if row_orig is not None or row_clone is not None:
                    continue
                matched = True

            elif want_origin is not None and want_clone is None:
                matched = (row_orig is None and row_clone is None) or (
                    row_orig is not None
                    and int(row_orig) == want_origin
                    and row_clone is None
                )

            else:
                matched = (
                    (row_orig is None and row_clone is None)
                    or (
                        row_orig is not None
                        and int(row_orig) == want_origin
                        and row_clone is None
                    )
                    or (
                        row_orig is not None
                        and row_clone is not None
                        and int(row_orig) == want_origin
                        and int(row_clone) == want_clone
                    )
                )

            if not matched:
                continue

            out[row["kind"]][row["scope"]].add(int(row["obj_id"]))

        return out

    def replace_filters(
        self,
        whitelist_categories: list[int],
        whitelist_channels: list[int],
        exclude_categories: list[int],
        exclude_channels: list[int],
    ) -> None:
        """
        Overwrite the *global* filters (NULL/NULL scope).
        Guild-scoped filters remain untouched.
        """
        cur = self.conn.cursor()

        cur.execute(
            """
            DELETE FROM filters
            WHERE original_guild_id IS NULL
              AND cloned_guild_id   IS NULL
            """
        )

        def ins(kind: str, scope: str, ids: list[int]):
            cur.executemany(
                """
                INSERT OR IGNORE INTO filters(
                    kind,
                    scope,
                    obj_id,
                    original_guild_id,
                    cloned_guild_id
                )
                VALUES(?,?,?,?,?)
                """,
                [(kind, scope, int(i), None, None) for i in ids if str(i).strip()],
            )

        ins("whitelist", "category", whitelist_categories)
        ins("whitelist", "channel", whitelist_channels)
        ins("exclude", "category", exclude_categories)
        ins("exclude", "channel", exclude_channels)

        self.conn.commit()

    def add_filter(
        self,
        kind: str,
        scope: str,
        obj_id: int,
        *,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ) -> None:
        """
        Insert a single filter row (no-op if it already exists).

        kind: 'whitelist' | 'exclude'
        scope: 'category' | 'channel'

        If you pass original_guild_id / cloned_guild_id, this becomes a
        per-mapping rule. If you omit them, it's global.
        """
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO filters(
                    kind,
                    scope,
                    obj_id,
                    original_guild_id,
                    cloned_guild_id
                )
                VALUES(?,?,?,?,?)
                """,
                (
                    str(kind),
                    str(scope),
                    int(obj_id),
                    int(original_guild_id) if original_guild_id is not None else None,
                    int(cloned_guild_id) if cloned_guild_id is not None else None,
                ),
            )

    def upsert_guild(
        self,
        guild_id: int,
        name: str,
        icon_url: Optional[str],
        owner_id: Optional[int],
        member_count: Optional[int],
        description: Optional[str],
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO guilds (guild_id, name, icon_url, owner_id, member_count,
                                    description, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(guild_id) DO UPDATE SET
                    name         = excluded.name,
                    icon_url     = excluded.icon_url,
                    owner_id     = excluded.owner_id,
                    member_count = excluded.member_count,
                    description  = excluded.description,
                    last_seen    = CURRENT_TIMESTAMP
                WHERE
                    IFNULL(name, '')          != IFNULL(excluded.name, '') OR
                    IFNULL(icon_url, '')      != IFNULL(excluded.icon_url, '') OR
                    IFNULL(owner_id, 0)       != IFNULL(excluded.owner_id, 0) OR
                    IFNULL(member_count, 0)   != IFNULL(excluded.member_count, 0) OR
                    IFNULL(description, '')   != IFNULL(excluded.description, '')
                """,
                (
                    int(guild_id),
                    name,
                    icon_url,
                    int(owner_id) if owner_id is not None else None,
                    int(member_count) if member_count is not None else None,
                    description,
                ),
            )

    def delete_guild(self, guild_id: int) -> None:
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM guilds WHERE guild_id = ?", (int(guild_id),))

    def get_all_guild_ids(self) -> list[int]:
        rows = self.conn.execute("SELECT guild_id FROM guilds").fetchall()
        return [int(r[0]) for r in rows]

    def get_guild(self, guild_id: int):
        return self.conn.execute(
            "SELECT * FROM guilds WHERE guild_id = ?", (int(guild_id),)
        ).fetchone()

    def get_all_guilds(self) -> list[dict]:
        """
        Returns all guilds as a list of dicts with keys:
        guild_id, name, icon_url, owner_id, member_count, description, last_seen, last_updated
        """
        with self.lock:
            cur = self.conn.execute(
                """
                SELECT guild_id, name, icon_url, owner_id, member_count, description, last_seen, last_updated
                FROM guilds
                ORDER BY LOWER(name) ASC
            """
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_original_channel_name(self, original_channel_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT original_channel_name FROM channel_mappings WHERE original_channel_id = ?",
            (int(original_channel_id),),
        ).fetchone()
        return row[0] if row else None

    def get_clone_channel_name(
        self, original_channel_id: int, cloned_guild_id: int
    ) -> str | None:
        row = self.conn.execute(
            """
            SELECT clone_channel_name
            FROM channel_mappings
            WHERE original_channel_id = ? AND cloned_guild_id = ?
            """,
            (int(original_channel_id), int(cloned_guild_id)),
        ).fetchone()
        return row[0] if row else None

    def set_channel_clone_name(
        self, original_channel_id: int, cloned_guild_id: int, clone_name: str | None
    ) -> None:
        with self.conn as con:
            con.execute(
                """
                UPDATE channel_mappings
                SET clone_channel_name = :name
                WHERE original_channel_id = :ocid
                AND cloned_guild_id     = :cgid
                """,
                {
                    "name": clone_name,
                    "ocid": int(original_channel_id),
                    "cgid": int(cloned_guild_id),
                },
            )

    def get_original_category_name(self, original_category_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT original_category_name FROM category_mappings WHERE original_category_id = ?",
            (int(original_category_id),),
        ).fetchone()
        return row[0] if row else None

    def get_clone_category_name(
        self, original_category_id: int, cloned_guild_id: int
    ) -> str | None:
        row = self.conn.execute(
            """
            SELECT cloned_category_name
            FROM category_mappings
            WHERE original_category_id = ? AND cloned_guild_id = ?
            """,
            (int(original_category_id), int(cloned_guild_id)),
        ).fetchone()
        return row[0] if row else None

    def set_category_clone_name(
        self, original_category_id: int, cloned_guild_id: int, clone_name: str | None
    ) -> None:
        with self.conn as con:
            con.execute(
                """
                UPDATE category_mappings
                SET cloned_category_name = :name
                WHERE original_category_id = :ocid
                AND cloned_guild_id       = :cgid
                """,
                {
                    "name": clone_name,
                    "ocid": int(original_category_id),
                    "cgid": int(cloned_guild_id),
                },
            )

    def resolve_original_category_id_by_name(self, name: str) -> int | None:
        """
        Resolve an original_category_id using a human name.
        Prefer current upstream name; fall back to pinned clone name.
        Case-insensitive exact match.
        """
        n = name.strip()
        if not n:
            return None
        row = self.conn.execute(
            "SELECT original_category_id FROM category_mappings WHERE LOWER(original_category_name)=LOWER(?) LIMIT 1",
            (n,),
        ).fetchone()
        if row:
            return int(row[0])
        row = self.conn.execute(
            "SELECT original_category_id FROM category_mappings WHERE cloned_category_name IS NOT NULL AND LOWER(cloned_category_name)=LOWER(?) LIMIT 1",
            (n,),
        ).fetchone()
        return int(row[0]) if row else None

    def add_role_block(self, original_role_id: int, cloned_guild_id: int) -> bool:
        """Block this original role id for a specific clone guild. Returns True if newly added."""
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO role_blocks(original_role_id, cloned_guild_id)
                VALUES (?, ?)
                """,
                (int(original_role_id), int(cloned_guild_id)),
            )
            return cur.rowcount > 0

    def remove_role_block(
        self, original_role_id: int, cloned_guild_id: int | None = None
    ) -> bool:
        """
        Remove a block.

        If cloned_guild_id is given, remove only for that clone guild.
        If None, remove the block for all clones of that original role.
        Returns True if any rows were removed.
        """
        with self.lock, self.conn:
            if cloned_guild_id is None:
                cur = self.conn.execute(
                    "DELETE FROM role_blocks WHERE original_role_id = ?",
                    (int(original_role_id),),
                )
            else:
                cur = self.conn.execute(
                    "DELETE FROM role_blocks WHERE original_role_id = ? AND cloned_guild_id = ?",
                    (int(original_role_id), int(cloned_guild_id)),
                )
            return cur.rowcount > 0

    def is_role_blocked(
        self, original_role_id: int, cloned_guild_id: int | None = None
    ) -> bool:
        """
        Check whether a role is blocked.

        If cloned_guild_id is provided, the check is scoped to that clone guild.
        If not, it checks for any block for this original role.
        """
        if cloned_guild_id is None:
            row = self.conn.execute(
                "SELECT 1 FROM role_blocks WHERE original_role_id = ?",
                (int(original_role_id),),
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT 1
                FROM role_blocks
                WHERE original_role_id = ? AND cloned_guild_id = ?
                """,
                (int(original_role_id), int(cloned_guild_id)),
            ).fetchone()
        return bool(row)

    def get_blocked_role_ids(self, cloned_guild_id: int | None = None) -> list[int]:
        """
        Return blocked original_role_id values.

        If cloned_guild_id is provided, only blocks for that clone are returned.
        """
        if cloned_guild_id is None:
            rows = self.conn.execute(
                "SELECT original_role_id FROM role_blocks"
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT original_role_id FROM role_blocks WHERE cloned_guild_id = ?",
                (int(cloned_guild_id),),
            ).fetchall()
        return [int(r[0]) for r in rows]

    def clear_role_blocks(self, cloned_guild_id: int | None = None) -> int:
        """
        Delete entries from the role_blocks table.

        If cloned_guild_id is provided, only entries for that clone guild are removed.
        Returns number of rows removed.
        """
        with self.lock, self.conn:
            if cloned_guild_id is None:
                cnt_row = self.conn.execute(
                    "SELECT COUNT(*) FROM role_blocks"
                ).fetchone()
                count = int(cnt_row[0] if cnt_row else 0)
                self.conn.execute("DELETE FROM role_blocks")
            else:
                cnt_row = self.conn.execute(
                    "SELECT COUNT(*) FROM role_blocks WHERE cloned_guild_id = ?",
                    (int(cloned_guild_id),),
                ).fetchone()
                count = int(cnt_row[0] if cnt_row else 0)
                self.conn.execute(
                    "DELETE FROM role_blocks WHERE cloned_guild_id = ?",
                    (int(cloned_guild_id),),
                )
            return count

    def upsert_message_mapping(
        self,
        original_guild_id: int,
        original_channel_id: int,
        original_message_id: int,
        cloned_channel_id: int | None,
        cloned_message_id: int | None,
        webhook_url: str | None = None,
        *,
        cloned_guild_id: int | None = None,
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO messages (
                    original_guild_id,
                    original_channel_id,
                    original_message_id,
                    cloned_guild_id,
                    cloned_channel_id,
                    cloned_message_id,
                    webhook_url,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s','now'), strftime('%s','now'))
                ON CONFLICT(original_message_id, cloned_guild_id) DO UPDATE SET
                    -- never overwrite with NULL; keep existing when excluded is NULL
                    original_guild_id   = COALESCE(excluded.original_guild_id, messages.original_guild_id),
                    original_channel_id = COALESCE(excluded.original_channel_id, messages.original_channel_id),
                    cloned_guild_id     = COALESCE(excluded.cloned_guild_id,     messages.cloned_guild_id),
                    cloned_channel_id   = COALESCE(excluded.cloned_channel_id,   messages.cloned_channel_id),
                    cloned_message_id   = COALESCE(excluded.cloned_message_id,   messages.cloned_message_id),
                    webhook_url         = COALESCE(excluded.webhook_url,         messages.webhook_url),
                    -- preserve created_at from first insert
                    created_at          = messages.created_at,
                    updated_at          = strftime('%s','now')
                """,
                (
                    int(original_guild_id),
                    int(original_channel_id),
                    int(original_message_id),
                    int(cloned_guild_id) if cloned_guild_id is not None else None,
                    int(cloned_channel_id) if cloned_channel_id is not None else None,
                    int(cloned_message_id) if cloned_message_id is not None else None,
                    str(webhook_url) if webhook_url else None,
                ),
            )

    def get_mapping_by_cloned(self, cloned_message_id: int):
        return self.conn.execute(
            "SELECT * FROM messages WHERE cloned_message_id = ?",
            (int(cloned_message_id),),
        ).fetchone()

    def get_message_mappings_for_original(self, original_message_id: int):
        """
        Return ALL message-mapping rows for this original message id (one per clone).
        """
        return self.conn.execute(
            "SELECT * FROM messages WHERE original_message_id = ? ORDER BY cloned_guild_id",
            (int(original_message_id),),
        ).fetchall()

    def get_message_mapping_pair(self, original_message_id: int, cloned_guild_id: int):
        """
        Return the single mapping row for (original_message_id, cloned_guild_id).
        """
        return self.conn.execute(
            "SELECT * FROM messages WHERE original_message_id = ? AND cloned_guild_id = ? LIMIT 1",
            (int(original_message_id), int(cloned_guild_id)),
        ).fetchone()

    def delete_old_messages(self, older_than_seconds: int = 7 * 24 * 3600) -> int:
        """
        Delete rows from messages where created_at is older than now - older_than_seconds.
        Returns the number of rows deleted.
        """
        with self.lock, self.conn:
            before = self.conn.total_changes
            self.conn.execute(
                """
                DELETE FROM messages
                WHERE created_at < (CAST(strftime('%s','now') AS INTEGER) - ?)
                """,
                (int(older_than_seconds),),
            )
            return self.conn.total_changes - before

    def delete_message_mapping(self, original_message_id: int) -> int:
        """
        Delete a single mapping row by original message id.
        Returns the number of rows deleted (0 or 1).
        """
        try:
            cur = self.conn.cursor()
            cur.execute(
                "DELETE FROM messages WHERE original_message_id = ?",
                (int(original_message_id),),
            )
            self.conn.commit()
            return cur.rowcount or 0
        except Exception:
            return 0

    def get_onjoin_roles(self, guild_id: int) -> list[int]:
        rows = self.conn.execute(
            "SELECT role_id FROM onjoin_roles WHERE guild_id=? ORDER BY role_id ASC",
            (int(guild_id),),
        ).fetchall()
        return [int(r[0]) for r in rows]

    def has_onjoin_role(self, guild_id: int, role_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM onjoin_roles WHERE guild_id=? AND role_id=?",
            (int(guild_id), int(role_id)),
        ).fetchone()
        return row is not None

    def add_onjoin_role(
        self, guild_id: int, role_id: int, added_by: int | None = None
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO onjoin_roles(guild_id, role_id, added_by) VALUES (?,?,?)",
                (int(guild_id), int(role_id), int(added_by) if added_by else None),
            )

    def remove_onjoin_role(self, guild_id: int, role_id: int) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "DELETE FROM onjoin_roles WHERE guild_id=? AND role_id=?",
                (int(guild_id), int(role_id)),
            )
            return cur.rowcount > 0

    def toggle_onjoin_role(
        self, guild_id: int, role_id: int, added_by: int | None = None
    ) -> bool:
        """Returns True if ADDED, False if REMOVED."""
        if self.has_onjoin_role(guild_id, role_id):
            self.remove_onjoin_role(guild_id, role_id)
            return False
        self.add_onjoin_role(guild_id, role_id, added_by)
        return True

    def clear_onjoin_roles(self, guild_id: int) -> int:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "DELETE FROM onjoin_roles WHERE guild_id=?",
                (int(guild_id),),
            )
            return cur.rowcount

    def backfill_create_run(
        self,
        original_channel_id: int,
        range_json: dict | None,
        *,
        run_id: str | None = None,
        original_guild_id: int | None = None,
        cloned_guild_id: int | None = None,
    ) -> str:
        run_id = run_id or uuid.uuid4().hex
        now = datetime.utcnow().isoformat() + "Z"

        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO backfill_runs(
                    run_id,
                    original_guild_id,
                    cloned_guild_id,
                    original_channel_id,
                    range_json,
                    started_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    int(original_guild_id) if original_guild_id is not None else None,
                    int(cloned_guild_id) if cloned_guild_id is not None else None,
                    int(original_channel_id),
                    json.dumps(range_json or {}),
                    now,
                    now,
                ),
            )
            self.conn.commit()
        return run_id

    def backfill_set_clone(self, run_id: str, clone_channel_id: int | None):
        now = datetime.utcnow().isoformat() + "Z"
        self.conn.execute(
            "UPDATE backfill_runs SET clone_channel_id=?, updated_at=? WHERE run_id=?",
            (int(clone_channel_id) if clone_channel_id else None, now, run_id),
        )
        self.conn.commit()

    def backfill_update_checkpoint(
        self,
        run_id: str,
        *,
        delivered: int | None = None,
        expected_total: int | None = None,
        last_orig_message_id: str | None = None,
        last_orig_timestamp: str | None = None,
    ):
        cols, vals = ["updated_at"], [datetime.utcnow().isoformat() + "Z"]
        if delivered is not None:
            cols += ["delivered"]
            vals += [int(delivered)]
        if expected_total is not None:
            cols += ["expected_total"]
            vals += [int(expected_total)]
        if last_orig_message_id is not None:
            cols += ["last_orig_message_id"]
            vals += [str(last_orig_message_id)]
        if last_orig_timestamp is not None:
            cols += ["last_orig_timestamp"]
            vals += [last_orig_timestamp]
        sql = (
            f"UPDATE backfill_runs SET {', '.join(c+'=?' for c in cols)} WHERE run_id=?"
        )
        self.conn.execute(sql, (*vals, run_id))
        self.conn.commit()

    def backfill_mark_done(self, run_id: str):
        now = datetime.utcnow().isoformat() + "Z"
        self.conn.execute(
            "UPDATE backfill_runs SET status='completed', updated_at=? WHERE run_id=?",
            (now, run_id),
        )
        self.conn.commit()

    def backfill_mark_failed(self, run_id: str, error: str | None):
        now = datetime.utcnow().isoformat() + "Z"
        self.conn.execute(
            "UPDATE backfill_runs SET status='failed', error=?, updated_at=? WHERE run_id=?",
            (error, now, run_id),
        )
        self.conn.commit()

    def backfill_abandon_running_on_boot(self):
        now = datetime.utcnow().isoformat() + "Z"
        self.conn.execute(
            "UPDATE backfill_runs SET status='aborted', updated_at=? WHERE status='running'",
            (now,),
        )
        self.conn.commit()

    def backfill_get_incomplete_for_channel(self, original_channel_id: int):
        cur = self.conn.execute(
            """
            SELECT
                run_id,
                original_channel_id,
                clone_channel_id,
                status,
                range_json,
                started_at,
                updated_at,
                delivered,
                expected_total,
                last_orig_message_id,
                last_orig_timestamp,
                error,
                original_guild_id,
                cloned_guild_id
            FROM backfill_runs
            WHERE original_channel_id = ? AND status = 'running'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (int(original_channel_id),),
        )
        row = cur.fetchone()
        if not row:
            return None

        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))

    def backfill_mark_aborted(self, run_id: str, reason: str | None = None) -> None:
        now = datetime.utcnow().isoformat() + "Z"
        self.conn.execute(
            "UPDATE backfill_runs SET status='cancelled', error=COALESCE(?, error), updated_at=? WHERE run_id=?",
            (reason, now, run_id),
        )
        self.conn.commit()

    def backfill_abort_running_for_channel(
        self, original_channel_id: int, reason: str | None = None
    ) -> int:
        now = datetime.utcnow().isoformat() + "Z"
        cur = self.conn.execute(
            "UPDATE backfill_runs SET status='cancelled', error=COALESCE(?, error), updated_at=? "
            "WHERE original_channel_id=? AND status='running'",
            (reason, now, int(original_channel_id)),
        )
        self.conn.commit()
        return cur.rowcount

    def get_clone_guild_ids(self, original_guild_id: int) -> List[int]:
        """
        Given a host/original guild, return all clone guild IDs.
        """
        with self.lock, self.conn:
            cur = self.conn.execute(
                "SELECT cloned_guild_id FROM guild_mappings WHERE original_guild_id = ?;",
                (original_guild_id,),
            )
            return [row[0] for row in cur.fetchall() if row[0] is not None]

    def get_host_guild_ids(self, cloned_guild_id: int) -> List[int]:
        """
        Given a clone guild, return all original/host guild IDs.
        """
        with self.lock, self.conn:
            cur = self.conn.execute(
                "SELECT original_guild_id FROM guild_mappings WHERE cloned_guild_id = ?;",
                (cloned_guild_id,),
            )
            return [row[0] for row in cur.fetchall() if row[0] is not None]

    def upsert_guild_mapping(
        self,
        *,
        mapping_id: Optional[str],
        mapping_name: str,
        original_guild_id: int,
        original_guild_name: str,
        original_guild_icon_url: str | None,
        cloned_guild_id: int,
        cloned_guild_name: str,
        settings: Optional[Dict] = None,
        overwrite_identity: bool = False,
    ) -> str:
        """
        Insert or update a guild mapping row.
        """
        if not mapping_id:
            mapping_id = uuid.uuid4().hex

        settings_json = json.dumps(settings or {}, separators=(",", ":"))

        update_assignments = [
            "mapping_name      = excluded.mapping_name",
            "original_guild_id = excluded.original_guild_id",
            "cloned_guild_id   = excluded.cloned_guild_id",
            "last_updated      = CURRENT_TIMESTAMP",
        ]

        if settings is not None:
            update_assignments.insert(3, "settings          = excluded.settings")

        if overwrite_identity:
            update_assignments.extend(
                [
                    "original_guild_name     = excluded.original_guild_name",
                    "original_guild_icon_url = excluded.original_guild_icon_url",
                    "cloned_guild_name       = excluded.cloned_guild_name",
                ]
            )

        update_sql = ",\n                ".join(update_assignments)

        sql = f"""
            INSERT INTO guild_mappings (
                mapping_id,
                mapping_name,
                original_guild_id,
                original_guild_name,
                original_guild_icon_url,
                cloned_guild_id,
                cloned_guild_name,
                settings,
                created_at,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(strftime('%s','now') AS INTEGER), CURRENT_TIMESTAMP)
            ON CONFLICT(mapping_id) DO UPDATE SET
                    {update_sql}
        """

        with self.conn:
            self.conn.execute(
                sql,
                (
                    mapping_id,
                    mapping_name or "",
                    int(original_guild_id),
                    original_guild_name or "",
                    original_guild_icon_url or None,
                    int(cloned_guild_id),
                    cloned_guild_name or "",
                    settings_json,
                ),
            )

        return mapping_id

    def delete_guild_mapping(self, mapping_id: str) -> None:
        """
        Hard-delete a mapping AND all data tied to that mapping's
        (original_guild_id, cloned_guild_id) pair across the DB.
        """

        m = self.get_mapping_by_id(mapping_id)
        if not m:
            return

        ogid = int(m["original_guild_id"] or 0)
        cgid = int(m["cloned_guild_id"] or 0)

        tables_to_clean = [
            "messages",
            "filters",
            "blocked_keywords",
            "backfill_runs",
            "role_blocks",
            "threads",
            "channel_mappings",
            "category_mappings",
            "role_mappings",
            "emoji_mappings",
            "sticker_mappings",
        ]

        with self.conn:
            for tbl in tables_to_clean:
                try:

                    if tbl == "role_blocks":
                        self.conn.execute(
                            f"""
                            DELETE FROM {tbl}
                            WHERE cloned_guild_id = ?
                            """,
                            (cgid,),
                        )
                    else:
                        self.conn.execute(
                            f"""
                            DELETE FROM {tbl}
                            WHERE original_guild_id = ?
                            AND cloned_guild_id   = ?
                            """,
                            (ogid, cgid),
                        )
                except sqlite3.OperationalError:

                    pass

            self.conn.execute(
                "DELETE FROM guild_mappings WHERE mapping_id = ?",
                (mapping_id,),
            )

    def get_mapping_by_original(self, original_guild_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM guild_mappings WHERE original_guild_id = ? LIMIT 1",
            (int(original_guild_id),),
        ).fetchone()
        if not row:
            return None
        d = {k: row[k] for k in row.keys()}
        try:
            d["settings"] = json.loads(d.get("settings") or "{}")
        except Exception:
            d["settings"] = {}
        return d

    def get_mapping_by_clone(self, cloned_guild_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM guild_mappings WHERE cloned_guild_id = ? LIMIT 1",
            (int(cloned_guild_id),),
        ).fetchone()

        if not row:
            return None

        d = {k: row[k] for k in row.keys()}

        try:
            d["settings"] = json.loads(d.get("settings") or "{}")
        except Exception:
            d["settings"] = {}

        return d

    def list_guild_mappings(self) -> List[dict]:
        cur = self.conn.execute(
            """
            SELECT
                mapping_id,
                mapping_name,
                original_guild_id,
                original_guild_name,
                original_guild_icon_url,
                cloned_guild_id,
                cloned_guild_name,
                settings,
                status,
                created_at,
                last_updated
            FROM guild_mappings
            ORDER BY created_at ASC, mapping_id ASC
            """
        )
        rows = cur.fetchall()
        out = []
        for r in rows:
            settings_raw = r["settings"]
            try:
                settings_obj = json.loads(settings_raw or "{}")
            except Exception:
                settings_obj = {}

            out.append(
                {
                    "mapping_id": r["mapping_id"],
                    "mapping_name": r["mapping_name"],
                    "original_guild_id": str(r["original_guild_id"] or ""),
                    "original_guild_name": r["original_guild_name"] or "",
                    "original_guild_icon_url": r["original_guild_icon_url"] or "",
                    "cloned_guild_id": str(r["cloned_guild_id"] or ""),
                    "cloned_guild_name": r["cloned_guild_name"] or "",
                    "settings": settings_obj,
                    "status": (r["status"] or "active"),
                }
            )
        return out

    def get_all_original_guild_ids(self) -> list[int]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT original_guild_id
            FROM guild_mappings
            WHERE original_guild_id IS NOT NULL
            AND original_guild_id != 0
            AND (status IS NULL OR status = 'active')
            """
        ).fetchall()
        return [int(r[0]) for r in rows]

    def get_all_clone_guild_ids(self) -> list[int]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT cloned_guild_id
            FROM guild_mappings
            WHERE cloned_guild_id IS NOT NULL
            AND cloned_guild_id != 0
            AND (status IS NULL OR status = 'active')
            """
        ).fetchall()
        return [int(r[0]) for r in rows]

    def is_clone_guild_id(self, guild_id: int) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM guild_mappings
            WHERE cloned_guild_id = ?
            AND (status IS NULL OR status = 'active')
            """,
            (int(guild_id),),
        ).fetchone()
        return bool(row)

    def bulk_fill_guild_ids(self, *, host_guild_id: int, clone_guild_id: int) -> dict:
        """
        Backfill original_guild_id and cloned_guild_id for legacy rows.
        - Uses precise joins where possible (e.g., categories → channels, forums → threads)
        - Only updates rows where the target column is NULL (or 0)
        - Bumps last_updated where a change is made
        Returns a dict of changed-row counts per table.
        """
        host = int(host_guild_id)
        clone = int(clone_guild_id)
        out = {}

        with self.lock, self.conn:
            cur = self.conn.cursor()

            cur.execute(
                """
                UPDATE category_mappings
                SET original_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
            """,
                (host,),
            )
            out["category_mappings.original_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE category_mappings
                SET cloned_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["category_mappings.cloned_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE channel_mappings AS ch
                SET original_guild_id = (
                    SELECT cm.original_guild_id
                    FROM category_mappings AS cm
                    WHERE cm.original_category_id = ch.original_parent_category_id
                ),
                    last_updated = CURRENT_TIMESTAMP
                WHERE (ch.original_guild_id IS NULL OR ch.original_guild_id = 0)
                AND ch.original_parent_category_id IS NOT NULL
            """
            )
            out["channel_mappings.orig_from_parent"] = cur.rowcount

            cur.execute(
                """
                UPDATE channel_mappings AS ch
                SET cloned_guild_id = (
                    SELECT cm.cloned_guild_id
                    FROM category_mappings AS cm
                    WHERE cm.cloned_category_id = ch.cloned_parent_category_id
                ),
                    last_updated = CURRENT_TIMESTAMP
                WHERE (ch.cloned_guild_id IS NULL OR ch.cloned_guild_id = 0)
                AND ch.cloned_parent_category_id IS NOT NULL
            """
            )
            out["channel_mappings.clone_from_parent"] = cur.rowcount

            cur.execute(
                """
                UPDATE channel_mappings
                SET original_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
            """,
                (host,),
            )
            out["channel_mappings.orig_fallback"] = cur.rowcount

            cur.execute(
                """
                UPDATE channel_mappings
                SET cloned_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["channel_mappings.clone_fallback"] = cur.rowcount

            cur.execute(
                """
                UPDATE threads AS t
                SET original_guild_id = (
                    SELECT ch.original_guild_id
                    FROM channel_mappings AS ch
                    WHERE ch.original_channel_id = t.forum_original_id
                ),
                    last_updated = CURRENT_TIMESTAMP
                WHERE (t.original_guild_id IS NULL OR t.original_guild_id = 0)
                AND t.forum_original_id IS NOT NULL
            """
            )
            out["threads.orig_from_forum"] = cur.rowcount

            cur.execute(
                """
                UPDATE threads AS t
                SET cloned_guild_id = (
                    SELECT ch.cloned_guild_id
                    FROM channel_mappings AS ch
                    WHERE ch.cloned_channel_id = t.forum_cloned_id
                ),
                    last_updated = CURRENT_TIMESTAMP
                WHERE (t.cloned_guild_id IS NULL OR t.cloned_guild_id = 0)
                AND t.forum_cloned_id IS NOT NULL
            """
            )
            out["threads.clone_from_forum"] = cur.rowcount

            cur.execute(
                """
                UPDATE threads
                SET original_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
            """,
                (host,),
            )
            out["threads.orig_fallback"] = cur.rowcount

            cur.execute(
                """
                UPDATE threads
                SET cloned_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["threads.clone_fallback"] = cur.rowcount

            cur.execute(
                """
                UPDATE emoji_mappings
                SET original_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
            """,
                (host,),
            )
            out["emoji_mappings.orig_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE emoji_mappings
                SET cloned_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["emoji_mappings.clone_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE sticker_mappings
                SET original_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
            """,
                (host,),
            )
            out["sticker_mappings.orig_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE sticker_mappings
                SET cloned_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["sticker_mappings.clone_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE filters
                SET original_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
            """,
                (host,),
            )
            out["filter_mappings.orig_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE filters
                SET cloned_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["filter_mappings.clone_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE backfill_runs
                SET original_guild_id = ?
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
            """,
                (host,),
            )
            out["backfill_mappings.orig_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE backfill_runs
                SET cloned_guild_id = ?
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["backfill_mappings.clone_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE role_mappings
                SET original_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
            """,
                (host,),
            )
            out["role_mappings.orig_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE role_mappings
                SET cloned_guild_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["role_mappings.clone_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE role_blocks
                SET cloned_guild_id = ?
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
            """,
                (clone,),
            )
            out["role_blocks.clone_set"] = cur.rowcount

            cur.execute(
                """
                UPDATE messages AS m
                SET cloned_guild_id = (
                    SELECT ch.cloned_guild_id
                    FROM channel_mappings AS ch
                    WHERE ch.cloned_channel_id = m.cloned_channel_id
                ),
                    updated_at = strftime('%s','now')
                WHERE (m.cloned_guild_id IS NULL OR m.cloned_guild_id = 0)
                  AND m.cloned_channel_id IS NOT NULL
                """
            )
            out["messages.clone_from_channel"] = cur.rowcount

            cur.execute(
                """
                UPDATE messages
                SET original_guild_id = ?,
                    updated_at = strftime('%s','now')
                WHERE (original_guild_id IS NULL OR original_guild_id = 0)
                """,
                (host,),
            )
            out["messages.orig_fallback"] = cur.rowcount

            cur.execute(
                """
                UPDATE messages
                SET cloned_guild_id = ?,
                    updated_at = strftime('%s','now')
                WHERE (cloned_guild_id IS NULL OR cloned_guild_id = 0)
                """,
                (clone,),
            )
            out["messages.clone_fallback"] = cur.rowcount

        return out

    def delete_config(self, key: str) -> None:
        """
        Remove a key from app_config.
        Safe to call even if the key doesn't exist.
        """
        with self.lock, self.conn:
            self.conn.execute(
                "DELETE FROM app_config WHERE key = ?",
                (key,),
            )
            self.conn.commit()

    def get_mapping_by_id(self, mapping_id: str) -> dict | None:
        """
        Return a single guild_mappings row (plus parsed settings) for a mapping_id.
        Keys match list_guild_mappings() output.
        """
        row = self.conn.execute(
            """
            SELECT
                mapping_id,
                mapping_name,
                original_guild_id,
                original_guild_name,
                original_guild_icon_url,
                cloned_guild_id,
                cloned_guild_name,
                settings,
                created_at,
                last_updated
            FROM guild_mappings
            WHERE mapping_id = ?
            LIMIT 1
            """,
            (mapping_id,),
        ).fetchone()

        if not row:
            return None

        try:
            settings_obj = json.loads(row["settings"] or "{}")
        except Exception:
            settings_obj = {}

        return {
            "mapping_id": row["mapping_id"],
            "mapping_name": row["mapping_name"],
            "original_guild_id": str(row["original_guild_id"] or ""),
            "original_guild_name": row["original_guild_name"] or "",
            "original_guild_icon_url": row["original_guild_icon_url"] or "",
            "cloned_guild_id": str(row["cloned_guild_id"] or ""),
            "cloned_guild_name": row["cloned_guild_name"] or "",
            "settings": settings_obj,
        }

    def get_channel_mapping_for_mapping(
        self, original_channel_id: int, mapping_id: str
    ):
        with self.conn:
            return self.conn.execute(
                """
                SELECT cm.*
                FROM channel_mappings cm
                JOIN guild_mappings gm ON gm.cloned_guild_id = cm.cloned_guild_id
                WHERE cm.original_channel_id = ?
                AND gm.mapping_id = ?
                LIMIT 1
                """,
                (int(original_channel_id), str(mapping_id)),
            ).fetchone()

    def backfill_get_incomplete_for_channel_in_clone(
        self, original_channel_id: int, cloned_guild_id: int
    ):
        cur = self.conn.execute(
            """
            SELECT
                run_id,
                original_channel_id,
                clone_channel_id,
                status,
                range_json,
                started_at,
                updated_at,
                delivered,
                expected_total,
                last_orig_message_id,
                last_orig_timestamp,
                error,
                original_guild_id,
                cloned_guild_id
            FROM backfill_runs
            WHERE original_channel_id = ?
            AND cloned_guild_id = ?
            AND status = 'running'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (int(original_channel_id), int(cloned_guild_id)),
        )
        row = cur.fetchone()
        if not row:
            return None

        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))

    def backfill_abort_running_for_channel_in_clone(
        self,
        original_channel_id: int,
        cloned_guild_id: int,
        reason: str | None = None,
    ) -> int:
        """
        Abort any 'running' backfill_runs rows for this (source channel, target clone guild)
        combo only. We do NOT touch runs for other cloned_guild_id values.
        """
        now = datetime.utcnow().isoformat() + "Z"
        cur = self.conn.execute(
            """
            UPDATE backfill_runs
            SET status='cancelled',
                error=COALESCE(?, error),
                updated_at=?
            WHERE original_channel_id = ?
            AND cloned_guild_id = ?
            AND status='running'
            """,
            (reason, now, int(original_channel_id), int(cloned_guild_id)),
        )
        self.conn.commit()
        return cur.rowcount

    def get_mapping_by_cloned_guild_id(self, cloned_guild_id: int):
        """
        Return the guild_mappings row for a given cloned_guild_id.
        Expected columns in guild_mappings:
        mapping_id, original_guild_id, cloned_guild_id, ...
        """
        return self.conn.execute(
            "SELECT * FROM guild_mappings WHERE cloned_guild_id = ? LIMIT 1",
            (int(cloned_guild_id),),
        ).fetchone()

    def get_blocked_keywords_by_origin(self) -> dict[int, list[str]]:
        """
        Returns { original_guild_id(int or 0 for global): [ 'word', 'word2', ... ], ... }
        Rows with original_guild_id NULL are treated as global (key 0).
        """
        rows = self.conn.execute(
            "SELECT original_guild_id, keyword FROM blocked_keywords"
        ).fetchall()

        out: dict[int, list[str]] = {}
        for r in rows:
            ogid = r["original_guild_id"]

            key = int(ogid) if ogid is not None else 0
            out.setdefault(key, []).append(str(r["keyword"]))
        return out

    def get_blocked_keywords_for_origin(self, original_guild_id: int) -> list[str]:
        """
        All keywords that apply to this origin guild, plus any global NULL/NULL ones.
        Deduped, lowercased, sorted.
        """
        rows = self.conn.execute(
            """
            SELECT keyword
            FROM blocked_keywords
            WHERE original_guild_id = ?
            OR original_guild_id IS NULL
            """,
            (int(original_guild_id),),
        ).fetchall()

        kws = {
            str(r["keyword"]).strip().lower() for r in rows if str(r["keyword"]).strip()
        }
        return sorted(kws)

    def toggle_blocked_keyword(
        self,
        keyword: str,
        *,
        original_guild_id: int | None,
        cloned_guild_id: int | None,
    ) -> tuple[bool, str]:
        """
        Toggle this keyword for the given mapping.
        Returns (changed: bool, action: 'added'|'removed'|'none')
        """
        if self.add_blocked_keyword(
            keyword,
            original_guild_id=original_guild_id,
            cloned_guild_id=cloned_guild_id,
        ):
            return True, "added"

        if self.remove_blocked_keyword(
            keyword,
            original_guild_id=original_guild_id,
            cloned_guild_id=cloned_guild_id,
        ):
            return True, "removed"

        return False, "none"

    def get_filters_for_mapping(self, mapping_id: str) -> dict:
        """
        Return all filter data for a mapping:
        {
            "whitelist": {
                "category": [catId, ...],
                "channel": [chanId, ...],
            },
            "exclude": {
                "category": [catId, ...],
                "channel": [chanId, ...],
            },
            "blocked_words": ["foo", "bar", ...],
        }
        """
        mapping_row = self.get_mapping_by_id(mapping_id)
        if not mapping_row:
            return {
                "whitelist": {"category": [], "channel": []},
                "exclude": {"category": [], "channel": []},
                "blocked_words": [],
            }

        host_gid = int(mapping_row["original_guild_id"])
        clone_gid = int(mapping_row["cloned_guild_id"])

        wl_cats: list[str] = []
        wl_chans: list[str] = []
        ex_cats: list[str] = []
        ex_chans: list[str] = []

        cur = self.conn.cursor()

        cur.execute(
            """
            SELECT kind, scope, obj_id
            FROM filters
            WHERE original_guild_id=? AND cloned_guild_id=?
            """,
            (host_gid, clone_gid),
        )

        for row in cur.fetchall():
            kind = (row["kind"] or "").strip().lower()
            scope = (row["scope"] or "").strip().lower()
            obj_id = str(row["obj_id"])

            if kind == "whitelist":
                if scope == "category":
                    wl_cats.append(obj_id)
                elif scope == "channel":
                    wl_chans.append(obj_id)

            elif kind == "exclude":
                if scope == "category":
                    ex_cats.append(obj_id)
                elif scope == "channel":
                    ex_chans.append(obj_id)

        cur.execute(
            """
            SELECT keyword
            FROM blocked_keywords
            WHERE original_guild_id=? AND cloned_guild_id=?
            ORDER BY keyword COLLATE NOCASE ASC
            """,
            (host_gid, clone_gid),
        )
        word_rows = cur.fetchall()
        blocked_words = [
            str(r["keyword"]).strip() for r in word_rows if str(r["keyword"]).strip()
        ]

        return {
            "whitelist": {
                "category": wl_cats,
                "channel": wl_chans,
            },
            "exclude": {
                "category": ex_cats,
                "channel": ex_chans,
            },
            "blocked_words": blocked_words,
        }

    def replace_filters_for_mapping(
        self,
        mapping_id: str,
        wl_categories: list[str],
        wl_channels: list[str],
        ex_categories: list[str],
        ex_channels: list[str],
    ):
        m = self.get_mapping_by_id(mapping_id)
        if not m:
            return

        ogid = int(m["original_guild_id"] or 0)
        cgid = int(m["cloned_guild_id"] or 0)

        with self.conn:
            self.conn.execute(
                """
                DELETE FROM filters
                WHERE original_guild_id = ?
                AND cloned_guild_id   = ?
                """,
                (ogid, cgid),
            )

            def bulk_insert(kind, scope, ids):
                for _id in ids:
                    try:
                        snow = int(_id)
                    except Exception:
                        continue
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO filters
                        (kind, scope, obj_id, original_guild_id, cloned_guild_id, last_updated)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (kind, scope, snow, ogid, cgid),
                    )

            bulk_insert("whitelist", "category", wl_categories)
            bulk_insert("whitelist", "channel", wl_channels)
            bulk_insert("exclude", "category", ex_categories)
            bulk_insert("exclude", "channel", ex_channels)

    def get_mapping_name_for_original(self, original_guild_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT mapping_name FROM guild_mappings WHERE original_guild_id = ? LIMIT 1",
            (int(original_guild_id),),
        ).fetchone()
        if row and row[0]:
            return row[0]
        return None

    def get_mapping_name_for_clone(self, cloned_guild_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT mapping_name FROM guild_mappings WHERE cloned_guild_id = ? LIMIT 1",
            (int(cloned_guild_id),),
        ).fetchone()
        if row and row[0]:
            return row[0]
        return None

    def replace_blocked_keywords_for_mapping(
        self,
        mapping_id: str,
        words: list[str],
    ) -> None:
        """
        Overwrite the blocked_keywords list for this mapping_id
        (original_guild_id + cloned_guild_id pair)
        with the provided words list.
        """

        if not mapping_id:
            return

        mapping_row = self.get_mapping_by_id(mapping_id)
        if not mapping_row:
            return

        host_gid = int(mapping_row["original_guild_id"])
        clone_gid = int(mapping_row["cloned_guild_id"])

        cleaned: list[str] = []
        for w in words or []:
            w2 = (w or "").strip()
            if not w2:
                continue
            if w2 not in cleaned:
                cleaned.append(w2)

        with self.conn:

            self.conn.execute(
                """
                DELETE FROM blocked_keywords
                WHERE original_guild_id=? AND cloned_guild_id=?
                """,
                (host_gid, clone_gid),
            )

            for w in cleaned:
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO blocked_keywords
                    (keyword, original_guild_id, cloned_guild_id, added_at)
                    VALUES (
                        ?, ?, ?,
                        strftime('%Y-%m-%d %H:%M:%S','now')
                    )
                    """,
                    (w, host_gid, clone_gid),
                )

    def get_clone_guild_ids_for_origin(self, original_guild_id: int) -> list[int]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT cloned_guild_id
            FROM guild_mappings
            WHERE original_guild_id = ?
            AND cloned_guild_id IS NOT NULL
            AND cloned_guild_id != 0
            AND (status IS NULL OR status = 'active')
            """,
            (int(original_guild_id),),
        ).fetchall()
        return [int(r[0]) for r in rows]

    def list_mappings_by_origin(self, original_guild_id: int):
        return self.conn.execute(
            "SELECT * FROM guild_mappings WHERE original_guild_id=?",
            (int(original_guild_id),),
        ).fetchall()

    def get_mapping_by_id(self, mapping_id: str):
        return self.conn.execute(
            "SELECT * FROM guild_mappings WHERE mapping_id = ? LIMIT 1",
            (str(mapping_id),),
        ).fetchone()

    def get_mapping_by_original_and_clone(
        self, original_guild_id: int, cloned_guild_id: int
    ):
        return self.conn.execute(
            "SELECT * FROM guild_mappings WHERE original_guild_id = ? AND cloned_guild_id = ? LIMIT 1",
            (int(original_guild_id), int(cloned_guild_id)),
        ).fetchone()

    def get_channel_mappings_for_original(self, original_channel_id: int):
        return self.conn.execute(
            "SELECT * FROM channel_mappings WHERE original_channel_id=? ORDER BY cloned_guild_id",
            (int(original_channel_id),),
        ).fetchall()

    def get_thread_mapping_by_original_and_clone(
        self, original_thread_id: int, cloned_guild_id: int
    ) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM threads WHERE original_thread_id = ? AND cloned_guild_id = ? LIMIT 1",
            (int(original_thread_id), int(cloned_guild_id)),
        ).fetchone()

    def get_channel_mapping_by_original_and_clone(
        self, original_channel_id: int, cloned_guild_id: int
    ):
        return self.conn.execute(
            "SELECT * FROM channel_mappings WHERE original_channel_id=? AND cloned_guild_id=? LIMIT 1",
            (int(original_channel_id), int(cloned_guild_id)),
        ).fetchone()

    def delete_channel_mapping_pair(
        self, original_channel_id: int, cloned_guild_id: int
    ):
        with self.lock, self.conn:
            self.conn.execute(
                "DELETE FROM channel_mappings WHERE original_channel_id=? AND cloned_guild_id=?",
                (int(original_channel_id), int(cloned_guild_id)),
            )

    def get_thread_mappings_for_original(self, original_thread_id: int) -> list[dict]:
        cur = self.conn.execute(
            """
            SELECT *
            FROM threads
            WHERE original_thread_id = ?
            ORDER BY cloned_guild_id, cloned_thread_id
            """,
            (int(original_thread_id),),
        )
        rows = cur.fetchall() or []
        return [dict(r) for r in rows]

    def get_thread_mapping_pair(
        self, original_thread_id: int, cloned_guild_id: int
    ) -> sqlite3.Row | None:
        """
        Return the single row for (original_thread_id, cloned_guild_id).
        """
        return self.conn.execute(
            "SELECT * FROM threads WHERE original_thread_id = ? AND cloned_guild_id = ? LIMIT 1",
            (int(original_thread_id), int(cloned_guild_id)),
        ).fetchone()

    def delete_forum_thread_mapping_for_clone(
        self, original_thread_id: int, cloned_guild_id: int
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                DELETE FROM threads
                WHERE original_thread_id = ? AND cloned_guild_id = ?
                """,
                (int(original_thread_id), int(cloned_guild_id)),
            )
            self.conn.commit()

    def delete_category_mapping_pair(
        self, original_category_id: int, cloned_guild_id: int
    ):
        with self.lock, self.conn:
            self.conn.execute(
                "DELETE FROM category_mappings WHERE original_category_id=? AND cloned_guild_id=?",
                (int(original_category_id), int(cloned_guild_id)),
            )

    def delete_message_mapping_pair(
        self, original_message_id: int, cloned_guild_id: int
    ) -> int:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "DELETE FROM messages WHERE original_message_id=? AND cloned_guild_id=?",
                (int(original_message_id), int(cloned_guild_id)),
            )
            return cur.rowcount or 0

    def get_emoji_mapping_for_clone(self, original_id: int, cloned_guild_id: int):
        return self.conn.execute(
            "SELECT * FROM emoji_mappings WHERE original_emoji_id = ? AND cloned_guild_id = ? LIMIT 1",
            (int(original_id), int(cloned_guild_id)),
        ).fetchone()

    def get_emoji_mappings_for_original(self, original_id: int) -> list:
        return self.conn.execute(
            "SELECT * FROM emoji_mappings WHERE original_emoji_id = ? ORDER BY cloned_guild_id",
            (int(original_id),),
        ).fetchall()

    def delete_emoji_mapping_for_clone(
        self, original_id: int, cloned_guild_id: int
    ) -> None:
        self.conn.execute(
            "DELETE FROM emoji_mappings WHERE original_emoji_id = ? AND cloned_guild_id = ?",
            (int(original_id), int(cloned_guild_id)),
        )
        self.conn.commit()

    def delete_emoji_mapping(self, original_id: int) -> None:
        self.conn.execute(
            "DELETE FROM emoji_mappings WHERE original_emoji_id = ?",
            (int(original_id),),
        )
        self.conn.commit()

    def get_sticker_mapping_for_clone(self, original_id: int, cloned_guild_id: int):
        return self.conn.execute(
            "SELECT * FROM sticker_mappings WHERE original_sticker_id = ? AND cloned_guild_id = ? LIMIT 1",
            (int(original_id), int(cloned_guild_id)),
        ).fetchone()

    def get_sticker_mappings_for_original(self, original_id: int) -> list:
        return self.conn.execute(
            "SELECT * FROM sticker_mappings WHERE original_sticker_id = ? ORDER BY cloned_guild_id",
            (int(original_id),),
        ).fetchall()

    def delete_sticker_mapping_for_clone(
        self, original_id: int, cloned_guild_id: int
    ) -> None:
        self.conn.execute(
            "DELETE FROM sticker_mappings WHERE original_sticker_id = ? AND cloned_guild_id = ?",
            (int(original_id), int(cloned_guild_id)),
        )
        self.conn.commit()

    def get_role_mapping_for_clone(self, original_id: int, cloned_guild_id: int):
        return self.conn.execute(
            "SELECT * FROM role_mappings WHERE original_role_id = ? AND cloned_guild_id = ? LIMIT 1",
            (int(original_id), int(cloned_guild_id)),
        ).fetchone()

    def get_role_mapping_by_cloned_id(self, cloned_role_id: int):
        return self.conn.execute(
            "SELECT * FROM role_mappings WHERE cloned_role_id = ?",
            (int(cloned_role_id),),
        ).fetchone()

    def get_role_mappings_for_original(self, original_id: int) -> list:
        return self.conn.execute(
            "SELECT * FROM role_mappings WHERE original_role_id = ? ORDER BY cloned_guild_id",
            (int(original_id),),
        ).fetchall()

    def delete_role_mapping_for_clone(
        self, original_id: int, cloned_guild_id: int
    ) -> None:
        self.conn.execute(
            "DELETE FROM role_mappings WHERE original_role_id = ? AND cloned_guild_id = ?",
            (int(original_id), int(cloned_guild_id)),
        )
        self.conn.commit()

    def get_category_mapping_by_original_and_clone(
        self, original_category_id: int, cloned_guild_id: int
    ):
        return self.conn.execute(
            "SELECT * FROM category_mappings WHERE original_category_id = ? AND cloned_guild_id = ? LIMIT 1",
            (int(original_category_id), int(cloned_guild_id)),
        ).fetchone()

    def get_category_mapping_for_clone(
        self, original_category_id: int, cloned_guild_id: int
    ) -> Optional[sqlite3.Row]:
        """
        Return the row from category_mappings for this (original_category_id, cloned_guild_id),
        or None if not mapped for this clone.
        """
        with self.lock, self.conn:
            return self.conn.execute(
                """
                SELECT *
                FROM category_mappings
                WHERE original_category_id=? AND cloned_guild_id=?
                LIMIT 1
                """,
                (int(original_category_id), int(cloned_guild_id)),
            ).fetchone()

    def iter_child_channel_mappings_for_clone_category(
        self, original_category_id: int, cloned_guild_id: int
    ) -> list[sqlite3.Row]:
        """
        All channel_mappings in THIS clone whose original parent is the given category.
        """
        with self.lock, self.conn:
            rows = self.conn.execute(
                """
                SELECT *
                FROM channel_mappings
                WHERE original_parent_category_id=? AND cloned_guild_id=?
                """,
                (int(original_category_id), int(cloned_guild_id)),
            ).fetchall()
            return list(rows)

    def delete_channel_mapping_for_clone(
        self, original_channel_id: int, cloned_guild_id: int
    ) -> Optional[int]:
        """
        Delete the mapping row for (original_channel_id, cloned_guild_id).
        Returns the cloned_channel_id if one existed (so caller can delete it in Discord).
        """
        with self.lock, self.conn:
            row = self.conn.execute(
                """
                SELECT cloned_channel_id
                FROM channel_mappings
                WHERE original_channel_id=? AND cloned_guild_id=?
                LIMIT 1
                """,
                (int(original_channel_id), int(cloned_guild_id)),
            ).fetchone()
            cloned_id = (
                int(row["cloned_channel_id"])
                if row and row["cloned_channel_id"] is not None
                else None
            )

            self.conn.execute(
                "DELETE FROM channel_mappings WHERE original_channel_id=? AND cloned_guild_id=?",
                (int(original_channel_id), int(cloned_guild_id)),
            )
            return cloned_id

    def reparent_children_to_root_for_clone(
        self, original_category_id: int, cloned_guild_id: int
    ) -> int:
        """
        Set cloned_parent_category_id = NULL for all channels under the given category
        but only inside THIS cloned guild.
        Returns the number of rows affected.
        """
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                UPDATE channel_mappings
                SET cloned_parent_category_id=NULL
                WHERE original_parent_category_id=? AND cloned_guild_id=?
                """,
                (int(original_category_id), int(cloned_guild_id)),
            )
            return cur.rowcount or 0

    def delete_category_mapping_pair(
        self, original_category_id: int, cloned_guild_id: int
    ) -> dict:
        """
        Delete exactly one category mapping pair for THIS clone.
        Returns {'original_category_id': ..., 'cloned_category_id': ...} with cloned id (or None).
        Does NOT touch children (callers decide whether to delete or reparent children).
        """
        with self.lock, self.conn:
            row = self.conn.execute(
                """
                SELECT original_category_id, cloned_category_id
                FROM category_mappings
                WHERE original_category_id=? AND cloned_guild_id=?
                LIMIT 1
                """,
                (int(original_category_id), int(cloned_guild_id)),
            ).fetchone()

            if not row:
                return {
                    "original_category_id": int(original_category_id),
                    "cloned_category_id": None,
                }

            cloned_cat_id = (
                int(row["cloned_category_id"])
                if row["cloned_category_id"] is not None
                else None
            )

            self.conn.execute(
                "DELETE FROM category_mappings WHERE original_category_id=? AND cloned_guild_id=?",
                (int(original_category_id), int(cloned_guild_id)),
            )
            return {
                "original_category_id": int(original_category_id),
                "cloned_category_id": cloned_cat_id,
            }

    def get_original_guild_id_for_category(
        self, original_category_id: int
    ) -> int | None:
        row = self.conn.execute(
            "SELECT original_guild_id FROM category_mappings WHERE original_category_id=? LIMIT 1",
            (original_category_id,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def get_original_guild_id_for_channel(self, original_channel_id: int) -> int | None:
        """
        Resolve the original_guild_id for a given original_channel_id.
        """
        row = self.conn.execute(
            "SELECT original_guild_id FROM channel_mappings WHERE original_channel_id=? LIMIT 1",
            (int(original_channel_id),),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def replace_role_blocks_for_mapping(
        self,
        mapping_id: str,
        original_role_ids: list[int],
    ) -> int:
        """
        Replace the set of blocked roles for a single mapping's clone guild.

        original_role_ids should be the *original* role IDs (host guild).
        Returns the number of blocked roles after the update.
        """
        m = self.get_mapping_by_id(mapping_id)
        if not m:
            return 0

        cloned_gid = int(m["cloned_guild_id"] or 0)
        if not cloned_gid:
            return 0

        ids: set[int] = set()
        for oid in original_role_ids:
            try:
                ids.add(int(oid))
            except Exception:
                continue

        with self.conn:

            self.clear_role_blocks(cloned_guild_id=cloned_gid)

            for oid in sorted(ids):
                try:
                    self.add_role_block(
                        original_role_id=oid, cloned_guild_id=cloned_gid
                    )
                except Exception:
                    continue

        return len(ids)

    def get_user_filters_for_mapping(self, mapping_id: str) -> dict[str, list[int]]:
        """
        Return user filters for a mapping:
        {
            "whitelist": [user_id, user_id, ...],
            "blacklist": [user_id, user_id, ...],
        }
        """
        mapping_row = self.get_mapping_by_id(mapping_id)
        if not mapping_row:
            return {"whitelist": [], "blacklist": []}

        host_gid = int(mapping_row["original_guild_id"])
        clone_gid = int(mapping_row["cloned_guild_id"])

        rows = self.conn.execute(
            """
            SELECT filter_type, user_id
            FROM user_filters
            WHERE original_guild_id = ? AND cloned_guild_id = ?
            ORDER BY filter_type, user_id
            """,
            (host_gid, clone_gid),
        ).fetchall()

        whitelist: list[int] = []
        blacklist: list[int] = []

        for row in rows:
            uid = int(row["user_id"])
            ftype = str(row["filter_type"]).strip().lower()

            if ftype == "whitelist":
                whitelist.append(uid)
            elif ftype == "blacklist":
                blacklist.append(uid)

        return {
            "whitelist": whitelist,
            "blacklist": blacklist,
        }

    def replace_user_filters_for_mapping(
        self,
        mapping_id: str,
        whitelist_users: list[int],
        blacklist_users: list[int],
    ) -> None:
        """
        Replace user filters for a mapping.
        """
        m = self.get_mapping_by_id(mapping_id)
        if not m:
            return

        ogid = int(m["original_guild_id"] or 0)
        cgid = int(m["cloned_guild_id"] or 0)

        with self.conn:

            self.conn.execute(
                """
                DELETE FROM user_filters
                WHERE original_guild_id = ? AND cloned_guild_id = ?
                """,
                (ogid, cgid),
            )

            for uid in whitelist_users:
                try:
                    user_id = int(uid)
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO user_filters
                        (user_id, filter_type, original_guild_id, cloned_guild_id, added_at)
                        VALUES (?, 'whitelist', ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (user_id, ogid, cgid),
                    )
                except (ValueError, TypeError):
                    continue

            for uid in blacklist_users:
                try:
                    user_id = int(uid)
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO user_filters
                        (user_id, filter_type, original_guild_id, cloned_guild_id, added_at)
                        VALUES (?, 'blacklist', ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (user_id, ogid, cgid),
                    )
                except (ValueError, TypeError):
                    continue

    def is_user_filtered(
        self,
        user_id: int,
        original_guild_id: int,
        cloned_guild_id: int,
    ) -> tuple[bool, str | None]:
        """
        Check if a user is filtered for message cloning.
        Returns (is_filtered: bool, reason: str | None)

        Logic:
        - If whitelist exists and user NOT in it -> filtered
        - If user in blacklist -> filtered
        - Otherwise -> not filtered
        """
        whitelist_count = self.conn.execute(
            """
            SELECT COUNT(*) FROM user_filters
            WHERE filter_type = 'whitelist'
            AND original_guild_id = ?
            AND cloned_guild_id = ?
            """,
            (int(original_guild_id), int(cloned_guild_id)),
        ).fetchone()[0]

        has_whitelist = whitelist_count > 0

        if has_whitelist:

            in_whitelist = self.conn.execute(
                """
                SELECT 1 FROM user_filters
                WHERE filter_type = 'whitelist'
                AND user_id = ?
                AND original_guild_id = ?
                AND cloned_guild_id = ?
                LIMIT 1
                """,
                (int(user_id), int(original_guild_id), int(cloned_guild_id)),
            ).fetchone()

            if not in_whitelist:
                return (True, "user_not_in_whitelist")

        in_blacklist = self.conn.execute(
            """
            SELECT 1 FROM user_filters
            WHERE filter_type = 'blacklist'
            AND user_id = ?
            AND original_guild_id = ?
            AND cloned_guild_id = ?
            LIMIT 1
            """,
            (int(user_id), int(original_guild_id), int(cloned_guild_id)),
        ).fetchone()

        if in_blacklist:
            return (True, "user_in_blacklist")

        return (False, None)

    def add_role_mention(
        self,
        original_guild_id: int,
        cloned_guild_id: int,
        cloned_role_id: int,
        cloned_channel_id: int | None = None,
    ) -> bool:
        """
        Add a role mention configuration.

        Returns True if a new config was created, False if it already existed.
        """
        cfg_id = secrets.token_hex(4)

        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO role_mentions
                (role_mention_id, original_guild_id, cloned_guild_id, cloned_channel_id, cloned_role_id, added_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    cfg_id,
                    int(original_guild_id),
                    int(cloned_guild_id),
                    int(cloned_channel_id) if cloned_channel_id is not None else None,
                    int(cloned_role_id),
                ),
            )
            return cur.rowcount > 0

    def remove_role_mention(
        self,
        original_guild_id: int,
        cloned_guild_id: int,
        cloned_role_id: int,
        cloned_channel_id: int | None = None,
    ) -> bool:
        """
        Remove a role mention configuration.
        """
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                DELETE FROM role_mentions
                WHERE original_guild_id = ?
                AND cloned_guild_id = ?
                AND cloned_role_id = ?
                AND (
                    (cloned_channel_id IS NULL AND ? IS NULL)
                    OR cloned_channel_id = ?
                )
                """,
                (
                    int(original_guild_id),
                    int(cloned_guild_id),
                    int(cloned_role_id),
                    int(cloned_channel_id) if cloned_channel_id is not None else None,
                    int(cloned_channel_id) if cloned_channel_id is not None else None,
                ),
            )
            return cur.rowcount > 0

    def remove_role_mention_by_id(
        self,
        original_guild_id: int,
        cloned_guild_id: int,
        role_mention_id: str,
    ) -> bool:
        """
        Remove a role mention configuration by its short ID.
        """
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                DELETE FROM role_mentions
                WHERE original_guild_id = ?
                AND cloned_guild_id = ?
                AND role_mention_id = ?
                """,
                (
                    int(original_guild_id),
                    int(cloned_guild_id),
                    str(role_mention_id),
                ),
            )
            return cur.rowcount > 0

    def get_role_mentions(
        self,
        original_guild_id: int,
        cloned_guild_id: int,
        cloned_channel_id: int | None = None,
    ) -> list[int]:
        """
        Get role mentions for a specific channel or globally in a clone guild.

        """
        if cloned_channel_id is None:

            rows = self.conn.execute(
                """
                SELECT cloned_role_id
                FROM role_mentions
                WHERE original_guild_id = ?
                AND cloned_guild_id = ?
                AND cloned_channel_id IS NULL
                ORDER BY added_at ASC
                """,
                (int(original_guild_id), int(cloned_guild_id)),
            ).fetchall()
        else:

            rows = self.conn.execute(
                """
                SELECT cloned_role_id
                FROM role_mentions
                WHERE original_guild_id = ?
                AND cloned_guild_id = ?
                AND (cloned_channel_id IS NULL OR cloned_channel_id = ?)
                ORDER BY 
                    CASE WHEN cloned_channel_id IS NULL THEN 0 ELSE 1 END,
                    added_at ASC
                """,
                (int(original_guild_id), int(cloned_guild_id), int(cloned_channel_id)),
            ).fetchall()

        return [int(r[0]) for r in rows]

    def list_all_role_mentions(
        self,
        original_guild_id: int,
        cloned_guild_id: int,
    ) -> list[dict]:
        """
        List all role mention configurations for a mapping.
        """
        rows = self.conn.execute(
            """
            SELECT role_mention_id, cloned_channel_id, cloned_role_id, added_at
            FROM role_mentions
            WHERE original_guild_id = ?
            AND cloned_guild_id = ?
            ORDER BY 
                CASE WHEN cloned_channel_id IS NULL THEN 0 ELSE 1 END,
                cloned_channel_id,
                added_at ASC
            """,
            (int(original_guild_id), int(cloned_guild_id)),
        ).fetchall()

        return [
            {
                "role_mention_id": r[0],
                "cloned_channel_id": r[1],
                "cloned_role_id": int(r[2]),
                "added_at": r[3],
            }
            for r in rows
        ]

    def set_channel_webhook_profile(
        self,
        cloned_channel_id: int,
        cloned_guild_id: int,
        webhook_name: str,
        webhook_avatar_url: str | None = None,
    ) -> None:
        """
        Set custom webhook name and avatar for a cloned channel.
        All messages sent to this channel will use this identity.
        """
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO channel_webhook_profiles
                (cloned_channel_id, cloned_guild_id, webhook_name, webhook_avatar_url, last_updated)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(cloned_channel_id, cloned_guild_id) DO UPDATE SET
                    webhook_name = excluded.webhook_name,
                    webhook_avatar_url = excluded.webhook_avatar_url,
                    last_updated = CURRENT_TIMESTAMP
                """,
                (
                    int(cloned_channel_id),
                    int(cloned_guild_id),
                    webhook_name,
                    webhook_avatar_url,
                ),
            )

    def get_channel_webhook_profile(
        self,
        cloned_channel_id: int,
        cloned_guild_id: int,
    ) -> dict | None:
        """
        Get custom webhook profile for a cloned channel.
        Returns dict with 'webhook_name' and 'webhook_avatar_url', or None.
        """
        row = self.conn.execute(
            """
            SELECT webhook_name, webhook_avatar_url, created_at, last_updated
            FROM channel_webhook_profiles
            WHERE cloned_channel_id = ?
            AND cloned_guild_id = ?
            LIMIT 1
            """,
            (int(cloned_channel_id), int(cloned_guild_id)),
        ).fetchone()

        if not row:
            return None

        return {
            "webhook_name": row["webhook_name"],
            "webhook_avatar_url": row["webhook_avatar_url"],
            "created_at": row["created_at"],
            "last_updated": row["last_updated"],
        }

    def delete_channel_webhook_profile(
        self,
        cloned_channel_id: int,
        cloned_guild_id: int,
    ) -> bool:
        """
        Delete custom webhook profile for a cloned channel.
        Returns True if a row was deleted.
        """
        with self.lock, self.conn:
            cur = self.conn.execute(
                """
                DELETE FROM channel_webhook_profiles
                WHERE cloned_channel_id = ?
                AND cloned_guild_id = ?
                """,
                (int(cloned_channel_id), int(cloned_guild_id)),
            )
            return cur.rowcount > 0

    def list_channel_webhook_profiles_for_guild(
        self,
        cloned_guild_id: int,
    ) -> list[dict]:
        """
        List all channel webhook profiles for a clone guild.
        """
        rows = self.conn.execute(
            """
            SELECT 
                cloned_channel_id,
                cloned_guild_id,
                webhook_name,
                webhook_avatar_url,
                created_at,
                last_updated
            FROM channel_webhook_profiles
            WHERE cloned_guild_id = ?
            ORDER BY last_updated DESC
            """,
            (int(cloned_guild_id),),
        ).fetchall()

        return [
            {
                "cloned_channel_id": int(r["cloned_channel_id"]),
                "cloned_guild_id": int(r["cloned_guild_id"]),
                "webhook_name": r["webhook_name"],
                "webhook_avatar_url": r["webhook_avatar_url"],
                "created_at": r["created_at"],
                "last_updated": r["last_updated"],
            }
            for r in rows
        ]

    def update_mapping_status(self, mapping_id: str, status: str) -> None:
        """
        Update the status for a guild mapping.

        Status is normalized to 'active' or 'paused'.
        """
        status_norm = "paused" if str(status).lower() == "paused" else "active"
        with self.lock, self.conn:
            self.conn.execute(
                """
                UPDATE guild_mappings
                SET status = ?,
                    last_updated = CURRENT_TIMESTAMP
                WHERE mapping_id = ?
                """,
                (status_norm, str(mapping_id)),
            )

    def clear_mapping_pair_state(
        self,
        original_guild_id: int,
        cloned_guild_id: int,
    ) -> None:
        """
        Wipe all per-(original_guild_id, cloned_guild_id) state across
        the DB, but keep the guild_mappings row itself.

        Used when repointing an existing mapping to a new clone guild
        so we don't leave stale rows for the old pair.
        """
        ogid = int(original_guild_id or 0)
        cgid = int(cloned_guild_id or 0)
        if not ogid and not cgid:
            return

        tables_to_clean = [
            "messages",
            "filters",
            "blocked_keywords",
            "backfill_runs",
            "role_blocks",
            "threads",
            "channel_mappings",
            "category_mappings",
            "role_mappings",
            "emoji_mappings",
            "sticker_mappings",
        ]

        with self.conn:
            for tbl in tables_to_clean:
                try:
                    if tbl == "role_blocks":
                        self.conn.execute(
                            f"""
                            DELETE FROM {tbl}
                            WHERE cloned_guild_id = ?
                            """,
                            (cgid,),
                        )
                    else:
                        self.conn.execute(
                            f"""
                            DELETE FROM {tbl}
                            WHERE original_guild_id = ?
                            AND cloned_guild_id   = ?
                            """,
                            (ogid, cgid),
                        )
                except sqlite3.OperationalError:
                    # Older DBs might not have all tables yet; ignore.
                    pass

    def cleanup_stale_mapping_pairs(self) -> dict[str, int]:
        """
        At boot: scan for any per-(original_guild_id, cloned_guild_id) state
        that no longer has a row in guild_mappings and wipe it using
        clear_mapping_pair_state().

        Returns a small stats dict for logging.
        """
        import sqlite3

        with self.lock:
            # 1) Build the set of valid mapping pairs from guild_mappings
            try:
                rows = self.conn.execute(
                    """
                    SELECT DISTINCT original_guild_id, cloned_guild_id
                    FROM guild_mappings
                    WHERE original_guild_id IS NOT NULL
                      AND cloned_guild_id IS NOT NULL
                      AND original_guild_id != 0
                      AND cloned_guild_id != 0
                    """
                ).fetchall()
            except sqlite3.OperationalError:
                # No guild_mappings table yet (very old DB?) -> nothing we can safely clean.
                return {"pairs_cleared": 0, "role_blocks_only": 0}

            valid_pairs: set[tuple[int, int]] = set()
            valid_clone_ids: set[int] = set()
            for r in rows or []:
                ogid = int(r["original_guild_id"] or 0)
                cgid = int(r["cloned_guild_id"] or 0)
                if not ogid or not cgid:
                    continue
                valid_pairs.add((ogid, cgid))
                valid_clone_ids.add(cgid)

            # 2) Collect all (host, clone) pairs that appear in the per-mapping tables
            tables_to_scan_for_pairs = [
                "messages",
                "filters",
                "blocked_keywords",
                "backfill_runs",
                "threads",
                "channel_mappings",
                "category_mappings",
                "role_mappings",
                "emoji_mappings",
                "sticker_mappings",
            ]

            found_pairs: set[tuple[int, int]] = set()

            for tbl in tables_to_scan_for_pairs:
                try:
                    rows = self.conn.execute(
                        f"""
                        SELECT DISTINCT original_guild_id, cloned_guild_id
                        FROM {tbl}
                        WHERE original_guild_id IS NOT NULL
                          AND cloned_guild_id IS NOT NULL
                          AND original_guild_id != 0
                          AND cloned_guild_id != 0
                        """
                    ).fetchall()
                except sqlite3.OperationalError:
                    # Table may not exist yet on older DBs; just skip.
                    continue

                for r in rows or []:
                    ogid = int(r["original_guild_id"] or 0)
                    cgid = int(r["cloned_guild_id"] or 0)
                    if not ogid or not cgid:
                        continue
                    found_pairs.add((ogid, cgid))

            # Pairs that show up in those tables but *not* in guild_mappings
            stale_pairs = {p for p in found_pairs if p not in valid_pairs}

            # 3) role_blocks is keyed only by cloned_guild_id, so clean any clones
            # that are no longer present in *any* mapping.
            stale_role_block_clones: set[int] = set()
            try:
                rb_rows = self.conn.execute(
                    """
                    SELECT DISTINCT cloned_guild_id
                    FROM role_blocks
                    WHERE cloned_guild_id IS NOT NULL
                      AND cloned_guild_id != 0
                    """
                ).fetchall()
            except sqlite3.OperationalError:
                rb_rows = []

            for r in rb_rows or []:
                cgid = int(r["cloned_guild_id"] or 0)
                if not cgid:
                    continue
                if cgid not in valid_clone_ids:
                    stale_role_block_clones.add(cgid)

            # 4) Actually wipe stale state using the helper
            for ogid, cgid in stale_pairs:
                self.clear_mapping_pair_state(ogid, cgid)

            for cgid in stale_role_block_clones:
                # ogid=0 -> clear_mapping_pair_state only affects role_blocks.
                self.clear_mapping_pair_state(0, cgid)

        return {
            "pairs_cleared": len(stale_pairs),
            "role_blocks_only": len(stale_role_block_clones),
        }
