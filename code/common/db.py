# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import sqlite3, threading
from typing import List, Optional


class DBManager:
    def __init__(self, db_path: str):
        self.path = db_path
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        self.conn.execute("PRAGMA foreign_keys = ON;")

        self.conn.execute("PRAGMA wal_checkpoint(FULL);")
        self.conn.execute("PRAGMA journal_mode = DELETE;")

        self.conn.execute("PRAGMA synchronous = FULL;")
        self.conn.execute("PRAGMA busy_timeout = 5000;")
        self.lock = threading.RLock()
        self._init_schema()

    def _init_schema(self):
        """
        Initializes the database schema by creating necessary tables, adding columns if they
        do not exist, and setting up triggers for automatic timestamp updates.
        """
        c = self.conn.cursor()

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS app_config(
        key           TEXT PRIMARY KEY,
        value         TEXT NOT NULL DEFAULT '',
        last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS filters (
        kind          TEXT NOT NULL CHECK(kind IN ('whitelist','exclude')),
        scope         TEXT NOT NULL CHECK(scope IN ('category','channel')),
        obj_id        INTEGER NOT NULL,
        last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(kind, scope, obj_id)
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS category_mappings (
          original_category_id   INTEGER PRIMARY KEY,
          original_category_name TEXT    NOT NULL,
          cloned_category_id     INTEGER,
          cloned_category_name   TEXT,
          last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )
        c.execute(
            """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_category_mappings_cloned_id
        ON category_mappings(cloned_category_id);
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS channel_mappings (
        original_channel_id           INTEGER PRIMARY KEY,
        original_channel_name         TEXT    NOT NULL,
        cloned_channel_id             INTEGER UNIQUE,
        clone_channel_name            TEXT,
        channel_webhook_url           TEXT,
        original_parent_category_id   INTEGER,
        cloned_parent_category_id     INTEGER,
        channel_type                  INTEGER NOT NULL DEFAULT 0,
        last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(original_parent_category_id)
            REFERENCES category_mappings(original_category_id) ON DELETE SET NULL,
        FOREIGN KEY(cloned_parent_category_id)
            REFERENCES category_mappings(cloned_category_id)   ON DELETE SET NULL
        );
        """
        )

        c.execute(
            """
        CREATE INDEX IF NOT EXISTS ix_channel_parent_orig
        ON channel_mappings(original_parent_category_id);
        """
        )
        c.execute(
            """
        CREATE INDEX IF NOT EXISTS ix_channel_parent_clone
        ON channel_mappings(cloned_parent_category_id);
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS threads (
        original_thread_id   INTEGER PRIMARY KEY,
        original_thread_name TEXT    NOT NULL,
        cloned_thread_id     INTEGER,
        forum_original_id    INTEGER,
        forum_cloned_id      INTEGER,
        last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(forum_original_id)
            REFERENCES channel_mappings(original_channel_id) ON DELETE SET NULL,
        FOREIGN KEY(forum_cloned_id)
            REFERENCES channel_mappings(cloned_channel_id)   ON DELETE SET NULL
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS emoji_mappings (
          original_emoji_id   INTEGER PRIMARY KEY,
          original_emoji_name TEXT    NOT NULL,
          cloned_emoji_id     INTEGER UNIQUE,
          cloned_emoji_name   TEXT    NOT NULL,
          last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS sticker_mappings (
        original_sticker_id   INTEGER PRIMARY KEY,
        original_sticker_name TEXT    NOT NULL,
        cloned_sticker_id     INTEGER UNIQUE,
        cloned_sticker_name   TEXT    NOT NULL,
        last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS role_mappings (
            original_role_id   INTEGER PRIMARY KEY,
            original_role_name TEXT    NOT NULL,
            cloned_role_id     INTEGER UNIQUE,
            cloned_role_name   TEXT    NOT NULL,
            last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS settings (
          id                INTEGER PRIMARY KEY CHECK (id = 1),
          blocked_keywords  TEXT    NOT NULL DEFAULT '',
          version TEXT NOT NULL DEFAULT '',
          notified_version TEXT NOT NULL DEFAULT '',
          last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )

        c.execute(
            "INSERT OR IGNORE INTO settings (id, blocked_keywords, version, notified_version) VALUES (1, '', '', '')"
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
            # For existing installs without guild_id, migrate rows and set guild_id=0
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

        # announcement_triggers (guild-scoped)
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

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS role_blocks (
        original_role_id INTEGER PRIMARY KEY,
        added_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )
        self.conn.commit()
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            original_guild_id    INTEGER NOT NULL,
            original_channel_id  INTEGER NOT NULL,
            original_message_id  INTEGER PRIMARY KEY,
            cloned_channel_id    INTEGER,
            cloned_message_id    INTEGER,           
            webhook_url          TEXT,     
            created_at           INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            updated_at           INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_orig_chan ON messages(original_channel_id);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_clone_msg ON messages(cloned_message_id);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);")

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
    ):
        """
        Create or rebuild table `name` to match the target schema.

        - If table missing -> CREATE and run post_sql.
        - If table exists and has all required columns -> run post_sql and return.
        - Else rebuild with a transaction or savepoint (depending on whether we're already
        inside a transaction), temporarily disabling FKs during the swap.
        """
        post_sql = post_sql or []
        exists = self._table_exists(name)

        if not exists:
            # Fresh create
            self.conn.execute(create_sql_template.format(table=name))
            for stmt in post_sql:
                self.conn.execute(stmt)
            return

        existing_cols = self._table_columns(name)
        if required_columns.issubset(existing_cols):
            # Already matches target; just ensure indexes
            for stmt in post_sql:
                self.conn.execute(stmt)
            return

        # ---- Rebuild path ----
        temp = f"_{name}_new"

        # Capture current FK setting to restore later
        prev_fk = self.conn.execute("PRAGMA foreign_keys").fetchone()[0]
        self.conn.execute("PRAGMA foreign_keys = OFF;")

        # Choose txn primitive: top-level BEGIN or nested SAVEPOINT
        in_txn = self.conn.in_transaction
        sp_name = f"sp_rebuild_{name}"
        try:
            if in_txn:
                self.conn.execute(f"SAVEPOINT {sp_name};")
            else:
                self.conn.execute("BEGIN;")

            # 1) create new table
            self.conn.execute(create_sql_template.format(table=temp))

            # 2) copy old -> new with defensive fallbacks for missing legacy columns
            new_cols = list(copy_map.keys())
            select_exprs = []
            for new_col in new_cols:
                expr = copy_map[new_col].strip()
                # If expression is a bare identifier that isn't in the old table, fallback.
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

            # 3) swap tables
            self.conn.execute(f"DROP TABLE {name};")
            self.conn.execute(f"ALTER TABLE {temp} RENAME TO {name};")

            # 4) recreate indexes
            for stmt in post_sql:
                self.conn.execute(stmt)

            # Commit appropriately
            if in_txn:
                self.conn.execute(f"RELEASE SAVEPOINT {sp_name};")
            else:
                self.conn.execute("COMMIT;")
        except Exception:
            if in_txn:
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name};")
                self.conn.execute(
                    f"RELEASE SAVEPOINT {sp_name};"
                )  # end the savepoint scope
            else:
                self.conn.execute("ROLLBACK;")
            raise
        finally:
            # Restore FK pragma to its previous value
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
    ):
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT cloned_category_id FROM category_mappings WHERE original_category_id=?",
                (orig_id,),
            ).fetchone()
            old_clone = row["cloned_category_id"] if row else None

            will_change_to_new = (
                row is not None and clone_id is not None and old_clone != clone_id
            )
            if will_change_to_new and old_clone is not None:
                self.conn.execute(
                    "UPDATE channel_mappings SET cloned_parent_category_id=NULL "
                    "WHERE cloned_parent_category_id=?",
                    (old_clone,),
                )

            clearing_parent = (
                row is not None and old_clone is not None and clone_id is None
            )
            if clearing_parent:
                self.conn.execute(
                    "UPDATE channel_mappings SET cloned_parent_category_id=NULL "
                    "WHERE cloned_parent_category_id=?",
                    (old_clone,),
                )

            self.conn.execute(
                """
                INSERT INTO category_mappings
                    (original_category_id, original_category_name, cloned_category_id, cloned_category_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(original_category_id) DO UPDATE SET
                    original_category_name = excluded.original_category_name,
                    cloned_category_id     = excluded.cloned_category_id,
                    cloned_category_name   = CASE
                        WHEN excluded.cloned_category_id IS NULL THEN NULL
                        WHEN excluded.cloned_category_name IS NOT NULL THEN excluded.cloned_category_name
                        ELSE category_mappings.cloned_category_name
                    END
                """,
                (orig_id, orig_name, clone_id, clone_name),
            )

            if clone_id is not None:
                self.conn.execute(
                    "UPDATE channel_mappings SET cloned_parent_category_id=? "
                    "WHERE original_parent_category_id=?",
                    (clone_id, orig_id),
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
            # mapping exists and confirms it's original
            cloned = row["cloned_channel_id"]
            return (
                int(row["original_channel_id"]),
                (int(cloned) if cloned is not None else None),
                "from_original",
            )

        # Fallback: we weren't able to find a mapping entry; treat the input as already-original.
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
    ):
        self.conn.execute(
            """INSERT INTO threads
                (original_thread_id, original_thread_name, cloned_thread_id,
                forum_original_id, forum_cloned_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(original_thread_id) DO UPDATE SET
                original_thread_name =excluded.original_thread_name,
                cloned_thread_id     =excluded.cloned_thread_id,
                forum_original_id    =excluded.forum_original_id,
                forum_cloned_id      =excluded.forum_cloned_id
            """,
            (
                orig_thread_id,
                orig_thread_name,
                clone_thread_id,
                forum_orig_id,
                forum_clone_id,
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
                clone_channel_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_channel_id) DO UPDATE SET
            original_channel_name       = excluded.original_channel_name,
            cloned_channel_id           = excluded.cloned_channel_id,
            channel_webhook_url         = excluded.channel_webhook_url,
            original_parent_category_id = excluded.original_parent_category_id,
            cloned_parent_category_id   = excluded.cloned_parent_category_id,
            channel_type                = excluded.channel_type,
            clone_channel_name          = COALESCE(excluded.clone_channel_name,
                                                    channel_mappings.clone_channel_name),
            last_updated                = CURRENT_TIMESTAMP
            """,
            (
                int(original_channel_id),
                original_channel_name,
                int(cloned_channel_id) if cloned_channel_id else None,
                channel_webhook_url,
                (
                    int(original_parent_category_id)
                    if original_parent_category_id
                    else None
                ),
                int(cloned_parent_category_id) if cloned_parent_category_id else None,
                int(channel_type),
                (clone_name.strip() if isinstance(clone_name, str) else None),
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

    def get_blocked_keywords(self) -> list[str]:
        """Fetches the list of blocked keywords from settings."""
        cur = self.conn.execute("SELECT blocked_keywords FROM settings WHERE id = 1")
        row = cur.fetchone()
        if not row or not row[0].strip():
            return []
        return [kw.strip() for kw in row[0].split(",") if kw.strip()]

    def add_blocked_keyword(self, keyword: str) -> bool:
        """
        Adds a keyword to the list of blocked keywords in the database.
        This method retrieves the current list of blocked keywords, checks if the
        provided keyword (case-insensitive) is already in the list, and if not,
        adds it to the list and updates the database.
        """
        kws = self.get_blocked_keywords()
        k = keyword.lower().strip()
        if k in kws:
            return False
        kws.append(k)
        csv = ",".join(kws)

        cur = self.conn.execute(
            "UPDATE settings SET blocked_keywords = ? WHERE id = 1", (csv,)
        )
        if cur.rowcount == 0:
            self.conn.execute(
                "INSERT INTO settings (id, blocked_keywords) VALUES (1, ?)", (csv,)
            )
        self.conn.commit()
        return True

    def remove_blocked_keyword(self, keyword: str) -> bool:
        """Removes a keyword if present; returns True if removed, False otherwise."""
        kws = self.get_blocked_keywords()
        k = keyword.lower().strip()
        if k not in kws:
            return False
        kws.remove(k)
        csv = ",".join(kws)
        self.conn.execute(
            "UPDATE settings SET blocked_keywords = ? WHERE id = 1",
            (csv,),
        )
        self.conn.commit()
        return True

    def get_all_emoji_mappings(self) -> list[sqlite3.Row]:
        """
        Retrieves all emoji mappings from the database.
        """
        return self.conn.execute("SELECT * FROM emoji_mappings").fetchall()

    def upsert_emoji_mapping(
        self, orig_id: int, orig_name: str, clone_id: int, clone_name: str
    ):
        self.conn.execute(
            """INSERT INTO emoji_mappings
                (original_emoji_id, original_emoji_name, cloned_emoji_id, cloned_emoji_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(original_emoji_id) DO UPDATE SET
                original_emoji_name=excluded.original_emoji_name,
                cloned_emoji_id    =excluded.cloned_emoji_id,
                cloned_emoji_name  =excluded.cloned_emoji_name
            """,
            (orig_id, orig_name, clone_id, clone_name),
        )
        self.conn.commit()

    def delete_emoji_mapping(self, orig_id: int):
        """
        Deletes a mapping from the emoji_mappings table in the database based on the given original emoji ID.
        """
        self.conn.execute(
            "DELETE FROM emoji_mappings WHERE original_emoji_id = ?",
            (orig_id,),
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

    def get_effective_announcement_triggers(self, guild_id: int) -> dict[str, list[tuple[int, int]]]:
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
            out.setdefault(r["keyword"], set()).add((int(r["filter_user_id"]), int(r["channel_id"])))
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
        self, orig_id: int, orig_name: str, clone_id: int, clone_name: str
    ):
        self.conn.execute(
            """INSERT INTO sticker_mappings
                (original_sticker_id, original_sticker_name, cloned_sticker_id, cloned_sticker_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(original_sticker_id) DO UPDATE SET
                original_sticker_name=excluded.original_sticker_name,
                cloned_sticker_id    =excluded.cloned_sticker_id,
                cloned_sticker_name  =excluded.cloned_sticker_name
            """,
            (orig_id, orig_name, clone_id, clone_name),
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
        clone_id: Optional[int],
        clone_name: Optional[str],
    ):
        self.conn.execute(
            """INSERT INTO role_mappings
                (original_role_id, original_role_name, cloned_role_id, cloned_role_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(original_role_id) DO UPDATE SET
                original_role_name=excluded.original_role_name,
                cloned_role_id    =excluded.cloned_role_id,
                cloned_role_name  =excluded.cloned_role_name
            """,
            (orig_id, orig_name, clone_id, clone_name),
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

    def get_filters(self) -> dict:
        """
        Returns {
          'whitelist': {'category': set[int], 'channel': set[int]},
          'exclude':   {'category': set[int], 'channel': set[int]}
        }
        """
        out = {
            "whitelist": {"category": set(), "channel": set()},
            "exclude": {"category": set(), "channel": set()},
        }
        for row in self.conn.execute("SELECT kind, scope, obj_id FROM filters"):
            out[row["kind"]][row["scope"]].add(int(row["obj_id"]))
        return out

    def replace_filters(
        self,
        whitelist_categories: list[int],
        whitelist_channels: list[int],
        exclude_categories: list[int],
        exclude_channels: list[int],
    ) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM filters")

        def ins(kind: str, scope: str, ids: list[int]):
            cur.executemany(
                "INSERT OR IGNORE INTO filters(kind,scope,obj_id) VALUES(?,?,?)",
                [(kind, scope, int(i)) for i in ids if str(i).strip()],
            )

        ins("whitelist", "category", whitelist_categories)
        ins("whitelist", "channel", whitelist_channels)
        ins("exclude", "category", exclude_categories)
        ins("exclude", "channel", exclude_channels)
        self.conn.commit()

    def add_filter(self, kind: str, scope: str, obj_id: int) -> None:
        """
        Insert a single filter row (no-op if it already exists).
        kind: 'whitelist' | 'exclude'
        scope: 'category' | 'channel'
        """
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO filters(kind,scope,obj_id) VALUES(?,?,?)",
                (str(kind), str(scope), int(obj_id)),
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

    def get_clone_channel_name(self, original_channel_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT clone_channel_name FROM channel_mappings WHERE original_channel_id = ?",
            (int(original_channel_id),),
        ).fetchone()

        return row[0] if row else None

    def set_channel_clone_name(
        self, original_channel_id: int, clone_name: str | None
    ) -> None:
        """
        Directly set clone_channel_name to a value or NULL (no COALESCE here).
        """
        with self.conn as con:
            con.execute(
                "UPDATE channel_mappings SET clone_channel_name = :name WHERE original_channel_id = :ocid",
                {"name": clone_name, "ocid": int(original_channel_id)},
            )

    def get_original_category_name(self, original_category_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT original_category_name FROM category_mappings WHERE original_category_id = ?",
            (int(original_category_id),),
        ).fetchone()
        return row[0] if row else None

    def get_clone_category_name(self, original_category_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT cloned_category_name FROM category_mappings WHERE original_category_id = ?",
            (int(original_category_id),),
        ).fetchone()
        return row[0] if row else None

    def set_category_clone_name(
        self, original_category_id: int, clone_name: str | None
    ) -> None:
        """
        Directly set cloned_category_name to a value or NULL (no COALESCE here).
        """
        with self.conn as con:
            con.execute(
                "UPDATE category_mappings SET cloned_category_name = :name WHERE original_category_id = :ocid",
                {"name": clone_name, "ocid": int(original_category_id)},
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

    def add_role_block(self, original_role_id: int) -> bool:
        """Block this original role id from being created/updated. Returns True if newly added."""
        with self.lock, self.conn:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO role_blocks(original_role_id) VALUES (?)",
                (int(original_role_id),),
            )
            return cur.rowcount > 0

    def remove_role_block(self, original_role_id: int) -> bool:
        """Remove a block. Returns True if removed."""
        with self.lock, self.conn:
            cur = self.conn.execute(
                "DELETE FROM role_blocks WHERE original_role_id = ?",
                (int(original_role_id),),
            )
            return cur.rowcount > 0

    def is_role_blocked(self, original_role_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM role_blocks WHERE original_role_id = ?",
            (int(original_role_id),),
        ).fetchone()
        return bool(row)

    def get_blocked_role_ids(self) -> list[int]:
        rows = self.conn.execute("SELECT original_role_id FROM role_blocks").fetchall()
        return [int(r[0]) for r in rows]

    def get_role_mapping_by_cloned_id(self, cloned_role_id: int):
        return self.conn.execute(
            "SELECT * FROM role_mappings WHERE cloned_role_id = ?",
            (int(cloned_role_id),),
        ).fetchone()

    def clear_role_blocks(self) -> int:
        """Delete all entries from the role_blocks table. Returns number of rows removed."""
        with self.lock, self.conn:

            cnt_row = self.conn.execute("SELECT COUNT(*) FROM role_blocks").fetchone()
            count = int(cnt_row[0] if cnt_row else 0)
            self.conn.execute("DELETE FROM role_blocks")
            return count

    def upsert_message_mapping(
        self,
        original_guild_id: int,
        original_channel_id: int,
        original_message_id: int,
        cloned_channel_id: int | None,
        cloned_message_id: int | None,
        webhook_url: str | None = None,
    ) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO messages (
                    original_guild_id, original_channel_id, original_message_id,
                    cloned_channel_id, cloned_message_id, webhook_url, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'), strftime('%s','now'))
                ON CONFLICT(original_message_id) DO UPDATE SET
                    original_guild_id   = excluded.original_guild_id,
                    original_channel_id = excluded.original_channel_id,
                    cloned_channel_id   = excluded.cloned_channel_id,
                    cloned_message_id   = excluded.cloned_message_id,
                    webhook_url         = COALESCE(excluded.webhook_url, webhook_url),
                    updated_at          = strftime('%s','now')
                """,
                (
                    int(original_guild_id),
                    int(original_channel_id),
                    int(original_message_id),
                    int(cloned_channel_id) if cloned_channel_id is not None else None,
                    int(cloned_message_id) if cloned_message_id is not None else None,
                    str(webhook_url) if webhook_url else None,
                ),
            )

    def get_mapping_by_original(self, original_message_id: int):
        return self.conn.execute(
            "SELECT * FROM messages WHERE original_message_id = ?",
            (int(original_message_id),),
        ).fetchone()

    def get_mapping_by_cloned(self, cloned_message_id: int):
        return self.conn.execute(
            "SELECT * FROM messages WHERE cloned_message_id = ?",
            (int(cloned_message_id),),
        ).fetchone()

    def delete_message_mapping(self, original_message_id: int) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "DELETE FROM messages WHERE original_message_id = ?",
                (int(original_message_id),),
            )
            
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