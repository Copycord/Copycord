# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
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

        c.execute("""
        CREATE TABLE IF NOT EXISTS app_config(
        key           TEXT PRIMARY KEY,
        value         TEXT NOT NULL DEFAULT '',
        last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS filters (
        kind          TEXT NOT NULL CHECK(kind IN ('whitelist','exclude')),
        scope         TEXT NOT NULL CHECK(scope IN ('category','channel')),
        obj_id        INTEGER NOT NULL,
        last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(kind, scope, obj_id)
        );
        """)

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
        c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_category_mappings_cloned_id
        ON category_mappings(cloned_category_id);
        """)

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

    
        c.execute("""
        CREATE INDEX IF NOT EXISTS ix_channel_parent_orig
        ON channel_mappings(original_parent_category_id);
        """)
        c.execute("""
        CREATE INDEX IF NOT EXISTS ix_channel_parent_clone
        ON channel_mappings(cloned_parent_category_id);
        """)
        
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
            """
        CREATE TABLE IF NOT EXISTS announcement_subscriptions (
          keyword   TEXT    NOT NULL,
          user_id   INTEGER NOT NULL,
          last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(keyword, user_id)
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS announcement_triggers (
          keyword        TEXT    NOT NULL,
          filter_user_id INTEGER NOT NULL,
          channel_id     INTEGER NOT NULL DEFAULT 0,
          last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(keyword, filter_user_id, channel_id)
        );
        """
        )
        
        c.execute("""
        CREATE TABLE IF NOT EXISTS join_dm_subscriptions (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            last_updated  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (guild_id, user_id)
        );
        """)
        
        c.execute("""
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
        """)
        self.conn.commit()

        
    def set_config(self, key: str, value: str) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO app_config(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def get_config(self, key: str, default: str = "") -> str:
        row = self.conn.execute("SELECT value FROM app_config WHERE key=?",(key,)).fetchone()
        return row["value"] if row else default

    def get_all_config(self) -> dict[str, str]:
        return {r["key"]: r["value"] for r in self.conn.execute("SELECT key, value FROM app_config")}

    def get_version(self) -> str:
        """
        Retrieves the version information from the settings table in the database.
        """
        row = self.conn.execute("SELECT version FROM settings WHERE id = 1").fetchone()
        return row[0] if row else ""

    def set_version(self, version: str):
        """
        Updates the version in the settings table of the database.
        """
        self.conn.execute("UPDATE settings SET version = ? WHERE id = 1", (version,))
        self.conn.commit()

    def get_notified_version(self) -> str:
        """
        Retrieves the notified version from the settings table in the database.
        """
        row = self.conn.execute(
            "SELECT notified_version FROM settings WHERE id = 1"
        ).fetchone()
        return row[0] if row else ""

    def set_notified_version(self, version: str):
        """
        Updates the notified_version field in the settings table to the specified version.
        """
        self.conn.execute(
            "UPDATE settings SET notified_version = ? WHERE id = 1", (version,)
        )
        self.conn.commit()

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
            old_clone = (row["cloned_category_id"] if row else None)

            will_change_to_new = (row is not None and clone_id is not None and old_clone != clone_id)
            if will_change_to_new and old_clone is not None:
                self.conn.execute(
                    "UPDATE channel_mappings SET cloned_parent_category_id=NULL "
                    "WHERE cloned_parent_category_id=?",
                    (old_clone,),
                )

            clearing_parent = (row is not None and old_clone is not None and clone_id is None)
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
    
    def get_channel_mapping_by_clone_id(self, cloned_channel_id: int) -> Optional[sqlite3.Row]:
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
    
    def get_channel_mapping_by_original_id(self, original_channel_id: int) -> Optional[sqlite3.Row]:
        """
        Look up a single channel mapping by the original (source) channel id.
        """
        return self.conn.execute(
            "SELECT * FROM channel_mappings WHERE original_channel_id = ? LIMIT 1",
            (original_channel_id,),
        ).fetchone()

    def resolve_original_from_any_id(self, any_channel_id: int) -> tuple[Optional[int], Optional[int], str]:
        """
        Accept either a cloned id or an original id.

        Returns:
            (original_id, cloned_id_or_none, source)
            where source is 'from_clone' | 'from_original' | 'assumed_original'
        """
        row = self.get_channel_mapping_by_clone_id(any_channel_id)
        if row:
            return int(row["original_channel_id"]), int(row["cloned_channel_id"]), "from_clone"

        row = self.get_channel_mapping_by_original_id(any_channel_id)
        if row:
            # mapping exists and confirms it's original
            cloned = row["cloned_channel_id"]
            return int(row["original_channel_id"]), (int(cloned) if cloned is not None else None), "from_original"

        # Fallback: we weren't able to find a mapping entry; treat the input as already-original.
        return int(any_channel_id), None, "assumed_original"

    def get_all_threads(self) -> List[sqlite3.Row]:
        """
        Retrieves all rows from the 'threads' table in the database.
        """
        return self.conn.execute("SELECT * FROM threads").fetchall()

    def upsert_forum_thread_mapping(self, orig_thread_id: int, orig_thread_name: str,
                                    clone_thread_id: Optional[int],
                                    forum_orig_id: int, forum_clone_id: Optional[int]):
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
            (orig_thread_id, orig_thread_name, clone_thread_id, forum_orig_id, forum_clone_id),
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
                int(original_parent_category_id) if original_parent_category_id else None,
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

    def upsert_emoji_mapping(self, orig_id: int, orig_name: str,
                            clone_id: int, clone_name: str):
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

    def add_announcement_user(self, keyword: str, user_id: int) -> bool:
        """Returns True if newly added, False if already present."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO announcement_subscriptions(keyword, user_id) VALUES (?, ?)",
            (keyword, user_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def remove_announcement_user(self, keyword: str, user_id: int) -> bool:
        """Returns True if removed, False if none existed."""
        cur = self.conn.execute(
            "DELETE FROM announcement_subscriptions WHERE keyword = ? AND user_id = ?",
            (keyword, user_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_announcement_users(self, keyword: str) -> list[int]:
        """
        Returns all user IDs who subscribed to this keyword
        OR who subscribed globally (keyword='*').
        """
        rows = self.conn.execute(
            "SELECT user_id FROM announcement_subscriptions "
            "WHERE keyword = ? OR keyword = '*'",
            (keyword,),
        ).fetchall()
        return [r["user_id"] for r in rows]

    def get_announcement_keywords(self) -> list[str]:
        """Distinct list of all keywords with at least one subscriber."""
        rows = self.conn.execute(
            "SELECT DISTINCT keyword FROM announcement_subscriptions"
        ).fetchall()
        return [r["keyword"] for r in rows]

    def add_announcement_trigger(
        self,
        keyword: str,
        filter_user_id: int = 0,
        channel_id: int = 0,
    ) -> bool:
        """
        Adds a new announcement trigger to the database.
        """
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO announcement_triggers(keyword, filter_user_id, channel_id) "
            "VALUES (?, ?, ?)",
            (keyword, filter_user_id, channel_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def remove_announcement_trigger(
        self,
        keyword: str,
        filter_user_id: int = 0,
        channel_id: int = 0,
    ) -> bool:
        """
        Removes an announcement trigger from the database.
        """
        cur = self.conn.execute(
            "DELETE FROM announcement_triggers "
            "WHERE keyword = ? AND filter_user_id = ? AND channel_id = ?",
            (keyword, filter_user_id, channel_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_announcement_triggers(self) -> dict[str, list[tuple[int, int]]]:
        """
        Retrieves announcement triggers from the database.
        """
        rows = self.conn.execute(
            "SELECT keyword, filter_user_id, channel_id FROM announcement_triggers"
        ).fetchall()
        d: dict[str, list[tuple[int, int]]] = {}
        for r in rows:
            d.setdefault(r["keyword"], []).append(
                (r["filter_user_id"], r["channel_id"])
            )
        return d

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

    def upsert_sticker_mapping(self, orig_id: int, orig_name: str,
                            clone_id: int, clone_name: str):
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

    def upsert_role_mapping(self, orig_id: int, orig_name: str,
                            clone_id: Optional[int], clone_name: Optional[str]):
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
        self.conn.execute("DELETE FROM role_mappings WHERE original_role_id = ?", (orig_id,))
        self.conn.commit()
        
    def get_role_mapping(self, orig_id: int):
        return self.conn.execute(
            "SELECT * FROM role_mappings WHERE original_role_id = ?",
            (orig_id,)
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
            "exclude":   {"category": set(), "channel": set()},
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
        ins("whitelist", "channel",  whitelist_channels)
        ins("exclude",   "category", exclude_categories)
        ins("exclude",   "channel",  exclude_channels)
        self.conn.commit()
        

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
        return self.conn.execute("SELECT * FROM guilds WHERE guild_id = ?", (int(guild_id),)).fetchone()

    def get_all_guilds(self) -> list[dict]:
        """
        Returns all guilds as a list of dicts with keys:
        guild_id, name, icon_url, owner_id, member_count, description, last_seen, last_updated
        """
        with self.lock:
            cur = self.conn.execute("""
                SELECT guild_id, name, icon_url, owner_id, member_count, description, last_seen, last_updated
                FROM guilds
                ORDER BY LOWER(name) ASC
            """)
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

    def set_channel_clone_name(self, original_channel_id: int, clone_name: str | None) -> None:
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

    def set_category_clone_name(self, original_category_id: int, clone_name: str | None) -> None:
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