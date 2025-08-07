import sqlite3
from typing import List, Optional


class DBManager:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        c = self.conn.cursor()

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS category_mappings (
          original_category_id   INTEGER PRIMARY KEY,
          original_category_name TEXT    NOT NULL,
          cloned_category_id     INTEGER,
          cloned_category_name   TEXT
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS channel_mappings (
          original_channel_id           INTEGER PRIMARY KEY,
          original_channel_name         TEXT    NOT NULL,
          cloned_channel_id             INTEGER UNIQUE,
          channel_webhook_url           TEXT,
          original_parent_category_id   INTEGER,
          cloned_parent_category_id     INTEGER,
          FOREIGN KEY(original_parent_category_id)
            REFERENCES category_mappings(original_category_id),
          FOREIGN KEY(cloned_parent_category_id)
            REFERENCES category_mappings(cloned_category_id)
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS threads (
          original_thread_id     INTEGER PRIMARY KEY,
          original_thread_name   TEXT    NOT NULL,
          cloned_thread_id       INTEGER,
          forum_original_id      INTEGER NOT NULL,
          forum_cloned_id        INTEGER,
          FOREIGN KEY(forum_original_id)
            REFERENCES channel_mappings(original_channel_id),
          FOREIGN KEY(forum_cloned_id)
            REFERENCES channel_mappings(cloned_channel_id)
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS emoji_mappings (
          original_emoji_id   INTEGER PRIMARY KEY,
          original_emoji_name TEXT    NOT NULL,
          cloned_emoji_id     INTEGER UNIQUE,
          cloned_emoji_name   TEXT    NOT NULL
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS settings (
          id                INTEGER PRIMARY KEY CHECK (id = 1),
          blocked_keywords  TEXT    NOT NULL DEFAULT ''
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS announcement_subscriptions (
          keyword   TEXT    NOT NULL,
          user_id   INTEGER NOT NULL,
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
          PRIMARY KEY(keyword, filter_user_id, channel_id)
        );
        """
        )

        cols = [r[1] for r in c.execute("PRAGMA table_info(settings)").fetchall()]
        if "version" not in cols:
            c.execute(
                "ALTER TABLE settings ADD COLUMN version TEXT NOT NULL DEFAULT ''"
            )
        if "notified_version" not in cols:
            c.execute(
                "ALTER TABLE settings ADD COLUMN notified_version TEXT NOT NULL DEFAULT ''"
            )

        c.execute(
            "INSERT OR IGNORE INTO settings (id, blocked_keywords) VALUES (1, '')"
        )

        # Now for each table we want to track:
        tables = [
            ("category_mappings", "original_category_id"),
            ("channel_mappings", "original_channel_id"),
            ("threads", "original_thread_id"),
            ("emoji_mappings", "original_emoji_id"),
            ("settings", "id"),
            ("announcement_subscriptions", "keyword"),
            ("announcement_triggers", "keyword"),
        ]

        for table, pk in tables:
            cols = [row[1] for row in c.execute(f"PRAGMA table_info({table})")]
            if "last_updated" not in cols:
                c.execute(f"ALTER TABLE {table} ADD COLUMN last_updated DATETIME")

                c.execute(f"UPDATE {table} SET last_updated = CURRENT_TIMESTAMP")

            insert_trig = f"trg_{table}_stamp_insert"
            if not c.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name=?",
                (insert_trig,),
            ).fetchone():
                c.execute(
                    f"""
                CREATE TRIGGER {insert_trig}
                  AFTER INSERT ON {table}
                  FOR EACH ROW
                BEGIN
                  UPDATE {table}
                    SET last_updated = CURRENT_TIMESTAMP
                  WHERE {pk} = NEW.{pk};
                END;
                """
                )

            update_trig = f"trg_{table}_stamp_update"
            if not c.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name=?",
                (update_trig,),
            ).fetchone():
                c.execute(
                    f"""
                CREATE TRIGGER {update_trig}
                  AFTER UPDATE ON {table}
                  FOR EACH ROW
                BEGIN
                  UPDATE {table}
                    SET last_updated = CURRENT_TIMESTAMP
                  WHERE {pk} = OLD.{pk};
                END;
                """
                )

        self.conn.commit()

    def get_version(self) -> str:
        row = self.conn.execute("SELECT version FROM settings WHERE id = 1").fetchone()
        return row[0] if row else ""

    def set_version(self, version: str):
        self.conn.execute("UPDATE settings SET version = ? WHERE id = 1", (version,))
        self.conn.commit()

    def get_notified_version(self) -> str:
        row = self.conn.execute(
            "SELECT notified_version FROM settings WHERE id = 1"
        ).fetchone()
        return row[0] if row else ""

    def set_notified_version(self, version: str):
        self.conn.execute(
            "UPDATE settings SET notified_version = ? WHERE id = 1", (version,)
        )
        self.conn.commit()

    def get_all_category_mappings(self) -> List[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM category_mappings").fetchall()

    def upsert_category_mapping(
        self,
        orig_id: int,
        orig_name: str,
        clone_id: Optional[int],
        clone_name: Optional[str],
    ):
        self.conn.execute(
            """INSERT OR REPLACE INTO category_mappings
               (original_category_id, original_category_name, cloned_category_id, cloned_category_name)
               VALUES (?, ?, ?, ?)""",
            (orig_id, orig_name, clone_id, clone_name),
        )
        self.conn.commit()

    def delete_category_mapping(self, orig_id: int):
        self.conn.execute(
            "DELETE FROM category_mappings WHERE original_category_id = ?", (orig_id,)
        )
        self.conn.commit()

    def count_categories(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM category_mappings").fetchone()[0]

    def get_all_channel_mappings(self) -> List[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM channel_mappings").fetchall()

    def get_all_threads(self) -> List[sqlite3.Row]:
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
            """INSERT OR REPLACE INTO threads
               (original_thread_id, original_thread_name,
                cloned_thread_id, forum_original_id,
                forum_cloned_id)
               VALUES (?, ?, ?, ?, ?)""",
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
        self.conn.execute(
            "DELETE FROM threads WHERE original_thread_id = ?",
            (orig_thread_id,),
        )
        self.conn.commit()

    def upsert_channel_mapping(
        self,
        orig_id: int,
        orig_name: str,
        clone_id: Optional[int],
        webhook_url: Optional[str],
        orig_cat_id: Optional[int],
        clone_cat_id: Optional[int],
    ):
        self.conn.execute(
            """INSERT OR REPLACE INTO channel_mappings
               (original_channel_id, original_channel_name,
                cloned_channel_id, channel_webhook_url,
                original_parent_category_id, cloned_parent_category_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (orig_id, orig_name, clone_id, webhook_url, orig_cat_id, clone_cat_id),
        )
        self.conn.commit()

    def delete_channel_mapping(self, orig_id: int):
        self.conn.execute(
            "DELETE FROM channel_mappings WHERE original_channel_id = ?", (orig_id,)
        )
        self.conn.commit()

    def count_channels(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM channel_mappings").fetchone()[0]

    def get_blocked_keywords(self) -> list[str]:
        """Fetches the list of blocked keywords from settings."""
        cur = self.conn.execute("SELECT blocked_keywords FROM settings WHERE id = 1")
        row = cur.fetchone()
        if not row or not row[0].strip():
            return []
        return [kw.strip() for kw in row[0].split(",") if kw.strip()]

    def add_blocked_keyword(self, keyword: str) -> bool:
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
        return self.conn.execute("SELECT * FROM emoji_mappings").fetchall()

    def upsert_emoji_mapping(
        self, orig_id: int, orig_name: str, clone_id: int, clone_name: str
    ):
        self.conn.execute(
            """INSERT OR REPLACE INTO emoji_mappings
               (original_emoji_id, original_emoji_name,
                cloned_emoji_id, cloned_emoji_name)
               VALUES (?, ?, ?, ?)""",
            (orig_id, orig_name, clone_id, clone_name),
        )
        self.conn.commit()

    def delete_emoji_mapping(self, orig_id: int):
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
        cur = self.conn.execute(
            "DELETE FROM announcement_triggers "
            "WHERE keyword = ? AND filter_user_id = ? AND channel_id = ?",
            (keyword, filter_user_id, channel_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_announcement_triggers(self) -> dict[str, list[tuple[int, int]]]:
        """
        Returns a mapping:
          keyword → list of (filter_user_id, channel_id) tuples.
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
