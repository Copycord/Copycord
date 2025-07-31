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
            )
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
            )
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
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings  (
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                blocked_keywords TEXT    NOT NULL DEFAULT ''
            );
        """)
        cols = [r[1] for r in c.execute(
            "PRAGMA table_info(settings)"
        ).fetchall()]
        if "version" not in cols:
            c.execute("ALTER TABLE settings ADD COLUMN version TEXT NOT NULL DEFAULT ''")
        if "notified_version" not in cols:
            c.execute("ALTER TABLE settings ADD COLUMN notified_version TEXT NOT NULL DEFAULT ''")
        
        c.execute(
            "INSERT OR IGNORE INTO settings (id, blocked_keywords) VALUES (1, '')"
        )
        self.conn.commit()
        
    def get_version(self) -> str:
        row = self.conn.execute("SELECT version FROM settings WHERE id = 1").fetchone()
        return row[0] if row else ""

    def set_version(self, version: str):
        self.conn.execute("UPDATE settings SET version = ? WHERE id = 1", (version,))
        self.conn.commit()
        
    def get_notified_version(self) -> str:
        row = self.conn.execute("SELECT notified_version FROM settings WHERE id = 1").fetchone()
        return row[0] if row else ""

    def set_notified_version(self, version: str):
        self.conn.execute("UPDATE settings SET notified_version = ? WHERE id = 1", (version,))
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
        cur = self.conn.execute(
            "SELECT blocked_keywords FROM settings WHERE id = 1"
        )
        row = cur.fetchone()
        if not row or not row[0].strip():
            return []
        return [kw.strip() for kw in row[0].split(",") if kw.strip()]

    def add_blocked_keyword(self, keyword: str) -> bool:
        kws = self.get_blocked_keywords()
        k   = keyword.lower().strip()
        if k in kws:
            return False
        kws.append(k)
        csv = ",".join(kws)

        cur = self.conn.execute(
            "UPDATE settings SET blocked_keywords = ? WHERE id = 1",
            (csv,)
        )
        if cur.rowcount == 0:
            self.conn.execute(
                "INSERT INTO settings (id, blocked_keywords) VALUES (1, ?)",
                (csv,)
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

    def upsert_emoji_mapping(self,
                             orig_id: int,
                             orig_name: str,
                             clone_id: int,
                             clone_name: str):
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
            "SELECT * FROM emoji_mappings WHERE original_emoji_id = ?",
            (original_id,)
        ).fetchone()