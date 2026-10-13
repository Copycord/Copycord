"""Seed the throwaway e2e DB with rows whose API routes need a live Discord account."""

import os
import sys
from pathlib import Path

home = Path(os.environ["COPYCORD_HOME"])
db_path = os.environ.get("DB_PATH") or str(home / "data" / "data.db")

from copycord.common.db import DBManager  # noqa: E402

db = DBManager(db_path, init_schema=True)
if not db.list_guild_mappings():
    db.upsert_guild_mapping(
        mapping_id=None,
        mapping_name="E2E Mapping",
        original_guild_id=111111111111111111,
        original_guild_name="E2E Host",
        original_guild_icon_url=None,
        cloned_guild_id=222222222222222222,
        cloned_guild_name="E2E Clone",
        settings={},
    )
if not db.list_backup_tokens():
    db.add_backup_token("e2e.fake.backup-token-value", "E2E backup")
print("seed_db: mappings", len(db.list_guild_mappings()), "backup tokens", len(db.list_backup_tokens()))
sys.exit(0)
