"""The one-time reset of a stored USER_TOKEN_USE_PROXIES false or null back to on."""

import json
import logging

from copycord.common.db import TOKEN_PROXIES_ON_KEY, DBManager


def _add(db, n, settings):
    return db.upsert_guild_mapping(
        mapping_id=None,
        mapping_name=f"m{n}",
        original_guild_id=100 + n,
        original_guild_name="",
        original_guild_icon_url=None,
        cloned_guild_id=200 + n,
        cloned_guild_name="",
        settings=settings,
    )


def _setting(db, mid):
    row = db.conn.execute("SELECT settings FROM guild_mappings WHERE mapping_id = ?", (mid,)).fetchone()
    return json.loads(row["settings"]).get("USER_TOKEN_USE_PROXIES", "missing")


def _set(db, mid, value):
    row = db.conn.execute("SELECT settings FROM guild_mappings WHERE mapping_id = ?", (mid,)).fetchone()
    settings = json.loads(row["settings"])
    settings["USER_TOKEN_USE_PROXIES"] = value
    with db._tx():
        db.conn.execute(
            "UPDATE guild_mappings SET settings = ? WHERE mapping_id = ?", (json.dumps(settings), mid)
        )


def test_runs_once_and_flips_only_false_and_null(tmp_path, caplog):
    path = str(tmp_path / "data.db")
    db = DBManager(path, init_schema=True)
    # A fresh DB records the run with nothing to change.
    assert db.get_config(TOKEN_PROXIES_ON_KEY) == "1"
    off = _add(db, 1, {"USER_TOKEN_USE_PROXIES": False})
    null = _add(db, 2, {"USER_TOKEN_USE_PROXIES": None})
    on = _add(db, 3, {"USER_TOKEN_USE_PROXIES": True})
    missing = _add(db, 4, {"CLONE_MESSAGES": True})
    # As an older version's DB: the migration has not run yet.
    with db._tx():
        db.conn.execute("DELETE FROM app_config WHERE key = ?", (TOKEN_PROXIES_ON_KEY,))
    db.close()

    caplog.set_level(logging.INFO, logger="common.db")
    db = DBManager(path, init_schema=True)
    assert _setting(db, off) is True
    assert _setting(db, null) is True
    assert _setting(db, on) is True
    assert _setting(db, missing) == "missing"
    lines = [r.getMessage() for r in caplog.records if "one-time migration" in r.getMessage()]
    assert lines == ["Send through proxies turned back on for 2 guild mapping(s) (one-time migration)"]

    # A false the user saves after the migration survives the next start.
    _set(db, off, False)
    db.close()
    caplog.clear()
    db = DBManager(path, init_schema=True)
    assert _setting(db, off) is False
    assert db._migrate_token_proxies_on() == 0
    assert not [r for r in caplog.records if "one-time migration" in r.getMessage()]
    db.close()


def test_a_string_off_is_flipped(tmp_path):
    path = str(tmp_path / "data.db")
    db = DBManager(path, init_schema=True)
    mid = _add(db, 1, {"USER_TOKEN_USE_PROXIES": "false"})
    keep = _add(db, 2, {"USER_TOKEN_USE_PROXIES": "true"})
    with db._tx():
        db.conn.execute("DELETE FROM app_config WHERE key = ?", (TOKEN_PROXIES_ON_KEY,))
    assert db._migrate_token_proxies_on() == 1
    assert _setting(db, mid) is True
    assert _setting(db, keep) == "true"
    assert db._migrate_token_proxies_on() == 0
    db.close()
