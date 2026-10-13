"""Tests for the app_config schema_version row and the one-time pre-v4 backup."""
import sqlite3

import pytest

from copycord.common.db import (
    SCHEMA_VERSION,
    SCHEMA_VERSION_KEY,
    DBManager,
    backup_pre_v4,
    needs_pre_v4_backup,
)


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setenv("DATA_DIR", str(d))
    return d


def _make_v3_db(path):
    """A DB as v3 left it: app_config with values but no schema_version."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE app_config(key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '',"
        " last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.execute("INSERT INTO app_config(key, value) VALUES('COMMAND_USERS', '1,2')")
    conn.commit()
    conn.close()


def _backups(data_dir):
    d = data_dir / "backups"
    return sorted(d.glob("pre-v4-*.db")) if d.exists() else []


def test_fresh_db_gets_schema_version_and_no_backup(data_dir):
    db = DBManager(str(data_dir / "data.db"), init_schema=True)
    assert db.get_config(SCHEMA_VERSION_KEY) == str(SCHEMA_VERSION) == "4"
    db.conn.close()
    assert _backups(data_dir) == []


def test_v3_db_is_backed_up_once_before_first_v4_open(data_dir):
    path = data_dir / "data.db"
    _make_v3_db(path)
    assert needs_pre_v4_backup(str(path))

    db = DBManager(str(path), init_schema=True)
    assert db.get_config(SCHEMA_VERSION_KEY) == "4"
    assert db.get_config("COMMAND_USERS") == "1,2"
    db.conn.close()

    backups = _backups(data_dir)
    assert len(backups) == 1
    conn = sqlite3.connect(str(backups[0]))
    try:
        rows = dict(conn.execute("SELECT key, value FROM app_config").fetchall())
    finally:
        conn.close()
    assert rows == {"COMMAND_USERS": "1,2"}

    assert not needs_pre_v4_backup(str(path))
    DBManager(str(path), init_schema=True).conn.close()
    assert _backups(data_dir) == backups


def test_open_without_init_schema_neither_backs_up_nor_writes_version(data_dir):
    path = data_dir / "data.db"
    _make_v3_db(path)
    db = DBManager(str(path))
    assert db.get_config(SCHEMA_VERSION_KEY, "") == ""
    db.conn.close()
    assert _backups(data_dir) == []


def test_db_without_app_config_needs_backup(tmp_path):
    path = tmp_path / "other.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE t(x)")
    conn.commit()
    conn.close()
    assert needs_pre_v4_backup(str(path))


def test_missing_or_empty_db_needs_no_backup(tmp_path):
    assert not needs_pre_v4_backup(str(tmp_path / "missing.db"))
    empty = tmp_path / "empty.db"
    empty.touch()
    assert not needs_pre_v4_backup(str(empty))
    assert backup_pre_v4(str(empty), tmp_path / "backups") is None


def test_backup_names_do_not_collide(tmp_path):
    path = tmp_path / "data.db"
    _make_v3_db(path)
    first = backup_pre_v4(str(path), tmp_path / "backups")
    second = backup_pre_v4(str(path), tmp_path / "backups")
    assert first != second
    assert first.exists() and second.exists()
