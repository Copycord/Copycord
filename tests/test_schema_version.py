"""Tests for the app_config schema_version row and the one-time pre-v4 backup."""
import os
import shutil
import sqlite3
import sys

import pytest

from copycord import __main__ as cli
from copycord.common.db import (
    SCHEMA_VERSION,
    SCHEMA_VERSION_KEY,
    BackupNotWritableError,
    DBManager,
    backup_pre_v4,
    check_pre_v4_backup,
    needs_pre_v4_backup,
)

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions")


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


@pytest.fixture()
def locked():
    """Make a folder read-only (0o500) for the test; skips under root, which chmod does not stop."""
    done = []

    def lock(path):
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o500)
        done.append(path)
        if os.access(path, os.W_OK):
            pytest.skip("running as root: a 0o500 folder is still writable")
        return path

    yield lock
    for path in done:
        path.chmod(0o700)


def test_backup_goes_next_to_a_db_outside_the_data_dir(data_dir, tmp_path):
    # A DB_PATH outside DATA_DIR: the copy goes beside the DB, not into DATA_DIR.
    path = tmp_path / "elsewhere" / "data.db"
    path.parent.mkdir()
    _make_v3_db(path)
    DBManager(str(path), init_schema=True).close()
    assert len(_backups(path.parent)) == 1
    assert _backups(data_dir) == []
    assert not (data_dir / "backups").exists()


@posix_only
def test_unwritable_backups_dir_is_one_clear_error(data_dir, locked):
    # v3 ran as root: data.db and backups stay root-owned inside a chowned /data.
    path = data_dir / "data.db"
    _make_v3_db(path)
    backups = locked(data_dir / "backups")
    for attempt in (lambda: backup_pre_v4(str(path)),
                    lambda: check_pre_v4_backup(str(path)),
                    lambda: DBManager(str(path), init_schema=True)):
        with pytest.raises(BackupNotWritableError) as err:
            attempt()
        assert err.value.directory == backups.resolve()
        assert str(backups.resolve()) in str(err.value)
        assert "sudo chown -R 1000:1000" in str(err.value)
    assert list(backups.iterdir()) == []
    # v4 did not open the DB: no schema_version written
    assert needs_pre_v4_backup(str(path))


@posix_only
def test_unwritable_db_folder_is_one_clear_error(tmp_path, locked):
    # No backups folder yet and the DB's folder is read-only: mkdir fails the same way.
    path = tmp_path / "ro" / "data.db"
    path.parent.mkdir()
    _make_v3_db(path)
    locked(path.parent)
    with pytest.raises(BackupNotWritableError) as err:
        backup_pre_v4(str(path))
    assert err.value.directory == path.parent.resolve() / "backups"


@posix_only
def test_run_stops_once_with_the_message(data_dir, locked, monkeypatch, capsys):
    path = data_dir / "data.db"
    _make_v3_db(path)
    monkeypatch.setenv("DB_PATH", str(path))
    backups = locked(data_dir / "backups")
    assert cli.backup_guard() is False
    err = capsys.readouterr().err
    assert err.count(str(backups.resolve())) == 1
    assert err.count("sudo chown -R 1000:1000") == 1
    assert "Traceback" not in err


def test_run_guard_passes_when_the_backup_can_be_written(data_dir, monkeypatch, capsys):
    path = data_dir / "data.db"
    _make_v3_db(path)
    monkeypatch.setenv("DB_PATH", str(path))
    assert cli.backup_guard() is True
    assert capsys.readouterr().err == ""
    # the probe leaves nothing behind; the admin's open makes the one backup
    assert list((data_dir / "backups").iterdir()) == []


@pytest.mark.parametrize("with_shm", [True, False], ids=["wal+shm", "wal-only"])
def test_v3_db_with_a_leftover_wal_still_backs_up(tmp_path, with_shm):
    live = tmp_path / "live"
    live.mkdir()
    conn = sqlite3.connect(str(live / "data.db"))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA wal_autocheckpoint = 0")
    conn.execute(
        "CREATE TABLE app_config(key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')"
    )
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("INSERT INTO app_config(key, value) VALUES('COMMAND_USERS', '1,2')")
    conn.commit()
    # Copy the files while the writer is still open, as a crash would leave them.
    crashed = tmp_path / "data"
    crashed.mkdir()
    names = ["data.db", "data.db-wal"] + (["data.db-shm"] if with_shm else [])
    for name in names:
        shutil.copyfile(live / name, crashed / name)
    conn.close()
    assert (crashed / "data.db-wal").stat().st_size > 0

    dest = backup_pre_v4(str(crashed / "data.db"))
    assert dest.parent == crashed.resolve() / "backups"
    out = sqlite3.connect(str(dest))
    try:
        rows = dict(out.execute("SELECT key, value FROM app_config").fetchall())
    finally:
        out.close()
    assert rows == {"COMMAND_USERS": "1,2"}


def test_no_backup_when_the_schema_is_already_v4(tmp_path):
    path = tmp_path / "data.db"
    DBManager(str(path), init_schema=True).close()
    assert not needs_pre_v4_backup(str(path))
    assert backup_pre_v4(str(path)) is None
    check_pre_v4_backup(str(path))
    DBManager(str(path), init_schema=True).close()
    assert not (tmp_path / "backups").exists()
