"""
Tests for the downgrade guard (src/copycord/common/db.py): a DB with a newer
schema_version is refused and never written, schema_version is never lowered,
`copycord run` and `copycord doctor` report it, and `copycord update` backs up
the DB before it installs (src/copycord/update.py).
"""
import os
import re
import sqlite3
import subprocess
import time

import pytest

from copycord import __main__ as cli
from copycord import service, update
from copycord.common import db as dbmod
from copycord.common.db import (
    PRE_UPDATE_RETAIN,
    SCHEMA_VERSION,
    SCHEMA_VERSION_KEY,
    DBManager,
    SchemaTooNewError,
    backup_pre_update,
)


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setenv("DATA_DIR", str(d))
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.setenv("BACKUP_DIR", str(d / "backups"))
    return d


def _stored(path):
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            "SELECT value FROM app_config WHERE key=?", (SCHEMA_VERSION_KEY,)
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _set_version(path, value):
    conn = sqlite3.connect(str(path))
    conn.execute(
        "INSERT INTO app_config(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (SCHEMA_VERSION_KEY, str(value)),
    )
    conn.commit()
    conn.close()


def _make_db(path, version=SCHEMA_VERSION):
    DBManager(str(path), init_schema=True).conn.close()
    _set_version(path, version)


# --------------------------------------------------------------------------
# refusing a newer schema
# --------------------------------------------------------------------------


@pytest.mark.parametrize("init_schema", [True, False])
def test_newer_schema_is_refused_and_untouched(data_dir, init_schema):
    path = data_dir / "data.db"
    _make_db(path, SCHEMA_VERSION + 1)
    before = path.read_bytes()
    with pytest.raises(SchemaTooNewError) as exc:
        DBManager(str(path), init_schema=init_schema)
    assert exc.value.stored == SCHEMA_VERSION + 1
    assert exc.value.supported == SCHEMA_VERSION
    assert path.read_bytes() == before
    assert _stored(path) == str(SCHEMA_VERSION + 1)
    assert not (data_dir / "backups").exists()


@pytest.mark.parametrize("version", [SCHEMA_VERSION, SCHEMA_VERSION - 1, 1])
def test_equal_and_older_schemas_open(data_dir, version):
    path = data_dir / "data.db"
    _make_db(path, version)
    m = DBManager(str(path), init_schema=True)
    m.conn.close()
    assert _stored(path) == str(SCHEMA_VERSION)


def test_missing_schema_version_keeps_todays_behaviour(data_dir):
    path = data_dir / "data.db"
    m = DBManager(str(path), init_schema=True)
    m.conn.close()
    assert _stored(path) == str(SCHEMA_VERSION)


def test_set_schema_version_never_lowers(data_dir):
    path = data_dir / "data.db"
    m = DBManager(str(path), init_schema=True)
    m.set_config(SCHEMA_VERSION_KEY, str(SCHEMA_VERSION + 3))
    m._set_schema_version()
    assert m.get_config(SCHEMA_VERSION_KEY) == str(SCHEMA_VERSION + 3)
    m.set_config(SCHEMA_VERSION_KEY, str(SCHEMA_VERSION - 1))
    m._set_schema_version()
    assert m.get_config(SCHEMA_VERSION_KEY) == str(SCHEMA_VERSION)
    m.conn.close()


# --------------------------------------------------------------------------
# copycord run / doctor
# --------------------------------------------------------------------------


def test_run_refuses_with_message(data_dir, monkeypatch, capsys):
    path = data_dir / "data.db"
    _make_db(path, SCHEMA_VERSION + 1)
    backup_pre_update(str(path), "4.1.0", data_dir / "backups")
    newest = dbmod.pre_update_backups(data_dir / "backups")[0]
    before = path.read_bytes()
    from copycord import supervisor

    async def boom():
        pytest.fail("the supervisor started")

    monkeypatch.setattr(supervisor, "run", boom)
    assert cli.main(["run"]) == 1
    err = re.sub(r"\s+", " ", capsys.readouterr().err)
    assert (
        f"This database was created by a newer Copycord (schema {SCHEMA_VERSION + 1}, "
        f"this version supports {SCHEMA_VERSION})."
    ) in err
    assert "copycord update" in err
    assert str(data_dir / "backups") in err
    assert newest.name in err
    assert path.read_bytes() == before


def test_doctor_fails_on_newer_schema(data_dir):
    path = data_dir / "data.db"
    _make_db(path, SCHEMA_VERSION + 1)
    check = cli.check_db(path)
    assert check.status == cli.FAIL
    assert "newer" in check.reason
    _set_version(path, SCHEMA_VERSION)
    assert cli.check_db(path).status == cli.PASS


# --------------------------------------------------------------------------
# pre-update backup
# --------------------------------------------------------------------------


def _counts(path):
    conn = sqlite3.connect(str(path))
    try:
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        ]
        return {t: conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables}
    finally:
        conn.close()


def test_backup_pre_update_copy_naming_and_rows(data_dir):
    path = data_dir / "data.db"
    m = DBManager(str(path), init_schema=True)
    m.set_config("COMMAND_USERS", "1,2")
    m.conn.close()
    dest = backup_pre_update(str(path), "4.0.0")
    assert dest.parent == data_dir / "backups"
    assert re.fullmatch(r"data-pre-update-4\.0\.0-\d{8}T\d{6}Z(-\d+)?\.db", dest.name)
    conn = sqlite3.connect(str(dest))
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    conn.close()
    assert _counts(dest) == _counts(path)
    assert _counts(dest)["app_config"] >= 2


def test_backup_pre_update_without_db(data_dir):
    assert backup_pre_update(str(data_dir / "data.db"), "4.0.0") is None
    assert not (data_dir / "backups").exists()


def test_backup_pre_update_keeps_five(data_dir):
    path = data_dir / "data.db"
    _make_db(path)
    backups = data_dir / "backups"
    backups.mkdir()
    daily = backups / "2026-01-01_03-17-00.tar.gz"
    daily.write_bytes(b"x")
    made = []
    for i in range(PRE_UPDATE_RETAIN + 2):
        made.append(backup_pre_update(str(path), f"4.0.{i}"))
        os.utime(made[-1], (time.time() - 100 + i, time.time() - 100 + i))
    left = dbmod.pre_update_backups(backups)
    assert len(left) == PRE_UPDATE_RETAIN
    assert set(left) == set(made[-PRE_UPDATE_RETAIN:])
    assert daily.exists()


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, cmd, capture=True, **kwargs):
        self.calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")


@pytest.fixture()
def uv_update(data_dir, monkeypatch):
    monkeypatch.setattr(update, "detect_installer", lambda: update.UV_TOOL)
    monkeypatch.setattr(update.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    monkeypatch.setattr(update.node_runtime, "ensure_npm", lambda: data_dir)
    monkeypatch.setattr(
        service, "service_state", lambda: service.State(False, False, "test")
    )
    rec = Recorder()
    monkeypatch.setattr(service, "run_cmd", rec)
    return rec


def test_update_backs_up_before_installing(data_dir, uv_update, monkeypatch, capsys):
    path = data_dir / "data.db"
    _make_db(path)
    seen = []

    def check_then_run(cmd, capture=True, **kw):
        seen.append(dbmod.pre_update_backups(data_dir / "backups"))
        return uv_update(cmd, capture, **kw)

    monkeypatch.setattr(service, "run_cmd", check_then_run)
    assert update.update(channel="dev", branch="v4/db-guard") == 0
    assert len(seen) == 1 and len(seen[0]) == 1
    assert seen[0][0].name.startswith("data-pre-update-4.0.0-")
    assert str(seen[0][0]) in re.sub(r"\s+", " ", capsys.readouterr().out)


def test_update_without_db_notes_and_installs(data_dir, uv_update, capsys):
    assert update.update(channel="dev") == 0
    assert len(uv_update.calls) == 1
    assert "no database yet" in capsys.readouterr().out


def test_update_backup_failure_aborts(data_dir, uv_update, monkeypatch, capsys):
    _make_db(data_dir / "data.db")

    def fail(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(dbmod, "backup_pre_update", fail)
    assert cli.main(["update", "--channel", "dev"]) == 1
    assert uv_update.calls == []
    err = capsys.readouterr().err
    assert "Database backup failed" in err and "disk full" in err
    assert "--no-backup" in err


def test_update_no_backup_skips(data_dir, uv_update, monkeypatch, capsys):
    _make_db(data_dir / "data.db")
    monkeypatch.setattr(
        dbmod, "backup_pre_update", lambda *a, **k: pytest.fail("backed up")
    )
    assert cli.main(["update", "--channel", "dev", "--no-backup"]) == 0
    assert len(uv_update.calls) == 1
    assert "skipped" in capsys.readouterr().err
    assert dbmod.pre_update_backups(data_dir / "backups") == []
