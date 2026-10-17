"""Tests for the database backups: the integrity check, secret.key and the
manifest in the archive, copycord backup and copycord restore. No network."""
import asyncio
import io
import json
import os
import sqlite3
import sys
import tarfile

import pytest

from copycord import __main__ as cli
from copycord.common import backup_scheduler as bs
from copycord.common.db import SCHEMA_VERSION, DBManager


def _unset(monkeypatch, *names):
    # setenv first so monkeypatch restores the original even when the code sets it again.
    for name in names:
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    """A data dir with a database (MARK=current) and a secret.key; Copycord is not running."""
    d = (tmp_path / "data").resolve()
    d.mkdir()
    monkeypatch.setenv("DATA_DIR", str(d))
    _unset(monkeypatch, "DB_PATH", "ROLE", "BACKUP_DIR", "PASSWORD", "SERVER_TOKEN",
           "CLIENT_TOKEN", "NO_COLOR", "BACKUP_RETAIN")
    monkeypatch.setenv("COPYCORD_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr(cli, "service_state", lambda: None)
    monkeypatch.setattr(cli, "copycord_on_port", lambda port, timeout=1.0: False)
    _make_db(d / "data.db", "current")
    (d / "secret.key").write_text("current-key", encoding="utf-8")
    return d


def _make_db(path, mark, schema=None):
    db = DBManager(str(path), init_schema=True)
    db.set_config("MARK", mark)
    if schema is not None:
        db.set_config("schema_version", str(schema))
    db.conn.close()


def _mark(path):
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute("SELECT value FROM app_config WHERE key='MARK'").fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def _tar(path, files):
    """A .tar.gz holding files {name: bytes}."""
    with tarfile.open(path, "w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return path


def _db_bytes(tmp_path, mark, schema=None):
    path = tmp_path / f"src-{mark}.db"
    _make_db(path, mark, schema)
    return path.read_bytes()


def _db_in(archive, tmp_path):
    """data.db of an archive, copied out."""
    out = tmp_path / f"out-{archive.name}.db"
    with tarfile.open(archive) as tar:
        out.write_bytes(tar.extractfile("data.db").read())
    return out


def _archives(data):
    return sorted((data / "backups").glob("*.tar.gz"))


def _no_terminal(monkeypatch):
    from copycord import ui

    monkeypatch.setattr(ui, "is_interactive", lambda: False)
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("prompted without a terminal"))


def _answers(monkeypatch, *replies):
    """Feed replies to the typed prompts; record each prompt asked."""
    from copycord import ui

    asked = []
    queue = list(replies)

    def fake_input(prompt=""):
        asked.append(prompt)
        return queue.pop(0)

    monkeypatch.setattr(ui, "is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", fake_input)
    return asked


# the integrity check


def test_integrity_problems(tmp_path):
    good = tmp_path / "good.db"
    _make_db(good, "x")
    assert bs.integrity_problems(good) == []
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"this is not a database" * 200)
    assert bs.integrity_problems(bad)


def test_failed_check_keeps_no_archive_and_doctor_reports_it(data_dir, monkeypatch, capsys):
    real = bs.integrity_problems
    monkeypatch.setattr(bs, "integrity_problems", lambda path: ["row 7 missing from index x"])
    assert cli.main(["backup"]) == 1
    err = capsys.readouterr().err
    assert "backup failed its check" in err and "row 7 missing from index x" in err
    backups = data_dir / "backups"
    assert not [p for p in backups.iterdir() if p.name != bs.CHECK_FAILED]
    assert (backups / bs.CHECK_FAILED).is_file()

    check = cli.check_backups(backups, data_dir / "data.db")
    assert (check.status, check.name, check.fix) == (cli.WARN, "last backup failed its check", "backup")
    assert "row 7 missing from index x" in check.reason

    # The next good backup clears it.
    monkeypatch.setattr(bs, "integrity_problems", real)
    assert cli.main(["backup"]) == 0
    assert not (backups / bs.CHECK_FAILED).exists()
    assert cli.check_backups(backups, data_dir / "data.db").status == cli.PASS


def test_scheduled_backup_that_fails_its_check_logs_a_warning(data_dir, monkeypatch, caplog):
    import logging

    monkeypatch.setattr(bs, "integrity_problems", lambda path: ["database disk image is malformed"])
    cfg = bs.BackupConfig(db_path=data_dir / "data.db", backup_dir=data_dir / "backups")
    scheduler = bs.DailySQLiteBackupScheduler(cfg, logger=logging.getLogger("test.backup"))
    with caplog.at_level(logging.WARNING, logger="test.backup"):
        with pytest.raises(bs.BackupCheckError):
            asyncio.run(scheduler.run_now())
    assert "failed its integrity check" in caplog.text
    assert not _archives(data_dir)
    assert bs.last_check_failure(data_dir / "backups")["reason"] == "database disk image is malformed"


def test_an_older_failure_does_not_hide_a_newer_backup(tmp_path):
    import time

    backups, db = tmp_path / "backups", tmp_path / "data.db"
    backups.mkdir()
    bs.record_check_failure(backups, "malformed")
    now = time.time()
    os.utime(backups / bs.CHECK_FAILED, (now - 7200, now - 7200))
    archive = backups / "2026-10-12_03-17-00.tar.gz"
    archive.write_bytes(b"x")
    os.utime(archive, (now - 3600, now - 3600))
    assert cli.check_backups(backups, db, now).name == "last backup 1 hour ago"


# what an archive holds


def test_backup_includes_the_key_and_a_manifest(data_dir, capsys):
    from copycord.common.config import CURRENT_VERSION

    assert cli.main(["backup"]) == 0
    [archive] = _archives(data_dir)
    assert f"Database backed up  {archive}" in capsys.readouterr().out
    with tarfile.open(archive) as tar:
        assert tar.getnames() == ["data.db", "secret.key", "manifest.json"]
        assert tar.extractfile("secret.key").read() == b"current-key"
        manifest = json.loads(tar.extractfile("manifest.json").read())
    assert manifest["format"] == 1
    assert manifest["copycord_version"] == CURRENT_VERSION.lstrip("v")
    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["created_at"].endswith("Z")
    assert manifest["files"] == ["data.db", "secret.key"]


def test_backup_without_a_key_has_only_the_database(data_dir):
    (data_dir / "secret.key").unlink()
    assert cli.main(["backup"]) == 0
    [archive] = _archives(data_dir)
    with tarfile.open(archive) as tar:
        assert tar.getnames() == ["data.db", "manifest.json"]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions")
def test_backup_archive_is_owner_only(data_dir):
    old = os.umask(0o022)
    try:
        assert cli.main(["backup"]) == 0
    finally:
        os.umask(old)
    [archive] = _archives(data_dir)
    assert archive.stat().st_mode & 0o777 == 0o600


def test_backup_json(data_dir, capsys):
    assert cli.main(["backup", "--json"]) == 0
    captured = capsys.readouterr()
    doc = json.loads(captured.out)
    [archive] = _archives(data_dir)
    assert doc == {"ok": True, "file": str(archive), "size": archive.stat().st_size,
                   "includes_key": True}


# restore


@pytest.fixture()
def saved(data_dir, tmp_path):
    """An archive of the database with MARK=saved and the key saved-key; the
    live database then moves on to MARK=current."""
    db = data_dir / "data.db"
    _make_db(db, "saved")
    (data_dir / "secret.key").write_text("saved-key", encoding="utf-8")
    assert cli.main(["backup"]) == 0
    [archive] = _archives(data_dir)
    _make_db(db, "current")
    (data_dir / "secret.key").write_text("current-key", encoding="utf-8")
    return archive


def test_restore_backs_up_the_current_database_first(data_dir, saved, tmp_path, monkeypatch, capsys):
    _no_terminal(monkeypatch)
    capsys.readouterr()
    assert cli.main(["restore", "--yes", str(saved)]) == 0
    assert _mark(data_dir / "data.db") == "saved"
    [before] = (data_dir / "backups").glob("pre-restore-*.tar.gz")
    assert _mark(_db_in(before, tmp_path)) == "current"
    with tarfile.open(before) as tar:
        assert tar.extractfile("secret.key").read() == b"current-key"
    out = capsys.readouterr().out
    assert f"backed up the current database  {before}" in out
    assert "Restore complete" in out
    assert f"To undo it, run copycord restore {before}" in out
    assert out.rstrip().endswith("Start Copycord with copycord service start or copycord run")
    assert not list(data_dir.glob(".restore-*"))


def test_restore_puts_the_key_back_and_keeps_the_current_one(data_dir, saved, monkeypatch, capsys):
    _no_terminal(monkeypatch)
    assert cli.main(["restore", "--yes", str(saved)]) == 0
    assert (data_dir / "secret.key").read_text(encoding="utf-8") == "saved-key"
    [kept] = data_dir.glob("secret.key.bak-*")
    assert kept.read_text(encoding="utf-8") == "current-key"
    out = capsys.readouterr().out
    assert "secret.key restored" in out and f"previous secret.key kept as  {kept}" in out


def test_restore_with_the_same_key_keeps_no_copy(data_dir, saved, monkeypatch):
    _no_terminal(monkeypatch)
    (data_dir / "secret.key").write_text("saved-key", encoding="utf-8")
    assert cli.main(["restore", "--yes", str(saved)]) == 0
    assert not list(data_dir.glob("secret.key.bak-*"))


def test_restore_an_older_archive_without_key_or_manifest(data_dir, tmp_path, monkeypatch, capsys):
    _no_terminal(monkeypatch)
    old = _tar(tmp_path / "2025-01-01_03-17-00.tar.gz", {"data.db": _db_bytes(tmp_path, "old")})
    assert cli.main(["restore", "--yes", str(old)]) == 0
    assert _mark(data_dir / "data.db") == "old"
    assert (data_dir / "secret.key").read_text(encoding="utf-8") == "current-key"
    assert not list(data_dir.glob("secret.key.bak-*"))
    assert "Includes the database only; your current secret.key is kept" in capsys.readouterr().out


def test_restore_a_pre_update_copy(data_dir, tmp_path, monkeypatch):
    _no_terminal(monkeypatch)
    copy = tmp_path / "data-pre-update-4.0.0-20261012T091500Z.db"
    copy.write_bytes(_db_bytes(tmp_path, "pre-update"))
    assert cli.main(["restore", "--yes", str(copy)]) == 0
    assert _mark(data_dir / "data.db") == "pre-update"


def test_restore_refuses_while_copycord_runs(data_dir, saved, monkeypatch, capsys):
    _no_terminal(monkeypatch)
    monkeypatch.setattr(cli, "copycord_on_port", lambda port, timeout=1.0: True)
    assert cli.main(["restore", "--yes", str(saved)]) == 1
    assert _mark(data_dir / "data.db") == "current"
    assert not list((data_dir / "backups").glob("pre-restore-*"))
    err = capsys.readouterr().err
    assert "Copycord is running; stop it before restoring" in err


def test_restore_refuses_a_newer_schema(data_dir, tmp_path, monkeypatch, capsys):
    _no_terminal(monkeypatch)
    newer = _tar(tmp_path / "newer.tar.gz",
                 {"data.db": _db_bytes(tmp_path, "newer", schema=SCHEMA_VERSION + 1)})
    assert cli.main(["restore", "--yes", str(newer)]) == 1
    assert _mark(data_dir / "data.db") == "current"
    assert not (data_dir / "backups").exists()
    err = capsys.readouterr().err
    assert f"the backup is from a newer Copycord (schema {SCHEMA_VERSION + 1}" in err
    assert "nothing was changed" in err


def test_restore_refuses_a_damaged_backup(data_dir, tmp_path, monkeypatch, capsys):
    _no_terminal(monkeypatch)
    bad = _tar(tmp_path / "bad.tar.gz", {"data.db": b"this is not a database" * 200})
    assert cli.main(["restore", "--yes", str(bad)]) == 1
    assert _mark(data_dir / "data.db") == "current"
    assert "the backup failed its integrity check, nothing was changed" in capsys.readouterr().err


def test_restore_reads_members_by_name_only(data_dir, tmp_path, monkeypatch):
    _no_terminal(monkeypatch)
    evil = _tar(tmp_path / "evil.tar.gz", {"../escaped.txt": b"x",
                                            "data.db": _db_bytes(tmp_path, "safe")})
    assert cli.main(["restore", "--yes", str(evil)]) == 0
    assert not (data_dir.parent / "escaped.txt").exists()
    assert _mark(data_dir / "data.db") == "safe"


def test_restore_without_a_terminal_needs_yes(data_dir, saved, monkeypatch, capsys):
    _no_terminal(monkeypatch)
    assert cli.main(["restore", str(saved)]) == 1
    assert _mark(data_dir / "data.db") == "current"
    assert "rerun with --yes" in capsys.readouterr().err


def test_restore_lists_the_newest_backups_and_asks(data_dir, saved, tmp_path, monkeypatch, capsys):
    older = data_dir / "backups" / "data-pre-update-3.28.0-20250101T000000Z.db"
    older.write_bytes(_db_bytes(tmp_path, "older"))
    os.utime(older, (1_700_000_000, 1_700_000_000))
    asked = _answers(monkeypatch, "1", "y")
    assert cli.main(["restore"]) == 0
    assert _mark(data_dir / "data.db") == "saved"
    assert len(asked) == 2 and "Replace the database in" in asked[1]
    out = capsys.readouterr().out
    assert "Which backup do you want to restore?" in out
    assert "1) " in out and f"({saved.name})" in out
    assert "2) " in out and "made before an update" in out and "3) Cancel" in out


def test_restore_cancel_changes_nothing(data_dir, saved, monkeypatch, capsys):
    _answers(monkeypatch, "2")
    assert cli.main(["restore"]) == 1
    assert _mark(data_dir / "data.db") == "current"
    assert not list((data_dir / "backups").glob("pre-restore-*"))
    assert "restore cancelled" in capsys.readouterr().err


def test_restore_undo_with_the_pre_restore_backup(data_dir, saved, monkeypatch):
    _no_terminal(monkeypatch)
    assert cli.main(["restore", "--yes", str(saved)]) == 0
    [before] = (data_dir / "backups").glob("pre-restore-*.tar.gz")
    assert cli.main(["restore", "--yes", str(before)]) == 0
    assert _mark(data_dir / "data.db") == "current"
    assert (data_dir / "secret.key").read_text(encoding="utf-8") == "current-key"


def test_pre_restore_backups_are_not_pruned(data_dir, monkeypatch):
    _no_terminal(monkeypatch)
    backups = data_dir / "backups"
    backups.mkdir()
    keep = backups / "pre-restore-20250101T000000Z.tar.gz"
    keep.write_bytes(b"x")
    os.utime(keep, (1_700_000_000, 1_700_000_000))
    monkeypatch.setenv("BACKUP_RETAIN", "1")
    assert cli.main(["backup"]) == 0
    assert cli.main(["backup"]) == 0
    assert keep.exists()
    assert len([p for p in _archives(data_dir) if p != keep]) == 1


def test_restore_replaces_a_stale_journal(data_dir, saved, monkeypatch):
    _no_terminal(monkeypatch)
    journal = data_dir / "data.db-journal"
    journal.write_bytes(b"")
    assert cli.main(["restore", "--yes", str(saved)]) == 0
    assert not journal.exists()
    assert _mark(data_dir / "data.db") == "saved"

