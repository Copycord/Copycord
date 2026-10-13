"""Shared HTTP session, DB close and backup logging on shutdown."""
import asyncio
import logging
import sqlite3
import sys
import types
from pathlib import Path

import pytest

from copycord import supervisor as sup_mod
from copycord.common import http_session
from copycord.common.backup_scheduler import BackupConfig, DailySQLiteBackupScheduler


def test_shared_session_is_reused_and_closed_on_shutdown():
    async def go():
        a = http_session.shared_session()
        b = http_session.shared_session()
        assert a is b
        assert a.timeout.total == 30 and a.timeout.connect == 10
        await http_session.close_shared_session()
        assert a.closed
        c = http_session.shared_session()
        assert c is not a and not c.closed
        await http_session.close_shared_session()
        assert c.closed
        await http_session.close_shared_session()

    asyncio.run(go())


def test_each_loop_gets_its_own_session():
    seen = []

    async def go():
        s = http_session.shared_session()
        seen.append(s)
        await http_session.close_shared_session()

    asyncio.run(go())
    asyncio.run(go())
    assert seen[0] is not seen[1]
    assert all(s.closed for s in seen)


def test_db_close_checkpoints_and_is_idempotent(db, tmp_db_path):
    db.set_config("X", "1")
    db.close()
    db.close()
    with pytest.raises(sqlite3.ProgrammingError):
        db.conn.execute("SELECT 1")
    assert not Path(tmp_db_path + "-wal").exists() or Path(tmp_db_path + "-wal").stat().st_size == 0


def test_supervisor_closes_the_admin_db(monkeypatch):
    closed = []
    fake = types.SimpleNamespace(db=types.SimpleNamespace(close=lambda: closed.append(1)))
    monkeypatch.setitem(sys.modules, "copycord.admin.app", fake)
    sup_mod._close_admin_db()
    assert closed == [1]


def test_supervisor_skips_admin_db_when_admin_never_loaded(monkeypatch):
    monkeypatch.delitem(sys.modules, "copycord.admin.app", raising=False)
    sup_mod._close_admin_db()


def test_apply_db_config_closes_its_db(monkeypatch, tmp_db_path):
    from copycord.common import db as dbmod

    closed = []
    real_close = dbmod.DBManager.close

    def spy(self):
        closed.append(self)
        real_close(self)

    monkeypatch.setattr(dbmod.DBManager, "close", spy)
    monkeypatch.setenv("DB_PATH", tmp_db_path)
    dbmod.DBManager(tmp_db_path, init_schema=True).close()
    closed.clear()
    sup_mod._apply_db_config()
    assert len(closed) == 1


def test_backup_scheduler_without_logger_uses_logging(tmp_path, caplog, capsys):
    cfg = BackupConfig(db_path=tmp_path / "data.db", backup_dir=tmp_path / "b")
    sched = DailySQLiteBackupScheduler(cfg)
    with caplog.at_level(logging.DEBUG, logger="copycord.backup"):
        sched._info("hello %s", "x")
        sched._warn("warn %d", 2)
    assert "hello x" in caplog.text and "warn 2" in caplog.text
    assert capsys.readouterr().out == ""
