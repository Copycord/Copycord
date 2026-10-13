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


def test_config_logs_a_failed_db_read_and_a_bad_number(monkeypatch, tmp_db_path, caplog):
    from copycord.common import db as dbmod
    from copycord.common.config import Config

    dbmod.DBManager(tmp_db_path, init_schema=True).close()
    monkeypatch.setenv("DB_PATH", tmp_db_path)
    monkeypatch.setenv("SYNC_INTERVAL_SECONDS", "soon")

    def boom(self, key, default=""):
        raise sqlite3.OperationalError("locked")

    monkeypatch.setattr(dbmod.DBManager, "get_config", boom)
    with caplog.at_level(logging.DEBUG):
        cfg = Config(logger=logging.getLogger("test.config"))
    assert cfg.SYNC_INTERVAL_SECONDS == 3600
    assert "DB read failed" in caplog.text
    assert "SYNC_INTERVAL_SECONDS='soon' is not a number" in caplog.text
    cfg.db.close()


def test_backup_scheduler_without_logger_uses_logging(tmp_path, caplog, capsys):
    cfg = BackupConfig(db_path=tmp_path / "data.db", backup_dir=tmp_path / "b")
    sched = DailySQLiteBackupScheduler(cfg)
    with caplog.at_level(logging.DEBUG, logger="copycord.backup"):
        sched._info("hello %s", "x")
        sched._warn("warn %d", 2)
    assert "hello x" in caplog.text and "warn 2" in caplog.text
    assert capsys.readouterr().out == ""


class _DbRole:
    """Opens the databases a real server/client role opens: Config().db and self.db
    (closed by run(), like ServerReceiver/ClientListener), plus a commands-cog DB and a
    module-level Config that nothing but the supervisor's close_all() closes."""

    instances = []

    def __init__(self):
        from copycord.common.config import Config
        from copycord.common.db import DBManager

        self.config = Config()
        self.db = DBManager(self.config.DB_PATH)
        self.cog_db = DBManager(self.config.DB_PATH)
        self.module_config = Config()
        self.ready = False
        _DbRole.instances.append(self)

    def run(self):
        loop = asyncio.get_event_loop()
        self._stop = asyncio.Event()
        try:
            self.db.set_config("ROLE_WROTE", "1")
            self.cog_db.set_config("COG_WROTE", "1")
            self.ready = True
            loop.run_until_complete(self._stop.wait())
        finally:
            self.db.close()
            self.config.db.close()

    async def _shutdown(self):
        if getattr(self, "_stop", None) is not None:
            self._stop.set()


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT handler path is POSIX-only")
def test_ctrl_c_shutdown_leaves_no_wal_or_shm(monkeypatch, tmp_path):
    """copycord run, stopped with SIGINT, leaves only data.db behind and it passes integrity_check."""
    import os
    import signal
    import weakref

    from copycord.common import db as dbmod
    from copycord.supervisor import Backoff, ThreadedRole, build_supervisor

    db_file = tmp_path / "data.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    # Only this test's connections; the session's admin.app DB stays open for other tests.
    monkeypatch.setattr(dbmod, "_OPEN", weakref.WeakSet())
    _DbRole.instances.clear()

    # The admin app's module-level DB (init_schema=True at import) never closes itself.
    admin_db = dbmod.DBManager(str(db_file), init_schema=True)
    monkeypatch.setitem(sys.modules, "copycord.admin.app", types.SimpleNamespace(db=admin_db))

    async def admin_role():
        admin_db.set_config("ADMIN_WROTE", "1")
        await asyncio.Event().wait()

    def fake_build():
        return build_supervisor(
            {
                "admin": admin_role,
                "server": ThreadedRole("server", _DbRole),
                "client": ThreadedRole("client", _DbRole),
            },
            control_host="127.0.0.1",
            control_ports={"server": 0, "client": 0},
            backoff=Backoff(initial=0.01, maximum=0.04, reset_after=60.0),
        )

    async def no_update():
        await asyncio.Event().wait()

    monkeypatch.setattr(sup_mod, "build_supervisor", fake_build)
    monkeypatch.setattr(sup_mod, "_auto_update", no_update)
    monkeypatch.setattr(sup_mod, "_set_runtime_env", lambda: None)
    monkeypatch.setattr(sup_mod, "setup_logging", lambda: None)

    async def ctrl_c():
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10
        while not (len(_DbRole.instances) == 2 and all(r.ready for r in _DbRole.instances)):
            assert loop.time() < deadline, "roles did not open their databases"
            await asyncio.sleep(0.01)
        assert Path(str(db_file) + "-wal").exists()
        os.kill(os.getpid(), signal.SIGINT)

    async def main():
        await asyncio.wait_for(asyncio.gather(sup_mod.run(), ctrl_c()), 30)

    asyncio.run(main())

    assert not Path(str(db_file) + "-wal").exists()
    assert not Path(str(db_file) + "-shm").exists()
    for role in _DbRole.instances:
        for mgr in (role.cog_db, role.module_config.db):
            with pytest.raises(sqlite3.ProgrammingError):
                mgr.conn.execute("SELECT 1")
    conn = sqlite3.connect(str(db_file))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        keys = {r[0] for r in conn.execute("SELECT key FROM app_config")}
    finally:
        conn.close()
    assert {"ADMIN_WROTE", "ROLE_WROTE", "COG_WROTE"} <= keys


def test_close_all_closes_every_open_db(monkeypatch, tmp_db_path):
    import weakref

    from copycord.common import db as dbmod

    monkeypatch.setattr(dbmod, "_OPEN", weakref.WeakSet())
    a, b = dbmod.DBManager(tmp_db_path, init_schema=True), dbmod.DBManager(tmp_db_path)
    a.close()
    dbmod.close_all()
    dbmod.close_all()
    with pytest.raises(sqlite3.ProgrammingError):
        b.conn.execute("SELECT 1")
    assert len(dbmod._OPEN) == 0
    assert not Path(tmp_db_path + "-wal").exists()
    assert not Path(tmp_db_path + "-shm").exists()
