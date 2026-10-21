"""The dashboard's Log Level re-levels root, the console and the role file handlers."""
import asyncio
import logging
import os
import tempfile
import threading
from unittest.mock import AsyncMock

import pytest

_tmpdir = tempfile.mkdtemp()
os.environ.setdefault("DATA_DIR", _tmpdir)
os.environ.setdefault("DB_PATH", os.path.join(_tmpdir, "test.db"))

from copycord import logging_setup, supervisor  # noqa: E402
from httpx import AsyncClient, ASGITransport  # noqa: E402

_LOGGERS = ("websockets", "selfclient.gateway", "selfclient.science", "asyncio")


@pytest.fixture()
def installed(tmp_path, monkeypatch):
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    saved = {n: logging.getLogger(n).level for n in _LOGGERS}
    for h in saved_handlers:
        root.removeHandler(h)
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    root.setLevel(logging.NOTSET)
    handlers = logging_setup.setup_logging(tmp_path)
    yield tmp_path, handlers
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()
    for h in saved_handlers:
        root.addHandler(h)
    root.setLevel(saved_level)
    for n, lvl in saved.items():
        logging.getLogger(n).setLevel(lvl)


def _emit_on(role, name, msg):
    def go():
        logging.getLogger(name).debug(msg)

    t = threading.Thread(target=go, name=f"copycord-{role}")
    t.start()
    t.join()


def _file_levels(handlers):
    return {r: h.level for r, h in handlers.items()}


def test_apply_level_debug_then_info(installed):
    tmp, handlers = installed
    root = logging.getLogger()
    console = next(h for h in root.handlers if getattr(h, logging_setup.CONSOLE_MARK, False))
    assert root.level == logging.INFO

    logging_setup.apply_level("DEBUG")
    assert root.level == logging.DEBUG
    assert console.level == logging.DEBUG
    assert _file_levels(handlers) == {"server": logging.DEBUG, "client": logging.DEBUG}
    assert logging.getLogger("selfclient.gateway").level == logging.DEBUG
    _emit_on("client", "selfclient.gateway", "gateway-frame-seen")
    for h in handlers.values():
        h.flush()
    assert "gateway-frame-seen" in (tmp / "client.out").read_text(encoding="utf-8")

    logging_setup.apply_level("INFO")
    assert root.level == logging.INFO
    assert console.level == logging.INFO
    assert _file_levels(handlers) == {"server": logging.INFO, "client": logging.INFO}
    assert logging.getLogger("selfclient.gateway").level == logging.WARNING
    _emit_on("client", "selfclient.gateway", "gateway-frame-after")
    for h in handlers.values():
        h.flush()
    assert "gateway-frame-after" not in (tmp / "client.out").read_text(encoding="utf-8")


def test_debug_level_leaves_out_asyncio_selector_line(installed):
    tmp, handlers = installed
    logging.getLogger("asyncio").setLevel(logging.NOTSET)
    logging_setup.apply_level("DEBUG")
    assert logging.getLogger("asyncio").level == logging.WARNING

    def go():
        # A real loop logs "Using selector: ..." at DEBUG as it is made.
        asyncio.new_event_loop().close()
        logging.getLogger("asyncio").warning("asyncio-warning-kept")
        logging.getLogger("copycord.test").debug("own-debug-kept")

    t = threading.Thread(target=go, name="copycord-client")
    t.start()
    t.join()
    for h in handlers.values():
        h.flush()
    out = (tmp / "client.out").read_text(encoding="utf-8")
    assert "Using selector" not in out
    assert "asyncio-warning-kept" in out
    assert "own-debug-kept" in out


def test_apply_level_is_idempotent_and_reads_env(installed, monkeypatch):
    _, handlers = installed
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    assert logging_setup.apply_level() == logging.DEBUG
    assert logging_setup.apply_level() == logging.DEBUG
    assert _file_levels(handlers) == {"server": logging.DEBUG, "client": logging.DEBUG}
    assert logging_setup.apply_level("nonsense") == logging.INFO


@pytest.mark.asyncio
async def test_saving_the_level_in_the_dashboard_applies_it(installed, monkeypatch):
    from copycord.admin import auth
    import copycord.admin.app as app_mod

    _, handlers = installed
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_mod, "_verify_tokens_for_save", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        app_mod, "_ws_cmd", AsyncMock(return_value={"ok": True, "running": False})
    )
    client = AsyncClient(
        transport=ASGITransport(app=app_mod.app),
        base_url="http://test",
        headers={"Origin": "http://test", "X-Copycord-CSRF": "1"},
        cookies={auth.ADMIN_COOKIE_NAME: auth.make_session_token()},
        follow_redirects=False,
    )
    root = logging.getLogger()
    base = {"SERVER_TOKEN": "srv-tok", "CLIENT_TOKEN": "cli-tok"}

    resp = await client.post("/save", data={**base, "LOG_LEVEL": "DEBUG"})
    assert resp.status_code == 303
    assert root.level == logging.DEBUG
    assert _file_levels(handlers) == {"server": logging.DEBUG, "client": logging.DEBUG}

    resp = await client.post("/save", data={**base, "LOG_LEVEL": "INFO"})
    assert resp.status_code == 303
    assert root.level == logging.INFO
    assert _file_levels(handlers) == {"server": logging.INFO, "client": logging.INFO}


def test_startup_follows_the_db_level_over_env(installed, monkeypatch, tmp_path):
    from copycord.common.db import DBManager

    db_path = str(tmp_path / "startup.db")
    db = DBManager(db_path, init_schema=True)
    db.set_config("LOG_LEVEL", "DEBUG")
    db.close()
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("LOG_LEVEL", "INFO")

    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()
    root.setLevel(logging.NOTSET)

    supervisor._apply_db_log_level()
    handlers = logging_setup.setup_logging(tmp_path / "logs")

    assert os.environ["LOG_LEVEL"] == "DEBUG"
    assert root.level == logging.DEBUG
    assert _file_levels(handlers) == {"server": logging.DEBUG, "client": logging.DEBUG}


def test_apply_db_config_re_levels_before_a_role_starts(installed, monkeypatch, tmp_path):
    from copycord.common.db import DBManager

    _, handlers = installed
    db_path = str(tmp_path / "role.db")
    db = DBManager(db_path, init_schema=True)
    db.set_config("LOG_LEVEL", "DEBUG")
    db.close()
    monkeypatch.setenv("DB_PATH", db_path)

    supervisor._apply_db_config()
    assert logging.getLogger().level == logging.DEBUG
    assert _file_levels(handlers) == {"server": logging.DEBUG, "client": logging.DEBUG}
