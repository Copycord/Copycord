"""
Tests for the supervisor's logging (src/copycord/logging_setup.py).
"""
import asyncio
import io
import logging
import threading

import pytest

from copycord.common.config import CURRENT_VERSION
from copycord import logging_setup as ls
from copycord import supervisor


@pytest.fixture()
def clean_root(monkeypatch):
    # tests/test_api.py sets LOG_LEVEL=WARNING for the whole session at import.
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    # Importing copycord.admin.app (other tests do) turns propagation off on the
    # "copycord" logger and may leave levels on role loggers; reset both.
    monkeypatch.setattr(logging.getLogger("copycord"), "propagate", True)
    names = ("copycord", "server", "server.server", "client", "client.client")
    saved_levels = {n: logging.getLogger(n).level for n in names}
    for n in names:
        logging.getLogger(n).setLevel(logging.NOTSET)
    root = logging.getLogger()
    saved, level = root.handlers[:], root.level
    root.handlers = []
    yield root
    for h in root.handlers:
        h.close()
    root.handlers = saved
    root.setLevel(level)
    for n, lvl in saved_levels.items():
        logging.getLogger(n).setLevel(lvl)


def _emit_on_thread(name, logger_name, msg, level=logging.INFO):
    t = threading.Thread(
        target=lambda: logging.getLogger(logger_name).log(level, msg), name=name
    )
    t.start()
    t.join()


def _read(tmp_path, role):
    p = tmp_path / f"{role}.out"
    return p.read_text(encoding="utf-8") if p.exists() else ""


def _setup(tmp_path, root):
    handlers = ls.setup_logging(tmp_path)
    buf = io.StringIO()
    console = next(h for h in root.handlers if getattr(h, ls.CONSOLE_MARK, False))
    console.setStream(buf)
    return handlers, buf


def test_records_route_by_thread_and_print_once(tmp_path, clean_root):
    _, buf = _setup(tmp_path, clean_root)
    _emit_on_thread("copycord-server", "common.websockets", "from-server")
    _emit_on_thread("copycord-client", "discord.gateway", "from-client")
    logging.getLogger("copycord.supervisor").info("from-admin")
    for h in clean_root.handlers:
        h.flush()

    server, client = _read(tmp_path, "server"), _read(tmp_path, "client")
    assert server.count("from-server") == 1 and "from-client" not in server
    assert client.count("from-client") == 1 and "from-server" not in client
    assert "from-admin" not in server + client
    console = buf.getvalue()
    assert console.count("from-admin") == 1
    assert "from-server" not in console and "from-client" not in console
    assert " | INFO  | from-server" in server
    assert "[server]" not in server


def test_role_warning_reaches_file_and_console_tagged(tmp_path, clean_root):
    _, buf = _setup(tmp_path, clean_root)
    _emit_on_thread("copycord-server", "server.server", "s-warn", logging.WARNING)
    _emit_on_thread("copycord-client", "client.client", "c-err", logging.ERROR)
    for h in clean_root.handlers:
        h.flush()
    assert "| WARNING | [server] s-warn" in buf.getvalue()
    assert "| ERROR | [client] c-err" in buf.getvalue()
    assert _read(tmp_path, "server").count("s-warn") == 1
    assert _read(tmp_path, "client").count("c-err") == 1


def test_admin_info_reaches_console_tagged(tmp_path, clean_root):
    _, buf = _setup(tmp_path, clean_root)
    _emit_on_thread("MainThread", "copycord.supervisor", "admin-info")
    _emit_on_thread("uvicorn-1", "copycord.supervisor", "uv-info")
    out = buf.getvalue()
    assert "| INFO  | [admin] admin-info" in out
    assert "[admin] uv-info" in out
    assert "admin-info" not in _read(tmp_path, "server") + _read(tmp_path, "client")


def test_startup_banner_prints_once(tmp_path, clean_root, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(supervisor, "_set_runtime_env", lambda: None)

    class _Sup:
        async def run(self, stop):
            # what the role modules log when they start
            _emit_on_thread(
                "copycord-server", "server.server", f"[x] Starting Copycord Server {CURRENT_VERSION}"
            )
            _emit_on_thread(
                "copycord-client", "client.client", f"[x] Starting Copycord Client {CURRENT_VERSION}"
            )

    monkeypatch.setattr(supervisor, "build_supervisor", lambda: _Sup())
    buf = io.StringIO()
    orig = ls.setup_logging

    def _setup_capture(*a, **kw):
        handlers = orig(*a, **kw)
        next(
            h for h in clean_root.handlers if getattr(h, ls.CONSOLE_MARK, False)
        ).setStream(buf)
        return handlers

    monkeypatch.setattr(supervisor, "setup_logging", _setup_capture)
    asyncio.run(supervisor.run())
    out = buf.getvalue()
    assert out.count(CURRENT_VERSION) == 1
    assert "[admin]" in out and "Starting Copycord" in out


def test_setup_is_idempotent(tmp_path, clean_root):
    _setup(tmp_path, clean_root)
    n = len(clean_root.handlers)
    ls.setup_logging(tmp_path)
    assert len(clean_root.handlers) == n


def test_existing_console_handler_is_reused(tmp_path, clean_root):
    ls.setup_logging(tmp_path, roles=())
    ls.setup_logging(tmp_path)
    consoles = [h for h in clean_root.handlers if getattr(h, ls.CONSOLE_MARK, False)]
    assert len(consoles) == 1


def test_clear_then_log_still_writes(tmp_path, clean_root):
    _setup(tmp_path, clean_root)
    _emit_on_thread("copycord-server", "server.server", "before-clear")
    with open(tmp_path / "server.out", "w", encoding="utf-8"):
        pass  # what POST /logs/clear does
    _emit_on_thread("copycord-server", "server.server", "after-clear")
    text = _read(tmp_path, "server")
    assert "before-clear" not in text
    assert text.count("after-clear") == 1


def test_size_stays_bounded(tmp_path, clean_root):
    ls.setup_logging(tmp_path, max_bytes=2000, backup_count=1)
    clean_root.setLevel(logging.INFO)
    for i in range(200):
        _emit_on_thread("copycord-client", "client.client", f"line {i} " + "x" * 50)
    assert (tmp_path / "client.out").stat().st_size < 2500
    assert (tmp_path / "client.out.1").exists()
