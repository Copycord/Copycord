"""
Tests for single-process logging (code/copycord/logging_setup.py).
"""
import io
import logging
import threading

import pytest

from copycord import logging_setup as ls


@pytest.fixture()
def clean_root(monkeypatch):
    # tests/test_api.py sets LOG_LEVEL=WARNING for the whole session at import.
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    root = logging.getLogger()
    saved, level = root.handlers[:], root.level
    root.handlers = []
    yield root
    for h in root.handlers:
        h.close()
    root.handlers = saved
    root.setLevel(level)


def _emit_on_thread(name, logger_name, msg):
    t = threading.Thread(
        target=lambda: logging.getLogger(logger_name).info(msg), name=name
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
    for msg in ("from-server", "from-client", "from-admin"):
        assert console.count(msg) == 1
    assert " | INFO  | from-server" in server


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
