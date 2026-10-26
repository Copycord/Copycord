"""The dashboard's Log Level re-levels root, the console and the role file handlers."""
import logging
import threading

import pytest

from copycord import logging_setup

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


def test_trace_lines_stay_hidden_at_debug(installed):
    tmp, handlers = installed
    logging_setup.apply_level("DEBUG")
    assert logging_setup.TRACE < logging.DEBUG

    def go():
        gw = logging.getLogger("selfclient.gateway")
        gw.debug("gateway-debug-visible")
        gw.log(logging_setup.TRACE, "gateway-trace-hidden")

    t = threading.Thread(target=go, name="copycord-client")
    t.start()
    t.join()
    for h in handlers.values():
        h.flush()
    text = (tmp / "client.out").read_text(encoding="utf-8")
    assert "gateway-debug-visible" in text
    assert "gateway-trace-hidden" not in text


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
