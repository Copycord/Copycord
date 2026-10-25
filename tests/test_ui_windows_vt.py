"""On a Windows console that cannot do VT escape codes (the classic cmd.exe of
older Windows) the CLI prints plain text: no colors, no links, no spinner and
the typed prompts, never a raw escape code."""

import atexit
import builtins
import ctypes
import io
import sys
from types import SimpleNamespace

import pytest

from copycord import ui


class Tty(io.StringIO):
    encoding = "utf-8"

    def isatty(self):
        return True


@pytest.fixture
def console(monkeypatch):
    """A terminal on stdin, stdout and stderr in Windows Terminal; vt(False)
    makes it one where VT processing cannot be turned on."""
    for name in ("NO_COLOR", "FORCE_COLOR", "COPYCORD_PLAIN_PROMPTS", "TMUX", "STY",
                 "SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("WT_SESSION", "1")
    monkeypatch.setattr(ui, "_rail", None)
    out, err = Tty(), Tty()
    monkeypatch.setattr(sys, "stdin", Tty())
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", err)

    def vt(ok):
        monkeypatch.setattr(ui, "windows_vt", lambda stream=None: ok)
        return out, err

    return vt


def _print_everything(out):
    ui.ok("installed", "see https://example.com/docs", stream=out)
    ui.warn("check this", stream=out)
    ui.info(ui.link("https://example.com", out), stream=out)
    ui.box(["one line", "https://example.com/more"], title="Note", stream=out)
    print(ui.brand_line("4.0.0", "abcdef1234", out), file=out)
    ui.heading("Heading", out)
    with ui.Rail("Update Copycord", out):
        ui.note("on the rail", out)
        with ui.section("Install", out):
            ui.ok("step")


def test_no_escape_codes_without_vt(console):
    out, err = console(False)
    assert ui.use_color(out) is False
    assert ui.hyperlinks(out) is False
    _print_everything(out)
    assert "installed" in out.getvalue()
    assert "\x1b" not in out.getvalue()
    assert "\x1b" not in err.getvalue()


def test_escape_codes_with_vt(console):
    out, _ = console(True)
    assert ui.use_color(out) is True
    _print_everything(out)
    assert "\x1b[" in out.getvalue()


def test_force_color_still_wins(console, monkeypatch):
    out, _ = console(False)
    monkeypatch.setenv("FORCE_COLOR", "1")
    assert ui.use_color(out) is True


def test_no_color_still_wins(console, monkeypatch):
    out, _ = console(True)
    monkeypatch.setenv("NO_COLOR", "1")
    assert ui.use_color(out) is False


def test_spinner_stays_off_without_vt(console, monkeypatch):
    _, err = console(False)
    # Not even FORCE_COLOR turns on a spinner that redraws with escape codes.
    monkeypatch.setenv("FORCE_COLOR", "1")
    with ui.Spinner("working", err) as spin:
        assert spin.active is False
    assert err.getvalue() == ""


def test_prompts_are_typed_without_vt(console, monkeypatch):
    out, _ = console(False)
    assert ui.raw_keys_available() is False
    asked = []
    answers = iter(["", "2"])

    def fake_input(prompt=""):
        asked.append(prompt)
        out.write(prompt)
        return next(answers)

    monkeypatch.setattr(builtins, "input", fake_input)
    assert ui.confirm("Update Copycord now?") is True
    assert ui.choose("Which one?", ["A", "B"]) == 1
    assert "(Y/n)" in asked[0]
    assert "\x1b" not in out.getvalue()


class Kernel32:
    """GetStdHandle / GetConsoleMode / SetConsoleMode of a console; hold=False
    takes SetConsoleMode but keeps the VT flag off."""

    def __init__(self, mode=0x0003, console=True, accept=True, hold=True):
        self.mode = mode
        self.console = console
        self.accept = accept
        self.hold = hold
        self.sets = []

    def GetStdHandle(self, handle_id):
        return 100 + -handle_id

    def GetConsoleMode(self, handle, ref):
        if not self.console:
            return 0
        ref._obj.value = self.mode
        return 1

    def SetConsoleMode(self, handle, mode):
        self.sets.append(mode)
        if not self.accept:
            return 0
        self.mode = mode if self.hold else mode & ~0x0004
        return 1


@pytest.fixture
def kernel32(monkeypatch):
    exits = []
    monkeypatch.setattr(atexit, "register", lambda *a: exits.append(a))

    def make(**kw):
        k = Kernel32(**kw)
        monkeypatch.setattr(ctypes, "windll", SimpleNamespace(kernel32=k), raising=False)
        return k, exits

    return make


def test_vt_is_turned_on_and_restored_at_exit(kernel32):
    k, exits = kernel32()
    assert ui._enable_windows_vt(-11) is True
    assert k.mode == 0x0007
    assert exits == [(k.SetConsoleMode, 111, 0x0003)]


def test_vt_already_on(kernel32):
    k, exits = kernel32(mode=0x0007)
    assert ui._enable_windows_vt(-12) is True
    assert k.sets == [] and exits == []


@pytest.mark.parametrize("kw", [
    {"console": False},
    {"accept": False},
    {"hold": False},
])
def test_no_vt_leaves_the_mode_as_it_was(kernel32, kw):
    k, exits = kernel32(**kw)
    assert ui._enable_windows_vt(-11) is False
    assert k.mode == 0x0003
    assert exits == []


def test_no_windll_is_no_vt(monkeypatch):
    monkeypatch.delattr(ctypes, "windll", raising=False)
    assert ui._enable_windows_vt(-11) is False


def test_windows_vt_is_tried_once_per_handle(monkeypatch):
    tried = []
    for name in ("NO_COLOR", "FORCE_COLOR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setattr(ui.os, "name", "nt")
    monkeypatch.setattr(ui, "_vt_state", {})
    monkeypatch.setattr(ui, "_enable_windows_vt", lambda h: tried.append(h) or False)
    out = Tty()
    assert ui.windows_vt(out) is False
    assert ui.windows_vt(out) is False
    assert ui.use_color(out) is False
    assert tried == [-11]


def test_windows_vt_off_windows(monkeypatch):
    monkeypatch.setattr(ui.os, "name", "posix")
    monkeypatch.setattr(ui, "_enable_windows_vt", lambda h: pytest.fail("tried"))
    assert ui.windows_vt(Tty()) is True
