"""The arrow-key prompts in copycord.ui and their typed fallback."""

import io
import re
import sys

import pytest

from copycord import ui

OPTIONS = ["Keep my data", "Delete everything", "Cancel"]
_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


@pytest.fixture()
def keys(monkeypatch):
    """A console whose key presses are given per test."""
    state = {"keys": [], "raw": []}

    class FakeRaw:
        def __enter__(self):
            state["raw"].append("enter")
            return self

        def __exit__(self, *exc):
            state["raw"].append("exit")

    def read():
        if not state["keys"]:
            pytest.fail("read a key with none left")
        key = state["keys"].pop(0)
        if isinstance(key, BaseException):
            raise key
        return key

    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr(ui, "raw_keys_available", lambda: True)
    monkeypatch.setattr(ui, "_RawMode", FakeRaw)
    monkeypatch.setattr(ui, "_read_key", read)
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("typed prompt used"))
    return state


def _screen(capsys):
    return _ANSI.sub("", capsys.readouterr().out)


def test_choose_enter_picks_the_default(keys, capsys):
    keys["keys"] = ["enter"]
    assert ui.choose("Your data is in /x", OPTIONS, default=0) == 0
    assert keys["raw"] == ["enter", "exit"]
    out = _screen(capsys)
    assert "❯ Keep my data" in out or "> Keep my data" in out
    assert out.rstrip().endswith("Your data is in /x Keep my data")


def test_choose_down_down_enter_picks_cancel(keys, capsys):
    keys["keys"] = ["down", "down", "enter"]
    assert ui.choose("Your data is in /x", OPTIONS) == 2
    assert _screen(capsys).rstrip().endswith("Your data is in /x Cancel")


def test_choose_up_wraps_and_kj_move(keys):
    keys["keys"] = ["up", "enter"]
    assert ui.choose("q", OPTIONS) == 2
    keys["keys"] = ["j", "j", "k", "enter"]
    assert ui.choose("q", OPTIONS) == 1


def test_choose_number_key_picks(keys):
    keys["keys"] = ["2"]
    assert ui.choose("q", OPTIONS) == 1


def test_choose_esc_cancels(keys, capsys):
    keys["keys"] = ["down", "esc"]
    with pytest.raises(KeyboardInterrupt):
        ui.choose("q", OPTIONS)
    assert keys["raw"] == ["enter", "exit"]
    out = capsys.readouterr().out
    assert out.count(ui._HIDE) == 1 and out.endswith(ui._SHOW)


def test_choose_ctrl_c_cancels(keys):
    keys["keys"] = [KeyboardInterrupt()]
    with pytest.raises(KeyboardInterrupt):
        ui.choose("q", OPTIONS)
    assert keys["raw"] == ["enter", "exit"]


def test_choose_falls_back_when_key_input_breaks(keys, monkeypatch, capsys):
    keys["keys"] = ["down", OSError("console went away")]
    monkeypatch.setattr("builtins.input", lambda *_: "3")
    assert ui.choose("q", OPTIONS) == 2
    assert keys["raw"] == ["enter", "exit"]
    out = capsys.readouterr().out
    assert out.count(ui._SHOW) == 1 and "3) Cancel" in out


def test_confirm_enter_takes_the_default(keys, capsys):
    keys["keys"] = ["enter"]
    assert ui.confirm("Update Copycord now?") is True
    out = _screen(capsys)
    assert "Update Copycord now?  ❯ Yes / No" in out
    assert out.rstrip().endswith("✓ Update Copycord now? Yes")
    keys["keys"] = ["enter"]
    assert ui.confirm("Fix it?", default=False) is False


def test_confirm_arrows_switch(keys):
    keys["keys"] = ["right", "enter"]
    assert ui.confirm("q") is False
    keys["keys"] = ["down", "up", "left", "enter"]
    assert ui.confirm("q") is False


def test_confirm_y_and_n_pick(keys):
    keys["keys"] = ["n"]
    assert ui.confirm("q", default=True) is False
    keys["keys"] = ["y"]
    assert ui.confirm("q", default=False) is True


def test_confirm_esc_cancels(keys):
    keys["keys"] = ["esc"]
    with pytest.raises(KeyboardInterrupt):
        ui.confirm("q")


def test_no_color_keeps_the_selector_without_codes(keys, monkeypatch, capsys):
    monkeypatch.setenv("NO_COLOR", "1")
    keys["keys"] = ["n"]
    assert ui.confirm("Update Copycord now?") is False
    out = capsys.readouterr().out
    assert "\x1b[38;5;99" not in out and "Update Copycord now? No" in out


def test_typed_fallback_without_a_terminal(monkeypatch, capsys):
    monkeypatch.setattr(ui, "_read_key", lambda: pytest.fail("read raw keys"))
    answers = iter(["", "3", "y"])
    monkeypatch.setattr("builtins.input", lambda *_: next(answers))
    assert ui.raw_keys_available() is False  # pytest's stdin is not a TTY
    assert ui.confirm("q", default=False) is False
    assert ui.choose("q", OPTIONS) == 2
    assert ui.confirm("q") is True
    assert "1) Keep my data (default)" in capsys.readouterr().out


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX console check")
@pytest.mark.parametrize("env", [{"TERM": "dumb"}, {"COPYCORD_PLAIN_PROMPTS": "1"}])
def test_raw_keys_off_by_env(monkeypatch, env):
    class Tty:
        def isatty(self):
            return True

    monkeypatch.setattr(sys, "stdin", Tty())
    monkeypatch.setattr(sys, "stdout", Tty())
    monkeypatch.delenv("COPYCORD_PLAIN_PROMPTS", raising=False)
    monkeypatch.setenv("TERM", "xterm")
    assert ui.raw_keys_available() is True
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert ui.raw_keys_available() is False


@pytest.mark.skipif(sys.platform == "win32", reason="termios")
def test_raw_mode_restores_the_terminal_on_error(monkeypatch):
    import termios

    calls = []
    monkeypatch.setattr(sys.stdin, "fileno", lambda: 0, raising=False)
    monkeypatch.setattr(termios, "tcgetattr", lambda fd: [0, 0, 0, 0xFFFF, 0, 0, [0] * 32])
    monkeypatch.setattr(termios, "tcsetattr", lambda fd, when, attrs: calls.append(attrs[3]))
    with pytest.raises(RuntimeError):
        with ui._RawMode():
            raise RuntimeError("boom")
    assert len(calls) == 2 and calls[-1] == 0xFFFF


@pytest.mark.skipif(sys.platform == "win32", reason="termios")
def test_raw_mode_failure_means_fallback(monkeypatch):
    import termios

    def broken(fd):
        raise termios.error("not a tty")

    monkeypatch.setattr(sys.stdin, "fileno", lambda: 0, raising=False)
    monkeypatch.setattr(termios, "tcgetattr", broken)
    monkeypatch.setattr(ui, "raw_keys_available", lambda: True)
    monkeypatch.setattr("builtins.input", lambda *_: "n")
    assert ui.confirm("q") is False


# --------------------------------------------------------------------------
# prompts on the step rail
# --------------------------------------------------------------------------


class TTY(io.StringIO):
    encoding = "utf-8"

    def isatty(self):
        return True


def _tty_out(monkeypatch, out=None):
    """Make sys.stdout a terminal, so a rail opened on it is shown. Called in the
    test itself: pytest puts its own sys.stdout back after the fixtures ran."""
    out = TTY() if out is None else out
    monkeypatch.setattr(sys, "stdout", out)
    return out


def _rail_screen(out):
    return _ANSI.sub("", out.getvalue())


def test_choose_on_the_rail(keys, monkeypatch):
    tty_out = _tty_out(monkeypatch)
    keys["keys"] = ["down", "enter"]
    with ui.Rail("Uninstall Copycord"):
        assert ui.choose("Your data is in /x", OPTIONS) == 1
    out = _rail_screen(tty_out)
    assert ("◆  Your data is in /x\n│  ❯ Keep my data\n│    Delete everything\n"
            "│    Cancel\n│    ↑/↓ to move, Enter to select") in out
    assert out.endswith("◇  Your data is in /x\n│  Delete everything\n└\n")


def test_confirm_on_the_rail(keys, monkeypatch):
    tty_out = _tty_out(monkeypatch)
    keys["keys"] = ["enter"]
    with ui.Rail("Updating Copycord"):
        assert ui.confirm("Update Copycord now?") is True
    out = _rail_screen(tty_out)
    assert "◆  Update Copycord now?\n│  ❯ Yes / No" in out
    assert out.endswith("◇  Update Copycord now?\n│  Yes\n└\n")


def test_cancel_on_the_rail(keys, monkeypatch):
    tty_out = _tty_out(monkeypatch)
    keys["keys"] = ["esc"]
    with pytest.raises(KeyboardInterrupt):
        with ui.Rail("Uninstall Copycord"):
            ui.choose("Your data is in /x", OPTIONS)
    assert _rail_screen(tty_out).endswith("✗  Your data is in /x\n│  cancelled\n└\n")


def test_prompts_off_the_rail_are_unchanged(keys, monkeypatch):
    tty_out = _tty_out(monkeypatch)
    keys["keys"] = ["enter"]
    assert ui.confirm("Update Copycord now?") is True
    out = _rail_screen(tty_out)
    assert "? Update Copycord now?  ❯ Yes / No" in out
    assert out.endswith("✓ Update Copycord now? Yes\n")


def test_typed_prompts_on_the_rail(monkeypatch):
    tty_out = _tty_out(monkeypatch)
    monkeypatch.setattr(ui, "raw_keys_available", lambda: False)
    asked = []
    answers = iter(["3", ""])
    monkeypatch.setattr("builtins.input", lambda prompt="": asked.append(_ANSI.sub("", prompt)) or next(answers))
    with ui.Rail("Uninstall Copycord"):
        assert ui.choose("Your data is in /x", OPTIONS) == 2
        assert ui.confirm("Update Copycord now?") is True
    out = _rail_screen(tty_out)
    assert ("◆  Your data is in /x\n│  1) Keep my data (default)\n│  2) Delete everything\n"
            "│  3) Cancel\n") in out
    assert asked == ["│  Enter 1, 2 or 3 [1] ", "◆  Update Copycord now? (Y/n) "]


def test_rail_prompts_fall_back_to_ascii(keys, monkeypatch):
    class AsciiTTY(TTY):
        encoding = "ascii"

    out = _tty_out(monkeypatch, AsciiTTY())
    keys["keys"] = ["enter"]
    with ui.Rail("Uninstall Copycord"):
        ui.choose("Your data is in /x", OPTIONS)
    screen = _rail_screen(out)
    assert "*  Your data is in /x\n|  > Keep my data\n|    Delete everything" in screen
    assert screen.endswith("o  Your data is in /x\n|  Keep my data\n+\n")
