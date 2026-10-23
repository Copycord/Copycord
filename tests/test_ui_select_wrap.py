"""The arrow-key selector redraws in place when its lines are wider than the
terminal: every redraw goes back over all the rows the last frame wrapped onto,
so the question shows once and the answer row stays under it."""

import os
import re
import sys
from types import SimpleNamespace

import pytest

from copycord import ui

LONG = (
    "Downgrade from v4.0.0.8.dev5+gcfb0832e8 to v4.0.0.7? Your data is backed "
    "up first; a newer database schema will block the older version until you "
    "restore the backup."
)
SHORT = "Continue?"
_CODE = re.compile(r"\033\[([0-9;?]*)([A-Za-z])")


class Screen:
    """A terminal of a fixed width, enough of one for the selector: text with
    the wrap at the last column, \\r, \\n, cursor up, erase below. Colors and
    the cursor show/hide codes do nothing."""

    def __init__(self, columns: int):
        self.columns = columns
        self.rows = [[]]
        self.row = 0
        self.col = 0
        self.ups = []
        # The row the cursor is on after each erase below.
        self.cleared_at = []

    def _line(self):
        while len(self.rows) <= self.row:
            self.rows.append([])
        return self.rows[self.row]

    def feed(self, data: str) -> None:
        i = 0
        while i < len(data):
            m = _CODE.match(data, i)
            if m:
                self._code(m.group(1), m.group(2))
                i = m.end()
                continue
            ch = data[i]
            i += 1
            if ch == "\r":
                self.col = 0
            elif ch == "\n":
                self.row += 1
                self.col = 0
                self._line()
            else:
                if self.col >= self.columns:
                    # The cursor waits at the last column; the next character
                    # goes to the start of the next row.
                    self.row += 1
                    self.col = 0
                line = self._line()
                while len(line) <= self.col:
                    line.append(" ")
                line[self.col] = ch
                self.col += 1

    def _code(self, arg: str, op: str) -> None:
        if op == "A":
            n = int(arg or "1")
            self.ups.append(n)
            self.row = max(0, self.row - n)
        elif op == "J":
            line = self._line()
            del line[self.col:]
            del self.rows[self.row + 1:]
            self.cleared_at.append(self.row)

    def text(self):
        lines = ["".join(x).rstrip() for x in self.rows]
        while lines and not lines[-1]:
            lines.pop()
        return lines


class FakeOut:
    encoding = "utf-8"

    def __init__(self, screen: Screen):
        self.screen = screen
        self.written = []

    def write(self, data: str) -> int:
        self.written.append(data)
        self.screen.feed(data)
        return len(data)

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return True


class NoRaw:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


@pytest.fixture
def term(monkeypatch):
    """A 60 column terminal on stdout; keys(...) scripts the keys pressed and
    records the screen as it is when each key is read."""
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setattr(ui, "_rail", None)
    state = SimpleNamespace(columns=60, frames=[])

    def setup(keys, columns=60, color=False, rail=False):
        if color:
            monkeypatch.delenv("NO_COLOR", raising=False)
            monkeypatch.setenv("FORCE_COLOR", "1")
        if rail:
            monkeypatch.setattr(ui, "_rail", SimpleNamespace(active=True))
        screen = Screen(columns)
        out = FakeOut(screen)
        monkeypatch.setattr(sys, "stdout", out)
        monkeypatch.setattr(
            ui.shutil, "get_terminal_size",
            lambda fallback=(80, 24): os.terminal_size((columns, 24)),
        )
        monkeypatch.setattr(ui, "raw_keys_available", lambda: True)
        monkeypatch.setattr(ui, "_RawMode", NoRaw)
        pending = list(keys)

        def read_key():
            state.frames.append(screen.text())
            return pending.pop(0)

        monkeypatch.setattr(ui, "_read_key", read_key)
        state.screen, state.out = screen, out
        return state

    return setup


def _count(lines, needle):
    return sum(needle in x for x in lines)


@pytest.mark.parametrize("color", [False, True])
def test_long_confirm_redraws_in_place(term, color):
    t = term(["right", "left", "right", "left", "right", "enter"], color=color)
    assert ui.confirm(LONG, default=False) is False

    # While choosing, the screen holds the question once and the Yes / No row.
    assert len(t.frames) == 6
    for frame in t.frames:
        assert _count(frame, "Downgrade from") == 1
        assert _count(frame, "restore the backup.") == 1
        assert "Yes" in frame[-1] and "No" in frame[-1]
        assert all(len(x) <= 60 for x in frame)

    # Every redraw went back to the first row of the frame before it.
    assert t.screen.cleared_at and set(t.screen.cleared_at) == {0}
    assert t.screen.ups and all(n > 0 for n in t.screen.ups)

    # The answer collapses to the question once, then the answer.
    final = t.screen.text()
    assert _count(final, "Downgrade from") == 1
    assert _count(final, "restore the backup.") == 1
    assert final[-1].endswith("No")
    assert "❯" not in "".join(final)


def test_long_confirm_on_the_rail_keeps_the_rail(term):
    t = term(["left", "right", "y"], rail=True)
    assert ui.confirm(LONG, default=True) is True
    for frame in t.frames:
        assert _count(frame, "Downgrade from") == 1
        assert frame[0].startswith("◆  ")
        assert all(x.startswith("│  ") for x in frame[1:])
        assert "Yes" in frame[-1] and "No" in frame[-1]
    assert set(t.screen.cleared_at) == {0}
    final = t.screen.text()
    assert _count(final, "Downgrade from") == 1
    assert final[0].startswith("◇  ")
    assert final[-1] == "│  Yes"


def test_cursor_up_matches_the_rows_of_the_last_frame(term):
    t = term(["right", "right", "right", "enter"])
    ui.confirm(LONG)
    # The question wraps onto several rows and the answer has one of its own;
    # each of the 4 redraws and the collapse goes up all but the last of them.
    rows = len(t.frames[0])
    assert rows >= 4
    assert all(len(frame) == rows for frame in t.frames)
    assert t.screen.ups == [rows - 1] * 5


def test_word_wider_than_the_terminal_is_counted(term):
    long_word = "x" * 130
    t = term(["right", "left", "enter"], columns=60)
    assert ui.confirm(f"Use {long_word}?", default=True) is True
    for frame in t.frames:
        assert _count(frame, "? Use") == 1
        assert sum(x.count("x") for x in frame) == 130
        assert "Yes" in frame[-1] and "No" in frame[-1]
    assert set(t.screen.cleared_at) == {0}
    final = t.screen.text()
    assert _count(final, "Use") == 1
    assert sum(x.count("x") for x in final) == 130
    assert final[-1].endswith("Yes")


def test_long_choose_redraws_in_place(term):
    options = ["Keep my data and remove only the program files from this computer",
               "Remove everything"]
    t = term(["down", "up", "down", "enter"], columns=40, rail=True)
    assert ui.choose(LONG, options) == 1
    for frame in t.frames:
        assert _count(frame, "Downgrade from") == 1
        assert _count(frame, "Remove everything") == 1
        assert "Enter to select" in frame[-1]
        assert all(len(x) <= 40 for x in frame)
    assert set(t.screen.cleared_at) == {0}
    final = t.screen.text()
    assert _count(final, "Downgrade from") == 1
    assert final[-1] == "│  Remove everything"


def test_short_confirm_is_unchanged(term):
    t = term(["right", "enter"])
    assert ui.confirm(SHORT, default=True) is False
    assert t.frames[0] == ["? Continue?  ❯ Yes / No"]
    assert t.frames[1] == ["? Continue?  Yes / ❯ No"]
    # One row a frame: each redraw only returns to the start of the row.
    assert t.screen.ups == []
    assert t.out.written.count("\r\033[J") == 3
    assert t.screen.text() == ["✓ Continue? No"]


def test_cells_counts_terminal_columns():
    assert ui._cells("◆  │ ❯ ↑/↓ ◇") == 11
    assert ui._cells("\033[1mab\033[0m") == 2
    assert ui._cells("日本") == 4
    assert ui._cells("é") == 1
