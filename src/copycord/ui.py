# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Terminal styling for the copycord CLI, plain ANSI.

Color follows NO_COLOR, FORCE_COLOR, TERM and whether the stream is a TTY.
Without color the layout and symbols stay the same; the symbols fall back to
ASCII only where the stream's encoding cannot print them.
"""

from __future__ import annotations

import itertools
import os
import sys
import threading
import time
from typing import Iterable, List, Optional, TextIO

RESET = "\033[0m"
_CODES = {
    "bold": "1",
    "dim": "2",
    "red": "31",
    "green": "32",
    "yellow": "33",
    "blue": "34",
    "magenta": "35",
    "cyan": "36",
    "gray": "90",
    "accent": "38;5;99",  # Copycord purple
}

LOGO = (
    "  ___                             _ ",
    " / __|___ _ __ _  _ __ ___ _ _ __| |",
    "| (__/ _ \\ '_ \\ || / _/ _ \\ '_/ _` |",
    " \\___\\___/ .__/\\_, \\__\\___/_| \\__,_|",
    "         |_|   |__/                 ",
)
TAGLINE = "Ultimate Discord server mirroring and utility toolkit"


def use_color(stream: Optional[TextIO] = None) -> bool:
    stream = sys.stdout if stream is None else stream
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR") not in (None, "", "0"):
        return True
    if os.environ.get("TERM") == "dumb":
        return False
    try:
        return stream.isatty()
    except (AttributeError, ValueError):
        return False


def is_interactive() -> bool:
    try:
        return sys.stdin.isatty() and sys.stdout.isatty()
    except (AttributeError, ValueError):
        return False


def _unicode(stream: TextIO) -> bool:
    enc = (getattr(stream, "encoding", None) or "").lower()
    return "utf" in enc


def _encodable(text: str, stream: TextIO) -> bool:
    enc = getattr(stream, "encoding", None) or "utf-8"
    try:
        text.encode(enc)
    except (LookupError, UnicodeEncodeError):
        return False
    return True


def dot(stream: Optional[TextIO] = None) -> str:
    """The separator in "dev channel · v4-dev"."""
    stream = sys.stdout if stream is None else stream
    return " · " if _encodable("·", stream) else " - "


def paint(text: str, *styles: str, stream: Optional[TextIO] = None) -> str:
    if not styles or not use_color(stream):
        return text
    codes = ";".join(_CODES[s] for s in styles)
    return f"\033[{codes}m{text}{RESET}"


def bold(text: str, stream=None) -> str:
    return paint(text, "bold", stream=stream)


def dim(text: str, stream=None) -> str:
    return paint(text, "dim", stream=stream)


def accent(text: str, stream=None) -> str:
    return paint(text, "accent", "bold", stream=stream)


def cmd(text: str, stream=None) -> str:
    """A command the user can copy, e.g. copycord update; plain without color."""
    return paint(text, "cyan", stream=stream)


# --------------------------------------------------------------------------
# step lines
# --------------------------------------------------------------------------

# status -> (color, symbol, ASCII fallback)
_MARKS = {
    "ok": ("green", "✓", "+"),
    "warn": ("yellow", "!", "!"),
    "fail": ("red", "✗", "x"),
    "info": ("blue", "•", "-"),
}


def mark(status: str, stream: Optional[TextIO] = None) -> str:
    stream = sys.stdout if stream is None else stream
    color, symbol, ascii_symbol = _MARKS[status]
    if not _encodable(symbol, stream):
        symbol = ascii_symbol
    return paint(symbol, color, "bold", stream=stream)


def line(status: str, text: str, detail: str = "", stream: Optional[TextIO] = None) -> None:
    """One status line: "✓ label  detail"."""
    stream = sys.stdout if stream is None else stream
    tail = f"  {dim(detail, stream)}" if detail else ""
    print(f"{mark(status, stream)} {text}{tail}", file=stream)


def ok(text, detail="", stream=None):
    line("ok", text, detail, stream)


def warn(text, detail="", stream=None):
    line("warn", text, detail, stream if stream is not None else sys.stderr)


def fail(text, detail="", stream=None):
    line("fail", text, detail, stream if stream is not None else sys.stderr)


def info(text, detail="", stream=None):
    line("info", text, detail, stream)


def attention(items: Iterable[tuple], stream: Optional[TextIO] = None) -> None:
    """The "Needs your attention" section: one item per (status, text, hint)."""
    stream = sys.stdout if stream is None else stream
    items = list(items)
    if not items:
        return
    heading("Needs your attention", stream)
    for status, text, hint in items:
        line(status, text, hint, stream)


def kv(rows: Iterable[tuple], stream: Optional[TextIO] = None, width: int = 0) -> None:
    """Aligned key and value rows."""
    stream = sys.stdout if stream is None else stream
    rows = list(rows)
    width = width or max((len(k) for k, _ in rows), default=0)
    for k, v in rows:
        print(f"  {dim(k.ljust(width), stream)}  {v}", file=stream)


# --------------------------------------------------------------------------
# headings and boxes
# --------------------------------------------------------------------------


def heading(text: str, stream: Optional[TextIO] = None) -> None:
    stream = sys.stdout if stream is None else stream
    print(f"\n{accent(text, stream)}", file=stream)


def _visible_len(text: str) -> int:
    out, skip = 0, False
    for ch in text:
        if ch == "\033":
            skip = True
        elif skip and ch == "m":
            skip = False
        elif not skip:
            out += 1
    return out


def box(lines: Iterable[str], title: str = "", status: str = "info",
        stream: Optional[TextIO] = None) -> None:
    """A boxed notice."""
    stream = sys.stdout if stream is None else stream
    lines = list(lines)
    fancy = use_color(stream) and _unicode(stream)
    tl, tr, bl, br, h, v = (
        ("╭", "╮", "╰", "╯", "─", "│")
        if fancy else ("+", "+", "+", "+", "-", "|")
    )
    color = _MARKS[status][0]
    width = max([_visible_len(x) for x in lines] + [len(title) + 2, 20])
    edge = lambda s: paint(s, color, stream=stream)  # noqa: E731
    top = f" {title} " if title else ""
    print(edge(tl + h + top + h * (width + 1 - len(top)) + tr), file=stream)
    for x in lines:
        pad = " " * (width - _visible_len(x))
        print(f"{edge(v)} {x}{pad} {edge(v)}", file=stream)
    print(edge(bl + h * (width + 2) + br), file=stream)


# --------------------------------------------------------------------------
# spinner
# --------------------------------------------------------------------------


class Spinner:
    """Context manager that shows a spinner on a TTY and nothing otherwise."""

    FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

    def __init__(self, text: str, stream: Optional[TextIO] = None):
        self.text = text
        self.stream = sys.stderr if stream is None else stream
        self.active = use_color(self.stream) and _unicode(self.stream)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _spin(self) -> None:
        for frame in itertools.cycle(self.FRAMES):
            if self._stop.is_set():
                break
            self.stream.write(f"\r{paint(frame, 'accent', stream=self.stream)} {self.text}")
            self.stream.flush()
            time.sleep(0.08)

    def __enter__(self) -> "Spinner":
        if self.active:
            self._thread = threading.Thread(target=self._spin, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        if self._thread is not None:
            self._stop.set()
            self._thread.join()
            self.stream.write("\r\033[2K")
            self.stream.flush()


# --------------------------------------------------------------------------
# prompts
# --------------------------------------------------------------------------


def ask(question: str, default: str = "", secret: bool = False) -> str:
    hint = f" {dim('[' + default + ']')}" if default and not secret else ""
    prompt = f"{paint('?', 'accent', 'bold')} {bold(question)}{hint} "
    if secret:
        import getpass

        value = getpass.getpass(prompt)
    else:
        value = input(prompt)
    return value.strip() or default


def _choose_typed(question: str, options: List[str], default: int) -> int:
    print(f"{paint('?', 'accent', 'bold')} {bold(question)}")
    for i, option in enumerate(options, 1):
        tail = f" {dim('(default)')}" if i - 1 == default else ""
        print(f"  {accent(str(i))}) {option}{tail}")
    numbers = ", ".join(str(i) for i in range(1, len(options)))
    while True:
        raw = ask(f"Enter {numbers} or {len(options)}", default=str(default + 1))
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return int(raw) - 1


def _confirm_typed(question: str, default: bool) -> bool:
    yn = "Y/n" if default else "y/N"
    while True:
        raw = ask(f"{question} ({yn})").lower()
        if not raw:
            return default
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False


# --------------------------------------------------------------------------
# arrow-key selector
# --------------------------------------------------------------------------

_HIDE, _SHOW = "\033[?25l", "\033[?25h"


class _NoRawKeys(Exception):
    """Raw key input is not available; use the typed prompt."""


def raw_keys_available() -> bool:
    """True when the arrow-key selector can read single keys from a console."""
    if os.environ.get("COPYCORD_PLAIN_PROMPTS") not in (None, "", "0"):
        return False
    if os.environ.get("TERM") == "dumb":
        return False
    try:
        if not (sys.stdin.isatty() and sys.stdout.isatty()):
            return False
    except (AttributeError, ValueError):
        return False
    if os.name == "nt":
        try:
            import msvcrt

            msvcrt.kbhit()
        except Exception:
            return False
    return True


class _RawMode:
    """Single keys without echo; the terminal mode is restored on exit."""

    def __init__(self):
        self._restore = None

    def __enter__(self) -> "_RawMode":
        try:
            if os.name == "nt":
                self._enter_windows()
            else:
                self._enter_posix()
        except Exception as e:
            raise _NoRawKeys(str(e)) from e
        return self

    def _enter_posix(self) -> None:
        import termios

        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        new = termios.tcgetattr(fd)
        new[3] &= ~(termios.ICANON | termios.ECHO | termios.ISIG)
        new[6][termios.VMIN] = 1
        new[6][termios.VTIME] = 0
        termios.tcsetattr(fd, termios.TCSADRAIN, new)
        self._restore = lambda: termios.tcsetattr(fd, termios.TCSADRAIN, old)

    def _enter_windows(self) -> None:
        import ctypes
        import msvcrt

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)  # stdout
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            raise OSError("stdout is not a Windows console")
        # VT processing so the redraw codes work in the classic console.
        if not kernel32.SetConsoleMode(handle, mode.value | 0x0004):
            raise OSError("no VT processing")
        msvcrt.kbhit()
        self._restore = lambda: kernel32.SetConsoleMode(handle, mode.value)

    def __exit__(self, *exc) -> None:
        if self._restore is not None:
            self._restore()
            self._restore = None


_WIN_ARROWS = {"H": "up", "P": "down", "K": "left", "M": "right"}
_ANSI_ARROWS = {"A": "up", "B": "down", "D": "left", "C": "right"}


def _read_key() -> str:
    """One key: up, down, left, right, enter, esc or the character typed."""
    if os.name == "nt":
        import msvcrt

        ch = msvcrt.getwch()
        if ch in ("\x00", "\xe0"):
            return _WIN_ARROWS.get(msvcrt.getwch(), "")
    else:
        import select

        fd = sys.stdin.fileno()

        def get(timeout=None):
            if timeout is not None and not select.select([fd], [], [], timeout)[0]:
                return ""
            data = os.read(fd, 1)
            if not data:
                raise EOFError
            return data.decode("latin-1")

        ch = get()
        if ch == "\x1b":
            nxt = get(0.05)
            if nxt in ("[", "O"):
                return _ANSI_ARROWS.get(get(0.05), "")
            return "esc"
    if ch == "\x03":
        raise KeyboardInterrupt
    if ch in ("\r", "\n"):
        return "enter"
    if ch == "\x1b":
        return "esc"
    return ch.lower()


def _pointer() -> str:
    return "❯" if _encodable("❯", sys.stdout) else ">"


def _select(render, keys, question: str, answer) -> int:
    """Drive a selector: render(index) gives the lines, keys(key, index) the
    next index, or (index, True) when picked. Returns the picked index."""
    out = sys.stdout
    state = {"lines": 0}

    def clear() -> None:
        # Back to the first row drawn and wipe everything below it.
        up = state["lines"] - 1
        out.write(f"\r\033[{up}A\033[J" if up > 0 else "\r\033[J")

    def draw(index: int) -> None:
        if state["lines"]:
            clear()
        rows = render(index)
        out.write("\n".join(rows))
        out.flush()
        state["lines"] = len(rows)

    def collapse(status: str, text: str) -> None:
        tail = accent(text) if status == "ok" else dim(text)
        clear()
        out.write(f"{mark(status)} {bold(question)} {tail}\n")
        out.flush()
        state["lines"] = 0

    index, picked = keys(None, None)
    with _RawMode():
        out.write(_HIDE)
        try:
            draw(index)
            while not picked:
                key = _read_key()
                if key in ("esc", "q"):
                    raise KeyboardInterrupt
                index, picked = keys(key, index)
                draw(index)
        except KeyboardInterrupt:
            collapse("fail", "cancelled")
            raise
        except Exception as e:
            if state["lines"]:
                out.write("\n")
            raise _NoRawKeys("key input failed") from e
        finally:
            out.write(_SHOW)
            out.flush()
    collapse("ok", answer(index))
    return index


def choose(question: str, options: List[str], default: int = 0) -> int:
    """Pick one option; returns its index. Arrow keys and Enter on a console,
    a numbered prompt otherwise. Enter picks the default."""
    if raw_keys_available():
        def render(index):
            rows = [f"{paint('?', 'accent', 'bold')} {bold(question)}"]
            for i, option in enumerate(options):
                if i == index:
                    rows.append(f"{accent(_pointer() + ' ' + option)}")
                else:
                    rows.append(f"  {dim(option)}")
            arrows = "↑/↓" if _encodable("↑↓", sys.stdout) else "Up/Down"
            rows.append(f"  {dim(arrows + ' to move, Enter to select')}")
            return rows

        def keys(key, index):
            if key is None:
                return default, False
            if key in ("up", "k", "left"):
                return (index - 1) % len(options), False
            if key in ("down", "j", "right"):
                return (index + 1) % len(options), False
            if key.isdigit() and 1 <= int(key) <= len(options):
                return int(key) - 1, True
            return index, key == "enter"

        try:
            return _select(render, keys, question, lambda i: options[i])
        except _NoRawKeys:
            pass
    return _choose_typed(question, options, default)


def confirm(question: str, default: bool = True) -> bool:
    """Yes or no; Left/Right or Up/Down switch, y/n pick, Enter accepts the
    highlighted answer (the default first). Typed y/N without a console."""
    if raw_keys_available():
        labels = ("Yes", "No")

        def render(index):
            parts = [accent(f"{_pointer()} {label}") if i == index else dim(label)
                     for i, label in enumerate(labels)]
            return [f"{paint('?', 'accent', 'bold')} {bold(question)}  {parts[0]}{dim(' / ')}{parts[1]}"]

        def keys(key, index):
            if key is None:
                return (0 if default else 1), False
            if key in ("left", "right", "up", "down", "h", "l", "k", "j", "\t"):
                return 1 - index, False
            if key == "y":
                return 0, True
            if key == "n":
                return 1, True
            return index, key == "enter"

        try:
            return _select(render, keys, question, lambda i: labels[i]) == 0
        except _NoRawKeys:
            pass
    return _confirm_typed(question, default)
