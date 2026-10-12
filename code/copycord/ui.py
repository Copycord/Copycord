# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Terminal styling for the copycord CLI: plain ANSI, no dependencies.

Color is on when the stream is a TTY, NO_COLOR is unset and TERM is not
"dumb"; FORCE_COLOR=1 turns it on anyway. Without color every helper prints
plain ASCII text, so piped output and logs stay greppable.
"""

from __future__ import annotations

import itertools
import os
import sys
import threading
import time
from typing import Iterable, Optional, TextIO

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
TAGLINE = "Mirror Discord servers, self-hosted."


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
    """A command the user can copy, e.g. `copycord update`."""
    return paint(text, "cyan", stream=stream)


# --------------------------------------------------------------------------
# step lines
# --------------------------------------------------------------------------

# status -> (colored symbol, unicode symbol, plain label)
_MARKS = {
    "ok": ("green", "✔", "PASS"),
    "warn": ("yellow", "!", "WARN"),
    "fail": ("red", "✖", "FAIL"),
    "info": ("blue", "•", "INFO"),
    "step": ("accent", "◇", "-"),
}


def mark(status: str, stream: Optional[TextIO] = None) -> str:
    stream = sys.stdout if stream is None else stream
    color, symbol, label = _MARKS[status]
    if use_color(stream) and _unicode(stream):
        return paint(symbol, color, "bold", stream=stream)
    return label


def line(status: str, text: str, detail: str = "", stream: Optional[TextIO] = None) -> None:
    """One step line: a check, cross, bang or bullet, a label and a dim detail.

    Plain form is "PASS  label: detail" so scripts can grep the status.
    """
    stream = sys.stdout if stream is None else stream
    if use_color(stream):
        tail = f"  {dim(detail, stream)}" if detail else ""
        print(f"{mark(status, stream)} {text}{tail}", file=stream)
    else:
        label = _MARKS[status][2]
        tail = f": {detail}" if detail else ""
        print(f"{label}  {text}{tail}", file=stream)


def ok(text, detail="", stream=None):
    line("ok", text, detail, stream)


def warn(text, detail="", stream=None):
    line("warn", text, detail, stream if stream is not None else sys.stderr)


def fail(text, detail="", stream=None):
    line("fail", text, detail, stream if stream is not None else sys.stderr)


def info(text, detail="", stream=None):
    line("info", text, detail, stream)


def kv(rows: Iterable[tuple], stream: Optional[TextIO] = None, width: int = 0) -> None:
    """Aligned "key  value" rows; plain form is the key padded to width, a space
    and the value."""
    stream = sys.stdout if stream is None else stream
    rows = list(rows)
    width = width or max((len(k) for k, _ in rows), default=0)
    if not use_color(stream):
        for k, v in rows:
            print(f"{k.ljust(width)} {v}", file=stream)
        return
    for k, v in rows:
        print(f"  {dim(k.ljust(width), stream)}  {v}", file=stream)


# --------------------------------------------------------------------------
# banner, headings and boxes
# --------------------------------------------------------------------------


def banner(version: str = "", stream: Optional[TextIO] = None) -> None:
    stream = sys.stdout if stream is None else stream
    if not use_color(stream):
        print(f"Copycord {version}".rstrip(), file=stream)
        return
    print(file=stream)
    for row in LOGO:
        print(paint(row, "accent", "bold", stream=stream), file=stream)
    sub = f"{version}  {TAGLINE}" if version else TAGLINE
    print(f"  {dim(sub, stream)}", file=stream)
    print(file=stream)


def heading(text: str, stream: Optional[TextIO] = None) -> None:
    stream = sys.stdout if stream is None else stream
    if use_color(stream):
        print(f"\n{accent(text, stream)}", file=stream)
    else:
        print(f"\n{text}", file=stream)


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
    """A boxed notice, e.g. the dashboard URL at the end of onboarding."""
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
    """`with Spinner("Checking GitHub"):` shows a spinner on a TTY, nothing else."""

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
# prompts (onboarding)
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


def confirm(question: str, default: bool = True) -> bool:
    yn = "Y/n" if default else "y/N"
    while True:
        raw = ask(f"{question} ({yn})").lower()
        if not raw:
            return default
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False
