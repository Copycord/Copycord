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

Interactive flows such as uninstall run inside a Rail, which joins their steps
with a line down the left on a terminal and changes nothing elsewhere.
"""

from __future__ import annotations

import itertools
import os
import re
import shutil
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
# links
# --------------------------------------------------------------------------

_URL = re.compile(r"https?://[^\s<>\"'`\033]+")
_ANSI = re.compile(r"\033\[[0-9;?]*[A-Za-z]|\033\][^\033\a]*(?:\033\\|\a)")


def _version(raw: str) -> tuple:
    return tuple(int(x) for x in re.findall(r"\d+", raw or "")[:2])


def hyperlinks(stream: Optional[TextIO] = None) -> bool:
    """True when the terminal on stream shows OSC 8 links as clickable."""
    stream = sys.stdout if stream is None else stream
    if not use_color(stream):
        return False
    try:
        if not stream.isatty():
            return False
    except (AttributeError, ValueError):
        return False
    env = os.environ
    # tmux and screen pass on the variables of the outer terminal but not the links.
    if env.get("TMUX") or env.get("STY"):
        return False
    if env.get("WT_SESSION") or env.get("KITTY_WINDOW_ID") or env.get("WEZTERM_PANE"):
        return True
    program = env.get("TERM_PROGRAM", "")
    if program in ("vscode", "WezTerm", "ghostty"):
        return True
    if program == "iTerm.app":
        return _version(env.get("TERM_PROGRAM_VERSION", "")) >= (3, 1)
    if env.get("TERM") == "xterm-kitty":
        return True
    vte = env.get("VTE_VERSION", "")
    return vte.isdigit() and int(vte) >= 5000


def link(url: str, stream: Optional[TextIO] = None) -> str:
    """url, clickable where the terminal supports it; the text shown is always the url."""
    if not hyperlinks(stream):
        return url
    return f"\033]8;;{url}\033\\{url}\033]8;;\033\\"


def linkify(text: str, stream: Optional[TextIO] = None) -> str:
    """text with every http(s) URL in it made clickable, see link()."""
    if "://" not in text or not hyperlinks(stream):
        return text

    def one(m: "re.Match") -> str:
        url = m.group(0)
        trail = len(url) - len(url.rstrip(".,;:!?)"))
        url, rest = (url[:-trail], url[-trail:]) if trail else (url, "")
        return link(url, stream) + rest

    return _URL.sub(one, text)


_DRIVE = re.compile(r"[A-Za-z]:[\\/]")
# Where a path in a sentence ends.
_PATH_END = re.compile(r"[^\s;,'\"`\033]*")


def _local(stream: Optional[TextIO] = None) -> bool:
    """True when a file link would open on this machine: links on, no SSH,
    no Docker and no --json."""
    if not hyperlinks(stream):
        return False
    env = os.environ
    if env.get("SSH_CONNECTION") or env.get("SSH_CLIENT") or env.get("SSH_TTY"):
        return False
    from copycord import jsonout
    from copycord.common.paths import in_docker

    return not in_docker() and not jsonout.active()


def file_uri(path) -> Optional[str]:
    """path as a file:// URI with this host's name, as `ls --hyperlink` writes
    it; file:///C:/... for a Windows path. None when path has no such form."""
    from urllib.parse import quote

    text = str(path)
    full = text if _DRIVE.match(text) else os.path.abspath(text)
    if _DRIVE.match(full):
        return "file:///" + quote(full.replace("\\", "/"), safe="/:")
    if full.startswith("/"):
        import socket

        return f"file://{quote(socket.gethostname())}{quote(full, safe='/')}"
    return None


def path_link(path, stream: Optional[TextIO] = None) -> str:
    """path as printed, a link that opens it where the terminal supports it
    and the session is on this machine."""
    text = str(path)
    uri = file_uri(text) if _local(stream) else None
    if uri is None:
        return text
    return f"\033]8;;{uri}\033\\{text}\033]8;;\033\\"


def link_paths(text: str, roots: Iterable, stream: Optional[TextIO] = None) -> str:
    """text with every path in or under one of roots made clickable, see path_link()."""
    roots = sorted({str(r) for r in roots if str(r)}, key=len, reverse=True)
    if not roots or not any(r in text for r in roots) or not _local(stream):
        return text
    pattern = re.compile(
        "(?:" + "|".join(re.escape(r) for r in roots) + r")(?:[\\/][^\s;,'\"`\033]*)?"
    )

    def one(m: "re.Match") -> str:
        path = m.group(0)
        # A longer name that only starts with a root, e.g. .copycord-old.
        if _PATH_END.match(text, m.end()).group(0).rstrip(".,;:!?)"):
            return path
        trail = len(path) - len(path.rstrip(".,;:!?)"))
        path, rest = (path[:-trail], path[-trail:]) if trail else (path, "")
        return path_link(path, stream) + rest

    return pattern.sub(one, text)


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
    """One status line: "✓ label  detail"; on the rail the mark takes its place."""
    stream = sys.stdout if stream is None else stream
    tail = f"  {dim(linkify(detail, stream), stream)}" if detail else ""
    gap = "  " if on_rail(stream) else " "
    print(f"{mark(status, stream)}{gap}{linkify(text, stream)}{tail}", file=stream)


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
    with section("Needs your attention", stream) as sec:
        for status, text, hint in items:
            line(status, text, hint, sec.out)


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
    return len(_ANSI.sub("", text))


def box(lines: Iterable[str], title: str = "", status: str = "info",
        stream: Optional[TextIO] = None) -> None:
    """A boxed notice; on the rail a titled step with the lines under it."""
    stream = sys.stdout if stream is None else stream
    lines = [linkify(x, stream) for x in lines]
    if on_rail(stream):
        if title:
            head = _rail_glyph("done", stream) if status == "info" else mark(status, stream)
            print(f"{head}  {bold(title, stream)}", file=stream)
        for x in lines:
            print(f"{_rail_glyph('bar', stream)}  {x}", file=stream)
        return
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
# step rail
# --------------------------------------------------------------------------

# A rail joins the steps of an interactive flow into one column:
#
#   ┌  Uninstall Copycord
#   │
#   ◆  Your data is in ...       (◇ once answered)
#   │  Keep my data
#   │
#   ✓  removed the copycord program
#   └  Your data is still in ...
#
# name -> (color, symbol, ASCII fallback); all of them single width.
_RAIL = {
    "start": ("gray", "┌", "+"),
    "bar": ("gray", "│", "|"),
    "ask": ("accent", "◆", "*"),
    "done": ("green", "◇", "o"),
    "end": ("gray", "└", "+"),
    "join": ("gray", "├", "+"),
    "rule": ("gray", "─", "-"),
    "corner": ("gray", "╮", "+"),
    "foot": ("gray", "╯", "+"),
}

_rail: Optional["Rail"] = None
# An ask() whose line was not finished, e.g. by Ctrl-C at the prompt.
_open_prompt = False


def _tty(stream: TextIO) -> bool:
    try:
        return stream.isatty()
    except (AttributeError, ValueError):
        return False


def on_rail(stream: Optional[TextIO] = None) -> bool:
    """True while a rail is open and stream is a terminal it shows on."""
    stream = sys.stdout if stream is None else stream
    return _rail is not None and _rail.active and _tty(stream)


def _rail_symbol(name: str, stream: TextIO) -> str:
    # One set for the whole rail, so it never mixes box glyphs and ASCII.
    fancy = _encodable("".join(symbol for _, symbol, _ in _RAIL.values()), stream)
    _, symbol, ascii_symbol = _RAIL[name]
    return symbol if fancy else ascii_symbol


def _rail_glyph(name: str, stream: TextIO, count: int = 1) -> str:
    return paint(_rail_symbol(name, stream) * count, _RAIL[name][0], stream=stream)


def note(text: str = "", stream: Optional[TextIO] = None) -> None:
    """A plain line; on the rail it hangs off the rail instead of its indent."""
    stream = sys.stdout if stream is None else stream
    if not on_rail(stream):
        print(text, file=stream)
        return
    text = text.lstrip()
    bar = _rail_glyph("bar", stream)
    print(f"{bar}  {text}" if text else bar, file=stream)


def gap(stream: Optional[TextIO] = None, blank: bool = True) -> None:
    """A blank line between steps, on the rail just the rail; blank=False
    prints nothing off the rail."""
    global _open_prompt
    stream = sys.stdout if stream is None else stream
    rail = on_rail(stream)
    if rail and _open_prompt:
        print(file=stream)
    _open_prompt = False
    if rail or blank:
        note("", stream)


class Rail:
    """Context manager for an interactive flow: opens the rail with title and
    closes it on the way out. Off a terminal, or under --json, title is a plain
    heading and the output keeps its usual layout."""

    def __init__(self, title: str, stream: Optional[TextIO] = None, brand: str = "",
                 heading: bool = True):
        self.title = title
        self.stream = sys.stdout if stream is None else stream
        # The header line above the rail, see brand_line(); only on the rail.
        self.brand = brand
        # Off the rail, whether title is printed as a heading.
        self.heading = heading
        self.active = False
        self._closed = False
        self._outer: Optional["Rail"] = None

    def __enter__(self) -> "Rail":
        global _rail
        from copycord import jsonout

        self.active = _tty(self.stream) and not jsonout.active()
        self._outer, _rail = _rail, self
        if self.active:
            s = self.stream
            if self.brand:
                print(f"\n{self.brand}", file=s)
            print(f"\n{_rail_glyph('start', s)}  {accent(self.title, s)}", file=s)
        elif self.heading:
            heading(self.title, self.stream)
        return self

    def close(self, text: str = "") -> None:
        """End the rail, with text after its last mark; off the rail text as is."""
        global _open_prompt
        if self._closed:
            return
        self._closed = True
        s = self.stream
        if not self.active:
            if text:
                print(text, file=s)
            return
        if _open_prompt:
            print(file=s)
            _open_prompt = False
        end = _rail_glyph("end", s)
        print(f"{end}  {text.lstrip()}" if text.strip() else end, file=s)

    def __exit__(self, *exc) -> None:
        global _rail
        try:
            self.close()
        finally:
            _rail = self._outer


def brand_line(version: str, commit: str = "", stream: Optional[TextIO] = None) -> str:
    """The header of a rail command: "Copycord v4.0.0 (abc1234) · tagline"."""
    stream = sys.stdout if stream is None else stream
    version = version if version[:1] in ("v", "u") else f"v{version}"
    sha = f" {dim('(' + commit[:7] + ')', stream)}" if commit else ""
    return f"{accent('Copycord', stream)} {version}{sha}{dim(dot(stream) + TAGLINE, stream)}"


def brand(stream: Optional[TextIO] = None) -> str:
    """brand_line() for the installed version and commit."""
    try:
        from copycord.common.config import CURRENT_VERSION as version
    except Exception:
        version = "unknown"
    try:
        from copycord.update import installed_commit

        commit = installed_commit() or ""
    except Exception:
        commit = ""
    return brand_line(version, commit, stream)


# A section box is at least this wide inside and at most this wide overall.
SECTION_MIN = 20
SECTION_MAX = 100
_SGR = re.compile(r"\033\[[0-9;]*m")


def _columns() -> int:
    return shutil.get_terminal_size((80, 24)).columns


def _wrap(text: str, width: int, indent: str = "  ") -> List[str]:
    """text in lines of at most width visible columns, broken at spaces; the
    color open at a break is closed and opened again on the next line. A word
    longer than width stays whole."""
    if _visible_len(text) <= width:
        return [text]
    rows: List[str] = []
    cur, size, style = "", 0, ""
    for word in text.split(" "):
        n = _visible_len(word)
        if cur and size + 1 + n > width:
            rows.append(cur + (RESET if style else ""))
            cur, size = indent + style, len(indent)
            joiner = ""
        else:
            joiner = " " if cur else ""
        cur += joiner + word
        size += len(joiner) + n
        for code in _SGR.findall(word):
            style = "" if code in (RESET, "\033[m") else code
    rows.append(cur)
    return rows


class _Capture:
    """Stands in for the terminal while a section collects its lines."""

    def __init__(self, real: TextIO):
        self.encoding = getattr(real, "encoding", None) or "utf-8"
        self.parts: List[str] = []

    def write(self, text: str) -> int:
        self.parts.append(text)
        return len(text)

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return True

    def lines(self) -> List[str]:
        text = "".join(self.parts).strip("\n")
        return text.split("\n") if text else []


class section:
    """A titled block of output. On the rail it is a box hanging off it, as
    wide as the terminal up to SECTION_MAX:

      │
      ◇  Install ──────────╮
      │                    │
      │  ✓ Python 3.12     │
      │                    │
      ├────────────────────╯

    Everything printed to stdout inside it lands in the box. Off the rail the
    title is a plain heading and the lines print as they come."""

    def __init__(self, title: str, stream: Optional[TextIO] = None):
        self.title = title
        self.stream = sys.stdout if stream is None else stream
        self.boxed = False
        # Where the lines of the section go: the box while boxed, else stream.
        self.out = self.stream
        self._capture: Optional[_Capture] = None
        self._saved: Optional[TextIO] = None

    def __enter__(self) -> "section":
        global _rail
        self.boxed = on_rail(self.stream)
        if not self.boxed:
            heading(self.title, self.stream)
            return self
        self._capture = self.out = _Capture(self.stream)
        self._saved, sys.stdout = sys.stdout, self._capture
        # Inside the box the lines keep their flat layout.
        self._rail, _rail = _rail, None
        return self

    def __exit__(self, *exc) -> None:
        global _rail
        if not self.boxed:
            return
        sys.stdout, _rail = self._saved, self._rail
        render_section(self.title, self._capture.lines(), self.stream)


def render_section(title: str, lines: Iterable[str], stream: Optional[TextIO] = None) -> None:
    """Print a section box (see section) with lines in it."""
    stream = sys.stdout if stream is None else stream
    g = lambda name: _rail_glyph(name, stream)  # noqa: E731
    room = max(SECTION_MIN, min(_columns(), SECTION_MAX) - 6)
    rows: List[str] = []
    for x in lines:
        rows.extend(_wrap(x.rstrip(), room))
    # Every box is as wide as the terminal (up to SECTION_MAX), so the boxes
    # of one command line up whatever is in them.
    width = max(room, len(title) + 2)
    bar = g("bar")
    print(bar, file=stream)
    dash = lambda n: _rail_glyph("rule", stream, n)  # noqa: E731
    print(f"{g('done')}  {accent(title, stream)} {dash(width + 1 - len(title))}{g('corner')}",
          file=stream)
    blank = f"{bar}{' ' * (width + 4)}{bar}"
    print(blank, file=stream)
    for x in rows:
        pad = " " * max(0, width - _visible_len(x))
        print(f"{bar}  {x}{pad}  {bar}", file=stream)
    print(blank, file=stream)
    print(f"{g('join')}{dash(width + 4)}{g('foot')}", file=stream)


# --------------------------------------------------------------------------
# spinner
# --------------------------------------------------------------------------


def _elapsed(seconds: float) -> str:
    """12s, or 2m 05s from a minute on."""
    s = int(seconds)
    return f"{s}s" if s < 60 else f"{s // 60}m {s % 60:02d}s"


def _taskbar_progress(stream: TextIO) -> bool:
    """Windows Terminal shows OSC 9;4 progress on the tab and the taskbar."""
    if not os.environ.get("WT_SESSION"):
        return False
    try:
        return stream.isatty()
    except (AttributeError, ValueError):
        return False


class Spinner:
    """Context manager that shows a spinner on a TTY and nothing otherwise.
    After a few seconds it adds the time spent; in Windows Terminal the tab
    also shows that Copycord is busy."""

    FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    RAIL_FRAMES = "◒◐◓◑"
    ELAPSED_AFTER = 3.0
    BUSY = "\033]9;4;3;0\033\\"
    IDLE = "\033]9;4;0;0\033\\"

    def __init__(self, text: str, stream: Optional[TextIO] = None):
        self.text = text
        self.stream = sys.stderr if stream is None else stream
        self.active = use_color(self.stream) and _unicode(self.stream)
        self.progress = self.active and _taskbar_progress(self.stream)
        self._started = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def render(self, frame: str, now: float) -> str:
        """The spinner line at time now (time.monotonic)."""
        spent = now - self._started
        tail = f"  {dim(_elapsed(spent), self.stream)}" if spent >= self.ELAPSED_AFTER else ""
        gap = "  " if on_rail(self.stream) else " "
        return f"\r{paint(frame, 'accent', stream=self.stream)}{gap}{self.text}{tail}"

    def _spin(self) -> None:
        frames = self.RAIL_FRAMES if on_rail(self.stream) else self.FRAMES
        for frame in itertools.cycle(frames):
            if self._stop.is_set():
                break
            self.stream.write(self.render(frame, time.monotonic()))
            self.stream.flush()
            self._stop.wait(0.08)

    def __enter__(self) -> "Spinner":
        if self.active:
            self._started = time.monotonic()
            if self.progress:
                self.stream.write(self.BUSY)
            self._thread = threading.Thread(target=self._spin, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        # Runs on success, on an error and on Ctrl-C alike.
        if self._thread is None:
            return
        self._stop.set()
        try:
            self._thread.join()
        finally:
            self._thread = None
            self.stream.write("\r\033[2K" + (self.IDLE if self.progress else ""))
            self.stream.flush()


# --------------------------------------------------------------------------
# prompts
# --------------------------------------------------------------------------


def _ask_head(question: str) -> str:
    """The question line: "? question", on the rail "◆  question"."""
    if on_rail():
        return f"{_rail_glyph('ask', sys.stdout)}  {bold(question)}"
    return f"{paint('?', 'accent', 'bold')} {bold(question)}"


def _ask_row(row: str) -> str:
    """A line under the question; on the rail it hangs off the rail."""
    return f"{_rail_glyph('bar', sys.stdout)}  {row}" if on_rail() else row


def ask(question: str, default: str = "", secret: bool = False, _follow_up: bool = False) -> str:
    global _open_prompt
    hint = f" {dim('[' + default + ']')}" if default and not secret else ""
    head = _ask_row(bold(question)) if _follow_up else _ask_head(question)
    prompt = f"{head}{hint} "
    _open_prompt = True
    if secret:
        import getpass

        value = getpass.getpass(prompt)
    else:
        value = input(prompt)
    _open_prompt = False
    return value.strip() or default


def _choose_typed(question: str, options: List[str], default: int) -> int:
    print(_ask_head(question))
    for i, option in enumerate(options, 1):
        tail = f" {dim('(default)')}" if i - 1 == default else ""
        row = f"{accent(str(i))}) {option}{tail}"
        print(_ask_row(row) if on_rail() else f"  {row}")
    numbers = ", ".join(str(i) for i in range(1, len(options)))
    while True:
        raw = ask(f"Enter {numbers} or {len(options)}", default=str(default + 1),
                  _follow_up=on_rail())
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
        if on_rail(out):
            head = _rail_glyph("done", out) if status == "ok" else mark(status, out)
            out.write(f"{head}  {bold(question)}\n{_ask_row(tail)}\n")
        else:
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
            rows = [_ask_head(question)]
            for i, option in enumerate(options):
                if i == index:
                    rows.append(_ask_row(f"{accent(_pointer() + ' ' + option)}"))
                else:
                    rows.append(_ask_row(f"  {dim(option)}"))
            arrows = "↑/↓" if _encodable("↑↓", sys.stdout) else "Up/Down"
            rows.append(_ask_row(f"  {dim(arrows + ' to move, Enter to select')}"))
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
            answer = f"{parts[0]}{dim(' / ')}{parts[1]}"
            if on_rail():
                return [_ask_head(question), _ask_row(answer)]
            return [f"{_ask_head(question)}  {answer}"]

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
