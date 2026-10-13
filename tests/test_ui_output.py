"""Status marks, clickable links and the spinner in copycord.ui."""

import io
import re
import unicodedata
from pathlib import Path

import pytest

from copycord import ui

ROOT = Path(__file__).resolve().parents[1]
URL = "https://copycord.github.io/Copycord/docs/cli"
TERMINAL_VARS = ("NO_COLOR", "FORCE_COLOR", "TERM", "WT_SESSION", "TERM_PROGRAM", "TERM_PROGRAM_VERSION",
                 "KITTY_WINDOW_ID", "WEZTERM_PANE", "VTE_VERSION", "TMUX", "STY")


class TTY(io.StringIO):
    """A UTF-8 terminal that records what is written to it."""

    encoding = "utf-8"

    def isatty(self):
        return True


@pytest.fixture()
def term(monkeypatch):
    """A plain terminal: no color settings and no known terminal program."""
    for name in TERMINAL_VARS:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def _osc8(url):
    return f"\033]8;;{url}\033\\{url}\033]8;;\033\\"


# --------------------------------------------------------------------------
# marks
# --------------------------------------------------------------------------


@pytest.mark.parametrize("status", sorted(ui._MARKS))
def test_marks_are_narrow_text_symbols(status):
    # Wide or emoji-style marks eat the space after them in Windows Terminal.
    symbol = ui._MARKS[status][1]
    assert len(symbol) == 1
    assert unicodedata.east_asian_width(symbol) not in ("W", "F")
    assert symbol not in "\u2714\u2716\u2705\u274c"


def test_ok_and_fail_marks():
    assert ui._MARKS["ok"][1:] == ("\u2713", "+")
    assert ui._MARKS["fail"][1:] == ("\u2717", "x")


def test_every_mark_has_the_same_gap(term):
    buf = TTY()
    for status in ui._MARKS:
        ui.line(status, "home writable", stream=buf)
    plain = [re.sub(r"\033\[[0-9;]*m", "", x) for x in buf.getvalue().splitlines()]
    assert [x[1:] for x in plain] == [" home writable"] * len(ui._MARKS)


def test_no_wide_marks_left_in_the_repo():
    paths = [ROOT / "install.sh", ROOT / "install.ps1", *sorted((ROOT / "src" / "copycord").glob("*.py")),
             *sorted((ROOT / "website" / "docs").rglob("*.md")), *sorted((ROOT / "tests").glob("*.py"))]
    for path in paths:
        text = path.read_text(encoding="utf-8")
        assert chr(0x2714) not in text and chr(0x2716) not in text, path
    ps1 = (ROOT / "install.ps1").read_text(encoding="utf-8")
    for code in ("0x2714", "0x2716", "\\u2714", "\\u2716"):
        assert code not in ps1


def test_installers_use_the_cli_marks():
    sh = (ROOT / "install.sh").read_text(encoding="utf-8")
    assert 'OK_MARK="\u2713"; FAIL_MARK="\u2717"' in sh
    assert '"\u2713 "* | "+ "*) done_line' in sh
    assert '"\u2717 "* | "x "*)' in sh
    ps1 = (ROOT / "install.ps1").read_text(encoding="utf-8")
    assert "$okMark = if ($utf) { [string][char]0x2713 }" in ps1
    assert "$failMark = if ($utf) { [string][char]0x2717 }" in ps1
    assert "'^([+x!]|\\u2713|\\u2717) (.*)$'" in ps1


# --------------------------------------------------------------------------
# links
# --------------------------------------------------------------------------


@pytest.mark.parametrize("env", [
    {"WT_SESSION": "c0ffee"},
    {"TERM_PROGRAM": "vscode"},
    {"TERM_PROGRAM": "iTerm.app", "TERM_PROGRAM_VERSION": "3.5.2"},
    {"TERM_PROGRAM": "WezTerm"},
    {"WEZTERM_PANE": "0"},
    {"KITTY_WINDOW_ID": "1"},
    {"TERM": "xterm-kitty"},
    {"VTE_VERSION": "7600"},
])
def test_links_on_in_terminals_that_show_them(term, env):
    for name, value in env.items():
        term.setenv(name, value)
    assert ui.hyperlinks(TTY())
    assert ui.link(URL, TTY()) == _osc8(URL)


@pytest.mark.parametrize("env", [
    {},
    {"VTE_VERSION": "4205"},
    {"TERM_PROGRAM": "iTerm.app", "TERM_PROGRAM_VERSION": "3.0.15"},
    {"TERM_PROGRAM": "Apple_Terminal"},
    {"WT_SESSION": "c0ffee", "NO_COLOR": "1"},
    {"WT_SESSION": "c0ffee", "TERM": "dumb"},
    {"WT_SESSION": "c0ffee", "TMUX": "/tmp/tmux-1000/default,1,0"},
    {"WT_SESSION": "c0ffee", "STY": "1.pts-0"},
])
def test_links_off_elsewhere(term, env):
    for name, value in env.items():
        term.setenv(name, value)
    assert not ui.hyperlinks(TTY())
    assert ui.link(URL, TTY()) == URL


def test_links_off_when_not_a_terminal(term):
    term.setenv("WT_SESSION", "c0ffee")
    term.setenv("FORCE_COLOR", "1")
    buf = io.StringIO()
    assert not ui.hyperlinks(buf)
    assert ui.link(URL, buf) == URL


def test_no_color_flag_turns_links_off(term):
    from copycord import __main__ as cli

    term.setenv("WT_SESSION", "c0ffee")
    term.setenv("NO_COLOR", "")
    assert ui.hyperlinks(TTY())
    cli._take_no_color(["--no-color", "doctor"])
    assert not ui.hyperlinks(TTY())


def test_linkify_keeps_the_text_and_trailing_punctuation(term):
    term.setenv("WT_SESSION", "c0ffee")
    text = f"see {URL}. Or https://nodejs.org, then retry"
    out = ui.linkify(text, TTY())
    assert out == f"see {_osc8(URL)}. Or {_osc8('https://nodejs.org')}, then retry"
    assert ui._ANSI.sub("", out) == text
    assert ui.linkify("no address here", TTY()) == "no address here"


def test_linkify_stops_at_a_color_code(term):
    term.setenv("WT_SESSION", "c0ffee")
    out = ui.linkify(f"\033[36mcurl {URL}\033[0m", TTY())
    assert out == f"\033[36mcurl {_osc8(URL)}\033[0m"


def test_status_line_links_its_urls(term):
    term.setenv("WT_SESSION", "c0ffee")
    buf = TTY()
    ui.fail("Node.js missing", f"install it from {URL}", stream=buf)
    assert _osc8(URL) in buf.getvalue()
    plain = ui._ANSI.sub("", buf.getvalue())
    assert plain == f"\u2717 Node.js missing  install it from {URL}\n"


def test_status_line_without_link_support_is_unchanged(term):
    buf = TTY()
    ui.fail("Node.js missing", f"install it from {URL}", stream=buf)
    assert "\033]" not in buf.getvalue()
    assert URL in buf.getvalue()


def test_box_width_ignores_link_codes(term):
    term.setenv("WT_SESSION", "c0ffee")
    buf = TTY()
    ui.box(["Docs:", URL], title="Help", stream=buf)
    rows = [ui._ANSI.sub("", x) for x in buf.getvalue().splitlines()]
    assert len({len(x) for x in rows}) == 1
    assert _osc8(URL) in buf.getvalue()


def test_visible_len_skips_color_and_link_codes():
    assert ui._visible_len(f"\033[2m{_osc8(URL)}\033[0m") == len(URL)
    assert ui._visible_len("\033]8;;https://x\ahttps://x\033]8;;\a") == len("https://x")


def test_help_docs_link(term, monkeypatch):
    from copycord import __main__ as cli

    term.setenv("WT_SESSION", "c0ffee")
    monkeypatch.setattr(ui.sys, "stdout", TTY())
    assert f"Docs: {_osc8(cli.DOCS_URL)}" in cli.format_help()
    term.delenv("WT_SESSION")
    assert f"Docs: {cli.DOCS_URL}" in ui._ANSI.sub("", cli.format_help())
    assert "\033]8" not in cli.format_help()


def test_installers_link_the_dashboard():
    sh = (ROOT / "install.sh").read_text(encoding="utf-8")
    assert 'box_link "$URL"' in sh
    assert "$ESC]8;;$1$ESC\\\\$1$ESC]8;;$ESC\\\\" in sh
    detect = sh[sh.index("HYPERLINKS=0"):sh.index("HYPERLINKS=0") + 700]
    for probe in ('[ "$TTY" = "1" ]', "TMUX", "STY", "WT_SESSION", "vscode", "VTE_VERSION", "KITTY_WINDOW_ID"):
        assert probe in detect
    ps1 = (ROOT / "install.ps1").read_text(encoding="utf-8")
    assert 'BoxLink "http://localhost:$port"' in ps1
    assert "$hyperlinks = $useColor -and ([bool]$env:WT_SESSION -or $env:TERM_PROGRAM -eq 'vscode')" in ps1
    assert '"$esc]8;;$Url$esc\\$Url$esc]8;;$esc\\"' in ps1


# --------------------------------------------------------------------------
# spinner
# --------------------------------------------------------------------------


@pytest.mark.parametrize("seconds, want", [(3, "3s"), (12.9, "12s"), (59, "59s"), (60, "1m 00s"), (125, "2m 05s")])
def test_elapsed(seconds, want):
    assert ui._elapsed(seconds) == want


def test_spinner_shows_the_time_spent_after_three_seconds(term):
    spin = ui.Spinner("Installing dependencies", stream=TTY())
    spin._started = 100.0
    assert ui._ANSI.sub("", spin.render("\u280b", 102.9)) == "\r\u280b Installing dependencies"
    assert ui._ANSI.sub("", spin.render("\u280b", 103.0)) == "\r\u280b Installing dependencies  3s"
    assert ui._ANSI.sub("", spin.render("\u280b", 112.4)) == "\r\u280b Installing dependencies  12s"


def test_spinner_is_silent_without_a_terminal(term):
    buf = io.StringIO()
    with ui.Spinner("Installing", stream=buf) as spin:
        assert not spin.active
    assert buf.getvalue() == ""


def test_spinner_shows_busy_in_windows_terminal(term):
    term.setenv("WT_SESSION", "c0ffee")
    buf = TTY()
    with ui.Spinner("Installing", stream=buf) as spin:
        assert spin.progress
    out = buf.getvalue()
    assert out.startswith(ui.Spinner.BUSY)
    assert out.endswith("\r\033[2K" + ui.Spinner.IDLE)
    assert ui.Spinner.BUSY == "\033]9;4;3;0\033\\" and ui.Spinner.IDLE == "\033]9;4;0;0\033\\"


@pytest.mark.parametrize("exc", [RuntimeError("install failed"), KeyboardInterrupt()])
def test_spinner_clears_busy_on_error_and_ctrl_c(term, exc):
    term.setenv("WT_SESSION", "c0ffee")
    buf = TTY()
    with pytest.raises(type(exc)):
        with ui.Spinner("Installing", stream=buf) as spin:
            raise exc
    out = buf.getvalue()
    assert out.startswith(ui.Spinner.BUSY)
    assert out.endswith("\r\033[2K" + ui.Spinner.IDLE)
    assert spin._thread is None


def test_spinner_no_busy_outside_windows_terminal(term):
    buf = TTY()
    with ui.Spinner("Installing", stream=buf) as spin:
        assert spin.active and not spin.progress
    assert "\033]9;4" not in buf.getvalue()


def test_spinner_no_busy_with_no_color(term):
    term.setenv("WT_SESSION", "c0ffee")
    term.setenv("NO_COLOR", "1")
    buf = TTY()
    with ui.Spinner("Installing", stream=buf) as spin:
        assert not spin.active and not spin.progress
    assert buf.getvalue() == ""


def test_installer_spinners_show_time_and_busy():
    sh = (ROOT / "install.sh").read_text(encoding="utf-8")
    run = sh[sh.index("run() {"):sh.index("\n}\n", sh.index("run() {"))]
    assert "printf '%s]9;4;3;0%s\\\\' \"$ESC\" \"$ESC\"" in run
    assert 'elif [ "$secs" -ge 3 ]; then' in run
    assert run.index("progress_off") > run.index('printf \'\\r%s\' "$ESC[2K"')
    assert "printf '%s]9;4;0;0%s\\\\' \"$ESC\" \"$ESC\"" in sh
    # The INT and TERM trap calls cleanup, which clears the tab progress.
    cleanup = sh[sh.index("cleanup() {"):sh.index("\n}\n", sh.index("cleanup() {"))]
    assert "progress_off" in cleanup
    assert "trap 'cleanup; exit 130' INT TERM" in sh
    assert 'if [ "$TTY" = "1" ] && [ -n "${WT_SESSION:-}" ]; then PROGRESS=1; fi' in sh

    ps1 = (ROOT / "install.ps1").read_text(encoding="utf-8")
    assert "$progressBar = $useColor -and [bool]$env:WT_SESSION" in ps1
    run = ps1[ps1.index("function Run("):ps1.index("function Run(") + 3000]
    assert ('try {\n            if ($progressBar) { Write-Host "$esc]9;4;3;0$esc\\" -NoNewline }' in run)
    assert ('} finally {\n            if ($progressBar) { Write-Host "$esc]9;4;0;0$esc\\" -NoNewline }' in run)
    assert "if ($secs -ge 3) { $tail = '  ' + (Spent $secs) }" in run


# --------------------------------------------------------------------------
# step rail
# --------------------------------------------------------------------------


class AsciiTTY(TTY):
    """A terminal whose encoding has no box glyphs."""

    encoding = "ascii"


class Pipe(io.StringIO):
    """A redirected stream: not a terminal."""

    encoding = "utf-8"


def _plain(buf):
    return re.sub(r"\033\[[0-9;]*m", "", buf.getvalue())


def _flow(buf):
    """A small uninstall-like flow on buf."""
    with ui.Rail("Uninstall Copycord", stream=buf) as rail:
        ui.note("Removes the Copycord program.", buf)
        ui.gap(buf)
        ui.ok("removed the copycord program", "uv tool uninstall copycord", stream=buf)
        ui.gap(buf, blank=False)
        rail.close("  Your data is still in /d")


@pytest.mark.parametrize("name", sorted(ui._RAIL))
def test_rail_glyphs_are_narrow_text_symbols(name):
    _, symbol, ascii_symbol = ui._RAIL[name]
    assert len(symbol) == 1 and len(ascii_symbol) == 1 and ascii_symbol.isascii()
    assert unicodedata.east_asian_width(symbol) not in ("W", "F")


def test_rail_on_a_terminal(term):
    buf = TTY()
    _flow(buf)
    assert _plain(buf).splitlines() == [
        "",
        "┌  Uninstall Copycord",
        "│  Removes the Copycord program.",
        "│",
        "✓  removed the copycord program  uv tool uninstall copycord",
        "│",
        "└  Your data is still in /d",
    ]
    assert ui._rail is None


def test_rail_falls_back_to_ascii_together(term):
    buf = AsciiTTY()
    _flow(buf)
    assert _plain(buf).splitlines() == [
        "",
        "+  Uninstall Copycord",
        "|  Removes the Copycord program.",
        "|",
        "+  removed the copycord program  uv tool uninstall copycord",
        "|",
        "+  Your data is still in /d",
    ]


def test_rail_off_without_a_terminal_keeps_the_plain_layout(term):
    buf = Pipe()
    _flow(buf)
    assert buf.getvalue().splitlines() == [
        "",
        "Uninstall Copycord",
        "Removes the Copycord program.",
        "",
        "✓ removed the copycord program  uv tool uninstall copycord",
        "  Your data is still in /d",
    ]


def test_rail_off_under_json(term):
    from copycord import jsonout

    buf = TTY()
    with jsonout.human_to_stderr():
        _flow(buf)
    assert "│" not in buf.getvalue() and "┌" not in buf.getvalue()
    assert "✓ removed the copycord program" in buf.getvalue()


def test_rail_closes_on_an_error(term):
    buf = TTY()
    with pytest.raises(RuntimeError):
        with ui.Rail("Updating Copycord", stream=buf):
            ui.fail("update failed", stream=buf)
            raise RuntimeError("boom")
    assert _plain(buf).splitlines()[-2:] == ["✗  update failed", "└"]
    assert ui._rail is None and not ui.on_rail(buf)


def test_rail_lines_on_another_terminal_stream(term):
    out, err = TTY(), TTY()
    with ui.Rail("Uninstall Copycord", stream=out):
        ui.warn("uninstall cancelled", "nothing was changed", stream=err)
        ui.warn("not a terminal", stream=Pipe())
    assert _plain(err) == "!  uninstall cancelled  nothing was changed\n"


def test_box_on_the_rail(term):
    buf = TTY()
    with ui.Rail("Updating Copycord", stream=buf):
        ui.box(["3.28.0 -> 4.0.0"], title="Update available", stream=buf)
        ui.box(["Could not back up"], title="Database backup failed", status="fail", stream=buf)
    assert _plain(buf).splitlines()[2:6] == [
        "◇  Update available",
        "│  3.28.0 -> 4.0.0",
        "✗  Database backup failed",
        "│  Could not back up",
    ]


def test_gap_finishes_a_prompt_line_left_open(term, monkeypatch):
    buf = TTY()
    with ui.Rail("Uninstall Copycord", stream=buf):
        monkeypatch.setattr(ui, "_open_prompt", True)
        ui.gap(buf)
    assert _plain(buf).endswith("Uninstall Copycord\n\n│\n└\n")
    assert ui._open_prompt is False


def test_spinner_on_the_rail_lines_up_with_the_marks(term):
    buf = TTY()
    with ui.Rail("Updating Copycord", stream=buf):
        spin = ui.Spinner("Installing", stream=buf)
        spin._started = 10.0
        assert _plain(io.StringIO(spin.render("⠋", 10.5))) == "\r⠋  Installing"
