"""copycord doctor prints each section as its checks finish.

EXPECTED holds what the renderer before the streaming change printed for
the checks in _stub(), so the text is compared with the old output itself.
"""

import io
import json
import re
import sys
import time

import pytest

from copycord import __main__ as cli
from copycord import ui

TERMINAL_VARS = ("NO_COLOR", "FORCE_COLOR", "TERM", "WT_SESSION", "TERM_PROGRAM", "TERM_PROGRAM_VERSION",
                 "KITTY_WINDOW_ID", "WEZTERM_PANE", "VTE_VERSION", "TMUX", "STY",
                 "SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY", "COPYCORD_SKIP_FRONTEND")


class TTY(io.StringIO):
    """A UTF-8 terminal that records what is written to it."""

    encoding = "utf-8"

    def isatty(self):
        return True


def _stub(setattr, setitem):
    """Fixed results for every doctor check; the .env fix works, the others fail."""
    C = cli.Check
    fixed = {}

    def env_file():
        if fixed.get("env_file"):
            return C(cli.PASS, ".env found", path="/srv/copycord/.env")
        return C(cli.WARN, ".env not found", path="/srv/copycord/.env", fix="env_file")

    def write_env(checks):
        fixed["env_file"] = True
        return "/srv/copycord/.env"

    def no_db(checks):
        raise cli.FixError("there is no database at /srv/copycord/data/data.db yet")

    def no_reinstall(checks):
        raise cli.FixError("Copycord was installed from source; run git pull")

    setattr(cli, "check_version", lambda: C(cli.PASS, "Copycord v4.0.0 (abcdef1) · stable channel"))
    setattr(cli, "check_python", lambda: C(cli.PASS, "Python 3.12.3"))
    setattr(cli, "check_frontend", lambda: C(
        cli.FAIL, "web UI missing", "the dashboard will not load; reinstall Copycord",
        "/srv/copycord/static/assets/main.js", "reinstall"))
    setattr(cli, "check_dependencies", lambda: C(cli.PASS, "dependencies OK"))
    setattr(cli, "check_env_file", env_file)
    setattr(cli, "check_password", lambda: C(cli.PASS, "dashboard password set"))
    setattr(cli, "check_role", lambda: C(cli.FAIL, "ROLE is set", "delete ROLE from your settings"))
    setattr(cli, "check_data_dir", lambda path: C(cli.PASS, "home writable", path="/srv/copycord/data"))
    setattr(cli, "check_layout", lambda: C(cli.PASS, "data folder layout"))
    setattr(cli, "check_db", lambda path: C(cli.INFO, "database created on first run",
                                            path="/srv/copycord/data/data.db"))
    setattr(cli, "check_backups", lambda: C(
        cli.WARN, "no backups yet", "the daily backup runs at BACKUP_AT while Copycord is running",
        "/srv/copycord/data/backups", "backup"))
    setattr(cli, "service_state", lambda: None)
    setattr(cli, "service_console", lambda: None)
    setattr(cli, "service_linger", lambda: None)
    setattr(cli, "check_admin_port", lambda service_running=False: C(cli.PASS, "admin port 8080 free"))
    setattr(cli, "check_service", lambda state, console=None, linger=None: C(
        cli.INFO, "service not installed", "copycord service install runs Copycord in the background"))
    setattr(cli, "check_updates", lambda: C(cli.INFO, "update check off",
                                            "COPYCORD_NO_UPDATE_CHECK is set"))
    setattr(cli, "brand", lambda: "Copycord v4.0.0 (abcdef1)")
    setattr(ui, "_columns", lambda: 78)
    setitem(cli.FIXES, "env_file", cli.Fix(write_env, ".env created with the default settings",
                                           "create it with the default settings"))
    setitem(cli.FIXES, "backup", cli.Fix(no_db, "database backed up", "back it up now"))
    setitem(cli.FIXES, "reinstall", cli.Fix(no_reinstall, "Copycord reinstalled", "reinstall Copycord"))


# name -> (doctor() arguments, terminal, NO_COLOR)
MODES = {
    "plain": ({}, False, False),
    "verbose": ({"verbose": True}, False, False),
    "brief": ({"brief": True}, False, False),
    "json": ({"as_json": True}, False, False),
    "fix": ({"fix": True}, False, False),
    "fix_brief": ({"fix": True, "brief": True}, False, False),
    "fix_json": ({"fix": True, "as_json": True}, False, False),
    "rail": ({}, True, True),
    "rail_fix": ({"fix": True}, True, True),
    "rail_color": ({}, True, False),
}


def render(mode, out=None):
    """(exit code, stdout) of copycord doctor in MODES[mode]; out is the stdout."""
    kwargs, tty, _ = MODES[mode]
    if out is None:
        out = TTY() if tty else io.StringIO()
    saved, sys.stdout = sys.stdout, out
    try:
        code = cli.doctor(**kwargs)
    finally:
        sys.stdout = saved
    return code, out.getvalue()


@pytest.fixture()
def stubbed(monkeypatch, tmp_path):
    for name in TERMINAL_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(ui, "is_interactive", lambda: False)
    _stub(monkeypatch.setattr, monkeypatch.setitem)
    return monkeypatch


@pytest.mark.parametrize("mode", list(MODES))
def test_output_matches_the_old_renderer(stubbed, mode):
    if MODES[mode][2]:
        stubbed.setenv("NO_COLOR", "1")
    code, out = render(mode)
    assert (code, out) == EXPECTED[mode]


def test_json_and_brief_run_every_check_before_printing(stubbed):
    real = cli.check_updates
    for mode in ("json", "brief"):
        seen, out = [], io.StringIO()
        stubbed.setattr(cli, "check_updates", lambda: seen.append(out.getvalue()) or real())
        render(mode, out)
        assert seen == [""]
        if mode == "json":
            assert json.loads(out.getvalue())["checks"][-1]["text"] == "update check off"


def test_sections_print_in_order(stubbed):
    _, out = render("plain")
    titles = ["Install", "Settings", "Data", "Running", "Updates", "Needs your attention"]
    lines = out.splitlines()
    assert [x for x in lines if x in titles] == titles


def test_a_section_is_written_before_the_next_one_is_checked(stubbed):
    seen = {}
    real = cli.check_updates

    def updates():
        seen["out"] = sys.stdout.getvalue()
        return real()

    stubbed.setattr(cli, "check_updates", updates)
    _, out = render("plain")
    before = seen["out"]
    assert "service not installed" in before
    assert before.rstrip().splitlines()[-1] == "Updates"
    assert "update check off" not in before
    assert out.startswith(before)


def test_on_the_rail_a_box_is_closed_before_the_next_one_is_checked(stubbed):
    stubbed.setenv("NO_COLOR", "1")
    seen = {}
    out = TTY()
    real = cli.check_db

    def db(path):
        # sys.stdout is the box here; the terminal is out.
        seen["out"] = out.getvalue()
        return real(path)

    stubbed.setattr(cli, "check_db", db)
    stubbed.setattr(sys, "stdout", out)
    cli.doctor()
    before = seen["out"].splitlines()
    # Install and Settings are whole boxes, Data has its top and first two rows.
    assert sum(x.startswith("├") for x in before) == 2
    assert before[-4].startswith("◇  Data ")
    assert "home writable" in before[-2] and "data folder layout" in before[-1]
    assert "database created" not in seen["out"]


def test_the_spinner_shows_on_the_row_being_checked(stubbed):
    def slow():
        time.sleep(0.3)
        return cli.Check(cli.PASS, "up to date")

    stubbed.setattr(cli, "check_updates", slow)
    _, out = render("rail_color")
    # The left edge of the box, a frame and the text, after the Updates title.
    bar = re.escape(ui._rail_glyph("bar", TTY()))
    spin = re.search(rf"\r{bar}  \S+ Checking for updates", out)
    assert spin and spin.start() > out.index("Updates")
    # Then the line is wiped and the row takes its place.
    rest = out[spin.end():]
    wipe = rest.index("\r\033[2K")
    assert "Checking" not in rest[wipe:]
    assert "up to date" in rest[wipe:].split("\n")[0]


def test_no_spinner_without_color_or_off_a_terminal(stubbed):
    for mode in ("plain", "rail"):
        if MODES[mode][2]:
            stubbed.setenv("NO_COLOR", "1")
        _, out = render(mode)
        assert "Checking" not in out and "\r" not in out


def test_a_box_drops_blank_lines_at_its_ends_as_before(monkeypatch):
    for name in TERMINAL_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(ui, "_columns", lambda: 60)
    text = "\n\nfirst\n\n  \nlast\n\n"
    live, whole = TTY(), TTY()
    with ui.Rail("t", stream=live):
        monkeypatch.setattr(sys, "stdout", live)
        with ui.section("Box"):
            for part in (text[:3], text[3:9], text[9:]):
                sys.stdout.write(part)
    with ui.Rail("t", stream=whole):
        ui.render_section("Box", ["first", "", "  ", "last"], whole)
    assert live.getvalue() == whole.getvalue()


EXPECTED = {
    'plain': (1, ''
        '\n'
        'Install\n'
        '✓ Copycord v4.0.0 (abcdef1) · stable channel\n'
        '✓ Python 3.12.3\n'
        '✗ web UI missing  the dashboard will not load; reinstall Copycord; /srv/copycord/static/assets/main.js\n'
        '✓ dependencies OK\n'
        '\n'
        'Settings\n'
        '✗ ROLE is set  delete ROLE from your settings\n'
        '! .env not found  /srv/copycord/.env\n'
        '✓ dashboard password set\n'
        '\n'
        'Data\n'
        '✓ home writable\n'
        '✓ data folder layout\n'
        '• database created on first run\n'
        '! no backups yet  the daily backup runs at BACKUP_AT while Copycord is running; /srv/copycord/data/backups\n'
        '\n'
        'Running\n'
        '✓ admin port 8080 free\n'
        '• service not installed  copycord service install runs Copycord in the background\n'
        '\n'
        'Updates\n'
        '• update check off  COPYCORD_NO_UPDATE_CHECK is set\n'
        '\n'
        'Needs your attention\n'
        '✗ web UI missing  the dashboard will not load; reinstall Copycord; run copycord doctor --fix to reinstall Copycord\n'
        '✗ ROLE is set  delete ROLE from your settings\n'
        '! .env not found  run copycord doctor --fix to create it with the default settings\n'
        '! no backups yet  the daily backup runs at BACKUP_AT while Copycord is running; run copycord doctor --fix to back it up now\n'
        '\n'
        '✗ 2 warnings, 2 problems\n'
        'Run copycord doctor --fix to fix 3 of them.\n'
    ),
    'verbose': (1, ''
        '\n'
        'Install\n'
        '✓ Copycord v4.0.0 (abcdef1) · stable channel\n'
        '✓ Python 3.12.3\n'
        '✗ web UI missing  the dashboard will not load; reinstall Copycord; /srv/copycord/static/assets/main.js\n'
        '✓ dependencies OK\n'
        '\n'
        'Settings\n'
        '✗ ROLE is set  delete ROLE from your settings\n'
        '! .env not found  /srv/copycord/.env\n'
        '✓ dashboard password set\n'
        '\n'
        'Data\n'
        '✓ home writable  /srv/copycord/data\n'
        '✓ data folder layout\n'
        '• database created on first run  /srv/copycord/data/data.db\n'
        '! no backups yet  the daily backup runs at BACKUP_AT while Copycord is running; /srv/copycord/data/backups\n'
        '\n'
        'Running\n'
        '✓ admin port 8080 free\n'
        '• service not installed  copycord service install runs Copycord in the background\n'
        '\n'
        'Updates\n'
        '• update check off  COPYCORD_NO_UPDATE_CHECK is set\n'
        '\n'
        'Needs your attention\n'
        '✗ web UI missing  the dashboard will not load; reinstall Copycord; run copycord doctor --fix to reinstall Copycord\n'
        '✗ ROLE is set  delete ROLE from your settings\n'
        '! .env not found  run copycord doctor --fix to create it with the default settings\n'
        '! no backups yet  the daily backup runs at BACKUP_AT while Copycord is running; run copycord doctor --fix to back it up now\n'
        '\n'
        '✗ 2 warnings, 2 problems\n'
        'Run copycord doctor --fix to fix 3 of them.\n'
    ),
    'brief': (1, ''
        '✗ web UI missing  the dashboard will not load; reinstall Copycord; /srv/copycord/static/assets/main.js\n'
        '✗ ROLE is set  delete ROLE from your settings\n'
        '✗ 2 warnings, 2 problems  run copycord doctor for details\n'
    ),
    'json': (1, ''
        '{\n'
        '  "ok": false,\n'
        '  "checks": [\n'
        '    {\n'
        '      "section": "Install",\n'
        '      "status": "ok",\n'
        '      "text": "Copycord v4.0.0 (abcdef1) \\u00b7 stable channel",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Install",\n'
        '      "status": "ok",\n'
        '      "text": "Python 3.12.3",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Install",\n'
        '      "status": "fail",\n'
        '      "text": "web UI missing",\n'
        '      "hint": "the dashboard will not load; reinstall Copycord; run copycord doctor --fix to reinstall Copycord",\n'
        '      "path": "/srv/copycord/static/assets/main.js",\n'
        '      "fixable": true,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Install",\n'
        '      "status": "ok",\n'
        '      "text": "dependencies OK",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Settings",\n'
        '      "status": "fail",\n'
        '      "text": "ROLE is set",\n'
        '      "hint": "delete ROLE from your settings",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Settings",\n'
        '      "status": "warn",\n'
        '      "text": ".env not found",\n'
        '      "hint": "run copycord doctor --fix to create it with the default settings",\n'
        '      "path": "/srv/copycord/.env",\n'
        '      "fixable": true,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Settings",\n'
        '      "status": "ok",\n'
        '      "text": "dashboard password set",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Data",\n'
        '      "status": "ok",\n'
        '      "text": "home writable",\n'
        '      "hint": "",\n'
        '      "path": "/srv/copycord/data",\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Data",\n'
        '      "status": "ok",\n'
        '      "text": "data folder layout",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Data",\n'
        '      "status": "info",\n'
        '      "text": "database created on first run",\n'
        '      "hint": "",\n'
        '      "path": "/srv/copycord/data/data.db",\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Data",\n'
        '      "status": "warn",\n'
        '      "text": "no backups yet",\n'
        '      "hint": "the daily backup runs at BACKUP_AT while Copycord is running; run copycord doctor --fix to back it up now",\n'
        '      "path": "/srv/copycord/data/backups",\n'
        '      "fixable": true,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Running",\n'
        '      "status": "ok",\n'
        '      "text": "admin port 8080 free",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Running",\n'
        '      "status": "info",\n'
        '      "text": "service not installed",\n'
        '      "hint": "copycord service install runs Copycord in the background",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Updates",\n'
        '      "status": "info",\n'
        '      "text": "update check off",\n'
        '      "hint": "COPYCORD_NO_UPDATE_CHECK is set",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    }\n'
        '  ],\n'
        '  "summary": {\n'
        '    "ok": 7,\n'
        '    "warn": 2,\n'
        '    "fail": 2,\n'
        '    "info": 3,\n'
        '    "fixed": 0,\n'
        '    "text": "2 warnings, 2 problems"\n'
        '  },\n'
        '  "fixes": []\n'
        '}\n'
    ),
    'fix': (1, ''
        '\n'
        'Install\n'
        '✓ Copycord v4.0.0 (abcdef1) · stable channel\n'
        '✓ Python 3.12.3\n'
        '✗ web UI missing  could not fix it: Copycord was installed from source; run git pull\n'
        '✓ dependencies OK\n'
        '\n'
        'Settings\n'
        '✗ ROLE is set  delete ROLE from your settings\n'
        '✓ .env created with the default settings  /srv/copycord/.env (fixed)\n'
        '✓ dashboard password set\n'
        '\n'
        'Data\n'
        '✓ home writable\n'
        '✓ data folder layout\n'
        '• database created on first run\n'
        '✗ no backups yet  could not fix it: there is no database at /srv/copycord/data/data.db yet\n'
        '\n'
        'Running\n'
        '✓ admin port 8080 free\n'
        '• service not installed  copycord service install runs Copycord in the background\n'
        '\n'
        'Updates\n'
        '• update check off  COPYCORD_NO_UPDATE_CHECK is set\n'
        '\n'
        'Needs your attention\n'
        '✗ web UI missing  could not fix it: Copycord was installed from source; run git pull\n'
        '✗ ROLE is set  delete ROLE from your settings\n'
        '! no backups yet  could not fix it: there is no database at /srv/copycord/data/data.db yet\n'
        '\n'
        '✓ Fixed 1 issue\n'
        '✗ 3 things need your attention\n'
    ),
    'fix_brief': (1, ''
        '✓ .env created with the default settings  /srv/copycord/.env\n'
        '✗ web UI missing  could not fix it: Copycord was installed from source; run git pull; /srv/copycord/static/assets/main.js\n'
        '✗ ROLE is set  delete ROLE from your settings\n'
        '✗ 1 warning, 2 problems  run copycord doctor for details\n'
    ),
    'fix_json': (1, ''
        '{\n'
        '  "ok": false,\n'
        '  "checks": [\n'
        '    {\n'
        '      "section": "Install",\n'
        '      "status": "ok",\n'
        '      "text": "Copycord v4.0.0 (abcdef1) \\u00b7 stable channel",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Install",\n'
        '      "status": "ok",\n'
        '      "text": "Python 3.12.3",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Install",\n'
        '      "status": "fail",\n'
        '      "text": "web UI missing",\n'
        '      "hint": "could not fix it: Copycord was installed from source; run git pull",\n'
        '      "path": "/srv/copycord/static/assets/main.js",\n'
        '      "fixable": true,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Install",\n'
        '      "status": "ok",\n'
        '      "text": "dependencies OK",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Settings",\n'
        '      "status": "fail",\n'
        '      "text": "ROLE is set",\n'
        '      "hint": "delete ROLE from your settings",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Settings",\n'
        '      "status": "ok",\n'
        '      "text": ".env created with the default settings",\n'
        '      "hint": "/srv/copycord/.env",\n'
        '      "path": "/srv/copycord/.env",\n'
        '      "fixable": false,\n'
        '      "fixed": true\n'
        '    },\n'
        '    {\n'
        '      "section": "Settings",\n'
        '      "status": "ok",\n'
        '      "text": "dashboard password set",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Data",\n'
        '      "status": "ok",\n'
        '      "text": "home writable",\n'
        '      "hint": "",\n'
        '      "path": "/srv/copycord/data",\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Data",\n'
        '      "status": "ok",\n'
        '      "text": "data folder layout",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Data",\n'
        '      "status": "info",\n'
        '      "text": "database created on first run",\n'
        '      "hint": "",\n'
        '      "path": "/srv/copycord/data/data.db",\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Data",\n'
        '      "status": "warn",\n'
        '      "text": "no backups yet",\n'
        '      "hint": "could not fix it: there is no database at /srv/copycord/data/data.db yet",\n'
        '      "path": "/srv/copycord/data/backups",\n'
        '      "fixable": true,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Running",\n'
        '      "status": "ok",\n'
        '      "text": "admin port 8080 free",\n'
        '      "hint": "",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Running",\n'
        '      "status": "info",\n'
        '      "text": "service not installed",\n'
        '      "hint": "copycord service install runs Copycord in the background",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    },\n'
        '    {\n'
        '      "section": "Updates",\n'
        '      "status": "info",\n'
        '      "text": "update check off",\n'
        '      "hint": "COPYCORD_NO_UPDATE_CHECK is set",\n'
        '      "path": null,\n'
        '      "fixable": false,\n'
        '      "fixed": false\n'
        '    }\n'
        '  ],\n'
        '  "summary": {\n'
        '    "ok": 8,\n'
        '    "warn": 1,\n'
        '    "fail": 2,\n'
        '    "info": 3,\n'
        '    "fixed": 1,\n'
        '    "text": "1 warning, 2 problems"\n'
        '  },\n'
        '  "fixes": [\n'
        '    {\n'
        '      "fix": "env_file",\n'
        '      "text": ".env created with the default settings",\n'
        '      "result": "/srv/copycord/.env"\n'
        '    }\n'
        '  ]\n'
        '}\n'
    ),
    'rail': (1, ''
        '\n'
        'Copycord v4.0.0 (abcdef1)\n'
        '\n'
        '┌  Copycord doctor\n'
        '│\n'
        '◇  Install ──────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✓ Copycord v4.0.0 (abcdef1) · stable channel                              │\n'
        '│  ✓ Python 3.12.3                                                           │\n'
        '│  ✗ web UI missing  the dashboard will not load; reinstall Copycord;        │\n'
        '│    /srv/copycord/static/assets/main.js                                     │\n'
        '│  ✓ dependencies OK                                                         │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Settings ─────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✗ ROLE is set  delete ROLE from your settings                             │\n'
        '│  ! .env not found  /srv/copycord/.env                                      │\n'
        '│  ✓ dashboard password set                                                  │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Data ─────────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✓ home writable                                                           │\n'
        '│  ✓ data folder layout                                                      │\n'
        '│  • database created on first run                                           │\n'
        '│  ! no backups yet  the daily backup runs at BACKUP_AT while Copycord is    │\n'
        '│    running; /srv/copycord/data/backups                                     │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Running ──────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✓ admin port 8080 free                                                    │\n'
        '│  • service not installed  copycord service install runs Copycord in the    │\n'
        '│    background                                                              │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Updates ──────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  • update check off  COPYCORD_NO_UPDATE_CHECK is set                       │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Needs your attention ─────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✗ web UI missing  the dashboard will not load; reinstall Copycord; run    │\n'
        '│    copycord doctor --fix to reinstall Copycord                             │\n'
        '│  ✗ ROLE is set  delete ROLE from your settings                             │\n'
        '│  ! .env not found  run copycord doctor --fix to create it with the         │\n'
        '│    default settings                                                        │\n'
        '│  ! no backups yet  the daily backup runs at BACKUP_AT while Copycord is    │\n'
        '│    running; run copycord doctor --fix to back it up now                    │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '│  Run copycord doctor --fix to fix 3 of them.\n'
        '│\n'
        '└  ✗ 2 warnings, 2 problems\n'
    ),
    'rail_fix': (1, ''
        '\n'
        'Copycord v4.0.0 (abcdef1)\n'
        '\n'
        '┌  Copycord doctor\n'
        '│\n'
        '◇  Install ──────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✓ Copycord v4.0.0 (abcdef1) · stable channel                              │\n'
        '│  ✓ Python 3.12.3                                                           │\n'
        '│  ✗ web UI missing  could not fix it: Copycord was installed from source;   │\n'
        '│    run git pull                                                            │\n'
        '│  ✓ dependencies OK                                                         │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Settings ─────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✗ ROLE is set  delete ROLE from your settings                             │\n'
        '│  ✓ .env created with the default settings  /srv/copycord/.env (fixed)      │\n'
        '│  ✓ dashboard password set                                                  │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Data ─────────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✓ home writable                                                           │\n'
        '│  ✓ data folder layout                                                      │\n'
        '│  • database created on first run                                           │\n'
        '│  ✗ no backups yet  could not fix it: there is no database at               │\n'
        '│    /srv/copycord/data/data.db yet                                          │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Running ──────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✓ admin port 8080 free                                                    │\n'
        '│  • service not installed  copycord service install runs Copycord in the    │\n'
        '│    background                                                              │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Updates ──────────────────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  • update check off  COPYCORD_NO_UPDATE_CHECK is set                       │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '◇  Needs your attention ─────────────────────────────────────────────────────╮\n'
        '│                                                                            │\n'
        '│  ✗ web UI missing  could not fix it: Copycord was installed from source;   │\n'
        '│    run git pull                                                            │\n'
        '│  ✗ ROLE is set  delete ROLE from your settings                             │\n'
        '│  ! no backups yet  could not fix it: there is no database at               │\n'
        '│    /srv/copycord/data/data.db yet                                          │\n'
        '│                                                                            │\n'
        '├────────────────────────────────────────────────────────────────────────────╯\n'
        '│\n'
        '✓  Fixed 1 issue\n'
        '│\n'
        '└  ✗ 3 things need your attention\n'
    ),
    'rail_color': (1, ''
        '\n'
        'Copycord v4.0.0 (abcdef1)\n'
        '\n'
        '\x1b[90m┌\x1b[0m  \x1b[38;5;99;1mCopycord doctor\x1b[0m\n'
        '\x1b[90m│\x1b[0m\n'
        '\x1b[32m◇\x1b[0m  \x1b[38;5;99;1mInstall\x1b[0m \x1b[90m──────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╮\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[32;1m✓\x1b[0m Copycord v4.0.0 (abcdef1) · stable channel                              \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[32;1m✓\x1b[0m Python 3.12.3                                                           \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[31;1m✗\x1b[0m web UI missing  \x1b[2mthe dashboard will not load; reinstall Copycord;\x1b[0m        \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m    \x1b[2m/srv/copycord/static/assets/main.js\x1b[0m                                     \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[32;1m✓\x1b[0m dependencies OK                                                         \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m├\x1b[0m\x1b[90m────────────────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╯\x1b[0m\n'
        '\x1b[90m│\x1b[0m\n'
        '\x1b[32m◇\x1b[0m  \x1b[38;5;99;1mSettings\x1b[0m \x1b[90m─────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╮\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[31;1m✗\x1b[0m ROLE is set  \x1b[2mdelete ROLE from your settings\x1b[0m                             \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[33;1m!\x1b[0m .env not found  \x1b[2m/srv/copycord/.env\x1b[0m                                      \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[32;1m✓\x1b[0m dashboard password set                                                  \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m├\x1b[0m\x1b[90m────────────────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╯\x1b[0m\n'
        '\x1b[90m│\x1b[0m\n'
        '\x1b[32m◇\x1b[0m  \x1b[38;5;99;1mData\x1b[0m \x1b[90m─────────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╮\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[32;1m✓\x1b[0m home writable                                                           \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[32;1m✓\x1b[0m data folder layout                                                      \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[34;1m•\x1b[0m database created on first run                                           \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[33;1m!\x1b[0m no backups yet  \x1b[2mthe daily backup runs at BACKUP_AT while Copycord is\x1b[0m    \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m    \x1b[2mrunning; /srv/copycord/data/backups\x1b[0m                                     \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m├\x1b[0m\x1b[90m────────────────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╯\x1b[0m\n'
        '\x1b[90m│\x1b[0m\n'
        '\x1b[32m◇\x1b[0m  \x1b[38;5;99;1mRunning\x1b[0m \x1b[90m──────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╮\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[32;1m✓\x1b[0m admin port 8080 free                                                    \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[34;1m•\x1b[0m service not installed  \x1b[2mcopycord service install runs Copycord in the\x1b[0m    \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m    \x1b[2mbackground\x1b[0m                                                              \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m├\x1b[0m\x1b[90m────────────────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╯\x1b[0m\n'
        '\x1b[90m│\x1b[0m\n'
        '\x1b[32m◇\x1b[0m  \x1b[38;5;99;1mUpdates\x1b[0m \x1b[90m──────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╮\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[34;1m•\x1b[0m update check off  \x1b[2mCOPYCORD_NO_UPDATE_CHECK is set\x1b[0m                       \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m├\x1b[0m\x1b[90m────────────────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╯\x1b[0m\n'
        '\x1b[90m│\x1b[0m\n'
        '\x1b[32m◇\x1b[0m  \x1b[38;5;99;1mNeeds your attention\x1b[0m \x1b[90m─────────────────────────────────────────────────────\x1b[0m\x1b[90m╮\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[31;1m✗\x1b[0m web UI missing  \x1b[2mthe dashboard will not load; reinstall Copycord; run\x1b[0m    \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m    \x1b[2m\x1b[36mcopycord doctor --fix\x1b[0m to reinstall Copycord\x1b[0m                             \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[31;1m✗\x1b[0m ROLE is set  \x1b[2mdelete ROLE from your settings\x1b[0m                             \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[33;1m!\x1b[0m .env not found  \x1b[2mrun \x1b[36mcopycord doctor --fix\x1b[0m to create it with the         \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m    default settings\x1b[0m                                                        \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[33;1m!\x1b[0m no backups yet  \x1b[2mthe daily backup runs at BACKUP_AT while Copycord is\x1b[0m    \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m    \x1b[2mrunning; run \x1b[36mcopycord doctor --fix\x1b[0m to back it up now\x1b[0m                    \x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m                                                                            \x1b[90m│\x1b[0m\n'
        '\x1b[90m├\x1b[0m\x1b[90m────────────────────────────────────────────────────────────────────────────\x1b[0m\x1b[90m╯\x1b[0m\n'
        '\x1b[90m│\x1b[0m\n'
        '\x1b[90m│\x1b[0m  \x1b[2mRun \x1b[0m\x1b[36mcopycord doctor --fix\x1b[0m\x1b[2m to fix 3 of them.\x1b[0m\n'
        '\x1b[90m│\x1b[0m\n'
        '\x1b[90m└\x1b[0m  \x1b[31;1m✗\x1b[0m 2 warnings, 2 problems\n'
    ),
}
