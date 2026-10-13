# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
copycord run                  start the dashboard, server bot and client
copycord restart              restart the OS service so it re-reads <home>/.env
copycord --version            print the version and the update channel
copycord doctor [--verbose]   grouped health checks: install, settings, data,
                              running and updates (exit 1 on a problem)
copycord config get KEY       read one setting (secrets masked)
copycord config set KEY VAL   write one setting: env-only keys such as PASSWORD
                              go to <home>/.env, the rest to the DB app_config
copycord config unset KEY     remove one setting
copycord config export [FILE] app_config as JSON (secrets masked on stdout)
copycord config import FILE   load app_config values from a JSON object
copycord service install [--user] [--data-dir PATH]
                              run Copycord as a systemd/launchd/Scheduled Task service
copycord service start|stop|status|uninstall [--user]
copycord update [--channel stable|beta|dev [--repo OWNER/NAME] [--ref REF]]
                [--tag VERSION] [--no-install] [--no-backup] [--yes]
                              update (uv tool / pipx) from the saved or given
                              channel and restart a running service
copycord update status        installed version, channel and latest on it
copycord uninstall [--purge] [--yes] [--keep-service] [--dry-run]
                              remove the service and the program (and the
                              data directory with --purge)
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import json
import logging
import os
import socket
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import List, Mapping, NamedTuple, NoReturn, Optional

from copycord.envfile import ENV_KEYS

logger = logging.getLogger("copycord")

ROLE_REMOVED_MESSAGE = (
    "ROLE is set, but Copycord v4 removed the ROLE option. Delete ROLE from "
    "your compose file (and the extra server/client services), or stay on a "
    "v3 image tag."
)

MIN_PYTHON = (3, 12)
# First Python release Copycord is not tested on (pyproject requires-python).
MAX_PYTHON = (3, 15)
DEFAULT_ADMIN_PORT = 8080
# The admin listens on all interfaces (supervisor.build_supervisor).
ADMIN_BIND_HOST = "0.0.0.0"
# Module name -> distribution that must provide it.
REQUIRED_IMPORTS = (
    ("curl_cffi", "curl_cffi"),
    ("discord", "py-cord"),
    ("fastapi", "fastapi"),
    ("uvicorn", "uvicorn"),
)

SECRET_MARKERS = ("TOKEN", "PASSWORD", "SECRET")
MASK = "********"

PASS, WARN, FAIL, INFO = "PASS", "WARN", "FAIL", "INFO"
# A backup older than this is a doctor warning.
BACKUP_MAX_AGE = 2 * 24 * 3600
DOCS_URL = "https://copycord.github.io/Copycord/docs/cli"


# (group, [(command, one-line description)]) for the top-level help.
COMMAND_GROUPS = (
    ("Get started", (
        ("run", "Run Copycord (dashboard, server bot and client)"),
        ("restart", "Restart the Copycord service to apply changed settings"),
        ("doctor", "Check this machine can run Copycord"),
    )),
    ("Settings", (
        ("config", "Read or write settings (get, set, unset, export, import)"),
    )),
    ("Service", (
        ("service", "Run Copycord as an OS service (install, start, stop, status)"),
    )),
    ("Updates", (
        ("update", "Update Copycord; switch channel with --channel stable|beta|dev"),
    )),
    ("Remove", (
        ("uninstall", "Remove the service and the program; --purge deletes the data"),
    )),
)

EXAMPLES = (
    ("copycord run", "start Copycord in this terminal"),
    ("copycord service install", "run it in the background"),
    ("copycord update status", "see whether an update is available"),
    ("copycord update --channel beta", "switch to pre-releases"),
    ("copycord update --channel dev --ref main", "install from git"),
)


# prog -> examples for `copycord <command> --help`.
COMMAND_EXAMPLES = {
    "copycord run": (
        ("copycord run", "start Copycord in this terminal"),
        ("copycord run --data-dir ~/copycord", "use another data directory"),
    ),
    "copycord restart": (
        ("copycord config set PASSWORD hunter2", "change a setting"),
        ("copycord restart", "apply it"),
    ),
    "copycord doctor": (
        ("copycord doctor", "check everything"),
        ("copycord doctor --verbose", "also show the paths checked"),
    ),
    "copycord config": (
        ("copycord config set PASSWORD hunter2", "change the dashboard password"),
        ("copycord config get ADMIN_PORT", "read one setting"),
        ("copycord config export backup.json", "save the app config (unmasked)"),
        ("copycord config import backup.json", "load it back"),
    ),
    "copycord service": (
        ("copycord service install", "install, enable and start the service"),
        ("copycord service install --user", "Linux: a per-user unit, no sudo"),
        ("copycord service status", "is it installed and running?"),
        ("copycord service stop", "stop it"),
    ),
    "copycord update": (
        ("copycord update status", "see whether an update is available"),
        ("copycord update", "update on the saved channel"),
        ("copycord update --channel beta", "switch to pre-releases"),
        ("copycord update --channel dev --ref v4-dev", "install a git branch"),
        ("copycord update --channel dev --repo you/Copycord --ref 1a2b3c4", "a fork's commit"),
        ("copycord update --tag 4.0.0", "install one exact version"),
    ),
    "copycord uninstall": (
        ("copycord uninstall --dry-run", "show what would be removed"),
        ("copycord uninstall", "remove the service and the program"),
        ("copycord uninstall --purge", "also delete the data"),
        ("copycord uninstall --yes --keep-service", "no prompts, keep the service"),
    ),
}

# prog -> (section title, [(name, text)]) printed before the examples.
COMMAND_NOTES = {
    "copycord update": ("Channels", (
        ("stable", "newest GitHub release"),
        ("beta", "newest GitHub release, pre-releases included"),
        ("dev", "a git branch, tag or commit (needs npm to build the dashboard)"),
    )),
}


def _version() -> str:
    try:
        from copycord.common.config import CURRENT_VERSION

        return CURRENT_VERSION
    except Exception:
        return "unknown"


def format_help() -> str:
    from copycord import ui

    import io

    buf = io.StringIO()
    out = sys.stdout

    def w(text: str = "") -> None:
        buf.write(text + "\n")

    if ui.use_color(out):
        logo = "\n".join(ui.paint(r, "accent", "bold") for r in ui.LOGO)
        w()
        w(logo)
        w(f"  {ui.dim(_version() + '  ' + ui.TAGLINE)}")
    else:
        w(f"Copycord {_version()}")
    w()
    w(f"{ui.bold('Usage:')} copycord <command> [options]")
    width = max(len(c) for _, cmds in COMMAND_GROUPS for c, _ in cmds)
    for group, cmds in COMMAND_GROUPS:
        w()
        w(ui.accent(group + ":"))
        for name, text in cmds:
            w(f"  {ui.cmd(name.ljust(width))}  {text}")
    w()
    w(ui.accent("Examples:"))
    ex_width = max(len(c) for c, _ in EXAMPLES)
    for example, text in EXAMPLES:
        w(f"  {ui.cmd(example.ljust(ex_width))}  {ui.dim(text)}")
    w()
    w(ui.accent("Options:"))
    opt_width = max(width, len("-V, --version"))
    w(f"  {ui.cmd('-h, --help'.ljust(opt_width))}  Show this help")
    w(f"  {ui.cmd('-V, --version'.ljust(opt_width))}  Print the version and the update channel")
    w()
    w(ui.dim("Run `copycord <command> --help` for the options of one command."))
    w(ui.dim(f"Docs: {DOCS_URL}"))
    return buf.getvalue()


def _sentence(text: str) -> str:
    return text[:1].upper() + text[1:] if text else ""


def _invocation(action: argparse.Action) -> str:
    """`--channel stable|beta|dev`, `-y, --yes`, `--data-dir PATH`."""
    flags = ", ".join(sorted(action.option_strings, key=len))
    if action.nargs == 0:
        return flags
    if action.choices:
        return f"{flags} {'|'.join(action.choices)}"
    return f"{flags} {action.metavar or action.dest.upper()}"


def format_command_help(parser: argparse.ArgumentParser) -> str:
    """`copycord <command> --help` in the style of the top-level overview."""
    import io
    import textwrap

    from copycord import ui

    buf = io.StringIO()

    def w(text: str = "") -> None:
        buf.write(text + "\n")

    def rows(title: str, items, style=ui.cmd) -> None:
        if not items:
            return
        width = min(max(len(n) for n, _ in items), 34)
        w()
        w(ui.accent(title + ":"))
        for name, text in items:
            lines = textwrap.wrap(text, max(30, 78 - width - 4)) or [""]
            if len(name) > width:
                w(f"  {style(name)}")
            else:
                w(f"  {style(name.ljust(width))}  {lines.pop(0)}".rstrip())
            for x in lines:
                w(f"  {' ' * width}  {x}")

    w(ui.accent(parser.prog))
    if parser.description:
        for x in textwrap.wrap(parser.description, 78):
            w(x)

    commands, arguments, options = [], [], []
    usage = [parser.prog]
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            usage.append("<command>")
            for choice in action._choices_actions:
                commands.append((choice.dest, _sentence(choice.help or "")))
        elif not action.option_strings:
            name = "|".join(action.choices) if action.choices else (action.metavar or action.dest).upper()
            usage.append(f"[{name}]" if action.nargs == "?" else name)
            arguments.append((name, _sentence(action.help or "")))
        elif not isinstance(action, argparse._HelpAction):
            options.append((_invocation(action), _sentence(action.help or "")))
    options.append(("-h, --help", "Show this help"))
    usage.append("[options]")
    w()
    w(f"{ui.bold('Usage:')} {' '.join(usage)}")
    rows("Commands", commands)
    rows("Arguments", arguments)
    rows("Options", options)
    note = COMMAND_NOTES.get(parser.prog)
    if note:
        rows(note[0], note[1])
    examples = COMMAND_EXAMPLES.get(parser.prog, ())
    if examples:
        width = min(max(len(c) for c, _ in examples), 44)
        w()
        w(ui.accent("Examples:"))
        for example, text in examples:
            if len(example) > width:
                w(f"  {ui.cmd(example)}")
                w(f"  {' ' * width}  {ui.dim(text)}")
            else:
                w(f"  {ui.cmd(example.ljust(width))}  {ui.dim(text)}")
    w()
    if commands:
        w(ui.dim(f"Run `{parser.prog} <command> --help` for the options of one command."))
    w(ui.dim(f"Docs: {DOCS_URL}"))
    return buf.getvalue()


def print_version() -> None:
    from copycord import ui
    from copycord.update import installed_commit, load_channel

    version = _version()
    commit = installed_commit()
    if commit:
        version = f"{version} ({commit[:7]})"
    if ui.use_color():
        ui.banner(version)
        ui.kv([("version", version), ("channel", load_channel().describe())])
    else:
        print(version)
        print(f"channel {load_channel().describe()}")
    if sys.stdout.isatty():
        from copycord import update_check

        update_check.start(wait=2.0)


class _VersionAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        print_version()
        parser.exit()


class _Parser(argparse.ArgumentParser):
    def format_help(self) -> str:
        if self.prog == "copycord":
            return format_help()
        return super().format_help()

    def error(self, message: str) -> NoReturn:
        """Top level: a styled one-liner plus a pointer to the help, not argparse's usage dump."""
        if self.prog != "copycord":
            super().error(message)
        from copycord import ui

        if "invalid choice" in message:
            bad = message.split("invalid choice: ", 1)[-1].split(" ", 1)[0].strip("'\"")
            text = f"unknown command '{bad}'"
        else:
            text = message
        ui.fail(text, f"run {ui.cmd('copycord --help', stream=sys.stderr)} to see the commands", stream=sys.stderr)
        self.exit(2)


class _SubParser(argparse.ArgumentParser):
    def format_help(self) -> str:
        return format_command_help(self)

    def error(self, message: str) -> NoReturn:
        """A command group without its subcommand shows the group's help; other
        mistakes get a styled one-liner and a pointer to the help."""
        if message.startswith("the following arguments are required:") and message.endswith("_command"):
            sys.stdout.write(self.format_help())
            self.exit(0)
        from copycord import ui

        ui.fail(message, f"run {ui.cmd(self.prog + ' --help', stream=sys.stderr)} for the options", stream=sys.stderr)
        self.exit(2)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    if not (sys.argv[1:] if argv is None else argv):
        # Bare `copycord`: the styled overview, not an argparse error.
        sys.stdout.write(format_help())
        raise SystemExit(0)
    parser = _Parser(prog="copycord")
    parser.add_argument("-V", "--version", action=_VersionAction, nargs=0,
                        help="print the version and the update channel")
    sub = parser.add_subparsers(dest="command", required=True, parser_class=_SubParser)

    data_dir = argparse.ArgumentParser(add_help=False)
    data_dir.add_argument(
        "--data-dir", metavar="PATH", help="data directory (sets DATA_DIR)"
    )

    sub.add_parser(
        "run",
        parents=[data_dir],
        help="run Copycord",
        description="Run the dashboard, the server bot and the client. "
        "Finish the setup in the dashboard.",
    )
    sub.add_parser(
        "restart",
        help="restart the Copycord service",
        description="Restart the OS service so it applies changed settings; "
        "<home>/.env is read on every start.",
    )
    doctor_p = sub.add_parser(
        "doctor",
        parents=[data_dir],
        help="check this machine can run Copycord",
        description="Check the install, the settings, the data, whether Copycord "
        "is running and whether an update is out. Exits 1 when a check fails.",
    )
    doctor_p.add_argument("--verbose", "-v", action="store_true",
                          help="show the path behind every check")

    config = sub.add_parser(
        "config",
        help="read or write settings",
        description="Env-only keys (" + ", ".join(ENV_KEYS) + ", BACKFILL_*, "
        "FORWARDING_*) live in <home>/.env; every other key is stored in the "
        "app config in the database.",
    )
    csub = config.add_subparsers(dest="config_command", required=True)
    get = csub.add_parser("get", parents=[data_dir], help="print one value")
    get.add_argument("key")
    get.add_argument("--show-secrets", action="store_true",
                     help="print token/password/secret values unmasked")
    set_ = csub.add_parser("set", parents=[data_dir], help="store one value")
    set_.add_argument("key")
    set_.add_argument("value")
    unset = csub.add_parser("unset", parents=[data_dir], help="remove one value")
    unset.add_argument("key")
    export = csub.add_parser(
        "export", parents=[data_dir], help="write all values as JSON"
    )
    export.add_argument(
        "file", nargs="?", help="write to FILE (unmasked) instead of stdout"
    )
    export.add_argument(
        "--show-secrets",
        action="store_true",
        help="print token/password/secret values on stdout",
    )
    imp = csub.add_parser(
        "import", parents=[data_dir], help="store values from a JSON object"
    )
    imp.add_argument("file")

    user = argparse.ArgumentParser(add_help=False)
    user.add_argument(
        "--user",
        action="store_true",
        help="Linux: per-user systemd unit (systemctl --user) instead of a system one",
    )
    service = sub.add_parser(
        "service",
        help="run Copycord as an OS service",
        description="Run Copycord in the background as a systemd unit (Linux), "
        "a launchd agent (macOS) or a Scheduled Task (Windows).",
    )
    ssub = service.add_subparsers(dest="service_command", required=True)
    ssub.add_parser(
        "install", parents=[user, data_dir], help="install, enable and start the service"
    )
    for name, text in (
        ("start", "start the service"),
        ("stop", "stop the service"),
        ("status", "print whether the service is installed and running"),
        ("uninstall", "stop and remove the service"),
    ):
        ssub.add_parser(name, parents=[user], help=text)

    update = sub.add_parser(
        "update",
        parents=[data_dir],
        help="update Copycord",
        description="Update Copycord from the saved channel (default stable), or "
        "switch channel. The channel is saved in <data dir>/update.json.",
    )
    update.add_argument(
        "action",
        nargs="?",
        choices=("status",),
        help="print the installed version, the channel and the latest on it",
    )
    target = update.add_mutually_exclusive_group()
    target.add_argument(
        "--channel",
        choices=("stable", "beta", "dev"),
        help="switch to this channel and save it (stable: GitHub releases, "
        "beta: GitHub pre-releases too, dev: a git ref, needs npm)",
    )
    target.add_argument(
        "--tag",
        metavar="VERSION",
        help="install this exact version once; the saved channel does not change",
    )
    update.add_argument(
        "--ref",
        dest="branch",
        metavar="REF",
        help="dev channel: the git branch, tag or commit to install (default main)",
    )
    update.add_argument(
        "--repo",
        metavar="OWNER/NAME",
        help="dev channel: the GitHub repo to install from (default Copycord/Copycord)",
    )
    update.add_argument(
        "--no-install",
        action="store_true",
        help="only save --channel/--repo/--ref, install nothing (used by the install scripts)",
    )
    update.add_argument(
        "--no-backup",
        action="store_true",
        help="skip the database backup made before every update",
    )
    update.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="install an older version without asking (needed with no terminal)",
    )

    uninst = sub.add_parser(
        "uninstall",
        parents=[data_dir],
        help="remove Copycord from this machine",
        description="Stop and remove the OS service, remove the copycord program "
        "(uv tool or pipx) and the saved update channel. The Copycord home "
        "(~/.copycord or COPYCORD_HOME: data, src, node) stays unless --purge is given.",
    )
    uninst.add_argument("--purge", action="store_true",
                        help="also delete the Copycord home (database, logs, backups, src, node)")
    uninst.add_argument("--yes", "-y", action="store_true",
                        help="skip the confirmation prompts (needed without a terminal)")
    uninst.add_argument("--keep-service", action="store_true",
                        help="leave the OS service installed")
    uninst.add_argument("--dry-run", action="store_true",
                        help="print the steps and change nothing")
    return parser.parse_args(argv)


def check_removed_options() -> bool:
    """False (after logging why) when the removed ROLE variable is still set."""
    if "ROLE" in os.environ:
        logger.error(ROLE_REMOVED_MESSAGE)
        return False
    return True


def apply_data_dir(args: argparse.Namespace) -> None:
    """--data-dir PATH overrides DATA_DIR for this process and its roles."""
    path = getattr(args, "data_dir", None)
    if path:
        os.environ["DATA_DIR"] = str(Path(path).expanduser().resolve())


# --------------------------------------------------------------------------
# doctor
# --------------------------------------------------------------------------


class Check(NamedTuple):
    status: str
    name: str
    # Why it is not OK and what to do; shown for every status.
    reason: str = ""
    # The file or folder checked; shown on WARN/FAIL or with --verbose.
    path: str = ""


# --- Install ---------------------------------------------------------------


def check_version() -> Check:
    from copycord import ui
    from copycord.update import installed_commit, load_channel

    version = f"Copycord v{_version().lstrip('v')}"
    try:
        commit = installed_commit()
    except Exception:
        commit = None
    if commit:
        version += f" ({commit[:7]})"
    sep = " · " if ui.use_color() and ui._unicode(sys.stdout) else ", "
    try:
        channel = load_channel().describe()
    except Exception:
        channel = "unknown"
    return Check(PASS, f"{version}{sep}channel {channel}")


def check_python(version_info=None) -> Check:
    v = sys.version_info if version_info is None else version_info
    found = ".".join(str(p) for p in tuple(v)[:3])
    if MIN_PYTHON <= tuple(v)[:2] < MAX_PYTHON:
        return Check(PASS, f"Python {found}")
    low = ".".join(str(p) for p in MIN_PYTHON)
    high = f"{MAX_PYTHON[0]}.{MAX_PYTHON[1] - 1}"
    return Check(FAIL, f"Python {found}", f"Copycord needs Python {low} to {high}")


def admin_static_dir() -> Optional[Path]:
    """admin/static of the importable admin package, without importing copycord.admin.app."""
    try:
        spec = importlib.util.find_spec("copycord.admin")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.submodule_search_locations:
        return None
    return Path(list(spec.submodule_search_locations)[0]) / "static"


def check_frontend(static_dir: Optional[Path] = None) -> Check:
    static_dir = admin_static_dir() if static_dir is None else static_dir
    if static_dir is None:
        return Check(FAIL, "web UI missing", "the admin package cannot be found")
    main_js = static_dir / "assets" / "main.js"
    if main_js.is_file():
        return Check(PASS, "web UI included", path=str(static_dir))
    return Check(
        FAIL,
        "web UI missing",
        "the dashboard will not load; reinstall Copycord (a git install needs npm "
        "to build it)",
        str(main_js),
    )


def check_import(module: str, dist: str) -> Check:
    from importlib import metadata

    try:
        importlib.import_module(module)
    except Exception as e:
        return Check(FAIL, dist, f"cannot import {module}: {e!r}")
    try:
        version = metadata.version(dist)
    except metadata.PackageNotFoundError:
        return Check(WARN, dist, f"{module} imports, but {dist} is not installed (wrong package?)")
    return Check(PASS, dist, version)


def check_dependencies(imports=None) -> Check:
    """One line for every REQUIRED_IMPORTS; names only the broken ones."""
    imports = REQUIRED_IMPORTS if imports is None else imports
    bad = [c for c in (check_import(m, d) for m, d in imports) if c.status != PASS]
    if not bad:
        return Check(PASS, "dependencies OK")
    status = FAIL if any(c.status == FAIL for c in bad) else WARN
    names = ", ".join(c.name for c in bad)
    return Check(
        status,
        f"missing dependencies: {names}",
        "; ".join(c.reason for c in bad) + "; reinstall Copycord to repair them",
    )


# --- Settings --------------------------------------------------------------


def check_role(env: Optional[Mapping[str, str]] = None) -> Check:
    env = os.environ if env is None else env
    if "ROLE" in env:
        return Check(FAIL, "ROLE is set", ROLE_REMOVED_MESSAGE)
    return Check(PASS, "ROLE not set")


def check_env_file(path: Optional[Path] = None) -> Check:
    from copycord import envfile

    path = envfile.env_path() if path is None else path
    if path.is_file():
        return Check(PASS, ".env found", path=str(path))
    return Check(
        WARN,
        ".env not found",
        f"{path} is missing, so every setting is at its default; "
        "`copycord config set PASSWORD <new>` creates it",
        str(path),
    )


def check_password(env: Optional[Mapping[str, str]] = None) -> Check:
    from copycord.envfile import DEFAULT_PASSWORD

    env = os.environ if env is None else env
    value = env.get("PASSWORD")
    if value == DEFAULT_PASSWORD:
        return Check(
            WARN,
            "dashboard password is the default",
            "change it with `copycord config set PASSWORD <new>`, then `copycord restart`",
        )
    if not value:
        return Check(INFO, "no dashboard password", "the dashboard has no login page")
    return Check(PASS, "dashboard password set")


def admin_port(env: Optional[Mapping[str, str]] = None) -> int:
    """ADMIN_PORT when it is valid, else the default."""
    env = os.environ if env is None else env
    try:
        port = int(env.get("ADMIN_PORT") or DEFAULT_ADMIN_PORT)
    except ValueError:
        return DEFAULT_ADMIN_PORT
    return port if 0 < port < 65536 else DEFAULT_ADMIN_PORT


def check_tokens(env: Optional[Mapping[str, str]] = None) -> Check:
    """SERVER_TOKEN / CLIENT_TOKEN as the app reads them: the DB app config,
    then the environment. The values are never printed."""
    from copycord.update_check import config_value

    env = os.environ if env is None else env
    missing = [
        key
        for key in ("SERVER_TOKEN", "CLIENT_TOKEN")
        if not (config_value(key).strip() or (env.get(key) or "").strip())
    ]
    if not missing:
        return Check(PASS, "Discord tokens set")
    return Check(
        WARN,
        f"{' and '.join(missing)} not set yet",
        f"finish setup at http://localhost:{admin_port(env)}",
    )


# --- Data ------------------------------------------------------------------


def check_data_dir(path: Path) -> Check:
    if path.exists():
        if not path.is_dir():
            return Check(FAIL, "home is not a directory", path=str(path))
        try:
            with tempfile.TemporaryFile(dir=path):
                pass
        except OSError as e:
            return Check(FAIL, "home is not writable", str(e), str(path))
        return Check(PASS, "home writable", path=str(path))
    parent = path.parent
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent
    if parent.is_dir() and os.access(parent, os.W_OK | os.X_OK):
        return Check(PASS, "home will be created on first run", path=str(path))
    return Check(
        FAIL, "home cannot be created", f"{parent} is not writable", str(path)
    )


def check_db(path: Path) -> Check:
    if not path.exists():
        return Check(INFO, "database created on first run", path=str(path))
    try:
        conn = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        try:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        finally:
            conn.close()
    except sqlite3.Error as e:
        return Check(FAIL, "database does not open", str(e), str(path))
    from copycord.common.db import SCHEMA_VERSION, backups_dir, stored_schema_version

    try:
        stored = stored_schema_version(str(path))
    except sqlite3.Error:
        stored = None
    if stored is not None and stored > SCHEMA_VERSION:
        return Check(
            FAIL,
            f"database schema {stored} is newer than this Copycord supports ({SCHEMA_VERSION})",
            "run `copycord update` to go back to a newer version, or restore a "
            f"backup from {backups_dir()}",
            str(path),
        )
    if "app_config" not in tables:
        return Check(WARN, "database has no app_config table yet", path=str(path))
    at = f" at schema {stored}" if stored is not None else ""
    return Check(PASS, f"database OK{at}", path=str(path))


def _age(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 3600:
        n, unit = max(1, seconds // 60), "minute"
    elif seconds < 2 * 86400:
        n, unit = seconds // 3600, "hour"
    else:
        n, unit = seconds // 86400, "day"
    return f"{n} {unit}{'' if n == 1 else 's'}"


def check_backups(
    backup_dir: Optional[Path] = None,
    db: Optional[Path] = None,
    now: Optional[float] = None,
) -> Check:
    """The newest daily (*.tar.gz) or pre-update (*.db) backup."""
    import time

    from copycord.common.db import backups_dir
    from copycord.common.paths import db_path

    backup_dir = backups_dir() if backup_dir is None else backup_dir
    db = db_path() if db is None else db
    now = time.time() if now is None else now
    times = []
    if backup_dir.is_dir():
        for pattern in ("*.tar.gz", "*.db"):
            for p in backup_dir.glob(pattern):
                if not p.name.startswith("."):
                    try:
                        times.append(p.stat().st_mtime)
                    except OSError:
                        pass
    hint = "the daily backup runs at BACKUP_AT (03:17 by default) while Copycord is running"
    if not times:
        # A new install: no DB yet, or the backups folder (made on the first
        # start, empty since) is younger than the backup interval allows.
        try:
            fresh = (
                not db.exists()
                or not backup_dir.is_dir()
                or now - backup_dir.stat().st_ctime < BACKUP_MAX_AGE
            )
        except OSError:
            fresh = True
        if fresh:
            return Check(INFO, "no backups yet", path=str(backup_dir))
        return Check(WARN, "no backups yet", hint, str(backup_dir))
    age = now - max(times)
    if age > BACKUP_MAX_AGE:
        return Check(WARN, f"last backup {_age(age)} ago", hint, str(backup_dir))
    return Check(PASS, f"last backup {_age(age)} ago", path=str(backup_dir))


# --- Running ---------------------------------------------------------------


def copycord_on_port(port: int, timeout: float = 1.0) -> bool:
    """True when the Copycord dashboard answers on 127.0.0.1:port."""
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/version", timeout=timeout) as r:
            data = json.loads(r.read(4096).decode("utf-8", "replace"))
        return isinstance(data, dict) and "current" in data
    except urllib.error.HTTPError as e:
        # Behind the login page.
        return e.code in (401, 403) or "login" in (e.headers.get("location") or "")
    except Exception:
        return False


def check_admin_port(
    env: Optional[Mapping[str, str]] = None,
    host: str = ADMIN_BIND_HOST,
    service_running: bool = False,
    probe=None,
) -> Check:
    env = os.environ if env is None else env
    raw = env.get("ADMIN_PORT") or str(DEFAULT_ADMIN_PORT)
    try:
        port = int(raw)
        if not 0 < port < 65536:
            raise ValueError
    except ValueError:
        return Check(FAIL, f"ADMIN_PORT={raw!r} is not a valid port")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Same as uvicorn: lingering TIME_WAIT sockets do not count as in use.
        # (On Windows SO_REUSEADDR would allow sharing a live port, so skip it.)
        if sys.platform != "win32":
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
        except OSError as e:
            probe = copycord_on_port if probe is None else probe
            if service_running or probe(port):
                return Check(PASS, f"port {port} in use by Copycord")
            return Check(
                WARN,
                f"port {port} in use",
                f"{e.strerror or e}; another program has it, so set ADMIN_PORT to a free port",
            )
    return Check(PASS, f"admin port {port} free")


def service_state():
    """The OS service state, or None when it cannot be read."""
    try:
        from copycord import service

        return service.service_state()
    except Exception:
        return None


def check_service(state) -> Check:
    if state is None:
        return Check(INFO, "service state unknown")
    if not state.installed:
        return Check(INFO, "service not installed",
                     "`copycord service install` runs Copycord in the background")
    if state.running:
        return Check(PASS, "service running", path=state.where)
    return Check(WARN, "service installed, not running",
                 "start it with `copycord service start`", state.where)


# --- Updates ---------------------------------------------------------------


def check_updates(timeout: float = 2.0) -> Check:
    """The cached update check; a network error is INFO, never a failure."""
    from copycord import update_check

    try:
        if not update_check.check_enabled():
            return Check(INFO, "update check off", "COPYCORD_NO_UPDATE_CHECK is set")
        result = update_check.check_cached(timeout=timeout)
    except Exception as e:
        return Check(INFO, "could not check for updates", str(e).splitlines()[0][:120] if str(e) else type(e).__name__)
    if result.available:
        return Check(INFO, update_check.hint_text(result).split(". Run")[0],
                     "install it with `copycord update`")
    return Check(PASS, "up to date")


def run_doctor() -> List[tuple]:
    """[(section, [Check])] in the order the doctor prints them."""
    from copycord.common.paths import data_dir, db_path

    settings = [check_env_file(), check_password(), check_tokens()]
    role = check_role()
    if role.status == FAIL:
        settings.insert(0, role)
    state = service_state()
    return [
        ("Install", [check_version(), check_python(), check_frontend(), check_dependencies()]),
        ("Settings", settings),
        ("Data", [check_data_dir(data_dir()), check_db(db_path()), check_backups()]),
        ("Running", [
            check_admin_port(service_running=bool(state and state.running)),
            check_service(state),
        ]),
        ("Updates", [check_updates()]),
    ]


def doctor_summary(checks: List[Check]) -> str:
    warns = sum(c.status == WARN for c in checks)
    fails = sum(c.status == FAIL for c in checks)
    if not warns and not fails:
        return "All good"
    return (f"{warns} warning{'' if warns == 1 else 's'}, "
            f"{fails} problem{'' if fails == 1 else 's'}")


def doctor(verbose: bool = False) -> int:
    from copycord import ui

    sections = run_doctor()
    if ui.use_color():
        ui.heading("Copycord doctor")
    status = {PASS: "ok", WARN: "warn", FAIL: "fail", INFO: "info"}
    checks: List[Check] = []
    for title, rows in sections:
        ui.heading(title)
        for c in rows:
            checks.append(c)
            parts = [c.reason]
            if c.path and (verbose or c.status in (WARN, FAIL)):
                parts.append(c.path)
            ui.line(status[c.status], c.name, "; ".join(p for p in parts if p), stream=sys.stdout)
    failed = any(c.status == FAIL for c in checks)
    summary = doctor_summary(checks)
    print()
    if not ui.use_color():
        print(summary)
    elif failed:
        ui.fail(summary, stream=sys.stdout)
    elif summary != "All good":
        ui.warn(summary, stream=sys.stdout)
    else:
        ui.ok(summary, f"start it with {ui.cmd('copycord run')}")
    return 1 if failed else 0


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------


def is_secret(key: str) -> bool:
    upper = key.upper()
    return any(marker in upper for marker in SECRET_MARKERS)


def masked(values: Mapping[str, str]) -> dict:
    return {k: (MASK if is_secret(k) and v else v) for k, v in values.items()}


def _open_db(create: bool):
    """DBManager on the configured DB; None (after saying why) when it is missing."""
    from copycord.common.db import DBManager
    from copycord.common.paths import db_path

    path = db_path()
    if not path.exists():
        if not create:
            print(f"no database at {path} (run Copycord once, or check --data-dir)",
                  file=sys.stderr)
            return None
        path.parent.mkdir(parents=True, exist_ok=True)
    return DBManager(str(path), init_schema=True)


def _import_values(raw) -> dict:
    """Validated {key: str} from a JSON object; raises ValueError on bad input."""
    if not isinstance(raw, dict):
        raise ValueError("expected a JSON object of key/value pairs")
    values = {}
    for key, value in raw.items():
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError(f"{key}: value must be a string or a number")
        values[key] = str(value)
    return values


def restart_hint() -> str:
    """How to apply a changed setting."""
    return "apply it with: copycord restart"


NOT_A_SERVICE = (
    "Copycord is not running as a service. Stop it in its terminal (Ctrl+C) "
    "and run `copycord run` again; <home>/.env is read on every start."
)


def restart_command() -> int:
    """Restart the OS service when it is installed and running."""
    from copycord import service

    try:
        state = service.service_state()
    except Exception as e:
        print(f"copycord restart: could not read the service state: {e}", file=sys.stderr)
        state = None
    if state is None or not state.installed:
        print(NOT_A_SERVICE)
        return 1
    if not state.running:
        print(f"{state.line()}; start it with: copycord service start")
        return 1
    try:
        service.restart_service()
    except service.ServiceError as e:
        print(f"copycord restart: {e}", file=sys.stderr)
        return 1
    print(f"restarted the copycord service ({state.where})")
    return 0


def _print_value(key: str, value: str, show_secrets: bool) -> None:
    print(MASK if is_secret(key) and value and not show_secrets else value)


def env_config_command(args: argparse.Namespace) -> int:
    """config get/set/unset for the env-only keys in <home>/.env."""
    from copycord import envfile

    cmd, key = args.config_command, args.key
    if cmd == "get":
        value = envfile.get(key)
        if value is None:
            print(f"{key} is not set in {envfile.env_path()}", file=sys.stderr)
            return 1
        _print_value(key, value, args.show_secrets)
        return 0
    if cmd == "set":
        path = envfile.set_value(key, args.value)
        print(f"{key} saved in {path}; {restart_hint()}", file=sys.stderr)
        return 0
    if cmd == "unset":
        path = envfile.env_path()
        if not envfile.unset(key):
            print(f"{key} is not set in {path}", file=sys.stderr)
            return 1
        print(f"{key} removed from {path}; {restart_hint()}", file=sys.stderr)
        return 0
    return 2


def config_command(args: argparse.Namespace) -> int:
    from copycord.envfile import is_env_key

    cmd = args.config_command
    if cmd in ("get", "set", "unset") and is_env_key(args.key):
        return env_config_command(args)
    db = _open_db(create=cmd in ("set", "import"))
    if db is None:
        return 1
    try:
        if cmd == "get":
            value = db.get_config(args.key, None)
            if value is None:
                print(f"{args.key} is not set", file=sys.stderr)
                return 1
            _print_value(args.key, value, args.show_secrets)
            return 0

        if cmd == "set":
            db.set_config(args.key, args.value)
            print(f"{args.key} saved; {restart_hint()}", file=sys.stderr)
            return 0

        if cmd == "unset":
            if db.get_config(args.key, None) is None:
                print(f"{args.key} is not set", file=sys.stderr)
                return 1
            db.delete_config(args.key)
            print(f"{args.key} removed; {restart_hint()}", file=sys.stderr)
            return 0

        if cmd == "export":
            values = dict(sorted(db.get_all_config().items()))
            if args.file:
                out = Path(args.file).expanduser()
                out.write_text(json.dumps(values, indent=2) + "\n", encoding="utf-8")
                try:
                    os.chmod(out, 0o600)
                except OSError:
                    pass
                print(f"exported {len(values)} keys to {out}", file=sys.stderr)
                return 0
            shown = values if args.show_secrets else masked(values)
            print(json.dumps(shown, indent=2))
            return 0

        if cmd == "import":
            try:
                raw = json.loads(Path(args.file).expanduser().read_text(encoding="utf-8"))
                values = _import_values(raw)
            except (OSError, ValueError) as e:
                print(f"cannot import {args.file}: {e}", file=sys.stderr)
                return 1
            imported = 0
            for key, value in values.items():
                if value == MASK:
                    print(f"skipped {key}: masked value from a stdout export",
                          file=sys.stderr)
                    continue
                db.set_config(key, value)
                imported += 1
            print(f"imported {imported} keys; {restart_hint()}", file=sys.stderr)
            return 0
    finally:
        db.conn.close()
    return 2


# --------------------------------------------------------------------------
# uninstall
# --------------------------------------------------------------------------

INSTALL_SCRIPT = (
    "curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh"
)


def purge_refusal(path: Path) -> Optional[str]:
    """Why --purge must not delete path; None when it may."""
    if not str(path).strip() or str(path) == ".":
        return "the Copycord home path is empty"
    resolved = path.expanduser().resolve()
    if resolved == Path(resolved.anchor):
        return f"{resolved} is a filesystem root"
    if resolved == Path.home().resolve():
        return f"{resolved} is your home directory"
    return None


def _forget_saved_home(removed: list) -> None:
    """Drop the home pointer the installer saved for a custom --home."""
    from copycord.common.paths import HOME_POINTER

    pointer = Path.home() / HOME_POINTER
    try:
        pointer.unlink()
        removed.append(f"saved home pointer: {pointer}")
    except OSError:
        pass


def _uninstall_service(dry_run: bool, removed: list, kept: list) -> bool:
    from copycord import ui
    from copycord.service import ServiceError, SystemdService, get_service

    if sys.platform in ("win32", "darwin"):
        services = [get_service()]
    else:
        services = [SystemdService(True), SystemdService(False)]
    ok = True
    for svc in services:
        if not svc.state().installed:
            ui.info("service not installed", svc.where)
            continue
        if dry_run:
            ui.info("would stop and remove the service", svc.where)
            removed.append(f"service: {svc.where}")
            continue
        try:
            for note in svc.uninstall():
                ui.ok("service", note)
            removed.append(f"service: {svc.where}")
        except ServiceError as e:
            ui.fail("service not removed", str(e))
            kept.append(f"service: {svc.where}")
            ok = False
    return ok


def _uninstall_program(dry_run: bool, purge: bool, removed: list, kept: list) -> bool:
    import shutil

    from copycord import ui
    from copycord.service import run_cmd
    from copycord.update import settings_path

    ok = True
    channel_file = settings_path()
    if not purge and channel_file.exists():
        if dry_run:
            ui.info("would remove the update channel file", str(channel_file))
            removed.append(f"update channel file: {channel_file}")
        else:
            try:
                channel_file.unlink()
                ui.ok("removed the update channel file", str(channel_file))
                removed.append(f"update channel file: {channel_file}")
            except OSError as e:
                ui.fail("update channel file not removed", f"{channel_file}: {e}")
                kept.append(f"update channel file: {channel_file}")
                ok = False

    commands = []
    if shutil.which("uv"):
        commands.append(["uv", "tool", "uninstall", "copycord"])
    if shutil.which("pipx"):
        commands.append(["pipx", "uninstall", "copycord"])
    manual = f'"{sys.executable}" -m pip uninstall copycord'
    if not commands:
        ui.warn("neither uv nor pipx is on PATH", f"remove the program by hand: {manual}")
        kept.append(f"copycord program (remove it with: {manual})")
        return ok
    if dry_run:
        ui.info("would run", " ".join(commands[0]))
        removed.append("copycord program")
        return ok
    if sys.platform == "win32":
        # Windows cannot delete the running copycord.exe, so the uninstall runs
        # in a detached shell once this process has exited.
        import subprocess

        text = " ".join(commands[0])
        flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        subprocess.Popen(
            ["cmd", "/c", f"timeout /t 3 /nobreak >nul & {text}"],
            creationflags=flags,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        ui.ok("removing the copycord program after this command exits", text)
        removed.append("copycord program")
        return ok
    for command in commands:
        text = " ".join(command)
        res = run_cmd(command)
        if res.returncode == 0:
            ui.ok("removed the copycord program", text)
            removed.append("copycord program")
            return ok
        detail = (res.stderr or res.stdout or "").strip().splitlines()
        ui.warn(f"{text} failed", detail[-1] if detail else f"exit {res.returncode}")
    kept.append(f"copycord program (remove it with: {manual})")
    return False


def uninstall_command(args: argparse.Namespace) -> int:
    from copycord.common.paths import data_dir
    from copycord import ui

    purge, dry_run, yes = args.purge, args.dry_run, args.yes
    target = data_dir()
    if purge:
        why = purge_refusal(target)
        if why:
            ui.fail("refusing to delete the Copycord home", why)
            return 1
    if not yes and not dry_run and not ui.is_interactive():
        ui.fail(
            "copycord uninstall needs a confirmation",
            "no terminal to ask in; rerun with --yes to uninstall without prompts",
        )
        return 1

    if ui.use_color():
        ui.banner(_version())
    ui.heading("Uninstall Copycord" + (" (dry run, nothing changes)" if dry_run else ""))
    if not args.keep_service:
        ui.line("step", "stop and remove the OS service")
    if purge:
        ui.line("step", "delete the Copycord home, including src and node", str(target))
    ui.line("step", "remove the copycord program and its update channel")
    print()

    if not yes and not dry_run:
        try:
            if not ui.confirm("Uninstall Copycord?", default=False):
                ui.warn("uninstall cancelled", "nothing was changed")
                return 1
            if purge:
                typed = ui.ask(f"Type DELETE to remove {target} and everything in it")
                if typed != "DELETE":
                    ui.warn("uninstall cancelled", "DELETE was not typed; nothing was changed")
                    return 1
        except (KeyboardInterrupt, EOFError):
            print()
            ui.warn("uninstall cancelled", "nothing was changed")
            return 130

    removed: list = []
    kept: list = []
    ok = True
    if args.keep_service:
        kept.append("OS service (--keep-service)")
    else:
        ok = _uninstall_service(dry_run, removed, kept) and ok

    if not purge:
        kept.append(f"Copycord home: {target} (add --purge to delete it)")
    elif not target.exists():
        ui.info("Copycord home does not exist", str(target))
    elif dry_run:
        ui.info("would delete the Copycord home", str(target))
        removed.append(f"Copycord home: {target}")
    else:
        import shutil
        import stat

        def _clear_readonly(func, path, _exc):
            # git marks pack files read-only; Windows refuses to delete those.
            os.chmod(path, stat.S_IWRITE)
            func(path)

        try:
            shutil.rmtree(target, onerror=_clear_readonly)
            ui.ok("deleted the Copycord home", str(target))
            removed.append(f"Copycord home: {target}")
            _forget_saved_home(removed)
        except OSError as e:
            ui.fail("Copycord home not deleted", f"{target}: {e}")
            kept.append(f"Copycord home: {target}")
            ok = False

    ok = _uninstall_program(dry_run, purge, removed, kept) and ok

    lines = [ui.bold("Would remove" if dry_run else "Removed")]
    lines += [f"  {x}" for x in removed or ["nothing"]]
    lines += ["", ui.bold("Kept")]
    lines += [f"  {x}" for x in kept or ["nothing"]]
    lines += ["", ui.bold("Reinstall"), f"  {ui.cmd(INSTALL_SCRIPT)}"]
    if dry_run:
        title, status = "Dry run: nothing changed", "info"
    elif ok:
        title, status = "Copycord uninstalled", "ok"
    else:
        title, status = "Uninstall incomplete", "warn"
    print()
    ui.box(lines, title=title, status=status)
    return 0 if ok else 1


def schema_too_new_message(err) -> None:
    """The refusal `copycord run` prints for a DB written by a newer Copycord."""
    from copycord import ui
    from copycord.common.db import backups_dir, pre_update_backups

    where = backups_dir()
    lines = [
        f"This database was created by a newer Copycord (schema {err.stored}, "
        f"this version supports {err.supported}).",
        f"Run {ui.cmd('copycord update', stream=sys.stderr)} to go back to a newer "
        f"version, or restore a backup from {where}.",
        f"Database: {err.db_path}",
    ]
    try:
        newest = pre_update_backups(where)
    except OSError:
        newest = []
    if newest:
        lines.append(f"Newest pre-update backup: {newest[0]}")
    ui.box(lines, title="Database is newer than Copycord", status="fail", stream=sys.stderr)


def schema_guard() -> bool:
    """False (after saying why) when the DB was written by a newer Copycord."""
    from copycord.common.db import SchemaTooNewError, check_schema_version
    from copycord.common.paths import db_path

    try:
        check_schema_version(str(db_path()))
    except SchemaTooNewError as e:
        schema_too_new_message(e)
        return False
    return True


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    apply_data_dir(args)
    # <home>/.env before any role starts; the real environment wins.
    from copycord import envfile

    envfile.load()

    if args.command == "doctor":
        return doctor(verbose=args.verbose)

    if args.command == "restart":
        return restart_command()

    if args.command == "config":
        return config_command(args)

    if args.command == "service":
        from copycord.service import service_command

        return service_command(args)

    if args.command == "uninstall":
        return uninstall_command(args)

    if args.command == "update":
        from copycord.update import update

        return update(
            check_only=args.action == "status",
            channel=args.channel,
            branch=args.branch,
            tag=args.tag,
            no_install=args.no_install,
            repo=args.repo,
            no_backup=args.no_backup,
            yes=args.yes,
        )

    if args.command == "run":
        from copycord.logging_setup import setup_logging

        # Console only here; supervisor.run adds the per-role files once DATA_DIR is set.
        setup_logging(roles=())
        if not check_removed_options():
            return 1
        # Before any role starts, so the dashboard never comes up half-way.
        if not schema_guard():
            return 1

        from copycord import supervisor, update_check
        from copycord.common.db import SchemaTooNewError

        update_check.start()
        try:
            asyncio.run(supervisor.run())
        except KeyboardInterrupt:
            pass
        except SchemaTooNewError as e:
            schema_too_new_message(e)
            return 1
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
