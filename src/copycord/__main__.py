# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
The copycord command line: run, restart, doctor, config, service, update and
uninstall.
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
# First Python release Copycord is not tested on.
MAX_PYTHON = (3, 15)
DEFAULT_ADMIN_PORT = 8080
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
BACKUP_MAX_AGE = 2 * 24 * 3600
DOCS_URL = "https://copycord.github.io/Copycord/docs/cli"


# (group, [(command, description)]) for the top-level help.
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
        ("update", "Update Copycord to the latest version"),
    )),
    ("Remove", (
        ("uninstall", "Remove Copycord; asks whether to delete your data"),
    )),
)

EXAMPLES = (
    ("copycord run", "start Copycord in this terminal"),
    ("copycord service install", "run it in the background"),
    ("copycord update", "update to the latest version"),
    ("copycord doctor", "check your setup"),
    ("copycord config set PASSWORD 'new-pass'", "change the dashboard password"),
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
        ("copycord doctor --fix", "fix what it can, then check again"),
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
        ("copycord update", "update to the latest version"),
        ("copycord update status", "see whether an update is available"),
        ("copycord update --tag 4.0.0", "install one exact version"),
    ),
    "copycord uninstall": (
        ("copycord uninstall --dry-run", "show what would be removed"),
        ("copycord uninstall", "remove Copycord; asks whether to delete your data"),
        ("copycord uninstall --yes", "no prompts, keep the data"),
        ("copycord uninstall --yes --purge", "no prompts, delete the data too"),
        ("copycord uninstall --yes --keep-service", "no prompts, keep the service"),
    ),
}

# prog -> (section title, option dests, examples) for options printed last.
COMMAND_ADVANCED = {
    "copycord update": ("Advanced (channels and git installs)", ("channel", "branch", "repo", "no_install"), (
        ("copycord update --channel beta", "switch to pre-releases"),
        ("copycord update --channel stable", "back to releases"),
        ("copycord update --channel dev --ref v4-dev", "install a git branch"),
        ("copycord update --channel dev --repo you/Copycord --ref 1a2b3c4", "a fork's commit"),
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
    w(f"  {ui.cmd('-V, --version'.ljust(opt_width))}  Print the version")
    w()
    w(ui.dim("Run `copycord <command> --help` for the options of one command."))
    w(ui.dim(f"Docs: {DOCS_URL}"))
    return buf.getvalue()


def _sentence(text: str) -> str:
    return text[:1].upper() + text[1:] if text else ""


def _invocation(action: argparse.Action) -> str:
    flags = ", ".join(sorted(action.option_strings, key=len))
    if action.nargs == 0:
        return flags
    if action.choices:
        return f"{flags} {'|'.join(action.choices)}"
    return f"{flags} {action.metavar or action.dest.upper()}"


def format_command_help(parser: argparse.ArgumentParser) -> str:
    """Help for `copycord <command> --help`, styled like the overview."""
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

    def example_rows(items) -> None:
        width = min(max(len(c) for c, _ in items), 44)
        for example, text in items:
            if len(example) > width:
                w(f"  {ui.cmd(example)}")
                w(f"  {' ' * width}  {ui.dim(text)}")
            else:
                w(f"  {ui.cmd(example.ljust(width))}  {ui.dim(text)}")

    advanced_title, advanced_dests, advanced_examples = COMMAND_ADVANCED.get(parser.prog, ("", (), ()))
    commands, arguments, options, advanced = [], [], [], []
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
        elif action.dest in advanced_dests:
            advanced.append((_invocation(action), _sentence(action.help or "")))
        elif not isinstance(action, argparse._HelpAction):
            options.append((_invocation(action), _sentence(action.help or "")))
    options.append(("-h, --help", "Show this help"))
    usage.append("[options]")
    w()
    w(f"{ui.bold('Usage:')} {' '.join(usage)}")
    rows("Commands", commands)
    rows("Arguments", arguments)
    rows("Options", options)
    examples = COMMAND_EXAMPLES.get(parser.prog, ())
    if examples:
        w()
        w(ui.accent("Examples:"))
        example_rows(examples)
    rows(advanced_title, advanced)
    if advanced_examples:
        w()
        example_rows(advanced_examples)
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
        """Print a short error and a pointer to the help instead of argparse's usage."""
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
        """Show a group's help when its subcommand is missing, else a short error."""
        if message.startswith("the following arguments are required:") and message.endswith("_command"):
            sys.stdout.write(self.format_help())
            self.exit(0)
        from copycord import ui

        ui.fail(message, f"run {ui.cmd(self.prog + ' --help', stream=sys.stderr)} for the options", stream=sys.stderr)
        self.exit(2)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    if not (sys.argv[1:] if argv is None else argv):
        sys.stdout.write(format_help())
        raise SystemExit(0)
    parser = _Parser(prog="copycord")
    parser.add_argument("-V", "--version", action=_VersionAction, nargs=0,
                        help="print the version")
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
        "is running and whether an update is out. Exits 1 when a check fails. "
        "--fix repairs what it can, then checks again.",
    )
    doctor_p.add_argument("--verbose", "-v", action="store_true",
                          help="show the path behind every check")
    doctor_p.add_argument("--fix", action="store_true",
                          help="apply every automatic fix, ask before moving the dashboard "
                          "to a free port when there is a terminal, then check again")
    doctor_p.add_argument("--brief", action="store_true",
                          help="print only the fixes applied, the failed checks and "
                          "one result line")

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
        help="update Copycord to the latest version",
        description="Update Copycord to the latest version and restart a running "
        "service. The database is backed up first.",
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
        "(uv tool or pipx) and the saved update channel. It then asks whether to "
        "delete your data too: the Copycord home (~/.copycord or COPYCORD_HOME: "
        "database, logs, backups, settings, src, node). Without a terminal the data "
        "is kept unless --purge is given.",
    )
    data_choice = uninst.add_mutually_exclusive_group()
    data_choice.add_argument("--purge", action="store_true",
                             help="also delete the Copycord home without asking "
                             "(database, logs, backups, settings, src, node)")
    data_choice.add_argument("--keep-data", action="store_true",
                             help="keep the Copycord home without asking")
    uninst.add_argument("--yes", "-y", action="store_true",
                        help="skip the confirmation prompts (needed without a terminal)")
    uninst.add_argument("--keep-service", action="store_true",
                        help="leave the OS service installed")
    uninst.add_argument("--dry-run", action="store_true",
                        help="print the steps and change nothing")
    return parser.parse_args(argv)


def check_removed_options() -> bool:
    """False when the removed ROLE variable is still set."""
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
    # Why it is not OK and what to do.
    reason: str = ""
    # Shown on WARN/FAIL or with --verbose.
    path: str = ""
    # Key into FIXES when `copycord doctor --fix` can repair it.
    fix: str = ""


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
    """The admin static dir, found without importing copycord.admin.app."""
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
        return Check(FAIL, "web UI missing", "the admin package cannot be found", fix="reinstall")
    main_js = static_dir / "assets" / "main.js"
    if main_js.is_file():
        return Check(PASS, "web UI included", path=str(static_dir))
    return Check(
        FAIL,
        "web UI missing",
        "the dashboard will not load; reinstall Copycord (a git install needs npm "
        "to build it)",
        str(main_js),
        "reinstall",
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
    """One check for all REQUIRED_IMPORTS, naming only the broken ones."""
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
        fix="reinstall",
    )


# --- Settings --------------------------------------------------------------


def check_role(env: Optional[Mapping[str, str]] = None) -> Check:
    env = os.environ if env is None else env
    if "ROLE" in env:
        return Check(FAIL, "ROLE is set", ROLE_REMOVED_MESSAGE.split("ROLE is set, but ", 1)[-1])
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
        "`copycord doctor --fix` writes it with the defaults",
        str(path),
        "env_file",
    )


def check_password(env: Optional[Mapping[str, str]] = None) -> Check:
    from copycord.envfile import DEFAULT_PASSWORD, env_path

    env = os.environ if env is None else env
    value = env.get("PASSWORD")
    if value == DEFAULT_PASSWORD:
        # doctor --fix never changes the password.
        return Check(
            WARN,
            "the default password is still set",
            f"change PASSWORD in {env_path()}, then run `copycord restart`",
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
    """The bot tokens as the app reads them: app config, then the environment."""
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
        return Check(WARN, "home folder missing", "Copycord creates it on first run",
                     str(path), "home")
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
        # A new install, or one too young to have had its first daily backup.
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
        return Check(WARN, "no backups yet", hint, str(backup_dir), "backup")
    age = now - max(times)
    if age > BACKUP_MAX_AGE:
        return Check(WARN, f"last backup {_age(age)} ago", hint, str(backup_dir), "backup")
    return Check(PASS, f"last backup {_age(age)} ago", path=str(backup_dir))


# --- Running ---------------------------------------------------------------


def copycord_on_port(port: int, timeout: float = 1.0) -> bool:
    """True when the dashboard answers on 127.0.0.1:port."""
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
        # Like uvicorn, ignore TIME_WAIT sockets; on Windows SO_REUSEADDR would share a live port.
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
                fix="port",
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
                 "start it with `copycord service start`", state.where, "service")


# --- Updates ---------------------------------------------------------------


def check_updates(timeout: float = 2.0) -> Check:
    """The cached update check; a network error is only INFO."""
    from copycord import update_check

    try:
        if not update_check.check_enabled():
            return Check(INFO, "update check off", "COPYCORD_NO_UPDATE_CHECK is set")
        result = update_check.check_cached(timeout=timeout)
    except Exception as e:
        return Check(INFO, "could not check for updates", str(e).splitlines()[0][:120] if str(e) else type(e).__name__)
    if result.available:
        text = update_check.hint_text(result).split(". Run")[0]
        return Check(INFO, text[:1].lower() + text[1:], "install it with `copycord update`")
    return Check(PASS, "up to date")


def run_doctor() -> List[tuple]:
    """[(section, [Check])] in print order."""
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


# --- fixes -----------------------------------------------------------------


class FixError(Exception):
    """A fix that cannot be applied."""


def _env_owned(key: str) -> None:
    """FixError when KEY is set in the real environment, which overrides <home>/.env."""
    from copycord import envfile

    if key in os.environ and envfile.get(key) != os.environ[key]:
        raise FixError(f"{key} is set in the environment (for example a compose file); "
                       "change it there")


def fix_home(checks: List[Check]) -> str:
    from copycord.common.paths import data_dir

    path = data_dir()
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def fix_env_file(checks: List[Check]) -> str:
    """Write the default <home>/.env."""
    from copycord import envfile

    path = envfile.write_template()
    envfile.load(path)
    return str(path)


def fix_backup(checks: List[Check]) -> str:
    """Run a daily backup now."""
    from copycord.common.backup_scheduler import (
        BackupConfig,
        DailySQLiteBackupScheduler,
        record_backup_stats,
    )
    from copycord.common.db import backups_dir
    from copycord.common.paths import db_path

    db = db_path()
    if not db.is_file():
        raise FixError(f"there is no database at {db} yet")
    cfg = BackupConfig(
        db_path=db,
        backup_dir=backups_dir(),
        retain=int(os.getenv("BACKUP_RETAIN") or "14"),
        timezone=os.getenv("TZ") or "UTC",
    )
    scheduler = DailySQLiteBackupScheduler(cfg, logger=logging.getLogger("copycord.backup"))
    archive = asyncio.run(scheduler.run_now())
    try:
        conn = sqlite3.connect(str(db))
        try:
            with conn:
                record_backup_stats(
                    lambda k, v: conn.execute(
                        "INSERT INTO app_config(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (k, v),
                    ),
                    archive,
                )
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return str(archive)


def _can_reinstall(checks: List[Check]) -> None:
    """FixError when `copycord update` cannot reinstall this install."""
    from copycord import update

    kind = update.detect_installer()
    if kind in (update.DOCKER, update.SOURCE):
        raise FixError(f"Copycord was installed with {kind}; {update.manual_hint(kind)}")
    if kind not in (update.UV_TOOL, update.PIPX):
        raise FixError(f"Copycord was installed with {kind}; reinstall it the same way")
    if os.environ.get("COPYCORD_SKIP_FRONTEND") and all(c.name == "web UI missing" for c in checks):
        raise FixError("COPYCORD_SKIP_FRONTEND is set, so a reinstall would leave the web UI out too")
    # A git reinstall builds the admin UI, which needs npm.
    git = update.git_source() is not None or update.load_channel().channel == update.DEV
    if git and not os.environ.get("COPYCORD_SKIP_FRONTEND"):
        from copycord import node_runtime

        if node_runtime.find_npm() is None:
            raise FixError(NPM_MISSING)


NPM_MISSING = ("reinstalling needs npm to build the web UI, and npm is not installed; "
               "install Node.js from https://nodejs.org, then run copycord doctor --fix again")


def fix_reinstall(checks: List[Check]) -> str:
    """Reinstall the running version."""
    from copycord import update

    _can_reinstall(checks)
    rc = update.update(reinstall=True)
    if rc:
        raise FixError(f"the reinstall failed (exit {rc}); see the output above")
    importlib.invalidate_caches()
    return f"v{update.current_version()}"


def fix_service(checks: List[Check]) -> str:
    from copycord import service

    svc = service.get_service()
    try:
        svc.start()
    except service.ServiceError as e:
        raise FixError(str(e)) from None
    return svc.where


def next_free_port(port: int, host: str = ADMIN_BIND_HOST) -> Optional[int]:
    """The first port above PORT that nothing listens on."""
    for candidate in range(port + 1, min(port + 101, 65536)):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if sys.platform != "win32":
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind((host, candidate))
            except OSError:
                continue
        return candidate
    return None


def _port_plan() -> int:
    _env_owned("ADMIN_PORT")
    port = admin_port()
    free = next_free_port(port)
    if free is None:
        raise FixError(f"no free port found above {port}; set ADMIN_PORT yourself")
    return free


def fix_port(checks: List[Check]) -> str:
    from copycord import envfile

    free = _port_plan()
    path = envfile.set_value("ADMIN_PORT", str(free))
    os.environ["ADMIN_PORT"] = str(free)
    return f"ADMIN_PORT={free} in {path}"


class Fix(NamedTuple):
    # (checks) -> what it did; raises FixError when it cannot be done.
    apply: object
    # Printed with the apply() result once it worked.
    done: str
    # Guided fixes only: (checks) -> the y/N question; may raise FixError.
    question: object = None
    # Completes "run `copycord doctor --fix` in a terminal to ..." when there is no terminal.
    skipped: str = ""

    @property
    def guided(self) -> bool:
        return self.question is not None


def _port_question(checks):
    return (f"Port {admin_port()} is taken by another program. "
            f"Set ADMIN_PORT to {_port_plan()}?")


# Run in this order: the home before its files, reinstall and settings before the service.
FIXES = {
    "home": Fix(fix_home, "created the Copycord home"),
    "env_file": Fix(fix_env_file, "recreated .env with the default settings"),
    "backup": Fix(fix_backup, "backed up the database"),
    "reinstall": Fix(fix_reinstall, "reinstalled Copycord"),
    "port": Fix(fix_port, "moved the dashboard to a free port", question=_port_question,
                skipped="move the dashboard to a free port"),
    "service": Fix(fix_service, "started the copycord service"),
}


def apply_fixes(checks: List[Check], notes: dict, brief: bool = False) -> int:
    """Apply the fixes for flagged checks, printing each one applied; returns how many.

    Fills notes with {check name: why it was not fixed}.
    """
    from copycord import ui

    interactive = ui.is_interactive()
    flagged = [c for c in checks if c.status in (WARN, FAIL)]
    wanted = {}
    for c in flagged:
        if c.fix in FIXES:
            wanted.setdefault(c.fix, []).append(c)
    applied = 0
    for key, fix in FIXES.items():
        if key not in wanted:
            continue
        found = wanted[key]
        name = found[0].name
        if fix.guided and not interactive:
            notes[name] = f"run `copycord doctor --fix` in a terminal to {fix.skipped}"
            continue
        try:
            if fix.guided and not ui.confirm(fix.question(found), default=False):
                notes[name] = "fix skipped"
                continue
            result = fix.apply(found)
        except (KeyboardInterrupt, EOFError):
            raise
        except Exception as e:
            notes[name] = f"could not fix it: {str(e) or type(e).__name__}"
            continue
        if not applied and not brief:
            ui.heading("Fixed")
        ui.ok(fix.done, result)
        applied += 1
    return applied


def _hint(c: Check, verbose: bool = False) -> str:
    parts = [c.reason]
    if c.path and (verbose or c.status in (WARN, FAIL)):
        parts.append(c.path)
    return "; ".join(p for p in parts if p)


def _print_checks(rows: List[Check], verbose: bool) -> None:
    from copycord import ui

    status = {PASS: "ok", WARN: "warn", FAIL: "fail", INFO: "info"}
    for c in rows:
        ui.line(status[c.status], c.name, _hint(c, verbose), stream=sys.stdout)


def _print_attention(checks: List[Check], notes: Optional[dict] = None) -> None:
    """Every WARN and FAIL once, with its hint and why --fix left it."""
    from copycord import ui

    notes = notes or {}
    ui.attention(
        (("fail" if c.status == FAIL else "warn", c.name,
          "; ".join(p for p in (notes.get(c.name, ""), _hint(c)) if p))
         for c in checks if c.status in (WARN, FAIL)),
        stream=sys.stdout,
    )


def _print_summary(checks: List[Check], fixable: int = 0) -> None:
    """Print the summary line and, if something is fixable, the --fix hint."""
    from copycord import ui

    summary = doctor_summary(checks)
    print()
    if not ui.use_color():
        print(summary)
    elif any(c.status == FAIL for c in checks):
        ui.fail(summary, stream=sys.stdout)
    elif summary != "All good":
        ui.warn(summary, stream=sys.stdout)
    else:
        ui.ok(summary, f"start it with {ui.cmd('copycord run')}")
    if fixable:
        print(ui.dim(f"Run `copycord doctor --fix` to fix {fixable} of them."))


def _print_brief(checks: List[Check]) -> None:
    """--brief: the FAIL lines and one result line."""
    from copycord import ui

    for c in checks:
        if c.status == FAIL:
            ui.fail(c.name, _hint(c), stream=sys.stdout)
    summary = doctor_summary(checks)
    if summary == "All good":
        ui.ok("All checks passed")
    elif any(c.status == FAIL for c in checks):
        ui.fail(summary, "run `copycord doctor` for details", stream=sys.stdout)
    else:
        ui.warn(summary, "run `copycord doctor` for details", stream=sys.stdout)


def doctor(verbose: bool = False, fix: bool = False, brief: bool = False) -> int:
    from copycord import ui

    if fix:
        # Fixes write to stderr and run uv; line-buffer so piped output stays in order.
        try:
            sys.stdout.reconfigure(line_buffering=True)
        except (AttributeError, ValueError):
            pass
    sections = run_doctor()
    checks: List[Check] = [c for _, rows in sections for c in rows]
    if not brief:
        if ui.use_color():
            ui.heading("Copycord doctor")
        for title, rows in sections:
            ui.heading(title)
            _print_checks(rows, verbose)
    flagged = [c for c in checks if c.status in (WARN, FAIL)]
    notes: dict = {}
    fixable = sum(c.fix in FIXES for c in flagged)
    if fix and flagged:
        try:
            applied = apply_fixes(checks, notes, brief)
        except (KeyboardInterrupt, EOFError):
            print()
            ui.warn("fixes stopped", "the ones above were applied", stream=sys.stdout)
            return 130
        if applied:
            checks = [c for _, rows in run_doctor() for c in rows]
        flagged = [c for c in checks if c.status in (WARN, FAIL)]
        # Only the guided fixes skipped for want of a terminal are worth another run.
        fixable = 0 if ui.is_interactive() else sum(
            c.fix in FIXES and FIXES[c.fix].guided for c in flagged)
    if brief:
        _print_brief(checks)
    else:
        _print_attention(checks, notes)
        _print_summary(checks, fixable)
    return 1 if any(c.status == FAIL for c in checks) else 0


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------


def is_secret(key: str) -> bool:
    upper = key.upper()
    return any(marker in upper for marker in SECRET_MARKERS)


def masked(values: Mapping[str, str]) -> dict:
    return {k: (MASK if is_secret(k) and v else v) for k, v in values.items()}


def _open_db(create: bool):
    """DBManager on the configured DB; None when it is missing."""
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
    """Why the Copycord home must not be deleted; None when it may."""
    if not str(path).strip() or str(path) == ".":
        return "the Copycord home path is empty"
    resolved = path.expanduser().resolve()
    if resolved == Path(resolved.anchor):
        return f"{resolved} is a filesystem root"
    if resolved == Path.home().resolve():
        return f"{resolved} is your home directory"
    return None


def _forget_saved_home(removed: list) -> None:
    """Remove the ~/.copycord-home pointer saved for a custom --home."""
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
        # Windows cannot delete the running copycord.exe; finish in a detached shell after exit.
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

    dry_run, yes = args.dry_run, args.yes
    # True deletes the data, False keeps it, None asks after the confirmation.
    purge: Optional[bool] = True if args.purge else False if args.keep_data else None
    if purge is None and (yes or dry_run):
        purge = False
    target = data_dir()
    if purge:
        why = purge_refusal(target)
        if why:
            ui.fail("refusing to delete the Copycord home", why)
            return 1
    if not yes and not dry_run and not ui.is_interactive():
        ui.fail(
            "copycord uninstall needs a confirmation",
            "no terminal to ask in; rerun with --yes (keeps your data unless you add --purge)",
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
    if purge is None:
        ui.line("step", "ask whether to delete your data too", str(target))
    print()

    if not yes and not dry_run:
        try:
            if not ui.confirm("Uninstall Copycord?", default=False):
                ui.warn("uninstall cancelled", "nothing was changed")
                return 1
            if purge is None:
                purge = ui.confirm(
                    "Also delete your Copycord data (database, logs, backups, settings) "
                    f"in {target}? This cannot be undone.",
                    default=False,
                )
                if purge:
                    why = purge_refusal(target)
                    if why:
                        ui.fail("refusing to delete the Copycord home", why)
                        return 1
                else:
                    ui.info("keeping your data", str(target))
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
        kept.append(f"Copycord home: {target} (database, logs, backups, settings)")
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
    """False when the DB was written by a newer Copycord."""
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
    from copycord import envfile

    envfile.load()

    if args.command == "doctor":
        return doctor(verbose=args.verbose, fix=args.fix, brief=args.brief)

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
