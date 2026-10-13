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
import difflib
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
from typing import List, Mapping, NamedTuple, NoReturn, Optional, Sequence

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
        ("run", "Run Copycord"),
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
    "copycord update": ("Advanced (channels and git installs)", ("channel", "branch", "repo", "no_install", "force"), (
        ("copycord update --channel beta", "switch to pre-releases"),
        ("copycord update --channel stable", "back to releases"),
        ("copycord update --channel dev --ref v4-dev", "install a git branch"),
        ("copycord update --channel dev --repo you/Copycord --ref 1a2b3c4", "a fork's commit"),
    )),
}


def _cmd(text: str) -> str:
    """A command inside a message, styled like ui.cmd."""
    from copycord import ui

    return ui.cmd(text)


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
    from copycord.common.paths import copycord_home

    w(f"  {ui.dim('Home:')} {ui.path_link(copycord_home(), out)}")
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
    w(f"  {ui.cmd('--no-color'.ljust(opt_width))}  Turn off color, same as NO_COLOR=1")
    w(f"  {ui.cmd('--json'.ljust(opt_width))}  Print the result as JSON, for scripts")
    w(f"  {ui.cmd('--verbose'.ljust(opt_width))}  Show more detail, and the full error if something fails")
    w()
    w(ui.dim("Run ") + ui.cmd("copycord <command> --help") + ui.dim(" for the options of one command."))
    w(ui.dim(f"Docs: {ui.link(DOCS_URL)}"))
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
        w(ui.dim("Run ") + ui.cmd(f"{parser.prog} <command> --help") + ui.dim(" for the options of one command."))
    w(ui.dim(f"Docs: {ui.link(DOCS_URL)}"))
    return buf.getvalue()


def version_report() -> dict:
    """What `copycord --version --json` prints."""
    from copycord.update import DEV, installed_commit, load_channel

    channel = load_channel()
    dev = channel.channel == DEV
    return {
        "version": _version(),
        "commit": installed_commit(),
        "channel": channel.channel,
        "ref": channel.ref if dev else None,
        "repo": channel.repo_name if dev else None,
    }


def print_version(as_json: bool = False) -> None:
    from copycord import ui
    from copycord.update import installed_commit, load_channel

    if as_json:
        from copycord import jsonout

        jsonout.emit(version_report())
        return
    version = _version()
    commit = installed_commit()
    if commit:
        version = f"{version} ({commit[:7]})"
    print(version)
    print(load_channel().describe(ui.dot()))
    if sys.stdout.isatty():
        from copycord import update_check

        update_check.start(wait=2.0)


class _VersionAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        print_version(as_json=getattr(parser, "json_output", False))
        parser.exit()


class _Parser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        if sys.version_info >= (3, 14):
            kwargs.setdefault("suggest_on_error", True)
        super().__init__(*args, **kwargs)

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
            bad = message.split("invalid choice: ", 1)[-1].split(" ", 1)[0].strip("'\",")
            text = f"unknown command '{bad}'."
            match = difflib.get_close_matches(bad, _command_names(self), n=1, cutoff=0.6)
            if match:
                text += f" Did you mean '{match[0]}'?"
        else:
            text = message
        ui.fail(text, stream=sys.stderr)
        hint = f"Run {ui.cmd('copycord --help', stream=sys.stderr)} to see all commands."
        print(f"  {hint}", file=sys.stderr)
        self.exit(2)


class _SubParser(_Parser):
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


def _command_names(parser: argparse.ArgumentParser) -> List[str]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return list(action.choices)
    return []


def _take_no_color(argv: List[str]) -> List[str]:
    """Drop --no-color wherever it is (before "--") and set NO_COLOR for this process."""
    end = argv.index("--") if "--" in argv else len(argv)
    if "--no-color" not in argv[:end]:
        return argv
    os.environ["NO_COLOR"] = "1"
    return [a for a in argv[:end] if a != "--no-color"] + argv[end:]


def wants_verbose(argv: Sequence[str]) -> bool:
    """True when --verbose (or doctor's -v) is given before "--"."""
    end = list(argv).index("--") if "--" in argv else len(argv)
    return any(a in ("--verbose", "-v") for a in argv[:end])


def _take_verbose(argv: List[str]) -> List[str]:
    """Drop --verbose wherever it is (before "--"); parse_args sets args.verbose."""
    end = argv.index("--") if "--" in argv else len(argv)
    return [a for a in argv[:end] if a != "--verbose"] + argv[end:]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    argv = _take_no_color(list(sys.argv[1:] if argv is None else argv))
    # --verbose works with every command: doctor shows its paths, and an
    # unexpected error also prints the traceback.
    verbose = wants_verbose(argv)
    argv = _take_verbose(argv)
    if not argv:
        sys.stdout.write(format_help())
        raise SystemExit(0)
    parser = _Parser(prog="copycord")
    end = argv.index("--") if "--" in argv else len(argv)
    # --version prints as soon as it is parsed, so it cannot wait for a later --json.
    parser.json_output = "--json" in argv[:end]
    parser.add_argument("-V", "--version", action=_VersionAction, nargs=0,
                        help="print the version")
    parser.add_argument("--no-color", action="store_true",
                        help="turn off color, same as NO_COLOR=1")
    parser.add_argument("--json", dest="top_json", action="store_true",
                        help="print the result as JSON")
    json_flag = argparse.ArgumentParser(add_help=False)
    json_flag.add_argument("--json", action="store_true",
                           help="print one JSON document on stdout and nothing else")
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
        parents=[data_dir, json_flag],
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
    get = csub.add_parser("get", parents=[data_dir, json_flag], help="print one value")
    get.add_argument("key")
    get.add_argument("--show-secrets", action="store_true",
                     help="print token/password/secret values unmasked")
    set_ = csub.add_parser("set", parents=[data_dir], help="store one value")
    set_.add_argument("key")
    set_.add_argument("value")
    unset = csub.add_parser("unset", parents=[data_dir], help="remove one value")
    unset.add_argument("key")
    export = csub.add_parser(
        "export", parents=[data_dir, json_flag], help="write all values as JSON"
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
        parents=[data_dir, json_flag],
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
        help="update without asking first; also confirms installing an older version "
        "(needed with no terminal)",
    )
    update.add_argument(
        "--force",
        action="store_true",
        help="reinstall even when up to date",
    )

    uninst = sub.add_parser(
        "uninstall",
        parents=[data_dir],
        help="remove Copycord from this machine",
        description="Stop and remove the OS service, remove the copycord program "
        "(uv tool or pipx) and the saved update channel. It asks one question: keep "
        "your data, delete it too (the Copycord home, ~/.copycord or COPYCORD_HOME: "
        "database, logs, backups, settings, src, node), or cancel. Without a terminal "
        "the data is kept unless --purge is given.",
    )
    data_choice = uninst.add_mutually_exclusive_group()
    data_choice.add_argument("--purge", action="store_true",
                             help="also delete the Copycord home without asking "
                             "(database, logs, backups, settings, src, node)")
    data_choice.add_argument("--keep-data", action="store_true",
                             help="keep the Copycord home without asking")
    uninst.add_argument("--yes", "-y", action="store_true",
                        help="skip the question (needed without a terminal)")
    uninst.add_argument("--keep-service", action="store_true",
                        help="leave the OS service installed")
    uninst.add_argument("--dry-run", action="store_true",
                        help="print the steps and change nothing")
    args = parser.parse_args(argv)
    args.verbose = verbose or getattr(args, "verbose", False)
    args.json = bool(getattr(args, "json", False) or args.top_json)
    if args.json and not json_supported(args):
        parser.error("--json works with --version, doctor, update status, config get "
                     "and config export")
    return args


def json_supported(args: argparse.Namespace) -> bool:
    """True for the reporting commands that can print JSON."""
    if args.command == "doctor":
        return True
    if args.command == "config":
        return args.config_command in ("get", "export")
    if args.command == "update":
        return args.action == "status"
    return False


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
    sep = ui.dot()
    try:
        channel = load_channel().describe(sep)
    except Exception:
        channel = "unknown channel"
    return Check(PASS, f"{version}{sep}{channel}")


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
    return Check(WARN, ".env not found", path=str(path), fix="env_file")


def check_password(env: Optional[Mapping[str, str]] = None) -> Check:
    from copycord.envfile import DEFAULT_PASSWORD, env_path

    env = os.environ if env is None else env
    value = env.get("PASSWORD")
    if value == DEFAULT_PASSWORD:
        # doctor --fix never changes the password.
        return Check(
            WARN,
            "the default password is still set",
            f"change PASSWORD in {env_path()}, then run {_cmd('copycord restart')}",
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


def check_layout() -> Check:
    """WARN while a native home still has data.db or secret.key loose in it."""
    from copycord.common.paths import copycord_home, needs_migration

    if needs_migration():
        home = copycord_home().resolve()
        return Check(WARN, "data files loose in the home",
                     "Copycord moves them into data and logs folders on its next start",
                     str(home), "layout")
    return Check(PASS, "data folder layout")


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
            f"run {_cmd('copycord update')} to go back to a newer version, or restore a "
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
    """The newest daily (*.tar.gz) or pre-update (*.db) backup, or a newer one that failed its check."""
    import time

    from copycord.common.backup_scheduler import last_check_failure
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
    failed = last_check_failure(backup_dir)
    if failed and (not times or failed["mtime"] >= max(times)):
        reason = ("the copy of the database did not pass the SQLite integrity check, so it "
                  "was not kept; older backups are untouched")
        if failed["reason"]:
            reason += f" ({failed['reason']})"
        return Check(WARN, "last backup failed its check", reason, str(backup_dir), "backup")
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


def service_console() -> Optional[str]:
    """Windows: the service's program when it opens a console window."""
    try:
        from copycord import service

        return service.console_task()
    except Exception:
        return None


def check_service(state, console: Optional[str] = None) -> Check:
    if state is None:
        return Check(INFO, "service state unknown")
    if not state.installed:
        return Check(INFO, "service not installed",
                     f"{_cmd('copycord service install')} runs Copycord in the background")
    if console:
        return Check(WARN, "service runs in a console window",
                     "the window stays open while Copycord runs, and closing it stops "
                     "Copycord", state.where, "service_window")
    if state.running:
        return Check(PASS, "service running", path=state.where)
    return Check(WARN, "service installed, not running",
                 f"start it with {_cmd('copycord service start')}", state.where, "service")


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
        return Check(INFO, text[:1].lower() + text[1:], f"install it with {_cmd('copycord update')}")
    return Check(PASS, "up to date")


def run_doctor() -> List[tuple]:
    """[(section, [Check])] in print order."""
    from copycord.common.paths import data_dir, db_path

    settings = [check_env_file(), check_password()]
    role = check_role()
    if role.status == FAIL:
        settings.insert(0, role)
    state = service_state()
    return [
        ("Install", [check_version(), check_python(), check_frontend(), check_dependencies()]),
        ("Settings", settings),
        ("Data", [check_data_dir(data_dir()), check_layout(), check_db(db_path()),
                  check_backups()]),
        ("Running", [
            check_admin_port(service_running=bool(state and state.running)),
            check_service(state, service_console() if state and state.installed else None),
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


def fix_layout(checks: List[Check]) -> str:
    """Move the old flat home into <home>/data and <home>/logs."""
    from copycord.common.paths import data_dir, migrate_layout

    state = service_state()
    if state and state.running:
        raise FixError("Copycord is running; stop it first (copycord service stop)")
    try:
        line = migrate_layout()
    except OSError as e:
        raise FixError(f"could not move the files, nothing changed: {e}")
    return line or str(data_dir())


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
    from copycord.common.paths import data_dir, db_path

    db = db_path()
    if not db.is_file():
        raise FixError(f"there is no database at {db} yet")
    cfg = BackupConfig(
        db_path=db,
        backup_dir=backups_dir(),
        retain=int(os.getenv("BACKUP_RETAIN") or "14"),
        timezone=os.getenv("TZ") or "UTC",
        key_path=data_dir() / "secret.key",
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


def fix_service_window(checks: List[Check]) -> str:
    """Windows: write the service task again so it runs without a window."""
    from copycord import service

    svc = service.get_service()
    try:
        svc.use_windowless()
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
    # The check's line once it worked, shown with the apply() result.
    done: str
    # Completes "run copycord doctor --fix to ..." in Needs your attention.
    todo: str
    # Guided fixes only: (checks) -> the y/N question; may raise FixError.
    question: object = None

    @property
    def guided(self) -> bool:
        return self.question is not None


def _port_question(checks):
    return (f"Port {admin_port()} is taken by another program. "
            f"Set ADMIN_PORT to {_port_plan()}?")


# Run in this order: the home before its files, reinstall and settings before the service.
FIXES = {
    "home": Fix(fix_home, "Copycord home created", "create it"),
    "layout": Fix(fix_layout, "data moved into the data folder", "move them now"),
    "env_file": Fix(fix_env_file, ".env created with the default settings",
                    "create it with the default settings"),
    "backup": Fix(fix_backup, "database backed up", "back it up now"),
    "reinstall": Fix(fix_reinstall, "Copycord reinstalled", "reinstall Copycord"),
    "port": Fix(fix_port, "dashboard moved to a free port", "move the dashboard to a free port",
                question=_port_question),
    "service": Fix(fix_service, "copycord service started", "start it"),
    "service_window": Fix(fix_service_window, "service runs without a window",
                          "run it without a window"),
}


def apply_fixes(checks: List[Check], notes: dict, fixed: dict, failed: set,
                interactive: Optional[bool] = None) -> int:
    """Apply the fixes for flagged checks; returns how many worked.

    Fills fixed with {fix key: what it did}, notes with {check name: why it was
    not fixed} and failed with the names whose fix raised. Guided fixes need
    interactive (default: whether there is a terminal).
    """
    from copycord import ui

    if interactive is None:
        interactive = ui.is_interactive()
    wanted = {}
    for c in checks:
        if c.status in (WARN, FAIL) and c.fix in FIXES:
            wanted.setdefault(c.fix, []).append(c)
    for key, fix in FIXES.items():
        if key not in wanted:
            continue
        found = wanted[key]
        name = found[0].name
        if fix.guided and not interactive:
            notes[name] = f"run {_cmd('copycord doctor --fix')} in a terminal to {fix.todo}"
            continue
        try:
            if fix.guided and not ui.confirm(fix.question(found), default=False):
                notes[name] = "fix skipped"
                continue
            fixed[key] = fix.apply(found)
        except (KeyboardInterrupt, EOFError):
            raise
        except Exception as e:
            notes[name] = f"could not fix it: {str(e) or type(e).__name__}"
            failed.add(name)
    return len(fixed)


def _hint(c: Check, verbose: bool = False, note: str = "") -> str:
    """The reason (or why --fix left it) and, on WARN/FAIL, the path."""
    parts = [note or c.reason]
    if c.path and (verbose or c.status in (WARN, FAIL)):
        parts.append(c.path)
    return "; ".join(p for p in parts if p)


def _link_home(text: str) -> str:
    """text with the paths in the Copycord home and the data dir clickable."""
    from copycord import ui
    from copycord.common.paths import copycord_home, data_dir

    home = copycord_home()
    return ui.link_paths(text, (home, home.resolve(), data_dir()), sys.stdout)


def _print_checks(rows: List[Check], verbose: bool, done: Optional[dict] = None,
                  notes: Optional[dict] = None, failed: Optional[set] = None) -> None:
    """One line per check; done maps a row index to (text, result) for a fixed check."""
    from copycord import ui

    done, notes, failed = done or {}, notes or {}, failed or set()
    status = {PASS: "ok", WARN: "warn", FAIL: "fail", INFO: "info"}
    for i, c in enumerate(rows):
        if i in done:
            text, result = done[i]
            ui.ok(text, _link_home(" ".join(p for p in (result, "(fixed)") if p)),
                  stream=sys.stdout)
        elif c.name in failed:
            ui.fail(c.name, _link_home(notes[c.name]), stream=sys.stdout)
        else:
            ui.line(status[c.status], c.name, _link_home(_hint(c, verbose)), stream=sys.stdout)


def _attention_hint(c: Check, note: str, fix: bool) -> str:
    """The reason, or why --fix left it, and how to fix it; the path is on the check's line."""
    if note:
        return note
    parts = [c.reason]
    if not fix and c.fix in FIXES:
        parts.append(f"run {_cmd('copycord doctor --fix')} to {FIXES[c.fix].todo}")
    return "; ".join(p for p in parts if p) or c.path


def _print_attention(checks: List[Check], notes: Optional[dict] = None, fix: bool = False) -> None:
    """Every WARN and FAIL once, with its hint and why --fix left it."""
    from copycord import ui

    notes = notes or {}
    ui.attention(
        (("fail" if c.status == FAIL else "warn", c.name,
          _link_home(_attention_hint(c, notes.get(c.name, ""), fix)))
         for c in checks if c.status in (WARN, FAIL)),
        stream=sys.stdout,
    )


def _print_summary(checks: List[Check], fixable: int = 0) -> None:
    """Print the summary line and, if something is fixable, the --fix hint."""
    from copycord import ui

    summary = doctor_summary(checks)
    print()
    if any(c.status == FAIL for c in checks):
        ui.fail(summary, stream=sys.stdout)
    elif summary != "All good":
        ui.warn(summary, stream=sys.stdout)
    else:
        ui.ok(summary, f"start it with {ui.cmd('copycord run')}")
    if fixable:
        print(ui.dim("Run ") + ui.cmd("copycord doctor --fix") + ui.dim(f" to fix {fixable} of them."))


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


def _print_fix_summary(checks: List[Check], applied: int) -> None:
    """--fix: what was fixed and what is left."""
    from copycord import ui

    left = [c for c in checks if c.status in (WARN, FAIL)]
    print()
    if applied:
        ui.ok(f"Fixed {_plural(applied, 'issue')}", stream=sys.stdout)
    if not left:
        ui.ok("Everything looks good", f"start it with {ui.cmd('copycord run')}",
              stream=sys.stdout)
        return
    things = _plural(len(left), "thing")
    text = f"{things} {'needs' if len(left) == 1 else 'need'} your attention"
    if any(c.status == FAIL for c in left):
        ui.fail(text, stream=sys.stdout)
    else:
        ui.warn(text, stream=sys.stdout)


def _print_brief(checks: List[Check], notes: dict, fixed: dict) -> None:
    """--brief: the fixes applied, the FAIL lines and one result line."""
    from copycord import ui

    for key, result in fixed.items():
        ui.ok(FIXES[key].done, _link_home(result), stream=sys.stdout)
    for c in checks:
        if c.status == FAIL:
            ui.fail(c.name, _link_home(_hint(c, note=notes.get(c.name, ""))), stream=sys.stdout)
    summary = doctor_summary(checks)
    if summary == "All good":
        ui.ok("All checks passed")
    elif any(c.status == FAIL for c in checks):
        ui.fail(summary, f"run {ui.cmd('copycord doctor')} for details", stream=sys.stdout)
    else:
        ui.warn(summary, f"run {ui.cmd('copycord doctor')} for details", stream=sys.stdout)


def _fixed_rows(before: List[tuple], after: List[tuple], fixed: dict) -> dict:
    """{(section, row index): (text, result)} for each fix that cleared its check."""
    rows = {}
    for (title, old), (_, new) in zip(before, after):
        if len(old) != len(new):
            continue
        for i, (o, n) in enumerate(zip(old, new)):
            if (o.fix in fixed and o.status in (WARN, FAIL) and n.status not in (WARN, FAIL)
                    and o.fix not in {k for k, _ in rows.values()}):
                rows[(title, i)] = (o.fix, fixed[o.fix])
    return {k: (FIXES[key].done, result) for k, (key, result) in rows.items()}


class DoctorRun(NamedTuple):
    # [(section, [Check])], after the fixes when any worked.
    sections: List[tuple]
    notes: dict
    fixed: dict
    failed: set
    # {(section, row index): (text, result)} for each check a fix cleared.
    done: dict
    # Ctrl+C while the fixes ran.
    stopped: bool = False

    @property
    def checks(self) -> List[Check]:
        return [c for _, rows in self.sections for c in rows]


def run_doctor_fixes(fix: bool, interactive: Optional[bool] = None) -> DoctorRun:
    """The checks and, with fix, the fixes followed by the checks again."""
    sections = run_doctor()
    checks: List[Check] = [c for _, rows in sections for c in rows]
    notes: dict = {}
    fixed: dict = {}
    failed: set = set()
    done: dict = {}
    # --fix runs first (the port fix asks), then one report shows the state after it.
    if fix and any(c.status in (WARN, FAIL) for c in checks):
        try:
            applied = apply_fixes(checks, notes, fixed, failed, interactive)
        except (KeyboardInterrupt, EOFError):
            return DoctorRun(sections, notes, fixed, failed, done, stopped=True)
        if applied:
            before, sections = sections, run_doctor()
            done = _fixed_rows(before, sections, fixed)
    return DoctorRun(sections, notes, fixed, failed, done)


CHECK_STATUS = {PASS: "ok", WARN: "warn", FAIL: "fail", INFO: "info"}


def doctor_report(run: DoctorRun, fix: bool = False) -> dict:
    """What `copycord doctor --json` prints."""
    checks = run.checks
    items = []
    for title, rows in run.sections:
        for i, c in enumerate(rows):
            note = run.notes.get(c.name, "")
            hint = note or c.reason
            if not note and not fix and c.status in (WARN, FAIL) and c.fix in FIXES:
                hint = "; ".join(p for p in (hint, f"run copycord doctor --fix to {FIXES[c.fix].todo}") if p)
            item = {
                "section": title,
                "status": CHECK_STATUS[c.status],
                "text": c.name,
                "hint": hint,
                "path": c.path or None,
                "fixable": c.status in (WARN, FAIL) and c.fix in FIXES,
                "fixed": (title, i) in run.done,
            }
            if item["fixed"]:
                item["text"], item["hint"] = run.done[(title, i)]
            items.append(item)
    counts = {name: sum(c.status == status for c in checks) for status, name in CHECK_STATUS.items()}
    report = {
        "ok": not any(c.status == FAIL for c in checks),
        "checks": items,
        "summary": {**counts, "fixed": len(run.fixed), "text": doctor_summary(checks)},
        "fixes": [{"fix": key, "text": FIXES[key].done, "result": result}
                  for key, result in run.fixed.items()],
    }
    if run.stopped:
        report["stopped"] = True
    return report


def doctor_json(fix: bool = False) -> int:
    """doctor --json: one JSON document on stdout; the fixes never ask."""
    from copycord import jsonout

    with jsonout.human_to_stderr():
        run = run_doctor_fixes(fix, interactive=False)
    jsonout.emit(doctor_report(run, fix))
    if run.stopped:
        return 130
    return 1 if any(c.status == FAIL for c in run.checks) else 0


def doctor(verbose: bool = False, fix: bool = False, brief: bool = False,
           as_json: bool = False) -> int:
    from copycord import ui

    if as_json:
        return doctor_json(fix)
    if fix:
        # Fixes write to stderr and run uv; line-buffer so piped output stays in order.
        try:
            sys.stdout.reconfigure(line_buffering=True)
        except (AttributeError, ValueError):
            pass
    run = run_doctor_fixes(fix)
    sections, checks = run.sections, run.checks
    notes, fixed, failed, done = run.notes, run.fixed, run.failed, run.done
    if run.stopped:
        print()
        ui.warn("fixes stopped",
                f"{_plural(len(fixed), 'fix')} applied before that" if fixed else "",
                stream=sys.stdout)
        return 130
    if brief:
        _print_brief(checks, notes, fixed)
        return 1 if any(c.status == FAIL for c in checks) else 0
    if ui.use_color():
        ui.heading("Copycord doctor")
    for title, rows in sections:
        ui.heading(title)
        _print_checks(rows, verbose, {i: v for (t, i), v in done.items() if t == title},
                      notes, failed)
    _print_attention(checks, notes, fix)
    if fix:
        _print_fix_summary(checks, len(fixed))
    else:
        _print_summary(checks, sum(c.status in (WARN, FAIL) and c.fix in FIXES for c in checks))
    return 1 if any(c.status == FAIL for c in checks) else 0


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------


def is_secret(key: str) -> bool:
    upper = key.upper()
    return any(marker in upper for marker in SECRET_MARKERS)


def masked(values: Mapping[str, str]) -> dict:
    return {k: (MASK if is_secret(k) and v else v) for k, v in values.items()}


def _no_db_text(path: Path) -> str:
    return f"no database at {path} (run Copycord once, or check --data-dir)"


def _open_db(create: bool):
    """DBManager on the configured DB; None when it is missing."""
    from copycord.common.db import DBManager
    from copycord.common.paths import db_path

    path = db_path()
    if not path.exists():
        if not create:
            print(_no_db_text(path), file=sys.stderr)
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
    "and run copycord run again; <home>/.env is read on every start."
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


def _print_value(key: str, value: Optional[str], show_secrets: bool,
                 as_json: bool = False, source: str = "", error: str = "") -> None:
    """The value, masked for a secret; with as_json a document that also says
    where it is stored and, when it is not set, why."""
    shown = MASK if is_secret(key) and value and not show_secrets else value
    if not as_json:
        print(shown)
        return
    from copycord import jsonout

    doc = {"key": key, "value": shown, "set": value is not None,
           "masked": shown != value, "source": source}
    if error:
        doc["error"] = error
    jsonout.emit(doc)


def env_config_command(args: argparse.Namespace) -> int:
    """config get/set/unset for the env-only keys in <home>/.env."""
    from copycord import envfile

    cmd, key = args.config_command, args.key
    if cmd == "get":
        value = envfile.get(key)
        if value is None:
            error = f"{key} is not set in {envfile.env_path()}"
            print(error, file=sys.stderr)
            if args.json:
                _print_value(key, None, False, True, "env", error)
            return 1
        _print_value(key, value, args.show_secrets, args.json, "env")
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
    as_json = args.json
    db = _open_db(create=cmd in ("set", "import"))
    if db is None:
        if as_json:
            from copycord import jsonout
            from copycord.common.paths import db_path

            if cmd == "get":
                _print_value(args.key, None, False, True, "database", _no_db_text(db_path()))
            else:
                jsonout.emit({"error": _no_db_text(db_path())})
        return 1
    try:
        if cmd == "get":
            value = db.get_config(args.key, None)
            if value is None:
                print(f"{args.key} is not set", file=sys.stderr)
                if as_json:
                    _print_value(args.key, None, False, True, "database", f"{args.key} is not set")
                return 1
            _print_value(args.key, value, args.show_secrets, as_json, "database")
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
                if as_json:
                    from copycord import jsonout

                    jsonout.emit({"file": str(out), "keys": len(values)})
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


def _uninstall_program(dry_run: bool, purge: bool, removed: list, kept: list,
                       launchers: Sequence[str] = ()) -> bool:
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
    moved: list = []
    scratch = None
    if sys.platform == "win32":
        # Windows cannot delete the running copycord.exe or loaded .pyd/.dll files, but
        # can rename them: move them into a scratch folder deleted after this exits.
        from copycord import winupdate
        from copycord.common.paths import home_dir

        if not purge:
            winupdate.clean_old(home_dir(), launchers)
        try:
            scratch = winupdate.scratch_dir(sys.prefix)
            moved = winupdate.move_aside(sys.prefix, launchers, scratch)
        except OSError:
            scratch = None
    for command in commands:
        text = " ".join(command)
        res = run_cmd(command)
        if res.returncode == 0:
            ui.ok("removed the copycord program", text)
            removed.append("copycord program")
            _delete_aside(scratch, moved, purge)
            return ok
        detail = (res.stderr or res.stdout or "").strip().splitlines()
        ui.warn(f"{text} failed", detail[-1] if detail else f"exit {res.returncode}")
    if moved:
        winupdate.restore(moved)
    _delete_aside(scratch, moved, purge)
    kept.append(f"copycord program (remove it with: {manual})")
    return False


def _delete_aside(scratch, moved: list, purge: bool) -> None:
    """Windows: delete the moved-aside files (and old update leftovers) after exit."""
    if scratch is None:
        return
    from copycord import winupdate
    from copycord.common.paths import home_dir

    folders = [scratch, *(dest.parent for _src, dest in moved)]
    if not purge:
        folders.append(home_dir() / winupdate.ASIDE)
    winupdate.delete_after_exit(folders)


def uninstall_command(args: argparse.Namespace) -> int:
    from copycord.common.paths import home_dir
    from copycord import ui

    dry_run, yes = args.dry_run, args.yes
    # True deletes the data, False keeps it, None asks after the confirmation.
    purge: Optional[bool] = True if args.purge else False if args.keep_data else None
    if purge is None and (yes or dry_run):
        purge = False
    target = home_dir()
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

    # Windows cannot delete the files of a running Copycord (see winupdate).
    launchers: list = []
    if sys.platform == "win32" and not dry_run:
        from copycord import winupdate

        launchers, other = winupdate.running()
        if other is not None:
            ui.fail("Copycord is running in another window "
                    f"(process {other}); stop it first, then run this again")
            return 1

    title = "Uninstall Copycord" + (" (dry run, nothing changes)" if dry_run else "")
    with ui.Rail(title) as rail:
        return _uninstall_steps(args, target, purge, launchers, rail)


def _uninstall_steps(args: argparse.Namespace, target: Path, purge: Optional[bool],
                     launchers: list, rail) -> int:
    """uninstall_command from its title on: the question, then the removal."""
    from copycord import ui

    dry_run, yes = args.dry_run, args.yes
    ui.note("Removes the Copycord program and its background service.")
    ui.gap()

    if not yes and not dry_run:
        keep, delete = "Keep my data", "Delete everything (database, logs, backups, settings)"
        options = [keep, delete] if purge is None else [delete if purge else keep]
        try:
            picked = options + ["Cancel"]
            # Enter never deletes data: with --purge the default is Cancel.
            default = len(picked) - 1 if purge else 0
            question = f"Your data is in {ui.path_link(target)}"
            choice = picked[ui.choose(question, picked, default=default)]
        except (KeyboardInterrupt, EOFError):
            choice = "Cancel"
            ui.gap()
        else:
            if choice == "Cancel":
                ui.gap(blank=False)
        if choice == "Cancel":
            ui.warn("uninstall cancelled", "nothing was changed")
            return 1
        purge = choice == delete
        if purge:
            why = purge_refusal(target)
            if why:
                ui.fail("refusing to delete the Copycord home", why)
                return 1
        ui.gap()

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

    ok = _uninstall_program(dry_run, purge, removed, kept, launchers) and ok

    ui.gap()
    if dry_run:
        ui.info("Dry run: nothing changed")
    elif ok:
        ui.ok("Copycord uninstalled")
    else:
        ui.warn("Uninstall incomplete", "see the messages above", stream=sys.stdout)
    if not purge and target.exists():
        from copycord.common.paths import data_dir

        rail.close(f"  Your data is still in {ui.path_link(data_dir(), rail.stream)}")
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


def port_busy_message(port: int) -> None:
    """What `copycord run` prints when a port it needs stays in use."""
    from copycord import logging_setup, ui

    text = f"Port {port} is in use, so Copycord cannot start"
    hint = (f"another Copycord may still be running: stop it with "
            f"{ui.cmd('copycord service stop', stream=sys.stderr)}, or run "
            f"{ui.cmd('copycord doctor', stream=sys.stderr)} to check the port")
    if logging_setup.WINDOWLESS:
        # No console to print to: the log file is all there is.
        logging.getLogger("copycord.supervisor").error(
            "%s. Another Copycord may still be running: stop it with copycord service "
            "stop, or run copycord doctor to check the port", text)
    ui.fail(text, hint, stream=sys.stderr)


def migrate_on_start() -> None:
    """Move an old flat native home into data and logs folders; on failure keep it."""
    from copycord.common.paths import migrate_layout

    log = logging.getLogger("copycord.layout")
    try:
        migrate_layout(log=log.info)
    except Exception as e:
        log.warning("Could not move the data into the data folder, still using the old "
                    "layout: %s", e)


def windowless_streams() -> None:
    """Without a console (copycordw.exe, pythonw.exe) sys.stdin, sys.stdout and
    sys.stderr are None; point them at the null device so printing works."""
    if sys.stdout is not None and sys.stderr is not None:
        return
    from copycord import logging_setup

    logging_setup.WINDOWLESS = True
    for name, mode in (("stdin", "r"), ("stdout", "w"), ("stderr", "w")):
        if getattr(sys, name) is None:
            setattr(sys, name, open(os.devnull, mode, encoding="utf-8"))


def main(argv: Optional[List[str]] = None) -> int:
    """Run one command; an unexpected error prints a short report instead of a traceback."""
    windowless_streams()
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        return run_command(argv)
    except Exception as e:
        # KeyboardInterrupt and SystemExit are not Exceptions, so they pass through.
        from copycord import crash

        return crash.report(e, argv, verbose=wants_verbose(argv))


def run_command(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    apply_data_dir(args)
    from copycord import envfile

    envfile.load()

    if args.command == "doctor":
        return doctor(verbose=args.verbose, fix=args.fix, brief=args.brief, as_json=args.json)

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
            force=args.force,
            as_json=args.json,
        )

    if args.command == "run":
        from copycord.logging_setup import setup_logging

        # Console only here; supervisor.run adds the per-role files once DATA_DIR is set.
        setup_logging(roles=())
        if not check_removed_options():
            return 1
        migrate_on_start()
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
        except supervisor.PortBusyError as e:
            port_busy_message(e.port)
            return 1
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
