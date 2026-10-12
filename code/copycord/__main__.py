# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
copycord onboard              guided first-run setup (tokens, command users, port)
copycord run                  admin, server and client in one process
copycord version              print the version and the update channel
copycord doctor               preflight checks (no network)
copycord config get KEY       read one app_config value from the DB
copycord config set KEY VAL   write one app_config value
copycord config export [FILE] app_config as JSON (secrets masked on stdout)
copycord config import FILE   load app_config values from a JSON object
copycord service install [--user] [--data-dir PATH]
                              run Copycord as a systemd/launchd/Scheduled Task service
copycord service start|stop|status|uninstall [--user]
copycord update [--channel stable|beta|dev [--repo OWNER/NAME] [--ref REF]]
                [--tag VERSION] [--no-install]
                              update (uv tool / pipx) from the saved or given
                              channel and restart a running service
copycord update status        installed version, channel and latest on it
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
from typing import List, Mapping, NamedTuple, Optional

logger = logging.getLogger("copycord")

ROLE_REMOVED_MESSAGE = (
    "ROLE is set, but Copycord v4 runs admin, server and client in one container "
    "and the ROLE option was removed. Delete ROLE from your compose file "
    "(and the extra server/client services), or stay on a v3 image tag."
)

MIN_PYTHON = (3, 12)
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

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"


# (group, [(command, one-line description)]) for the top-level help.
COMMAND_GROUPS = (
    ("Get started", (
        ("onboard", "Guided first-run setup: tokens, command users, port"),
        ("run", "Run Copycord (dashboard, server bot and client)"),
        ("doctor", "Check this machine can run Copycord"),
    )),
    ("Settings", (
        ("config", "Read or write the stored app config (get, set, export, import)"),
    )),
    ("Service", (
        ("service", "Run Copycord as an OS service (install, start, stop, status)"),
    )),
    ("Updates", (
        ("update", "Update Copycord; switch channel with --channel stable|beta|dev"),
        ("version", "Print the version and the update channel"),
    )),
)

EXAMPLES = (
    ("copycord onboard", "set up Copycord step by step"),
    ("copycord run", "start Copycord in this terminal"),
    ("copycord service install", "run it in the background"),
    ("copycord update status", "see whether an update is available"),
    ("copycord update --channel beta", "switch to pre-releases"),
    ("copycord update --channel dev --ref main", "install from git"),
)


def _version() -> str:
    try:
        from common.config import CURRENT_VERSION

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
    w(f"  {ui.cmd('-V, --version'.ljust(opt_width))}  Print the version and exit")
    w()
    w(ui.dim("Run `copycord <command> --help` for the options of one command."))
    w(ui.dim("Docs: https://copycord.github.io/Copycord/docs/cli"))
    return buf.getvalue()


class _Parser(argparse.ArgumentParser):
    def format_help(self) -> str:
        if self.prog == "copycord":
            return format_help()
        return super().format_help()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = _Parser(prog="copycord")
    parser.add_argument("-V", "--version", action="version", version=f"copycord {_version()}")
    sub = parser.add_subparsers(dest="command", required=True, parser_class=argparse.ArgumentParser)

    data_dir = argparse.ArgumentParser(add_help=False)
    data_dir.add_argument(
        "--data-dir", metavar="PATH", help="data directory (sets DATA_DIR)"
    )

    onboard = sub.add_parser(
        "onboard",
        parents=[data_dir],
        help="guided first-run setup",
        description="Guided setup of what Copycord needs: the server bot token, "
        "the client token, the command users and the dashboard port. "
        "Values go to the app config in the database.",
        epilog="examples:\n"
        "  copycord onboard\n"
        "  copycord onboard --non-interactive --server-token XXX --client-token YYY\n"
        "  SERVER_TOKEN=XXX CLIENT_TOKEN=YYY copycord onboard --non-interactive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    onboard.add_argument(
        "--non-interactive",
        action="store_true",
        help="no prompts: take values from the flags and the environment",
    )
    onboard.add_argument("--server-token", metavar="TOKEN",
                         help="Discord bot token for the clone server (SERVER_TOKEN)")
    onboard.add_argument("--client-token", metavar="TOKEN",
                         help="Discord user token that reads the source server (CLIENT_TOKEN)")
    onboard.add_argument("--command-users", metavar="IDS",
                         help="comma-separated Discord user IDs allowed to run bot commands")
    onboard.add_argument("--port", type=int, metavar="PORT",
                         help="dashboard port to show in the URL (ADMIN_PORT, default 8080)")
    onboard.add_argument("--force", action="store_true",
                         help="ask again even for values that are already set")

    sub.add_parser(
        "run",
        parents=[data_dir],
        help="run Copycord",
        description="Run the dashboard, the server bot and the client in one process. "
        "On first start in a terminal, with no tokens saved yet, runs `copycord onboard` first.",
    ).add_argument("--no-onboard", action="store_true",
                   help="never start the onboarding, even when unconfigured")
    sub.add_parser(
        "version", parents=[data_dir], help="print the Copycord version and update channel"
    )
    sub.add_parser(
        "doctor", parents=[data_dir], help="check this machine can run Copycord"
    )

    config = sub.add_parser("config", help="read or write the stored app config")
    csub = config.add_subparsers(dest="config_command", required=True)
    get = csub.add_parser("get", parents=[data_dir], help="print one value")
    get.add_argument("key")
    set_ = csub.add_parser("set", parents=[data_dir], help="store one value")
    set_.add_argument("key")
    set_.add_argument("value")
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
    service = sub.add_parser("service", help="run Copycord as an OS service")
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
        epilog="channels:\n"
        "  stable  newest GitHub release\n"
        "  beta    newest GitHub release, pre-releases included\n"
        "  dev     git install: uv tool install --force git+https://github.com/<repo>@<ref>\n"
        "          (needs npm to build the admin UI)\n"
        "\nexamples:\n"
        "  copycord update status\n"
        "  copycord update\n"
        "  copycord update --channel beta\n"
        "  copycord update --channel dev --ref v4-dev\n"
        "  copycord update --channel dev --repo you/Copycord --ref 1a2b3c4\n"
        "  copycord update --tag 4.0.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    update.add_argument(
        "action",
        nargs="?",
        choices=("status",),
        help="status: print the installed version, the channel and the latest on it",
    )
    update.add_argument(
        "--check",
        action="store_true",
        help="same as `copycord update status`",
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
        "--branch",
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
    reason: str


def check_python(version_info=None) -> Check:
    v = sys.version_info if version_info is None else version_info
    found = ".".join(str(p) for p in tuple(v)[:3])
    if tuple(v)[:2] >= MIN_PYTHON:
        return Check(PASS, "python", f"Python {found}")
    need = ".".join(str(p) for p in MIN_PYTHON)
    return Check(FAIL, "python", f"Python {found}, Copycord needs {need} or newer")


def check_data_dir(path: Path) -> Check:
    if path.exists():
        if not path.is_dir():
            return Check(FAIL, "data dir", f"{path} exists but is not a directory")
        try:
            with tempfile.TemporaryFile(dir=path):
                pass
        except OSError as e:
            return Check(FAIL, "data dir", f"{path} is not writable ({e})")
        return Check(PASS, "data dir", f"{path} is writable")
    parent = path.parent
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent
    if parent.is_dir() and os.access(parent, os.W_OK | os.X_OK):
        return Check(PASS, "data dir", f"{path} will be created on first run")
    return Check(
        FAIL, "data dir", f"{path} cannot be created ({parent} is not writable)"
    )


def check_admin_port(
    env: Optional[Mapping[str, str]] = None, host: str = ADMIN_BIND_HOST
) -> Check:
    env = os.environ if env is None else env
    raw = env.get("ADMIN_PORT") or str(DEFAULT_ADMIN_PORT)
    try:
        port = int(raw)
        if not 0 < port < 65536:
            raise ValueError
    except ValueError:
        return Check(FAIL, "admin port", f"ADMIN_PORT={raw!r} is not a valid port")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Same as uvicorn: lingering TIME_WAIT sockets do not count as in use.
        # (On Windows SO_REUSEADDR would allow sharing a live port, so skip it.)
        if sys.platform != "win32":
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
        except OSError as e:
            return Check(
                FAIL,
                "admin port",
                f"port {port} is in use ({e.strerror or e}); is Copycord already "
                "running? Set ADMIN_PORT to use another port",
            )
    return Check(PASS, "admin port", f"port {port} is free")


def admin_static_dir() -> Optional[Path]:
    """admin/static of the importable admin package, without importing admin.app."""
    try:
        spec = importlib.util.find_spec("admin")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.submodule_search_locations:
        return None
    return Path(list(spec.submodule_search_locations)[0]) / "static"


def check_frontend(static_dir: Optional[Path] = None) -> Check:
    static_dir = admin_static_dir() if static_dir is None else static_dir
    if static_dir is None:
        return Check(FAIL, "frontend", "the admin package cannot be found")
    main_js = static_dir / "assets" / "main.js"
    if main_js.is_file():
        return Check(PASS, "frontend", f"assets in {static_dir}")
    return Check(
        WARN,
        "frontend",
        f"{main_js} is missing, the admin UI will not load "
        "(run `npm run build` in code/admin/frontend and copy dist/ to admin/static)",
    )


def check_role(env: Optional[Mapping[str, str]] = None) -> Check:
    env = os.environ if env is None else env
    if "ROLE" in env:
        return Check(FAIL, "ROLE", ROLE_REMOVED_MESSAGE)
    return Check(PASS, "ROLE", "unset")


def check_import(module: str, dist: str) -> Check:
    from importlib import metadata

    name = f"import {module}"
    try:
        importlib.import_module(module)
    except Exception as e:
        return Check(FAIL, name, f"cannot import {module}: {e!r} (install {dist})")
    try:
        version = metadata.version(dist)
    except metadata.PackageNotFoundError:
        return Check(
            WARN, name, f"{module} imports, but {dist} is not installed (wrong package?)"
        )
    return Check(PASS, name, f"{dist} {version}")


def check_db(path: Path) -> Check:
    if not path.exists():
        return Check(PASS, "database", f"{path} will be created on first run")
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
        return Check(FAIL, "database", f"{path} does not open: {e}")
    if "app_config" not in tables:
        return Check(
            WARN, "database", f"{path} opens but has no app_config table yet"
        )
    return Check(PASS, "database", f"{path} opens")


def run_doctor() -> List[Check]:
    from common.paths import data_dir, db_path

    checks = [
        check_python(),
        check_role(),
        check_data_dir(data_dir()),
        check_db(db_path()),
        check_admin_port(),
        check_frontend(),
    ]
    checks.extend(check_import(m, d) for m, d in REQUIRED_IMPORTS)
    return checks


def doctor() -> int:
    from copycord import ui

    checks = run_doctor()
    if ui.use_color():
        ui.heading("Copycord doctor")
    status = {PASS: "ok", WARN: "warn", FAIL: "fail"}
    for c in checks:
        ui.line(status[c.status], c.name, c.reason, stream=sys.stdout)
    failed = sum(c.status == FAIL for c in checks)
    if ui.use_color():
        print()
        if failed:
            ui.fail(f"{failed} check(s) failed", stream=sys.stdout)
        else:
            ui.ok("ready to run", ui.cmd("copycord run"))
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
    from common.db import DBManager
    from common.paths import db_path

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


def config_command(args: argparse.Namespace) -> int:
    cmd = args.config_command
    db = _open_db(create=cmd in ("set", "import"))
    if db is None:
        return 1
    try:
        if cmd == "get":
            value = db.get_config(args.key, None)
            if value is None:
                print(f"{args.key} is not set", file=sys.stderr)
                return 1
            print(value)
            return 0

        if cmd == "set":
            db.set_config(args.key, args.value)
            print(f"{args.key} saved; restart Copycord to apply", file=sys.stderr)
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
            print(f"imported {imported} keys; restart Copycord to apply",
                  file=sys.stderr)
            return 0
    finally:
        db.conn.close()
    return 2


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    apply_data_dir(args)

    if args.command == "version":
        from common.config import CURRENT_VERSION
        from copycord import ui
        from copycord.update import load_channel

        if ui.use_color():
            ui.banner(CURRENT_VERSION)
            ui.kv([("version", CURRENT_VERSION), ("channel", load_channel().describe())])
        else:
            print(CURRENT_VERSION)
            print(f"channel {load_channel().describe()}")
        return 0

    if args.command == "onboard":
        from copycord.onboard import onboard

        return onboard(args)

    if args.command == "doctor":
        return doctor()

    if args.command == "config":
        return config_command(args)

    if args.command == "service":
        from copycord.service import service_command

        return service_command(args)

    if args.command == "update":
        from copycord.update import update

        return update(
            check_only=args.check or args.action == "status",
            channel=args.channel,
            branch=args.branch,
            tag=args.tag,
            no_install=args.no_install,
            repo=args.repo,
        )

    if args.command == "run":
        from copycord.logging_setup import setup_logging

        # Console only here; supervisor.run adds the per-role files once DATA_DIR is set.
        setup_logging(roles=())
        if not check_removed_options():
            return 1

        if not getattr(args, "no_onboard", False):
            from copycord.onboard import maybe_onboard_first_run

            rc = maybe_onboard_first_run()
            if rc is not None:
                return rc

        from copycord import supervisor

        try:
            asyncio.run(supervisor.run())
        except KeyboardInterrupt:
            pass
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
