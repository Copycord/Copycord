# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
copycord onboard                      guided setup in a terminal
copycord onboard --non-interactive [--server-token T] [--client-token T]
                 [--command-users IDS] [--port PORT]
                                      the same without prompts; values also
                                      come from SERVER_TOKEN, CLIENT_TOKEN,
                                      COMMAND_USERS and ADMIN_PORT

The essentials are the ones common.config.Config reads with no usable
default: SERVER_TOKEN (the bot in the clone server) and CLIENT_TOKEN (the
account that reads the source server). COMMAND_USERS is optional. They are
saved to app_config in the database, which Config reads before the
environment. `copycord run` starts this on first start when neither token is
saved and a terminal is attached.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

from copycord import ui

ESSENTIALS = ("SERVER_TOKEN", "CLIENT_TOKEN")
DEFAULT_PORT = 8080


def _open_db():
    from common.db import DBManager
    from common.paths import db_path

    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    return DBManager(str(path), init_schema=True)


def _stored(db, key: str) -> str:
    try:
        return (db.get_config(key, "") or "").strip()
    except Exception:
        return ""


def missing_essentials(db=None) -> list:
    """Essential keys set neither in app_config nor in the environment."""
    close = db is None
    if db is None:
        from common.paths import db_path

        if not db_path().exists():
            return [k for k in ESSENTIALS if not os.environ.get(k, "").strip()]
        db = _open_db()
    try:
        return [k for k in ESSENTIALS
                if not _stored(db, k) and not os.environ.get(k, "").strip()]
    finally:
        if close:
            db.conn.close()


def looks_like_token(value: str) -> bool:
    """Discord tokens are three dot-separated base64 parts."""
    parts = value.split(".")
    return len(parts) == 3 and all(parts) and " " not in value


def valid_user_ids(raw: str) -> bool:
    ids = [p.strip() for p in raw.split(",") if p.strip()]
    return all(p.isdigit() for p in ids)


def dashboard_url(port: int) -> str:
    return f"http://localhost:{port}"


def _port(args: argparse.Namespace) -> int:
    if getattr(args, "port", None):
        return args.port
    try:
        return int(os.environ.get("ADMIN_PORT") or DEFAULT_PORT)
    except ValueError:
        return DEFAULT_PORT


def _ask_token(label: str, key: str, help_text: str) -> str:
    ui.line("step", label, help_text)
    while True:
        value = ui.ask(f"{key}", secret=True)
        if not value:
            ui.warn("a value is required", "press Ctrl+C to stop", stream=sys.stdout)
            continue
        if not looks_like_token(value):
            ui.warn("that does not look like a Discord token", stream=sys.stdout)
            if not ui.confirm("Use it anyway?", default=False):
                continue
        return value


def _finish(port: int, saved: list, stream=None) -> None:
    from common.paths import data_dir

    stream = sys.stdout if stream is None else stream
    url = dashboard_url(port)
    lines = [
        f"Dashboard  {ui.bold(url)}",
        f"Data dir   {data_dir()}",
        "",
        f"Start now        {ui.cmd('copycord run')}",
        f"Run as service   {ui.cmd('copycord service install')}",
        f"Check setup      {ui.cmd('copycord doctor')}",
    ]
    if port != DEFAULT_PORT:
        lines.append(f"Port {port}: start with ADMIN_PORT={port} set")
    if not os.environ.get("PASSWORD"):
        lines.append(ui.dim("Tip: set PASSWORD to require a dashboard login"))
    print(file=stream)
    ui.box(lines, title="Copycord is ready", status="ok", stream=stream)


def onboard(args: argparse.Namespace) -> int:
    interactive = ui.is_interactive() and not args.non_interactive
    db = _open_db()
    try:
        return _onboard(args, db, interactive)
    except (KeyboardInterrupt, EOFError):
        print()
        ui.warn("onboarding cancelled", "nothing more was saved")
        return 130
    finally:
        db.conn.close()


def _onboard(args: argparse.Namespace, db, interactive: bool) -> int:
    from common.config import CURRENT_VERSION
    from common.paths import data_dir

    port = _port(args)
    given = {
        "SERVER_TOKEN": args.server_token or os.environ.get("SERVER_TOKEN", ""),
        "CLIENT_TOKEN": args.client_token or os.environ.get("CLIENT_TOKEN", ""),
        "COMMAND_USERS": args.command_users or os.environ.get("COMMAND_USERS", ""),
    }
    given = {k: v.strip() for k, v in given.items()}

    if interactive:
        ui.banner(CURRENT_VERSION)
        ui.heading("Welcome to Copycord")
        print(ui.dim("This sets up what Copycord needs to start. "
                     "Press Enter to keep a value that is already saved."))
        print()
    ui.ok("data directory", str(data_dir()))

    values = {}
    for key, label, help_text in (
        ("SERVER_TOKEN", "Server bot token",
         "the bot you added to your clone server (Discord Developer Portal)"),
        ("CLIENT_TOKEN", "Client token",
         "the account that is a member of the source server"),
    ):
        current = _stored(db, key)
        if given[key]:
            values[key] = given[key]
        elif current and not args.force:
            ui.ok(label, "already saved")
        elif interactive:
            values[key] = _ask_token(label, key, help_text)
        else:
            ui.fail(f"{label} is missing",
                    f"pass --{key.lower().replace('_', '-')} or set {key}")
            return 1
        if key in values and not looks_like_token(values[key]) and not interactive:
            ui.warn(f"{label} does not look like a Discord token", "saved anyway")

    users = given["COMMAND_USERS"]
    if not users and interactive and (args.force or not _stored(db, "COMMAND_USERS")):
        ui.line("step", "Command users",
                "Discord user IDs allowed to run bot commands, comma-separated (optional)")
        while True:
            users = ui.ask("COMMAND_USERS", default=_stored(db, "COMMAND_USERS"))
            if valid_user_ids(users):
                break
            ui.warn("use numeric user IDs separated by commas", stream=sys.stdout)
    if users:
        if not valid_user_ids(users):
            ui.fail("COMMAND_USERS must be numeric user IDs separated by commas")
            return 1
        values["COMMAND_USERS"] = ",".join(p.strip() for p in users.split(",") if p.strip())

    if interactive and not getattr(args, "port", None):
        while True:
            raw = ui.ask("Dashboard port", default=str(port))
            if raw.isdigit() and 0 < int(raw) < 65536:
                port = int(raw)
                break
            ui.warn("enter a port between 1 and 65535", stream=sys.stdout)

    for key, value in values.items():
        db.set_config(key, value)
        shown = "saved" if "TOKEN" not in key else "saved (hidden)"
        ui.ok(key, shown)

    _finish(port, list(values))
    return 0


def maybe_onboard_first_run() -> Optional[int]:
    """Run the onboarding before `copycord run` when unconfigured in a terminal.

    None to go on starting Copycord; otherwise the exit code to stop with.
    """
    if not ui.is_interactive() or os.environ.get("COPYCORD_NO_ONBOARD"):
        return None
    try:
        missing = missing_essentials()
    except Exception:
        return None
    if len(missing) < len(ESSENTIALS):
        return None
    ui.info("Copycord is not set up yet; starting the onboarding",
            "skip with --no-onboard")
    args = argparse.Namespace(
        non_interactive=False, server_token=None, client_token=None,
        command_users=None, port=None, force=False,
    )
    rc = onboard(args)
    if rc:
        return rc
    print()
    return None if ui.confirm("Start Copycord now?", default=True) else 0

