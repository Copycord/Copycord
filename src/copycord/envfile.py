# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
<home>/.env: the settings read from the environment rather than the database
(PASSWORD, ADMIN_PORT, TZ, ...). Loaded at startup without overriding the
real environment.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List, Optional

FILENAME = ".env"

ENV_KEYS: Dict[str, str] = {
    "PASSWORD": "dashboard login password; change it from the default (empty: no login page)",
    "SECRET_KEY": "signs the dashboard login cookie (default: a key saved in <home>/data/secret.key)",
    "ADMIN_PORT": "dashboard port",
    "DASHBOARD_HOST": "where the dashboard listens; 127.0.0.1 is this computer only",
    "TZ": "time zone for scheduled backups and export timestamps, e.g. America/New_York",
    "LOG_FORMAT": "HUMAN or JSON log lines",
    "BACKUP_DIR": "where the daily database backups go",
    "BACKUP_AT": "time of the daily backup, HH:MM",
    "BACKUP_RETAIN": "how many daily backups to keep",
    "COPYCORD_NO_UPDATE_CHECK": "1 turns off the update check on start; 0 keeps it on",
}

DEFAULT_PASSWORD = "copycord"

# BACKUP_DIR is left out: the default <data dir>/backups applies, and the
# template shows it as a commented example.
ENV_DEFAULTS: Dict[str, str] = {
    "PASSWORD": DEFAULT_PASSWORD,
    "ADMIN_PORT": "8080",
    "TZ": "UTC",
    "LOG_LEVEL": "INFO",
    "LOG_FORMAT": "HUMAN",
    "BACKUP_AT": "03:17",
    "BACKUP_RETAIN": "14",
    "COPYCORD_NO_UPDATE_CHECK": "0",
}

# LOG_LEVEL is an app config key; it is in the template so early log lines follow it.
LOG_LEVEL_TEXT = "DEBUG or INFO; the dashboard's log level wins once it is saved"

SECRET_KEY_TEXT = (
    "SECRET_KEY is left out: Copycord keeps its own key in <home>/data/secret.key"
)

DASHBOARD_HOST_TEXT = (
    "the dashboard is on this computer only (127.0.0.1); remove the # and use 0.0.0.0\n"
    "# to open it to your network, after setting your own PASSWORD"
)

BACKUP_DIR_TEXT = "where the daily database backups go; remove the # to use another folder"

TEMPLATE_KEYS = (
    "PASSWORD",
    "SECRET_KEY",
    "ADMIN_PORT",
    "DASHBOARD_HOST",
    "TZ",
    "LOG_LEVEL",
    "LOG_FORMAT",
    "BACKUP_DIR",
    "BACKUP_AT",
    "BACKUP_RETAIN",
    "COPYCORD_NO_UPDATE_CHECK",
)

# Tuning knobs read with os.getenv; not in the template.
ENV_PREFIXES = ("BACKFILL_", "FORWARDING_")

_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")
_BARE = re.compile(r"^[A-Za-z0-9_./:@+,-]*$")


def is_env_key(key: str) -> bool:
    return key in ENV_KEYS or key.startswith(ENV_PREFIXES)


def env_path() -> Path:
    """<home>/.env; a .env left in a custom data dir is still read for now."""
    from copycord.common.paths import data_dir, home_dir

    path = home_dir() / FILENAME
    old = data_dir() / FILENAME
    if not path.exists() and old.is_file():
        return old
    return path


def load(path: Optional[Path] = None) -> bool:
    """Load <home>/.env into os.environ; real environment variables win."""
    path = env_path() if path is None else path
    if not path.is_file():
        return False
    from dotenv import load_dotenv

    return load_dotenv(path, override=False)


def get(key: str, path: Optional[Path] = None) -> Optional[str]:
    path = env_path() if path is None else path
    if not path.is_file():
        return None
    from dotenv import dotenv_values

    return dotenv_values(path).get(key)


def _quote(value: str) -> str:
    if _BARE.match(value):
        return value
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _read_lines(path: Path) -> List[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []


def _write_lines(path: Path, lines: List[str]) -> None:
    text = "\n".join(lines) + ("\n" if lines else "")
    path.parent.mkdir(parents=True, exist_ok=True)
    # A new file is created 0600 (it can hold PASSWORD); an existing one keeps its mode.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def set_value(key: str, value: str, path: Optional[Path] = None) -> Path:
    """Set KEY in the file, in place when it is there; comments are kept."""
    path = env_path() if path is None else path
    lines = _read_lines(path)
    new = f"{key}={_quote(value)}"
    done = False
    out = []
    for line in lines:
        m = _LINE.match(line)
        if m and m.group(1) == key:
            if not done:
                out.append(new)
                done = True
            continue
        out.append(line)
    if not done:
        placeholder = next(
            (i for i, line in enumerate(out) if line.startswith(f"#{key}=")), None
        )
        if placeholder is not None:
            out[placeholder] = new
        else:
            out.append(new)
    _write_lines(path, out)
    return path


def unset(key: str, path: Optional[Path] = None) -> bool:
    """Remove KEY from the file; False when it was not set there."""
    path = env_path() if path is None else path
    lines = _read_lines(path)
    kept = [line for line in lines if not ((m := _LINE.match(line)) and m.group(1) == key)]
    if len(kept) == len(lines):
        return False
    _write_lines(path, kept)
    return True


def template(home: Optional[Path] = None) -> str:
    """The default <home>/.env written on first install."""
    home = env_path().parent if home is None else Path(home)
    lines = [
        "# Copycord settings that are read from the environment only.",
        "# Everything else is set in the dashboard. Each value below is the default.",
        "# Edit one here or run `copycord config set KEY VALUE`, then run",
        "# `copycord restart` to apply it.",
        "",
    ]
    texts = {**ENV_KEYS, "LOG_LEVEL": LOG_LEVEL_TEXT}
    for key in TEMPLATE_KEYS:
        if key == "SECRET_KEY":
            lines += [f"# {SECRET_KEY_TEXT}", ""]
        elif key == "DASHBOARD_HOST":
            lines += [f"# {DASHBOARD_HOST_TEXT}", "#DASHBOARD_HOST=127.0.0.1", ""]
        elif key == "BACKUP_DIR":
            lines += [f"# {BACKUP_DIR_TEXT}", f"#BACKUP_DIR={default_backup_dir(home)}", ""]
        else:
            lines += [f"# {texts[key]}", f"{key}={ENV_DEFAULTS[key]}", ""]
    return "\n".join(lines)


def default_backup_dir(home: Path) -> Path:
    """The backups folder used when BACKUP_DIR is not set, for the .env in home:
    <home>/data/backups natively, <data dir>/backups when .env sits in the data dir."""
    from copycord.common.paths import DATA_SUBDIR, data_dir, home_dir

    home = Path(home).resolve()
    if home == home_dir() and home != data_dir():
        return data_dir() / "backups"
    if home == data_dir():
        return home / "backups"
    return home / DATA_SUBDIR / "backups"


def write_template(path: Optional[Path] = None) -> Path:
    """Write template() to a new <home>/.env; raises FileExistsError if it exists."""
    path = env_path() if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
        f.write(template(path.parent))
    return path
