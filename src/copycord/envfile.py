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

# Each key's comment line in the template.
ENV_KEYS: Dict[str, str] = {
    "PASSWORD": "Dashboard password",
    "SECRET_KEY": "Key that signs the login cookie",
    "ADMIN_PORT": "Dashboard port",
    "DASHBOARD_HOST": "Who can open the dashboard: 127.0.0.1 = this computer, 0.0.0.0 = your network",
    "TZ": "Time zone for backups and exports",
    "LOG_FORMAT": "Log style: HUMAN or JSON",
    "BACKUP_DIR": "Backup folder",
    "BACKUP_AT": "Daily backup time (HH:MM)",
    "BACKUP_RETAIN": "Backups to keep",
    "COPYCORD_NO_UPDATE_CHECK": "Skip the update check: 1 = yes, 0 = no",
}

DEFAULT_PASSWORD = "copycord"

# BACKUP_DIR is not here: the template writes the real folder for its home.
ENV_DEFAULTS: Dict[str, str] = {
    "PASSWORD": DEFAULT_PASSWORD,
    "ADMIN_PORT": "8080",
    "DASHBOARD_HOST": "127.0.0.1",
    "TZ": "UTC",
    "LOG_LEVEL": "INFO",
    "LOG_FORMAT": "HUMAN",
    "BACKUP_AT": "03:17",
    "BACKUP_RETAIN": "14",
    "COPYCORD_NO_UPDATE_CHECK": "0",
}

# LOG_LEVEL is an app config key; it is in the template so early log lines follow it.
LOG_LEVEL_TEXT = "Log detail: INFO or DEBUG"

# SECRET_KEY is not in the template: Copycord keeps its own in <home>/data/secret.key.
TEMPLATE_KEYS = (
    "PASSWORD",
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
    """<home>/.env, or the data dir's .env when only that one exists."""
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


def new_password() -> str:
    """A random first-run dashboard password."""
    import secrets

    return secrets.token_urlsafe(16)


def template(home: Optional[Path] = None, password: Optional[str] = None) -> str:
    """The default <home>/.env written on first install; password replaces
    the default PASSWORD."""
    home = env_path().parent if home is None else Path(home)
    values = dict(ENV_DEFAULTS)
    values["BACKUP_DIR"] = _quote(str(default_backup_dir(home)))
    if password is not None:
        values["PASSWORD"] = _quote(password)
    lines = [
        "# Copycord settings. Everything else is in the dashboard.",
        "# After a change, run: copycord restart",
        "",
    ]
    texts = {**ENV_KEYS, "LOG_LEVEL": LOG_LEVEL_TEXT}
    for key in TEMPLATE_KEYS:
        lines += [f"# {texts[key]}", f"{key}={values[key]}", ""]
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


def write_template(path: Optional[Path] = None, password: Optional[str] = None) -> Path:
    """Write template() to a new <home>/.env; raises FileExistsError if it exists."""
    path = env_path() if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
        f.write(template(path.parent, password))
    return path
