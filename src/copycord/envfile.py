# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
<Copycord home>/.env: settings that are read from the environment only.

Most settings live in the app config (the database, edited in the dashboard).
A few are read with os.getenv before the database is open, or never go
through it (PASSWORD, ADMIN_PORT, TZ, ...). A native install has no compose
file to set them in, so `copycord` loads <home>/.env at startup without
overriding variables already set in the real environment, and
`copycord config set` writes these keys there.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List, Optional

FILENAME = ".env"

# Env-only keys and a one-line description each.
ENV_KEYS: Dict[str, str] = {
    "PASSWORD": "dashboard login password; change it from the default (empty: no login page)",
    "SECRET_KEY": "signs the dashboard login cookie (default: a key saved in <home>/secret.key)",
    "ADMIN_PORT": "dashboard port",
    "TZ": "time zone for the daily backup, e.g. Europe/Berlin",
    "UI_TZ": "time zone shown in exports",
    "LOG_FORMAT": "HUMAN or JSON log lines",
    "BACKUP_DIR": "where the daily database backups go",
    "BACKUP_AT": "time of the daily backup, HH:MM",
    "BACKUP_RETAIN": "how many daily backups to keep",
    "COPYCORD_NO_UPDATE_CHECK": "1 turns off the update check on start; 0 keeps it on",
}

# The dashboard password a new install starts with.
DEFAULT_PASSWORD = "copycord"

# The defaults the code uses, which the template writes as live values.
# BACKUP_DIR defaults to <home>/backups (see template).
ENV_DEFAULTS: Dict[str, str] = {
    "PASSWORD": DEFAULT_PASSWORD,
    "ADMIN_PORT": "8080",
    "TZ": "UTC",
    "UI_TZ": "America/New_York",
    "LOG_LEVEL": "INFO",
    "LOG_FORMAT": "HUMAN",
    "BACKUP_AT": "03:17",
    "BACKUP_RETAIN": "14",
    "COPYCORD_NO_UPDATE_CHECK": "0",
}

# LOG_LEVEL is in the template so the first log lines follow it, but it is an
# app config key: `copycord config` and the dashboard keep it in the database.
LOG_LEVEL_TEXT = "DEBUG or INFO; the dashboard's log level wins once it is saved"

# Copycord saves its own SECRET_KEY, so the template leaves it out and says so.
SECRET_KEY_TEXT = (
    "SECRET_KEY is left out: Copycord keeps its own key in <home>/secret.key"
)

# The keys the template lists, in order.
TEMPLATE_KEYS = (
    "PASSWORD",
    "SECRET_KEY",
    "ADMIN_PORT",
    "TZ",
    "UI_TZ",
    "LOG_LEVEL",
    "LOG_FORMAT",
    "BACKUP_DIR",
    "BACKUP_AT",
    "BACKUP_RETAIN",
    "COPYCORD_NO_UPDATE_CHECK",
)

# Tuning knobs read with os.getenv; set them only when asked to.
ENV_PREFIXES = ("BACKFILL_", "FORWARDING_")

_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")
_BARE = re.compile(r"^[A-Za-z0-9_./:@+,-]*$")


def is_env_key(key: str) -> bool:
    return key in ENV_KEYS or key.startswith(ENV_PREFIXES)


def env_path() -> Path:
    from copycord.common.paths import data_dir

    return data_dir() / FILENAME


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
        # Fill the template's commented placeholder, else append.
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
    """The <home>/.env the installer writes on first install: every key set
    to its default, with a one-line description above it."""
    home = env_path().parent if home is None else Path(home)
    defaults = {**ENV_DEFAULTS, "BACKUP_DIR": str(home / "backups")}
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
        else:
            lines += [f"# {texts[key]}", f"{key}={defaults[key]}", ""]
    return "\n".join(lines)


def write_template(path: Optional[Path] = None) -> Path:
    """Write template() to a missing <home>/.env (0600), as the installer
    does; FileExistsError when the file is there, it is never overwritten."""
    path = env_path() if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
        f.write(template(path.parent))
    return path
