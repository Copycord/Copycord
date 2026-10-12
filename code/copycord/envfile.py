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

# Env-only keys and a one-line description each, in template order.
ENV_KEYS: Dict[str, str] = {
    "PASSWORD": "dashboard login password; no login page when unset",
    "SECRET_KEY": "signs the dashboard login cookie (default: a key saved in <home>)",
    "ADMIN_PORT": "dashboard port (default 8080)",
    "TZ": "time zone for the daily backup, e.g. Europe/Berlin (default UTC)",
    "UI_TZ": "time zone shown in exports (default America/New_York)",
    "LOG_FORMAT": "HUMAN or JSON log lines (default HUMAN)",
    "BACKUP_DIR": "where the daily database backups go (default <home>/backups)",
    "BACKUP_AT": "time of the daily backup, HH:MM (default 03:17)",
    "BACKUP_RETAIN": "how many daily backups to keep (default 14)",
    "COPYCORD_NO_UPDATE_CHECK": "1 turns off the update check on start",
}

# Tuning knobs read with os.getenv; set them only when asked to.
ENV_PREFIXES = ("BACKFILL_", "FORWARDING_")

_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")
_BARE = re.compile(r"^[A-Za-z0-9_./:@+,-]*$")


def is_env_key(key: str) -> bool:
    return key in ENV_KEYS or key.startswith(ENV_PREFIXES)


def env_path() -> Path:
    from common.paths import data_dir

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
        placeholder = f"#{key}="
        if placeholder in out:
            out[out.index(placeholder)] = new
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


def template() -> str:
    """The commented <home>/.env the installer writes on first install."""
    lines = [
        "# Copycord settings that are read from the environment only.",
        "# Everything else is set in the dashboard. Uncomment a line to use it,",
        "# or run `copycord config set KEY VALUE`, then restart Copycord.",
        "",
    ]
    for key, text in ENV_KEYS.items():
        lines.append(f"# {text}")
        lines.append(f"#{key}=")
        lines.append("")
    return "\n".join(lines)
