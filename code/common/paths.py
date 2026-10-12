# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Where Copycord keeps its data (DB, logs, backups, exports).

One visible home directory holds everything Copycord owns: ~/.copycord, or
COPYCORD_HOME when set. The data dir is the home root itself; a git install
keeps its checkout in src/ and its per-user Node in node/.

Precedence: DATA_DIR (Docker sets it to /data, `copycord run --data-dir` sets
it), then /data inside Docker, then COPYCORD_HOME, then ~/.copycord. The
installer saves a custom --home in ~/.copycord-home so the service and the CLI
agree without an environment variable; COPYCORD_HOME wins over that file.
Everything that needs the data dir resolves it here so the supervisor,
logging and config agree.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Mapping, Optional

DOCKER_DATA_DIR = "/data"
HOME_DIRNAME = ".copycord"
HOME_POINTER = ".copycord-home"

_notified: set = set()


def in_docker() -> bool:
    return os.path.exists("/.dockerenv")


def legacy_data_dir(
    platform: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    home: Optional[Path] = None,
) -> Path:
    """The pre-4.0 per-user platform data directory."""
    platform = sys.platform if platform is None else platform
    env = os.environ if env is None else env
    home = Path.home() if home is None else Path(home)
    if platform == "win32":
        local = env.get("LOCALAPPDATA") or str(home / "AppData" / "Local")
        return Path(local) / "Copycord"
    if platform == "darwin":
        return home / "Library" / "Application Support" / "Copycord"
    return home / ".local" / "share" / "copycord"


def copycord_home(
    env: Optional[Mapping[str, str]] = None,
    home: Optional[Path] = None,
) -> Path:
    """COPYCORD_HOME, else the path saved by the installer, else ~/.copycord."""
    env = os.environ if env is None else env
    home = Path.home() if home is None else Path(home)
    raw = env.get("COPYCORD_HOME")
    if raw:
        return Path(raw).expanduser()
    try:
        saved = (home / HOME_POINTER).read_text(encoding="utf-8").strip()
    except OSError:
        saved = ""
    if saved:
        return Path(saved).expanduser()
    return home / HOME_DIRNAME


def platform_data_dir(
    platform: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    home: Optional[Path] = None,
) -> Path:
    """The data directory for a native (non-Docker) install.

    The Copycord home, unless it does not exist yet while the old platform
    directory holds a data.db: that one keeps being used (nothing is moved).
    """
    target = copycord_home(env, home)
    if not target.exists():
        old = legacy_data_dir(platform, env, home)
        if (old / "data.db").is_file():
            if str(old) not in _notified:
                _notified.add(str(old))
                print(
                    f"copycord: using the old data directory {old}; to move to "
                    f"{target}, stop Copycord and move that directory there",
                    file=sys.stderr,
                )
            return old
    return target


def default_data_dir(
    platform: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    home: Optional[Path] = None,
    docker: Optional[bool] = None,
) -> Path:
    """The data dir when DATA_DIR is unset."""
    docker = in_docker() if docker is None else docker
    if docker:
        return Path(DOCKER_DATA_DIR)
    return platform_data_dir(platform, env, home)


def data_dir(env: Optional[Mapping[str, str]] = None) -> Path:
    """DATA_DIR if set, else default_data_dir(); absolute, not created."""
    env = os.environ if env is None else env
    raw = env.get("DATA_DIR")
    path = Path(raw).expanduser() if raw else default_data_dir(env=env)
    return path.resolve()


def db_path(env: Optional[Mapping[str, str]] = None) -> Path:
    """DB_PATH if set, else <data dir>/data.db."""
    env = os.environ if env is None else env
    raw = env.get("DB_PATH")
    return Path(raw).expanduser().resolve() if raw else data_dir(env) / "data.db"
