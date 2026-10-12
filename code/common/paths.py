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

DATA_DIR wins when set (Docker sets it to /data, `copycord run --data-dir`
sets it). Otherwise Docker falls back to /data and a native install uses the
per-user platform directory. Everything that needs the data dir resolves it
here so the supervisor, logging and config agree.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Mapping, Optional

DOCKER_DATA_DIR = "/data"


def in_docker() -> bool:
    return os.path.exists("/.dockerenv")


def platform_data_dir(
    platform: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    home: Optional[Path] = None,
) -> Path:
    """The per-user data directory for a native (non-Docker) install."""
    platform = sys.platform if platform is None else platform
    env = os.environ if env is None else env
    home = Path.home() if home is None else Path(home)
    if platform == "win32":
        local = env.get("LOCALAPPDATA") or str(home / "AppData" / "Local")
        return Path(local) / "Copycord"
    if platform == "darwin":
        return home / "Library" / "Application Support" / "Copycord"
    return home / ".local" / "share" / "copycord"


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
