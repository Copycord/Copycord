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

The home is COPYCORD_HOME, else the home saved by the installer in
~/.copycord-home, else ~/.copycord. It holds .env, src, data and logs.
The data dir is DATA_DIR, else /data inside Docker, else <home>/data.
Docker and a custom DATA_DIR keep everything flat in the data dir.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Mapping, MutableMapping, Optional

DOCKER_DATA_DIR = "/data"
HOME_DIRNAME = ".copycord"
HOME_POINTER = ".copycord-home"
DATA_SUBDIR = "data"
LOGS_SUBDIR = "logs"

# Moved from <home> into <home>/data or <home>/logs by migrate_layout().
DATA_ENTRIES = (
    "data.db", "data.db-wal", "data.db-shm", "data.db-journal", "secret.key",
    "backups", "exports", "scrapes", "proxies.txt", "update.json", "update-check.json",
)
OUT_ENTRIES = tuple(
    f"{role}.out{suffix}" for role in ("server", "client", "scraper") for suffix in ("", ".1")
)

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
        # utf-8-sig: earlier Windows installers wrote the pointer with a BOM.
        saved = (home / HOME_POINTER).read_text(encoding="utf-8-sig").strip()
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
    """<home>/data; <home> itself while it still has the old flat layout; or the
    pre-4.0 directory if only that one has a data.db."""
    target = copycord_home(env, home) / DATA_SUBDIR
    if old_layout(target.parent):
        return target.parent
    if not target.parent.exists():
        old = legacy_data_dir(platform, env, home)
        if (old / "data.db").is_file():
            if str(old) not in _notified:
                _notified.add(str(old))
                print(
                    f"copycord: using the old data directory {old}; to move to "
                    f"{target.parent}, stop Copycord and move that directory there",
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


def home_dir(env: Optional[Mapping[str, str]] = None) -> Path:
    """Where .env and the logs folder live: the Copycord home when the data dir is
    <home>/data (or the old <home> itself), else the data dir (Docker, a custom
    DATA_DIR, the pre-4.0 directory)."""
    data = data_dir(env)
    home = copycord_home(env).resolve()
    return home if data in (home / DATA_SUBDIR, home) else data


def split_layout(env: Optional[Mapping[str, str]] = None) -> bool:
    """True when data and logs are separate folders under the home."""
    return home_dir(env) != data_dir(env)


def logs_dir(env: Optional[Mapping[str, str]] = None) -> Path:
    """<home>/logs (update.log; natively also the *.out role logs)."""
    return home_dir(env) / LOGS_SUBDIR


def out_dir(env: Optional[Mapping[str, str]] = None) -> Path:
    """Where server.out, client.out and scraper.out go: logs_dir() natively,
    the data dir in Docker or with a custom DATA_DIR."""
    return logs_dir(env) if split_layout(env) else data_dir(env)


def out_candidates(name: str, env: Optional[Mapping[str, str]] = None) -> list:
    """Paths to try for a *.out file, the current location first, then the
    pre-4.1 one (<data dir>, or <home> before the data folder)."""
    found = [out_dir(env) / name, data_dir(env) / name, home_dir(env) / name]
    return list(dict.fromkeys(found))


def old_layout(home: Path) -> bool:
    """data.db or secret.key directly in home and no home/data/data.db."""
    if (home / DATA_SUBDIR / "data.db").exists():
        return False
    return (home / "data.db").is_file() or (home / "secret.key").is_file()


def needs_migration(env: Optional[Mapping[str, str]] = None, docker: Optional[bool] = None) -> bool:
    """True when the old flat native layout is in use: data.db or secret.key
    directly in <home> and no <home>/data/data.db. Never inside Docker."""
    env = os.environ if env is None else env
    docker = in_docker() if docker is None else docker
    if docker:
        return False
    home = copycord_home(env).resolve()
    raw = env.get("DATA_DIR")
    # Older service units pass --data-dir <home>; that is the old default too.
    if raw and Path(raw).expanduser().resolve() != home:
        return False
    return old_layout(home)


def migrate_layout(
    env: Optional[MutableMapping[str, str]] = None,
    docker: Optional[bool] = None,
    log=None,
) -> Optional[str]:
    """Move an old flat native home into <home>/data and <home>/logs.

    Backs up data.db first, renames each entry (same volume, so atomic), never
    overwrites, and puts everything back if a rename fails. Returns the line it
    logged, or None when there was nothing to do. Raises OSError (with the old
    layout restored) when it could not finish.
    """
    env = os.environ if env is None else env
    if not needs_migration(env, docker):
        return None
    home = copycord_home(env).resolve()
    data, logs = home / DATA_SUBDIR, home / LOGS_SUBDIR
    db = home / "data.db"
    if db.is_file():
        from copycord.common.db import backup_pre_update

        raw = env.get("BACKUP_DIR")
        backup_pre_update(
            str(db), "layout",
            backup_dir=Path(raw).expanduser() if raw else home / "backups",
        )
    plan = [(home / n, data / n) for n in DATA_ENTRIES]
    plan += [(home / n, logs / n) for n in OUT_ENTRIES]
    moved = []
    kept = []
    created = [d for d in (data, logs) if not d.exists()]
    try:
        for d in (data, logs):
            d.mkdir(exist_ok=True)
        for src, dest in plan:
            if not os.path.lexists(src):
                continue
            if os.path.lexists(dest):
                kept.append(src.name)
                continue
            os.rename(src, dest)
            moved.append((src, dest))
    except OSError:
        for src, dest in reversed(moved):
            try:
                os.rename(dest, src)
            except OSError:
                pass
        for d in created:
            try:
                d.rmdir()
            except OSError:
                pass
        raise
    if env.get("DATA_DIR"):
        env["DATA_DIR"] = str(data)
    if env.get("DB_PATH") and Path(env["DB_PATH"]).expanduser().resolve() == db:
        env["DB_PATH"] = str(data / "data.db")
    line = f"Moved Copycord data into {data} and logs into {logs}"
    if kept:
        line += f" (left {', '.join(kept)} in {home}: already in the new place)"
    if log is not None:
        log(line)
    return line
