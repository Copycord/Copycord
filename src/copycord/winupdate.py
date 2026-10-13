# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Windows only: `uv tool install --force` deletes the tool environment, and
Windows cannot delete a running .exe or a loaded .pyd/.dll, but it can rename
them. So right before the install the executable files of the environment and
the running launcher are moved into a .copycord-old folder on the same drive;
the install then runs in this process like on Linux, and the next update
deletes the old folder.
"""

from __future__ import annotations

import json
import ntpath
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

# Parents that mean a process was started by Task Scheduler (the service).
_SERVICE_PARENTS = {"svchost.exe", "taskeng.exe", "taskhostw.exe"}

_PS_QUERY = (
    "Get-CimInstance Win32_Process | "
    "Select-Object ProcessId,ParentProcessId,Name,ExecutablePath | "
    "ConvertTo-Json -Compress"
)

def _processes() -> Optional[list]:
    try:
        res = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", _PS_QUERY],
            capture_output=True, text=True, timeout=30,
        )
        rows = json.loads(res.stdout or "null")
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    if isinstance(rows, dict):
        rows = [rows]
    return rows if isinstance(rows, list) else None


def _under(path: Optional[str], root: str) -> bool:
    if not path:
        return False
    try:
        return ntpath.normcase(ntpath.normpath(path)).startswith(
            ntpath.normcase(ntpath.normpath(root)) + "\\"
        )
    except (TypeError, ValueError):
        return False


def scan(rows: list, me: int, prefix: str):
    """(pids of this command to wait for, pid of another running Copycord or None)."""
    by_pid = {r.get("ProcessId"): r for r in rows}

    def is_copycord(r) -> bool:
        name = (r.get("Name") or "").lower()
        return name == "copycord.exe" or _under(r.get("ExecutablePath"), prefix)

    mine: List[int] = []
    pid, seen = me, set()
    while pid in by_pid and pid not in seen:
        seen.add(pid)
        r = by_pid[pid]
        if pid != me and not is_copycord(r):
            break
        mine.append(pid)
        pid = r.get("ParentProcessId")

    # A launcher of another install (another folder) does not lock these files.
    dirs = {ntpath.normcase(ntpath.dirname(p)) for p in
            (by_pid[i].get("ExecutablePath") for i in mine if i != me)
            if p and not _under(p, prefix)}

    def blocks(r) -> bool:
        path = r.get("ExecutablePath")
        if _under(path, prefix):
            return True
        return is_copycord(r) and (not dirs or not path
                                   or ntpath.normcase(ntpath.dirname(path)) in dirs)

    for r in rows:
        pid = r.get("ProcessId")
        if pid in seen or not blocks(r):
            continue
        top, hops = r, 0
        while hops < 50:
            parent = by_pid.get(top.get("ParentProcessId"))
            if parent is None or not is_copycord(parent):
                break
            top, hops = parent, hops + 1
        parent = by_pid.get(top.get("ParentProcessId"))
        if parent is not None and (parent.get("Name") or "").lower() in _SERVICE_PARENTS:
            continue
        return mine, pid
    return mine, None


ASIDE = ".copycord-old"
_LOCKED = {".exe", ".dll", ".pyd"}


def _aside_dir(path: Path, home: Path) -> Path:
    """Where path is moved: under the Copycord home when on the same drive."""
    same = ntpath.splitdrive(str(path))[0].lower() == ntpath.splitdrive(str(home))[0].lower()
    return (home if same else path.parent) / ASIDE / str(os.getpid())


def clean_old(home: Path, launchers: List[str]) -> None:
    """Delete what earlier updates moved aside."""
    for folder in {home / ASIDE, *(Path(p).parent / ASIDE for p in launchers)}:
        if folder.is_dir():
            shutil.rmtree(folder, ignore_errors=True)


def move_aside(prefix: str, launchers: List[str], home: Path,
               failed: Optional[List[str]] = None) -> List[Tuple[Path, Path]]:
    """Rename the executable files of the environment and the launchers out of the way."""
    files = [p for p in Path(prefix).rglob("*") if p.suffix.lower() in _LOCKED and p.is_file()]
    files += [Path(p) for p in launchers if Path(p).is_file()]
    moved = []
    for i, src in enumerate(files):
        dest = _aside_dir(src, home) / f"{i}-{src.name}"
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, dest)
        except OSError as e:
            if failed is not None:
                failed.append(f"{src}: {e}")
            continue
        moved.append((src, dest))
    return moved


def restore(moved: List[Tuple[Path, Path]]) -> None:
    """Put moved files back when the install left the old environment in place."""
    for src, dest in moved:
        try:
            if src.parent.is_dir() and not src.exists():
                os.replace(dest, src)
        except OSError:
            pass


def running():
    """(launcher .exe paths of this command outside the environment, pid of a
    Copycord running outside the service or None)."""
    rows = _processes()
    if rows is None:
        return [], None
    mine, other = scan(rows, os.getpid(), sys.prefix)
    by_pid = {r.get("ProcessId"): r for r in rows}
    exes = [by_pid[p].get("ExecutablePath") for p in mine if p != os.getpid()]
    return [e for e in exes if e and not _under(e, sys.prefix)], other
