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
Windows cannot delete the copycord.exe and python.exe that run `copycord
update`. So the install runs in a helper started from the base Python with a
script copied to a temp dir; it waits for this process to exit, stops the
service, installs, and restarts the service.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Mapping, Optional, Sequence

from copycord import service

# Parents that mean a process was started by Task Scheduler (the service).
_SERVICE_PARENTS = {"svchost.exe", "taskeng.exe", "taskhostw.exe"}

_PS_QUERY = (
    "Get-CimInstance Win32_Process | "
    "Select-Object ProcessId,ParentProcessId,Name,ExecutablePath | "
    "ConvertTo-Json -Compress"
)

HELPER = r'''
import ctypes, json, os, subprocess, sys, time

job = json.load(open(sys.argv[1], encoding="utf-8"))
log = open(job["log"], "a", encoding="utf-8")

def say(text):
    for out in (sys.stdout, log):
        try:
            out.write(text + "\n")
            out.flush()
        except Exception:
            pass

def wait_for(pid, seconds):
    k32 = ctypes.windll.kernel32
    h = k32.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE
    if h:
        k32.WaitForSingleObject(h, int(seconds * 1000))
        k32.CloseHandle(h)

def schtasks(*args):
    return subprocess.run(["schtasks", *args, "/TN", job["task"]],
                          capture_output=True, text=True).returncode

for pid in job["pids"]:
    wait_for(pid, 60)
time.sleep(1)
say("")
if job["restart"]:
    schtasks("/End")
    time.sleep(3)
    say("Stopped the Copycord service for the update.")
say("$ " + job["hint"])
proc = subprocess.Popen(job["cmd"], env=job["env"], stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT, text=True, errors="replace")
for line in proc.stdout:
    say(line.rstrip("\r\n"))
rc = proc.wait()
if rc == 0:
    say("Copycord was updated: " + job["spec"])
    if job["channel"]:
        try:
            with open(job["channel_path"], "w", encoding="utf-8") as f:
                f.write(job["channel"])
            say("Update channel set to " + job["channel_label"] + ".")
        except OSError as e:
            say("Could not save the update channel: " + str(e))
else:
    say("The update failed (exit %d). Reinstall Copycord with: %s" % (rc, job["hint"]))
if job["restart"]:
    if schtasks("/Run") == 0:
        say("Restarted the Copycord service.")
    else:
        say("Could not restart the Copycord service; start it with: copycord service start")
say("Log: " + job["log"])
log.close()
try:
    os.remove(sys.argv[1])
except OSError:
    pass
sys.exit(rc)
'''


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
        return os.path.normcase(os.path.abspath(path)).startswith(
            os.path.normcase(os.path.abspath(root)) + os.sep
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

    for r in rows:
        pid = r.get("ProcessId")
        if pid in seen or not is_copycord(r):
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


def base_python() -> Optional[str]:
    """A python.exe outside the tool environment, or None."""
    exe = getattr(sys, "_base_executable", None) or sys.executable
    if not exe or _under(exe, sys.prefix) or not Path(exe).is_file():
        return None
    return exe


def log_path() -> Path:
    from copycord.common.paths import data_dir

    return data_dir() / "logs" / "update.log"


def handoff(
    pids: List[int],
    cmd: Sequence[str],
    hint: str,
    spec: str,
    env: Optional[Mapping[str, str]],
    restart: bool,
    channel_json: Optional[str] = None,
    channel_path: Optional[Path] = None,
    channel_label: str = "",
) -> int:
    """Start the helper and return; the install runs once this process exits."""
    from copycord import ui

    py = base_python()
    if py is None:
        ui.fail("could not find the Python that runs the update outside Copycord",
                f"close Copycord and run: {hint}")
        return 1
    log = log_path()
    log.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="copycord-update-"))
    script = work / "copycord_update.py"
    script.write_text(HELPER, encoding="utf-8")
    job = work / "job.json"
    job.write_text(json.dumps({
        "pids": pids or [os.getpid()],
        "cmd": list(cmd),
        "hint": hint,
        "spec": spec,
        "env": dict(os.environ if env is None else env),
        "restart": restart,
        "task": service.TASK_NAME,
        "log": str(log),
        "channel": channel_json,
        "channel_path": str(channel_path) if channel_path else None,
        "channel_label": channel_label,
    }), encoding="utf-8")
    flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    try:
        subprocess.Popen([py, "-I", str(script), str(job)], cwd=str(work),
                         creationflags=flags, close_fds=True)
    except OSError as e:
        ui.fail("could not start the update", str(e))
        return 1
    ui.info("the update continues in this window once this command exits",
            f"log: {log}")
    return 0


def running():
    """(pids of this command, pid of a Copycord running outside the service or None)."""
    rows = _processes()
    if rows is None:
        return [os.getpid()], None
    return scan(rows, os.getpid(), sys.prefix)
