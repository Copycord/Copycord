# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Registers `copycord run --data-dir PATH` with the host's service manager: a
systemd unit on Linux, a launchd agent on macOS, a logon Scheduled Task on
Windows.
"""

from __future__ import annotations

import ntpath
import os
import plistlib
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Mapping, NamedTuple, Optional, Sequence, Tuple
from xml.sax.saxutils import escape, unescape

SERVICE_NAME = "copycord"
UNIT_NAME = "copycord.service"
LAUNCHD_LABEL = "com.copycord"
TASK_NAME = "Copycord"

SYSTEMD_SYSTEM_DIR = Path("/etc/systemd/system")

# Windows: a captured command needs no console window of its own; without it a
# windowless Copycord (the service) flashes one for every command.
NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def run_cmd(
    cmd: Sequence[str], capture: bool = True, env: Optional[Mapping[str, str]] = None
) -> subprocess.CompletedProcess:
    """Run an external command; tests replace this."""
    try:
        return subprocess.run(
            list(cmd), capture_output=capture, text=True, check=False,
            env=None if env is None else dict(env),
            creationflags=NO_WINDOW if capture else 0,
        )
    except OSError as e:
        return subprocess.CompletedProcess(list(cmd), 127, "", str(e))


def is_root() -> bool:
    geteuid = getattr(os, "geteuid", None)
    return geteuid is not None and geteuid() == 0


def copycord_command() -> List[str]:
    """Absolute command that starts the installed `copycord` console script."""
    argv0 = Path(sys.argv[0]) if sys.argv and sys.argv[0] else None
    if argv0 is not None and argv0.stem.lower() == "copycord":
        # On Windows the entry point drops ".exe" from argv[0].
        for exe in (argv0, argv0.with_name(argv0.name + ".exe")):
            if exe.is_file():
                return [str(exe.resolve())]
    found = shutil.which("copycord")
    if found:
        return [str(Path(found).resolve())]
    return [str(Path(sys.executable).resolve()), "-m", "copycord"]


class State(NamedTuple):
    installed: bool
    running: bool
    where: str

    def line(self) -> str:
        if not self.installed:
            return f"copycord service: not installed ({self.where})"
        run = "running" if self.running else "not running"
        return f"copycord service: installed, {run} ({self.where})"


def _fail(result: subprocess.CompletedProcess, what: str) -> str:
    detail = (result.stderr or result.stdout or "").strip()
    cmd = " ".join(result.args) if isinstance(result.args, list) else str(result.args)
    return f"{what} failed ({cmd}, exit {result.returncode}){': ' + detail if detail else ''}"


class ServiceError(Exception):
    pass


def _check(result: subprocess.CompletedProcess, what: str) -> None:
    if result.returncode != 0:
        raise ServiceError(_fail(result, what))


# --------------------------------------------------------------------------
# Linux: systemd
# --------------------------------------------------------------------------


def _systemd_quote(arg: str) -> str:
    if arg and not any(c in arg for c in ' \t"\\\'$%'):
        return arg
    escaped = arg.replace("\\", "\\\\").replace('"', '\\"')
    escaped = escaped.replace("$", "$$").replace("%", "%%")
    return f'"{escaped}"'


def systemd_unit(
    command: Sequence[str], data_dir: Path, user: bool, run_as: Optional[str] = None
) -> str:
    exec_start = " ".join(
        _systemd_quote(a) for a in [*command, "run", "--data-dir", str(data_dir)]
    )
    lines = [
        "[Unit]",
        "Description=Copycord (Discord server mirroring)",
        "Wants=network-online.target",
        "After=network-online.target",
        "",
        "[Service]",
        "Type=simple",
        f"ExecStart={exec_start}",
        "Restart=on-failure",
        "RestartSec=10",
        "Environment=PYTHONUNBUFFERED=1",
    ]
    if run_as and not user:
        lines.append(f"User={run_as}")
    lines += [
        "",
        "[Install]",
        f"WantedBy={'default.target' if user else 'multi-user.target'}",
        "",
    ]
    return "\n".join(lines)


def login_name() -> str:
    return os.environ.get("USER") or os.environ.get("LOGNAME") or "$USER"


def linger_enabled(name: Optional[str] = None) -> Optional[bool]:
    """Whether systemd starts this user's services at boot; None without loginctl."""
    result = run_cmd(["loginctl", "show-user", name or login_name(), "-p", "Linger"])
    if result.returncode == 127:
        return None
    if result.returncode != 0:
        # loginctl fails for a user with no session and linger off.
        return False
    return (result.stdout or "").strip().lower() == "linger=yes"


def enable_linger(name: Optional[str] = None) -> bool:
    """Turn linger on without sudo; polkit allows it for your own account on most systems."""
    return run_cmd(["loginctl", "enable-linger", name or login_name()]).returncode == 0


def ensure_linger(name: Optional[str] = None) -> bool:
    """Linger on (already, or turned on now): a user service starts at boot."""
    return bool(linger_enabled(name)) or enable_linger(name)


def linger_hint(name: Optional[str] = None) -> str:
    return (
        "to start Copycord at boot and keep it running after you log out, run: "
        f"sudo loginctl enable-linger {name or login_name()}"
    )


class SystemdService:
    def __init__(self, user: bool, home: Optional[Path] = None):
        self.user = user
        self.home = Path.home() if home is None else Path(home)

    @property
    def unit_path(self) -> Path:
        if self.user:
            return self.home / ".config" / "systemd" / "user" / UNIT_NAME
        return SYSTEMD_SYSTEM_DIR / UNIT_NAME

    @property
    def where(self) -> str:
        scope = "systemd user unit" if self.user else "systemd system unit"
        return f"{scope} {self.unit_path}"

    def _systemctl(self, *args: str) -> List[str]:
        return ["systemctl", "--user", *args] if self.user else ["systemctl", *args]

    @property
    def scope(self) -> str:
        return "user" if self.user else "system"

    def _need_root(self) -> None:
        if not self.user and not is_root():
            raise ServiceError(
                f"the system service ({self.unit_path}) needs root. Leave out --system "
                "to use a service for your own account, which needs no sudo"
            )

    def install(self, command: Sequence[str], data_dir: Path) -> List[str]:
        self._need_root()
        run_as = None
        if not self.user:
            sudo_user = os.environ.get("SUDO_USER")
            if sudo_user and sudo_user != "root":
                run_as = sudo_user
        self.unit_path.parent.mkdir(parents=True, exist_ok=True)
        self.unit_path.write_text(
            systemd_unit(command, data_dir, self.user, run_as), encoding="utf-8"
        )
        _check(run_cmd(self._systemctl("daemon-reload")), "daemon-reload")
        _check(run_cmd(self._systemctl("enable", "--now", UNIT_NAME)), "enable")
        at_boot = not self.user or ensure_linger()
        when = "starts at boot" if at_boot else "starts when you log in"
        notes = [
            f"created {self.scope} service {UNIT_NAME} ({when})",
            f"unit file: {self.unit_path}",
            f"enabled and started {UNIT_NAME}",
        ]
        if not at_boot:
            notes.append(f"needs your attention: {linger_hint()}")
        return notes

    def start(self) -> None:
        self._need_root()
        _check(run_cmd(self._systemctl("start", UNIT_NAME)), "start")

    def stop(self) -> None:
        self._need_root()
        _check(run_cmd(self._systemctl("stop", UNIT_NAME)), "stop")

    def restart(self) -> None:
        self._need_root()
        _check(run_cmd(self._systemctl("restart", UNIT_NAME)), "restart")

    def state(self) -> State:
        installed = self.unit_path.exists()
        running = False
        if installed:
            running = run_cmd(self._systemctl("is-active", "--quiet", UNIT_NAME)).returncode == 0
        return State(installed, running, self.where)

    def uninstall(self) -> List[str]:
        self._need_root()
        if not self.unit_path.exists():
            return [f"nothing to remove ({self.unit_path} does not exist)"]
        run_cmd(self._systemctl("disable", "--now", UNIT_NAME))
        self.unit_path.unlink()
        run_cmd(self._systemctl("daemon-reload"))
        return [f"stopped, disabled and removed {self.unit_path}"]


# --------------------------------------------------------------------------
# macOS: launchd
# --------------------------------------------------------------------------


def launchd_plist(command: Sequence[str], data_dir: Path) -> bytes:
    return plistlib.dumps(
        {
            "Label": LAUNCHD_LABEL,
            "ProgramArguments": [*command, "run", "--data-dir", str(data_dir)],
            "RunAtLoad": True,
            "KeepAlive": {"SuccessfulExit": False},
            "ThrottleInterval": 10,
            "StandardOutPath": str(data_dir / "copycord.out.log"),
            "StandardErrorPath": str(data_dir / "copycord.err.log"),
            "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
        }
    )


class LaunchdService:
    def __init__(self, home: Optional[Path] = None, uid: Optional[int] = None):
        self.home = Path.home() if home is None else Path(home)
        self.uid = os.getuid() if uid is None else uid

    @property
    def plist_path(self) -> Path:
        return self.home / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"

    @property
    def domain(self) -> str:
        return f"gui/{self.uid}"

    @property
    def target(self) -> str:
        return f"{self.domain}/{LAUNCHD_LABEL}"

    @property
    def where(self) -> str:
        return f"launchd agent {self.plist_path}"

    def _loaded(self) -> subprocess.CompletedProcess:
        return run_cmd(["launchctl", "print", self.target])

    def install(self, command: Sequence[str], data_dir: Path) -> List[str]:
        data_dir.mkdir(parents=True, exist_ok=True)
        self.plist_path.parent.mkdir(parents=True, exist_ok=True)
        if self._loaded().returncode == 0:
            run_cmd(["launchctl", "bootout", self.target])
        self.plist_path.write_bytes(launchd_plist(command, data_dir))
        _check(
            run_cmd(["launchctl", "bootstrap", self.domain, str(self.plist_path)]),
            "launchctl bootstrap",
        )
        return [
            f"wrote {self.plist_path}",
            "loaded and started com.copycord (it starts again at every login)",
            f"output goes to {data_dir / 'copycord.out.log'} and copycord.err.log",
        ]

    def _need_plist(self) -> None:
        if not self.plist_path.exists():
            raise ServiceError(
                f"not installed ({self.plist_path} is missing); "
                "run copycord service install first"
            )

    def start(self) -> None:
        self._need_plist()
        if self._loaded().returncode == 0:
            _check(run_cmd(["launchctl", "kickstart", self.target]), "launchctl kickstart")
        else:
            _check(
                run_cmd(["launchctl", "bootstrap", self.domain, str(self.plist_path)]),
                "launchctl bootstrap",
            )

    def stop(self) -> None:
        if self._loaded().returncode == 0:
            _check(run_cmd(["launchctl", "bootout", self.target]), "launchctl bootout")

    def restart(self) -> None:
        self._need_plist()
        if self._loaded().returncode == 0:
            _check(
                run_cmd(["launchctl", "kickstart", "-k", self.target]),
                "launchctl kickstart",
            )
        else:
            self.start()

    def state(self) -> State:
        installed = self.plist_path.exists()
        running = False
        if installed:
            res = self._loaded()
            running = res.returncode == 0 and "state = running" in (res.stdout or "")
        return State(installed, running, self.where)

    def uninstall(self) -> List[str]:
        if self._loaded().returncode == 0:
            run_cmd(["launchctl", "bootout", self.target])
        if not self.plist_path.exists():
            return [f"nothing to remove ({self.plist_path} does not exist)"]
        self.plist_path.unlink()
        return [f"unloaded and removed {self.plist_path}"]


# --------------------------------------------------------------------------
# Windows: Scheduled Task
# --------------------------------------------------------------------------


def _windows_quote(arg: str) -> str:
    return f'"{arg}"' if (" " in arg or "\t" in arg or not arg) else arg


# Programs that open a console window when the service starts them.
_CONSOLE_PROGRAMS = {"copycord.exe", "python.exe"}


def windowless_command(command: Sequence[str]) -> List[str]:
    """The same command without a console window: copycordw.exe next to the
    copycord.exe launcher, else pythonw.exe of this Python environment."""
    command = list(command)
    exe = Path(command[0])
    name = exe.name.lower()
    if name == "python.exe":
        gui = exe.with_name("pythonw.exe")
        return [str(gui), *command[1:]] if gui.is_file() else command
    if name != "copycord.exe":
        return command
    gui = exe.with_name("copycordw.exe")
    if gui.is_file():
        return [str(gui), *command[1:]]
    pythonw = Path(sys.executable).with_name("pythonw.exe")
    if pythonw.is_file():
        return [str(pythonw), "-m", "copycord", *command[1:]]
    return command


def opens_window(command: str) -> bool:
    """Whether the task's program is a console program, which shows a window."""
    return ntpath.basename(command.strip().strip('"')).lower() in _CONSOLE_PROGRAMS


def task_data_dir(arguments: str) -> Optional[Path]:
    """The --data-dir in the task's arguments."""
    m = re.search(r'--data-dir\s+(?:"([^"]*)"|(\S+))', arguments or "")
    return Path(m.group(1) or m.group(2)) if m else None


def windows_user(env=None) -> str:
    env = os.environ if env is None else env
    user = env.get("USERNAME") or "%USERNAME%"
    domain = env.get("USERDOMAIN")
    return f"{domain}\\{user}" if domain else user


def task_xml(command: Sequence[str], data_dir: Path, user: str) -> str:
    exe, *rest = list(command)
    args = " ".join(
        _windows_quote(a) for a in [*rest, "run", "--data-dir", str(data_dir)]
    )
    u = escape(user)
    return f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Copycord (Discord server mirroring)</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>{u}</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{u}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{escape(exe)}</Command>
      <Arguments>{escape(args)}</Arguments>
    </Exec>
  </Actions>
</Task>
"""


def list_processes() -> Optional[list]:
    """Windows processes as rows (ProcessId, ParentProcessId, Name,
    ExecutablePath), or None when they cannot be listed; tests replace this."""
    from copycord import winupdate

    return winupdate._processes()


def busy_ports(env=None) -> List[int]:
    """The ports `copycord run` binds that something still holds."""
    env = os.environ if env is None else env
    wanted = [
        ("127.0.0.1", env.get("CONTROL_PORT_SERVER") or "9101"),
        ("127.0.0.1", env.get("CONTROL_PORT_CLIENT") or "9102"),
        ("0.0.0.0", env.get("ADMIN_PORT") or "8080"),
    ]
    busy = []
    for host, raw in wanted:
        try:
            port = int(raw)
        except ValueError:
            continue
        if not port_free(host, port):
            busy.append(port)
    return busy


def port_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Like uvicorn, ignore TIME_WAIT sockets; on Windows SO_REUSEADDR would share a live port.
        if sys.platform != "win32":
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
        except OSError:
            return False
    return True


# How long stop and restart wait for the old Copycord to exit and free its ports.
STOP_WAIT = 20.0
_clock = time.monotonic
_sleep = time.sleep


class ScheduledTaskService:
    def __init__(self, env=None):
        self.env = os.environ if env is None else env
        self.name = self.env.get("COPYCORD_TASK_NAME") or TASK_NAME

    @property
    def where(self) -> str:
        return f'scheduled task "{self.name}"'

    def _query(self) -> subprocess.CompletedProcess:
        return run_cmd(["schtasks", "/Query", "/TN", self.name, "/FO", "CSV", "/NH"])

    def task_action(self) -> Optional[Tuple[str, str]]:
        """(Command, Arguments) of the installed task, or None."""
        res = run_cmd(["schtasks", "/Query", "/TN", self.name, "/XML"])
        if res.returncode != 0:
            return None
        text = res.stdout or ""
        command = re.search(r"<Command>(.*?)</Command>", text, re.S)
        arguments = re.search(r"<Arguments>(.*?)</Arguments>", text, re.S)
        if command is None:
            return None
        return (unescape(command.group(1)).strip(),
                unescape(arguments.group(1)).strip() if arguments else "")

    def console_command(self) -> Optional[str]:
        """The task's program when it opens a console window, else None."""
        action = self.task_action()
        return action[0] if action and opens_window(action[0]) else None

    def use_windowless(self) -> List[str]:
        """Write the task again so it starts Copycord without a window,
        keeping its data dir, and start it."""
        command = windowless_command(copycord_command())
        if opens_window(command[0]):
            raise ServiceError(
                "this Copycord has no copycordw.exe or pythonw.exe to run without a "
                "window; update Copycord, then run copycord service install"
            )
        action = self.task_action()
        data = task_data_dir(action[1]) if action else None
        return self.install(command, data or install_data_dir(False))

    def install(self, command: Sequence[str], data_dir: Path) -> List[str]:
        command = windowless_command(command)
        xml = task_xml(command, data_dir, windows_user(self.env))
        # A running copy keeps its old command (and its window) until it ends.
        if self.state().running:
            run_cmd(["schtasks", "/End", "/TN", self.name])
            self._end(ports=True)
        fd, tmp = tempfile.mkstemp(suffix=".xml", prefix="copycord-task-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(xml.encode("utf-16"))
            _check(
                run_cmd(["schtasks", "/Create", "/TN", self.name, "/XML", tmp, "/F"]),
                "schtasks /Create",
            )
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        _check(run_cmd(["schtasks", "/Run", "/TN", self.name]), "schtasks /Run")
        return [
            f'created scheduled task "{self.name}" (runs at logon, restarts on failure)',
            "started it now",
        ]

    def start(self) -> None:
        _check(run_cmd(["schtasks", "/Run", "/TN", self.name]), "schtasks /Run")

    def stop(self) -> None:
        _check(run_cmd(["schtasks", "/End", "/TN", self.name]), "schtasks /End")
        self._end(ports=False)

    def restart(self) -> None:
        run_cmd(["schtasks", "/End", "/TN", self.name])
        self._end(ports=True)
        self.start()

    def _leftovers(self) -> List[int]:
        from copycord import winupdate

        rows = list_processes()
        if not rows:
            return []
        exe = Path(copycord_command()[0])
        launcher_dir = str(exe.parent) if exe.stem.lower() == "copycord" else None
        return winupdate.service_leftovers(rows, os.getpid(), sys.prefix, launcher_dir)

    def _end(self, ports: bool) -> None:
        """After schtasks /End: end what is left of the processes the task
        started, then wait until they are gone and, with ports, until the
        ports Copycord binds are free. /End may end only the first process,
        and a port stays taken for a moment after its process exits."""
        deadline = _clock() + STOP_WAIT
        while True:
            left = self._leftovers()
            for pid in left:
                run_cmd(["taskkill", "/F", "/T", "/PID", str(pid)])
            running = self.state().running
            busy = busy_ports(self.env) if ports else []
            if not (left or running or busy):
                return
            if _clock() >= deadline:
                break
            _sleep(0.5)
        if busy:
            what = f"port {busy[0]} is still in use" if len(busy) == 1 else (
                "ports " + ", ".join(str(p) for p in busy) + " are still in use")
        elif left:
            what = "Copycord is still running (process " + ", ".join(str(p) for p in left) + ")"
        else:
            what = f"the {self.where} is still running"
        raise ServiceError(
            f"Copycord did not stop within {STOP_WAIT:.0f} seconds: {what}. "
            "Close any other Copycord, or run copycord doctor to check the port, "
            "then run copycord service start"
        )

    def state(self) -> State:
        res = self._query()
        if res.returncode != 0:
            return State(False, False, self.where)
        # CSV row: "TaskName","Next Run Time","Status"
        running = any(
            line.strip().rstrip('"').endswith("Running")
            for line in (res.stdout or "").splitlines()
        )
        return State(True, running, self.where)

    def uninstall(self) -> List[str]:
        if self._query().returncode != 0:
            return [f'nothing to remove (no scheduled task "{self.name}")']
        run_cmd(["schtasks", "/End", "/TN", self.name])
        try:
            self._end(ports=False)
        except ServiceError:
            pass
        _check(
            run_cmd(["schtasks", "/Delete", "/TN", self.name, "/F"]), "schtasks /Delete"
        )
        return [f'stopped and deleted scheduled task "{self.name}"']


# --------------------------------------------------------------------------
# entry points
# --------------------------------------------------------------------------


def get_service(
    user: bool = False,
    platform: Optional[str] = None,
    home: Optional[Path] = None,
    system: bool = False,
):
    """The service manager for this OS.

    Linux: --user or --system picks the unit; otherwise the one that is
    installed, and with none or both, the user unit unless running as root.
    """
    platform = sys.platform if platform is None else platform
    if platform == "win32":
        return ScheduledTaskService()
    if platform == "darwin":
        return LaunchdService(home=home)
    if user or system:
        return SystemdService(not system, home)
    user_svc = SystemdService(True, home)
    system_svc = SystemdService(False, home)
    has_user, has_system = user_svc.unit_path.exists(), system_svc.unit_path.exists()
    if has_user != has_system:
        return user_svc if has_user else system_svc
    return system_svc if is_root() else user_svc


def both_installed(home: Optional[Path] = None, platform: Optional[str] = None) -> bool:
    """Linux: a user unit and a system unit both exist."""
    platform = sys.platform if platform is None else platform
    if platform in ("win32", "darwin"):
        return False
    return (
        SystemdService(True, home).unit_path.exists()
        and SystemdService(False, home).unit_path.exists()
    )


def user_linger(platform: Optional[str] = None) -> Optional[bool]:
    """Linux user unit: whether linger is on; None for any other service."""
    platform = sys.platform if platform is None else platform
    svc = get_service(platform=platform)
    if not isinstance(svc, SystemdService) or not svc.user or not svc.unit_path.exists():
        return None
    return linger_enabled()


def service_state() -> State:
    return get_service().state()


def console_task() -> Optional[str]:
    """Windows: the service task's program when it opens a console window."""
    if sys.platform != "win32":
        return None
    return ScheduledTaskService().console_command()


def restart_service() -> None:
    get_service().restart()


def install_data_dir(user: bool, env=None, platform: Optional[str] = None) -> Path:
    """The data dir written into the service; under sudo, SUDO_USER's rather than root's."""
    from copycord.common.paths import data_dir, platform_data_dir

    env = os.environ if env is None else env
    platform = sys.platform if platform is None else platform
    sudo_user = env.get("SUDO_USER")
    linux_system = platform not in ("win32", "darwin") and not user
    if linux_system and not env.get("DATA_DIR") and sudo_user and sudo_user != "root":
        home = Path(os.path.expanduser(f"~{sudo_user}"))
        return platform_data_dir(platform="linux", env=env, home=home).resolve()
    return data_dir(env)


def service_command(args) -> int:
    action = args.service_command
    user = bool(getattr(args, "user", False))
    system = bool(getattr(args, "system", False))
    if action == "install" and sys.platform not in ("win32", "darwin"):
        svc = SystemdService(user or not (system or is_root()))
    else:
        svc = get_service(user, system=system)
    if action == "install":
        target = install_data_dir(getattr(svc, "user", user))
    elif not (user or system) and both_installed():
        print(
            f"a user service and a system service are both installed; using the "
            f"{svc.scope} service ({svc.unit_path}). Add --user or --system to choose.",
            file=sys.stderr,
        )
    try:
        if action == "install":
            for note in svc.install(copycord_command(), target):
                print(note)
            print(f"data dir: {target}")
            return 0
        if action == "start":
            svc.start()
            print(svc.state().line())
            return 0
        if action == "stop":
            svc.stop()
            print(svc.state().line())
            return 0
        if action == "status":
            st = svc.state()
            print(st.line())
            return 0 if st.running else 3
        if action == "uninstall":
            for note in svc.uninstall():
                print(note)
            return 0
    except ServiceError as e:
        print(f"copycord service {action}: {e}", file=sys.stderr)
        return 1
    return 2
