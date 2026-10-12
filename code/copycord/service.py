# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
copycord service install [--user] [--data-dir PATH]
copycord service start|stop|status|uninstall [--user]

Registers `copycord run --data-dir PATH` with the host's service manager:
  Linux    systemd unit (~/.config/systemd/user with --user, else /etc/systemd/system)
  macOS    launchd agent ~/Library/LaunchAgents/com.copycord.plist
  Windows  Scheduled Task "Copycord" at logon of the current user

Every external command goes through run_cmd() so tests can fake it.
"""

from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, NamedTuple, Optional, Sequence
from xml.sax.saxutils import escape

SERVICE_NAME = "copycord"
UNIT_NAME = "copycord.service"
LAUNCHD_LABEL = "com.copycord"
TASK_NAME = "Copycord"

SYSTEMD_SYSTEM_DIR = Path("/etc/systemd/system")


def run_cmd(cmd: Sequence[str], capture: bool = True) -> subprocess.CompletedProcess:
    """The one place Copycord runs an external command (systemctl, launchctl, ...)."""
    try:
        return subprocess.run(
            list(cmd), capture_output=capture, text=True, check=False
        )
    except OSError as e:
        return subprocess.CompletedProcess(list(cmd), 127, "", str(e))


def is_root() -> bool:
    geteuid = getattr(os, "geteuid", None)
    return geteuid is not None and geteuid() == 0


def copycord_command() -> List[str]:
    """Absolute command that starts the installed `copycord` console script."""
    argv0 = Path(sys.argv[0]) if sys.argv and sys.argv[0] else None
    if argv0 is not None and argv0.stem.lower() == "copycord" and argv0.is_file():
        return [str(argv0.resolve())]
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

    def _need_root(self) -> None:
        if not self.user and not is_root():
            raise ServiceError(
                f"the system service ({self.unit_path}) needs root: rerun with sudo, "
                "or use `copycord service install --user` for a per-user service"
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
        notes = [f"wrote {self.unit_path}", "enabled and started copycord.service"]
        if self.user:
            name = os.environ.get("USER") or os.environ.get("LOGNAME") or "$USER"
            notes.append(
                "to keep it running after you log out (and start it at boot), run: "
                f"sudo loginctl enable-linger {name}"
            )
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
            # Restart when it exits with an error (like systemd Restart=on-failure).
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
                "run `copycord service install` first"
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


class ScheduledTaskService:
    def __init__(self, env=None):
        self.env = os.environ if env is None else env

    @property
    def where(self) -> str:
        return f'scheduled task "{TASK_NAME}"'

    def _query(self) -> subprocess.CompletedProcess:
        return run_cmd(["schtasks", "/Query", "/TN", TASK_NAME, "/FO", "CSV", "/NH"])

    def install(self, command: Sequence[str], data_dir: Path) -> List[str]:
        xml = task_xml(command, data_dir, windows_user(self.env))
        fd, tmp = tempfile.mkstemp(suffix=".xml", prefix="copycord-task-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(xml.encode("utf-16"))
            _check(
                run_cmd(["schtasks", "/Create", "/TN", TASK_NAME, "/XML", tmp, "/F"]),
                "schtasks /Create",
            )
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        _check(run_cmd(["schtasks", "/Run", "/TN", TASK_NAME]), "schtasks /Run")
        return [
            f'created scheduled task "{TASK_NAME}" (runs at logon, restarts on failure)',
            "started it now",
        ]

    def start(self) -> None:
        _check(run_cmd(["schtasks", "/Run", "/TN", TASK_NAME]), "schtasks /Run")

    def stop(self) -> None:
        _check(run_cmd(["schtasks", "/End", "/TN", TASK_NAME]), "schtasks /End")

    def restart(self) -> None:
        run_cmd(["schtasks", "/End", "/TN", TASK_NAME])
        self.start()

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
            return [f'nothing to remove (no scheduled task "{TASK_NAME}")']
        run_cmd(["schtasks", "/End", "/TN", TASK_NAME])
        _check(
            run_cmd(["schtasks", "/Delete", "/TN", TASK_NAME, "/F"]), "schtasks /Delete"
        )
        return [f'stopped and deleted scheduled task "{TASK_NAME}"']


# --------------------------------------------------------------------------
# entry points
# --------------------------------------------------------------------------


def get_service(
    user: bool = False, platform: Optional[str] = None, home: Optional[Path] = None
):
    """The service manager for this OS. On Linux without --user, an existing
    user unit is used when there is no system unit (start/stop/status)."""
    platform = sys.platform if platform is None else platform
    if platform == "win32":
        return ScheduledTaskService()
    if platform == "darwin":
        return LaunchdService(home=home)
    if not user:
        user_svc = SystemdService(True, home)
        system_svc = SystemdService(False, home)
        if user_svc.unit_path.exists() and not system_svc.unit_path.exists():
            return user_svc
        return system_svc
    return SystemdService(True, home)


def service_state() -> State:
    return get_service().state()


def restart_service() -> None:
    get_service().restart()


def install_data_dir(user: bool, env=None, platform: Optional[str] = None) -> Path:
    """Data dir baked into the service: DATA_DIR / --data-dir, else the default.
    A system unit installed with sudo runs as SUDO_USER, so it defaults to
    that user's data dir rather than root's."""
    from common.paths import data_dir, platform_data_dir

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
    if action == "install":
        target = install_data_dir(user)
        svc = (
            get_service(user)
            if sys.platform in ("win32", "darwin")
            else SystemdService(user)
        )
    else:
        svc = get_service(user)
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
