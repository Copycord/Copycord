"""Tests for `copycord service`: unit, plist and task XML and the commands per OS."""
import plistlib
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from copycord import __main__ as cli
from copycord import service


class FakeRun:
    """Records commands; answers with (returncode, stdout) by command prefix."""

    def __init__(self, answers=None):
        self.calls = []
        self.answers = answers or {}

    def __call__(self, cmd, capture=True):
        cmd = list(cmd)
        self.calls.append(cmd)
        rc, out = 0, ""
        for prefix, answer in self.answers.items():
            if tuple(cmd[: len(prefix)]) == prefix:
                rc, out = answer
        return subprocess.CompletedProcess(cmd, rc, out, "")


@pytest.fixture()
def fake_run(monkeypatch):
    fake = FakeRun()
    monkeypatch.setattr(service, "run_cmd", fake)
    return fake


@pytest.fixture()
def system_dir(tmp_path, monkeypatch):
    d = tmp_path / "etc-systemd-system"
    monkeypatch.setattr(service, "SYSTEMD_SYSTEM_DIR", d)
    return d


EXE = ["/home/u/.local/bin/copycord"]


# --------------------------------------------------------------------------
# Linux: systemd
# --------------------------------------------------------------------------


def test_systemd_unit_runs_absolute_executable_with_data_dir():
    unit = service.systemd_unit(EXE, PurePosixPath("/srv/copy cord"), user=True)
    assert (
        'ExecStart=/home/u/.local/bin/copycord run --data-dir "/srv/copy cord"' in unit
    )
    assert "Restart=on-failure" in unit
    assert "WantedBy=default.target" in unit
    assert "User=" not in unit


def test_systemd_system_unit_targets_multi_user_and_runs_as_sudo_user():
    unit = service.systemd_unit(EXE, PurePosixPath("/d"), user=False, run_as="mac")
    assert "WantedBy=multi-user.target" in unit
    assert "User=mac" in unit


def test_systemd_quote_escapes_specifiers():
    assert service._systemd_quote("/a/100%") == '"/a/100%%"'
    assert service._systemd_quote("/plain") == "/plain"


def test_user_install_writes_unit_and_enables(tmp_path, fake_run, system_dir, monkeypatch, capsys):
    monkeypatch.setenv("USER", "mac")
    svc = service.SystemdService(True, home=tmp_path)
    notes = svc.install(EXE, tmp_path / "data")
    unit = tmp_path / ".config" / "systemd" / "user" / "copycord.service"
    assert unit.is_file()
    assert "run --data-dir" in unit.read_text()
    assert fake_run.calls == [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "enable", "--now", "copycord.service"],
    ]
    assert any("loginctl enable-linger mac" in n for n in notes)
    assert not system_dir.exists()


def test_system_install_needs_root(tmp_path, fake_run, system_dir, monkeypatch):
    monkeypatch.setattr(service, "is_root", lambda: False)
    svc = service.SystemdService(False, home=tmp_path)
    with pytest.raises(service.ServiceError, match="needs root"):
        svc.install(EXE, tmp_path / "data")
    assert fake_run.calls == []
    assert not (system_dir / "copycord.service").exists()


def test_system_install_as_root(tmp_path, fake_run, system_dir, monkeypatch):
    monkeypatch.setattr(service, "is_root", lambda: True)
    monkeypatch.setenv("SUDO_USER", "mac")
    svc = service.SystemdService(False, home=tmp_path)
    notes = svc.install(EXE, tmp_path / "data")
    unit = (system_dir / "copycord.service").read_text()
    assert "User=mac" in unit
    assert fake_run.calls == [
        ["systemctl", "daemon-reload"],
        ["systemctl", "enable", "--now", "copycord.service"],
    ]
    assert not any("enable-linger" in n for n in notes)


def test_systemd_install_reports_systemctl_failure(tmp_path, monkeypatch, system_dir):
    fake = FakeRun({("systemctl", "--user", "enable"): (1, "")})
    monkeypatch.setattr(service, "run_cmd", fake)
    with pytest.raises(service.ServiceError, match="enable failed"):
        service.SystemdService(True, home=tmp_path).install(EXE, tmp_path / "d")


def test_systemd_state_start_stop_uninstall(tmp_path, system_dir, monkeypatch):
    svc = service.SystemdService(True, home=tmp_path)
    fake = FakeRun()
    monkeypatch.setattr(service, "run_cmd", fake)
    assert svc.state() == service.State(False, False, svc.where)
    assert fake.calls == []

    svc.install(EXE, tmp_path / "d")
    fake.calls.clear()
    assert svc.state().running
    assert fake.calls == [["systemctl", "--user", "is-active", "--quiet", "copycord.service"]]

    fake.answers[("systemctl", "--user", "is-active")] = (3, "")
    st = svc.state()
    assert st.installed and not st.running
    assert "installed, not running" in st.line()

    fake.calls.clear()
    svc.start()
    svc.stop()
    svc.restart()
    assert fake.calls == [
        ["systemctl", "--user", "start", "copycord.service"],
        ["systemctl", "--user", "stop", "copycord.service"],
        ["systemctl", "--user", "restart", "copycord.service"],
    ]

    fake.calls.clear()
    svc.uninstall()
    assert not svc.unit_path.exists()
    assert fake.calls == [
        ["systemctl", "--user", "disable", "--now", "copycord.service"],
        ["systemctl", "--user", "daemon-reload"],
    ]


def test_get_service_prefers_existing_user_unit(tmp_path, system_dir):
    assert service.get_service(platform="linux", home=tmp_path).user is False
    user_unit = tmp_path / ".config" / "systemd" / "user" / "copycord.service"
    user_unit.parent.mkdir(parents=True)
    user_unit.write_text("x")
    assert service.get_service(platform="linux", home=tmp_path).user is True
    system_dir.mkdir()
    (system_dir / "copycord.service").write_text("x")
    assert service.get_service(platform="linux", home=tmp_path).user is False
    assert service.get_service(True, platform="linux", home=tmp_path).user is True


def test_install_data_dir_uses_sudo_users_home(monkeypatch, tmp_path):
    real = service.os.path.expanduser
    monkeypatch.setattr(
        service.os.path,
        "expanduser",
        lambda p: str(tmp_path / "home-mac") if str(p) == "~mac" else real(p),
    )
    env = {"SUDO_USER": "mac"}
    got = service.install_data_dir(False, env=env, platform="linux")
    assert got == (tmp_path / "home-mac" / ".copycord" / "data").resolve()
    env = {"SUDO_USER": "mac", "COPYCORD_HOME": str(tmp_path / "ch")}
    assert service.install_data_dir(False, env=env, platform="linux") == (
        tmp_path / "ch" / "data"
    ).resolve()
    env = {"SUDO_USER": "mac", "DATA_DIR": str(tmp_path / "explicit")}
    assert service.install_data_dir(False, env=env, platform="linux") == (
        tmp_path / "explicit"
    ).resolve()


# --------------------------------------------------------------------------
# macOS: launchd
# --------------------------------------------------------------------------


def test_launchd_plist_contents(tmp_path):
    data = tmp_path / "data"
    plist = plistlib.loads(service.launchd_plist(EXE, data))
    assert plist["Label"] == "com.copycord"
    assert plist["ProgramArguments"] == [*EXE, "run", "--data-dir", str(data)]
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] == {"SuccessfulExit": False}
    assert plist["StandardOutPath"] == str(data / "copycord.out.log")
    assert plist["StandardErrorPath"] == str(data / "copycord.err.log")


def test_launchd_install_and_lifecycle(tmp_path, monkeypatch):
    fake = FakeRun({("launchctl", "print"): (113, "")})
    monkeypatch.setattr(service, "run_cmd", fake)
    svc = service.LaunchdService(home=tmp_path, uid=501)
    svc.install(EXE, tmp_path / "data")
    plist_path = tmp_path / "Library" / "LaunchAgents" / "com.copycord.plist"
    assert plist_path.is_file()
    assert (tmp_path / "data").is_dir()
    assert fake.calls == [
        ["launchctl", "print", "gui/501/com.copycord"],
        ["launchctl", "bootstrap", "gui/501", str(plist_path)],
    ]

    fake.answers[("launchctl", "print")] = (0, "state = running\n")
    fake.calls.clear()
    assert svc.state() == service.State(True, True, svc.where)

    fake.calls.clear()
    svc.restart()
    assert fake.calls[-1] == ["launchctl", "kickstart", "-k", "gui/501/com.copycord"]

    fake.calls.clear()
    svc.stop()
    assert fake.calls[-1] == ["launchctl", "bootout", "gui/501/com.copycord"]

    fake.calls.clear()
    svc.uninstall()
    assert not plist_path.exists()
    assert ["launchctl", "bootout", "gui/501/com.copycord"] in fake.calls


def test_launchd_reinstall_boots_out_first(tmp_path, monkeypatch):
    fake = FakeRun({("launchctl", "print"): (0, "state = running")})
    monkeypatch.setattr(service, "run_cmd", fake)
    svc = service.LaunchdService(home=tmp_path, uid=501)
    svc.install(EXE, tmp_path / "data")
    assert [c[:2] for c in fake.calls] == [
        ["launchctl", "print"],
        ["launchctl", "bootout"],
        ["launchctl", "bootstrap"],
    ]


def test_launchd_start_when_not_loaded_bootstraps(tmp_path, monkeypatch):
    fake = FakeRun({("launchctl", "print"): (113, "")})
    monkeypatch.setattr(service, "run_cmd", fake)
    svc = service.LaunchdService(home=tmp_path, uid=501)
    with pytest.raises(service.ServiceError, match="not installed"):
        svc.start()
    svc.plist_path.parent.mkdir(parents=True)
    svc.plist_path.write_bytes(b"x")
    fake.calls.clear()
    svc.start()
    assert fake.calls[-1] == ["launchctl", "bootstrap", "gui/501", str(svc.plist_path)]


# --------------------------------------------------------------------------
# Windows: Scheduled Task
# --------------------------------------------------------------------------

WIN_EXE = [r"C:\Users\Mac Doe\.local\bin\copycord.exe"]


def test_task_xml_runs_at_logon_with_restart_on_failure():
    xml = service.task_xml(WIN_EXE, Path(r"C:\Users\Mac Doe\AppData\Local\Copycord"), "PC\\mac")
    assert "<LogonTrigger>" in xml
    assert "<UserId>PC\\mac</UserId>" in xml
    assert "<RestartOnFailure>" in xml
    assert "<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>" in xml
    assert f"<Command>{WIN_EXE[0]}</Command>" in xml
    assert '<Arguments>run --data-dir "C:\\Users\\Mac Doe\\AppData\\Local\\Copycord"</Arguments>' in xml


def test_task_xml_python_module_fallback_keeps_module_args():
    xml = service.task_xml([r"C:\Py\python.exe", "-m", "copycord"], Path(r"C:\d"), "u")
    assert "<Command>C:\\Py\\python.exe</Command>" in xml
    assert "<Arguments>-m copycord run --data-dir C:\\d</Arguments>" in xml


@pytest.fixture()
def no_processes(monkeypatch):
    """No leftover processes, free ports and no real waiting."""
    monkeypatch.setattr(service, "list_processes", lambda: [])
    monkeypatch.setattr(service, "busy_ports", lambda env=None: [])
    monkeypatch.setattr(service, "_sleep", lambda s: None)


def test_scheduled_task_install_and_lifecycle(tmp_path, monkeypatch, no_processes):
    written = {}
    task = {"running": False}

    def fake(cmd, capture=True):
        cmd = list(cmd)
        calls.append(cmd)
        if cmd[:2] == ["schtasks", "/Create"]:
            xml_path = cmd[cmd.index("/XML") + 1]
            written["xml"] = Path(xml_path).read_bytes().decode("utf-16")
        if cmd[:2] == ["schtasks", "/Run"]:
            task["running"] = True
        if cmd[:2] == ["schtasks", "/End"]:
            task["running"] = False
        if cmd[:2] == ["schtasks", "/Query"]:
            status = "Running" if task["running"] else "Ready"
            return subprocess.CompletedProcess(cmd, 0, f'"\\Copycord","N/A","{status}"\n', "")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    calls = []
    monkeypatch.setattr(service, "run_cmd", fake)
    svc = service.ScheduledTaskService(env={"USERNAME": "mac", "USERDOMAIN": "PC"})
    svc.install(WIN_EXE, tmp_path / "data")
    assert [c[:2] for c in calls] == [
        ["schtasks", "/Query"], ["schtasks", "/Create"], ["schtasks", "/Run"],
    ]
    create = calls[1]
    assert create[-1] == "/F"
    assert "<UserId>PC\\mac</UserId>" in written["xml"]
    assert not Path(create[create.index("/XML") + 1]).exists()

    assert svc.state() == service.State(True, True, svc.where)

    calls.clear()
    svc.restart()
    actions = [c[:2] for c in calls if c[1] != "/Query"]
    assert actions == [["schtasks", "/End"], ["schtasks", "/Run"]]
    assert calls[0] == ["schtasks", "/End", "/TN", "Copycord"]
    assert calls[-1] == ["schtasks", "/Run", "/TN", "Copycord"]

    calls.clear()
    svc.uninstall()
    assert calls[-1] == ["schtasks", "/Delete", "/TN", "Copycord", "/F"]


def test_scheduled_task_state_not_installed_and_ready(monkeypatch):
    fake = FakeRun({("schtasks", "/Query"): (1, "")})
    monkeypatch.setattr(service, "run_cmd", fake)
    svc = service.ScheduledTaskService(env={})
    assert svc.state() == service.State(False, False, svc.where)
    fake.answers[("schtasks", "/Query")] = (0, '"\\Copycord","N/A","Ready"\n')
    assert svc.state() == service.State(True, False, svc.where)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def test_cli_status_prints_one_line(monkeypatch, capsys):
    class Svc:
        def state(self):
            return service.State(True, False, "test unit")

    monkeypatch.setattr(service, "get_service", lambda user=False: Svc())
    assert cli.main(["service", "status"]) == 3
    out = capsys.readouterr().out
    assert out.strip() == "copycord service: installed, not running (test unit)"
    assert len(out.strip().splitlines()) == 1


def test_cli_install_passes_data_dir_and_command(tmp_path, monkeypatch, capsys):
    seen = {}

    class Svc:
        def install(self, command, data_dir):
            seen["command"], seen["data_dir"] = command, data_dir
            return ["ok"]

    monkeypatch.setenv("DATA_DIR", str(tmp_path / "before"))
    monkeypatch.setattr(service, "get_service", lambda user=False: Svc())
    monkeypatch.setattr(service, "SystemdService", lambda user: Svc())
    monkeypatch.setattr(service, "copycord_command", lambda: EXE)
    target = tmp_path / "svc-data"
    assert cli.main(["service", "install", "--user", "--data-dir", str(target)]) == 0
    assert seen == {"command": EXE, "data_dir": target.resolve()}


def test_cli_service_error_is_reported(monkeypatch, capsys):
    class Svc:
        def start(self):
            raise service.ServiceError("needs root")

    monkeypatch.setattr(service, "get_service", lambda user=False: Svc())
    assert cli.main(["service", "start"]) == 1
    assert "needs root" in capsys.readouterr().err


def test_copycord_command_prefers_running_console_script(tmp_path, monkeypatch):
    exe = tmp_path / "bin" / "copycord"
    exe.parent.mkdir()
    exe.write_text("#!/bin/sh\n")
    monkeypatch.setattr(service.sys, "argv", [str(exe), "service", "install"])
    assert service.copycord_command() == [str(exe.resolve())]

    monkeypatch.setattr(service.sys, "argv", ["-c"])
    monkeypatch.setattr(service.shutil, "which", lambda name: None)
    cmd = service.copycord_command()
    assert cmd[1:] == ["-m", "copycord"]
    assert Path(cmd[0]).is_absolute()
