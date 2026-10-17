"""Tests for `copycord update`: installer detection, status and service restart. No network."""
import subprocess
import urllib.error

import pytest

from copycord import __main__ as cli
from copycord import service, update

RELEASES = "https://api.github.com/repos/Copycord/Copycord/releases"
LIST_URL = RELEASES + "?per_page=100"


def wheel_url(tag):
    return (
        f"https://github.com/Copycord/Copycord/releases/download/{tag}/"
        f"copycord-{tag.lstrip('v')}-py3-none-any.whl"
    )


def release(tag, prerelease=False, draft=False):
    name = wheel_url(tag).rsplit("/", 1)[1]
    return {
        "tag_name": tag,
        "prerelease": prerelease,
        "draft": draft,
        "assets": [{"name": name, "browser_download_url": wheel_url(tag)}],
    }


RELEASE_LIST = [release("v4.1.0b1", prerelease=True), release("v4.0.0"), release("v3.28.0")]
WHEEL = wheel_url("v4.0.0")


@pytest.fixture(autouse=True)
def _data_dir(tmp_path, monkeypatch):
    # The saved update channel lives in the data dir: start each test on stable.
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))


# --------------------------------------------------------------------------
# installer detection
# --------------------------------------------------------------------------


def test_detect_uv_tool_by_receipt(tmp_path):
    (tmp_path / "uv-receipt.toml").write_text("[tool]\n")
    assert update.detect_installer(str(tmp_path), "uv", docker=False) == update.UV_TOOL


def test_detect_pipx_by_metadata_file(tmp_path):
    (tmp_path / "pipx_metadata.json").write_text("{}")
    assert update.detect_installer(str(tmp_path), "pip", docker=False) == update.PIPX


def test_detect_plain_pip_and_uv_pip(tmp_path):
    venv = tmp_path / "venv"
    assert update.detect_installer(str(venv), "pip", docker=False) == update.PIP
    assert update.detect_installer(str(venv), "uv", docker=False) == update.UV_PIP


def test_detect_source_checkout_and_docker(tmp_path):
    assert update.detect_installer(str(tmp_path), None, docker=False) == update.SOURCE
    assert update.detect_installer(str(tmp_path), "pip", docker=True) == update.DOCKER


def test_install_commands():
    assert update.install_command(update.UV_TOOL, WHEEL)[0] == [
        "uv", "tool", "install", "--force", "--python", "3.12", WHEEL
    ]
    assert update.install_command(update.PIPX, WHEEL)[0] == [
        "pipx", "install", "--force", WHEEL
    ]
    cmd, hint = update.install_command(update.PIP, WHEEL)
    assert cmd is None and f'-m pip install --upgrade "{WHEEL}"' in hint
    cmd, hint = update.install_command(update.UV_PIP, WHEEL)
    assert cmd is None and "uv pip install --upgrade" in hint and WHEEL in hint


# --------------------------------------------------------------------------
# update status
# --------------------------------------------------------------------------


@pytest.fixture()
def fake_net(monkeypatch):
    """_fetch_json serving the faked releases list; an Exception is raised."""
    state = {"value": RELEASE_LIST, "seen": []}

    def fetch(url, timeout):
        state["seen"].append((url, timeout))
        if isinstance(state["value"], Exception):
            raise state["value"]
        return state["value"]

    monkeypatch.setattr(update, "_fetch_json", fetch)
    return state


def test_check_reports_newer_version(monkeypatch, capsys, fake_net):
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    assert cli.main(["update", "status"]) == 0
    out = capsys.readouterr().out
    assert "current  3.28.0" in out
    assert "stable channel" in out
    assert "latest   4.0.0 (v4.0.0)" in out
    assert "Update available" in out
    assert fake_net["seen"] == [(LIST_URL, 5.0)]


def test_check_github_failure_is_a_warning(monkeypatch, capsys, fake_net):
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    fake_net["value"] = urllib.error.URLError("timed out")
    assert update.update(check_only=True) == 0
    out = capsys.readouterr().out
    assert "! " in out and "timed out" in out


def test_check_does_not_upgrade(monkeypatch, fake_net):
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    monkeypatch.setattr(service, "run_cmd", lambda *a, **k: pytest.fail("ran a command"))
    assert update.update(check_only=True) == 0


def test_is_newer():
    assert update.is_newer("4.0.1", "4.0.0")
    assert update.is_newer("4.10.0", "4.9.9")
    assert not update.is_newer("4.0.0", "4.0.0")
    assert not update.is_newer("3.28.0", "4.0.0")


# --------------------------------------------------------------------------
# upgrade + restart
# --------------------------------------------------------------------------


class Recorder:
    def __init__(self, rc=0):
        self.calls = []
        self.rc = rc

    def __call__(self, cmd, capture=True):
        self.calls.append(list(cmd))
        return subprocess.CompletedProcess(list(cmd), self.rc, "", "")


@pytest.fixture()
def uv_tool(monkeypatch, fake_net):
    monkeypatch.setattr(update, "detect_installer", lambda: update.UV_TOOL)
    monkeypatch.setattr(update.shutil, "which", lambda name: f"/usr/bin/{name}")
    rec = Recorder()
    monkeypatch.setattr(service, "run_cmd", rec)
    restarts = []
    monkeypatch.setattr(service, "restart_service", lambda: restarts.append(1))
    return rec, restarts


def _state(monkeypatch, installed, running):
    monkeypatch.setattr(
        service, "service_state", lambda: service.State(installed, running, "test")
    )


def test_update_restarts_running_service(uv_tool, monkeypatch):
    rec, restarts = uv_tool
    _state(monkeypatch, True, True)
    assert cli.main(["update"]) == 0
    assert rec.calls == [["uv", "tool", "install", "--force", "--python", "3.12", WHEEL]]
    assert restarts == [1]


@pytest.mark.parametrize("installed,running", [(False, False), (True, False)])
def test_update_does_not_restart_stopped_or_missing_service(
    uv_tool, monkeypatch, installed, running
):
    rec, restarts = uv_tool
    _state(monkeypatch, installed, running)
    assert update.update() == 0
    assert rec.calls == [["uv", "tool", "install", "--force", "--python", "3.12", WHEEL]]
    assert restarts == []


def test_update_failure_skips_restart(uv_tool, monkeypatch):
    rec, restarts = uv_tool
    rec.rc = 2
    _state(monkeypatch, True, True)
    assert update.update() == 2
    assert restarts == []


def test_update_pipx(monkeypatch, fake_net):
    monkeypatch.setattr(update, "detect_installer", lambda: update.PIPX)
    monkeypatch.setattr(update.shutil, "which", lambda name: f"/usr/bin/{name}")
    rec = Recorder()
    monkeypatch.setattr(service, "run_cmd", rec)
    _state(monkeypatch, False, False)
    assert update.update() == 0
    assert rec.calls == [["pipx", "install", "--force", WHEEL]]


def test_update_plain_pip_prints_command(monkeypatch, capsys, fake_net):
    monkeypatch.setattr(update, "detect_installer", lambda: update.PIP)
    monkeypatch.setattr(service, "run_cmd", lambda *a, **k: pytest.fail("ran a command"))
    assert update.update() == 1
    assert f'-m pip install --upgrade "{WHEEL}"' in capsys.readouterr().out


def test_update_on_the_latest_release_installs_nothing(uv_tool, monkeypatch, capsys):
    rec, restarts = uv_tool
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    monkeypatch.setattr(update, "git_source", lambda: None)
    monkeypatch.setattr(update, "backup_database", lambda skip=False: pytest.fail("backed up"))
    assert cli.main(["update"]) == 0
    assert rec.calls == [] and restarts == []
    assert "Copycord is up to date (4.0.0)" in capsys.readouterr().out


# --------------------------------------------------------------------------
# auto-update: record, check, roll back
# --------------------------------------------------------------------------

OLD_WHEEL = wheel_url("v3.28.0")


class Cmds:
    """service.run_cmd fake: installs and copycord checks, each with its own exit code."""

    def __init__(self):
        self.calls = []
        self.rc = {"install": 0, "rollback": 0, "--version": 0, "doctor": 0}

    def __call__(self, cmd, capture=True, env=None):
        cmd = list(cmd)
        self.calls.append(cmd)
        if cmd[:3] == ["uv", "tool", "install"]:
            rc = self.rc["rollback" if cmd[-1] == OLD_WHEEL else "install"]
        else:
            rc = self.rc["doctor" if "doctor" in cmd else "--version"]
        return subprocess.CompletedProcess(cmd, rc, "", "")

    def installs(self, spec):
        return [c for c in self.calls if c[:3] == ["uv", "tool", "install"] and c[-1] == spec]


@pytest.fixture()
def auto(monkeypatch, fake_net):
    monkeypatch.setattr(update, "detect_installer", lambda: update.UV_TOOL)
    monkeypatch.setattr(update.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    monkeypatch.setattr(update, "installed_commit", lambda: None)
    monkeypatch.setattr(update, "git_source", lambda: None)
    monkeypatch.setattr(update, "installed_version", lambda: ("4.0.0", None))
    monkeypatch.setattr(update, "fetch_release",
                        lambda tag, timeout=5.0: update.Release("v3.28.0", "3.28.0", False, OLD_WHEEL))
    cmds = Cmds()
    monkeypatch.setattr(service, "run_cmd", cmds)
    restarts = []
    monkeypatch.setattr(service, "restart_service", lambda: restarts.append(1))
    _state(monkeypatch, True, True)
    return cmds, restarts


def test_auto_update_records_the_previous_version(auto):
    import json
    import os

    cmds, restarts = auto
    assert update.update(yes=True, auto=True) == 0
    path = update.previous_path()
    prev = json.loads(path.read_text())
    assert prev["version"] == "3.28.0" and prev["channel"] == "stable"
    assert prev["method"] == update.UV_TOOL and prev["spec"] == OLD_WHEEL
    assert prev["pending"]["version"] == "4.0.0"
    if os.name == "posix":
        assert path.stat().st_mode & 0o777 == 0o600
    assert len(cmds.installs(WHEEL)) == 1 and cmds.installs(OLD_WHEEL) == []
    assert restarts == [1]
    assert update.auto_update_stopped() is None


def test_health_failure_rolls_back_exactly_once(auto, monkeypatch):
    cmds, restarts = auto
    assert update.update(yes=True, auto=True) == 0
    monkeypatch.setattr(update, "wait_healthy", lambda: False)
    assert update.finish_pending() is False
    assert len(cmds.installs(OLD_WHEEL)) == 1
    assert cmds.installs(OLD_WHEEL)[0][:4] == ["uv", "tool", "install", "--reinstall"]
    assert restarts == [1, 1]
    stopped = update.auto_update_stopped()
    assert stopped["from"] == "3.28.0" and stopped["to"] == "4.0.0"
    # Nothing is pending any more, so a second start does not roll back again.
    assert update.finish_pending() is None
    assert len(cmds.installs(OLD_WHEEL)) == 1


def test_health_success_keeps_the_record_and_rolls_nothing_back(auto, monkeypatch):
    cmds, restarts = auto
    assert update.update(yes=True, auto=True) == 0
    monkeypatch.setattr(update, "wait_healthy", lambda: True)
    assert update.finish_pending() is True
    prev = update.load_previous()
    assert prev["spec"] == OLD_WHEEL and prev["pending"] is None
    assert cmds.installs(OLD_WHEEL) == [] and restarts == [1]
    assert update.auto_update_stopped() is None


def test_failed_version_check_rolls_back_before_restart(auto):
    cmds, restarts = auto
    cmds.rc["--version"] = 1
    assert update.update(yes=True, auto=True) == 1
    assert len(cmds.installs(OLD_WHEEL)) == 1
    assert restarts == [1]
    assert update.auto_update_stopped() is not None
    assert update.load_previous()["pending"] is None


def test_old_doctor_problem_does_not_roll_back(auto):
    cmds, restarts = auto
    cmds.rc["doctor"] = 1
    assert update.update(yes=True, auto=True) == 0
    assert cmds.installs(OLD_WHEEL) == []


def test_rollback_failure_stops_auto_update_and_logs(auto, caplog):
    import logging

    cmds, restarts = auto
    cmds.rc["--version"] = 1
    cmds.rc["rollback"] = 1
    with caplog.at_level(logging.ERROR, logger="copycord.update"):
        assert update.update(yes=True, auto=True) == 1
    assert len(cmds.installs(OLD_WHEEL)) == 1
    assert restarts == []
    assert update.auto_update_stopped() is not None
    text = caplog.text
    assert "3.28.0" in text and "4.0.0" in text and "putting 3.28.0 back failed too" in text


def test_manual_update_turns_auto_update_back_on(auto):
    update.stop_auto_update("test", "3.28.0", "4.0.0")
    assert update.update(yes=True) == 0
    assert update.auto_update_stopped() is None


def test_auto_update_waits_when_the_old_version_cannot_be_found(auto, monkeypatch):
    cmds, restarts = auto

    def missing(tag, timeout=5.0):
        raise update.ReleaseError("no release")

    monkeypatch.setattr(update, "fetch_release", missing)
    assert update.update(yes=True, auto=True) == 1
    assert cmds.installs(WHEEL) == []


def test_health_url_follows_the_dashboard_settings(monkeypatch):
    monkeypatch.delenv("DASHBOARD_HOST", raising=False)
    monkeypatch.delenv("ADMIN_PORT", raising=False)
    assert update.health_url() == "http://127.0.0.1:8080/health"
    monkeypatch.setenv("DASHBOARD_HOST", "192.168.1.5")
    monkeypatch.setenv("ADMIN_PORT", "9000")
    assert update.health_url() == "http://192.168.1.5:9000/health"


def test_wait_healthy_gives_up_after_the_timeout(monkeypatch):
    now = [0.0]

    def fail(*a, **k):
        raise urllib.error.URLError("refused")

    monkeypatch.setattr(update.urllib.request, "urlopen", fail)
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        now[0] += s

    assert update.wait_healthy(timeout=10, interval=2, sleep=sleep, clock=lambda: now[0]) is False
    assert len(sleeps) == 5
