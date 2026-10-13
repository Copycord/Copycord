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


def test_detect_uv_tool_by_path(tmp_path):
    prefix = tmp_path / ".local" / "share" / "uv" / "tools" / "copycord"
    assert update.detect_installer(str(prefix), "uv", docker=False) == update.UV_TOOL


def test_detect_pipx_by_metadata_file(tmp_path):
    (tmp_path / "pipx_metadata.json").write_text("{}")
    assert update.detect_installer(str(tmp_path), "pip", docker=False) == update.PIPX


def test_detect_pipx_by_path(tmp_path):
    prefix = tmp_path / ".local" / "share" / "pipx" / "venvs" / "copycord"
    assert update.detect_installer(str(prefix), "pip", docker=False) == update.PIPX


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
    assert "current 3.28.0" in out
    assert "channel stable" in out
    assert "latest  4.0.0 (v4.0.0)" in out
    assert "Update available" in out
    assert fake_net["seen"] == [(LIST_URL, 5.0)]


def test_check_up_to_date(monkeypatch, capsys, fake_net):
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    assert update.update(check_only=True) == 0
    assert "up to date" in capsys.readouterr().out


def test_check_github_failure_is_a_warning(monkeypatch, capsys, fake_net):
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    fake_net["value"] = urllib.error.URLError("timed out")
    assert update.update(check_only=True) == 0
    out = capsys.readouterr().out
    assert "WARN" in out and "timed out" in out


def test_check_does_not_upgrade(monkeypatch, fake_net):
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    monkeypatch.setattr(service, "run_cmd", lambda *a, **k: pytest.fail("ran a command"))
    assert update.update(check_only=True) == 0


def test_is_newer():
    assert update.is_newer("4.0.1", "4.0.0")
    assert update.is_newer("4.10.0", "4.9.9")
    assert not update.is_newer("4.0.0", "4.0.0")
    assert not update.is_newer("3.28.0", "4.0.0")


def test_is_older():
    assert update.is_older("3.28.0", "4.0.0")
    assert update.is_older("4.0.0b1", "4.0.0")
    assert not update.is_older("4.0.0", "4.0.0")
    assert not update.is_older("4.0.1", "4.0.0")
    assert not update.is_older("main", "4.0.0")
    assert not update.is_older("4.0.0", "abc123")


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


def test_update_tool_missing_from_path(monkeypatch, capsys, fake_net):
    monkeypatch.setattr(update, "detect_installer", lambda: update.UV_TOOL)
    monkeypatch.setattr(update.shutil, "which", lambda name: None)
    monkeypatch.setattr(service, "run_cmd", lambda *a, **k: pytest.fail("ran a command"))
    assert update.update() == 1
    assert f"uv tool install --force --python 3.12 {WHEEL}" in capsys.readouterr().err
