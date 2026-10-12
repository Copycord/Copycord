"""
Tests for `copycord update` (code/copycord/update.py): installer detection,
--check against a faked PyPI response, and restarting the service only when
it was running. No network, no real uv/pipx.
"""
import io
import json
import subprocess

import pytest

from copycord import __main__ as cli
from copycord import service, update


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


def test_upgrade_commands():
    assert update.upgrade_command(update.UV_TOOL)[0] == ["uv", "tool", "upgrade", "copycord"]
    assert update.upgrade_command(update.PIPX)[0] == ["pipx", "upgrade", "copycord"]
    cmd, hint = update.upgrade_command(update.PIP)
    assert cmd is None and "-m pip install --upgrade copycord" in hint
    cmd, hint = update.upgrade_command(update.UV_PIP)
    assert cmd is None and "uv pip install --upgrade" in hint


# --------------------------------------------------------------------------
# --check
# --------------------------------------------------------------------------


def _fake_pypi(monkeypatch, version=None, error=None):
    seen = {}

    def urlopen(req, timeout=None):
        seen["url"], seen["timeout"] = req.full_url, timeout
        if error:
            raise error
        return io.BytesIO(json.dumps({"info": {"version": version}}).encode())

    monkeypatch.setattr(update.urllib.request, "urlopen", urlopen)
    return seen


def test_check_reports_newer_version(monkeypatch, capsys):
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    seen = _fake_pypi(monkeypatch, "4.0.0")
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "current 3.28.0" in out
    assert "latest  4.0.0" in out
    assert "update is available" in out
    assert seen == {"url": "https://pypi.org/pypi/copycord/json", "timeout": 5.0}


def test_check_up_to_date(monkeypatch, capsys):
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    _fake_pypi(monkeypatch, "4.0.0")
    assert update.update(check_only=True) == 0
    assert "up to date" in capsys.readouterr().out


def test_check_pypi_failure_is_a_warning(monkeypatch, capsys):
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    _fake_pypi(monkeypatch, error=OSError("timed out"))
    assert update.update(check_only=True) == 0
    out = capsys.readouterr().out
    assert "WARN" in out and "timed out" in out


def test_check_does_not_upgrade(monkeypatch):
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    _fake_pypi(monkeypatch, "4.0.1")
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
def uv_tool(monkeypatch):
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
    assert rec.calls == [["uv", "tool", "upgrade", "copycord"]]
    assert restarts == [1]


@pytest.mark.parametrize("installed,running", [(False, False), (True, False)])
def test_update_does_not_restart_stopped_or_missing_service(
    uv_tool, monkeypatch, installed, running
):
    rec, restarts = uv_tool
    _state(monkeypatch, installed, running)
    assert update.update() == 0
    assert rec.calls == [["uv", "tool", "upgrade", "copycord"]]
    assert restarts == []


def test_update_failure_skips_restart(uv_tool, monkeypatch):
    rec, restarts = uv_tool
    rec.rc = 2
    _state(monkeypatch, True, True)
    assert update.update() == 2
    assert restarts == []


def test_update_pipx(monkeypatch):
    monkeypatch.setattr(update, "detect_installer", lambda: update.PIPX)
    monkeypatch.setattr(update.shutil, "which", lambda name: f"/usr/bin/{name}")
    rec = Recorder()
    monkeypatch.setattr(service, "run_cmd", rec)
    _state(monkeypatch, False, False)
    assert update.update() == 0
    assert rec.calls == [["pipx", "upgrade", "copycord"]]


def test_update_plain_pip_prints_command(monkeypatch, capsys):
    monkeypatch.setattr(update, "detect_installer", lambda: update.PIP)
    monkeypatch.setattr(service, "run_cmd", lambda *a, **k: pytest.fail("ran a command"))
    assert update.update() == 1
    assert "-m pip install --upgrade copycord" in capsys.readouterr().out


def test_update_tool_missing_from_path(monkeypatch, capsys):
    monkeypatch.setattr(update, "detect_installer", lambda: update.UV_TOOL)
    monkeypatch.setattr(update.shutil, "which", lambda name: None)
    monkeypatch.setattr(service, "run_cmd", lambda *a, **k: pytest.fail("ran a command"))
    assert update.update() == 1
    assert "uv tool upgrade copycord" in capsys.readouterr().err
