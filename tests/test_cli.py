"""
Tests for the copycord CLI (code/copycord/__main__.py) and the data-dir
resolution (code/common/paths.py). No network.
"""
import json
import os
import socket
import sys
from pathlib import Path

import pytest

from common import paths
from common.config import CURRENT_VERSION
from copycord import __main__ as cli


def _unset(monkeypatch, *names):
    # setenv first so monkeypatch records the original and restores it even
    # when the code under test sets the variable again.
    for name in names:
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    """A temp data dir; --data-dir writes os.environ, so DATA_DIR is restored."""
    d = tmp_path / "data"
    monkeypatch.setenv("DATA_DIR", str(d))
    _unset(monkeypatch, "DB_PATH", "ROLE")
    return d


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _statuses(out):
    return [line.split()[0] for line in out.strip().splitlines()]


# --------------------------------------------------------------------------
# version
# --------------------------------------------------------------------------


def test_version_prints_current_version(capsys):
    assert cli.main(["version"]) == 0
    assert capsys.readouterr().out.strip() == CURRENT_VERSION


# --------------------------------------------------------------------------
# doctor
# --------------------------------------------------------------------------


def test_doctor_passes_with_temp_data_dir(data_dir, monkeypatch, capsys):
    monkeypatch.setenv("ADMIN_PORT", str(_free_port()))
    # The import checks depend on what this venv has installed; covered below.
    monkeypatch.setattr(cli, "REQUIRED_IMPORTS", ())

    assert cli.main(["doctor", "--data-dir", str(data_dir)]) == 0

    out = capsys.readouterr().out
    assert set(_statuses(out)) <= {"PASS", "WARN"}
    assert f"data dir: {data_dir.resolve()} will be created" in out
    assert "ROLE: unset" in out
    # Read-only checks: nothing created.
    assert not data_dir.exists()


def test_doctor_role_set_fails(data_dir, monkeypatch, capsys):
    monkeypatch.setenv("ADMIN_PORT", str(_free_port()))
    monkeypatch.setattr(cli, "REQUIRED_IMPORTS", ())
    monkeypatch.setenv("ROLE", "server")

    assert cli.main(["doctor", "--data-dir", str(data_dir)]) == 1

    lines = capsys.readouterr().out.splitlines()
    role = next(line for line in lines if " ROLE:" in line)
    assert role.startswith("FAIL")
    assert "ROLE option was removed" in role


def test_doctor_port_in_use_fails(data_dir, monkeypatch, capsys):
    monkeypatch.setattr(cli, "REQUIRED_IMPORTS", ())
    with socket.socket() as s:
        s.bind((cli.ADMIN_BIND_HOST, 0))
        s.listen(1)
        port = s.getsockname()[1]
        monkeypatch.setenv("ADMIN_PORT", str(port))

        check = cli.check_admin_port()
        assert check.status == cli.FAIL
        assert str(port) in check.reason

        assert cli.main(["doctor", "--data-dir", str(data_dir)]) == 1
    out = capsys.readouterr().out
    assert any(
        line.startswith("FAIL") and "admin port" in line for line in out.splitlines()
    )


def test_check_admin_port_free_and_invalid():
    assert cli.check_admin_port({"ADMIN_PORT": str(_free_port())}).status == cli.PASS
    assert cli.check_admin_port({"ADMIN_PORT": "nope"}).status == cli.FAIL
    assert cli.check_admin_port({"ADMIN_PORT": "70000"}).status == cli.FAIL


def test_check_python_floor():
    assert cli.check_python((3, 11, 9)).status == cli.FAIL
    assert cli.check_python((3, 12, 0)).status == cli.PASS
    assert cli.check_python((3, 14, 1)).status == cli.PASS


def test_check_data_dir(tmp_path):
    assert cli.check_data_dir(tmp_path).status == cli.PASS
    assert cli.check_data_dir(tmp_path / "a" / "b").status == cli.PASS
    f = tmp_path / "file"
    f.write_text("x")
    assert cli.check_data_dir(f).status == cli.FAIL


def test_check_db(tmp_path):
    from common.db import DBManager

    assert cli.check_db(tmp_path / "missing.db").status == cli.PASS

    bad = tmp_path / "bad.db"
    bad.write_bytes(b"this is not a sqlite database" * 200)
    assert cli.check_db(bad).status == cli.FAIL

    good = tmp_path / "good.db"
    DBManager(str(good), init_schema=True).conn.close()
    check = cli.check_db(good)
    assert check.status == cli.PASS
    assert "opens" in check.reason


def test_check_frontend(tmp_path):
    assert cli.check_frontend(tmp_path).status == cli.WARN
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "main.js").write_text("")
    assert cli.check_frontend(tmp_path).status == cli.PASS


def test_check_import():
    assert cli.check_import("json", "pytest").status == cli.PASS
    assert cli.check_import("no_such_module_copycord", "nothing").status == cli.FAIL


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------


def test_config_round_trip_with_masking(data_dir, tmp_path, capsys):
    dd = ["--data-dir", str(data_dir)]
    assert cli.main(["config", "set", "SERVER_TOKEN", "srv-secret", *dd]) == 0
    assert cli.main(["config", "set", "CLIENT_TOKEN", "cli-secret", *dd]) == 0
    assert cli.main(["config", "set", "ADMIN_PASSWORD", "pw", *dd]) == 0
    assert cli.main(["config", "set", "COMMAND_USERS", "1,2", *dd]) == 0
    assert (data_dir / "data.db").exists()
    capsys.readouterr()

    assert cli.main(["config", "get", "COMMAND_USERS", *dd]) == 0
    assert capsys.readouterr().out.strip() == "1,2"

    # stdout export masks tokens, passwords and secrets.
    assert cli.main(["config", "export", *dd]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["SERVER_TOKEN"] == cli.MASK
    assert shown["CLIENT_TOKEN"] == cli.MASK
    assert shown["ADMIN_PASSWORD"] == cli.MASK
    assert shown["COMMAND_USERS"] == "1,2"

    assert cli.main(["config", "export", "--show-secrets", *dd]) == 0
    out = capsys.readouterr().out
    assert json.loads(out)["SERVER_TOKEN"] == "srv-secret"

    # A file export keeps the real values (it is the backup) and prints none.
    backup = tmp_path / "backup.json"
    assert cli.main(["config", "export", str(backup), *dd]) == 0
    assert "srv-secret" not in capsys.readouterr().out
    exported = json.loads(backup.read_text())
    assert exported["SERVER_TOKEN"] == "srv-secret"

    # Import into a fresh data dir and read the values back.
    other = tmp_path / "other"
    od = ["--data-dir", str(other)]
    assert cli.main(["config", "import", str(backup), *od]) == 0
    capsys.readouterr()
    assert cli.main(["config", "get", "SERVER_TOKEN", *od]) == 0
    assert capsys.readouterr().out.strip() == "srv-secret"
    assert cli.main(["config", "export", "--show-secrets", *od]) == 0
    assert json.loads(capsys.readouterr().out) == exported


def test_config_import_skips_masked_values(data_dir, tmp_path, capsys):
    dd = ["--data-dir", str(data_dir)]
    src = tmp_path / "masked.json"
    src.write_text(json.dumps({"SERVER_TOKEN": cli.MASK, "SYNC_INTERVAL_SECONDS": 60}))

    assert cli.main(["config", "import", str(src), *dd]) == 0
    assert "skipped SERVER_TOKEN" in capsys.readouterr().err
    assert cli.main(["config", "get", "SERVER_TOKEN", *dd]) == 1
    capsys.readouterr()
    assert cli.main(["config", "get", "SYNC_INTERVAL_SECONDS", *dd]) == 0
    assert capsys.readouterr().out.strip() == "60"


def test_config_import_rejects_bad_json(data_dir, tmp_path):
    dd = ["--data-dir", str(data_dir)]
    src = tmp_path / "list.json"
    src.write_text("[1, 2]")
    assert cli.main(["config", "import", str(src), *dd]) == 1
    src.write_text(json.dumps({"A": {"nested": 1}}))
    assert cli.main(["config", "import", str(src), *dd]) == 1


def test_config_get_without_db_does_not_create_it(data_dir, capsys):
    assert cli.main(["config", "get", "SERVER_TOKEN", "--data-dir", str(data_dir)]) == 1
    assert "no database" in capsys.readouterr().err
    assert not data_dir.exists()


def test_data_dir_flag_sets_env(data_dir, tmp_path, monkeypatch):
    target = tmp_path / "elsewhere"
    monkeypatch.setattr(cli, "REQUIRED_IMPORTS", ())
    monkeypatch.setenv("ADMIN_PORT", str(_free_port()))
    cli.main(["doctor", "--data-dir", str(target)])

    assert os.environ["DATA_DIR"] == str(target.resolve())
    assert paths.data_dir() == target.resolve()
    assert paths.db_path() == target.resolve() / "data.db"


# --------------------------------------------------------------------------
# data-dir defaults
# --------------------------------------------------------------------------


@pytest.fixture()
def native(monkeypatch, tmp_path):
    """No DATA_DIR, not in Docker, HOME at a temp dir."""
    _unset(monkeypatch, "DATA_DIR", "DB_PATH", "LOCALAPPDATA")
    monkeypatch.setattr(paths, "in_docker", lambda: False)
    home = tmp_path / "home"
    monkeypatch.setattr(paths.Path, "home", classmethod(lambda cls: home))
    return home


def test_default_linux(native, monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    assert paths.data_dir() == (native / ".local" / "share" / "copycord").resolve()
    assert paths.db_path() == paths.data_dir() / "data.db"


def test_default_macos(native, monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    expected = native / "Library" / "Application Support" / "Copycord"
    assert paths.data_dir() == expected.resolve()


def test_default_windows(native, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "platform", "win32")
    local = tmp_path / "LocalAppData"
    monkeypatch.setenv("LOCALAPPDATA", str(local))
    assert paths.data_dir() == (local / "Copycord").resolve()


def test_default_windows_without_localappdata(native, monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    expected = native / "AppData" / "Local" / "Copycord"
    assert paths.data_dir() == expected.resolve()


def test_docker_default_is_data(native, monkeypatch):
    monkeypatch.setattr(paths, "in_docker", lambda: True)
    assert paths.default_data_dir() == Path("/data")


def test_data_dir_env_wins(native, monkeypatch, tmp_path):
    monkeypatch.setattr(paths, "in_docker", lambda: True)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "custom"))
    assert paths.data_dir() == (tmp_path / "custom").resolve()


def test_db_path_env_wins(native, monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "x.db"))
    assert paths.db_path() == (tmp_path / "x.db").resolve()


def test_platform_data_dir_is_pure(tmp_path):
    home = tmp_path
    assert paths.platform_data_dir("linux", {}, home) == home / ".local/share/copycord"
    assert paths.platform_data_dir("darwin", {}, home) == (
        home / "Library/Application Support/Copycord"
    )
    assert paths.platform_data_dir("win32", {"LOCALAPPDATA": "C:/L"}, home) == (
        Path("C:/L") / "Copycord"
    )


def test_supervisor_and_logging_agree(native, monkeypatch):
    from copycord import logging_setup

    monkeypatch.setattr(sys, "platform", "linux")
    assert logging_setup.logs_dir() == paths.data_dir()
