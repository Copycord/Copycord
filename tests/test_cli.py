"""
Tests for the copycord CLI (src/copycord/__main__.py) and the data-dir
resolution (src/copycord/common/paths.py). No network.
"""
import json
import os
import socket
import sys
from pathlib import Path

import pytest

from copycord.common import paths
from copycord.common.config import CURRENT_VERSION
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


def test_version_prints_current_version(data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines == [CURRENT_VERSION, "channel stable"]


@pytest.mark.parametrize("argv", (["version"], ["update", "--check"], ["update", "--branch", "main"], ["onboard"]))
def test_removed_commands_and_flags(argv, data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2


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
    assert cli.check_python((3, 15, 0)).status == cli.FAIL


def test_check_data_dir(tmp_path):
    assert cli.check_data_dir(tmp_path).status == cli.PASS
    assert cli.check_data_dir(tmp_path / "a" / "b").status == cli.PASS
    f = tmp_path / "file"
    f.write_text("x")
    assert cli.check_data_dir(f).status == cli.FAIL


def test_check_db(tmp_path):
    from copycord.common.db import DBManager

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
    assert capsys.readouterr().out.strip() == cli.MASK
    assert cli.main(["config", "get", "SERVER_TOKEN", "--show-secrets", *od]) == 0
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


def test_config_unset_db_key(data_dir, capsys):
    dd = ["--data-dir", str(data_dir)]
    assert cli.main(["config", "set", "COMMAND_USERS", "1,2", *dd]) == 0
    assert cli.main(["config", "unset", "COMMAND_USERS", *dd]) == 0
    assert "COMMAND_USERS removed" in capsys.readouterr().err
    assert cli.main(["config", "get", "COMMAND_USERS", *dd]) == 1
    assert cli.main(["config", "unset", "COMMAND_USERS", *dd]) == 1


def test_config_env_only_keys_go_to_env_file(data_dir, monkeypatch, capsys):
    from copycord import envfile

    _unset(monkeypatch, "PASSWORD", "ADMIN_PORT")
    dd = ["--data-dir", str(data_dir)]
    env = data_dir / ".env"
    data_dir.mkdir(parents=True)
    env.write_text("# my notes\nADMIN_PORT=9000\n", encoding="utf-8")

    assert cli.main(["config", "set", "PASSWORD", "it's a #pass", *dd]) == 0
    assert "apply it with: copycord restart" in capsys.readouterr().err
    assert cli.main(["config", "set", "ADMIN_PORT", "9090", *dd]) == 0
    text = env.read_text(encoding="utf-8")
    assert text.startswith("# my notes\nADMIN_PORT=9090\n")
    assert text.count("ADMIN_PORT") == 1
    assert not (data_dir / "data.db").exists()
    assert envfile.get("PASSWORD", env) == "it's a #pass"

    capsys.readouterr()
    assert cli.main(["config", "get", "PASSWORD", *dd]) == 0
    assert capsys.readouterr().out.strip() == cli.MASK
    assert cli.main(["config", "get", "PASSWORD", "--show-secrets", *dd]) == 0
    assert capsys.readouterr().out.strip() == "it's a #pass"
    assert cli.main(["config", "get", "ADMIN_PORT", *dd]) == 0
    assert capsys.readouterr().out.strip() == "9090"

    assert cli.main(["config", "unset", "PASSWORD", *dd]) == 0
    assert cli.main(["config", "get", "PASSWORD", *dd]) == 1
    assert cli.main(["config", "unset", "PASSWORD", *dd]) == 1
    assert "# my notes" in env.read_text(encoding="utf-8")


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes")
def test_env_file_is_created_private(data_dir, monkeypatch):
    _unset(monkeypatch, "PASSWORD")
    assert cli.main(["config", "set", "PASSWORD", "pw", "--data-dir", str(data_dir)]) == 0
    assert (data_dir / ".env").stat().st_mode & 0o777 == 0o600


def test_env_file_is_loaded_without_overriding(data_dir, monkeypatch):
    data_dir.mkdir(parents=True)
    (data_dir / ".env").write_text(
        "COPYCORD_TEST_FROM_FILE=file\nCOPYCORD_TEST_REAL=file\n", encoding="utf-8"
    )
    _unset(monkeypatch, "COPYCORD_TEST_FROM_FILE")
    monkeypatch.setenv("COPYCORD_TEST_REAL", "real")
    assert cli.main(["doctor", "--data-dir", str(data_dir)]) in (0, 1)
    assert os.environ["COPYCORD_TEST_FROM_FILE"] == "file"
    assert os.environ["COPYCORD_TEST_REAL"] == "real"


def test_env_template_sets_every_default(tmp_path):
    from copycord import envfile

    home = tmp_path / "home"
    text = envfile.template(home)
    lines = text.splitlines()
    assert envfile.DEFAULT_PASSWORD == "copycord"
    expected = {
        "PASSWORD": "copycord",
        "ADMIN_PORT": "8080",
        "TZ": "UTC",
        "UI_TZ": "America/New_York",
        "LOG_LEVEL": "INFO",
        "LOG_FORMAT": "HUMAN",
        "BACKUP_DIR": str(home / "backups"),
        "BACKUP_AT": "03:17",
        "BACKUP_RETAIN": "14",
        "COPYCORD_NO_UPDATE_CHECK": "0",
    }
    active = [x for x in lines if x and not x.startswith("#")]
    assert active == [f"{k}={v}" for k, v in expected.items()]
    for key in expected:
        # A one-line description above each key.
        above = lines[lines.index(f"{key}={expected[key]}") - 1]
        assert above.startswith("# ") and len(above) > 4
    # Copycord saves its own SECRET_KEY; the template says so instead of setting one.
    assert not any(x.lstrip("#").startswith("SECRET_KEY=") for x in lines)
    assert any(x.startswith("# SECRET_KEY") and "secret.key" in x for x in lines)
    assert "copycord restart" in text
    assert set(envfile.ENV_KEYS) - {"SECRET_KEY"} <= set(expected)

    env = tmp_path / ".env"
    env.write_text(text, encoding="utf-8")
    from dotenv import dotenv_values

    assert dotenv_values(env) == expected


def test_env_template_defaults_match_the_code(tmp_path, monkeypatch):
    """The values the template writes, or an empty value, act like no value."""
    from copycord import envfile, update_check

    monkeypatch.delenv(update_check.ENV_OPT_OUT, raising=False)
    monkeypatch.setattr(update_check, "_flag", lambda key, default: default)
    assert update_check.check_enabled()
    for value in (envfile.ENV_DEFAULTS["COPYCORD_NO_UPDATE_CHECK"], ""):
        monkeypatch.setenv(update_check.ENV_OPT_OUT, value)
        assert update_check.check_enabled()
    monkeypatch.setenv(update_check.ENV_OPT_OUT, "1")
    assert not update_check.check_enabled()

    import re

    root = Path(__file__).resolve().parents[1] / "src" / "copycord"
    sources = "\n".join(
        (root / f).read_text(encoding="utf-8")
        for f in ("admin/app.py", "supervisor.py", "client/export_runners.py")
    )
    for key in ("ADMIN_PORT", "TZ", "UI_TZ", "BACKUP_AT", "BACKUP_RETAIN"):
        reads = re.findall(rf'os\.getenv\("{key}"\) or "([^"]*)"', sources)
        assert reads, key
        assert set(reads) == {envfile.ENV_DEFAULTS[key]}, key
        # No read that keeps an empty value instead of the default.
        assert not re.search(rf'os\.getenv\("{key}", ', sources), key
    assert 'os.getenv("BACKUP_DIR") or str(DATA_DIR / "backups")' in sources


def test_config_set_updates_the_template_lines_in_place(tmp_path):
    from copycord import envfile

    env = tmp_path / ".env"
    template = envfile.template(tmp_path)
    env.write_text(template, encoding="utf-8")
    envfile.set_value("PASSWORD", "pw", env)
    envfile.set_value("ADMIN_PORT", "9000", env)
    envfile.set_value("COPYCORD_NO_UPDATE_CHECK", "1", env)
    lines = env.read_text(encoding="utf-8").splitlines()
    before = template.splitlines()
    assert len(lines) == len(before)
    for old, new in (
        ("PASSWORD=copycord", "PASSWORD=pw"),
        ("ADMIN_PORT=8080", "ADMIN_PORT=9000"),
        ("COPYCORD_NO_UPDATE_CHECK=0", "COPYCORD_NO_UPDATE_CHECK=1"),
    ):
        assert old not in lines
        assert lines.index(new) == before.index(old)
    assert [x for x in lines if x.startswith("#")] == [x for x in before if x.startswith("#")]

    # SECRET_KEY has no line in the template; set appends one.
    envfile.set_value("SECRET_KEY", "s3cret", env)
    assert env.read_text(encoding="utf-8").splitlines()[-1] == "SECRET_KEY=s3cret"


def test_config_set_fills_a_commented_line(tmp_path):
    from copycord import envfile

    env = tmp_path / ".env"
    env.write_text("# dashboard port\n#ADMIN_PORT=8080\n", encoding="utf-8")
    envfile.set_value("ADMIN_PORT", "9000", env)
    assert env.read_text(encoding="utf-8") == "# dashboard port\nADMIN_PORT=9000\n"


def _service_state(monkeypatch, installed, running, restart_error=None):
    from copycord import service

    restarts = []

    def restart():
        if restart_error:
            raise service.ServiceError(restart_error)
        restarts.append(1)

    state = service.State(installed, running, "systemd user unit /x/copycord.service")
    monkeypatch.setattr(service, "service_state", lambda: state)
    monkeypatch.setattr(service, "restart_service", restart)
    return restarts


def test_restart_restarts_a_running_service(monkeypatch, capsys):
    restarts = _service_state(monkeypatch, True, True)
    assert cli.main(["restart"]) == 0
    assert restarts == [1]
    assert "restarted the copycord service" in capsys.readouterr().out


def test_restart_without_a_service_says_to_rerun_in_the_terminal(monkeypatch, capsys):
    restarts = _service_state(monkeypatch, False, False)
    assert cli.main(["restart"]) == 1
    assert restarts == []
    out = capsys.readouterr().out
    assert "Copycord is not running as a service" in out
    assert "copycord run" in out


def test_restart_a_stopped_service_points_at_service_start(monkeypatch, capsys):
    restarts = _service_state(monkeypatch, True, False)
    assert cli.main(["restart"]) == 1
    assert restarts == []
    assert "copycord service start" in capsys.readouterr().out


def test_restart_reports_a_failed_restart(monkeypatch, capsys):
    _service_state(monkeypatch, True, True, restart_error="needs root")
    assert cli.main(["restart"]) == 1
    assert "copycord restart: needs root" in capsys.readouterr().err


def test_help_lists_restart(capsys):
    with pytest.raises(SystemExit):
        cli.main(["--help"])
    assert "restart" in capsys.readouterr().out


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
    _unset(monkeypatch, "DATA_DIR", "DB_PATH", "LOCALAPPDATA", "COPYCORD_HOME")
    monkeypatch.setattr(paths, "in_docker", lambda: False)
    home = tmp_path / "home"
    monkeypatch.setattr(paths.Path, "home", classmethod(lambda cls: home))
    return home


@pytest.mark.parametrize("platform", ["linux", "darwin", "win32"])
def test_default_is_copycord_home_everywhere(native, monkeypatch, platform):
    monkeypatch.setattr(sys, "platform", platform)
    _unset(monkeypatch, "COPYCORD_HOME")
    assert paths.data_dir() == (native / ".copycord").resolve()
    assert paths.db_path() == paths.data_dir() / "data.db"


def test_copycord_home_env_overrides(native, monkeypatch, tmp_path):
    monkeypatch.setenv("COPYCORD_HOME", str(tmp_path / "ch"))
    assert paths.data_dir() == (tmp_path / "ch").resolve()


def test_data_dir_beats_copycord_home(native, monkeypatch, tmp_path):
    monkeypatch.setenv("COPYCORD_HOME", str(tmp_path / "ch"))
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "dd"))
    assert paths.data_dir() == (tmp_path / "dd").resolve()


def test_docker_beats_copycord_home(native, monkeypatch, tmp_path):
    monkeypatch.setenv("COPYCORD_HOME", str(tmp_path / "ch"))
    monkeypatch.setattr(paths, "in_docker", lambda: True)
    assert paths.data_dir() == Path("/data").resolve()


def test_saved_home_pointer(native, monkeypatch, tmp_path):
    _unset(monkeypatch, "COPYCORD_HOME")
    native.mkdir(parents=True)
    (native / ".copycord-home").write_text(str(tmp_path / "saved") + "\n")
    assert paths.data_dir() == (tmp_path / "saved").resolve()
    monkeypatch.setenv("COPYCORD_HOME", str(tmp_path / "ch"))
    assert paths.data_dir() == (tmp_path / "ch").resolve()


@pytest.mark.parametrize(
    "platform,rel",
    [
        ("linux", Path(".local") / "share" / "copycord"),
        ("darwin", Path("Library") / "Application Support" / "Copycord"),
    ],
)
def test_legacy_dir_used_when_home_missing(native, monkeypatch, capsys, platform, rel):
    monkeypatch.setattr(sys, "platform", platform)
    _unset(monkeypatch, "COPYCORD_HOME")
    paths._notified.clear()
    old = native / rel
    old.mkdir(parents=True)
    (old / "data.db").write_bytes(b"")
    assert paths.data_dir() == old.resolve()
    assert "old data directory" in capsys.readouterr().err
    paths.data_dir()
    assert capsys.readouterr().err == ""
    (native / ".copycord").mkdir()
    assert paths.data_dir() == (native / ".copycord").resolve()


def test_legacy_dir_windows(native, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "platform", "win32")
    _unset(monkeypatch, "COPYCORD_HOME")
    local = tmp_path / "LocalAppData"
    monkeypatch.setenv("LOCALAPPDATA", str(local))
    (local / "Copycord").mkdir(parents=True)
    (local / "Copycord" / "data.db").write_bytes(b"")
    assert paths.data_dir() == (local / "Copycord").resolve()


def test_legacy_dir_without_db_is_ignored(native, monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    _unset(monkeypatch, "COPYCORD_HOME")
    (native / ".local" / "share" / "copycord").mkdir(parents=True)
    assert paths.data_dir() == (native / ".copycord").resolve()


def test_default_windows_without_localappdata(native, monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    expected = native / ".copycord"
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


def test_legacy_data_dir_is_pure(tmp_path):
    home = tmp_path
    assert paths.legacy_data_dir("linux", {}, home) == home / ".local/share/copycord"
    assert paths.legacy_data_dir("darwin", {}, home) == (
        home / "Library/Application Support/Copycord"
    )
    assert paths.legacy_data_dir("win32", {"LOCALAPPDATA": "C:/L"}, home) == (
        Path("C:/L") / "Copycord"
    )


def test_supervisor_and_logging_agree(native, monkeypatch):
    from copycord import logging_setup

    monkeypatch.setattr(sys, "platform", "linux")
    assert logging_setup.logs_dir() == paths.data_dir()
