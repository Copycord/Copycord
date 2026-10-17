"""Tests for --json on the reporting commands. No network."""
import json
import socket

import pytest

from copycord.common.config import CURRENT_VERSION
from copycord import __main__ as cli


def _unset(monkeypatch, *names):
    # setenv first so monkeypatch restores the original even when the code sets it again.
    for name in names:
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    d = tmp_path / "data"
    monkeypatch.setenv("DATA_DIR", str(d))
    _unset(monkeypatch, "DB_PATH", "ROLE", "BACKUP_DIR", "PASSWORD", "SERVER_TOKEN",
           "CLIENT_TOKEN", "NO_COLOR")
    # Color would be on without --json; --json must turn it off.
    monkeypatch.setenv("FORCE_COLOR", "1")
    monkeypatch.setenv("COPYCORD_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr(cli, "service_state", lambda: None)
    return d


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def doctor_env(data_dir, monkeypatch):
    """A doctor run that passes: a .env, a free port, a password, no import checks."""
    data_dir.mkdir(parents=True)
    (data_dir / ".env").write_text("TZ=UTC\n", encoding="utf-8")
    monkeypatch.setenv("ADMIN_PORT", str(_free_port()))
    monkeypatch.setenv("PASSWORD", "not-the-default")
    monkeypatch.setattr(cli, "REQUIRED_IMPORTS", ())
    monkeypatch.setattr(cli, "check_frontend", lambda: cli.Check(cli.PASS, "web UI included"))
    return data_dir


def _doc(capsys):
    """The one JSON document on stdout; fails on anything else there."""
    captured = capsys.readouterr()
    assert "\x1b[" not in captured.out
    return json.loads(captured.out), captured.err


def _check(doc, text):
    return next(c for c in doc["checks"] if text in c["text"])


# --------------------------------------------------------------------------
# --version
# --------------------------------------------------------------------------


@pytest.mark.parametrize("argv", [["--version", "--json"], ["--json", "-V"]])
def test_version_json(argv, data_dir, monkeypatch, capsys):
    from copycord import update

    monkeypatch.setattr(update, "installed_commit", lambda: "7cf48660c2cd76d8a4bf5aac5b427904a2dcd86b")
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 0
    doc, _ = _doc(capsys)
    assert doc == {
        "version": CURRENT_VERSION,
        "commit": "7cf48660c2cd76d8a4bf5aac5b427904a2dcd86b",
        "channel": "stable",
        "ref": None,
        "repo": None,
    }


@pytest.mark.parametrize("argv", [
    ["run", "--json"],
    ["--json", "run"],
    ["config", "set", "A", "1", "--json"],
    ["update", "--json"],
    ["--json", "service", "stop"],
])
def test_json_only_on_reporting_commands(argv, data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "--json" in captured.err


# --------------------------------------------------------------------------
# doctor
# --------------------------------------------------------------------------


def test_doctor_json_all_good(doctor_env, capsys):
    assert cli.main(["doctor", "--json", "--data-dir", str(doctor_env)]) == 0
    doc, _ = _doc(capsys)
    assert doc["ok"] is True
    assert doc["fixes"] == []
    sections = [c["section"] for c in doc["checks"]]
    assert list(dict.fromkeys(sections)) == ["Install", "Settings", "Data", "Running", "Updates"]
    for c in doc["checks"]:
        assert set(c) == {"section", "status", "text", "hint", "path", "fixable", "fixed"}
        assert c["status"] in ("ok", "info")
        assert c["fixed"] is False
    summary = doc["summary"]
    assert summary["text"] == "All good"
    assert summary["warn"] == summary["fail"] == summary["fixed"] == 0
    assert summary["ok"] + summary["info"] == len(doc["checks"])
    home = _check(doc, "home writable")
    assert home["path"] == str(doctor_env.resolve())
    assert _check(doc, "database created on first run")["status"] == "info"


def test_doctor_json_failure_keeps_the_exit_code(doctor_env, monkeypatch, capsys):
    monkeypatch.setenv("ROLE", "server")
    assert cli.main(["doctor", "--json", "--data-dir", str(doctor_env)]) == 1
    doc, _ = _doc(capsys)
    assert doc["ok"] is False
    role = _check(doc, "ROLE is set")
    assert (role["section"], role["status"]) == ("Settings", "fail")
    assert role["hint"].startswith("Copycord v4 removed the ROLE option.")
    assert (doc["summary"]["fail"], doc["summary"]["text"]) == (1, "0 warnings, 1 problem")


def test_doctor_json_names_the_fix(doctor_env, capsys):
    env = doctor_env / ".env"
    env.unlink()
    assert cli.main(["doctor", "--json", "--data-dir", str(doctor_env)]) == 0
    doc, _ = _doc(capsys)
    missing = _check(doc, ".env not found")
    assert missing["status"] == "warn" and missing["fixable"] is True
    assert missing["hint"] == "run copycord doctor --fix to create it with the default settings"
    assert missing["path"] == str(env.resolve())
    assert not env.exists()


def test_doctor_fix_json_marks_the_fixed_checks(doctor_env, capsys):
    env = doctor_env / ".env"
    env.unlink()
    assert cli.main(["doctor", "--fix", "--json", "--data-dir", str(doctor_env)]) == 0
    doc, _ = _doc(capsys)
    assert env.is_file()
    fixed = [c for c in doc["checks"] if c["fixed"]]
    assert [(c["section"], c["status"], c["text"]) for c in fixed] == [
        ("Settings", "ok", ".env created with the default settings")]
    assert fixed[0]["hint"] == str(env.resolve())
    assert doc["fixes"] == [{"fix": "env_file", "text": ".env created with the default settings",
                             "result": str(env.resolve())}]
    assert doc["summary"]["fixed"] == 1 and doc["ok"] is True


def test_doctor_fix_json_never_asks(doctor_env, monkeypatch, capsys):
    from copycord import ui

    def no_prompt(*args, **kwargs):
        raise AssertionError("--json asked a question")

    monkeypatch.setattr(ui, "is_interactive", lambda: True)
    monkeypatch.setattr(ui, "confirm", no_prompt)
    monkeypatch.setattr("builtins.input", no_prompt)
    monkeypatch.setattr(cli, "copycord_on_port", lambda p, timeout=1.0: False)
    with socket.socket() as s:
        s.bind((cli.ADMIN_BIND_HOST, 0))
        s.listen(1)
        port = s.getsockname()[1]
        monkeypatch.setenv("ADMIN_PORT", str(port))
        assert cli.main(["doctor", "--fix", "--json", "--data-dir", str(doctor_env)]) == 0
    doc, _ = _doc(capsys)
    taken = _check(doc, f"port {port} in use")
    assert taken["fixed"] is False
    assert "in a terminal" in taken["hint"]


def test_doctor_fix_json_sends_fix_output_to_stderr(doctor_env, monkeypatch, capsys):
    def noisy_fix(checks):
        print("working on it")
        return "done"

    monkeypatch.setitem(cli.FIXES, "env_file", cli.FIXES["env_file"]._replace(apply=noisy_fix))
    (doctor_env / ".env").unlink()
    cli.main(["doctor", "--fix", "--json", "--data-dir", str(doctor_env)])
    doc, err = _doc(capsys)
    assert "working on it" in err
    assert doc["fixes"][0]["result"] == "done"


# --------------------------------------------------------------------------
# update status
# --------------------------------------------------------------------------


@pytest.fixture()
def status_env(data_dir, monkeypatch):
    from copycord import update, update_check

    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    monkeypatch.setattr(update, "installed_commit", lambda: None)
    monkeypatch.setattr(update, "detect_installer", lambda: update.UV_TOOL)
    monkeypatch.setattr(update_check, "last_check_time", lambda: 0.0)

    def spinner(*args, **kwargs):
        raise AssertionError("--json showed a spinner")

    monkeypatch.setattr(update.ui, "Spinner", spinner)
    return data_dir


def test_update_status_json(status_env, monkeypatch, capsys):
    from copycord import update

    release = update.Release("v4.1.0", "4.1.0", False, "https://example.invalid/copycord.whl")
    monkeypatch.setattr(update, "fetch_latest_release", lambda prerelease, timeout=0: release)
    assert cli.main(["update", "status", "--json"]) == 0
    doc, _ = _doc(capsys)
    assert doc == {
        "current": "4.0.0",
        "commit": None,
        "channel": "stable",
        "ref": None,
        "repo": None,
        "method": update.UV_TOOL,
        "last_checked": "1970-01-01T00:00:00Z",
        "auto_update": False,
        "latest": "4.1.0",
        "latest_tag": "v4.1.0",
        "update_available": True,
        "error": None,
    }


def test_update_status_json_without_github(status_env, monkeypatch, capsys):
    from copycord import update

    def offline(prerelease, timeout=0):
        raise OSError("network is unreachable")

    monkeypatch.setattr(update, "fetch_latest_release", offline)
    assert cli.main(["update", "status", "--json"]) == 0
    doc, _ = _doc(capsys)
    assert doc["error"] == "network is unreachable"
    assert doc["latest"] is None and doc["update_available"] is None


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------


def test_config_get_json(data_dir, capsys):
    dd = ["--data-dir", str(data_dir)]
    assert cli.main(["config", "set", "COMMAND_USERS", "1,2", *dd]) == 0
    assert cli.main(["config", "set", "SERVER_TOKEN", "srv-secret", *dd]) == 0
    capsys.readouterr()

    assert cli.main(["config", "get", "COMMAND_USERS", "--json", *dd]) == 0
    assert _doc(capsys)[0] == {"key": "COMMAND_USERS", "value": "1,2", "set": True,
                               "masked": False, "source": "database"}
    assert cli.main(["config", "get", "SERVER_TOKEN", "--json", *dd]) == 0
    doc, _ = _doc(capsys)
    assert (doc["value"], doc["masked"]) == (cli.MASK, True)
    assert cli.main(["config", "get", "SERVER_TOKEN", "--json", "--show-secrets", *dd]) == 0
    assert _doc(capsys)[0]["value"] == "srv-secret"

    assert cli.main(["config", "get", "NOPE", "--json", *dd]) == 1
    doc, err = _doc(capsys)
    assert (doc["key"], doc["value"], doc["set"]) == ("NOPE", None, False)
    assert doc["error"] == "NOPE is not set" and "NOPE is not set" in err


def test_config_get_json_env_key(data_dir, monkeypatch, capsys):
    _unset(monkeypatch, "ADMIN_PORT")
    data_dir.mkdir(parents=True)
    (data_dir / ".env").write_text("ADMIN_PORT=9000\n", encoding="utf-8")
    dd = ["--data-dir", str(data_dir)]
    assert cli.main(["config", "get", "ADMIN_PORT", "--json", *dd]) == 0
    assert _doc(capsys)[0] == {"key": "ADMIN_PORT", "value": "9000", "set": True,
                               "masked": False, "source": "env"}
    assert cli.main(["config", "get", "BACKUP_AT", "--json", *dd]) == 1
    doc, _ = _doc(capsys)
    assert doc["set"] is False and doc["source"] == "env" and "not set" in doc["error"]


def test_config_export_json(data_dir, tmp_path, capsys):
    dd = ["--data-dir", str(data_dir)]
    assert cli.main(["config", "set", "SERVER_TOKEN", "srv-secret", *dd]) == 0
    assert cli.main(["config", "set", "COMMAND_USERS", "1,2", *dd]) == 0
    capsys.readouterr()

    assert cli.main(["config", "export", *dd]) == 0
    plain = json.loads(capsys.readouterr().out)
    assert cli.main(["config", "export", "--json", *dd]) == 0
    assert _doc(capsys)[0] == plain

    out = tmp_path / "backup.json"
    assert cli.main(["config", "export", str(out), "--json", *dd]) == 0
    doc, err = _doc(capsys)
    assert doc == {"file": str(out), "keys": len(plain)}
    assert json.loads(out.read_text())["SERVER_TOKEN"] == "srv-secret"
    assert "Exported" in err
