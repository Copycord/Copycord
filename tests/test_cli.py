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
    _unset(monkeypatch, "DB_PATH", "ROLE", "BACKUP_DIR", "PASSWORD", "SERVER_TOKEN", "CLIENT_TOKEN")
    # No network and no real OS service in the doctor tests.
    monkeypatch.setenv("COPYCORD_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr(cli, "service_state", lambda: None)
    return d


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _statuses(out):
    return [line.split()[0] for line in out.strip().splitlines()
            if line.split() and line.split()[0] in ("PASS", "WARN", "FAIL", "INFO")]


def _line(out, text):
    return next(line for line in out.splitlines() if text in line)


@pytest.fixture()
def doctor_env(data_dir, monkeypatch):
    """A doctor run that passes: a .env, a free port, a password, tokens, no import checks."""
    data_dir.mkdir(parents=True)
    (data_dir / ".env").write_text("TZ=UTC\n", encoding="utf-8")
    monkeypatch.setenv("ADMIN_PORT", str(_free_port()))
    monkeypatch.setenv("PASSWORD", "not-the-default")
    monkeypatch.setenv("SERVER_TOKEN", "x")
    monkeypatch.setenv("CLIENT_TOKEN", "y")
    monkeypatch.setattr(cli, "REQUIRED_IMPORTS", ())
    monkeypatch.setattr(cli, "check_frontend", lambda: cli.Check(cli.PASS, "web UI included"))
    return data_dir


# --------------------------------------------------------------------------
# version
# --------------------------------------------------------------------------


def test_version_prints_current_version(data_dir, capsys, monkeypatch):
    from copycord import update

    monkeypatch.setattr(update, "installed_commit", lambda: None)
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines == [CURRENT_VERSION, "channel stable"]


def test_version_prints_short_commit_of_a_git_install(data_dir, capsys, monkeypatch):
    from copycord import update

    monkeypatch.setattr(update, "installed_commit", lambda: "7cf48660c2cd76d8a4bf5aac5b427904a2dcd86b")
    with pytest.raises(SystemExit):
        cli.main(["--version"])
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines == [f"{CURRENT_VERSION} (7cf4866)", "channel stable"]


def test_bare_copycord_prints_the_overview(data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "Usage: copycord <command>" in out and "Examples:" in out


def test_overview_shows_only_what_a_regular_user_needs(data_dir, capsys):
    with pytest.raises(SystemExit):
        cli.main([])
    out = capsys.readouterr().out
    assert "update     Update Copycord to the latest version\n" in out
    assert "uninstall  Remove Copycord; asks whether to delete your data\n" in out
    assert "-V, --version  Print the version\n" in out
    examples = out.split("Examples:\n", 1)[1].split("\n\n", 1)[0].splitlines()
    assert [line.split("  ")[1].strip() for line in examples] == [
        "copycord run",
        "copycord service install",
        "copycord update",
        "copycord doctor",
        "copycord config set PASSWORD 'new-pass'",
    ]
    for advanced in ("--channel", "beta", "--ref", "--repo", "update status"):
        assert advanced not in out


def test_unknown_command_is_one_styled_line(data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["frobnicate"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "unknown command 'frobnicate'" in err and "copycord --help" in err
    assert "usage:" not in err


@pytest.mark.parametrize("group", ("service", "config"))
def test_command_group_without_subcommand_prints_its_help(group, data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main([group])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert f"Usage: copycord {group} <command> [options]" in out
    assert "Commands:" in out and "Examples:" in out


@pytest.mark.parametrize("command", ("config", "service", "update", "uninstall", "doctor", "run", "restart"))
def test_command_help_is_styled_with_examples(command, data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main([command, "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert out.startswith(f"copycord {command}\n")
    assert f"Usage: copycord {command}" in out
    assert "Options:" in out and "-h, --help" in out
    assert "Examples:" in out and f"  copycord {command}" in out
    assert "\033[" not in out and "usage:" not in out


def test_update_help_keeps_channels_in_the_advanced_section(data_dir, capsys):
    with pytest.raises(SystemExit):
        cli.main(["update", "--help"])
    out = capsys.readouterr().out
    assert "Usage: copycord update [status] [options]" in out
    regular, advanced = out.split("Advanced (channels and git installs):\n", 1)
    assert "--tag VERSION" in regular and "-y, --yes" in regular and "--no-backup" in regular
    for flag in ("--channel", "--ref", "--repo", "--no-install", "beta"):
        assert flag not in regular
    assert "--channel stable|beta|dev" in advanced
    assert "--ref REF" in advanced and "--repo OWNER/NAME" in advanced and "--no-install" in advanced
    assert "copycord update --channel beta" in advanced
    assert "copycord update --channel dev --ref v4-dev" in advanced


def test_command_help_is_colored_on_a_tty(data_dir, capsys, monkeypatch):
    monkeypatch.setenv("FORCE_COLOR", "1")
    _unset(monkeypatch, "NO_COLOR")
    with pytest.raises(SystemExit):
        cli.main(["service", "--help"])
    assert "\033[" in capsys.readouterr().out
    monkeypatch.setenv("NO_COLOR", "1")
    with pytest.raises(SystemExit):
        cli.main(["service", "--help"])
    assert "\033[" not in capsys.readouterr().out


def test_subcommand_mistake_is_one_styled_line(data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["update", "--channel", "nightly"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert err.startswith("FAIL") and "copycord update --help" in err


@pytest.mark.parametrize("argv", (["version"], ["update", "--check"], ["update", "--branch", "main"], ["onboard"]))
def test_removed_commands_and_flags(argv, data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2


# --------------------------------------------------------------------------
# uninstall
# --------------------------------------------------------------------------


@pytest.fixture()
def uninstall_env(data_dir, monkeypatch):
    """A data dir with a database; the service and program steps are stubbed."""
    data_dir.mkdir(parents=True)
    (data_dir / "data.db").write_text("db", encoding="utf-8")
    monkeypatch.setattr(cli, "_uninstall_service", lambda dry_run, removed, kept: True)
    monkeypatch.setattr(cli, "_uninstall_program", lambda dry_run, purge, removed, kept: True)
    monkeypatch.setenv("NO_COLOR", "1")
    return data_dir


def _answers(monkeypatch, *replies):
    """Feed replies to the prompts; record each prompt asked."""
    from copycord import ui

    asked = []
    queue = list(replies)

    def fake_input(prompt=""):
        asked.append(prompt)
        return queue.pop(0)

    monkeypatch.setattr(ui, "is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", fake_input)
    return asked


def test_uninstall_asks_and_deletes_the_data(uninstall_env, monkeypatch, capsys):
    asked = _answers(monkeypatch, "y", "y", "DELETE")
    assert cli.main(["uninstall"]) == 0
    assert not uninstall_env.exists()
    assert len(asked) == 3
    assert "Also delete your Copycord data (database, logs, backups, settings)" in asked[1]
    assert str(uninstall_env) in asked[1] and "This cannot be undone." in asked[1]
    assert "(y/N)" in asked[1]
    assert "Type DELETE" in asked[2]
    assert f"Copycord home: {uninstall_env}" in capsys.readouterr().out


def test_uninstall_asks_and_keeps_the_data_by_default(uninstall_env, monkeypatch, capsys):
    asked = _answers(monkeypatch, "y", "")
    assert cli.main(["uninstall"]) == 0
    assert (uninstall_env / "data.db").exists()
    assert len(asked) == 2
    out = capsys.readouterr().out
    assert "keeping your data" in out
    kept = out.split("Kept", 1)[1]
    assert f"Copycord home: {uninstall_env}" in kept


def test_uninstall_data_yes_without_delete_changes_nothing(uninstall_env, monkeypatch, capsys):
    _answers(monkeypatch, "y", "y", "delete")
    assert cli.main(["uninstall"]) == 1
    assert (uninstall_env / "data.db").exists()
    assert "DELETE was not typed" in capsys.readouterr().err


def test_uninstall_data_yes_refuses_the_home_directory(uninstall_env, monkeypatch, capsys):
    monkeypatch.setenv("DATA_DIR", str(Path.home()))
    _answers(monkeypatch, "y", "y")
    assert cli.main(["uninstall"]) == 1
    err = capsys.readouterr().err
    assert "refusing to delete the Copycord home" in err and "home directory" in err


def test_uninstall_without_a_terminal_keeps_the_data(uninstall_env, monkeypatch, capsys):
    from copycord import ui

    monkeypatch.setattr(ui, "is_interactive", lambda: False)
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("prompted without a terminal"))
    assert cli.main(["uninstall", "--yes"]) == 0
    assert (uninstall_env / "data.db").exists()
    out = capsys.readouterr().out
    assert f"Copycord home: {uninstall_env}" in out.split("Kept", 1)[1]


def test_uninstall_without_a_terminal_needs_yes(uninstall_env, monkeypatch, capsys):
    from copycord import ui

    monkeypatch.setattr(ui, "is_interactive", lambda: False)
    assert cli.main(["uninstall"]) == 1
    assert (uninstall_env / "data.db").exists()
    assert "rerun with --yes" in capsys.readouterr().err


def test_uninstall_purge_deletes_without_asking(uninstall_env, monkeypatch, capsys):
    from copycord import ui

    monkeypatch.setattr(ui, "is_interactive", lambda: False)
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("prompted with --purge --yes"))
    assert cli.main(["uninstall", "--yes", "--purge"]) == 0
    assert not uninstall_env.exists()


def test_uninstall_purge_interactive_skips_the_question(uninstall_env, monkeypatch):
    asked = _answers(monkeypatch, "y", "DELETE")
    assert cli.main(["uninstall", "--purge"]) == 0
    assert not uninstall_env.exists()
    assert not any("Also delete" in a for a in asked)


def test_uninstall_keep_data_does_not_ask(uninstall_env, monkeypatch, capsys):
    asked = _answers(monkeypatch, "y")
    assert cli.main(["uninstall", "--keep-data"]) == 0
    assert (uninstall_env / "data.db").exists()
    assert len(asked) == 1


def test_uninstall_purge_and_keep_data_conflict(uninstall_env, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["uninstall", "--purge", "--keep-data"])
    assert exc.value.code == 2


def test_uninstall_help_mentions_the_data_question(data_dir, capsys):
    with pytest.raises(SystemExit):
        cli.main(["uninstall", "--help"])
    out = capsys.readouterr().out
    assert "--keep-data" in out and "--purge" in out
    assert "asks whether to delete your data" in out
    with pytest.raises(SystemExit):
        cli.main(["--help"])
    assert "Remove Copycord; asks whether to delete your data" in capsys.readouterr().out


# --------------------------------------------------------------------------
# doctor
# --------------------------------------------------------------------------


def test_doctor_all_good(doctor_env, capsys):
    data_dir = doctor_env
    assert cli.main(["doctor", "--data-dir", str(data_dir)]) == 0

    out = capsys.readouterr().out
    lines = out.splitlines()
    for title in ("Install", "Settings", "Data", "Running", "Updates"):
        assert title in lines
    assert set(_statuses(out)) <= {"PASS", "INFO"}
    assert lines[-1] == "All good"
    assert "PASS  home writable" in lines
    assert "PASS  .env found" in lines
    assert "PASS  dependencies OK" in lines
    assert "INFO  database created on first run" in lines
    assert "INFO  no backups yet" in lines
    assert any(line.startswith("PASS  Copycord v") and ", channel stable" in line for line in lines)
    # No ROLE line and no paths when nothing is wrong.
    assert "ROLE" not in out
    assert str(data_dir.resolve()) not in out
    # Read-only checks: nothing created next to the .env.
    assert os.listdir(data_dir) == [".env"]


def test_doctor_verbose_shows_paths(doctor_env, capsys):
    data_dir = doctor_env
    assert cli.main(["doctor", "--verbose", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert f"PASS  home writable: {data_dir.resolve()}" in out
    assert f"PASS  .env found: {data_dir.resolve() / '.env'}" in out


def test_doctor_role_set_fails(doctor_env, monkeypatch, capsys):
    monkeypatch.setenv("ROLE", "server")

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 1

    out = capsys.readouterr().out
    role = _line(out, "ROLE is set")
    assert role.startswith("FAIL")
    assert role.startswith("FAIL  ROLE is set: Copycord v4 removed the ROLE option.")
    assert "one container" not in role
    assert out.splitlines()[-1] == "0 warnings, 1 problem"


def test_doctor_default_password_warns(doctor_env, monkeypatch, capsys):
    monkeypatch.setenv("PASSWORD", "copycord")

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 0

    out = capsys.readouterr().out
    line = _line(out, "dashboard password")
    assert line.startswith("WARN  dashboard password is the default")
    assert "copycord config set PASSWORD" in line and "copycord restart" in line
    assert out.splitlines()[-1] == "1 warning, 0 problems"


def test_doctor_missing_tokens_warn_without_values(doctor_env, monkeypatch, capsys):
    from copycord.common.db import DBManager

    _unset(monkeypatch, "CLIENT_TOKEN")
    monkeypatch.setenv("SERVER_TOKEN", "")
    monkeypatch.setenv("ADMIN_PORT", str(_free_port()))
    db = DBManager(str(doctor_env / "data.db"), init_schema=True)
    db.set_config("SERVER_TOKEN", "secret-bot-token")
    db.conn.close()

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 0

    out = capsys.readouterr().out
    line = _line(out, "not set yet")
    port = os.environ["ADMIN_PORT"]
    # The DB value counts, as in the app; only CLIENT_TOKEN is missing.
    assert line == f"WARN  CLIENT_TOKEN not set yet: finish setup at http://localhost:{port}"
    assert "secret-bot-token" not in out


def test_check_tokens_both_missing(data_dir):
    check = cli.check_tokens({"ADMIN_PORT": "9000"})
    assert check.status == cli.WARN
    assert check.name == "SERVER_TOKEN and CLIENT_TOKEN not set yet"
    assert check.reason == "finish setup at http://localhost:9000"
    assert cli.check_tokens({"SERVER_TOKEN": "a", "CLIENT_TOKEN": "b"}).status == cli.PASS


def test_check_env_file(tmp_path):
    missing = cli.check_env_file(tmp_path / ".env")
    assert missing.status == cli.WARN and missing.path == str(tmp_path / ".env")
    (tmp_path / ".env").write_text("PASSWORD=x\n")
    assert cli.check_env_file(tmp_path / ".env").status == cli.PASS


def test_doctor_newer_schema_fails(doctor_env, capsys):
    from copycord.common.db import SCHEMA_VERSION, SCHEMA_VERSION_KEY, DBManager

    db = DBManager(str(doctor_env / "data.db"), init_schema=True)
    db.set_config(SCHEMA_VERSION_KEY, str(SCHEMA_VERSION + 1))
    db.conn.close()

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 1

    out = capsys.readouterr().out
    line = _line(out, "database schema")
    assert line.startswith(f"FAIL  database schema {SCHEMA_VERSION + 1} is newer")
    assert str(doctor_env.resolve() / "data.db") in line
    assert out.splitlines()[-1].endswith("1 problem")


def test_check_backups(tmp_path):
    import time

    now = time.time()
    backups, db = tmp_path / "backups", tmp_path / "data.db"
    # Fresh install: no DB, no backups.
    assert cli.check_backups(backups, db, now).status == cli.INFO

    backups.mkdir()
    db.write_bytes(b"")
    # A new backups folder with nothing in it yet is still a new install.
    assert cli.check_backups(backups, db, now).status == cli.INFO
    # Three days on and still nothing: a warning.
    none = cli.check_backups(backups, db, now + 3 * 86400)
    assert (none.status, none.name) == (cli.WARN, "no backups yet")

    old = backups / "20260101-031700.tar.gz"
    old.write_bytes(b"x")
    os.utime(old, (now - 3 * 86400, now - 3 * 86400))
    stale = cli.check_backups(backups, db, now)
    assert stale.status == cli.WARN
    assert stale.name == "last backup 3 days ago"
    assert "BACKUP_AT" in stale.reason

    new = backups / "data-pre-update-4.0.0-20260102T000000Z.db"
    new.write_bytes(b"x")
    os.utime(new, (now - 5 * 3600, now - 5 * 3600))
    recent = cli.check_backups(backups, db, now)
    assert recent.status == cli.PASS and recent.name == "last backup 5 hours ago"


def test_doctor_stale_backup_warns(doctor_env, monkeypatch, capsys):
    import time

    from copycord.common.db import DBManager

    DBManager(str(doctor_env / "data.db"), init_schema=True).conn.close()
    archive = doctor_env / "backups" / "20260101-031700.tar.gz"
    archive.parent.mkdir()
    archive.write_bytes(b"x")
    past = time.time() - 10 * 86400
    os.utime(archive, (past, past))

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 0

    out = capsys.readouterr().out
    assert _line(out, "last backup").startswith("WARN  last backup 10 days ago")
    assert str(archive.parent.resolve()) in _line(out, "last backup")


def test_doctor_missing_dependency_names_only_it(doctor_env, monkeypatch, capsys):
    monkeypatch.setattr(
        cli, "REQUIRED_IMPORTS", (("json", "pytest"), ("no_such_module_copycord", "nothing-dist"))
    )

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 1

    out = capsys.readouterr().out
    line = _line(out, "missing dependencies")
    assert line.startswith("FAIL  missing dependencies: nothing-dist:")
    assert "pytest" not in line
    assert out.splitlines()[-1] == "0 warnings, 1 problem"


def test_doctor_port_in_use(doctor_env, monkeypatch, capsys):
    with socket.socket() as s:
        s.bind((cli.ADMIN_BIND_HOST, 0))
        s.listen(1)
        port = s.getsockname()[1]
        monkeypatch.setenv("ADMIN_PORT", str(port))

        check = cli.check_admin_port(probe=lambda p: False)
        assert check.status == cli.WARN
        assert check.name == f"port {port} in use"
        assert "ADMIN_PORT" in check.reason

        ours = cli.check_admin_port(probe=lambda p: p == port)
        assert (ours.status, ours.name) == (cli.PASS, f"port {port} in use by Copycord")
        running = cli.check_admin_port(service_running=True, probe=lambda p: False)
        assert running.status == cli.PASS

        monkeypatch.setattr(cli, "copycord_on_port", lambda p, timeout=1.0: False)
        assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 0
    out = capsys.readouterr().out
    assert f"WARN  port {port} in use" in out
    assert out.splitlines()[-1] == "1 warning, 0 problems"


def test_check_admin_port_free_and_invalid():
    check = cli.check_admin_port({"ADMIN_PORT": str(_free_port())})
    assert check.status == cli.PASS and check.name.endswith(" free")
    assert cli.check_admin_port({"ADMIN_PORT": "nope"}).status == cli.FAIL
    assert cli.check_admin_port({"ADMIN_PORT": "70000"}).status == cli.FAIL


def test_check_service():
    from copycord.service import State

    assert cli.check_service(None).status == cli.INFO
    assert cli.check_service(State(False, False, "x")).status == cli.INFO
    assert cli.check_service(State(True, True, "x")).status == cli.PASS
    stopped = cli.check_service(State(True, False, "x"))
    assert stopped.status == cli.WARN and "copycord service start" in stopped.reason


def test_check_updates_never_fails(data_dir, monkeypatch):
    from copycord import update_check

    _unset(monkeypatch, "COPYCORD_NO_UPDATE_CHECK")

    def offline(**_):
        raise OSError("network is unreachable")

    monkeypatch.setattr(update_check, "check_cached", offline)
    check = cli.check_updates()
    assert check.status == cli.INFO and "network is unreachable" in check.reason

    result = update_check.Result(True, "4.0.0", "4.1.0", 0.0)
    monkeypatch.setattr(update_check, "check_cached", lambda **_: result)
    check = cli.check_updates()
    assert check.status == cli.INFO and check.name == "update available: v4.1.0 (you have v4.0.0)"

    monkeypatch.setattr(update_check, "check_cached", lambda **_: result._replace(available=False))
    assert cli.check_updates().status == cli.PASS


def test_doctor_summary():
    ok, warn, fail = cli.Check(cli.PASS, "a"), cli.Check(cli.WARN, "b"), cli.Check(cli.FAIL, "c")
    assert cli.doctor_summary([ok, cli.Check(cli.INFO, "i")]) == "All good"
    assert cli.doctor_summary([ok, warn, warn, fail]) == "2 warnings, 1 problem"
    assert cli.doctor_summary([fail, fail]) == "0 warnings, 2 problems"


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
    from copycord.common.db import SCHEMA_VERSION, DBManager

    assert cli.check_db(tmp_path / "missing.db").status == cli.INFO

    bad = tmp_path / "bad.db"
    bad.write_bytes(b"this is not a sqlite database" * 200)
    assert cli.check_db(bad).status == cli.FAIL

    good = tmp_path / "good.db"
    DBManager(str(good), init_schema=True).conn.close()
    check = cli.check_db(good)
    assert check.status == cli.PASS
    assert check.name == f"database OK at schema {SCHEMA_VERSION}"


def test_check_frontend(tmp_path):
    missing = cli.check_frontend(tmp_path)
    assert missing.status == cli.FAIL and missing.name == "web UI missing"
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "main.js").write_text("")
    assert cli.check_frontend(tmp_path).status == cli.PASS


def test_check_import():
    assert cli.check_import("json", "pytest").status == cli.PASS
    assert cli.check_import("no_such_module_copycord", "nothing").status == cli.FAIL
    assert cli.check_dependencies((("json", "pytest"),)).name == "dependencies OK"


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
