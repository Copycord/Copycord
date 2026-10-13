"""Tests for the copycord CLI and the data-dir resolution. No network."""
import io
import json
import os
import socket
import subprocess
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
    monkeypatch.setattr(cli, "service_linger", lambda: None)
    return d


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


SYMBOLS = {"✓": "PASS", "!": "WARN", "✗": "FAIL", "•": "INFO"}
ALL_GOOD = "✓ All good  start it with copycord run"


def _statuses(out):
    return [SYMBOLS[line.split()[0]] for line in out.strip().splitlines()
            if line.split() and line.split()[0] in SYMBOLS]


def _line(out, text):
    return next(line for line in out.splitlines() if text in line)


FIX_HINT = "Run copycord doctor --fix to fix {} of them."


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
    assert lines == [CURRENT_VERSION, "stable channel"]


def test_version_prints_short_commit_of_a_git_install(data_dir, capsys, monkeypatch):
    from copycord import update

    monkeypatch.setattr(update, "installed_commit", lambda: "7cf48660c2cd76d8a4bf5aac5b427904a2dcd86b")
    with pytest.raises(SystemExit):
        cli.main(["--version"])
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines == [f"{CURRENT_VERSION} (7cf4866)", "stable channel"]


def test_bare_copycord_prints_the_overview(data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "Usage: copycord <command>" in out and "Examples:" in out


def test_overview_shows_the_home(data_dir, monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("COPYCORD_HOME", str(tmp_path / "cc-home"))
    with pytest.raises(SystemExit):
        cli.main([])
    assert f"Home: {tmp_path / 'cc-home'}\n" in capsys.readouterr().out


def _local_links(monkeypatch):
    """Windows Terminal on this machine, so paths print as file:// links."""
    from copycord import ui

    _unset(monkeypatch, "NO_COLOR", "FORCE_COLOR", "TERM", "TMUX", "STY",
           "SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY")
    monkeypatch.setenv("WT_SESSION", "c0ffee")
    monkeypatch.setattr(paths, "in_docker", lambda: False)
    return lambda path: f"\033]8;;{ui.file_uri(path)}\033\\{path}\033]8;;\033\\"


def test_overview_links_the_home(data_dir, monkeypatch, tmp_path):
    from copycord import ui

    link = _local_links(monkeypatch)
    home = tmp_path / "cc-home"
    monkeypatch.setenv("COPYCORD_HOME", str(home))
    monkeypatch.setattr(sys, "stdout", _Terminal())
    with pytest.raises(SystemExit):
        cli.main([])
    out = sys.stdout.getvalue()
    assert f" {link(home)}\n" in out
    assert f"  Home: {home}\n" in ui._ANSI.sub("", out)


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
    assert "Did you mean" not in err


def test_unknown_command_suggests_a_close_one(data_dir, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["updte"])
    assert exc.value.code == 2
    err = capsys.readouterr().err.splitlines()
    assert err == ["✗ unknown command 'updte'. Did you mean 'update'?",
                   "  Run copycord --help to see all commands."]


def test_no_color_flag_turns_color_off(data_dir, monkeypatch, capsys):
    from copycord import update

    monkeypatch.setenv("NO_COLOR", "")
    monkeypatch.setenv("FORCE_COLOR", "1")
    monkeypatch.setattr(update, "installed_commit", lambda: None)
    with pytest.raises(SystemExit):
        cli.main(["--no-color", "--version"])
    assert os.environ["NO_COLOR"] == "1"
    out = capsys.readouterr().out
    assert "\033[" not in out and out.splitlines() == [CURRENT_VERSION, "stable channel"]
    assert cli._take_no_color(["doctor", "--no-color", "--", "--no-color"]) == ["doctor", "--", "--no-color"]


def test_symbols_fall_back_to_ascii(monkeypatch):
    import io

    from copycord import ui

    monkeypatch.setenv("NO_COLOR", "1")
    for enc, want in (("utf-8", "✓ done  x\n✗ bad\n"), ("cp1252", "+ done  x\nx bad\n")):
        buf = io.TextIOWrapper(io.BytesIO(), encoding=enc)
        ui.ok("done", "x", stream=buf)
        ui.fail("bad", stream=buf)
        buf.seek(0)
        assert buf.read() == want


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
    assert err.startswith("✗") and "copycord update --help" in err


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
    monkeypatch.setattr(cli, "_uninstall_program", lambda dry_run, purge, removed, kept, *a: True)
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


def test_uninstall_asks_one_question_and_deletes_the_data(uninstall_env, monkeypatch, capsys):
    asked = _answers(monkeypatch, "2")
    assert cli.main(["uninstall"]) == 0
    assert not uninstall_env.exists()
    assert len(asked) == 1
    out = capsys.readouterr().out
    assert "Removes the Copycord program and its background service." in out
    assert f"Your data is in {uninstall_env}" in out
    assert "1) Keep my data (default)" in out
    assert "2) Delete everything (database, logs, backups, settings)" in out
    assert "3) Cancel" in out
    assert "Copycord uninstalled" in out and "Your data is still in" not in out


def test_uninstall_keeps_the_data_on_enter(uninstall_env, monkeypatch, capsys):
    asked = _answers(monkeypatch, "")
    assert cli.main(["uninstall"]) == 0
    assert (uninstall_env / "data.db").exists()
    assert len(asked) == 1
    out = capsys.readouterr().out
    assert "Copycord uninstalled" in out
    assert f"Your data is still in {uninstall_env}" in out


def test_uninstall_cancel_changes_nothing(uninstall_env, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_uninstall_service", lambda *a: pytest.fail("ran after cancel"))
    _answers(monkeypatch, "3")
    assert cli.main(["uninstall"]) == 1
    assert (uninstall_env / "data.db").exists()
    assert "nothing was changed" in capsys.readouterr().err


def test_uninstall_asks_again_on_a_bad_answer(uninstall_env, monkeypatch):
    asked = _answers(monkeypatch, "x", "1")
    assert cli.main(["uninstall"]) == 0
    assert (uninstall_env / "data.db").exists()
    assert len(asked) == 2


def test_uninstall_delete_refuses_the_home_directory(uninstall_env, monkeypatch, capsys):
    monkeypatch.setenv("DATA_DIR", str(Path.home()))
    _answers(monkeypatch, "2")
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
    assert f"Your data is still in {uninstall_env}" in out


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


def test_uninstall_purge_interactive_offers_delete_or_cancel(uninstall_env, monkeypatch, capsys):
    _answers(monkeypatch, "1")
    assert cli.main(["uninstall", "--purge"]) == 0
    assert not uninstall_env.exists()
    out = capsys.readouterr().out
    assert "1) Delete everything" in out and "2) Cancel" in out and "Keep my data" not in out


def test_uninstall_purge_enter_cancels(uninstall_env, monkeypatch, capsys):
    _answers(monkeypatch, "")
    assert cli.main(["uninstall", "--purge"]) == 1
    assert (uninstall_env / "data.db").exists()


def test_uninstall_keep_data_does_not_ask(uninstall_env, monkeypatch, capsys):
    asked = _answers(monkeypatch, "")
    assert cli.main(["uninstall", "--keep-data"]) == 0
    assert (uninstall_env / "data.db").exists()
    assert len(asked) == 1
    out = capsys.readouterr().out
    assert "1) Keep my data" in out and "Delete everything" not in out


class _Terminal(io.StringIO):
    encoding = "utf-8"

    def isatty(self):
        return True


def _terminal(monkeypatch):
    """Put stdout and stderr on a terminal, so the rail is shown. Called in the
    test itself: pytest puts its own streams back after the fixtures ran."""
    out, err = _Terminal(), _Terminal()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", err)
    return out, err


def _removed_program(dry_run, purge, removed, kept, *a):
    from copycord import ui

    ui.ok("removed the copycord program")
    return True


def test_uninstall_on_a_terminal_runs_on_the_rail(uninstall_env, monkeypatch):
    data, (out, _err) = uninstall_env, _terminal(monkeypatch)
    monkeypatch.setattr(cli, "_uninstall_program", _removed_program)
    asked = _answers(monkeypatch, "")
    assert cli.main(["uninstall"]) == 0
    assert asked == ["│  Enter 1, 2 or 3 [1] "]
    assert out.getvalue().splitlines() == [
        "",
        "┌  Uninstall Copycord",
        "│  Removes the Copycord program and its background service.",
        "│",
        f"◆  Your data is in {data}",
        "│  1) Keep my data (default)",
        "│  2) Delete everything (database, logs, backups, settings)",
        "│  3) Cancel",
        "│",
        "✓  removed the copycord program",
        "│",
        "✓  Copycord uninstalled",
        f"└  Your data is still in {data}",
    ]


def test_uninstall_on_a_terminal_links_the_data(uninstall_env, monkeypatch):
    from copycord import ui

    data, link = uninstall_env, _local_links(monkeypatch)
    out, _err = _terminal(monkeypatch)
    _answers(monkeypatch, "")
    assert cli.main(["uninstall"]) == 0
    assert f"Your data is in {link(data)}" in out.getvalue()
    assert f"Your data is still in {link(data)}" in out.getvalue()
    plain = ui._ANSI.sub("", out.getvalue()).splitlines()
    assert f"\u25c6  Your data is in {data}" in plain
    assert plain[-1] == f"\u2514  Your data is still in {data}"


def test_uninstall_cancel_on_a_terminal_closes_the_rail(uninstall_env, monkeypatch):
    data, (out, err) = uninstall_env, _terminal(monkeypatch)
    monkeypatch.setattr(cli, "_uninstall_service", lambda *a: pytest.fail("ran after cancel"))
    _answers(monkeypatch, "3")
    assert cli.main(["uninstall"]) == 1
    assert (data / "data.db").exists()
    assert out.getvalue().splitlines()[-3:] == ["│  3) Cancel", "│", "└"]
    assert err.getvalue() == "!  uninstall cancelled  nothing was changed\n"


def test_uninstall_without_a_terminal_has_no_rail(uninstall_env, capsys):
    assert cli.main(["uninstall", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("\nUninstall Copycord (dry run, nothing changes)\n"
                          "Removes the Copycord program and its background service.\n\n")
    assert not any(glyph in out for glyph in "┌│◆◇└")
    assert out.endswith(f" Dry run: nothing changed\n  Your data is still in {uninstall_env}\n")


def test_uninstall_purge_and_keep_data_conflict(uninstall_env, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["uninstall", "--purge", "--keep-data"])
    assert exc.value.code == 2


@pytest.fixture()
def win_uninstall(data_dir, tmp_path, monkeypatch):
    """Windows uninstall with the service, processes, files and uv recorded."""
    import shutil
    from copycord import service, winupdate

    data_dir.mkdir(parents=True)
    calls = []
    monkeypatch.setattr(cli.sys, "platform", "win32")
    monkeypatch.setattr(winupdate, "running", lambda: calls.append("running") or (["C:/bin/copycord.exe"], None))
    monkeypatch.setattr(cli, "_uninstall_service",
                        lambda dry_run, removed, kept: calls.append("service") or True)
    monkeypatch.setattr(winupdate, "clean_old", lambda home, launchers: calls.append("clean"))
    monkeypatch.setattr(winupdate, "scratch_dir", lambda prefix: tmp_path / "scratch")
    moved = [(Path("C:/env/python.exe"), tmp_path / "scratch" / ".copycord-old" / "1" / "0-python.exe")]

    def move_aside(prefix, launchers, home):
        calls.append(("move", launchers, home))
        return moved

    monkeypatch.setattr(winupdate, "move_aside", move_aside)
    monkeypatch.setattr(winupdate, "restore", lambda m: calls.append(("restore", m)))
    monkeypatch.setattr(winupdate, "delete_after_exit", lambda folders: calls.append(("delete", folders)))
    monkeypatch.setattr(shutil, "which", lambda name: "uv" if name == "uv" else None)
    result = {"code": 0}

    def run_cmd(cmd, *a, **k):
        calls.append(" ".join(cmd))
        return __import__("subprocess").CompletedProcess(cmd, result["code"], "", "Access is denied")

    monkeypatch.setattr(service, "run_cmd", run_cmd)
    monkeypatch.setenv("NO_COLOR", "1")
    return calls, result, moved


def test_uninstall_windows_moves_files_aside_then_runs_uv(win_uninstall, data_dir, tmp_path, capsys):
    from copycord import winupdate

    calls, _result, moved = win_uninstall
    assert cli.main(["uninstall", "--yes"]) == 0
    assert calls[:4] == ["running", "service", "clean",
                         ("move", ["C:/bin/copycord.exe"], tmp_path / "scratch")]
    assert calls[4] == "uv tool uninstall copycord"
    assert calls[5] == ("delete", [tmp_path / "scratch", moved[0][1].parent,
                                   data_dir / winupdate.ASIDE])
    assert not any(c[0] == "restore" for c in calls if isinstance(c, tuple))
    out = capsys.readouterr().out
    assert "Copycord uninstalled" in out


def test_uninstall_windows_purge_skips_the_update_leftovers(win_uninstall, data_dir, tmp_path):
    calls, _result, moved = win_uninstall
    assert cli.main(["uninstall", "--yes", "--purge"]) == 0
    assert "clean" not in calls
    assert calls[-1] == ("delete", [tmp_path / "scratch", moved[0][1].parent])


def test_uninstall_windows_restores_the_files_when_uv_fails(win_uninstall, capsys):
    calls, result, moved = win_uninstall
    result["code"] = 1
    assert cli.main(["uninstall", "--yes"]) == 1
    i = calls.index("uv tool uninstall copycord")
    assert calls[i + 1] == ("restore", moved)
    captured = capsys.readouterr()
    assert "Uninstall incomplete" in captured.out and "Access is denied" in captured.err


def test_uninstall_windows_refuses_while_another_copycord_runs(win_uninstall, monkeypatch, capsys):
    from copycord import winupdate

    calls, _result, _moved = win_uninstall
    monkeypatch.setattr(winupdate, "running", lambda: calls.append("running") or ([], 4242))
    assert cli.main(["uninstall", "--yes"]) == 1
    assert calls == ["running"]
    err = capsys.readouterr().err
    assert "Copycord is running in another window (process 4242); stop it first" in err


def test_uninstall_linux_runs_uv_without_moving_files(win_uninstall, monkeypatch, capsys):
    calls, _result, _moved = win_uninstall
    monkeypatch.setattr(cli.sys, "platform", "linux")
    assert cli.main(["uninstall", "--yes"]) == 0
    assert calls == ["service", "uv tool uninstall copycord"]
    assert "Copycord uninstalled" in capsys.readouterr().out


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
    assert lines[-1] == ALL_GOOD
    assert "✓ home writable" in lines
    assert "✓ .env found" in lines
    assert "✓ dependencies OK" in lines
    assert "• database created on first run" in lines
    assert "• no backups yet" in lines
    assert any(line.startswith("✓ Copycord v") and line.endswith(" · stable channel") for line in lines)
    assert "ROLE" not in out
    assert str(data_dir.resolve()) not in out
    assert os.listdir(data_dir) == [".env"]


def test_doctor_verbose_shows_paths(doctor_env, capsys):
    data_dir = doctor_env
    assert cli.main(["doctor", "--verbose", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert f"✓ home writable  {data_dir.resolve()}" in out
    assert f"✓ .env found  {data_dir.resolve() / '.env'}" in out


def test_doctor_role_set_fails(doctor_env, monkeypatch, capsys):
    monkeypatch.setenv("ROLE", "server")

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 1

    out = capsys.readouterr().out
    role = _line(out, "ROLE is set")
    assert role.startswith("✗")
    assert role.startswith("✗ ROLE is set  Copycord v4 removed the ROLE option.")
    assert "one container" not in role
    assert out.splitlines()[-1] == "✗ 0 warnings, 1 problem"


def test_doctor_default_password_warns(doctor_env, monkeypatch, capsys):
    monkeypatch.setenv("PASSWORD", "copycord")

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 0

    out = capsys.readouterr().out
    env = (doctor_env / ".env").resolve()
    assert _line(out, "the default password") == (
        f"! the default password is still set  change PASSWORD in {env}, "
        "then run copycord restart"
    )
    assert out.splitlines()[-1] == "! 1 warning, 0 problems"


def test_doctor_links_the_paths_in_the_home(doctor_env, monkeypatch):
    from copycord import ui

    link = _local_links(monkeypatch)
    monkeypatch.setenv("PASSWORD", "copycord")
    monkeypatch.setattr(sys, "stdout", _Terminal())
    assert cli.main(["doctor", "--verbose", "--data-dir", str(doctor_env)]) == 0
    out = sys.stdout.getvalue()
    env = (doctor_env / ".env").resolve()
    assert f"change PASSWORD in {link(env)}, " in out
    assert link(doctor_env.resolve()) in out
    plain = ui._ANSI.sub("", out)
    assert f"! the default password is still set  change PASSWORD in {env}, " in plain
    assert f"\u2713 home writable  {doctor_env.resolve()}\n" in plain


def test_doctor_json_has_no_links(doctor_env, monkeypatch):
    _local_links(monkeypatch)
    monkeypatch.setenv("PASSWORD", "copycord")
    out, err = _terminal(monkeypatch)
    assert cli.main(["doctor", "--json", "--data-dir", str(doctor_env)]) == 0
    assert "\033" not in out.getvalue() and "\033" not in err.getvalue()
    hint = [c["hint"] for c in json.loads(out.getvalue())["checks"] if c["text"].startswith("the default")]
    assert hint == [f"change PASSWORD in {(doctor_env / '.env').resolve()}, then run copycord restart"]


def test_doctor_does_not_check_the_tokens(doctor_env, monkeypatch, capsys):
    _unset(monkeypatch, "SERVER_TOKEN", "CLIENT_TOKEN")
    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 0
    out = capsys.readouterr().out
    assert "TOKEN" not in out
    assert out.splitlines()[-1] == ALL_GOOD


def test_doctor_missing_env_file_shows_the_path_once(doctor_env, capsys):
    env = doctor_env / ".env"
    env.unlink()
    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 0
    out = capsys.readouterr().out
    assert _line(out, ".env not found") == f"! .env not found  {env.resolve()}"
    assert _after(out, "Needs your attention") == [
        "! .env not found  run copycord doctor --fix to create it with the default settings"]
    assert out.count(str(env.resolve())) == 1
    assert out.splitlines()[-2:] == ["! 1 warning, 0 problems", FIX_HINT.format(1)]


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
    assert line.startswith(f"✗ database schema {SCHEMA_VERSION + 1} is newer")
    assert str(doctor_env.resolve() / "data.db") in line
    assert out.splitlines()[-1].endswith("1 problem")


def test_check_backups(tmp_path):
    import time

    now = time.time()
    backups, db = tmp_path / "backups", tmp_path / "data.db"
    assert cli.check_backups(backups, db, now).status == cli.INFO

    backups.mkdir()
    db.write_bytes(b"")
    assert cli.check_backups(backups, db, now).status == cli.INFO
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
    assert _line(out, "last backup").startswith("! last backup 10 days ago")
    assert str(archive.parent.resolve()) in _line(out, "last backup")


def test_doctor_missing_dependency_names_only_it(doctor_env, monkeypatch, capsys):
    monkeypatch.setattr(
        cli, "REQUIRED_IMPORTS", (("json", "pytest"), ("no_such_module_copycord", "nothing-dist"))
    )

    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 1

    out = capsys.readouterr().out
    line = _line(out, "missing dependencies")
    assert line.startswith("✗ missing dependencies: nothing-dist  cannot import")
    assert "pytest" not in line
    assert out.splitlines()[-2:] == ["✗ 0 warnings, 1 problem", FIX_HINT.format(1)]


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
    assert f"! port {port} in use" in out
    assert out.splitlines()[-2:] == ["! 1 warning, 0 problems", FIX_HINT.format(1)]


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
    console = cli.check_service(State(True, True, "x"), r"C:\bin\copycord.exe")
    assert console.status == cli.WARN and console.fix == "service_window"
    assert console.name == "service runs in a console window"
    assert cli.check_service(State(True, True, "x"), linger=True).status == cli.PASS
    lingerless = cli.check_service(State(True, True, "x"), linger=False)
    assert lingerless.status == cli.WARN and lingerless.fix == "linger"
    assert lingerless.name == "service starts only when you log in"
    assert "sudo loginctl enable-linger" in lingerless.reason


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
    missing = cli.check_data_dir(tmp_path / "a" / "b")
    assert (missing.status, missing.name, missing.fix) == (cli.WARN, "home folder missing", "home")
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
# doctor --fix
# --------------------------------------------------------------------------


def _fix(data_dir, *flags):
    return cli.main(["doctor", "--fix", *flags, "--data-dir", str(data_dir)])


def _after(out, title):
    """The lines under a heading, up to the next blank line."""
    lines = out.splitlines()
    start = lines.index(title) + 1
    end = lines.index("", start) if "" in lines[start:] else len(lines)
    return lines[start:end]


def test_doctor_has_no_yes_flag(doctor_env, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["doctor", "--fix", "--yes", "--data-dir", str(doctor_env)])
    assert exc.value.code == 2
    assert "unrecognized arguments: --yes" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        cli.main(["doctor", "--help"])
    assert "--yes" not in capsys.readouterr().out


def test_doctor_fix_with_nothing_to_fix(doctor_env, capsys):
    assert _fix(doctor_env) == 0
    out = capsys.readouterr().out
    lines = out.splitlines()
    assert "(fixed)" not in out and "Needs your attention" not in lines
    assert lines[-1] == "✓ Everything looks good  start it with copycord run"


def test_doctor_fix_recreates_the_env_file(doctor_env, capsys):
    from copycord import envfile

    env = doctor_env / ".env"
    env.unlink()
    assert _fix(doctor_env) == 0

    out = capsys.readouterr().out
    # The report shows the state after the fix, once.
    assert ".env not found" not in out
    assert (f"✓ .env created with the default settings  {env.resolve()} (fixed)"
            in _after(out, "Settings"))
    # The installer's file: the same template, the same live defaults.
    assert env.read_text(encoding="utf-8") == envfile.template(env.resolve().parent)
    assert f"BACKUP_DIR={env.resolve().parent / 'backups'}" in env.read_text(encoding="utf-8").splitlines()
    if sys.platform != "win32":
        assert (env.stat().st_mode & 0o777) == 0o600
    # The re-check runs after the fix; the real environment's PASSWORD still wins.
    assert "Needs your attention" not in out
    assert out.splitlines()[-2:] == ["✓ Fixed 1 issue", "✓ Everything looks good  start it with copycord run"]


def test_doctor_fix_applies_the_automatic_fixes_without_asking(doctor_env, monkeypatch, capsys):
    (doctor_env / ".env").unlink()
    _stale_backup(doctor_env)
    asked = _answers(monkeypatch)
    assert _fix(doctor_env) == 0
    assert asked == []
    out = capsys.readouterr().out
    fixes = [x for x in out.splitlines() if x.endswith("(fixed)")]
    assert [x.split("  ")[0] for x in fixes] == [
        "✓ .env created with the default settings",
        "✓ database backed up",
    ]
    assert "✓ Fixed 2 issues" in out.splitlines()


def test_doctor_fix_creates_the_home_before_the_env_file(doctor_env, capsys):
    import shutil

    shutil.rmtree(doctor_env)
    assert _fix(doctor_env) == 0
    out = capsys.readouterr().out
    assert f"✓ Copycord home created  {doctor_env.resolve()} (fixed)" in _after(out, "Data")
    assert _after(out, "Settings")[0].startswith("✓ .env created with the default settings")
    assert (doctor_env / ".env").is_file()


def _stale_backup(data_dir):
    import time

    from copycord.common.db import DBManager

    DBManager(str(data_dir / "data.db"), init_schema=True).conn.close()
    old = data_dir / "backups" / "2026-01-01_03-17-00.tar.gz"
    old.parent.mkdir()
    old.write_bytes(b"x")
    past = time.time() - 10 * 86400
    os.utime(old, (past, past))
    return old.parent


def test_doctor_fix_backs_up_a_stale_database(doctor_env, capsys):
    import tarfile

    from copycord.common.db import DBManager

    backups = _stale_backup(doctor_env)
    assert _fix(doctor_env) == 0

    out = capsys.readouterr().out
    archives = sorted(p for p in backups.glob("*.tar.gz") if not p.name.startswith("2026-01-01"))
    assert len(archives) == 1
    with tarfile.open(archives[0]) as tar:
        assert tar.getnames() == ["data.db", "manifest.json"]
    assert f"✓ database backed up  {archives[0].resolve()} (fixed)" in _after(out, "Data")
    # The dashboard's last-backup values, as after a scheduled backup.
    db = DBManager(str(doctor_env / "data.db"))
    assert db.get_config("DB_LAST_BACKUP_FILE") == archives[0].name
    db.conn.close()
    assert out.splitlines()[-1] == "✓ Everything looks good  start it with copycord run"


@pytest.fixture()
def reinstall(doctor_env, monkeypatch):
    """The web UI is missing until update.update(reinstall=True) runs."""
    from copycord import update

    calls = []
    state = {"rc": 0}

    def fake_update(**kwargs):
        calls.append(kwargs)
        return state["rc"]

    def frontend():
        if calls and not state["rc"]:
            return cli.Check(cli.PASS, "web UI included")
        return cli.Check(cli.FAIL, "web UI missing", "reinstall", "/x/main.js", "reinstall")

    monkeypatch.setattr(update, "update", fake_update)
    monkeypatch.setattr(update, "detect_installer", lambda: update.UV_TOOL)
    monkeypatch.setattr(cli, "check_frontend", frontend)
    _unset(monkeypatch, "COPYCORD_SKIP_FRONTEND")
    return calls, state


def test_doctor_fix_reinstalls_once_for_ui_and_dependencies(reinstall, doctor_env, monkeypatch, capsys):
    calls, _ = reinstall
    monkeypatch.setattr(cli, "REQUIRED_IMPORTS", (("no_such_module_copycord", "nothing-dist"),))
    assert _fix(doctor_env) == 1
    assert calls == [{"reinstall": True}]
    out = capsys.readouterr().out
    fixes = [x for x in out.splitlines() if x.endswith("(fixed)")]
    assert fixes == [f"✓ Copycord reinstalled  v{CURRENT_VERSION.lstrip('v')} (fixed)"]
    assert out.splitlines()[-2:] == ["✓ Fixed 1 issue", "✗ 1 thing needs your attention"]


def test_doctor_fix_reinstall_then_the_recheck_passes(reinstall, doctor_env, capsys):
    calls, _ = reinstall
    assert _fix(doctor_env) == 0
    assert calls == [{"reinstall": True}]
    out = capsys.readouterr().out
    assert out.splitlines()[-1] == "✓ Everything looks good  start it with copycord run"


def test_doctor_fix_reinstall_failure_is_reported(reinstall, doctor_env, capsys):
    calls, state = reinstall
    state["rc"] = 3
    assert _fix(doctor_env) == 1
    out = capsys.readouterr().out
    assert "(fixed)" not in out and "Fixed" not in out
    # In place, and again with what is left.
    assert _after(out, "Install")[2].startswith(
        "✗ web UI missing  could not fix it: the reinstall failed (exit 3)")
    assert out.splitlines()[-1] == "✗ 1 thing needs your attention"
    assert _after(out, "Needs your attention")[0].startswith(
        "✗ web UI missing  could not fix it: the reinstall failed (exit 3)")


@pytest.mark.parametrize("kind", ("source", "docker", "pip"))
def test_doctor_fix_does_not_reinstall_what_update_cannot(reinstall, doctor_env, monkeypatch, capsys, kind):
    from copycord import update

    calls, _ = reinstall
    monkeypatch.setattr(update, "detect_installer", lambda: kind)
    assert _fix(doctor_env) == 1
    assert calls == []
    assert f"✗ web UI missing  could not fix it: Copycord was installed with {kind}" in capsys.readouterr().out


def test_doctor_fix_does_not_reinstall_without_the_frontend_build(reinstall, doctor_env, monkeypatch, capsys):
    calls, _ = reinstall
    monkeypatch.setenv("COPYCORD_SKIP_FRONTEND", "1")
    assert _fix(doctor_env) == 1
    assert calls == []
    assert "COPYCORD_SKIP_FRONTEND is set" in capsys.readouterr().out


def test_doctor_fix_on_a_git_install_without_npm_fails_early(reinstall, doctor_env, monkeypatch, capsys):
    from copycord import node_runtime, update

    calls, _ = reinstall
    monkeypatch.setattr(update, "git_source", lambda: update.GitSource(
        "https://github.com/Copycord/Copycord", "v4/doctor-fix", "d" * 40))
    monkeypatch.setattr(node_runtime, "find_npm", lambda env=None: None)
    assert _fix(doctor_env) == 1
    assert calls == []
    out = capsys.readouterr().out
    assert f"✗ web UI missing  could not fix it: {cli.NPM_MISSING}" in out
    assert "npm is not installed" in cli.NPM_MISSING and "Node.js" in cli.NPM_MISSING


def test_doctor_fix_on_a_git_install_with_npm_reinstalls(reinstall, doctor_env, monkeypatch):
    from copycord import node_runtime, update

    calls, _ = reinstall
    monkeypatch.setattr(update, "git_source", lambda: update.GitSource(
        "https://github.com/Copycord/Copycord", "v4/doctor-fix", "d" * 40))
    monkeypatch.setattr(node_runtime, "find_npm", lambda env=None: node_runtime.Path("/usr/bin"))
    assert _fix(doctor_env) == 0
    assert calls == [{"reinstall": True}]


@pytest.fixture()
def stopped_service(doctor_env, monkeypatch):
    from copycord import service

    state = {"running": False, "error": None}

    class FakeService:
        where = "systemd user unit /x/copycord.service"

        def start(self):
            if state["error"]:
                raise service.ServiceError(state["error"])
            state["running"] = True

    monkeypatch.setattr(service, "get_service", lambda: FakeService())
    monkeypatch.setattr(cli, "service_state",
                        lambda: service.State(True, state["running"], FakeService.where))
    return state


def test_doctor_fix_starts_a_stopped_service(stopped_service, doctor_env, capsys):
    assert _fix(doctor_env) == 0
    out = capsys.readouterr().out
    assert "✓ copycord service started  systemd user unit /x/copycord.service (fixed)" in out
    assert stopped_service["running"]
    assert out.splitlines()[-1] == "✓ Everything looks good  start it with copycord run"


def test_doctor_fix_service_start_failure(stopped_service, doctor_env, capsys):
    stopped_service["error"] = "the system service needs root: rerun with sudo"
    assert _fix(doctor_env) == 0
    out = capsys.readouterr().out
    assert "service installed, not running  could not fix it: the system service needs root" in out


def test_doctor_fix_rewrites_a_service_in_a_console_window(doctor_env, monkeypatch, capsys):
    from copycord import service

    state = {"console": r"C:\Users\Mac\.local\bin\copycord.exe"}

    class FakeService:
        where = 'scheduled task "Copycord"'

        def use_windowless(self):
            state["console"] = None
            return ["ok"]

    monkeypatch.setattr(service, "get_service", lambda: FakeService())
    monkeypatch.setattr(cli, "service_state",
                        lambda: service.State(True, True, FakeService.where))
    monkeypatch.setattr(cli, "service_console", lambda: state["console"])
    assert _fix(doctor_env) == 0
    out = capsys.readouterr().out
    assert '✓ service runs without a window  scheduled task "Copycord" (fixed)' in out
    assert state["console"] is None


@pytest.mark.parametrize("enabled", [True, False])
def test_doctor_fix_turns_linger_on(doctor_env, monkeypatch, capsys, enabled):
    from copycord import service

    monkeypatch.setenv("USER", "mac")
    linger = {"on": False}
    calls = []

    def run(cmd, capture=True):
        calls.append(list(cmd))
        linger["on"] = linger["on"] or enabled
        return subprocess.CompletedProcess(list(cmd), 0 if enabled else 1, "", "")

    monkeypatch.setattr(service, "run_cmd", run)
    monkeypatch.setattr(cli, "service_state",
                        lambda: service.State(True, True, "systemd user unit /x/copycord.service"))
    monkeypatch.setattr(cli, "service_linger", lambda: linger["on"])
    _fix(doctor_env)
    out = capsys.readouterr().out
    assert calls == [["loginctl", "enable-linger", "mac"]]
    if enabled:
        assert "✓ service starts at boot  linger on for mac (fixed)" in out
    else:
        assert "could not fix it: to start Copycord at boot" in out
        assert "sudo loginctl enable-linger mac" in out


def test_doctor_flags_a_service_in_a_console_window(doctor_env, monkeypatch, capsys):
    from copycord import service

    monkeypatch.setattr(cli, "service_state", lambda: service.State(True, True, "task"))
    monkeypatch.setattr(cli, "service_console", lambda: r"C:\bin\copycord.exe")
    cli.main(["doctor", "--data-dir", str(doctor_env)])
    out = capsys.readouterr().out
    assert "service runs in a console window" in out
    assert "run it without a window" in out


def test_run_with_a_busy_port_prints_one_line(data_dir, monkeypatch, capsys):
    from copycord import supervisor

    async def busy():
        raise supervisor.PortBusyError(9101)

    monkeypatch.setattr(supervisor, "run", busy)
    assert cli.main(["run"]) == 1
    err = capsys.readouterr().err
    assert "Port 9101 is in use, so Copycord cannot start" in err
    assert "copycord service stop" in err and "copycord doctor" in err
    assert "Traceback" not in err and "crash" not in err.lower()


def test_main_without_console_streams(data_dir, monkeypatch):
    from copycord import logging_setup, service

    class Svc:
        def state(self):
            return service.State(True, True, "task")

    monkeypatch.setattr(service, "get_service", lambda user=False: Svc())
    monkeypatch.setattr(logging_setup, "WINDOWLESS", False)
    for name in ("stdin", "stdout", "stderr"):
        monkeypatch.setattr(sys, name, None)
    assert cli.main(["service", "status"]) == 0
    assert logging_setup.WINDOWLESS
    assert not sys.stdin.isatty() and not sys.stdout.isatty()
    print("no console", file=sys.stderr)


@pytest.fixture()
def default_password(doctor_env, monkeypatch):
    (doctor_env / ".env").write_text("PASSWORD=copycord\n", encoding="utf-8")
    monkeypatch.setenv("PASSWORD", "copycord")
    return doctor_env / ".env"


def test_doctor_fix_never_changes_the_password(default_password, doctor_env, monkeypatch, capsys):
    import getpass

    # A terminal, and still no question about the password.
    asked = _answers(monkeypatch)
    monkeypatch.setattr(getpass, "getpass", lambda *_: pytest.fail("asked for a password"))
    assert _fix(doctor_env) == 0
    assert asked == []
    assert default_password.read_text(encoding="utf-8") == "PASSWORD=copycord\n"
    out = capsys.readouterr().out
    assert _after(out, "Needs your attention") == [
        f"! the default password is still set  change PASSWORD in "
        f"{default_password.resolve()}, then run copycord restart"]
    assert "Fixed" not in out
    assert "no automatic fix" not in out
    assert out.splitlines()[-1] == "! 1 thing needs your attention"


def test_doctor_fix_without_a_terminal_leaves_the_password(default_password, doctor_env, capsys):
    assert _fix(doctor_env) == 0
    assert default_password.read_text(encoding="utf-8") == "PASSWORD=copycord\n"
    out = capsys.readouterr().out
    assert "in a terminal" not in out
    assert out.splitlines()[-1] == "! 1 thing needs your attention"


@pytest.fixture()
def taken_port(doctor_env, monkeypatch):
    with socket.socket() as s:
        s.bind((cli.ADMIN_BIND_HOST, 0))
        s.listen(1)
        port = s.getsockname()[1]
        (doctor_env / ".env").write_text(f"ADMIN_PORT={port}\n", encoding="utf-8")
        monkeypatch.setenv("ADMIN_PORT", str(port))
        monkeypatch.setattr(cli, "copycord_on_port", lambda p, timeout=1.0: False)
        yield port


def test_doctor_fix_moves_to_a_free_port(taken_port, doctor_env, monkeypatch, capsys):
    from copycord import envfile

    asked = _answers(monkeypatch, "y")
    assert _fix(doctor_env) == 0
    new = int(envfile.get("ADMIN_PORT", doctor_env / ".env"))
    assert new > taken_port
    assert f"Port {taken_port} is taken by another program. Set ADMIN_PORT to {new}?" in asked[0]
    out = capsys.readouterr().out
    assert f"✓ dashboard moved to a free port  ADMIN_PORT={new}" in out
    assert f"port {taken_port} in use" not in out
    assert out.splitlines()[-1] == "✓ Everything looks good  start it with copycord run"


def test_doctor_fix_port_declined(taken_port, doctor_env, monkeypatch, capsys):
    _answers(monkeypatch, "n")
    assert _fix(doctor_env) == 0
    assert (doctor_env / ".env").read_text(encoding="utf-8") == f"ADMIN_PORT={taken_port}\n"
    assert f"port {taken_port} in use  fix skipped" in capsys.readouterr().out


def test_doctor_fix_without_a_terminal_skips_the_port(taken_port, doctor_env, capsys):
    assert _fix(doctor_env) == 0
    out = capsys.readouterr().out
    assert (f"port {taken_port} in use  "
            "run copycord doctor --fix in a terminal to move the dashboard to a free port") in out
    assert (doctor_env / ".env").read_text(encoding="utf-8") == f"ADMIN_PORT={taken_port}\n"


def test_next_free_port_skips_a_taken_one():
    with socket.socket() as s:
        s.bind((cli.ADMIN_BIND_HOST, 0))
        s.listen(1)
        port = s.getsockname()[1]
        with socket.socket() as t:
            try:
                t.bind((cli.ADMIN_BIND_HOST, port + 1))
                t.listen(1)
            except OSError:
                pytest.skip("the next port is taken already")
            assert cli.next_free_port(port) not in (port, port + 1)


def test_doctor_fix_never_touches_role_or_a_newer_schema(doctor_env, monkeypatch, capsys):
    from copycord.common.db import SCHEMA_VERSION, SCHEMA_VERSION_KEY, DBManager

    monkeypatch.setenv("ROLE", "server")
    db = DBManager(str(doctor_env / "data.db"), init_schema=True)
    db.set_config(SCHEMA_VERSION_KEY, str(SCHEMA_VERSION + 1))
    db.conn.close()
    before = sorted(os.listdir(doctor_env))

    assert _fix(doctor_env) == 1

    out = capsys.readouterr().out
    left = _after(out, "Needs your attention")
    role = next(x for x in left if "ROLE is set" in x)
    assert "Delete ROLE" in role
    schema = next(x for x in left if "database schema" in x)
    assert "copycord update" in schema and "restore a backup" in schema
    assert "Fixed" not in out.splitlines() and "no automatic fix" not in out
    assert sorted(os.listdir(doctor_env)) == before


def test_doctor_fix_rechecks_after_the_fixes(doctor_env, monkeypatch, capsys):
    runs = []
    real = cli.run_doctor
    monkeypatch.setattr(cli, "run_doctor", lambda: runs.append(1) or real())
    (doctor_env / ".env").unlink()
    assert _fix(doctor_env) == 0
    out = capsys.readouterr().out
    assert len(runs) == 2
    lines = out.splitlines()
    assert lines.count("Settings") == 1 and "Re-check" not in lines
    assert "1 warning, 0 problems" not in lines
    assert lines[-2:] == ["✓ Fixed 1 issue", "✓ Everything looks good  start it with copycord run"]


def test_doctor_fix_ctrl_c_stops(taken_port, doctor_env, monkeypatch, capsys):
    from copycord import ui

    monkeypatch.setattr(ui, "is_interactive", lambda: True)

    def interrupt(prompt=""):
        raise KeyboardInterrupt

    monkeypatch.setattr("builtins.input", interrupt)
    assert _fix(doctor_env) == 130
    assert (doctor_env / ".env").read_text(encoding="utf-8") == f"ADMIN_PORT={taken_port}\n"
    assert "Needs your attention" not in capsys.readouterr().out


def test_doctor_fix_brief_after_a_fix(doctor_env, capsys):
    (doctor_env / ".env").unlink()
    assert _fix(doctor_env, "--brief") == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("✓ .env created with the default settings")
    assert lines[-1] == "✓ All checks passed"
    assert "Install" not in lines and "Fixed" not in lines


def test_doctor_fix_brief_shows_only_the_failures(reinstall, doctor_env, capsys):
    _, state = reinstall
    state["rc"] = 3
    assert _fix(doctor_env, "--brief") == 1
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("✗ web UI missing  could not fix it: the reinstall failed (exit 3)")
    assert lines[-1] == "✗ 0 warnings, 1 problem  run copycord doctor for details"
    assert len(lines) == 2


def test_doctor_brief_with_a_warning(default_password, doctor_env, capsys):
    assert _fix(doctor_env, "--brief") == 0
    assert capsys.readouterr().out.splitlines() == [
        "! 1 warning, 0 problems  run copycord doctor for details"]


def test_doctor_ends_with_what_needs_attention(default_password, doctor_env, capsys):
    assert cli.main(["doctor", "--data-dir", str(doctor_env)]) == 0
    out = capsys.readouterr().out
    left = _after(out, "Needs your attention")
    assert len(left) == 1 and left[0].startswith("! the default password is still set")
    assert out.splitlines()[-1] == "! 1 warning, 0 problems"


def test_envfile_write_template_never_overwrites(tmp_path):
    from copycord import envfile

    path = envfile.write_template(tmp_path / "home" / ".env")
    assert path.read_text(encoding="utf-8") == envfile.template(tmp_path / "home")
    path.write_text("PASSWORD=mine\n", encoding="utf-8")
    with pytest.raises(FileExistsError):
        envfile.write_template(path)
    assert path.read_text(encoding="utf-8") == "PASSWORD=mine\n"


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

    assert cli.main(["config", "export", *dd]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["SERVER_TOKEN"] == cli.MASK
    assert shown["CLIENT_TOKEN"] == cli.MASK
    assert shown["ADMIN_PASSWORD"] == cli.MASK
    assert shown["COMMAND_USERS"] == "1,2"

    assert cli.main(["config", "export", "--show-secrets", *dd]) == 0
    out = capsys.readouterr().out
    assert json.loads(out)["SERVER_TOKEN"] == "srv-secret"

    backup = tmp_path / "backup.json"
    assert cli.main(["config", "export", str(backup), *dd]) == 0
    assert "srv-secret" not in capsys.readouterr().out
    exported = json.loads(backup.read_text())
    assert exported["SERVER_TOKEN"] == "srv-secret"

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
    assert paths.data_dir() == (native / ".copycord" / "data").resolve()
    assert paths.db_path() == paths.data_dir() / "data.db"


def test_copycord_home_env_overrides(native, monkeypatch, tmp_path):
    monkeypatch.setenv("COPYCORD_HOME", str(tmp_path / "ch"))
    assert paths.data_dir() == (tmp_path / "ch" / "data").resolve()


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
    assert paths.data_dir() == (tmp_path / "saved" / "data").resolve()
    monkeypatch.setenv("COPYCORD_HOME", str(tmp_path / "ch"))
    assert paths.data_dir() == (tmp_path / "ch" / "data").resolve()


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
    assert paths.data_dir() == (native / ".copycord" / "data").resolve()


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
    assert paths.data_dir() == (native / ".copycord" / "data").resolve()


def test_default_windows_without_localappdata(native, monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    expected = native / ".copycord" / "data"
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
    assert logging_setup.logs_dir() == (native / ".copycord" / "logs").resolve()
    assert paths.data_dir() == (native / ".copycord" / "data").resolve()


# --------------------------------------------------------------------------
# <home>/data and <home>/logs layout, and moving the old flat home
# --------------------------------------------------------------------------


def _make_db(path: Path) -> None:
    import sqlite3

    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE t (x)")
        conn.execute("INSERT INTO t VALUES (1)")
    conn.close()


@pytest.fixture()
def flat_home(native):
    """A native home in the old layout: everything loose in ~/.copycord."""
    home = native / ".copycord"
    home.mkdir(parents=True)
    _make_db(home / "data.db")
    (home / "secret.key").write_text("k")
    (home / "update.json").write_text("{}")
    (home / "update-check.json").write_text("{}")
    (home / "server.out").write_text("s")
    (home / "client.out").write_text("c")
    (home / ".env").write_text("PASSWORD=x\n")
    (home / "src").mkdir()
    (home / "logs").mkdir()
    (home / "logs" / "update.log").write_text("u")
    return home


def test_native_layout_helpers(native):
    home = (native / ".copycord").resolve()
    assert paths.home_dir() == home
    assert paths.data_dir() == home / "data"
    assert paths.logs_dir() == home / "logs"
    assert paths.out_dir() == home / "logs"
    assert paths.out_candidates("server.out") == [
        home / "logs" / "server.out", home / "data" / "server.out", home / "server.out",
    ]


def test_docker_layout_is_flat(native, monkeypatch):
    monkeypatch.setattr(paths, "in_docker", lambda: True)
    data = Path("/data").resolve()
    assert paths.home_dir() == data
    assert paths.out_dir() == data
    assert paths.logs_dir() == data / "logs"


def test_custom_data_dir_is_flat(native, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "dd"))
    dd = (tmp_path / "dd").resolve()
    assert paths.data_dir() == dd
    assert paths.home_dir() == dd
    assert paths.out_dir() == dd


def test_service_data_dir_keeps_home(native, monkeypatch):
    home = (native / ".copycord").resolve()
    monkeypatch.setenv("DATA_DIR", str(home / "data"))
    assert paths.home_dir() == home
    assert paths.out_dir() == home / "logs"


def test_env_file_stays_in_home(native):
    from copycord import envfile

    assert envfile.env_path() == (native / ".copycord" / ".env").resolve()


def test_old_layout_keeps_working_before_the_move(flat_home):
    assert paths.data_dir() == flat_home.resolve()
    assert paths.out_dir() == flat_home.resolve()
    assert paths.needs_migration()


def test_migration_backs_up_then_moves(flat_home, monkeypatch):
    from copycord.common import db as dbmod

    order = []
    real = dbmod.backup_pre_update

    def spy(*a, **k):
        assert (flat_home / "data.db").is_file()
        order.append("backup")
        return real(*a, **k)

    monkeypatch.setattr(dbmod, "backup_pre_update", spy)
    lines = []
    line = paths.migrate_layout(log=lines.append)
    assert order == ["backup"]
    assert lines == [line] and "Moved Copycord data" in line
    data, logs = flat_home / "data", flat_home / "logs"
    for name in ("data.db", "secret.key", "update.json", "update-check.json"):
        assert (data / name).is_file() and not (flat_home / name).exists()
    assert list((data / "backups").glob("data-pre-update-layout-*.db"))
    assert (logs / "server.out").read_text() == "s"
    assert (logs / "client.out").read_text() == "c"
    assert (logs / "update.log").read_text() == "u"
    assert (flat_home / ".env").is_file() and (flat_home / "src").is_dir()
    assert paths.data_dir() == data.resolve()
    assert not paths.needs_migration()
    assert paths.migrate_layout() is None


def test_migration_skips_when_data_db_exists(flat_home):
    (flat_home / "data").mkdir()
    _make_db(flat_home / "data" / "data.db")
    assert not paths.needs_migration()
    assert paths.migrate_layout() is None
    assert (flat_home / "data.db").is_file()
    assert (flat_home / "secret.key").read_text() == "k"


def test_migration_never_overwrites(flat_home):
    (flat_home / "data").mkdir()
    (flat_home / "data" / "secret.key").write_text("new")
    line = paths.migrate_layout()
    assert "secret.key" in line
    assert (flat_home / "data" / "secret.key").read_text() == "new"
    assert (flat_home / "secret.key").read_text() == "k"
    assert (flat_home / "data" / "data.db").is_file()


def test_migration_does_nothing_in_docker(flat_home, monkeypatch):
    monkeypatch.setattr(paths, "in_docker", lambda: True)
    assert not paths.needs_migration()
    assert paths.migrate_layout() is None
    assert (flat_home / "data.db").is_file()
    assert not (flat_home / "data").exists()


def test_migration_skips_custom_data_dir(flat_home, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "dd"))
    assert paths.migrate_layout() is None
    assert (flat_home / "data.db").is_file()


def test_migration_follows_old_service_data_dir(flat_home, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(flat_home))
    env = dict(os.environ)
    assert paths.migrate_layout(env) is not None
    assert env["DATA_DIR"] == str((flat_home / "data").resolve())


def test_migration_failure_restores_old_layout(flat_home, monkeypatch):
    real = os.rename
    calls = []

    def flaky(src, dest):
        calls.append(src)
        if len(calls) == 3:
            raise OSError("disk says no")
        return real(src, dest)

    monkeypatch.setattr(paths.os, "rename", flaky)
    with pytest.raises(OSError):
        paths.migrate_layout()
    monkeypatch.setattr(paths.os, "rename", real)
    assert (flat_home / "data.db").is_file()
    assert (flat_home / "secret.key").read_text() == "k"
    assert (flat_home / "server.out").is_file()
    assert not (flat_home / "data" / "data.db").exists()
    assert paths.data_dir() == flat_home.resolve()


def test_migrate_on_start_logs_and_survives_failure(flat_home, monkeypatch, caplog):
    import logging

    monkeypatch.setattr(paths, "migrate_layout", lambda **k: (_ for _ in ()).throw(OSError("x")))
    with caplog.at_level(logging.INFO):
        cli.migrate_on_start()
    assert "still using the old layout" in caplog.text
    assert (flat_home / "data.db").is_file()


def test_doctor_flags_and_fixes_layout(flat_home, monkeypatch):
    monkeypatch.setattr(cli, "service_state", lambda: None)
    check = cli.check_layout()
    assert check.status == cli.WARN and check.fix == "layout"
    assert "Moved Copycord data" in cli.fix_layout([check])
    assert cli.check_layout().status == cli.PASS
    assert (flat_home / "data" / "data.db").is_file()


def test_doctor_layout_fix_refuses_while_running(flat_home, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(cli, "service_state", lambda: SimpleNamespace(running=True))
    with pytest.raises(cli.FixError):
        cli.fix_layout([])
    assert (flat_home / "data.db").is_file()
