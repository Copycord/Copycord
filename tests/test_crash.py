"""Tests for the crash report the copycord command prints on an unexpected error."""
import tempfile

import pytest

from copycord import __main__ as cli
from copycord import crash


def _unset(monkeypatch, *names):
    for name in names:
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    d = tmp_path / "data"
    monkeypatch.setenv("DATA_DIR", str(d))
    _unset(monkeypatch, "DB_PATH", "ROLE", "PASSWORD")
    monkeypatch.setenv("COPYCORD_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr(cli, "service_state", lambda: None)
    return d


@pytest.fixture()
def boom(data_dir, monkeypatch):
    """doctor raises an error nobody expects."""

    def run_doctor():
        raise RuntimeError("boom\nsecond line")

    monkeypatch.setattr(cli, "run_doctor", run_doctor)
    return data_dir


def _logs(data_dir):
    return sorted((data_dir.resolve() / "logs").glob("copycord-crash-*.log"))


def test_crash_prints_three_plain_lines_and_writes_a_log(boom, capsys):
    assert cli.main(["doctor"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    [log] = _logs(boom)
    assert captured.err.splitlines() == [
        "Copycord hit an unexpected error: RuntimeError: boom",
        f"The details were saved to {log}",
        "Report it at https://github.com/Copycord/Copycord/issues",
    ]
    assert "Traceback" not in captured.err
    text = log.read_text(encoding="utf-8")
    assert "Command: copycord doctor\n" in text
    assert "Traceback (most recent call last)" in text
    assert "RuntimeError: boom\nsecond line" in text


@pytest.mark.parametrize("argv", [["--verbose", "doctor"], ["doctor", "--verbose"], ["doctor", "-v"]])
def test_verbose_also_prints_the_traceback(argv, boom, capsys):
    assert cli.main(argv) == 1
    err = capsys.readouterr().err
    assert "Traceback (most recent call last)" in err
    assert "in run_doctor" in err
    assert err.splitlines()[-3] == "Copycord hit an unexpected error: RuntimeError: boom"


def test_json_crash_goes_to_stderr(boom, capsys):
    assert cli.main(["doctor", "--json"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Copycord hit an unexpected error: RuntimeError: boom" in captured.err


def test_keyboard_interrupt_is_not_a_crash(data_dir, monkeypatch):
    def interrupted():
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "run_doctor", interrupted)
    with pytest.raises(KeyboardInterrupt):
        cli.main(["doctor"])
    assert _logs(data_dir) == []


def test_system_exit_passes_through(data_dir, monkeypatch):
    def leave():
        raise SystemExit(3)

    monkeypatch.setattr(cli, "run_doctor", leave)
    with pytest.raises(SystemExit) as exc:
        cli.main(["doctor"])
    assert exc.value.code == 3
    assert _logs(data_dir) == []


def test_the_log_leaves_out_the_values(data_dir, monkeypatch, capsys):
    from copycord import envfile

    def broken(key, value):
        raise ValueError("disk said no")

    monkeypatch.setattr(envfile, "set_value", broken)
    assert cli.main(["config", "set", "PASSWORD", "hunter2"]) == 1
    [log] = _logs(data_dir)
    text = log.read_text(encoding="utf-8")
    assert "Command: copycord config set\n" in text
    assert "hunter2" not in text
    assert "ValueError: disk said no" in capsys.readouterr().err


def test_log_falls_back_to_the_temp_folder(boom, tmp_path, monkeypatch, capsys):
    from copycord.common import paths

    blocked = tmp_path / "not-a-folder"
    blocked.write_text("", encoding="utf-8")
    monkeypatch.setattr(paths, "logs_dir", lambda env=None: blocked / "logs")
    temp = tmp_path / "temp"
    temp.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp))

    assert cli.main(["doctor"]) == 1
    [log] = temp.glob("copycord-crash-*.log")
    assert f"The details were saved to {log}" in capsys.readouterr().err


def test_no_writable_folder_still_reports(boom, monkeypatch, capsys):
    monkeypatch.setattr(crash, "write_log", lambda exc, argv=(), dirs=None: None)
    assert cli.main(["doctor"]) == 1
    assert capsys.readouterr().err.splitlines() == [
        "Copycord hit an unexpected error: RuntimeError: boom",
        "The details could not be saved to a log file.",
        "Report it at https://github.com/Copycord/Copycord/issues",
    ]


def test_summary():
    assert crash.summary(ValueError()) == "ValueError"
    assert crash.summary(OSError("first\nsecond")) == "OSError: first"
    assert crash._command(["--no-color", "config", "get", "PASSWORD", "--", "x"]) == "copycord config get"


def test_verbose_is_accepted_by_every_command(data_dir):
    assert cli.parse_args(["--verbose", "doctor"]).verbose is True
    assert cli.parse_args(["doctor"]).verbose is False
    assert cli.parse_args(["config", "get", "KEY", "--verbose"]).verbose is True
