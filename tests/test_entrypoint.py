"""entrypoint.sh run under sh with id, setpriv and chown stubbed on PATH."""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = ROOT / "entrypoint.sh"

pytestmark = pytest.mark.skipif(os.name == "nt" or not shutil.which("sh"), reason="needs a POSIX sh")

STUBS = {
    "id": '[ "$1" = -u ] && echo "$FAKE_UID"\n',
    "chown": 'echo "chown $*" >> "$LOG"\n',
    # Logs the call, then answers "test -w" from FAKE_APP_WRITABLE or runs the command.
    "setpriv": (
        'echo "setpriv $*" >> "$LOG"\n'
        'while [ "${1#--}" != "$1" ]; do shift; done\n'
        'if [ "$1" = test ]; then [ "$FAKE_APP_WRITABLE" = 1 ]; exit; fi\n'
        'exec "$@"\n'
    ),
}


def _write_exe(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
    path.chmod(0o755)


@pytest.fixture
def run(tmp_path):
    stubs = tmp_path / "stubs"
    for name, body in STUBS.items():
        _write_exe(stubs / name, body)
    _write_exe(tmp_path / "venv" / "copycord" / "bin" / "copycord", 'echo "copycord $*"\n')
    data = tmp_path / "data"
    data.mkdir()
    (data / "data.db").write_bytes(b"v3")
    (data / "backups").mkdir()
    log = tmp_path / "log"
    log.touch()

    def _run(uid, app_writable=True):
        env = {
            "PATH": "%s%s%s" % (stubs, os.pathsep, os.environ.get("PATH", "")),
            "DATA_DIR": str(data),
            "VENV_BASE": str(tmp_path / "venv"),
            "LOG": str(log),
            "FAKE_UID": str(uid),
            "FAKE_APP_WRITABLE": "1" if app_writable else "0",
        }
        p = subprocess.run(["sh", str(ENTRYPOINT)], env=env, capture_output=True, text=True, timeout=30)
        return p, log.read_text(encoding="utf-8"), data

    return _run


def test_entrypoint_parses():
    subprocess.run(["sh", "-n", str(ENTRYPOINT)], check=True)
    if shutil.which("bash"):
        subprocess.run(["bash", "-n", str(ENTRYPOINT)], check=True)


def test_root_chowns_then_drops_to_uid_1000(run):
    p, log, data = run(0)
    assert p.returncode == 0, p.stderr
    assert p.stdout == "copycord run\n"
    assert p.stderr == ""
    assert "setpriv --reuid=1000 --regid=1000 --clear-groups test -w %s\n" % data in log
    assert any(
        line.startswith("setpriv --reuid=1000 --regid=1000 --clear-groups ") and line.endswith("/copycord/bin/copycord run")
        for line in log.splitlines()
    )
    # Real find picks what to chown, so only files the runner owns as non-1000 show up.
    if (os.getuid(), os.getgid()) != (1000, 1000):
        assert "chown -h 1000:1000 %s" % data in log
        assert str(data / "data.db") in log


def test_root_stays_root_silently_when_chown_does_not_take(run):
    # Windows Docker Desktop bind mounts ignore chown; the user cannot fix that, so no message.
    p, log, data = run(0, app_writable=False)
    assert p.returncode == 0, p.stderr
    assert p.stdout == "copycord run\n"
    assert p.stderr == ""
    assert not any(line.endswith("copycord run") for line in log.splitlines())


def test_non_root_runs_directly(run):
    p, log, _ = run(1000)
    assert p.returncode == 0, p.stderr
    assert p.stdout == "copycord run\n"
    assert "setpriv" not in log and "chown" not in log


@pytest.mark.skipif(os.name != "nt" and os.geteuid() == 0, reason="root can write a 0555 folder")
def test_non_root_refuses_unwritable_backups(run):
    _, _, data = run(1000)
    (data / "backups").chmod(0o555)
    try:
        p, _, _ = run(1000)
    finally:
        (data / "backups").chmod(0o755)
    assert p.returncode == 1
    assert p.stdout == ""
    assert "%s is not writable by uid 1000" % (data / "backups") in p.stderr
    assert "sudo chown -R 1000:1000" in p.stderr
