"""
Static checks of the install scripts at the repo root (install.sh,
install.ps1): they exist, fail loudly, resolve releases from the GitHub
releases API, honour the COPYCORD_* variables and never touch Docker.
Nothing is executed and there is no network.
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SH = ROOT / "install.sh"
PS1 = ROOT / "install.ps1"
API = "https://api.github.com/repos/Copycord/Copycord/releases"
ENV_VARS = ("COPYCORD_CHANNEL", "COPYCORD_VERSION", "COPYCORD_ALLOW_ROOT")


def _text(path):
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("path", [SH, PS1])
def test_script_exists(path):
    assert path.is_file()


def test_sh_is_posix_and_strict():
    text = _text(SH)
    assert text.startswith("#!/bin/sh\n")
    assert re.search(r"^set -eu$", text, re.M)
    assert "bash" not in text.split("\n", 1)[0]


def test_ps1_is_strict():
    text = _text(PS1)
    assert "$ErrorActionPreference = 'Stop'" in text
    assert "Set-StrictMode" in text


@pytest.mark.parametrize("path", [SH, PS1])
def test_uses_the_releases_api(path):
    text = _text(path)
    assert "api.github.com/repos/" in text and "/releases" in text
    assert "Copycord/Copycord" in text
    assert "/latest" in text and "/tags/" in text
    assert "pypi" not in text.lower()


@pytest.mark.parametrize("path", [SH, PS1])
@pytest.mark.parametrize("var", ENV_VARS)
def test_reads_the_env_vars(path, var):
    assert var in _text(path)


@pytest.mark.parametrize("path", [SH, PS1])
def test_installs_the_wheel_with_uv_tool(path):
    text = _text(path)
    assert "uv tool install --force" in text
    assert ".whl" in text
    assert "uv tool update-shell" in text
    assert "astral.sh/uv/install." in text


@pytest.mark.parametrize("path", [SH, PS1])
def test_refuses_root_unless_allowed(path):
    text = _text(path)
    marker = "id -u" if path == SH else "Administrator"
    assert marker in text
    assert "COPYCORD_ALLOW_ROOT" in text


@pytest.mark.parametrize("path", [SH, PS1])
def test_saves_the_channel(path):
    assert "update --channel" in _text(path) and "--no-install" in _text(path)


@pytest.mark.parametrize("path", [SH, PS1])
def test_prints_next_steps(path):
    text = _text(path)
    for step in ("copycord doctor", "copycord run", "copycord service install --user"):
        assert step in text


@pytest.mark.parametrize("path", [SH, PS1])
def test_never_touches_docker(path):
    code = [line for line in _text(path).splitlines() if not line.lstrip().startswith("#")]
    assert not any(re.search(r"\bdocker\b", line, re.I) for line in code)


@pytest.mark.parametrize("path", [SH, PS1])
def test_no_em_dashes(path):
    assert "—" not in _text(path)
