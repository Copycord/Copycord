"""Static checks of install.sh and install.ps1. Nothing is executed."""
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
    text = _text(path)
    assert "--channel" in text and "--no-install" in text


@pytest.mark.parametrize("path", [SH, PS1])
def test_prints_next_steps(path):
    text = _text(path)
    for step in ("copycord update", "localhost:"):
        assert step in text
    assert "Finish the setup in the dashboard" in text
    assert "onboard" not in text.lower()


@pytest.mark.parametrize("path", [SH, PS1])
def test_env_template_is_not_in_the_installer(path):
    text = _text(path)
    # The installed CLI writes .env; the only PASSWORD line left is the box check.
    assert "ENVEOF" not in text and "$envTemplate" not in text
    assert text.count("PASSWORD=copycord") == 1
    assert "BACKUP_DIR" not in text
    assert "Keep in step with envfile" not in text


@pytest.mark.parametrize("path", [SH, PS1])
def test_writes_the_env_template_once_with_the_cli(path):
    text = _text(path)
    assert "Test-Path $envFile" in text or '[ -e "$ENV_FILE" ]' in text
    if path == SH:
        assert '"$BIN_DIR/copycord" config init' in text
    else:
        assert "& $exe config init" in text
    # After the CLI is installed, before the box that reads the file.
    assert text.index("config init") > text.index("uv tool dir --bin")
    assert text.index("config init") < text.index("Needs your attention:")


@pytest.mark.parametrize("path", [SH, PS1])
def test_success_box_names_the_env_file_and_what_needs_attention(path):
    text = _text(path)
    assert "Settings: $ENV_FILE" in text or "Settings: $envFile" in text
    assert "Default dashboard password: copycord" not in text
    box = text[text.index("Needs your attention:"):]
    # Only while the default password is still set.
    assert "PASSWORD=copycord" in box[:box.index("Change the default dashboard password:")]
    assert "then run: copycord restart" in box
    assert "Finish the setup in the dashboard once it runs:" in box


@pytest.mark.parametrize("path", [SH, PS1])
def test_only_a_dry_run_prints_the_plan(path):
    text = _text(path)
    guard = 'if [ "$DRY_RUN" = "1" ]; then' if path == SH else "if ($dryRun) {"
    close = "fi" if path == SH else "}"
    calls = ('Plan$RESET', 'plan_line "home"') if path == SH else ("'  Plan'", "PlanLine 'home'")
    for call in calls:
        assert _guarded_by(text, call, guard, close)


@pytest.mark.parametrize("path", [SH, PS1])
def test_uses_the_trimmed_cli(path):
    text = _text(path)
    assert "--version" in text
    assert "'--ref'" in text or "--ref $REF" in text
    assert "copycord version" not in text and "--branch" not in text


@pytest.mark.parametrize("path", [SH, PS1])
def test_never_touches_docker(path):
    code = [line for line in _text(path).splitlines() if not line.lstrip().startswith("#")]
    assert not any(re.search(r"\bdocker\b", line, re.I) for line in code)


@pytest.mark.parametrize("path", [SH, PS1])
def test_no_em_dashes(path):
    assert "—" not in _text(path)


def _guarded_by(text, call, guard, close):
    """True when CALL sits inside the block GUARD opens (closed by CLOSE at its indent)."""
    lines = text.splitlines()
    at = next(i for i, line in enumerate(lines) if call in line)
    for i in range(at - 1, -1, -1):
        if guard in lines[i]:
            indent = len(lines[i]) - len(lines[i].lstrip())
            closing = indent * " " + close
            return not any(line.rstrip() == closing for line in lines[i + 1:at])
    return False


def test_sh_repairs_with_the_doctor_after_an_upgrade_only():
    text = _text(SH)
    call = 'NO_COLOR=1 "$BIN_DIR/copycord" doctor --fix --brief </dev/null'
    assert call in text
    assert _guarded_by(text, call, 'if [ "$UPGRADE" = "1" ]; then', "fi")
    # An existing program or home makes it an upgrade; a first install has neither.
    assert re.search(r'^UPGRADE=0$', text, re.M)
    detect = text[text.index("UPGRADE=0"):text.index("UPGRADE=1")]
    for probe in ("command -v copycord", '"$COPYCORD_HOME/.env"', '"$COPYCORD_HOME/data.db"'):
        assert probe in detect
    # Before the install replaces anything, after the settings and the channel.
    assert text.index("UPGRADE=0") < text.index("install_release()")
    assert text.index("<<ENVEOF") < text.index(call) > text.index("--no-install\n")
    assert "| while IFS= read -r line; do" in text[text.index(call):].splitlines()[0]
    assert "done || true" in text[text.index(call):]


def test_ps1_repairs_with_the_doctor_after_an_upgrade_only():
    text = _text(PS1)
    call = "& $exe doctor --fix --brief 2>&1"
    assert call in text
    assert _guarded_by(text, call, "if ($upgrade) {", "}")
    detect = text[text.index("$upgrade = "):text.index("PlanLine 'repair'")]
    for probe in ("Get-Command copycord", "Join-Path $homeDir '.env'", "Join-Path $homeDir 'data.db'"):
        assert probe in detect
    assert text.index("$upgrade = ") < text.index("Step 'Finishing up'")
    assert text.index("$envTemplate") < text.index(call) > text.index("'--no-install'")
    block = text[text.index(call) - 300:text.index(call) + 1400]
    assert "$env:NO_COLOR = '1'" in block
    assert "$ErrorActionPreference = 'Continue'" in block
    assert "$global:LASTEXITCODE = 0" in block


@pytest.mark.parametrize("path", [SH, PS1])
def test_plan_names_the_repair_step(path):
    assert "copycord doctor --fix (an upgrade or reinstall)" in _text(path)


@pytest.mark.parametrize("path", [SH, PS1])
def test_doctor_has_no_yes_flag(path):
    assert "doctor --fix --yes" not in _text(path)


@pytest.mark.parametrize("path", [SH, PS1])
def test_stops_when_already_installed(path):
    text = _text(path)
    assert "is already installed" in text
    assert "Run copycord update to update it, or rerun with" in text
    assert "git ls-remote" in text
    assert "--version" in text and "channel" in text
    assert "COPYCORD_FORCE" in text


@pytest.mark.parametrize("path, flag", [(SH, "--force) FORCE=1"), (PS1, "[switch]$Force")])
def test_force_reinstalls(path, flag):
    text = _text(path)
    assert flag in text
    help_flag = "--force" if path == SH else "-Force"
    assert re.search(r"^\s+" + re.escape(help_flag) + r"\s+reinstall", text, re.M)


@pytest.mark.parametrize("path", [SH, PS1])
def test_the_check_comes_before_any_install_step(path):
    text = _text(path)
    check = text.index("is already installed")
    assert check < text.index("Preparing uv")
    assert check < text.index("uv tool install --force")
