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
    # install.sh fetches the latest uv installer; install.ps1 pins a version.
    assert "astral.sh/uv/install." in text or "astral.sh/uv/$UvVersion/install.ps1" in text


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
    assert "copycord update" in text
    assert "localhost:" in text or 'HOST="localhost"' in text or "$dashHost = 'localhost'" in text
    assert "Finish the setup in the dashboard" in text
    assert "onboard" not in text.lower()


@pytest.mark.parametrize("path", [SH, PS1])
def test_env_template_is_not_in_the_installer(path):
    text = _text(path)
    # The installed CLI writes .env; the only PASSWORD line left is the sh box check
    # for an older install that still has the default password.
    assert "ENVEOF" not in text and "$envTemplate" not in text
    assert text.count("PASSWORD=copycord") == (1 if path == SH else 0)
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
    assert "Finish the setup in the dashboard once it runs:" in box
    if path == PS1:
        # The password read back from the settings file is shown in the box.
        assert 'BoxLine "    $password"' in box
        return
    # Only while the default password is still set.
    assert "PASSWORD=copycord" in box[:box.index("Change the default dashboard password:")]
    assert "then run: copycord restart" in box


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
    assert text.index('"$BIN_DIR/copycord" config init') < text.index(call) > text.index("--no-install\n")
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
    assert text.index("& $exe config init") < text.index(call) > text.index("'--no-install'")
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


def _sh_function(text, name):
    start = text.index(f"\n{name}() {{\n")
    return text[start:text.index("\n}\n", start)]


def test_sh_shows_the_new_password_and_the_saved_address():
    text = _text(SH)
    # Read back only when config init just made the file, and shown once in the box.
    init = text.index('"$BIN_DIR/copycord" config init')
    assert 'NEW_PASSWORD="$(env_value PASSWORD --show-secrets)"' in text[init:init + 300]
    assert 'NEW_PASSWORD=""' in text[:init]
    assert '"$BIN_DIR/copycord" config get "$@"' in _sh_function(text, "env_value")
    box = text[text.index("Needs your attention:"):]
    assert box.count("$NEW_PASSWORD") == 2
    assert "shown once" in box
    # ADMIN_PORT and DASHBOARD_HOST come from .env; localhost unless a host is set.
    assert 'PORT="$(env_value ADMIN_PORT)"' in text
    assert 'HOST="$(env_value DASHBOARD_HOST)"' in text
    assert 'URL="http://$HOST:$PORT"' in text
    assert "localhost:${ADMIN_PORT" not in text
    assert text.index('URL="http://$HOST:$PORT"') > text.index('"$DRY_RUN" = "1" ]; then\n    say "  ${YELLOW}${BOLD}Dry run complete')


def test_sh_never_cleans_a_git_dir_it_did_not_create():
    text = _text(SH)
    owned = _sh_function(text, "owned_checkout")
    assert '"$GIT_DIR_PATH/.git/$GIT_MARKER"' in owned
    assert "remote get-url origin" in owned and '"$REPO_URL.git"' in owned
    assert "status --porcelain" in owned
    checkout = _sh_function(text, "checkout_ref")
    assert ': >"$GIT_DIR_PATH/.git/$GIT_MARKER"' in checkout
    assert checkout.index("$GIT_MARKER") < checkout.index("clean -fdx")
    install = _sh_function(text, "install_git")
    guard = install.index("! owned_checkout")
    assert "nothing was changed" in install[guard:guard + 400]
    # Before anything is downloaded or checked out, dry run included.
    assert guard < install.index("Installing Node") < install.index("run \"Checking out $REF\" checkout_ref")
    assert "never point it at" in text[:text.index("--verify ")]


def test_sh_stops_the_service_around_the_install():
    text = _text(SH)
    stop = _sh_function(text, "stop_service")
    assert 'service status --json' in stop and 'service stop' in stop
    assert '[ "$UPGRADE" = "1" ]' in stop
    assert "service start" in _sh_function(text, "restart_service")
    # Stopped right before each uv install, started after the repair.
    for install in ('run "uv tool install copycord $TAG"', 'run "uv tool install copycord from $GIT_DIR_PATH"'):
        at = text.index(install)
        assert "stop_service" in text[at - 200:at]
    assert text.index("\nrestart_service\n") > text.index("doctor --fix --brief </dev/null")
    # A failed install starts it again.
    assert '"$SERVICE_EXE" service start' in _sh_function(text, "cleanup")


def test_sh_upgrade_detection_checks_the_data_folder():
    text = _text(SH)
    detect = text[text.index("UPGRADE=0"):text.index("UPGRADE=1")]
    assert '"$COPYCORD_HOME/data/data.db"' in detect


def test_sh_checks_the_wheel_before_installing_it():
    text = _text(SH)
    release = _sh_function(text, "install_release")
    assert '"digest"' in release and "sha256:" in release
    assert 'download "$WHEEL_URL" "$WHEEL_FILE"' in release
    check = release.index('sha256_of "$WHEEL_FILE"')
    assert check < release.index("stop_service") < release.index('uv tool install --force --python "$PYTHON_VERSION" "$WHEEL_FILE"')
    assert "nothing was installed" in release
    # The local, checked file is installed, never the URL.
    assert '"$WHEEL_URL"' not in release[release.index('run "uv tool install'):]


def test_sh_has_one_release_lookup():
    text = _text(SH)
    lookup = _sh_function(text, "release_tag")
    assert '"$API?per_page=30"' in lookup and '"$API/latest"' in lookup
    assert '"draft"' in lookup and "newest_version" in lookup
    # Both the already-installed check and the install use it; nothing else asks the API.
    assert "release_tag" in _sh_function(text, "already_installed")
    assert 'TAG="$(release_tag)"' in _sh_function(text, "install_release")
    assert text.count("$API?per_page") == 1 and text.count("$API/latest") == 1
    # The token goes to the GitHub API only.
    fetch = _sh_function(text, "fetch")
    assert "GITHUB_TOKEN" in fetch and "https://api.github.com/*" in fetch
    # Pre-releases rank below the release they lead to.
    newest = _sh_function(text, "newest_version")
    assert "rank = 3" in newest and "sort -k1,1n -k2,2n -k3,3n -k4,4n -k5,5n" in newest


def _sh_box_functions():
    text = _text(SH)
    return text[text.index("BOX_NL='\n"):text.index('\nsay ""\nif [ "$DRY_RUN" = "1" ]; then\n    say "  ${YELLOW}')]


def _render_sh_box(tmp_path, rows, hyperlinks="0", color=True):
    import subprocess
    esc = "\x1b"
    script = tmp_path / "box.sh"
    script.write_text(
        "ESC=$(printf '\\033')\n"
        f"HYPERLINKS={hyperlinks}\n"
        + ("ACCENT=$ESC'[35m'; RESET=$ESC'[0m'; YELLOW=$ESC'[33m'; BOLD=$ESC'[1m'\n" if color
           else "ACCENT=''; RESET=''; YELLOW=''; BOLD=''\n")
        + 'BOX_TL="+"; BOX_TR="+"; BOX_BL="+"; BOX_BR="+"; BOX_H="-"; BOX_V="|"\n'
        + _sh_box_functions() + "\n" + rows + "\nbox_flush\n",
        encoding="utf-8",
    )
    out = subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, check=True,
        env={"PATH": "/usr/bin:/bin", "LC_ALL": "C.UTF-8"},
    ).stdout
    assert esc in out or not color
    return out


def _plain(line):
    return re.sub(r"\x1b\]8;;[^\x1b]*\x1b\\|\x1b\[[0-9;]*m", "", line)


@pytest.mark.parametrize("hyperlinks", ["0", "1"])
def test_sh_box_borders_line_up_for_long_rows(tmp_path, hyperlinks):
    home = "/home/someone/" + "very-long-folder/" * 5 + "copycord"
    rows = "\n".join([
        'box_line "Copycord v4.0.0-beta.12-with-a-long-label is installed"',
        f'box_line "Home: {home}"',
        f'box_line "Settings: {home}/.env"',
        'box_line ""',
        'box_item "Finish the setup in the dashboard once it runs:"',
        'box_link "http://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:8080"',
        'box_item "Your dashboard password (shown once):"',
        'box_line "    hunter2hunter2hunter2"',
        'box_line "    saved as PASSWORD in the settings file"',
    ])
    lines = _render_sh_box(tmp_path, rows, hyperlinks).splitlines()
    plain = [_plain(line) for line in lines]
    assert plain[0].startswith("  +-") and plain[-1].startswith("  +-")
    assert len(plain) == 11
    assert len({len(line) for line in plain}) == 1
    assert len(plain[0]) > 2 + 56
    for line in plain[1:-1]:
        assert line.startswith("  | ") and line.endswith(" |")
    if hyperlinks == "1":
        assert "\x1b]8;;http://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:8080\x1b\\" in "".join(lines)


def test_sh_box_keeps_the_usual_width_for_short_rows(tmp_path):
    rows = 'box_line "Home: /home/a"\nbox_item "Change it:"\nbox_link "http://localhost:8080"'
    plain = [_plain(line) for line in _render_sh_box(tmp_path, rows).splitlines()]
    assert {len(line) for line in plain} == {2 + 56}
    assert plain[2].startswith("  | ! Change it:")


def test_ps1_box_width_comes_from_the_rows():
    text = _text(PS1)
    box = text[text.index("function BoxLine"):]
    assert "* 54" not in box and "-52" not in box and "-50" not in box
    assert "$width = 52" in box
    assert "$rule = [string]$box.H * ($width + 2)" in box
    assert "PadRight($Width" in box
    assert "shown once" in box and "saved as PASSWORD in the settings file" in box
