"""Static checks of install.ps1 for the production audit fixes. Nothing is executed."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PS1 = ROOT / "install.ps1"


def _text():
    return PS1.read_text(encoding="utf-8")


def _final_box(text):
    return text[text.index("function BoxLine"):]


def test_ps1_has_no_em_dashes():
    assert chr(0x2014) not in _text()


def test_final_box_reads_the_password_back_once():
    box = _final_box(_text())
    assert "PASSWORD=copycord" not in _text()
    assert "EnvValue 'PASSWORD'" in box
    assert box.count('BoxLine "    $password"') == 1
    assert "config get $Key --show-secrets --json" in _text()


def test_final_box_url_uses_the_settings_file():
    box = _final_box(_text())
    assert "$env:ADMIN_PORT" not in box
    assert "EnvValue 'ADMIN_PORT'" in box
    assert "EnvValue 'DASHBOARD_HOST'" in box
    assert 'BoxLink "http://${dashHost}:$port"' in box
    assert "$dashHost = 'localhost'" in box


def test_env_value_falls_back_to_the_env_file_line():
    text = _text()
    body = text[text.index("function EnvValue"):text.index("$port = EnvValue")]
    assert "Get-Content $envFile" in body
    assert "ConvertFrom-Json" in body


def test_git_dir_guard_runs_before_any_reset():
    text = _text()
    guard = text.index("copycord-installer")
    assert guard < text.index("'remote', 'set-url'")
    assert guard < text.index("'checkout', '--force'")
    assert guard < text.index("'clean', '-fdx'")
    assert "remote get-url origin" in text
    assert "status --porcelain --untracked-files=no" in text
    assert "so it stops here" in text


def test_clone_writes_the_installer_marker():
    text = _text()
    clone = text.index("'clone', '--filter=blob:none'")
    assert text.index("WriteAllText($gitMarker", clone) > clone


def test_service_is_stopped_before_and_started_after_the_install():
    text = _text()
    stop = text.index("'service', 'stop'")
    assert "service status --json" in text
    assert text.count("StopRunning\n") + text.count("StopRunning }") >= 3
    start = text.index("'service', 'start'")
    assert start > text.index("uv tool install copycord from $gitDir")
    assert start > stop
    # A failed install still starts it again.
    finally_block = text[text.index("# A failed install still starts"):]
    assert "service start" in finally_block.split("# --- done")[0]


def test_settings_and_data_are_limited_to_the_user():
    text = _text()
    block = text[text.index("tokens: only this user may open them"):]
    block = block[:block.index("# The CLI writes update.json")]
    assert "icacls $item /inheritance:r /grant:r $grant" in block
    assert "(OI)(CI)F" in block
    assert "secret.key" in block
    assert "Warn " in block
    assert text.index("config init") < text.index("tokens: only this user may open them")


def test_upgrade_detection_checks_the_current_data_path():
    assert "'data\\data.db'" in _text()


def test_uv_is_pinned_and_not_piped():
    text = _text()
    assert re.search(r"else \{ '\d+\.\d+\.\d+' \}\n\s+\$UvInstaller = \"https://astral\.sh/uv/\$UvVersion/install\.ps1\"", text)
    assert "| iex\")" not in text
    assert "'-File', $uvScript" in text


def test_one_release_lookup_sorted_by_version():
    text = _text()
    assert text.count("Invoke-RestMethod") == 3
    body = text[text.index("function FindRelease"):text.index("function AlreadyInstalled")]
    assert "Sort-Object" in body and "ReleaseKey" in body
    assert "GITHUB_TOKEN" in body and "HTTP $status" in body
    assert text.count("$release = FindRelease") == 1 and "$found = FindRelease" in text


def test_option_names_and_errors_match_install_sh():
    text = _text()
    for alias in ("[Alias('Method')]", "[Alias('Dir')]", "[Alias('Home')]", "[Alias('Verbose')]"):
        assert alias in text
    assert "$env:COPYCORD_VERBOSE -eq '1'" in text
    assert 'throw "copycord install: -CopycordHome must be an absolute path' in text


def test_release_wheel_is_checked_before_install():
    text = _text()
    check = text.index("Get-FileHash -Algorithm SHA256 $wheelFile")
    assert "$wheel.digest" in text
    assert check < text.index("uv tool install copycord $tag")
    assert "$wheelFile)" in text
    assert "$wheel.browser_download_url)" not in text
