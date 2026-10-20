"""Release versions carry a build number (v4.0.0.1, v4.0.0.2, ...); beta is the
GitHub Pre-release flag only. Checks the installers and the container workflow."""
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from copycord import update
from copycord.common.config import CURRENT_VERSION

ROOT = Path(__file__).resolve().parents[1]
INSTALL_SH = (ROOT / "install.sh").read_text(encoding="utf-8")
INSTALL_PS1 = (ROOT / "install.ps1").read_text(encoding="utf-8")


def test_current_version_has_a_build_number():
    assert re.fullmatch(r"v\d+\.\d+\.\d+\.\d+", CURRENT_VERSION), CURRENT_VERSION
    assert update.is_newer(CURRENT_VERSION.lstrip("v"), "4.0.0b1")


def test_update_available_counts_the_build_number():
    # The dashboard /version check and the release watcher compare v-prefixed tags.
    assert update.is_older("v4.0.0.1", "v4.0.0.2")
    assert update.is_older("v4.0.0b1", "v4.0.0.1")
    assert not update.is_older("v4.0.0.2", "v4.0.0.2")
    assert not update.is_older("v4.0.0.2", "v4.0.0.1")


def _newest_version(tags):
    body = re.search(r"^newest_version\(\) \{\n.*?^\}\n", INSTALL_SH, re.S | re.M).group(0)
    out = subprocess.run(
        ["bash", "-c", body + "newest_version"],
        input="\n".join(tags) + "\n", capture_output=True, text=True, check=True, timeout=30,
    )
    return out.stdout.strip()


@pytest.mark.skipif(not (shutil.which("bash") and shutil.which("awk")), reason="needs bash and awk")
@pytest.mark.parametrize("tags, newest", [
    (["v4.0.0.1", "v4.0.0.2", "v4.0.0"], "v4.0.0.2"),
    (["v4.0.0.10", "v4.0.0.9"], "v4.0.0.10"),
    (["v4.0.0b1", "v4.0.0.1"], "v4.0.0.1"),
    (["v4.0.0.1", "v4.0.1"], "v4.0.1"),
    (["v4.0.0b2", "v4.0.0rc1", "v4.0.0b1"], "v4.0.0rc1"),
    (["v3.28.0", "nightly", "v4.0.0b1"], "v4.0.0b1"),
])
def test_install_sh_newest_version(tags, newest):
    assert _newest_version(tags) == newest


def _sh_release_re():
    return re.search(r'''grep -Eq '(\^v\?\[0-9\][^']*)'; then\s+METHOD="release"''', INSTALL_SH).group(1)


def _ps1_release_re():
    return re.search(r"\$version -cmatch '([^']*)'", INSTALL_PS1).group(1)


def _ps1_release_key_re():
    return re.search(r"\$Tag -notmatch '([^']*)'", INSTALL_PS1).group(1)


@pytest.mark.parametrize("value", ["4.0.0.1", "v4.0.0.12", "4.0.0", "v4.0.0b1", "4.0.0rc1"])
def test_installers_take_release_versions(value):
    assert re.fullmatch(_sh_release_re(), value)
    assert re.fullmatch(_ps1_release_re(), value)
    assert re.fullmatch(_ps1_release_key_re(), value, re.I)


@pytest.mark.parametrize("value", ["main", "feature/x", "4.0", "0123abc"])
def test_installers_treat_other_values_as_git_refs(value):
    assert not re.fullmatch(_sh_release_re(), value)
    assert not re.fullmatch(_ps1_release_re(), value)


def test_install_ps1_release_key_reads_the_build_number():
    m = re.fullmatch(_ps1_release_key_re(), "v4.0.0.7", re.I)
    assert m.groups()[:5] == ("4", "0", "0", "7", None)


def test_container_build_runs_on_the_release_and_reads_its_flag():
    wf = (ROOT / ".github/workflows/build_containers.yml").read_text(encoding="utf-8")
    on = wf.split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
    assert "release:" in on and "published" in on and "workflow_dispatch:" in on
    assert "push:" not in on  # a tag push would build the same release twice
    assert "github.event.release.prerelease" in wf
    assert "$IMAGE:beta" in wf and "$IMAGE:latest" in wf
