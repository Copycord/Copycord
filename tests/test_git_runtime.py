"""Tests for copycord.git_runtime. The network and the platform are faked."""
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from copycord import git_runtime

ROOT = Path(__file__).resolve().parents[1]
TAG = git_runtime.MINGIT_TAG
NAME = git_runtime.asset_name()
RELEASE_URL = git_runtime.RELEASE_URL.format(tag=TAG)
ZIP_URL = git_runtime.DOWNLOAD_URL.format(tag=TAG, name=NAME)


def mingit_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("cmd/git.exe", "MZ")
        z.writestr("mingw64/bin/git.exe", "MZ")
    return buf.getvalue()


def release(sha=None, digest=None):
    """A release like Git for Windows publishes: a checksum table in the notes, asset digests."""
    body = "Changes...\n\nFilename | SHA-256\n-------- | -------\n"
    body += f"Git-2.51.0-64-bit.exe | {'a' * 64}\n"
    if sha:
        body += f"{NAME} | {sha}\n"
    asset = {"name": NAME, "browser_download_url": ZIP_URL}
    if digest:
        asset["digest"] = f"sha256:{digest}"
    return json.dumps({"tag_name": TAG, "body": body, "assets": [asset]}).encode()


@pytest.fixture()
def home(tmp_path, monkeypatch):
    h = tmp_path / "cchome"
    monkeypatch.setenv("COPYCORD_HOME", str(h))
    monkeypatch.setattr(git_runtime.shutil, "which", lambda name: None)
    monkeypatch.setattr(git_runtime, "_windows", lambda: True)
    monkeypatch.setattr(git_runtime.platform, "machine", lambda: "AMD64")
    return h


@pytest.fixture()
def net(monkeypatch):
    """URL -> bytes; every fetched URL is recorded."""
    responses, seen = {}, []

    def fetch(url, dest):
        seen.append(url)
        if url not in responses:
            raise OSError(f"404 {url}")
        Path(dest).write_bytes(responses[url])

    monkeypatch.setattr(git_runtime, "_fetch", fetch)
    return responses, seen


def test_git_on_path_is_used(monkeypatch, net):
    _, seen = net
    monkeypatch.setattr(git_runtime.shutil, "which",
                        lambda name: "/usr/bin/git" if name == "git" else None)
    assert git_runtime.ensure_git() == Path("/usr/bin")
    assert seen == []


@pytest.mark.parametrize("notes, digest", [(True, False), (False, True), (True, True)])
def test_windows_downloads_and_checks_mingit(home, net, notes, digest):
    responses, seen = net
    data = mingit_zip()
    sha = hashlib.sha256(data).hexdigest()
    responses[RELEASE_URL] = release(sha if notes else None, sha if digest else None)
    responses[ZIP_URL] = data
    bin_dir = git_runtime.ensure_git()
    assert bin_dir == home / "git" / NAME[: -len(".zip")] / "cmd"
    assert (bin_dir / "git.exe").is_file()
    assert seen == [RELEASE_URL, ZIP_URL]
    # The next run uses it again without downloading.
    assert git_runtime.ensure_git() == bin_dir
    assert len(seen) == 2


def test_checksum_mismatch_installs_nothing(home, net):
    responses, _ = net
    responses[RELEASE_URL] = release("0" * 64)
    responses[ZIP_URL] = mingit_zip()
    with pytest.raises(git_runtime.GitError, match="sha256 mismatch"):
        git_runtime.ensure_git()
    assert not (home / "git" / NAME[: -len(".zip")]).exists()


def test_no_published_checksum_fails_closed(home, net):
    responses, seen = net
    responses[RELEASE_URL] = release()
    responses[ZIP_URL] = mingit_zip()
    with pytest.raises(git_runtime.GitError, match="cannot be checked"):
        git_runtime.ensure_git()
    assert ZIP_URL not in seen


def test_different_checksums_fail_closed(home, net):
    responses, _ = net
    data = mingit_zip()
    responses[RELEASE_URL] = release(hashlib.sha256(data).hexdigest(), "1" * 64)
    responses[ZIP_URL] = data
    with pytest.raises(git_runtime.GitError, match="different checksums"):
        git_runtime.ensure_git()


def test_unreadable_release_names_the_fix(home, net):
    with pytest.raises(git_runtime.GitError, match="winget install --id Git.Git"):
        git_runtime.ensure_git()


def test_zip_without_git_is_rejected(home, net):
    responses, _ = net
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("README", "")
    data = buf.getvalue()
    responses[RELEASE_URL] = release(hashlib.sha256(data).hexdigest())
    responses[ZIP_URL] = data
    with pytest.raises(git_runtime.GitError, match="no cmd/git.exe"):
        git_runtime.ensure_git()


def test_missing_git_off_windows_raises_with_the_package_manager(monkeypatch, net):
    _, seen = net
    monkeypatch.setattr(git_runtime, "_windows", lambda: False)
    monkeypatch.setattr(git_runtime.sys, "platform", "linux")
    monkeypatch.setattr(git_runtime.shutil, "which",
                        lambda name: "/usr/bin/dnf" if name == "dnf" else None)
    with pytest.raises(git_runtime.GitError, match="sudo dnf install git"):
        git_runtime.ensure_git()
    assert seen == []


def test_install_hints():
    none = lambda name: None  # noqa: E731
    assert "xcode-select --install" in git_runtime.install_hint("darwin", none)
    assert "winget install --id Git.Git" in git_runtime.install_hint("win32", none)
    apt = lambda name: "/usr/bin/apt-get" if name == "apt-get" else None  # noqa: E731
    assert "sudo apt install git" in git_runtime.install_hint("linux", apt)
    assert "your package manager" in git_runtime.install_hint("linux", none)


@pytest.mark.parametrize("tag, version", [
    ("v2.51.0.windows.1", "2.51.0"),
    ("v2.47.1.windows.2", "2.47.1.2"),
])
def test_mingit_version(tag, version):
    assert git_runtime.mingit_version(tag) == version
    assert git_runtime.asset_name(tag) == f"MinGit-{version}-64-bit.zip"


def test_bad_tag_is_an_error():
    with pytest.raises(git_runtime.GitError):
        git_runtime.mingit_version("v2.51.0")


def test_with_path_prepends_once():
    env = git_runtime.with_path(Path("/g/cmd"), {"PATH": "/usr/bin", "X": "1"})
    assert env["PATH"].split(git_runtime.os.pathsep) == ["/g/cmd", "/usr/bin"]
    assert env["X"] == "1"
    again = git_runtime.with_path(Path("/g/cmd"), env)
    assert again["PATH"] == env["PATH"]


def test_installers_agree_on_mingit_tag_and_location():
    ps1 = (ROOT / "install.ps1").read_text(encoding="utf-8")
    assert f"$MinGitTag = '{TAG}'" in ps1
    assert "$gitHome = Join-Path $homeDir 'git'" in ps1
    assert '$minGitName = "MinGit-$minGitVersion-64-bit"' in ps1
