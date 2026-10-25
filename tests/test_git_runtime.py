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
    monkeypatch.setattr(git_runtime.ui, "is_interactive", lambda: False)
    monkeypatch.setattr(git_runtime.shutil, "which",
                        lambda name: "/usr/bin/dnf" if name == "dnf" else None)
    with pytest.raises(git_runtime.GitError, match="sudo dnf install git"):
        git_runtime.ensure_git()
    assert seen == []


class Pkg:
    """A fake Linux or macOS machine: the tools on PATH, root or not, and every command run."""

    def __init__(self, monkeypatch, platform="linux", tools=("apt-get", "sudo"), root=False):
        self.tools = set(tools)
        self.calls = []
        self.fail = {}  # argv[0] after sudo -n -> exit code
        monkeypatch.setattr(git_runtime, "_windows", lambda: False)
        monkeypatch.setattr(git_runtime.sys, "platform", platform)
        monkeypatch.setattr(git_runtime.ui, "is_interactive", lambda: True)
        monkeypatch.delenv("CI", raising=False)
        monkeypatch.setattr(git_runtime, "_is_root", lambda: root)
        monkeypatch.setattr(git_runtime, "_git_version", lambda git: "2.43.0")
        monkeypatch.setattr(git_runtime.shutil, "which",
                            lambda name: f"/usr/bin/{name}" if name in self.tools else None)
        monkeypatch.setattr(git_runtime.subprocess, "run", self.run)

    def run(self, argv, **kwargs):
        argv = list(argv)
        self.calls.append(argv)
        cmd = argv[2:] if argv[:2] == ["sudo", "-n"] else argv
        if cmd[0] == "env":
            cmd = cmd[2:]
        rc = self.fail.get(cmd[0], 0)
        if rc == 0 and "git" in cmd and cmd[0] != "sudo":
            self.tools.add("git")
        return git_runtime.subprocess.CompletedProcess(argv, rc, "E: something broke\n" if rc else "")


APT = "env DEBIAN_FRONTEND=noninteractive apt-get".split()


def test_non_interactive_raises_the_hint_and_runs_nothing(monkeypatch):
    pkg = Pkg(monkeypatch)
    monkeypatch.setattr(git_runtime.ui, "is_interactive", lambda: False)
    with pytest.raises(git_runtime.GitError, match="sudo apt install git"):
        git_runtime.ensure_git()
    assert pkg.calls == []


@pytest.mark.parametrize("ci", ["1", "true"])
def test_ci_never_installs(monkeypatch, ci):
    pkg = Pkg(monkeypatch)
    monkeypatch.setenv("CI", ci)
    with pytest.raises(git_runtime.GitError, match="sudo apt install git"):
        git_runtime.ensure_git()
    assert pkg.calls == []


def test_interactive_false_wins_over_a_terminal(monkeypatch):
    pkg = Pkg(monkeypatch)
    with pytest.raises(git_runtime.GitError, match="git is not installed"):
        git_runtime.ensure_git(interactive=False)
    assert pkg.calls == []


def test_apt_asks_sudo_then_updates_and_installs(monkeypatch, capsys):
    pkg = Pkg(monkeypatch)
    assert git_runtime.ensure_git() == Path("/usr/bin")
    assert pkg.calls == [
        ["sudo", "-v"],
        ["sudo", "-n", *APT, "update", "-qq"],
        ["sudo", "-n", *APT, "install", "-y", "-qq", "git"],
    ]
    out = capsys.readouterr()
    text = out.out + out.err
    assert "needs administrator rights to install git with apt-get" in text
    assert "installing git with apt-get" in text
    assert "installed git 2.43.0" in text
    assert text.index("administrator rights") < text.index("installed git")


def test_root_skips_sudo(monkeypatch):
    pkg = Pkg(monkeypatch, tools=("apt-get",), root=True)
    assert git_runtime.ensure_git() == Path("/usr/bin")
    assert pkg.calls == [[*APT, "update", "-qq"], [*APT, "install", "-y", "-qq", "git"]]


def test_no_sudo_and_not_root_is_a_clear_error(monkeypatch):
    pkg = Pkg(monkeypatch, tools=("apt-get",))
    with pytest.raises(git_runtime.GitError, match="sudo is not installed; install sudo or run this again as root"):
        git_runtime.ensure_git()
    assert pkg.calls == []


def test_sudo_refused_installs_nothing(monkeypatch):
    pkg = Pkg(monkeypatch)
    pkg.fail["sudo"] = 1
    with pytest.raises(git_runtime.GitError, match=r"sudo did not get administrator rights \(exit 1\)"):
        git_runtime.ensure_git()
    assert pkg.calls == [["sudo", "-v"]]


@pytest.mark.parametrize("tools, commands", [
    (("apk",), [["apk", "add", "--no-cache", "git"]]),
    (("apt-get",), [[*APT, "update", "-qq"], [*APT, "install", "-y", "-qq", "git"]]),
    (("pacman",), [["pacman", "-Sy", "--noconfirm", "git"]]),
    (("dnf",), [["dnf", "install", "-y", "-q", "git"]]),
    (("yum",), [["yum", "install", "-y", "-q", "git"]]),
    # The first one found wins.
    (("apk", "apt-get", "dnf"), [["apk", "add", "--no-cache", "git"]]),
    (("dnf", "yum"), [["dnf", "install", "-y", "-q", "git"]]),
])
def test_each_package_manager(monkeypatch, tools, commands):
    pkg = Pkg(monkeypatch, tools=(*tools, "sudo"))
    git_runtime.ensure_git()
    assert pkg.calls == [["sudo", "-v"]] + [["sudo", "-n", *c] for c in commands]


def test_no_known_package_manager_gives_the_hint(monkeypatch):
    pkg = Pkg(monkeypatch, tools=("sudo",))
    with pytest.raises(git_runtime.GitError, match="no known package manager.*your package manager"):
        git_runtime.ensure_git()
    assert pkg.calls == []


def test_macos_uses_brew_without_sudo(monkeypatch):
    pkg = Pkg(monkeypatch, platform="darwin", tools=("brew", "sudo"))
    assert git_runtime.ensure_git() == Path("/usr/bin")
    assert pkg.calls == [["brew", "install", "git"]]


def test_macos_without_brew_gives_the_hint(monkeypatch):
    pkg = Pkg(monkeypatch, platform="darwin", tools=("sudo",))
    with pytest.raises(git_runtime.GitError, match="Homebrew is not installed.*xcode-select --install"):
        git_runtime.ensure_git()
    assert pkg.calls == []


def test_package_manager_failure_names_it_and_the_exit_code(monkeypatch):
    pkg = Pkg(monkeypatch, tools=("dnf", "sudo"))
    pkg.fail["dnf"] = 7
    with pytest.raises(git_runtime.GitError, match=r"dnf could not install git \(exit 7\): E: something broke"):
        git_runtime.ensure_git()


def test_installed_but_not_on_path(monkeypatch):
    Pkg(monkeypatch, platform="darwin", tools=("brew",))
    monkeypatch.setattr(git_runtime.subprocess, "run",
                        lambda argv, **kw: git_runtime.subprocess.CompletedProcess(argv, 0, ""))
    with pytest.raises(git_runtime.GitError, match="not on PATH"):
        git_runtime.ensure_git()


def test_windows_never_uses_a_package_manager(home, net, monkeypatch):
    responses, _ = net
    data = mingit_zip()
    sha = hashlib.sha256(data).hexdigest()
    responses[RELEASE_URL] = release(sha)
    responses[ZIP_URL] = data
    monkeypatch.setattr(git_runtime.ui, "is_interactive", lambda: True)
    monkeypatch.setattr(git_runtime, "install_via_package_manager",
                        lambda *a, **k: pytest.fail("package manager used on Windows"))
    assert git_runtime.ensure_git() == home / "git" / NAME[: -len(".zip")] / "cmd"


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
