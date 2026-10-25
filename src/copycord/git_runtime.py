"""
git for installs of a git ref, which uv clones with the git command.

Uses git on PATH, else on Windows a per-user MinGit under <home>/git/,
downloading it from the Git for Windows release and checking it against the
sha256 that release publishes. macOS and Linux never download git; a missing
git is an error naming the package manager command. install.ps1 uses the same
location and version.
"""
from __future__ import annotations

import json
import os
import platform
import re
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Mapping, Optional

from copycord import ui
from copycord.common import paths
from copycord.node_runtime import _sha256

# The Git for Windows release MinGit comes from. To bump it, set the tag of a
# newer release from https://github.com/git-for-windows/git/releases (the
# checksum is read from that release, so nothing else changes), and keep it in
# step with $MinGitTag in install.ps1.
MINGIT_TAG = "v2.51.0.windows.1"
RELEASE_URL = "https://api.github.com/repos/git-for-windows/git/releases/tags/{tag}"
DOWNLOAD_URL = "https://github.com/git-for-windows/git/releases/download/{tag}/{name}"
TIMEOUT = 60.0

# 64-bit MinGit; Windows on ARM runs it too.
_WINDOWS_MACHINES = {"amd64", "x86_64", "arm64", "aarch64"}
_HEX64 = re.compile(r"[0-9a-fA-F]{64}")

# Linux package managers, first found wins.
_LINUX_PACKAGERS = (
    ("apt-get", "sudo apt install git"),
    ("dnf", "sudo dnf install git"),
    ("yum", "sudo yum install git"),
    ("pacman", "sudo pacman -S git"),
    ("zypper", "sudo zypper install git"),
    ("apk", "sudo apk add git"),
)


class GitError(Exception):
    pass


def install_hint(sys_platform: Optional[str] = None,
                 which: Optional[Callable[[str], Optional[str]]] = None) -> str:
    """How to install git on this platform."""
    sys_platform = sys.platform if sys_platform is None else sys_platform
    which = shutil.which if which is None else which
    if sys_platform == "win32":
        command = "winget install --id Git.Git"
    elif sys_platform == "darwin":
        command = "xcode-select --install (or brew install git)"
    else:
        command = next((cmd for tool, cmd in _LINUX_PACKAGERS if which(tool)),
                       "your package manager")
    return f"install git with {command} and run the update again"


def mingit_version(tag: str = MINGIT_TAG) -> str:
    """v2.51.0.windows.1 -> 2.51.0, v2.51.0.windows.2 -> 2.51.0.2 (as in the asset names)."""
    m = re.fullmatch(r"v?(\d+(?:\.\d+)*)\.windows\.(\d+)", tag)
    if m is None:
        raise GitError(f"{tag} is not a Git for Windows release tag")
    return m.group(1) if m.group(2) == "1" else f"{m.group(1)}.{m.group(2)}"


def asset_name(tag: str = MINGIT_TAG) -> str:
    return f"MinGit-{mingit_version(tag)}-64-bit.zip"


def git_home(env: Optional[Mapping[str, str]] = None) -> Path:
    return paths.copycord_home(env) / "git"


def bin_dir(home: Path, tag: str = MINGIT_TAG) -> Path:
    # MinGit has git.exe in cmd/ (a wrapper that sets up its own PATH) and mingw64/bin/.
    return home / asset_name(tag)[: -len(".zip")] / "cmd"


def _has_git(directory: Path) -> bool:
    return (directory / "git.exe").is_file()


def _fetch(url: str, dest: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "copycord"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp, open(dest, "wb") as out:
        shutil.copyfileobj(resp, out)


def published_sha256(release: dict, filename: str) -> Optional[str]:
    """The sha256 the release publishes for filename: the asset digest and the
    checksum table in its notes. None when it has neither; GitError when they differ."""
    found = set()
    for asset in release.get("assets") or []:
        if isinstance(asset, dict) and asset.get("name") == filename:
            m = re.fullmatch(r"sha256:([0-9a-fA-F]{64})", str(asset.get("digest") or ""))
            if m:
                found.add(m.group(1).lower())
    # The notes end with a "Filename | SHA-256" table.
    for row in str(release.get("body") or "").splitlines():
        cells = [c.strip().strip("`* ") for c in row.strip().strip("|").split("|")]
        if len(cells) >= 2 and cells[0] == filename and _HEX64.fullmatch(cells[1]):
            found.add(cells[1].lower())
    if len(found) > 1:
        raise GitError(f"the release lists different checksums for {filename}")
    return found.pop() if found else None


def download_mingit(tag: str, home: Path) -> Path:
    """Download, verify and unpack MinGit into home; returns the dir holding git.exe."""
    name = asset_name(tag)
    url = DOWNLOAD_URL.format(tag=tag, name=name)
    hint = install_hint("win32")
    home.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".download-", dir=home) as tmp:
        tmp_dir = Path(tmp)
        notes = tmp_dir / "release.json"
        release_url = RELEASE_URL.format(tag=tag)
        try:
            _fetch(release_url, notes)
            release = json.loads(notes.read_text(encoding="utf-8", errors="replace"))
        except Exception as e:
            raise GitError(f"could not read the Git for Windows {tag} release ({release_url}): {e}; {hint}")
        if not isinstance(release, dict):
            raise GitError(f"could not read the Git for Windows {tag} release ({release_url}); {hint}")
        expected = published_sha256(release, name)
        if not expected:
            raise GitError(f"the Git for Windows {tag} release publishes no sha256 for {name}, "
                           f"so it cannot be checked; {hint}")
        archive = tmp_dir / name
        try:
            _fetch(url, archive)
        except Exception as e:
            raise GitError(f"could not download {url}: {e}; {hint}")
        actual = _sha256(archive)
        if actual != expected:
            raise GitError(f"sha256 mismatch for {name}: expected {expected}, got {actual}; {hint}")
        staging = tmp_dir / "unpacked"
        try:
            with zipfile.ZipFile(archive) as z:
                z.extractall(staging)
        except (OSError, zipfile.BadZipFile) as e:
            raise GitError(f"could not unpack {name}: {e}; {hint}")
        if not _has_git(staging / "cmd"):
            raise GitError(f"{name} has no cmd/git.exe; {hint}")
        target = bin_dir(home, tag).parent
        shutil.rmtree(target, ignore_errors=True)
        os.replace(staging, target)
    return bin_dir(home, tag)


def _windows() -> bool:
    return sys.platform == "win32"


def ensure_git(env: Optional[Mapping[str, str]] = None) -> Path:
    """The directory holding git, downloading MinGit on Windows if needed."""
    found = shutil.which("git")
    if found:
        ui.ok("git found", found)
        return Path(found).parent
    if not _windows():
        raise GitError(f"git is not installed; {install_hint()}")
    machine = platform.machine()
    if machine.lower() not in _WINDOWS_MACHINES:
        raise GitError(f"no MinGit build for {machine}; {install_hint('win32')}")
    home = git_home(env)
    version = mingit_version()
    existing = bin_dir(home)
    if _has_git(existing):
        ui.ok(f"git {version} found", str(existing / "git.exe"))
        return existing
    with ui.Spinner(f"Downloading git {version} (MinGit) into {home}"):
        result = download_mingit(MINGIT_TAG, home)
    ui.ok(f"installed git {version} for Copycord", str(result / "git.exe"))
    return result


def with_path(directory: Path, env: Optional[Mapping[str, str]] = None) -> dict:
    """A copy of env (default os.environ) with directory first on PATH, unless it is on PATH already."""
    out = dict(os.environ if env is None else env)
    key = next((k for k in out if k.upper() == "PATH"), "PATH")
    rest = out.get(key, "")
    norm = os.path.normcase(os.path.normpath(str(directory)))
    if any(os.path.normcase(os.path.normpath(p)) == norm for p in rest.split(os.pathsep) if p):
        return out
    out[key] = str(directory) + (os.pathsep + rest if rest else "")
    return out
