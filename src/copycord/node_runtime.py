"""
Node.js for builds from source, which compile the admin UI with npm.

Uses npm on PATH, else a per-user Node under <home>/node/, downloading and
verifying the official build when needed. The installers use the same
location and version.
"""
from __future__ import annotations

import hashlib
import os
import platform
import shutil
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Mapping, Optional

from copycord import ui
from copycord.common import paths

NODE_DIST = "https://nodejs.org/dist"
# Keep in step with NODE_VERSION in install.sh and $NodeVersion in install.ps1.
NODE_VERSION = "22.12.0"
VERSION_ENV = "COPYCORD_NODE_VERSION"
TIMEOUT = 60.0

_OS = {"linux": "linux", "darwin": "darwin", "win32": "win"}
_ARCH = {
    "x86_64": "x64",
    "amd64": "x64",
    "aarch64": "arm64",
    "arm64": "arm64",
    "armv7l": "armv7l",
}

INSTALL_HINT = (
    "install Node.js 22 or newer (it includes npm) from https://nodejs.org "
    "and run the update again"
)


class NodeError(Exception):
    pass


def node_version(env: Optional[Mapping[str, str]] = None) -> str:
    env = os.environ if env is None else env
    return (env.get(VERSION_ENV) or NODE_VERSION).lstrip("v")


def node_home(env: Optional[Mapping[str, str]] = None) -> Path:
    return paths.copycord_home(env) / "node"


def platform_name(sys_platform: Optional[str] = None, machine: Optional[str] = None) -> str:
    """Node's platform name (linux-x64, win-x64, ...); NodeError if unsupported."""
    sys_platform = sys.platform if sys_platform is None else sys_platform
    machine = platform.machine() if machine is None else machine
    os_name = _OS.get(sys_platform)
    if os_name is None and sys_platform.startswith("linux"):
        os_name = "linux"
    arch = _ARCH.get(machine.lower())
    if os_name is None or arch is None or (os_name != "linux" and arch == "armv7l"):
        raise NodeError(f"no official Node build for {sys_platform} {machine}; {INSTALL_HINT}")
    return f"{os_name}-{arch}"


def dist_name(version: str, plat: str) -> str:
    return f"node-v{version}-{plat}"


def bin_dir(root: Path, plat: str) -> Path:
    # The Windows zip has node.exe and npm.cmd at the top, the tarballs in bin/.
    return root if plat.startswith("win-") else root / "bin"


def _has_npm(directory: Path, plat: str) -> bool:
    return (directory / ("npm.cmd" if plat.startswith("win-") else "npm")).is_file()


def _fetch(url: str, dest: Path) -> None:
    with urllib.request.urlopen(url, timeout=TIMEOUT) as resp, open(dest, "wb") as out:
        shutil.copyfileobj(resp, out)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def expected_sha256(sums: str, filename: str) -> Optional[str]:
    for row in sums.splitlines():
        parts = row.split()
        if len(parts) == 2 and parts[1] == filename:
            return parts[0].lower()
    return None


def _unpack(archive: Path, dest: Path) -> None:
    if archive.name.endswith(".zip"):
        with zipfile.ZipFile(archive) as z:
            z.extractall(dest)
    else:
        with tarfile.open(archive) as t:
            t.extractall(dest, filter="data")


def download_node(version: str, plat: str, home: Path) -> Path:
    """Download, verify and unpack Node into home; returns its bin dir."""
    name = dist_name(version, plat)
    exts = [".zip"] if plat.startswith("win-") else [".tar.xz", ".tar.gz"]
    base = f"{NODE_DIST}/v{version}"
    home.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".download-", dir=home) as tmp:
        tmp_dir = Path(tmp)
        sums_file = tmp_dir / "SHASUMS256.txt"
        try:
            _fetch(f"{base}/SHASUMS256.txt", sums_file)
        except Exception as e:
            raise NodeError(f"could not download {base}/SHASUMS256.txt: {e}; {INSTALL_HINT}")
        sums = sums_file.read_text(encoding="utf-8", errors="replace")
        for ext in exts:
            expected = expected_sha256(sums, name + ext)
            if expected:
                break
        else:
            raise NodeError(f"Node {version} has no {name}{exts[0]} on nodejs.org; {INSTALL_HINT}")
        filename = name + ext
        archive = tmp_dir / filename
        try:
            _fetch(f"{base}/{filename}", archive)
        except Exception as e:
            raise NodeError(f"could not download {base}/{filename}: {e}; {INSTALL_HINT}")
        actual = _sha256(archive)
        if actual != expected:
            raise NodeError(
                f"sha256 mismatch for {filename}: expected {expected}, got {actual}; {INSTALL_HINT}"
            )
        staging = tmp_dir / "unpacked"
        try:
            _unpack(archive, staging)
        except (OSError, tarfile.TarError, zipfile.BadZipFile) as e:
            raise NodeError(f"could not unpack {filename}: {e}; {INSTALL_HINT}")
        unpacked = staging / name
        if not _has_npm(bin_dir(unpacked, plat), plat):
            raise NodeError(f"{filename} has no npm in {name}; {INSTALL_HINT}")
        target = home / name
        shutil.rmtree(target, ignore_errors=True)
        os.replace(unpacked, target)
    return bin_dir(target, plat)


def find_npm(env: Optional[Mapping[str, str]] = None) -> Optional[Path]:
    """The directory holding npm, or None; never downloads."""
    found = shutil.which("npm")
    if found:
        return Path(found).parent
    plat = platform_name()
    existing = bin_dir(node_home(env) / dist_name(node_version(env), plat), plat)
    return existing if _has_npm(existing, plat) else None


def ensure_npm(env: Optional[Mapping[str, str]] = None) -> Path:
    """The directory holding npm, downloading Node if needed."""
    found = shutil.which("npm")
    if found:
        ui.ok("npm found", found)
        return Path(found).parent
    plat = platform_name()
    version = node_version(env)
    home = node_home(env)
    name = dist_name(version, plat)
    existing = bin_dir(home / name, plat)
    if _has_npm(existing, plat):
        ui.ok(f"Node {version} found", str(home / name))
        return existing
    with ui.Spinner(f"Downloading Node {version} ({plat}) into {home}"):
        result = download_node(version, plat, home)
    ui.ok(f"Node {version} installed", str(home / name))
    return result


def with_path(directory: Path, env: Optional[Mapping[str, str]] = None) -> dict:
    """A copy of env (default os.environ) with directory first on PATH."""
    out = dict(os.environ if env is None else env)
    key = next((k for k in out if k.upper() == "PATH"), "PATH")
    rest = out.get(key, "")
    out[key] = str(directory) + (os.pathsep + rest if rest else "")
    return out
