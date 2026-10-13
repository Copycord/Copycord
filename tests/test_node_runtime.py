"""Tests for copycord.node_runtime. The network is faked with tiny archives."""
import hashlib
import io
import re
import tarfile
import zipfile
from pathlib import Path

import pytest

from copycord import node_runtime

ROOT = Path(__file__).resolve().parents[1]
VERSION = node_runtime.NODE_VERSION
BASE = f"https://nodejs.org/dist/v{VERSION}"


def tarball(name, mode="w:xz"):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode=mode) as t:
        data = b"#!/bin/sh\necho 10.9.0\n"
        info = tarfile.TarInfo(f"{name}/bin/npm")
        info.size = len(data)
        info.mode = 0o755
        t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def zipball(name):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(f"{name}/npm.cmd", "@echo 10.9.0\r\n")
        z.writestr(f"{name}/node.exe", "")
    return buf.getvalue()


def sums(**files):
    return "".join(f"{hashlib.sha256(d).hexdigest()}  {n}\n" for n, d in files.items())


@pytest.fixture()
def home(tmp_path, monkeypatch):
    h = tmp_path / "cchome"
    monkeypatch.setenv("COPYCORD_HOME", str(h))
    monkeypatch.delenv(node_runtime.VERSION_ENV, raising=False)
    monkeypatch.setattr(node_runtime.shutil, "which", lambda name: None)
    monkeypatch.setattr(node_runtime, "platform_name", lambda: "linux-x64")
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

    monkeypatch.setattr(node_runtime, "_fetch", fetch)
    return responses, seen


def test_npm_on_path_is_reused(home, net, monkeypatch):
    _, seen = net
    monkeypatch.setattr(node_runtime.shutil, "which", lambda name: "/opt/node/bin/npm")
    assert node_runtime.ensure_npm() == Path("/opt/node/bin")
    assert seen == []
    assert not home.exists()


def test_existing_home_node_is_reused(home, net):
    _, seen = net
    bin_dir = home / "node" / f"node-v{VERSION}-linux-x64" / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "npm").write_text("")
    assert node_runtime.ensure_npm() == bin_dir
    assert seen == []


def test_download_verify_unpack(home, net):
    responses, seen = net
    name = f"node-v{VERSION}-linux-x64"
    xz, gz = tarball(name), tarball(name, "w:gz")
    responses[f"{BASE}/SHASUMS256.txt"] = sums(
        **{f"{name}.tar.gz": gz, f"{name}.tar.xz": xz, f"{name}.zip": b"other"}
    ).encode()
    responses[f"{BASE}/{name}.tar.xz"] = xz
    result = node_runtime.ensure_npm()
    assert result == home / "node" / name / "bin"
    assert (result / "npm").read_bytes().startswith(b"#!/bin/sh")
    assert seen == [f"{BASE}/SHASUMS256.txt", f"{BASE}/{name}.tar.xz"]
    assert [p.name for p in (home / "node").iterdir()] == [name]
    assert node_runtime.ensure_npm() == result
    assert len(seen) == 2


def test_tar_gz_when_there_is_no_tar_xz(home, net, monkeypatch):
    responses, seen = net
    monkeypatch.setenv(node_runtime.VERSION_ENV, "20.18.1")
    base = "https://nodejs.org/dist/v20.18.1"
    name = "node-v20.18.1-linux-x64"
    gz = tarball(name, "w:gz")
    responses[f"{base}/SHASUMS256.txt"] = sums(**{f"{name}.tar.gz": gz}).encode()
    responses[f"{base}/{name}.tar.gz"] = gz
    assert node_runtime.ensure_npm() == home / "node" / name / "bin"
    assert seen[-1] == f"{base}/{name}.tar.gz"


def test_windows_zip(home, net, monkeypatch):
    responses, _ = net
    monkeypatch.setattr(node_runtime, "platform_name", lambda: "win-x64")
    name = f"node-v{VERSION}-win-x64"
    z = zipball(name)
    responses[f"{BASE}/SHASUMS256.txt"] = sums(**{f"{name}.zip": z}).encode()
    responses[f"{BASE}/{name}.zip"] = z
    result = node_runtime.ensure_npm()
    assert result == home / "node" / name
    assert (result / "npm.cmd").is_file()


def test_checksum_mismatch_fails(home, net):
    responses, _ = net
    name = f"node-v{VERSION}-linux-x64"
    responses[f"{BASE}/SHASUMS256.txt"] = sums(**{f"{name}.tar.xz": b"the real one"}).encode()
    responses[f"{BASE}/{name}.tar.xz"] = tarball(name)
    with pytest.raises(node_runtime.NodeError, match="sha256 mismatch") as e:
        node_runtime.ensure_npm()
    assert "install Node.js" in str(e.value)
    assert not (home / "node" / name).exists()
    assert list((home / "node").iterdir()) == []


def test_download_failure_fails(home, net):
    with pytest.raises(node_runtime.NodeError, match="SHASUMS256.txt"):
        node_runtime.ensure_npm()


@pytest.mark.parametrize(
    "plat, machine, name",
    [
        ("linux", "x86_64", "linux-x64"),
        ("linux", "aarch64", "linux-arm64"),
        ("darwin", "x86_64", "darwin-x64"),
        ("darwin", "arm64", "darwin-arm64"),
        ("win32", "AMD64", "win-x64"),
    ],
)
def test_platform_name(plat, machine, name):
    assert node_runtime.platform_name(plat, machine) == name


@pytest.mark.parametrize("plat, machine", [("freebsd13", "amd64"), ("linux", "mips"), ("darwin", "armv7l")])
def test_unsupported_platform_fails(plat, machine):
    with pytest.raises(node_runtime.NodeError, match="install Node.js"):
        node_runtime.platform_name(plat, machine)


def test_with_path_prepends():
    env = node_runtime.with_path(Path("/n/bin"), {"PATH": "/usr/bin", "X": "1"})
    assert env["PATH"].split(node_runtime.os.pathsep) == ["/n/bin", "/usr/bin"]
    assert env["X"] == "1"


def test_installers_agree_on_node_version_and_location():
    sh = (ROOT / "install.sh").read_text(encoding="utf-8")
    ps1 = (ROOT / "install.ps1").read_text(encoding="utf-8")
    assert re.search(r'NODE_VERSION="\$\{COPYCORD_NODE_VERSION:-([0-9.]+)\}"', sh).group(1) == VERSION
    assert re.search(r"else \{ '([0-9.]+)' \}", ps1).group(1) == VERSION
    assert 'NODE_HOME="$COPYCORD_HOME/node"' in sh
    assert "$nodeHome = Join-Path $homeDir 'node'" in ps1
    assert "printf 'node-v%s-%s-%s' \"$NODE_VERSION\" \"$os\" \"$arch\"" in sh
    assert 'node-v$NodeVersion-win-$arch' in ps1
