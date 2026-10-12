# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
copycord update                       reinstall from the saved channel (default
                                      stable) with the tool that installed
                                      Copycord, then restart the service if it
                                      is installed and running
copycord update --channel stable|beta|dev [--branch NAME]
                                      switch channel (saved in <data dir>/update.json)
copycord update --tag VERSION         install that exact version once; the
                                      saved channel stays as it is
copycord update --check               print the installed version, the saved
                                      channel and the latest on that channel

stable  latest final release on PyPI
beta    latest PyPI release, pre-releases included (e.g. 4.0.0b1)
dev     git install of a branch (default main) of REPO_URL; needs npm to
        build the admin UI
"""

from __future__ import annotations

import json
import re
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import NamedTuple, Optional, Tuple

from copycord import service

PYPI_URL = "https://pypi.org/pypi/copycord/json"
PYPI_TIMEOUT = 5.0
REPO_URL = "https://github.com/Copycord/Copycord"
GITHUB_COMMITS_URL = "https://api.github.com/repos/Copycord/Copycord/commits/{branch}"
GITHUB_TIMEOUT = 5.0

STABLE, BETA, DEV = "stable", "beta", "dev"
CHANNELS = (STABLE, BETA, DEV)
DEFAULT_CHANNEL = STABLE
DEFAULT_BRANCH = "main"
SETTINGS_FILE = "update.json"

UV_TOOL, PIPX, UV_PIP, PIP, DOCKER, SOURCE = (
    "uv tool",
    "pipx",
    "uv pip",
    "pip",
    "docker",
    "source",
)


# --------------------------------------------------------------------------
# saved channel
# --------------------------------------------------------------------------


class Channel(NamedTuple):
    channel: str
    branch: Optional[str] = None  # set for DEV only

    def describe(self) -> str:
        if self.channel == DEV:
            return f"{DEV} (branch {self.branch or DEFAULT_BRANCH})"
        return self.channel


def settings_path() -> Path:
    from common.paths import data_dir

    return data_dir() / SETTINGS_FILE


def load_channel(path: Optional[Path] = None) -> Channel:
    """The saved channel; Channel(stable) when nothing (valid) is saved."""
    path = settings_path() if path is None else path
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return Channel(DEFAULT_CHANNEL)
    if not isinstance(raw, dict) or raw.get("channel") not in CHANNELS:
        return Channel(DEFAULT_CHANNEL)
    if raw["channel"] == DEV:
        branch = raw.get("branch")
        return Channel(DEV, branch if isinstance(branch, str) and branch else DEFAULT_BRANCH)
    return Channel(raw["channel"])


def save_channel(channel: Channel, path: Optional[Path] = None) -> Path:
    path = settings_path() if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"channel": channel.channel}
    if channel.channel == DEV:
        data["branch"] = channel.branch or DEFAULT_BRANCH
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# install commands
# --------------------------------------------------------------------------


def installer_metadata() -> Optional[str]:
    """Contents of copycord's dist-info INSTALLER file; None when not installed."""
    from importlib import metadata

    try:
        dist = metadata.distribution("copycord")
    except metadata.PackageNotFoundError:
        return None
    return (dist.read_text("INSTALLER") or "").strip().lower()


def detect_installer(
    prefix: Optional[str] = None,
    installer: Optional[str] = "",
    docker: Optional[bool] = None,
) -> str:
    """How this Copycord was installed: UV_TOOL, PIPX, UV_PIP, PIP, DOCKER or SOURCE.

    installer is the dist-info INSTALLER text (None: copycord is not installed
    as a package, e.g. run from a checkout); "" reads it.
    """
    if docker is None:
        from common.paths import in_docker

        docker = in_docker()
    if docker:
        return DOCKER
    root = Path(sys.prefix if prefix is None else prefix)
    if installer == "":
        installer = installer_metadata()
    if installer is None:
        return SOURCE
    parts = [p.lower() for p in root.parts]
    pairs = list(zip(parts, parts[1:]))
    if (root / "uv-receipt.toml").exists() or ("uv", "tools") in pairs:
        return UV_TOOL
    if (root / "pipx_metadata.json").exists() or ("pipx", "venvs") in pairs:
        return PIPX
    if installer == "uv":
        return UV_PIP
    return PIP


def package_spec(channel: Channel, tag: Optional[str] = None) -> str:
    """What to install: copycord, copycord==TAG or copycord @ git+REPO_URL@BRANCH."""
    if tag:
        return f"copycord=={tag.lstrip('v')}"
    if channel.channel == DEV:
        return f"copycord @ git+{REPO_URL}@{channel.branch or DEFAULT_BRANCH}"
    return "copycord"


def _quote(arg: str) -> str:
    return f'"{arg}"' if " " in arg else arg


def install_command(
    kind: str, channel: Channel, tag: Optional[str] = None
) -> Tuple[Optional[list], str]:
    """(command to run, or None) and a human hint for the install kind and channel."""
    spec = package_spec(channel, tag)
    beta = channel.channel == BETA and not tag
    if kind == UV_TOOL:
        cmd = ["uv", "tool", "install", "--force"]
        cmd += ["--prerelease", "allow"] if beta else []
        cmd.append(spec)
        return cmd, " ".join(_quote(a) for a in cmd)
    if kind == PIPX:
        cmd = ["pipx", "install", "--force"]
        cmd += ["--pip-args=--pre"] if beta else []
        cmd.append(spec)
        return cmd, " ".join(_quote(a) for a in cmd)
    py = sys.executable
    if kind == UV_PIP:
        pre = " --prerelease allow" if beta else ""
        return None, f'uv pip install --upgrade{pre} --python "{py}" "{spec}"'
    if kind == PIP:
        pre = " --pre" if beta else ""
        return None, f'"{py}" -m pip install --upgrade{pre} "{spec}"'
    if kind == DOCKER:
        return None, "pull the new image and recreate the container (docker compose pull && docker compose up -d)"
    return None, "update your checkout (git pull), or install the package: uv tool install copycord"


# --------------------------------------------------------------------------
# versions
# --------------------------------------------------------------------------


def current_version() -> str:
    from common.config import CURRENT_VERSION

    return CURRENT_VERSION.lstrip("v")


# PEP 440 subset: 4.0.0, 4.0.0b1, 4.0.0rc2, 4.0.0.post1, 4.0.0.dev3, 4.0.0+local
_VERSION_RE = re.compile(
    r"^\s*v?(?P<release>\d+(?:\.\d+)*)"
    r"(?:[-_.]?(?P<pre>a|alpha|b|beta|c|rc|pre|preview)[-_.]?(?P<pre_n>\d*))?"
    r"(?:[-_.]?(?:post|rev|r)[-_.]?(?P<post>\d*))?"
    r"(?:[-_.]?dev[-_.]?(?P<dev>\d*))?"
    r"(?:\+[a-z0-9.]*)?\s*$",
    re.IGNORECASE,
)
_PRE_RANK = {"a": 0, "alpha": 0, "b": 1, "beta": 1, "c": 2, "rc": 2, "pre": 2, "preview": 2}


def parse_version(v: str):
    """Sort key for v; None when it is not a version this comparator knows."""
    m = _VERSION_RE.match(v)
    if not m:
        return None
    release = [int(n) for n in m["release"].split(".")]
    while len(release) > 1 and release[-1] == 0:
        release.pop()
    has_post = m["post"] is not None
    has_dev = m["dev"] is not None
    if m["pre"] is not None:
        pre = (0, _PRE_RANK[m["pre"].lower()], int(m["pre_n"] or 0))
    elif has_dev and not has_post:
        pre = (-1,)  # 4.0.0.dev1 sorts before 4.0.0a1
    else:
        pre = (1,)
    post = (0, int(m["post"] or 0)) if has_post else (-1,)
    dev = (0, int(m["dev"] or 0)) if has_dev else (1,)
    return (tuple(release), pre, post, dev)


def is_prerelease(v: str) -> bool:
    m = _VERSION_RE.match(v)
    return bool(m) and (m["pre"] is not None or m["dev"] is not None)


def is_newer(latest: str, current: str) -> bool:
    a, b = parse_version(latest), parse_version(current)
    if a is None or b is None:
        return latest != current
    return a > b


def _fetch_json(url: str, timeout: float):
    req = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": "copycord-update"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def latest_from_pypi(data: dict, prerelease: bool) -> str:
    """Highest non-yanked release in a PyPI JSON document.

    Final releases only unless prerelease; falls back to info.version.
    """
    best = None
    for version, files in (data.get("releases") or {}).items():
        if not files or all(f.get("yanked") for f in files):
            continue
        key = parse_version(version)
        if key is None or (not prerelease and is_prerelease(version)):
            continue
        if best is None or key > best[0]:
            best = (key, version)
    if best is not None:
        return best[1]
    return str(data["info"]["version"])


def fetch_latest_version(
    prerelease: bool = False, url: str = PYPI_URL, timeout: float = PYPI_TIMEOUT
) -> str:
    return latest_from_pypi(_fetch_json(url, timeout), prerelease)


class Commit(NamedTuple):
    sha: str
    date: str
    subject: str


def fetch_branch_head(branch: str, timeout: float = GITHUB_TIMEOUT) -> Commit:
    data = _fetch_json(GITHUB_COMMITS_URL.format(branch=branch), timeout)
    commit = data.get("commit") or {}
    date = (commit.get("committer") or {}).get("date") or ""
    subject = (commit.get("message") or "").splitlines()[0:1]
    return Commit(str(data["sha"]), date, subject[0] if subject else "")


def installed_commit() -> Optional[str]:
    """Commit of a git install (PEP 610 direct_url.json); None otherwise."""
    from importlib import metadata

    try:
        raw = metadata.distribution("copycord").read_text("direct_url.json")
        return json.loads(raw)["vcs_info"]["commit_id"] if raw else None
    except Exception:
        return None


def check(channel: Optional[Channel] = None) -> int:
    channel = load_channel() if channel is None else channel
    current = current_version()
    print(f"current {current}")
    print(f"channel {channel.describe()}")

    if channel.channel == DEV:
        branch = channel.branch or DEFAULT_BRANCH
        try:
            head = fetch_branch_head(branch)
        except Exception as e:
            print(f"WARN  could not get the head of {branch} from GitHub: {e}")
            return 0
        print(f"latest  {branch} @ {head.sha[:12]} {head.date} {head.subject}".rstrip())
        installed = installed_commit()
        if installed is None:
            print("installed commit unknown: run `copycord update` to install the branch head")
        elif installed == head.sha:
            print("Copycord is up to date")
        else:
            print(f"installed {installed[:12]}; an update is available: run `copycord update`")
        return 0

    try:
        latest = fetch_latest_version(prerelease=channel.channel == BETA)
    except Exception as e:
        print(f"WARN  could not get the latest version from PyPI: {e}")
        return 0
    print(f"latest  {latest}")
    if is_newer(latest, current):
        print("an update is available: run `copycord update`")
    else:
        print("Copycord is up to date")
    return 0


# --------------------------------------------------------------------------
# update
# --------------------------------------------------------------------------


def update(
    check_only: bool = False,
    channel: Optional[str] = None,
    branch: Optional[str] = None,
    tag: Optional[str] = None,
) -> int:
    saved = load_channel()
    if tag and (channel or branch):
        print("--tag installs one exact version; it cannot be combined with --channel or --branch",
              file=sys.stderr)
        return 2
    if branch and (channel or saved.channel) != DEV:
        print("--branch only applies to the dev channel (use --channel dev --branch NAME)",
              file=sys.stderr)
        return 2
    if channel == DEV or (channel is None and branch):
        target = Channel(DEV, branch or (saved.branch if saved.channel == DEV else None)
                         or DEFAULT_BRANCH)
    elif channel:
        target = Channel(channel)
    else:
        target = saved

    if check_only:
        return check(target)

    kind = detect_installer()
    cmd, hint = install_command(kind, target, tag)
    if cmd is None:
        print(f"Copycord was installed with {kind}; update it with:\n  {hint}")
        return 1
    if shutil.which(cmd[0]) is None:
        print(f"{cmd[0]} is not on PATH; update Copycord with:\n  {hint}", file=sys.stderr)
        return 1
    if target.channel == DEV and not tag and shutil.which("npm") is None:
        print(
            "the dev channel builds the admin UI from source and needs Node.js/npm, "
            "but npm is not on PATH. Install Node.js (which includes npm) and retry, "
            "or stay on the stable or beta channel (built with the UI included).",
            file=sys.stderr,
        )
        return 1

    # The service state is read before the install replaces the files.
    try:
        state = service.service_state()
    except Exception as e:
        print(f"WARN  could not read the service state: {e}", file=sys.stderr)
        state = None

    print(f"$ {hint}")
    result = service.run_cmd(cmd, capture=False)
    if result.returncode != 0:
        print(f"update failed (exit {result.returncode})", file=sys.stderr)
        return result.returncode

    # --tag is a one-off; a channel switch is saved once the install worked.
    if not tag and (channel or branch):
        try:
            path = save_channel(target)
            print(f"update channel set to {target.describe()} ({path})")
        except OSError as e:
            print(f"WARN  could not save the update channel: {e}", file=sys.stderr)

    if state is not None and state.installed and state.running:
        try:
            service.restart_service()
        except service.ServiceError as e:
            print(f"updated, but restarting the service failed: {e}", file=sys.stderr)
            return 1
        print("restarted the copycord service")
    return 0
