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
copycord update --channel stable|beta|dev [--repo OWNER/NAME] [--ref REF]
                                      switch channel (saved in <data dir>/update.json)
copycord update --tag VERSION         install that exact version once; the
                                      saved channel stays as it is
copycord update status              print the installed version, the saved
                                      channel and the latest on that channel
                                      (--check is the same)
copycord update --channel beta --no-install
                                      only save the channel (the install
                                      scripts use this)

stable  newest GitHub release that is not a draft or a pre-release
beta    newest GitHub release that is not a draft, pre-releases included
        (e.g. v4.0.0b1)
dev     git install of a branch, tag or commit (--ref, default main) of a
        GitHub repo (--repo, default Copycord/Copycord):
        uv tool install --force git+https://github.com/<repo>@<ref>
        needs npm to build the admin UI

update.json is {"channel": ...}; dev adds "branch" (the ref) and, when it is
not the default, "repo". Files without "repo" read as Copycord/Copycord.

stable, beta and --tag install the wheel asset of the release by its URL.
"""

from __future__ import annotations

import json
import re
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import NamedTuple, Optional, Tuple

from copycord import service, ui

DEFAULT_REPO = "Copycord/Copycord"
REPO_URL = f"https://github.com/{DEFAULT_REPO}"
RELEASES_URL = "https://api.github.com/repos/Copycord/Copycord/releases"
RELEASE_TAG_URL = RELEASES_URL + "/tags/{tag}"
GITHUB_COMMITS_URL = "https://api.github.com/repos/{repo}/commits/{branch}"
GITHUB_TIMEOUT = 5.0
# Python `copycord update` asks uv for: the recommended, tested version.
PYTHON_VERSION = "3.12"

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


_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def valid_repo(repo: str) -> bool:
    return bool(_REPO_RE.match(repo or ""))


class Channel(NamedTuple):
    channel: str
    branch: Optional[str] = None  # DEV only: the git ref (branch, tag or sha)
    repo: Optional[str] = None  # DEV only: owner/name; None means DEFAULT_REPO

    @property
    def ref(self) -> str:
        return self.branch or DEFAULT_BRANCH

    @property
    def repo_name(self) -> str:
        return self.repo or DEFAULT_REPO

    def describe(self) -> str:
        if self.channel == DEV:
            if self.repo and self.repo != DEFAULT_REPO:
                return f"{DEV} ({self.repo}@{self.ref})"
            return f"{DEV} (branch {self.ref})"
        return self.channel


def settings_path() -> Path:
    from copycord.common.paths import data_dir

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
        repo = raw.get("repo")
        repo = repo if isinstance(repo, str) and valid_repo(repo) and repo != DEFAULT_REPO else None
        return Channel(DEV, branch if isinstance(branch, str) and branch else DEFAULT_BRANCH, repo)
    return Channel(raw["channel"])


def save_channel(channel: Channel, path: Optional[Path] = None) -> Path:
    path = settings_path() if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"channel": channel.channel}
    if channel.channel == DEV:
        data["branch"] = channel.ref
        # Only a non-default repo is written, so older Copycords read the file too.
        if channel.repo and channel.repo != DEFAULT_REPO:
            data["repo"] = channel.repo
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
        from copycord.common.paths import in_docker

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


def git_spec(branch: Optional[str] = None, repo: Optional[str] = None) -> str:
    return f"git+https://github.com/{repo or DEFAULT_REPO}@{branch or DEFAULT_BRANCH}"


def _quote(arg: str) -> str:
    return f'"{arg}"' if " " in arg else arg


def install_command(kind: str, spec: str) -> Tuple[Optional[list], str]:
    """(command to run, or None) and a human hint for installing spec with kind.

    spec is a release wheel URL (stable, beta, --tag) or git_spec() (dev).
    """
    if kind == UV_TOOL:
        cmd = ["uv", "tool", "install", "--force", "--python", PYTHON_VERSION, spec]
        return cmd, " ".join(_quote(a) for a in cmd)
    if kind == PIPX:
        cmd = ["pipx", "install", "--force", spec]
        return cmd, " ".join(_quote(a) for a in cmd)
    py = sys.executable
    if kind == UV_PIP:
        return None, f'uv pip install --upgrade --python "{py}" "{spec}"'
    if kind == PIP:
        return None, f'"{py}" -m pip install --upgrade "{spec}"'
    return None, manual_hint(kind)


def manual_hint(kind: str) -> str:
    """How to update an install `copycord update` cannot reinstall from a URL."""
    if kind == DOCKER:
        return "pull the new image and recreate the container (docker compose pull && docker compose up -d)"
    return (
        "update your checkout (git pull), or install a release with the install script: "
        "curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh"
    )


# --------------------------------------------------------------------------
# versions
# --------------------------------------------------------------------------


def current_version() -> str:
    from copycord.common.config import CURRENT_VERSION

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


class ReleaseError(Exception):
    """A release that cannot be installed (not found, no wheel asset)."""


class Release(NamedTuple):
    tag: str
    version: str
    prerelease: bool
    wheel_url: Optional[str]  # None when the release has no wheel asset

    def wheel(self) -> str:
        if not self.wheel_url:
            raise ReleaseError(
                f"release {self.tag} has no wheel asset (copycord-*.whl); "
                f"see {REPO_URL}/releases/tag/{self.tag}"
            )
        return self.wheel_url


def _wheel_asset(release: dict) -> Optional[str]:
    """browser_download_url of the copycord-*.whl asset of a GitHub release."""
    for asset in release.get("assets") or []:
        name = str(asset.get("name") or "")
        if name.startswith("copycord-") and name.endswith(".whl"):
            return asset.get("browser_download_url") or None
    return None


def _release(data: dict) -> Release:
    tag = str(data["tag_name"])
    return Release(tag, tag.lstrip("v"), bool(data.get("prerelease")), _wheel_asset(data))


def latest_from_releases(data: list, prerelease: bool) -> Release:
    """Newest release in a GitHub releases list: drafts are skipped, pre-releases
    too unless prerelease. Newest is the highest version; tags that are not
    versions are skipped."""
    best = None
    for item in data or []:
        if not isinstance(item, dict) or item.get("draft"):
            continue
        if item.get("prerelease") and not prerelease:
            continue
        key = parse_version(str(item.get("tag_name") or ""))
        if key is None:
            continue
        if best is None or key > best[0]:
            best = (key, item)
    if best is None:
        kind = "release" if prerelease else "stable release"
        raise ReleaseError(f"no published {kind} found on {REPO_URL}/releases")
    return _release(best[1])


def fetch_latest_release(
    prerelease: bool = False, url: str = RELEASES_URL, timeout: float = GITHUB_TIMEOUT
) -> Release:
    return latest_from_releases(_fetch_json(f"{url}?per_page=100", timeout), prerelease)


def fetch_release(tag: str, timeout: float = GITHUB_TIMEOUT) -> Release:
    """The release tagged v<VERSION> (tag may be given with or without the v)."""
    import urllib.error

    tag = "v" + tag.lstrip("v")
    try:
        data = _fetch_json(RELEASE_TAG_URL.format(tag=tag), timeout)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise ReleaseError(f"no release {tag} on {REPO_URL}/releases") from e
        raise
    if data.get("draft"):
        raise ReleaseError(f"release {tag} is a draft")
    return _release(data)


class Commit(NamedTuple):
    sha: str
    date: str
    subject: str


def fetch_branch_head(
    branch: str, timeout: float = GITHUB_TIMEOUT, repo: str = DEFAULT_REPO
) -> Commit:
    data = _fetch_json(GITHUB_COMMITS_URL.format(repo=repo, branch=branch), timeout)
    commit = data.get("commit") or {}
    date = (commit.get("committer") or {}).get("date") or ""
    subject = (commit.get("message") or "").splitlines()[0:1]
    return Commit(str(data["sha"]), date, subject[0] if subject else "")


def installed_commit() -> Optional[str]:
    """Commit of a git install; None otherwise.

    PEP 610 direct_url.json carries it for `git+https://...` installs. The
    installer's git method installs from its local checkout (a file:// URL),
    so for those the commit is read from that checkout.
    """
    import subprocess
    import urllib.parse
    from importlib import metadata

    try:
        raw = metadata.distribution("copycord").read_text("direct_url.json")
        info = json.loads(raw) if raw else {}
    except Exception:
        return None
    commit = (info.get("vcs_info") or {}).get("commit_id")
    if commit:
        return commit
    url = info.get("url", "")
    if not url.startswith("file://"):
        return None
    path = urllib.parse.unquote(urllib.parse.urlparse(url).path)
    if sys.platform == "win32" and re.match(r"^/[A-Za-z]:", path):
        path = path[1:]
    try:
        res = subprocess.run(
            ["git", "-C", path, "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = res.stdout.strip()
    return sha if res.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", sha) else None


def _row(key: str, value: str) -> None:
    ui.kv([(key, value)], width=7)


def check(channel: Optional[Channel] = None) -> int:
    channel = load_channel() if channel is None else channel
    current = current_version()
    out = sys.stdout
    ui.heading("Copycord update status")
    _row("current", current)
    _row("channel", channel.describe())
    _row("method", detect_installer())
    from copycord import update_check

    _row("checked", update_check.last_check_line())
    _row("auto", "on (AUTO_UPDATE)" if update_check.auto_update_enabled() else "off (AUTO_UPDATE=true turns it on)")

    if channel.channel == DEV:
        ref, repo = channel.ref, channel.repo_name
        try:
            with ui.Spinner(f"Reading {repo}@{ref} from GitHub"):
                head = fetch_branch_head(ref, repo=repo)
        except Exception as e:
            ui.warn(f"could not get the head of {ref} from GitHub", str(e), stream=out)
            return 0
        _row("latest", f"{ref} @ {head.sha[:12]} {head.date} {head.subject}".rstrip())
        installed = installed_commit()
        if installed is None:
            ui.info("installed commit unknown",
                    f"run {ui.cmd('copycord update')} to install the {ref} head")
        elif installed == head.sha:
            ui.ok("Copycord is up to date")
        else:
            ui.box([f"installed {installed[:12]}, {ref} is at {head.sha[:12]}",
                    f"Run {ui.cmd('copycord update')} to update."],
                   title="Update available", status="warn")
        return 0

    try:
        with ui.Spinner("Checking GitHub releases"):
            release = fetch_latest_release(prerelease=channel.channel == BETA)
    except ReleaseError as e:
        ui.warn(str(e), stream=out)
        return 0
    except Exception as e:
        ui.warn("could not get the latest release from GitHub", str(e), stream=out)
        return 0
    latest = release.version
    _row("latest", f"{latest} ({release.tag})")
    if release.wheel_url is None:
        ui.warn(f"release {release.tag} has no wheel asset yet", stream=out)
    if is_newer(latest, current):
        ui.box([f"{current} -> {latest}",
                f"Run {ui.cmd('copycord update')} to install it."],
               title="Update available", status="warn")
    else:
        ui.ok("Copycord is up to date")
    return 0


# --------------------------------------------------------------------------
# update
# --------------------------------------------------------------------------


def _err(text: str, detail: str = "") -> None:
    ui.fail(text, detail)


def update(
    check_only: bool = False,
    channel: Optional[str] = None,
    branch: Optional[str] = None,
    tag: Optional[str] = None,
    no_install: bool = False,
    repo: Optional[str] = None,
) -> int:
    saved = load_channel()
    if tag and (channel or branch or repo):
        _err("--tag installs one exact version; it cannot be combined with "
             "--channel, --repo or --ref")
        return 2
    if (branch or repo) and (channel or saved.channel) != DEV:
        _err("--ref and --repo only apply to the dev channel "
             "(use --channel dev --repo OWNER/NAME --ref REF)")
        return 2
    if repo and not valid_repo(repo):
        _err(f"--repo {repo!r} is not OWNER/NAME", "for example Copycord/Copycord")
        return 2
    if channel == DEV or (channel is None and (branch or repo)):
        keep = saved if saved.channel == DEV else Channel(DEV)
        new_repo = repo or keep.repo
        target = Channel(DEV, branch or keep.branch or DEFAULT_BRANCH,
                         None if new_repo == DEFAULT_REPO else new_repo)
    elif channel:
        target = Channel(channel)
    else:
        target = saved
    switching = bool(channel or branch or repo)

    if no_install:
        if tag or not switching:
            _err("--no-install only saves a channel; give --channel "
                 "(and --repo/--ref for dev)")
            return 2
        try:
            path = save_channel(target)
        except OSError as e:
            _err("could not save the update channel", str(e))
            return 1
        ui.ok(f"update channel set to {target.describe()}", str(path))
        return 0

    if check_only:
        return check(target)

    kind = detect_installer()
    if kind in (DOCKER, SOURCE):
        print(f"Copycord was installed with {kind}; update it with:\n  {manual_hint(kind)}")
        return 1

    ui.heading(f"Updating Copycord ({target.describe() if not tag else 'tag ' + tag})")
    if target.channel == DEV and not tag:
        spec = git_spec(target.ref, target.repo)
    else:
        try:
            with ui.Spinner("Checking GitHub releases"):
                if tag:
                    release = fetch_release(tag)
                else:
                    release = fetch_latest_release(prerelease=target.channel == BETA)
                spec = release.wheel()
        except ReleaseError as e:
            _err(f"cannot update: {e}")
            return 1
        except Exception as e:
            _err(f"cannot update: could not get the release from GitHub: {e}")
            return 1
        ui.ok(f"release {release.tag}")

    cmd, hint = install_command(kind, spec)
    if cmd is None:
        print(f"Copycord was installed with {kind}; update it with:\n  {hint}")
        return 1
    if shutil.which(cmd[0]) is None:
        _err(f"{cmd[0]} is not on PATH", f"update Copycord with: {hint}")
        return 1
    if target.channel == DEV and not tag:
        if shutil.which("npm") is None:
            ui.box(
                [
                    "The dev channel builds the admin UI from source and needs",
                    "Node.js/npm, but npm is not on PATH.",
                    "Install Node.js (which includes npm) and retry, or stay on",
                    f"the stable or beta channel: {ui.cmd('copycord update --channel stable')}",
                ],
                title="npm not found", status="fail", stream=sys.stderr,
            )
            return 1
        ui.ok("npm found", "the admin UI is built during the install")

    # The service state is read before the install replaces the files.
    try:
        state = service.service_state()
    except Exception as e:
        ui.warn("could not read the service state", str(e))
        state = None

    print(f"$ {hint}")
    result = service.run_cmd(cmd, capture=False)
    if result.returncode != 0:
        _err(f"update failed (exit {result.returncode})")
        return result.returncode
    ui.ok("installed", spec)

    # --tag is a one-off; a channel switch is saved once the install worked.
    if not tag and switching:
        try:
            path = save_channel(target)
            ui.ok(f"update channel set to {target.describe()}", str(path))
        except OSError as e:
            ui.warn("could not save the update channel", str(e))

    if state is not None and state.installed and state.running:
        try:
            service.restart_service()
        except service.ServiceError as e:
            _err(f"updated, but restarting the service failed: {e}")
            return 1
        ui.ok("restarted the copycord service")
    return 0
