# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
copycord update: reinstall Copycord from the saved channel with the tool that
installed it, back up the database first, then restart the service.

The channel is saved in <data dir>/update.json. stable and beta install a
release wheel; dev builds a git ref, which needs npm for the admin UI.
"""

from __future__ import annotations

import contextlib
import json
import re
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import NamedTuple, Optional, Tuple

from copycord import node_runtime, service, ui, winupdate

DEFAULT_REPO = "Copycord/Copycord"
REPO_URL = f"https://github.com/{DEFAULT_REPO}"
RELEASES_URL = "https://api.github.com/repos/Copycord/Copycord/releases"
RELEASE_TAG_URL = RELEASES_URL + "/tags/{tag}"
GITHUB_COMMITS_URL = "https://api.github.com/repos/{repo}/commits/{branch}"
GITHUB_TIMEOUT = 5.0
# The Python version uv is asked for.
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
    branch: Optional[str] = None  # dev only: branch, tag or sha
    repo: Optional[str] = None  # dev only; None means DEFAULT_REPO

    @property
    def ref(self) -> str:
        return self.branch or DEFAULT_BRANCH

    @property
    def repo_name(self) -> str:
        return self.repo or DEFAULT_REPO

    def describe(self, sep: str = " · ") -> str:
        """"stable channel", "dev channel · v4-dev" or "dev channel · owner/repo@ref"."""
        if self.channel == DEV:
            if self.repo and self.repo != DEFAULT_REPO:
                return f"{DEV} channel{sep}{self.repo}@{self.ref}"
            return f"{DEV} channel{sep}{self.ref}"
        return f"{self.channel} channel"


def settings_path() -> Path:
    from copycord.common.paths import data_dir

    return data_dir() / SETTINGS_FILE


def load_channel(path: Optional[Path] = None) -> Channel:
    """The saved channel; if none, dev at its ref for a git install, else stable."""
    fallback = Channel(DEFAULT_CHANNEL)
    if path is None:
        path = settings_path()
        source = git_source()
        if source is not None and source.repo:
            fallback = Channel(DEV, source.ref or DEFAULT_BRANCH,
                               None if source.repo == DEFAULT_REPO else source.repo)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return fallback
    if not isinstance(raw, dict) or raw.get("channel") not in CHANNELS:
        return fallback
    if raw["channel"] == DEV:
        branch = raw.get("branch")
        repo = raw.get("repo")
        repo = repo if isinstance(repo, str) and valid_repo(repo) and repo != DEFAULT_REPO else None
        return Channel(DEV, branch if isinstance(branch, str) and branch else DEFAULT_BRANCH, repo)
    return Channel(raw["channel"])


def channel_text(channel: Channel) -> str:
    data = {"channel": channel.channel}
    if channel.channel == DEV:
        data["branch"] = channel.ref
        # Only a non-default repo is written, so older versions can still read the file.
        if channel.repo and channel.repo != DEFAULT_REPO:
            data["repo"] = channel.repo
    return json.dumps(data, indent=2) + "\n"


def save_channel(channel: Channel, path: Optional[Path] = None) -> Path:
    path = settings_path() if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(channel_text(channel), encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# install commands
# --------------------------------------------------------------------------


def installer_metadata() -> Optional[str]:
    """The dist-info INSTALLER text; None when not installed as a package."""
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
    """How Copycord was installed; installer="" reads the INSTALLER file."""
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


def install_command(kind: str, spec: str, reinstall: bool = False) -> Tuple[Optional[list], str]:
    """The install command for spec (or None) and a hint; reinstall also rebuilds cached packages."""
    if kind == UV_TOOL:
        cmd = ["uv", "tool", "install", "--force", "--python", PYTHON_VERSION, spec]
        if reinstall:
            cmd.insert(3, "--reinstall")
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
    """How to update an install copycord update cannot reinstall from a URL."""
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
    """Sort key for v; None when it does not parse."""
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


def is_older(target: str, current: str) -> bool:
    """True when both parse and target sorts below current."""
    a, b = parse_version(target), parse_version(current)
    return a is not None and b is not None and a < b


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
    """Download URL of the release's copycord-*.whl asset."""
    for asset in release.get("assets") or []:
        name = str(asset.get("name") or "")
        if name.startswith("copycord-") and name.endswith(".whl"):
            return asset.get("browser_download_url") or None
    return None


def _release(data: dict) -> Release:
    tag = str(data["tag_name"])
    return Release(tag, tag.lstrip("v"), bool(data.get("prerelease")), _wheel_asset(data))


def latest_from_releases(data: list, prerelease: bool) -> Release:
    """The highest-versioned release, skipping drafts and, unless prerelease, pre-releases."""
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
    """The release tagged v<VERSION>, with or without the v."""
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


def build_commit() -> Optional[str]:
    """The commit stamped into the wheel at build time, or None."""
    from importlib import resources

    try:
        raw = (resources.files("copycord") / "_build.json").read_text(encoding="utf-8")
        commit = json.loads(raw).get("commit", "")
    except Exception:
        return None
    return commit if re.fullmatch(r"[0-9a-f]{40}", commit) else None


class GitSource(NamedTuple):
    url: str  # the git URL, without git+ and @ref
    ref: Optional[str]  # requested_revision
    commit: Optional[str]  # commit_id

    @property
    def repo(self) -> Optional[str]:
        """OWNER/NAME for a GitHub URL; None for other hosts."""
        m = re.match(r"^https://github\.com/([^/]+/[^/]+?)(?:\.git)?/?$", self.url)
        return m.group(1) if m and valid_repo(m.group(1)) else None

    def spec(self) -> str:
        """The same URL at the installed commit, else the requested ref."""
        rev = self.commit or self.ref
        return f"git+{self.url}@{rev}" if rev else f"git+{self.url}"


def git_source() -> Optional[GitSource]:
    """Source of a git install from PEP 610 direct_url.json; None otherwise."""
    from importlib import metadata

    try:
        raw = metadata.distribution("copycord").read_text("direct_url.json")
        info = json.loads(raw) if raw else {}
    except Exception:
        return None
    vcs = info.get("vcs_info") if isinstance(info, dict) else None
    url = info.get("url") if isinstance(info, dict) else None
    if not isinstance(vcs, dict) or vcs.get("vcs") != "git" or not isinstance(url, str) or not url:
        return None
    ref, commit = vcs.get("requested_revision"), vcs.get("commit_id")
    return GitSource(url, ref if isinstance(ref, str) and ref else None,
                     commit if isinstance(commit, str) and commit else None)


def installed_commit() -> Optional[str]:
    """Commit of a git install, from direct_url.json or the local checkout; None otherwise."""
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
    commit = build_commit()
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
    tty = sys.stdout.isatty()
    with ui.Rail("Copycord update status", brand=ui.brand() if tty else "") as rail:
        # On the rail the rows are a section; off it they print as they come.
        with ui.section("Copycord") if rail.active else contextlib.nullcontext():
            verdict = _status_rows(channel)
        if verdict is not None:
            verdict()
    return 0


def _status_rows(channel: Channel):
    """The rows of check(); returns what prints after them, or None when a
    warning already said why there is no answer."""
    current = current_version()
    _row("current", current)
    _row("channel", channel.describe(ui.dot()))
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
            ui.warn(f"could not get the head of {ref} from GitHub", str(e), stream=sys.stdout)
            return None
        _row("latest", f"{ref} @ {head.sha[:12]} {head.date} {head.subject}".rstrip())
        installed = installed_commit()
        if installed is None:
            return lambda: ui.info("installed commit unknown",
                                   f"run {ui.cmd('copycord update')} to install the {ref} head")
        if installed == head.sha:
            return lambda: ui.ok("Copycord is up to date")
        return lambda: ui.box([f"installed {installed[:12]}, {ref} is at {head.sha[:12]}",
                               f"Run {ui.cmd('copycord update')} to update."],
                              title="Update available", status="warn")

    try:
        with ui.Spinner("Checking GitHub releases"):
            release = fetch_latest_release(prerelease=channel.channel == BETA)
    except ReleaseError as e:
        ui.warn(str(e), stream=sys.stdout)
        return None
    except Exception as e:
        ui.warn("could not get the latest release from GitHub", str(e), stream=sys.stdout)
        return None
    latest = release.version
    _row("latest", f"{latest} ({release.tag})")
    if release.wheel_url is None:
        ui.warn(f"release {release.tag} has no wheel asset yet", stream=sys.stdout)
    if is_newer(latest, current):
        return lambda: ui.box([f"{current} -> {latest}",
                               f"Run {ui.cmd('copycord update')} to install it."],
                              title="Update available", status="warn")
    return lambda: ui.ok("Copycord is up to date")


def status_report(channel: Optional[Channel] = None) -> dict:
    """What `copycord update status --json` prints; asks GitHub without a spinner."""
    import time

    from copycord import update_check

    channel = load_channel() if channel is None else channel
    current = current_version()
    dev = channel.channel == DEV
    checked = update_check.last_check_time()
    data = {
        "current": current,
        "commit": installed_commit(),
        "channel": channel.channel,
        "ref": channel.ref if dev else None,
        "repo": channel.repo_name if dev else None,
        "method": detect_installer(),
        "last_checked": None if checked is None else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(checked)),
        "auto_update": update_check.auto_update_enabled(),
        "latest": None,
        "latest_tag": None,
        "update_available": None,
        "error": None,
    }
    try:
        if dev:
            head = fetch_branch_head(channel.ref, repo=channel.repo_name)
            data["latest"] = head.sha
            if data["commit"] is not None:
                data["update_available"] = data["commit"] != head.sha
        else:
            release = fetch_latest_release(prerelease=channel.channel == BETA)
            data["latest"], data["latest_tag"] = release.version, release.tag
            data["update_available"] = is_newer(release.version, current)
    except Exception as e:
        data["error"] = str(e) or type(e).__name__
    return data


# --------------------------------------------------------------------------
# update
# --------------------------------------------------------------------------


def _err(text: str, detail: str = "") -> None:
    ui.fail(text, detail)


def backup_database(skip: bool = False) -> bool:
    """Back up the database before updating; False when it failed."""
    from copycord.common.db import backup_pre_update
    from copycord.common.paths import db_path

    if skip:
        ui.warn("database backup skipped (--no-backup)")
        return True
    path = db_path()
    try:
        dest = backup_pre_update(str(path), current_version())
    except Exception as e:
        ui.box(
            [
                f"Could not back up {path} before updating:",
                str(e),
                "Nothing was installed. Fix the cause, or update without a",
                f"backup: {ui.cmd('copycord update --no-backup', stream=sys.stderr)}",
            ],
            title="Database backup failed", status="fail", stream=sys.stderr,
        )
        return False
    if dest is None:
        ui.info("no database yet, nothing to back up", str(path))
    else:
        ui.ok("backed up the database", str(dest))
    return True


def confirm_downgrade(current: str, target: str, yes: bool = False) -> int:
    """0 to go ahead with a downgrade, else the exit code."""
    if yes:
        ui.warn(f"downgrading from v{current} to v{target} (--yes)")
        return 0
    if not ui.is_interactive():
        _err(f"copycord update would downgrade from v{current} to v{target}",
             "no terminal to ask in; rerun with --yes to downgrade without a prompt")
        return 1
    ui.gap(blank=False)
    try:
        ok = ui.confirm(
            f"Downgrade from v{current} to v{target}? Your data is backed up first; "
            "a newer database schema will block the older version until you "
            "restore the backup.",
            default=False,
        )
    except (KeyboardInterrupt, EOFError):
        ui.gap()
        ui.warn("update cancelled", "nothing was changed")
        return 130
    ui.gap(blank=False)
    if not ok:
        ui.warn("update cancelled", "nothing was changed")
        return 1
    return 0


def confirm_update(lines: list, question: str, title: str = "Update available") -> int:
    """Show what will change and ask; 0 to go ahead, else the exit code."""
    if lines:
        ui.gap(blank=False)
        ui.box(lines, title=title, status="info")
    ui.gap(blank=False)
    try:
        ok = ui.confirm(question, default=True)
    except (KeyboardInterrupt, EOFError):
        ui.gap()
        ui.warn("update cancelled", "nothing was changed")
        return 130
    ui.gap(blank=False)
    if not ok:
        ui.note("Update cancelled, nothing was changed")
        return CANCELLED
    return 0


CANCELLED = -1  # confirm_update: the user said no; update() exits 0


def _installed_label() -> str:
    commit = installed_commit()
    return current_version() + (f" ({commit[:7]})" if commit else "")


def _target_label(target: Channel) -> str:
    ref = target.ref if target.repo_name == DEFAULT_REPO else f"{target.repo_name}@{target.ref}"
    head = _heads.get((target.repo_name, target.ref))
    if head is None:
        return ref
    return f"{ref} ({head.sha[:7]})"


# Branch heads read by nothing_new, so the prompt can show the new commit.
_heads: dict = {}


def nothing_new(target: Channel) -> Tuple[bool, Optional[Release]]:
    """(True, ...) when the install already matches the newest of target; prints why.

    Also returns the release it fetched, so the install does not ask GitHub twice.
    """
    current = current_version()
    if target.channel == DEV:
        installed = installed_commit()
        if installed is None:
            return False, None
        try:
            with ui.Spinner(f"Reading {target.repo_name}@{target.ref} from GitHub"):
                head = fetch_branch_head(target.ref, repo=target.repo_name)
        except Exception as e:
            ui.warn(f"could not get the head of {target.ref} from GitHub; installing anyway", str(e))
            return False, None
        _heads[(target.repo_name, target.ref)] = head
        if head.sha != installed:
            return False, None
        ui.ok(f"Copycord is up to date ({current}, {installed[:7]})")
        return True, None
    # A git install on a release channel is replaced by the release.
    if git_source() is not None:
        return False, None
    try:
        with ui.Spinner("Checking GitHub releases"):
            release = fetch_latest_release(prerelease=target.channel == BETA)
    except Exception:
        return False, None  # the install below reports it
    if is_newer(release.version, current) or is_older(release.version, current):
        return False, release
    ui.ok(f"Copycord is up to date ({current})")
    return True, release


def update(
    check_only: bool = False,
    channel: Optional[str] = None,
    branch: Optional[str] = None,
    tag: Optional[str] = None,
    no_install: bool = False,
    repo: Optional[str] = None,
    no_backup: bool = False,
    yes: bool = False,
    reinstall: bool = False,
    force: bool = False,
    as_json: bool = False,
) -> int:
    """Run an update; reinstall=True reinstalls the running version, for doctor --fix.

    With nothing new on the saved channel it changes nothing, unless force.
    as_json only applies with check_only.
    """
    saved = load_channel()
    source = git_source() if reinstall else None
    if reinstall:
        channel = branch = repo = None
        tag = None if saved.channel == DEV or source else current_version()
        yes = True
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
        ui.ok(f"update channel set to {target.describe(ui.dot())}", str(path))
        return 0

    if check_only:
        if as_json:
            from copycord import jsonout

            jsonout.emit(status_report(target))
            return 0
        return check(target)

    kind = detect_installer()
    if kind in (DOCKER, SOURCE):
        print(f"Copycord was installed with {kind}; update it with:\n  {manual_hint(kind)}")
        return 1
    release = None
    if not (tag or reinstall or force or target != saved):
        done, release = nothing_new(target)
        if done:
            return 0
    # Windows cannot replace the files of a running Copycord, so they are moved
    # aside first (see winupdate); another running Copycord blocks the update.
    windows = sys.platform == "win32" and kind in (UV_TOOL, PIPX)
    launchers: list = []
    if windows:
        launchers, other = winupdate.running()
        if other is not None:
            _err("Copycord is running in another window "
                 f"(process {other}); stop it first, then run this again")
            return 1

    if reinstall:
        label = "v" + tag if tag else target.describe(ui.dot()) if target.channel == DEV else "git"
        title = f"Reinstalling Copycord{ui.dot()}{label}"
    else:
        title = f"Updating Copycord{ui.dot()}{target.describe(ui.dot()) if not tag else 'tag ' + tag}"
    with ui.Rail(title, brand=ui.brand() if sys.stdout.isatty() else ""):
        ui.gap(blank=False)
        git = source is not None or (target.channel == DEV and not tag)
        if source is not None:
            spec = source.spec()
        elif target.channel == DEV and not tag:
            spec = git_spec(target.ref, target.repo)
        else:
            try:
                with ui.Spinner("Checking GitHub releases"):
                    if release is None and tag:
                        release = fetch_release(tag)
                    elif release is None:
                        release = fetch_latest_release(prerelease=target.channel == BETA)
                    spec = release.wheel()
            except ReleaseError as e:
                _err(f"cannot update: {e}")
                return 1
            except Exception as e:
                _err(f"cannot update: could not get the release from GitHub: {e}")
                return 1
            ui.ok(f"release {release.tag}")

        cmd, hint = install_command(kind, spec, reinstall)
        if cmd is None:
            ui.note(f"Copycord was installed with {kind}; update it with:")
            ui.note(f"  {hint}")
            return 1
        if shutil.which(cmd[0]) is None:
            _err(f"{cmd[0]} is not on PATH", f"update Copycord with: {hint}")
            return 1
        # Dev refs have no comparable version, so only releases can be a downgrade.
        if release is not None and is_older(release.version, current_version()):
            rc = confirm_downgrade(current_version(), release.version, yes)
            if rc:
                return rc
        elif not yes and ui.is_interactive():
            current = current_version()
            same = release.version == current if release is not None else force
            if same and target == saved:
                rc = confirm_update([], f"Reinstall Copycord {current}?")
            else:
                new = release.version if release is not None else _target_label(target)
                title = (f"Switch to the {target.channel} channel"
                         if target.channel != saved.channel else "Update available")
                rc = confirm_update([f"{_installed_label()} -> {new}"], "Update Copycord now?", title)
            if rc == CANCELLED:
                return 0
            if rc:
                return rc
        if not backup_database(no_backup):
            return 1
        # Only a git build needs npm, for the admin UI.
        cmd_env = None
        if git:
            try:
                npm_dir = node_runtime.ensure_npm()
            except node_runtime.NodeError as e:
                ui.box(
                    [
                        "The dev channel builds the admin UI from source and needs",
                        "Node.js/npm, which could not be set up:",
                        str(e),
                        f"Or stay on a release: {ui.cmd('copycord update --channel stable')}",
                    ],
                    title="Node.js not available", status="fail", stream=sys.stderr,
                )
                return 1
            cmd_env = node_runtime.with_path(npm_dir)

        # Read before the install replaces the files.
        try:
            state = service.service_state()
        except Exception as e:
            ui.warn("could not read the service state", str(e))
            state = None

        restart = bool(state is not None and state.installed and state.running)
        if release is not None:
            what = release.tag
        elif source is not None:
            what = source.ref or (source.commit or "")[:12] or "git"
        else:
            what = target.ref
        text = f"Installing Copycord {what}" + (" and building the admin UI" if git else "")
        result, log = _install(cmd, hint, cmd_env, windows, launchers, restart, text + "...")
        if result.returncode != 0:
            sys.stdout.flush()
            _err(f"update failed (exit {result.returncode})", f"reinstall with: {hint}")
            tail = (log.read_text(encoding="utf-8", errors="replace").splitlines()
                    if log.is_file() else [])[-20:]
            tail = [t for t in tail if not t.startswith(("launchers: ", "moved aside: ", "could not move"))]
            for line in tail:
                ui.note(f"  {line}", sys.stderr)
            ui.info("full log", str(log), stream=sys.stderr)
            if windows and restart:
                _restart_service()
            return result.returncode
        version, commit = installed_version()
        if version:
            ui.ok(f"Copycord updated to {version}" + (f" ({commit[:7]})" if commit else ""))
        else:
            ui.ok("Copycord updated", spec)

        # --tag is a one-off; a channel switch is saved once the install worked.
        if not tag and switching:
            try:
                path = save_channel(target)
                ui.ok(f"update channel set to {target.describe(ui.dot())}", str(path))
            except OSError as e:
                ui.warn("could not save the update channel", str(e))

        if restart and not _restart_service():
            return 1
        return 0


def _restart_service() -> bool:
    try:
        service.restart_service()
    except service.ServiceError as e:
        _err(f"updated, but restarting the service failed: {e}")
        return False
    ui.ok("restarted the copycord service")
    return True


def log_path() -> Path:
    from copycord.common.paths import logs_dir

    return logs_dir() / "update.log"


def _install(cmd, hint, env, windows, launchers, restart, text):
    """Run the install quietly into the update log; (result, log path)."""
    import subprocess

    log = log_path()
    moved: list = []
    failed: list = []
    if windows:
        from copycord.common.paths import home_dir

        if restart:
            try:
                service.get_service().stop()
            except service.ServiceError:
                pass
        winupdate.clean_old(home_dir(), launchers)
        moved = winupdate.move_aside(sys.prefix, launchers, home_dir(), failed)
    with ui.Spinner(text) as spin:
        if not spin.active:
            ui.info(text)
        if env is None:
            result = service.run_cmd(cmd, capture=True)
        else:
            result = service.run_cmd(cmd, capture=True, env=env)
    if windows and result.returncode != 0:
        winupdate.restore(moved)
    aside = "".join(f"moved aside: {src} -> {dest}\n" for src, dest in moved)
    aside += "".join(f"could not move aside: {f}\n" for f in failed)
    try:
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(f"launchers: {launchers}\n{aside}$ {hint}\n"
                       f"{result.stdout or ''}{result.stderr or ''}", encoding="utf-8")
    except OSError:
        pass
    return result, log


_VERSION_CODE = (
    "import json, copycord.update as u; "
    "print(json.dumps([u.current_version(), u.installed_commit()]))"
)


def installed_version() -> Tuple[Optional[str], Optional[str]]:
    """(version, commit) of the freshly installed Copycord, read by its own Python."""
    import subprocess

    exe = Path(sys.prefix) / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    try:
        res = subprocess.run([str(exe), "-I", "-c", _VERSION_CODE],
                             capture_output=True, text=True, timeout=60)
        version, commit = json.loads(res.stdout.strip().splitlines()[-1])
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return None, None
    return (version or None), (commit or None)
