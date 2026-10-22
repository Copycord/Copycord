# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
The Copycord version. It comes from the git tag at build time (hatch-vcs): a
build at tag v4.0.0.3 is 4.0.0.3, shown as v4.0.0.3. A build of any other
commit is a dev build, 4.0.0.4.dev5+g1a2b3c4, shown as "dev (1a2b3c4)".
"""

from __future__ import annotations

import functools
import json
import logging
import re
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger("copycord.version")

DIST_NAME = "copycord"
# What a build without git history or a version reports.
UNKNOWN = "0+unknown"
DEV = "dev"

# src/copycord/version.py -> the checkout, when Copycord runs from a source tree.
_SOURCE_ROOT = Path(__file__).resolve().parents[2]
_DESCRIBE_RE = re.compile(
    r"^v?(?P<tag>\d+(?:\.\d+)*)-(?P<distance>\d+)-g(?P<sha>[0-9a-f]+)(?P<dirty>-dirty)?$"
)
_SHA_ONLY_RE = re.compile(r"^(?P<sha>[0-9a-f]{7,40})(?P<dirty>-dirty)?$")
# The commit in a dev version's local part: 4.0.0.4.dev5+g1a2b3c4(.d20261022).
_LOCAL_SHA_RE = re.compile(r"\+(?:[a-z0-9]+\.)*g(?P<sha>[0-9a-f]{7,40})(?:\.|$)", re.IGNORECASE)


def describe_to_version(describe: str) -> Optional[str]:
    """The version hatch-vcs builds for `git describe --tags --long --dirty` output.

    At a tag: the tag. Past it or dirty: the last number plus one, .devN and the commit.
    """
    describe = describe.strip()
    m = _DESCRIBE_RE.match(describe)
    if m is None:
        m = _SHA_ONLY_RE.match(describe)
        if m is None:
            return None
        # No tag in the history (a shallow clone).
        return f"0.1.dev0+g{m['sha']}" + (".dirty" if m["dirty"] else "")
    tag, distance = m["tag"], int(m["distance"])
    if distance == 0 and not m["dirty"]:
        return tag
    head, _, last = tag.rpartition(".")
    bumped = f"{head}.{int(last) + 1}" if head else str(int(last) + 1)
    return f"{bumped}.dev{distance}+g{m['sha']}" + (".dirty" if m["dirty"] else "")


def _git(root: Path, *args: str) -> Optional[str]:
    try:
        res = subprocess.run(["git", "-C", str(root), *args],
                             capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return res.stdout.strip() if res.returncode == 0 else None


def _is_checkout(root: Path) -> bool:
    return (root / ".git").exists() and (root / "pyproject.toml").is_file()


def source_version(root: Path = _SOURCE_ROOT) -> Optional[str]:
    """The version of a git checkout Copycord runs from; None when it is not one."""
    if not _is_checkout(root):
        return None
    out = _git(root, "describe", "--tags", "--long", "--dirty", "--always",
               "--abbrev=7", "--match", "v[0-9]*")
    return describe_to_version(out) if out else None


def installed_version() -> Optional[str]:
    """The version in the installed package metadata; None when not installed."""
    from importlib import metadata

    try:
        return metadata.version(DIST_NAME) or None
    except metadata.PackageNotFoundError:
        return None


@functools.lru_cache(maxsize=1)
def package_version() -> str:
    """The PEP 440 version of the running Copycord: 4.0.0.3 or 4.0.0.4.dev5+g1a2b3c4.

    The installed package metadata says it, except when the code runs from a git
    checkout (PYTHONPATH=src, an editable install): metadata can be from another
    build then, so the checkout's own tag and commit decide. 0+unknown when neither.
    """
    return source_version() or installed_version() or UNKNOWN


def is_dev_build(version: Optional[str] = None) -> bool:
    """True for anything but a release: a .dev or local part (+g1a2b3c4, +unknown)."""
    v = package_version() if version is None else version
    return "+" in v or bool(re.search(r"\.?dev\d*", v, re.IGNORECASE))


def _build_json_commit() -> Optional[str]:
    """The commit hatch_build.py stamps into a wheel (copycord/_build.json)."""
    from importlib import resources

    try:
        raw = (resources.files("copycord") / "_build.json").read_text(encoding="utf-8")
        commit = json.loads(raw).get("commit", "")
    except Exception:
        logger.debug("build stamp unreadable", exc_info=True)
        return None
    return commit if re.fullmatch(r"[0-9a-f]{40}", commit or "") else None


def short_commit(version: Optional[str] = None) -> Optional[str]:
    """The 7-character commit of a dev build: from its version, else (for the
    running Copycord) the wheel's build stamp or the checkout; None when unknown."""
    v = package_version() if version is None else version
    m = _LOCAL_SHA_RE.search(v)
    if m:
        return m["sha"][:7].lower()
    if version is not None:
        return None
    sha = _build_json_commit()
    if sha is None and _is_checkout(_SOURCE_ROOT):
        sha = _git(_SOURCE_ROOT, "rev-parse", "HEAD")
    return sha[:7] if sha and re.fullmatch(r"[0-9a-f]{7,40}", sha) else None


def display_version(version: Optional[str] = None) -> str:
    """The version as shown: v4.0.0.3 for a release, "dev (1a2b3c4)" (or "dev") for a dev build."""
    v = package_version() if version is None else version
    if not is_dev_build(v):
        return "v" + v.lstrip("v")
    sha = short_commit(version)
    return f"{DEV} ({sha})" if sha else DEV
