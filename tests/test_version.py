"""The version comes from the git tag; any other build is a dev build that shows
its commit, is never offered a release on its own and leaves the DB stamp alone."""
import asyncio
import logging
import shutil
import subprocess
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from copycord import update, version

ROOT = Path(__file__).resolve().parents[1]


def _git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", *args],
        capture_output=True, text=True, check=True, timeout=30,
    ).stdout.strip()


@pytest.mark.skipif(shutil.which("git") is None, reason="needs git")
def test_a_checkout_at_a_tag_is_that_version_and_later_commits_are_dev(tmp_path):
    _git(tmp_path, "init", "-q")
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n", encoding="utf-8")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-q", "-m", "one")
    _git(tmp_path, "tag", "v4.0.0.3")
    assert version.source_version(tmp_path) == "4.0.0.3"

    _git(tmp_path, "commit", "-q", "--allow-empty", "-m", "two")
    sha = _git(tmp_path, "rev-parse", "--short=7", "HEAD")
    dev = version.source_version(tmp_path)
    assert dev == f"4.0.0.4.dev1+g{sha}"
    assert version.display_version(dev) == f"dev ({sha})"


@pytest.mark.parametrize("describe, expected", [
    ("v4.0.0.3-0-g1a2b3c4", "4.0.0.3"),
    ("v4.0.0.3-5-g1a2b3c4", "4.0.0.4.dev5+g1a2b3c4"),
    ("v4.0.0.3-0-g1a2b3c4-dirty", "4.0.0.4.dev0+g1a2b3c4.dirty"),
    ("1a2b3c4", "0.1.dev0+g1a2b3c4"),
    ("not a describe", None),
])
def test_describe_to_version(describe, expected):
    assert version.describe_to_version(describe) == expected


def test_a_dev_build_sorts_after_the_last_release_and_before_the_next():
    assert update.is_newer("4.0.0.4.dev5+g1a2b3c4", "4.0.0.3")
    assert update.is_older("4.0.0.4.dev5+g1a2b3c4", "4.0.0.4")


@pytest.mark.parametrize("v, dev, shown", [
    ("4.0.0.3", False, "v4.0.0.3"),
    ("4.0.0.4.dev5+g1a2b3c4", True, "dev (1a2b3c4)"),
    ("4.0.0.4.dev5+g1a2b3c4.d20261022", True, "dev (1a2b3c4)"),
    ("0+unknown", True, "dev"),
])
def test_is_dev_build_and_display(v, dev, shown):
    assert version.is_dev_build(v) is dev
    assert version.display_version(v) == shown


def test_without_git_or_metadata_the_version_is_unknown(tmp_path, monkeypatch):
    # A source tree without .git (a zip download, a Docker build context).
    (tmp_path / "pyproject.toml").write_text("", encoding="utf-8")
    assert version.source_version(tmp_path) is None
    monkeypatch.setattr(version, "source_version", lambda: None)
    monkeypatch.setattr(version, "installed_version", lambda: None)
    version.package_version.cache_clear()
    try:
        assert version.package_version() == version.UNKNOWN == "0+unknown"
        assert version.is_dev_build()
    finally:
        version.package_version.cache_clear()


def test_the_build_takes_the_version_from_the_tag():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert "hatch-vcs" in project["build-system"]["requires"]
    hatch = project["tool"]["hatch"]["version"]
    assert hatch["source"] == "vcs"
    assert hatch["fallback-version"] == version.UNKNOWN
    assert "CURRENT_VERSION = \"" not in (ROOT / "src/copycord/common/config.py").read_text(encoding="utf-8")


class _DB:
    def __init__(self):
        self.stamped = []

    def get_version(self):
        return ""

    def set_version(self, v):
        self.stamped.append(v)

    def get_config(self, key, default=""):
        return default

    def get_notified_version(self):
        return ""


class _Bot:
    guilds = []

    def __init__(self):
        self.cycles = 1

    async def wait_until_ready(self):
        pass

    def is_closed(self):
        self.cycles -= 1
        return self.cycles < 0


@pytest.mark.parametrize("dev", [False, True])
def test_only_a_release_stamps_its_version_in_the_db(monkeypatch, dev):
    from copycord.common import config

    monkeypatch.setattr(config, "is_dev_build", lambda: dev)
    cfg = config.Config.__new__(config.Config)
    cfg.logger = logging.getLogger("test_version")
    cfg.RELEASE_CHECK_INTERVAL_SECONDS = 0
    db = _DB()
    asyncio.run(cfg.setup_release_watcher(SimpleNamespace(bot=_Bot(), db=db), should_dm=False))
    assert db.stamped == ([] if dev else [config.CURRENT_VERSION])
