"""The dashboard release watcher follows the saved channel like the CLI: beta sees
pre-releases, stable does not, and /version reports update_available from what it stored."""
import asyncio

import pytest

from copycord import update
from copycord.admin import app as app_module

RELEASES = [
    {"tag_name": "v4.0.0.2", "prerelease": False, "draft": False},
    {"tag_name": "v4.0.0.3", "prerelease": True, "draft": False},
]


class FakeDb:
    def __init__(self):
        self.config = {}

    def get_config(self, key, default=""):
        return self.config.get(key, default)

    def set_config(self, key, value):
        self.config[key] = value

    def get_version(self):
        return "v4.0.0.2"


@pytest.fixture()
def watcher(monkeypatch):
    db = FakeDb()
    monkeypatch.setattr(app_module, "db", db)
    monkeypatch.setattr(app_module, "CURRENT_VERSION", "v4.0.0.2")
    monkeypatch.setattr(app_module, "is_dev_build", lambda: False)
    # GitHub is never called: the release list is canned.
    monkeypatch.setattr(update, "_fetch_json", lambda url, timeout: RELEASES)

    def use(channel):
        monkeypatch.setattr(update, "load_channel", lambda: update.Channel(channel))

    return db, use


def test_beta_channel_sees_a_newer_prerelease(watcher):
    db, use = watcher
    use(update.BETA)
    asyncio.run(app_module._poll_release())
    assert db.config["latest_tag"] == "v4.0.0.3"
    assert db.config["latest_url"].endswith("/releases/tag/v4.0.0.3")
    assert app_module.get_version()["update_available"] is True


def test_stable_channel_ignores_the_prerelease(watcher):
    db, use = watcher
    use(update.STABLE)
    asyncio.run(app_module._poll_release())
    assert db.config["latest_tag"] == "v4.0.0.2"
    assert app_module.get_version()["update_available"] is False


def test_switching_channel_shows_on_the_next_poll(watcher):
    db, use = watcher
    use(update.BETA)
    asyncio.run(app_module._poll_release())
    use(update.STABLE)
    asyncio.run(app_module._poll_release())
    assert db.config["latest_tag"] == "v4.0.0.2"
    assert app_module.get_version()["update_available"] is False
