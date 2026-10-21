"""The release watcher sets the server's status and says nothing on the client."""
import logging
from types import SimpleNamespace

import pytest

from copycord.common.config import CURRENT_VERSION, Config


class _Bot:
    """Closed after one watcher cycle."""

    def __init__(self):
        self.checks = 0

    async def wait_until_ready(self):
        pass

    def is_closed(self):
        self.checks += 1
        return self.checks > 1


class _DB:
    def get_version(self):
        return CURRENT_VERSION

    def set_version(self, v):
        pass

    def get_config(self, key, default=None):
        return default

    def get_notified_version(self):
        return ""


def _watcher_self(logger):
    return SimpleNamespace(logger=logger, RELEASE_CHECK_INTERVAL_SECONDS=0)


@pytest.mark.asyncio
async def test_client_receiver_skips_status_without_a_log_line(caplog):
    caplog.set_level(logging.DEBUG, logger="test.release")
    receiver = SimpleNamespace(bot=_Bot(), db=_DB())
    await Config.setup_release_watcher(
        _watcher_self(logging.getLogger("test.release")), receiver, should_dm=False
    )
    assert receiver.bot.checks == 2
    assert not [r for r in caplog.records if "status update" in r.getMessage()]


@pytest.mark.asyncio
async def test_server_receiver_still_gets_the_status():
    seen = []

    async def update_status(text):
        seen.append(text)

    receiver = SimpleNamespace(bot=_Bot(), db=_DB(), update_status=update_status)
    await Config.setup_release_watcher(
        _watcher_self(logging.getLogger("test.release")), receiver, should_dm=False
    )
    assert seen == [CURRENT_VERSION]
