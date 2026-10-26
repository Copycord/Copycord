"""Broad last-resort handlers keep their outcome and log the traceback at debug level."""

import logging

import pytest

from copycord.common import token_proxy
from copycord.server.token_pool import TokenSessionPool, _Session


pytestmark = pytest.mark.asyncio


class _BoomClient:
    async def close(self):
        raise RuntimeError("close boom")


async def test_teardown_close_failure_is_logged_with_traceback(caplog):
    caplog.set_level(logging.DEBUG, logger="test.token_pool")
    pool = TokenSessionPool(
        logger=logging.getLogger("test.token_pool"),
        proxy_for=lambda token: None,
        proxy_pool=object(),
    )
    sess = _Session("k1", "tok-1", "online")
    sess.client = _BoomClient()

    await pool._teardown(sess)

    assert sess.client is None and sess.runner is None
    records = [r for r in caplog.records if "close failed for k1" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert records[0].exc_info and records[0].exc_info[0] is RuntimeError


def test_proxies_configured_failure_is_false_and_logged(caplog):
    caplog.set_level(logging.DEBUG, logger="common.token_proxy")

    class Boom:
        @property
        def enabled(self):
            raise RuntimeError("pool boom")

    assert token_proxy.proxies_configured(Boom()) is False
    records = [r for r in caplog.records if r.name == "common.token_proxy"]
    assert len(records) == 1 and records[0].levelno == logging.DEBUG
    assert records[0].exc_info and records[0].exc_info[0] is RuntimeError
