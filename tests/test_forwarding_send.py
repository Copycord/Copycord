"""
Unit tests for ForwardingManager._send_discord_webhook fan-out behavior.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

CODE_DIR = os.path.join(os.path.dirname(__file__), "..", "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(CODE_DIR))

import client.forwarding as fwd  # noqa: E402
from client.forwarding import (  # noqa: E402
    ForwardingManager,
    ForwardingRule,
    ForwardingJob,
    ForwardingFilters,
    RetryableForwardingError,
)

VALID_A = "https://discord.com/api/webhooks/111/aaa"
VALID_B = "https://discord.com/api/webhooks/222/bbb"


def _manager(db):
    return ForwardingManager(
        config=SimpleNamespace(),
        db=db,
        ws=SimpleNamespace(),
        loop=asyncio.get_event_loop(),
    )


def _rule(urls):
    return ForwardingRule(
        rule_id="r1",
        guild_id=1,
        label="L",
        provider="discord",
        enabled=True,
        config={"urls": list(urls)},
        filters=ForwardingFilters.from_dict({}),
    )


def _attrs():
    return {"content": "hello", "message_id": 9001, "channel_name": "general"}


def _job(rule):
    return ForwardingJob(
        provider_queue="discord", rule=rule, message_id="9001", attrs=_attrs()
    )


def _install_fake_post(monkeypatch, status_by_url):
    """Patch _post_with_discord_429_retry to a recorder driven by status_by_url."""
    calls = []

    async def fake(session, url, payload):
        calls.append(url)
        status = status_by_url[url]
        return status, "", None

    monkeypatch.setattr(fwd, "_post_with_discord_429_retry", fake)
    return calls


class TestFanOut:
    @pytest.mark.asyncio
    async def test_posts_to_every_url_and_records_one_event(self, db, monkeypatch):
        mgr = _manager(db)
        rule = _rule([VALID_A, VALID_B])
        job = _job(rule)
        calls = _install_fake_post(monkeypatch, {VALID_A: 204, VALID_B: 204})

        await mgr._send_discord_webhook(
            rule=rule, attrs=_attrs(), session=None, job=job, attempt=0
        )

        assert calls == [VALID_A, VALID_B]
        assert job.delivered_urls == {VALID_A, VALID_B}
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is True

    @pytest.mark.asyncio
    async def test_transient_failure_requeues_and_retry_skips_delivered(
        self, db, monkeypatch
    ):
        mgr = _manager(db)
        rule = _rule([VALID_A, VALID_B])
        job = _job(rule)

        # Attempt 1: A succeeds, B is a transient 500 -> job should requeue.
        _install_fake_post(monkeypatch, {VALID_A: 204, VALID_B: 500})
        with pytest.raises(RetryableForwardingError):
            await mgr._send_discord_webhook(
                rule=rule, attrs=_attrs(), session=None, job=job, attempt=0
            )
        assert job.delivered_urls == {VALID_A}
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is False

        # Attempt 2 (retry): only B is posted, then the event is recorded.
        calls = _install_fake_post(monkeypatch, {VALID_A: 204, VALID_B: 204})
        await mgr._send_discord_webhook(
            rule=rule, attrs=_attrs(), session=None, job=job, attempt=1
        )
        assert calls == [VALID_B]
        assert job.delivered_urls == {VALID_A, VALID_B}
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is True

    @pytest.mark.asyncio
    async def test_permanent_4xx_is_dropped_not_retried(self, db, monkeypatch):
        mgr = _manager(db)
        rule = _rule([VALID_A, VALID_B])
        job = _job(rule)
        _install_fake_post(monkeypatch, {VALID_A: 204, VALID_B: 404})

        # No exception: B is dropped, A delivered, event still recorded.
        await mgr._send_discord_webhook(
            rule=rule, attrs=_attrs(), session=None, job=job, attempt=0
        )
        assert job.delivered_urls == {VALID_A}
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is True

    @pytest.mark.asyncio
    async def test_all_urls_dropped_records_event_no_exception(self, db, monkeypatch):
        mgr = _manager(db)
        rule = _rule([VALID_A, VALID_B])
        job = _job(rule)
        _install_fake_post(monkeypatch, {VALID_A: 404, VALID_B: 404})

        # No exception: both dropped, nothing delivered, event still recorded.
        await mgr._send_discord_webhook(
            rule=rule, attrs=_attrs(), session=None, job=job, attempt=0
        )
        assert job.delivered_urls == set()
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is True
