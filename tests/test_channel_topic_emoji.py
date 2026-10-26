import logging

import discord
import pytest

from copycord.server import server as srv

TOPIC = "Rules <:gone:123456789012345678>  and <a:spin:223456789012345678> here 🎉"
CLEAN = "Rules and here 🎉"


def _http_error(status, code, message, errors=None):
    resp = type("Resp", (), {"status": status, "reason": message})()
    data = {"code": code, "message": message}
    if errors:
        data["errors"] = errors
    return discord.HTTPException(resp, data)


def _deleted_emoji_error():
    return _http_error(
        400, 50035, "Invalid Form Body",
        {"topic": {"_errors": [{"code": "X", "message": "Field contains deleted emoji"}]}},
    )


class _Channel:
    def __init__(self, errors=()):
        self.id = 555
        self.topic = None
        self.calls = []
        self._errors = list(errors)

    async def edit(self, **kw):
        self.calls.append(kw)
        if self._errors:
            raise self._errors.pop(0)
        if "topic" in kw:
            self.topic = kw["topic"]


def _receiver():
    return srv.ServerReceiver.__new__(srv.ServerReceiver)


def test_strip_custom_emoji_keeps_unicode():
    assert srv._strip_custom_emoji(TOPIC) == CLEAN
    assert srv._strip_custom_emoji("plain 😀 text") == "plain 😀 text"


@pytest.mark.asyncio
async def test_deleted_emoji_topic_retried_once_without_custom_emoji(caplog):
    r = _receiver()
    ch = _Channel([_deleted_emoji_error()])
    changes = {"topic": TOPIC, "slowmode_delay": 5}
    with caplog.at_level(logging.INFO, logger="server"):
        await r._edit_channel_metadata(ch, changes)
    assert ch.calls == [
        {"topic": TOPIC, "slowmode_delay": 5},
        {"topic": CLEAN, "slowmode_delay": 5},
    ]
    assert changes["topic"] == CLEAN
    infos = [x for x in caplog.records if "without its deleted custom emoji" in x.getMessage()]
    assert len(infos) == 1 and infos[0].levelno == logging.INFO
    assert not [x for x in caplog.records if x.levelno >= logging.WARNING]

    # Later syncs compare and send the sanitized topic.
    assert r._clone_topic(ch.id, TOPIC) == CLEAN
    assert ch.topic == r._clone_topic(ch.id, TOPIC)
    # Other channels are untouched.
    assert r._clone_topic(999, TOPIC) == TOPIC


@pytest.mark.asyncio
async def test_retry_failure_raises_original_error():
    r = _receiver()
    original = _deleted_emoji_error()
    ch = _Channel([original, _http_error(500, 0, "boom")])
    with pytest.raises(discord.HTTPException) as info:
        await r._edit_channel_metadata(ch, {"topic": TOPIC})
    assert info.value is original
    assert len(ch.calls) == 2
    assert r._clone_topic(ch.id, TOPIC) == TOPIC


@pytest.mark.asyncio
async def test_other_400_not_retried():
    r = _receiver()
    err = _http_error(
        400, 50035, "Invalid Form Body",
        {"name": {"_errors": [{"code": "X", "message": "Must be 100 or fewer in length."}]}},
    )
    ch = _Channel([err])
    with pytest.raises(discord.HTTPException):
        await r._edit_channel_metadata(ch, {"topic": TOPIC})
    assert len(ch.calls) == 1


@pytest.mark.asyncio
async def test_topic_without_custom_emoji_untouched():
    r = _receiver()
    ch = _Channel()
    await r._edit_channel_metadata(ch, {"topic": "hello 😀"})
    assert ch.calls == [{"topic": "hello 😀"}]
    assert r._clone_topic(ch.id, "hello 😀") == "hello 😀"
    # Nothing custom to strip: the error is not retried.
    ch2 = _Channel([_deleted_emoji_error()])
    with pytest.raises(discord.HTTPException):
        await r._edit_channel_metadata(ch2, {"topic": "hello 😀"})
    assert len(ch2.calls) == 1
