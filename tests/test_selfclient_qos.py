"""selfclient.qos: READY's qos_token decoded, kept per account in memory only,
and sent back wrapped on the next fresh IDENTIFY in the same process. A restart
sends the default.

The tokens are real captures with the user id replaced by SYN_USER.
"""
import asyncio
import base64

import pytest

from copycord.selfclient import qos

from tests.test_selfclient_gateway import FakeWS, hello, make_client, stop, wait_until

SYN_USER = 111111111111111111
DAY_MS = 86_400_000

# A READY qos_token.
READY_TOKEN = (
    "Cj0Jx3FghPe+igER+QspYqEBAAAiIBoE6Y1CNzp9N+w03RLZFwMb7xcTNlXyKIx2yIgA9YrqKfkTQqqhAQAA"
    "EiBy1IAPq7nYDYs1ubqac5YlD6kMjd50uivIvvAaBh1uKRgB"
)
ISSUED = 1792648219641
EXPIRES = 1793857819641
# What the next fresh IDENTIFY carries for it.
READY_WRAPPED = (
    "CgASYwo9CcdxYIT3vooBEfkLKWKhAQAAIiAaBOmNQjc6fTfsNN0S2RcDG+8XEzZV8iiMdsiIAPWK6in5E0Kq"
    "oQEAABIgctSAD6u52A2LNbm6mnOWJQ+pDI3edLoryL7wGgYdbikYAQ=="
)

# An IDENTIFY qos_token from a capture, wrapping an earlier READY token.
EARLIER_IDENTIFY = (
    "CgASYwo9CcdxYIT3vooBEXBrEF2hAQAAIiAaBOmNQjc6fTfsNN0S2RcDG+8XEzZV8iiMdsiIAPWK6ilwcyml"
    "oQEAABIgaDycMgprJrvl0dkvy7vZez/PRTje2WeazTb6fxp44HMYAQ=="
)
EARLIER_READY = (
    "Cj0Jx3FghPe+igERcGsQXaEBAAAiIBoE6Y1CNzp9N+w03RLZFwMb7xcTNlXyKIx2yIgA9YrqKXBzKaWhAQAA"
    "EiBoPJwyCmsmu+XR2S/Lu9l7P89FON7ZZ5rNNvp/GnjgcxgB"
)
EARLIER_ISSUED = 1792562719600

# A day after READY_TOKEN was issued: both tokens are live.
NOW = ISSUED + DAY_MS


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setattr(qos, "_now_ms", lambda: NOW)


def ready_frame():
    d = {
        "session_id": "sess-1",
        "resume_gateway_url": "wss://resume.example.gg",
        "user": {"id": str(SYN_USER), "username": "someone"},
        "guilds": [{"id": "1"}],
        "qos_token": READY_TOKEN,
    }
    return {"op": 0, "t": "READY", "s": 1, "d": d}


def test_codec_round_trip():
    t = qos.decode_ready_token(READY_TOKEN)
    assert t.user_id == SYN_USER
    assert t.issued_ms == ISSUED
    assert t.expires_ms == EXPIRES == ISSUED + 14 * DAY_MS
    assert qos.wrap(t.raw) == READY_WRAPPED
    assert qos.unwrap(READY_WRAPPED) == t.raw
    # An IDENTIFY from a capture unwraps to its READY token and wraps back the same.
    inner = qos.unwrap(EARLIER_IDENTIFY)
    assert base64.b64encode(inner).decode() == EARLIER_READY
    assert qos.decode_raw(inner).issued_ms == EARLIER_ISSUED
    assert qos.wrap(inner) == EARLIER_IDENTIFY


def test_only_a_live_token_for_this_user_is_sent_back():
    assert qos.choose(READY_TOKEN, str(SYN_USER))[0] == READY_WRAPPED
    assert qos.choose(READY_TOKEN, now_ms=EXPIRES - 1)[0] == READY_WRAPPED
    assert qos.choose(READY_TOKEN, now_ms=EXPIRES)[0] == qos.NO_TOKEN
    assert qos.choose(READY_TOKEN, "222")[0] == qos.NO_TOKEN
    assert qos.choose(None)[0] == qos.NO_TOKEN
    assert qos.choose("not base64 !!")[0] == qos.NO_TOKEN


@pytest.mark.asyncio
async def test_ready_token_goes_out_on_the_next_identify_until_a_restart():
    first = FakeWS([hello(), ready_frame(), {"op": 9, "d": False}])
    second = FakeWS([hello()])
    client, _ = make_client(first, second)
    task = asyncio.create_task(client.run(refresh_build=False))
    await wait_until(lambda: second.frames(2))
    await stop(client, task)
    assert first.frames(2)[0]["d"]["qos_token"] == qos.NO_TOKEN
    assert second.frames(2)[0]["d"]["qos_token"] == READY_WRAPPED

    # A restart: a new process has a new, empty store and a new client.
    qos.set_store(qos.QosTokenStore())
    ws = FakeWS([hello()])
    client, _ = make_client(ws)
    task = asyncio.create_task(client.run(refresh_build=False))
    await wait_until(lambda: ws.frames(2))
    await stop(client, task)
    assert ws.frames(2)[0]["d"]["qos_token"] == qos.NO_TOKEN
