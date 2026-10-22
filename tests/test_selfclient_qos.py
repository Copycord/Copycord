"""selfclient.qos: READY's qos_token decoded, kept per account in memory only,
and sent back wrapped on the next fresh IDENTIFY in the same process, never on
RESUME. A restart sends the default.

The tokens are real captures with the user id replaced by SYN_USER.
"""
import asyncio
import base64
import logging

import pytest

from copycord.common import traffic_capture
from copycord.selfclient import qos
from copycord.selfclient.gateway import GatewayClient

from tests.test_selfclient_gateway import FakeWS, hello, make_client, stop, wait_until

TOKEN = "test-token-not-real"
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


def ready_frame(qos_token=READY_TOKEN, user_id=str(SYN_USER), seq=1):
    d = {
        "session_id": "sess-1",
        "resume_gateway_url": "wss://resume.example.gg",
        "user": {"id": user_id, "username": "someone"},
        "guilds": [{"id": "1"}],
    }
    if qos_token is not None:
        d["qos_token"] = qos_token
    return {"op": 0, "t": "READY", "s": seq, "d": d}


def remember(qos_token=READY_TOKEN, user_id=str(SYN_USER), token=TOKEN):
    return qos.remember_ready(token, ready_frame(qos_token, user_id)["d"])


# codec


class TestCodec:
    def test_ready_token_decodes(self):
        t = qos.decode_ready_token(READY_TOKEN)
        assert len(t.raw) == 99
        assert t.user_id == SYN_USER
        assert t.issued_ms == ISSUED
        assert t.expires_ms == EXPIRES == ISSUED + 14 * DAY_MS
        top = qos.read_fields(t.raw)
        assert len(top[1][0]) == 61
        assert len(top[2][0]) == 32
        assert top[3] == [1]

    def test_wrap_gives_the_identify_string(self):
        t = qos.decode_ready_token(READY_TOKEN)
        assert qos.wrap(t.raw) == READY_WRAPPED
        assert base64.b64decode(READY_WRAPPED)[:4] == b"\x0a\x00\x12\x63"

    def test_identify_capture_unwraps_and_rewraps(self):
        inner = qos.unwrap(EARLIER_IDENTIFY)
        assert base64.b64encode(inner).decode() == EARLIER_READY
        t = qos.decode_raw(inner)
        assert t.user_id == SYN_USER
        assert t.issued_ms == EARLIER_ISSUED
        assert t.expires_ms == EARLIER_ISSUED + 14 * DAY_MS
        assert qos.wrap(inner) == EARLIER_IDENTIFY

    def test_no_token_is_f1_f1_1(self):
        assert qos.NO_TOKEN == "CgIIAQ=="
        assert qos.read_fields(base64.b64decode(qos.NO_TOKEN)) == {1: [b"\x08\x01"]}
        assert qos.unwrap(qos.NO_TOKEN) is None

    @pytest.mark.parametrize(
        "value",
        [
            None,
            "",
            42,
            "not base64 !!",
            qos.NO_TOKEN,  # f1 has no expiry
            READY_TOKEN[:40],  # truncated
            base64.b64encode(b"\x0b\x01").decode(),  # wire type 3
            base64.b64encode(b"\x0a\x03\x29\x01\x02").decode(),  # short fixed64
        ],
    )
    def test_junk_does_not_decode(self, value):
        assert qos.decode_ready_token(value) is None

    def test_long_token_takes_a_two_byte_length(self):
        raw = bytes(range(200))
        wrapped = base64.b64decode(qos.wrap(raw))
        assert wrapped[:5] == b"\x0a\x00\x12\xc8\x01"
        assert qos.unwrap(qos.wrap(raw)) == raw


class TestChoose:
    def test_live_token_is_wrapped(self):
        assert qos.choose(READY_TOKEN)[0] == READY_WRAPPED
        assert qos.choose(READY_TOKEN, str(SYN_USER))[0] == READY_WRAPPED

    def test_expired_token_falls_back(self):
        assert qos.choose(READY_TOKEN, now_ms=EXPIRES - 1)[0] == READY_WRAPPED
        assert qos.choose(READY_TOKEN, now_ms=EXPIRES)[0] == qos.NO_TOKEN

    def test_another_users_token_falls_back(self):
        assert qos.choose(READY_TOKEN, "222")[0] == qos.NO_TOKEN

    def test_nothing_or_junk_falls_back(self):
        assert qos.choose(None)[0] == qos.NO_TOKEN
        assert qos.choose("not base64 !!")[0] == qos.NO_TOKEN


# store


class TestStore:
    def test_remembered_token_is_sent_back(self):
        assert qos.identify_token(TOKEN) == qos.NO_TOKEN
        assert remember().expires_ms == EXPIRES
        assert qos.identify_token(TOKEN) == READY_WRAPPED

    def test_newest_ready_wins(self):
        remember(READY_TOKEN)
        remember(EARLIER_READY)
        assert qos.identify_token(TOKEN) == EARLIER_IDENTIFY
        remember(READY_TOKEN)
        assert qos.identify_token(TOKEN) == READY_WRAPPED

    def test_ready_without_a_token_keeps_the_stored_one(self):
        remember()
        assert remember(qos_token=None) is None
        assert qos.identify_token(TOKEN) == READY_WRAPPED

    def test_each_account_has_its_own(self):
        remember()
        assert qos.identify_token("another-token") == qos.NO_TOKEN

    def test_expired_token_is_not_sent(self, monkeypatch):
        remember()
        monkeypatch.setattr(qos, "_now_ms", lambda: EXPIRES + 1)
        assert qos.identify_token(TOKEN) == qos.NO_TOKEN

    def test_token_issued_to_another_user_is_not_sent(self):
        # READY named another account than the one the token was issued to.
        remember(user_id="222")
        assert qos.identify_token(TOKEN) == qos.NO_TOKEN
        remember()
        assert qos.identify_token(TOKEN, "222") == qos.NO_TOKEN
        assert qos.identify_token(TOKEN, str(SYN_USER)) == READY_WRAPPED

    def test_new_store_after_a_restart_starts_empty(self):
        remember()
        assert qos.identify_token(TOKEN) == READY_WRAPPED
        qos.set_store(qos.QosTokenStore())
        assert qos.identify_token(TOKEN) == qos.NO_TOKEN

    def test_default_store_is_one_per_process(self):
        qos.set_store(None)
        remember()
        assert qos.store() is qos.store()
        assert qos.identify_token(TOKEN) == READY_WRAPPED

    def test_logs_never_carry_the_token(self, caplog):
        caplog.set_level(logging.DEBUG, logger="selfclient.qos")
        remember()
        qos.identify_token(TOKEN)
        assert READY_TOKEN not in caplog.text
        assert READY_WRAPPED not in caplog.text
        assert qos._iso(EXPIRES) in caplog.text
        assert qos.decode_ready_token(READY_TOKEN).ref in caplog.text


# gateway


class TestGateway:
    @pytest.mark.asyncio
    async def test_fresh_identify_sends_the_stored_token(self):
        remember()
        ws = FakeWS([hello()])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(2))
        await stop(client, task)

        d = ws.frames(2)[0]["d"]
        assert list(d)[-1] == "qos_token"
        assert d["qos_token"] == READY_WRAPPED

    @pytest.mark.asyncio
    async def test_ready_token_goes_out_on_the_next_identify(self):
        first = FakeWS([hello(), ready_frame(), {"op": 9, "d": False}])
        second = FakeWS([hello()])
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(2))
        await stop(client, task)

        assert first.frames(2)[0]["d"]["qos_token"] == qos.NO_TOKEN
        assert second.frames(2)[0]["d"]["qos_token"] == READY_WRAPPED
        assert client.user_id == str(SYN_USER)

    @pytest.mark.asyncio
    async def test_resume_carries_no_qos_token(self):
        first = FakeWS([hello(), ready_frame(), {"op": 7, "d": None}])
        second = FakeWS([hello(), {"op": 0, "t": "RESUMED", "s": 2, "d": {}}])
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(37))
        await stop(client, task)

        assert not second.frames(2)
        (resume,) = second.frames(6)
        assert list(resume["d"]) == ["token", "session_id", "seq"]
        assert all("qos_token" not in (m.get("d") or {}) for m in second.sent)

    @pytest.mark.asyncio
    async def test_restart_sends_the_default_token(self):
        ws = FakeWS([hello(), ready_frame()])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(37))
        await stop(client, task)
        assert qos.identify_token(TOKEN) == READY_WRAPPED

        # A restart: a new process has a new, empty store and a new client.
        qos.set_store(qos.QosTokenStore())
        ws2 = FakeWS([hello()])
        client2, _ = make_client(ws2)
        assert client2 is not client
        task2 = asyncio.create_task(client2.run(refresh_build=False))
        await wait_until(lambda: ws2.frames(2))
        await stop(client2, task2)
        assert ws2.frames(2)[0]["d"]["qos_token"] == qos.NO_TOKEN

    @pytest.mark.asyncio
    async def test_another_users_token_is_not_sent(self):
        first = FakeWS([hello(), ready_frame(user_id="222"), {"op": 9, "d": False}])
        second = FakeWS([hello()])
        client, _ = make_client(first, second)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: second.frames(2))
        await stop(client, task)
        assert second.frames(2)[0]["d"]["qos_token"] == qos.NO_TOKEN

    @pytest.mark.asyncio
    async def test_expired_token_is_not_sent(self, monkeypatch):
        remember()
        monkeypatch.setattr(qos, "_now_ms", lambda: EXPIRES)
        ws = FakeWS([hello()])
        client, _ = make_client(ws)
        task = asyncio.create_task(client.run(refresh_build=False))
        await wait_until(lambda: ws.frames(2))
        await stop(client, task)
        assert ws.frames(2)[0]["d"]["qos_token"] == qos.NO_TOKEN

    def test_gateway_client_starts_without_a_user(self):
        assert GatewayClient(TOKEN).user_id is None


class TestTokenAvatar:
    @pytest.mark.asyncio
    async def test_identify_sends_the_stored_token(self):
        from copycord.server import token_avatar

        assert (await token_avatar._identify_payload(TOKEN))["d"]["qos_token"] == qos.NO_TOKEN
        remember()
        d = (await token_avatar._identify_payload(TOKEN))["d"]
        assert list(d)[-1] == "qos_token"
        assert d["qos_token"] == READY_WRAPPED


class TestCapture:
    def test_ready_summary_keeps_the_qos_token(self):
        summary = traffic_capture.summarize_ready(ready_frame()["d"])
        assert summary["qos_token"] == READY_TOKEN

    def test_identify_keeps_the_qos_token_and_redacts_the_auth_token(self):
        d = traffic_capture.redact_json({"token": TOKEN, "qos_token": READY_WRAPPED})
        assert d["qos_token"] == READY_WRAPPED
        assert d["token"].startswith("<redacted")
