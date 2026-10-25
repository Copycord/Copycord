"""The token sender's REST calls in the dev-mode traffic capture (traffic_capture.py).

A real UserTokenSender on a fake curl_cffi session, as in test_token_sender.py;
nothing reaches Discord. The token below is not a real one.
"""
import asyncio
import json
import time
import types

import pytest
from curl_cffi.requests.exceptions import RequestException as CurlRequestException

from copycord.common import traffic_capture as tc
from copycord.server import token_sender
from copycord.server.token_sender import SEND_OK, UserTokenSender

TOKEN = "sender-capture-token-not-real-0123456789.abcdef.ghijklmnop"


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    tc.forget_all()
    yield tmp_path
    tc.forget_all()


@pytest.fixture
def dev(data, monkeypatch):
    monkeypatch.setenv("COPYCORD_ENV", "dev")
    return data


@pytest.fixture
def prod(data, monkeypatch):
    monkeypatch.delenv("COPYCORD_ENV", raising=False)
    return data


@pytest.fixture
def no_wait(monkeypatch):
    async def _instant(*a, **k):
        return None

    monkeypatch.setattr(asyncio, "sleep", _instant)


def capture_files(root):
    return sorted((root / tc.CAPTURE_DIR).glob(f"*/{tc.EVENTS_FILE}"))


def read_text(root):
    (path,) = capture_files(root)
    return path.read_text(encoding="utf-8")


def read_events(root):
    return [json.loads(line) for line in read_text(root).splitlines()]


def of_type(events, type_):
    return [e for e in events if e["type"] == type_]


class Resp:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body
        self.headers = {}
        self.text = "" if body is None else json.dumps(body)

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class Session:
    """Answers from a script: (status, body) or an exception to raise."""

    def __init__(self, script=()):
        self.script = list(script)
        self.calls = []

    def _next(self, method, url, kwargs):
        self.calls.append((method, url, kwargs))
        item = self.script.pop(0) if self.script else (200, {})
        if isinstance(item, BaseException):
            raise item
        return Resp(*item)

    async def post(self, url, **kwargs):
        return self._next("POST", url, kwargs)

    async def request(self, method, url, **kwargs):
        return self._next(method, url, kwargs)


def make_sender(session):
    s = UserTokenSender(
        db=None,
        ratelimit=None,
        action_type=None,
        session_provider=lambda: None,
        logger=types.SimpleNamespace(
            debug=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
    )

    async def _fake_tls(token, *, use_proxy=False):
        return session

    s._tls_session = _fake_tls
    return s


URL = f"{token_sender.DISCORD_API_BASE}/channels/1/messages"


@pytest.mark.asyncio
async def test_message_post_is_recorded_with_redacted_auth(dev):
    session = Session([(200, {"id": "5"})])
    s = make_sender(session)

    assert (await s._send_with_token(TOKEN, 1, "hello", []))[0] == SEND_OK

    text = read_text(dev)
    assert TOKEN not in text
    events = read_events(dev)
    assert [e["type"] for e in events] == [
        "capture_start",
        "request",
        "request_headers",
        "request_body",
        "response",
    ]
    req, hdrs, body, resp = events[1:]
    assert req["method"] == "POST" and req["url"] == URL
    assert req["resourceType"] == "curl_cffi"
    rid = req["requestId"]
    assert hdrs["requestId"] == body["requestId"] == resp["requestId"] == rid
    # The header dict actually handed to curl_cffi, after extra_headers and referer.
    _, _, kwargs = session.calls[0]
    sent = kwargs["headers"]
    assert set(hdrs["headers"]) == {k for k, v in sent.items() if v is not None}
    assert hdrs["headers"]["Authorization"] == f"<redacted len={len(TOKEN)}>"
    assert hdrs["headers"]["X-Context-Properties"] == sent["X-Context-Properties"]
    assert hdrs["headers"]["Referer"] == sent["Referer"]
    assert hdrs["superProperties"]["browser"] == "Chrome"
    assert body["json"] == kwargs["json"]
    assert body["json"]["content"] == "hello"
    assert resp["status"] == 200


@pytest.mark.asyncio
async def test_dev_off_never_asks_for_a_recorder(prod, monkeypatch):
    called = []
    monkeypatch.setattr(tc, "recorder_for", lambda token: called.append(token))
    session = Session([(200, {"id": "5"})])
    s = make_sender(session)

    assert (await s._send_with_token(TOKEN, 1, "hello", []))[0] == SEND_OK
    await s._type_for(1, TOKEN, 0)

    assert called == []
    assert not (prod / tc.CAPTURE_DIR).exists()
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_429_then_200_is_two_requests(dev, no_wait):
    s = make_sender(Session([(429, {"retry_after": 0.01}), (200, {"id": "5"})]))

    assert (await s._send_with_token(TOKEN, 1, "hello", []))[0] == SEND_OK

    events = read_events(dev)
    reqs = of_type(events, "request")
    assert len(reqs) == 2
    assert reqs[0]["requestId"] != reqs[1]["requestId"]
    assert len(of_type(events, "request_body")) == 2
    assert [(r["requestId"], r["status"]) for r in of_type(events, "response")] == [
        (reqs[0]["requestId"], 429),
        (reqs[1]["requestId"], 200),
    ]


@pytest.mark.asyncio
async def test_network_error_is_request_failed(dev, no_wait):
    s = make_sender(Session([CurlRequestException(f"boom via {TOKEN}"), (200, {"id": "5"})]))

    assert (await s._send_with_token(TOKEN, 1, "hello", []))[0] == SEND_OK

    assert TOKEN not in read_text(dev)
    events = read_events(dev)
    reqs = of_type(events, "request")
    (failed,) = of_type(events, "request_failed")
    assert failed["requestId"] == reqs[0]["requestId"]
    assert "boom" in failed["errorText"]
    (resp,) = of_type(events, "response")
    assert resp["requestId"] == reqs[1]["requestId"] and resp["status"] == 200


@pytest.mark.asyncio
async def test_typing_post_is_recorded(dev):
    s = make_sender(Session([(204, None)]))

    await s._type_for(7, TOKEN, 0)

    events = read_events(dev)
    (req,) = of_type(events, "request")
    assert req["method"] == "POST"
    assert req["url"] == f"{token_sender.DISCORD_API_BASE}/channels/7/typing"
    assert req["headers"]["Authorization"] == f"<redacted len={len(TOKEN)}>"
    (body,) = of_type(events, "request_body")
    assert body["unavailable"] is True
    (resp,) = of_type(events, "response")
    assert resp["status"] == 204


@pytest.mark.asyncio
async def test_multipart_records_field_names_not_bytes(dev):
    session = Session([(200, {"id": "5"})])
    s = make_sender(session)
    files = [("a.png", b"PNG-BYTES-NOT-RECORDED", "image/png")]

    status, _ = await s._request_with_token(
        TOKEN, URL, mime_factory=lambda: s._build_multipart("hi", files)
    )

    assert status == SEND_OK
    assert "multipart" in session.calls[0][2]
    assert "PNG-BYTES-NOT-RECORDED" not in read_text(dev)
    (body,) = of_type(read_events(dev), "request_body")
    assert body["json"]["multipart"] == ["payload_json", "files[0]"]
    assert body["json"]["payload_json"]["content"] == "hi"
    assert body["json"]["payload_json"]["attachments"] == [{"id": 0, "filename": "a.png"}]


@pytest.mark.asyncio
async def test_patch_and_delete_are_recorded(dev):
    s = make_sender(Session([(200, {}), (204, None)]))

    await s._request_with_token(TOKEN, f"{URL}/9", json_body={"content": "x"}, method="PATCH")
    await s._request_with_token(TOKEN, f"{URL}/9", method="DELETE")

    events = read_events(dev)
    assert [r["method"] for r in of_type(events, "request")] == ["PATCH", "DELETE"]
    (body,) = of_type(events, "request_body")
    assert body["json"] == {"content": "x"}
    assert [r["status"] for r in of_type(events, "response")] == [200, 204]


@pytest.mark.asyncio
async def test_a_recorder_failure_never_reaches_the_send(dev, monkeypatch):
    class Broken:
        def request(self, *a, **k):
            raise RuntimeError("recorder broke")

        response = failed = request

    monkeypatch.setattr(tc, "recorder_for", lambda token: Broken())
    s = make_sender(Session([(200, {"id": "5"})]))

    assert (await s._send_with_token(TOKEN, 1, "hello", []))[0] == SEND_OK


@pytest.mark.asyncio
async def test_sender_only_token_gets_a_recorder_and_reap_stops_it(dev):
    s = make_sender(Session([(200, {"id": "5"})]))
    await s._send_with_token(TOKEN, 1, "hello", [])
    rec = tc.recorder_for(TOKEN)
    assert rec is not None and not rec.stopped

    # Recent records (a live gateway) keep the capture open.
    s._token_last_seen[TOKEN] = time.monotonic() - 10
    await s.reap_idle_state(idle_ttl=5)
    assert not rec.stopped

    rec.last -= 10_000
    s._token_last_seen[TOKEN] = time.monotonic() - 10
    await s.reap_idle_state(idle_ttl=5)
    assert rec.stopped
    (stop,) = of_type(read_events(dev), "capture_stop")
    assert stop["reason"] == "sender idle"
    assert stop["counts"]["requests"] == 1 and stop["counts"]["bodies"] == 1
