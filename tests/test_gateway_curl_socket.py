"""The gateway socket over curl_cffi: open_websocket's dial (Chrome impersonation, the
upgrade headers, the proxy, the message size, whose session), and CurlWebSocket's thin
interface the receive loop uses (send, recv, async for, close, close code). A fake
curl_cffi session and socket stand in for Discord; nothing connects."""
import asyncio
import json
import logging
import types
import zlib

import pytest
from curl_cffi.const import CurlWsFlag
from curl_cffi.requests.websockets import WebSocketClosed, WebSocketError

from copycord.common import selfbot_headers
from copycord.selfclient import client as client_mod
from copycord.selfclient import gateway as g
from copycord.selfclient.errors import LoginFailure

URL = g.gateway_url(zlib_stream=True)
HEADERS = {"User-Agent": "Mozilla/5.0 test", "Origin": "https://discord.com"}


class FakeCurlWS:
    """curl_cffi's AsyncWebSocket: recv() gives (payload, flags), send(payload, flags),
    close(code, message), and the close code Discord sent."""

    def __init__(self):
        self.inbox = asyncio.Queue()
        self.sent = []
        self.closed_with = None
        self.close_code = None
        self.close_reason = None

    def feed(self, payload, flags=CurlWsFlag.BINARY):
        self.inbox.put_nowait((payload, int(flags)))

    def server_close(self, code, reason=""):
        """Discord's close frame: curl_cffi notes the code, then recv() reports closed."""
        self.inbox.put_nowait(("close", code, reason))

    def fail(self, exc):
        self.inbox.put_nowait(exc)

    async def recv(self):
        item = await self.inbox.get()
        if isinstance(item, BaseException):
            raise item
        if item[0] == "close":
            self.close_code, self.close_reason = item[1], item[2]
            raise WebSocketClosed("Connection closed")
        return item

    async def send(self, payload, flags=CurlWsFlag.BINARY):
        if self.closed_with is not None:
            raise WebSocketClosed("WebSocket is closed")
        self.sent.append((payload, int(flags)))

    async def close(self, code=1000, message=b"", timeout=3.0):
        self.closed_with = (code, message)


class FakeSession:
    """curl_cffi's AsyncSession: records how it was made and each ws_connect."""

    fail = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.connects = []
        self.closed = False
        self.ws = FakeCurlWS()

    async def ws_connect(self, url, **kwargs):
        self.connects.append((url, kwargs))
        if self.fail is not None:
            raise self.fail
        return self.ws

    async def close(self):
        self.closed = True


def _zlib_frames(*payloads):
    z = zlib.compressobj()
    return [z.compress(json.dumps(p).encode()) + z.flush(zlib.Z_SYNC_FLUSH) for p in payloads]


# open_websocket


@pytest.mark.asyncio
async def test_dial_impersonates_rest_target_with_only_our_upgrade_headers():
    sess = FakeSession()
    ws = await g.open_websocket(URL, HEADERS, None, session=sess)

    assert isinstance(ws, g.CurlWebSocket)
    [(url, kwargs)] = sess.connects
    assert url == URL
    # The TLS target is REST's.
    assert kwargs["impersonate"] == selfbot_headers.TLS_IMPERSONATE
    assert kwargs["impersonate"] == g.fingerprint.TLS_IMPERSONATE
    # Exactly the headers Copycord set on the upgrade before, nothing of curl's own.
    assert kwargs["headers"] == HEADERS
    assert kwargs["default_headers"] is False
    assert kwargs["accept_encoding"] is None
    # Larger than curl_cffi's 4 MiB default and than a 9.5 MB READY.
    assert kwargs["max_message_size"] == g.MAX_FRAME_SIZE >= 15 * 1024 * 1024
    assert kwargs["timeout"] == g.OPEN_TIMEOUT
    assert kwargs["proxy"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proxy, dialed",
    [
        ("http://user:pw@proxy.example:8080", "http://user:pw@proxy.example:8080"),
        # The old tunnel spoke plain CONNECT to https:// proxies; to curl that is http://.
        ("https://user:pw@proxy.example:8443", "http://user:pw@proxy.example:8443"),
        # socks5 resolved the host at the proxy (python-socks); curl calls that socks5h.
        ("socks5://user:pw@proxy.example:1080", "socks5h://user:pw@proxy.example:1080"),
        ("socks5h://proxy.example:1080", "socks5h://proxy.example:1080"),
        ("socks4://proxy.example:1080", "socks4://proxy.example:1080"),
    ],
)
async def test_the_tokens_proxy_is_passed_to_curl(proxy, dialed):
    sess = FakeSession()
    await g.open_websocket(URL, HEADERS, proxy, session=sess)
    assert sess.connects[0][1]["proxy"] == dialed


@pytest.mark.asyncio
async def test_an_unsupported_proxy_scheme_never_dials():
    sess = FakeSession()
    with pytest.raises(g.ProxyTunnelError):
        await g.open_websocket(URL, HEADERS, "ftp://proxy.example:21", session=sess)
    assert sess.connects == []


@pytest.mark.asyncio
async def test_a_shared_session_stays_open_when_the_socket_closes():
    sess = FakeSession()
    ws = await g.open_websocket(URL, HEADERS, session=sess)
    await ws.close(code=g.RESUME_CLOSE_CODE)
    assert sess.ws.closed_with == (g.RESUME_CLOSE_CODE, "")
    assert sess.closed is False


@pytest.mark.asyncio
async def test_without_a_session_one_is_made_for_the_socket_and_closed_with_it(monkeypatch):
    made = []

    def make(**kwargs):
        made.append(FakeSession(**kwargs))
        return made[-1]

    monkeypatch.setattr(g, "_CurlAsyncSession", make)
    ws = await g.open_websocket(URL, HEADERS, "http://proxy.example:8080")
    [sess] = made
    assert sess.kwargs == {"impersonate": selfbot_headers.TLS_IMPERSONATE}
    assert sess.connects[0][1]["proxy"] == "http://proxy.example:8080"
    assert not sess.closed
    await ws.close()
    assert sess.closed and sess.ws.closed_with == (1000, "")


@pytest.mark.asyncio
async def test_a_failed_dial_closes_the_session_it_made(monkeypatch):
    made = []

    class Failing(FakeSession):
        fail = ConnectionError("proxy refused")

    def make(**kwargs):
        made.append(Failing(**kwargs))
        return made[-1]

    monkeypatch.setattr(g, "_CurlAsyncSession", make)
    with pytest.raises(ConnectionError):
        await g.open_websocket(URL, HEADERS)
    assert made[0].closed


# CurlWebSocket


@pytest.mark.asyncio
async def test_send_is_a_text_frame_and_bytes_a_binary_one():
    fake = FakeCurlWS()
    ws = g.CurlWebSocket(fake)
    await ws.send('{"op": 1}')
    await ws.send(b"\x00\x01")
    assert fake.sent == [('{"op": 1}', int(CurlWsFlag.TEXT)), (b"\x00\x01", int(CurlWsFlag.BINARY))]


@pytest.mark.asyncio
async def test_recv_gives_bytes_for_binary_and_str_for_text():
    fake = FakeCurlWS()
    ws = g.CurlWebSocket(fake)
    fake.feed(b"\x78\x9c")
    fake.feed(b'{"op": 10}', CurlWsFlag.TEXT)
    assert await ws.recv() == b"\x78\x9c"
    assert await ws.recv() == '{"op": 10}'


@pytest.mark.asyncio
async def test_a_recv_cut_by_a_timeout_keeps_its_frame_for_the_next():
    fake = FakeCurlWS()
    ws = g.CurlWebSocket(fake)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ws.recv(), timeout=0.05)
    fake.feed(b"first")
    fake.feed(b"second")
    assert await ws.recv() == b"first"
    assert await ws.recv() == b"second"


@pytest.mark.asyncio
async def test_async_for_ends_on_discords_close_and_keeps_its_code():
    fake = FakeCurlWS()
    ws = g.CurlWebSocket(fake)
    fake.feed(b"a")
    fake.feed(b"b")
    fake.server_close(4004, "Authentication failed.")
    assert [raw async for raw in ws] == [b"a", b"b"]
    assert ws.close_code == 4004
    assert ws.close_reason == "Authentication failed."


@pytest.mark.asyncio
async def test_recv_after_close_raises_socket_closed():
    fake = FakeCurlWS()
    ws = g.CurlWebSocket(fake)
    fake.server_close(4000)
    with pytest.raises(g.SocketClosed) as info:
        await ws.recv()
    assert info.value.code == 4000


@pytest.mark.asyncio
async def test_a_transport_error_reports_its_close_code_but_not_a_curl_errno():
    fake = FakeCurlWS()
    ws = g.CurlWebSocket(fake)
    fake.fail(WebSocketClosed("Connection closed unexpectedly by server (EOF)", 1006))
    with pytest.raises(g.SocketClosed):
        await ws.recv()
    assert ws.close_code == 1006

    other = g.CurlWebSocket(FakeCurlWS())
    other._ws.fail(WebSocketError("recv failure", 56))
    with pytest.raises(g.SocketClosed):
        await other.recv()
    assert other.close_code is None


@pytest.mark.asyncio
async def test_send_on_a_closed_socket_is_what_the_client_treats_as_closed(caplog):
    caplog.set_level(logging.WARNING, logger="selfclient.gateway")
    fake = FakeCurlWS()
    ws = g.CurlWebSocket(fake)
    await ws.close(code=4000)
    with pytest.raises(g.SocketClosed):
        await ws.send("{}")

    client = g.GatewayClient("tok", reconnect=False)
    client._ws = ws
    await client._send({"op": 3, "d": {}})
    await client._send_bypass(client._heartbeat_payload())
    assert "tried to send on a closed socket" in caplog.text
    assert "tried to send heartbeat on closed socket" in caplog.text


# The receive loop on the adapter


@pytest.mark.asyncio
async def test_gateway_client_reads_zlib_stream_and_the_close_code_through_the_adapter(
    monkeypatch,
):
    sess = FakeSession()
    opened = []

    async def connector(url, headers, proxy):
        ws = await g.open_websocket(url, headers, proxy, session=sess)
        opened.append(ws)
        return ws

    client = g.GatewayClient("tok", reconnect=False, connector=connector)
    hello, = _zlib_frames({"op": 10, "d": {"heartbeat_interval": 41250}})
    # Split over two frames, as Discord may.
    sess.ws.feed(hello[:5])
    sess.ws.feed(hello[5:])

    async def close_after_identify():
        while not any(json.loads(p)["op"] == 2 for p, _ in sess.ws.sent):
            await asyncio.sleep(0.01)
        sess.ws.server_close(4004, "Authentication failed.")

    closer = asyncio.ensure_future(close_after_identify())
    with pytest.raises(LoginFailure):
        await asyncio.wait_for(client.run(refresh_build=False), timeout=5)
    await closer

    ops = [json.loads(p)["op"] for p, _ in sess.ws.sent]
    assert ops[:2] == [2, 4]
    assert {flags for _, flags in sess.ws.sent} == {int(CurlWsFlag.TEXT)}
    assert sess.connects[0][0] == URL
    assert client.fatal_close == (4004, "Authentication failed.")
    # Closed by the client too, keeping 4000 as it does on every socket it ends.
    assert sess.ws.closed_with == (g.RESUME_CLOSE_CODE, "")
    assert not sess.closed


@pytest.mark.asyncio
async def test_the_main_client_dials_on_its_rest_session(monkeypatch):
    calls = []

    async def fake_open(url, headers, proxy=None, **kwargs):
        calls.append((url, headers, proxy, kwargs))
        return FakeCurlWS()

    monkeypatch.setattr(client_mod, "open_websocket", fake_open)
    rest_session = object()
    client = client_mod.Client(http=types.SimpleNamespace(session=rest_session, token="tok"))
    await client._tracked_connector(URL, HEADERS, "http://proxy.example:8080")
    [(url, headers, proxy, kwargs)] = calls
    assert (url, headers, proxy) == (URL, HEADERS, "http://proxy.example:8080")
    assert kwargs["session"] is rest_session
