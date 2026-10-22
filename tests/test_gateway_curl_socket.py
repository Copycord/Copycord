"""The gateway socket over curl_cffi: open_websocket's dial (Chrome impersonation, the
upgrade headers, the proxy, the message size), and CurlWebSocket's thin interface the
receive loop uses (send, recv, async for, close code). A fake curl_cffi session and
socket stand in for Discord; nothing connects."""
import asyncio

import pytest
from curl_cffi.const import CurlWsFlag
from curl_cffi.requests.websockets import WebSocketClosed

from copycord.common import selfbot_headers
from copycord.selfclient import gateway as g

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

    async def recv(self):
        item = await self.inbox.get()
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
    """curl_cffi's AsyncSession: records each ws_connect."""

    def __init__(self):
        self.connects = []
        self.ws = FakeCurlWS()

    async def ws_connect(self, url, **kwargs):
        self.connects.append((url, kwargs))
        return self.ws


# open_websocket


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proxy, dialed",
    [
        (None, None),
        # The old tunnel spoke plain CONNECT to https:// proxies; to curl that is http://.
        ("https://user:pw@proxy.example:8443", "http://user:pw@proxy.example:8443"),
        # socks5 resolved the host at the proxy (python-socks); curl calls that socks5h.
        ("socks5://user:pw@proxy.example:1080", "socks5h://user:pw@proxy.example:1080"),
    ],
)
async def test_dial_impersonates_rest_target_with_only_our_upgrade_headers(proxy, dialed):
    sess = FakeSession()
    ws = await g.open_websocket(URL, HEADERS, proxy, session=sess)

    assert isinstance(ws, g.CurlWebSocket)
    [(url, kwargs)] = sess.connects
    assert url == URL
    # The TLS target is REST's.
    assert kwargs["impersonate"] == selfbot_headers.TLS_IMPERSONATE
    # Exactly the headers Copycord set on the upgrade before, nothing of curl's own.
    assert kwargs["headers"] == HEADERS
    assert kwargs["default_headers"] is False
    assert kwargs["accept_encoding"] is None
    # Larger than curl_cffi's 4 MiB default and than a 9.5 MB READY.
    assert kwargs["max_message_size"] == g.MAX_FRAME_SIZE >= 15 * 1024 * 1024
    assert kwargs["proxy"] == dialed


# CurlWebSocket


@pytest.mark.asyncio
async def test_text_and_binary_frames_both_ways():
    fake = FakeCurlWS()
    ws = g.CurlWebSocket(fake)
    await ws.send('{"op": 1}')
    await ws.send(b"\x00\x01")
    assert fake.sent == [('{"op": 1}', int(CurlWsFlag.TEXT)), (b"\x00\x01", int(CurlWsFlag.BINARY))]
    fake.feed(b"\x78\x9c")
    fake.feed(b'{"op": 10}', CurlWsFlag.TEXT)
    assert await ws.recv() == b"\x78\x9c"
    assert await ws.recv() == '{"op": 10}'


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
    # The client's own close passes its code through; a send after it is a closed socket.
    await ws.close(code=g.RESUME_CLOSE_CODE)
    assert fake.closed_with == (g.RESUME_CLOSE_CODE, "")
    with pytest.raises(g.SocketClosed):
        await ws.send("{}")
