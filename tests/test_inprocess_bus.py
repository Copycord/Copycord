"""
Tests for the in-process transport (src/copycord/common/inprocess.py) behind
WebsocketManager and AdminBus.

Each role gets its own event loop on its own thread, as ThreadedRole runs
server and client, so every delivery here crosses a thread boundary. No
sockets are opened except where split mode is checked, and those calls are
faked.
"""
import asyncio
import json
import threading
import time

import pytest

from copycord.common import inprocess
from copycord.common import websockets as ws_mod
from copycord.common.websockets import AdminBus, WebsocketManager
from copycord.supervisor import build_supervisor


class LoopThread:
    """An event loop running forever on a daemon thread."""

    def __init__(self, name):
        self.name = name
        self.loop = asyncio.new_event_loop()
        # The loop only holds weak references to its tasks. A task parked on
        # a future (start_server, subscribe) that nobody else holds is a
        # garbage cycle, and a gc run destroys it mid-test, unregistering its
        # endpoint. Keep every spawned future, as the roles keep _ws_task.
        self.spawned = []
        self.thread = threading.Thread(target=self._run, name=name, daemon=True)
        self.thread.start()

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def spawn(self, coro):
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        self.spawned.append(fut)
        return fut

    def run(self, coro, timeout=5.0):
        return self.spawn(coro).result(timeout)

    def close(self):
        async def _cancel_all():
            me = asyncio.current_task()
            tasks = [t for t in asyncio.all_tasks() if t is not me]
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            self.run(_cancel_all())
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(5)
            self.loop.close()


def _wait_for(pred, timeout=3.0):
    deadline = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.005)


@pytest.fixture
def reg():
    inprocess.disable()
    r = inprocess.enable()
    yield r
    inprocess.disable()


@pytest.fixture
def loops():
    made = []

    def make(name):
        lt = LoopThread(name)
        made.append(lt)
        return lt

    yield make
    for lt in made:
        lt.close()


PORT = 18765
URL = f"ws://127.0.0.1:{PORT}"


def _serve(lt, handler, port=PORT):
    mgr = WebsocketManager(send_url="ws://unused", listen_host="0.0.0.0", listen_port=port)
    lt.spawn(mgr.start_server(handler))
    _wait_for(lambda: inprocess.active().has(f"ws://127.0.0.1:{port}"))
    return mgr


# --------------------------------------------------------------------------
# WebsocketManager
# --------------------------------------------------------------------------


def test_request_crosses_loops_on_different_threads(reg, loops):
    srv, cli = loops("srv-loop"), loops("cli-loop")
    seen = {}

    async def handler(msg):
        seen["thread"] = threading.current_thread().name
        seen["loop"] = asyncio.get_running_loop()
        seen["msg"] = msg
        return {"type": "pong", "data": msg["data"]}

    _serve(srv, handler)

    async def ask():
        seen["caller"] = threading.current_thread().name
        mgr = WebsocketManager(send_url=URL)
        return await mgr.request(
            {"type": "ping", "data": {"n": 1}, "rid": "r-1"}, timeout=2.0, max_attempts=1
        )

    reply = cli.run(ask())
    assert reply == {"type": "pong", "data": {"n": 1}, "rid": "r-1"}
    assert seen["thread"] == "srv-loop"
    assert seen["caller"] == "cli-loop"
    assert seen["loop"] is srv.loop
    # Same message shape the socket server hands its handler.
    assert seen["msg"] == {"type": "ping", "data": {"n": 1}, "rid": "r-1"}


def test_request_none_reply_becomes_ok_and_gets_rid(reg, loops):
    srv, cli = loops("srv-loop"), loops("cli-loop")

    async def handler(msg):
        return None

    _serve(srv, handler)
    reply = cli.run(
        WebsocketManager(send_url=URL).request({"type": "x"}, timeout=2.0, max_attempts=1)
    )
    assert reply["ok"] is True
    assert reply["rid"]


def test_request_handler_failure_reply(reg, loops):
    srv, cli = loops("srv-loop"), loops("cli-loop")

    async def handler(msg):
        raise ValueError("boom")

    _serve(srv, handler)
    reply = cli.run(
        WebsocketManager(send_url=URL).request(
            {"type": "x", "rid": "r-2"}, timeout=2.0, max_attempts=1
        )
    )
    assert reply == {"ok": False, "error": "handler-failed", "rid": "r-2"}


def test_request_timeout_returns_none(reg, loops):
    srv, cli = loops("srv-loop"), loops("cli-loop")
    release = threading.Event()

    async def handler(msg):
        while not release.is_set():
            await asyncio.sleep(0.01)
        return {"late": True}

    _serve(srv, handler)
    try:
        reply = cli.run(
            WebsocketManager(send_url=URL).request(
                {"type": "slow"}, timeout=0.05, max_attempts=1
            )
        )
    finally:
        release.set()
    assert reply is None


def test_request_without_endpoint_returns_none(reg, loops):
    cli = loops("cli-loop")
    reply = cli.run(
        WebsocketManager(send_url="ws://127.0.0.1:18999").request(
            {"type": "x"}, timeout=0.5, max_attempts=1
        )
    )
    assert reply is None


def test_send_is_fire_and_forget_across_threads(reg, loops):
    srv, cli = loops("srv-loop"), loops("cli-loop")
    got = []
    done = threading.Event()

    async def handler(msg):
        got.append((threading.current_thread().name, msg))
        done.set()

    _serve(srv, handler)
    payload = {"type": "message", "data": {"id": 7}}
    cli.run(WebsocketManager(send_url=URL).send(payload, max_attempts=1))
    assert done.wait(3)
    thread, msg = got[0]
    assert thread == "srv-loop"
    assert msg["type"] == "message" and msg["data"] == {"id": 7} and msg["rid"]
    # The sender's dict is not shared with the receiving thread.
    assert "rid" not in payload


def test_send_from_both_sides(reg, loops):
    """Server and client each listen and each send to the other."""
    a, b = loops("server-loop"), loops("client-loop")
    inbox = {"server-loop": [], "client-loop": []}
    done = threading.Event()

    def make(name):
        async def handler(msg):
            inbox[threading.current_thread().name].append(msg["type"])
            if inbox["server-loop"] and inbox["client-loop"]:
                done.set()

        return handler

    _serve(a, make("server-loop"), port=18801)
    _serve(b, make("client-loop"), port=18802)
    a.run(WebsocketManager(send_url="ws://client:18802").send({"type": "to-client"}))
    b.run(WebsocketManager(send_url="ws://server:18801").send({"type": "to-server"}))
    assert done.wait(3)
    assert inbox == {"server-loop": ["to-server"], "client-loop": ["to-client"]}


def test_stopped_server_unregisters(reg, loops):
    srv = loops("srv-loop")

    async def handler(msg):
        return {}

    mgr = WebsocketManager(send_url="ws://unused", listen_port=PORT)
    fut = srv.spawn(mgr.start_server(handler))
    _wait_for(lambda: reg.has(URL))
    srv.loop.call_soon_threadsafe(fut.cancel)
    _wait_for(lambda: not reg.has(URL))


def test_closed_loop_endpoint_is_dropped(reg):
    lt = LoopThread("gone-loop")

    async def handler(msg):
        return {}

    async def register():
        reg.register(("port", PORT), handler)

    lt.run(register())
    lt.loop.call_soon_threadsafe(lt.loop.stop)
    lt.thread.join(5)
    lt.loop.close()
    assert not reg.has(URL)
    with pytest.raises(inprocess.EndpointUnavailable):
        reg.dispatch(URL, {"type": "x"})


# --------------------------------------------------------------------------
# AdminBus
# --------------------------------------------------------------------------


class FakeHub:
    """The parts of copycord.admin.app.BusHub the in-process bus touches."""

    def __init__(self):
        self.sinks = set()
        self.published = []

    async def add_ui(self, ws):
        self.sinks.add(ws)
        await ws.send_text(json.dumps({"kind": "status", "role": "server", "payload": {"replay": 1}}))

    async def remove_ui(self, ws):
        self.sinks.discard(ws)

    async def on_bus(self, ev):
        self.published.append((threading.current_thread().name, ev))
        text = json.dumps({"kind": ev["kind"], "role": ev["role"], "payload": ev["payload"]})
        for s in list(self.sinks):
            await s.send_text(text)


ADMIN_URL = "ws://127.0.0.1:8080/bus"


def _admin(lt):
    hub = FakeHub()

    async def register():
        reg = inprocess.active()
        reg.register(inprocess.BUS_PATH, hub.on_bus)
        reg.register_feed(hub.add_ui, hub.remove_ui)

    lt.run(register())
    return hub


def test_publish_reaches_admin_loop(reg, loops):
    admin, cli = loops("admin-loop"), loops("cli-loop")
    hub = _admin(admin)
    cli.run(AdminBus("client", admin_ws_url=ADMIN_URL).status(running=True))
    _wait_for(lambda: hub.published)
    thread, ev = hub.published[0]
    assert thread == "admin-loop"
    assert ev["kind"] == "status" and ev["role"] == "client"
    assert ev["payload"] == {"running": True}


def test_subscribe_receives_published_events_across_three_threads(reg, loops):
    admin, srv, cli = loops("admin-loop"), loops("srv-loop"), loops("cli-loop")
    hub = _admin(admin)
    events = []

    async def on_event(ev):
        events.append((threading.current_thread().name, ev))

    sub_bus = AdminBus("server", admin_ws_url=ADMIN_URL)
    srv.spawn(sub_bus.subscribe(ADMIN_URL, on_event))
    _wait_for(lambda: len(hub.sinks) == 1)
    # Replay on attach, as /ws/out does.
    _wait_for(lambda: len(events) == 1)

    cli.run(AdminBus("client", admin_ws_url=ADMIN_URL).publish("log", {"text": "hi"}))
    _wait_for(lambda: len(events) == 2)
    thread, ev = events[1]
    assert thread == "srv-loop"
    assert ev == {"kind": "log", "role": "client", "payload": {"text": "hi"}}


def test_subscribe_detaches_on_cancel(reg, loops):
    admin, srv = loops("admin-loop"), loops("srv-loop")
    hub = _admin(admin)

    async def on_event(ev):
        pass

    fut = srv.spawn(AdminBus("server").subscribe(ADMIN_URL, on_event))
    _wait_for(lambda: len(hub.sinks) == 1)
    srv.loop.call_soon_threadsafe(fut.cancel)
    _wait_for(lambda: not hub.sinks)


def test_admin_request_falls_back_when_unregistered(reg, loops):
    cli = loops("cli-loop")
    assert cli.run(inprocess.admin_request(URL, {"type": "x"}, 0.5)) is None


def test_admin_request_hits_registered_agent(reg, loops):
    srv, admin = loops("srv-loop"), loops("admin-loop")

    async def handler(msg):
        return {"ok": True, "type": msg["type"]}

    _serve(srv, handler)
    reply = admin.run(inprocess.admin_request(URL, {"type": "backfills_status_query"}, 1.0))
    assert reply["ok"] is True and reply["type"] == "backfills_status_query"


# --------------------------------------------------------------------------
# Split mode and selection
# --------------------------------------------------------------------------


@pytest.fixture
def split():
    inprocess.disable()
    yield
    inprocess.disable()


def test_split_mode_send_and_request_use_real_sockets(split, monkeypatch):
    dialled = []

    async def fake_connect(url, **kw):
        dialled.append(url)
        raise OSError("refused")

    monkeypatch.setattr(ws_mod.websockets, "connect", fake_connect)

    async def main():
        mgr = WebsocketManager(send_url=URL)
        await mgr.send({"type": "x"}, max_attempts=1)
        assert await mgr.request({"type": "y"}, timeout=0.2, max_attempts=1) is None

    asyncio.run(main())
    assert dialled == [URL, URL]


def test_split_mode_start_server_binds_socket(split, monkeypatch):
    served = []

    class FakeServer:
        def close(self):
            served.append("closed")

        async def wait_closed(self):
            pass

    async def fake_serve(handler, host, port, **kw):
        served.append((host, port))
        return FakeServer()

    monkeypatch.setattr(ws_mod.websockets, "serve", fake_serve)

    async def main():
        mgr = WebsocketManager(send_url="ws://unused", listen_host="0.0.0.0", listen_port=PORT)
        task = asyncio.ensure_future(mgr.start_server(lambda m: None))
        while not served:
            await asyncio.sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    assert served == [("0.0.0.0", PORT), "closed"]


async def _idle():
    await asyncio.Event().wait()


def _fake_factories():
    return {n: _idle for n in ("admin", "server", "client")}


def test_build_supervisor_fake_factories_default_off(split):
    build_supervisor(factories=_fake_factories(), control_ports={"server": 0, "client": 0})
    assert inprocess.active() is None


def test_build_supervisor_enables_inprocess_for_all_roles(split):
    build_supervisor(
        factories=_fake_factories(),
        control_ports={"server": 0, "client": 0},
        inprocess_transport=True,
    )
    assert inprocess.active() is not None
