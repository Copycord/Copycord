"""Tests for the supervisor. Fake roles only; network is limited to loopback control sockets."""
import asyncio
import json
import logging
import os
import threading

import pytest

from copycord import supervisor as sup_mod
from copycord.__main__ import main, parse_args
from copycord.supervisor import (
    Backoff,
    RoleSupervisor,
    Supervisor,
    ThreadedRole,
    build_supervisor,
)

FAST = Backoff(initial=0.01, maximum=0.04, reset_after=60.0)


class FakeRole:
    """Role coroutine factory that records starts/stops and can crash on demand."""

    def __init__(self, name, log=None, crashes=0, exit_after=None):
        self.name = name
        self.log = log if log is not None else []
        self.crashes_left = crashes
        self.exit_after = exit_after
        self.starts = 0

    async def __call__(self):
        self.starts += 1
        if self.crashes_left > 0:
            self.crashes_left -= 1
            raise RuntimeError(f"{self.name} boom")
        if self.exit_after is not None:
            await asyncio.sleep(self.exit_after)
            return
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.log.append(self.name)
            raise


async def _wait_until(pred, timeout=2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


def _fakes(log=None, **crashes):
    return {
        name: FakeRole(name, log, crashes=crashes.get(name, 0))
        for name in ("admin", "server", "client")
    }


def _loopback(fakes):
    return build_supervisor(
        fakes,
        control_host="127.0.0.1",
        control_ports={"server": 0, "client": 0},
        backoff=FAST,
    )


async def _cmd(port, payload):
    import websockets

    async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
        await ws.send(payload if isinstance(payload, str) else json.dumps(payload))
        return json.loads(await ws.recv())


# --------------------------------------------------------------------------
# Restart with backoff
# --------------------------------------------------------------------------


def test_crash_restarts_only_that_role_with_backoff():
    async def main():
        fakes = _fakes(client=3)
        sup = _loopback(fakes)
        await sup.start()
        try:
            sup.roles["server"].start()
            sup.roles["client"].start()
            await _wait_until(lambda: fakes["client"].starts == 4)
            await asyncio.sleep(0.02)

            client = sup.roles["client"]
            assert client.crashes == 3
            assert client.restart_delays == [0.01, 0.02, 0.04]
            assert client.is_running()

            for other in ("admin", "server"):
                assert fakes[other].starts == 1
                assert sup.roles[other].crashes == 0
                assert sup.roles[other].is_running()
        finally:
            await sup.shutdown()

    asyncio.run(main())


def test_backoff_is_capped():
    delays = []

    async def fake_sleep(d):
        delays.append(d)
        await asyncio.sleep(0)

    async def main():
        role = FakeRole("client", crashes=6)
        rs = RoleSupervisor(
            "client",
            role,
            backoff=Backoff(initial=1, maximum=8, reset_after=60),
            sleep=fake_sleep,
        )
        rs.start()
        await _wait_until(lambda: role.starts == 7)
        await rs.stop()

    asyncio.run(main())
    assert delays == [1, 2, 4, 8, 8, 8]


def test_backoff_resets_after_a_healthy_run():
    delays = []

    async def fake_sleep(d):
        delays.append(d)
        await asyncio.sleep(0)

    async def main():
        state = {"n": 0}

        async def flaky():
            state["n"] += 1
            if state["n"] == 3:
                # Long enough to count as healthy for reset_after=0.05.
                await asyncio.sleep(0.08)
            if state["n"] <= 4:
                raise RuntimeError("boom")
            await asyncio.Event().wait()

        rs = RoleSupervisor(
            "server",
            flaky,
            backoff=Backoff(initial=1, maximum=60, reset_after=0.05),
            sleep=fake_sleep,
        )
        rs.start()
        await _wait_until(lambda: state["n"] == 5)
        await rs.stop()

    asyncio.run(main())
    assert delays == [1, 2, 1, 2]


def test_exit_restarts_admin_but_not_server():
    async def main():
        admin = FakeRole("admin", exit_after=0)
        server = FakeRole("server", exit_after=0)
        ra = RoleSupervisor("admin", admin, backoff=FAST, restart_on_exit=True)
        rs = RoleSupervisor("server", server, backoff=FAST)
        ra.start()
        rs.start()
        await _wait_until(lambda: admin.starts >= 3)
        await _wait_until(lambda: not rs.is_running())
        assert server.starts == 1
        assert ra.is_running()
        await ra.stop()

    asyncio.run(main())


def test_system_exit_in_role_is_treated_as_crash():
    async def main():
        state = {"n": 0}

        async def exits():
            state["n"] += 1
            if state["n"] == 1:
                raise SystemExit(1)
            await asyncio.Event().wait()

        rs = RoleSupervisor("admin", exits, backoff=FAST, restart_on_exit=True)
        rs.start()
        await _wait_until(lambda: state["n"] == 2)
        assert rs.crashes == 1
        assert rs.is_running()
        await rs.stop()

    asyncio.run(main())


# --------------------------------------------------------------------------
# Shutdown
# --------------------------------------------------------------------------


def test_shutdown_order_client_server_admin():
    log = []

    async def main():
        fakes = _fakes(log)
        sup = _loopback(fakes)
        stop = asyncio.Event()

        async def drive():
            await _wait_until(lambda: fakes["admin"].starts == 1)
            ports = {c.role.name: c.port for c in sup.controls}
            assert (await _cmd(ports["server"], {"cmd": "start"}))["status"] == "started"
            assert (await _cmd(ports["client"], {"cmd": "start"}))["status"] == "started"
            await _wait_until(
                lambda: fakes["server"].starts == 1 and fakes["client"].starts == 1
            )
            stop.set()

        await asyncio.gather(sup.run(stop), drive())

        assert not any(r.is_running() for r in sup.roles.values())
        assert all(c._server is None for c in sup.controls)

    asyncio.run(main())
    assert log == ["client", "server", "admin"]


def test_shutdown_on_cancellation_keeps_order():
    """Windows path: Ctrl+C cancels the main task instead of a signal handler."""
    log = []

    async def main():
        fakes = _fakes(log)
        sup = _loopback(fakes)
        stop = asyncio.Event()
        task = asyncio.ensure_future(sup.run(stop))
        await _wait_until(lambda: fakes["admin"].starts == 1)
        sup.roles["server"].start()
        sup.roles["client"].start()
        await _wait_until(
            lambda: fakes["server"].starts == 1 and fakes["client"].starts == 1
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    assert log == ["client", "server", "admin"]


# --------------------------------------------------------------------------
# Control protocol
# --------------------------------------------------------------------------


def test_control_protocol_start_stop_status():
    async def main():
        fakes = _fakes()
        sup = _loopback(fakes)
        await sup.start()
        try:
            port = next(c.port for c in sup.controls if c.role.name == "server")
            assert port != 0

            st = await _cmd(port, {"cmd": "status"})
            assert st == {
                "ok": True,
                "running": False,
                "pid": None,
                "started_at": None,
                "uptime_sec": None,
            }

            r = await _cmd(port, {"cmd": "start"})
            assert r == {"ok": True, "status": "started", "pid": os.getpid()}
            await _wait_until(lambda: fakes["server"].starts == 1)

            st = await _cmd(port, {"cmd": "status"})
            assert st["ok"] is True and st["running"] is True
            assert st["pid"] == os.getpid()
            assert st["started_at"] and st["uptime_sec"] >= 0

            r = await _cmd(port, {"cmd": "start"})
            assert r["status"] == "already-running"
            assert fakes["server"].starts == 1

            assert fakes["client"].starts == 0

            r = await _cmd(port, {"cmd": "stop"})
            assert r == {"ok": True, "status": "stopped", "code": None}
            assert not sup.roles["server"].is_running()

            r = await _cmd(port, {"cmd": "stop"})
            assert r == {"ok": True, "status": "already-stopped"}

            assert (await _cmd(port, {"cmd": "nope"})) == {
                "ok": False,
                "error": "unknown-cmd",
            }
            assert (await _cmd(port, "not json")) == {
                "ok": False,
                "error": "unknown-cmd",
            }
            assert sup.roles["admin"].is_running()
        finally:
            await sup.shutdown()

    asyncio.run(main())


def test_control_status_while_crash_looping_reports_running():
    async def main():
        fakes = _fakes(client=2)
        sup = _loopback(fakes)
        await sup.start()
        try:
            port = next(c.port for c in sup.controls if c.role.name == "client")
            await _cmd(port, {"cmd": "start"})
            await _wait_until(lambda: fakes["client"].starts == 3)
            st = await _cmd(port, {"cmd": "status"})
            assert st["running"] is True
        finally:
            await sup.shutdown()

    asyncio.run(main())


# --------------------------------------------------------------------------
# Single mode
# --------------------------------------------------------------------------


def test_default_layout(monkeypatch):
    monkeypatch.delenv("CONTROL_PORT_SERVER", raising=False)
    monkeypatch.delenv("CONTROL_PORT_CLIENT", raising=False)
    sup = build_supervisor(_fakes())
    assert list(sup.roles) == ["admin", "server", "client"]
    assert sup.autostart == ["admin"]
    assert {(c.role.name, c.host, c.port) for c in sup.controls} == {
        ("server", "127.0.0.1", 9101),
        ("client", "127.0.0.1", 9102),
    }


def test_parse_args_has_no_role_option():
    assert parse_args(["run"].copy()).command == "run"
    with pytest.raises(SystemExit):
        parse_args(["run", "--role", "client"])


def test_role_env_guard_exits_before_starting(monkeypatch, caplog):
    started = []
    monkeypatch.setattr(
        sup_mod, "run", lambda *a, **k: started.append(1), raising=True
    )
    monkeypatch.setenv("ROLE", "server")
    # Guard against an earlier test leaving propagation off on "copycord",
    # which would hide the record from caplog.
    monkeypatch.setattr(logging.getLogger("copycord"), "propagate", True)
    with caplog.at_level("ERROR", logger="copycord"):
        assert main(["run"]) != 0
    assert started == []
    assert "v4 removed the ROLE option" in caplog.text
    assert "compose file" in caplog.text


def test_role_env_guard_trips_on_empty_value(monkeypatch):
    monkeypatch.setenv("ROLE", "")
    assert main(["run"]) == 1


# --------------------------------------------------------------------------
# ThreadedRole (how real server/client run in-process)
# --------------------------------------------------------------------------


class _BlockingRole:
    """Mimics ServerReceiver/ClientListener: run() drives its own loop."""

    instances = []

    def __init__(self, crash=False):
        self.crash = crash
        self.shutdown_called = False
        self.thread = None
        _BlockingRole.instances.append(self)

    def run(self):
        loop = asyncio.get_event_loop()
        self._stop = asyncio.Event()
        self.thread = threading.current_thread()
        if self.crash:
            raise RuntimeError("thread boom")
        loop.run_until_complete(self._stop.wait())

    async def _shutdown(self):
        self.shutdown_called = True
        if getattr(self, "_stop", None) is not None:
            self._stop.set()


def test_threaded_role_stop_calls_shutdown_and_joins():
    _BlockingRole.instances.clear()

    async def main():
        rs = RoleSupervisor("server", ThreadedRole("server", _BlockingRole), backoff=FAST)
        rs.start()
        await _wait_until(
            lambda: _BlockingRole.instances and _BlockingRole.instances[0].thread
        )
        inst = _BlockingRole.instances[0]
        assert inst.thread is not threading.main_thread()
        r = await rs.stop()
        assert r["status"] == "stopped"
        assert inst.shutdown_called
        await _wait_until(lambda: not inst.thread.is_alive())

    asyncio.run(main())


def test_threaded_role_crash_is_restarted():
    _BlockingRole.instances.clear()
    built = {"n": 0}

    def build():
        built["n"] += 1
        return _BlockingRole(crash=built["n"] == 1)

    async def main():
        rs = RoleSupervisor("client", ThreadedRole("client", build), backoff=FAST)
        rs.start()
        await _wait_until(
            lambda: len(_BlockingRole.instances) == 2
            and _BlockingRole.instances[1].thread
        )
        assert rs.crashes == 1
        assert _BlockingRole.instances[0].shutdown_called
        assert rs.is_running()
        await rs.stop()

    asyncio.run(main())


# --------------------------------------------------------------------------
# Busy ports at startup
# --------------------------------------------------------------------------


def _held_port():
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen()
    return s, s.getsockname()[1]


def test_control_port_busy_at_start_is_retried(monkeypatch):
    monkeypatch.setattr(sup_mod, "PORT_RETRY", 0.05)

    async def main():
        holder, port = _held_port()
        ctl = sup_mod.ControlEndpoint(RoleSupervisor("server", FakeRole("server")),
                                      "127.0.0.1", port, port_wait=5)
        starting = asyncio.ensure_future(ctl.start())
        await asyncio.sleep(0.3)
        assert not starting.done()
        holder.close()
        await asyncio.wait_for(starting, 5)
        try:
            assert ctl.port == port
            assert (await _cmd(port, {"cmd": "status"}))["ok"] is True
        finally:
            await ctl.close()

    asyncio.run(main())


def test_control_port_still_busy_raises_port_busy(monkeypatch):
    monkeypatch.setattr(sup_mod, "PORT_RETRY", 0.05)

    async def main():
        holder, port = _held_port()
        try:
            ctl = sup_mod.ControlEndpoint(RoleSupervisor("server", FakeRole("server")),
                                          "127.0.0.1", port, port_wait=0.2)
            with pytest.raises(sup_mod.PortBusyError) as exc:
                await ctl.start()
            assert exc.value.port == port
        finally:
            holder.close()

    asyncio.run(main())


def test_admin_port_busy_stops_the_start_before_any_role(monkeypatch):
    monkeypatch.setattr(sup_mod, "PORT_RETRY", 0.05)

    async def main():
        holder, port = _held_port()
        fakes = _fakes()
        sup = _loopback(fakes)
        sup.ports, sup.port_wait = [("127.0.0.1", port)], 0.2
        try:
            with pytest.raises(sup_mod.PortBusyError) as exc:
                await sup.start()
            assert exc.value.port == port
            assert fakes["admin"].starts == 0
        finally:
            holder.close()
            await sup.shutdown()

    asyncio.run(main())


def test_admin_port_freed_while_waiting(monkeypatch):
    monkeypatch.setattr(sup_mod, "PORT_RETRY", 0.05)

    async def main():
        holder, port = _held_port()
        asyncio.get_running_loop().call_later(0.2, holder.close)
        await sup_mod.wait_for_port("127.0.0.1", port, timeout=5)

    asyncio.run(main())


def test_real_supervisor_waits_for_the_admin_port(monkeypatch):
    monkeypatch.setenv("ADMIN_PORT", "8181")
    monkeypatch.setattr(sup_mod, "admin_factory", lambda **k: FakeRole("admin"))
    monkeypatch.setattr(sup_mod, "server_factory", lambda: FakeRole("server"))
    monkeypatch.setattr(sup_mod, "client_factory", lambda: FakeRole("client"))
    monkeypatch.delenv("DASHBOARD_HOST", raising=False)
    sup = build_supervisor(inprocess_transport=False)
    assert sup.ports == [("127.0.0.1", 8181)]
    assert build_supervisor(_fakes()).ports == []
    monkeypatch.setenv("DASHBOARD_HOST", "0.0.0.0")
    assert build_supervisor(inprocess_transport=False).ports == [("0.0.0.0", 8181)]


@pytest.mark.parametrize("listen, agents", [
    (None, "127.0.0.1"),
    ("0.0.0.0", "127.0.0.1"),
    ("::", "127.0.0.1"),
    ("192.168.1.20", "192.168.1.20"),
])
def test_roles_reach_the_dashboard_over_loopback_unless_it_listens_on_one_address(
    monkeypatch, tmp_path, listen, agents
):
    # _set_runtime_env fills in many keys; keep them out of the other tests.
    monkeypatch.setattr(os, "environ", dict(os.environ))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("ADMIN_HOST", raising=False)
    if listen is None:
        monkeypatch.delenv("DASHBOARD_HOST", raising=False)
    else:
        monkeypatch.setenv("DASHBOARD_HOST", listen)
    sup_mod._set_runtime_env()
    assert os.environ["ADMIN_HOST"] == agents
