"""Tests for the supervisor. Fake roles only; network is limited to loopback control sockets."""
import asyncio
import json
import logging
import os
import threading

import pytest

from copycord import supervisor as sup_mod
from copycord.__main__ import main
from copycord.supervisor import (
    Backoff,
    RoleSupervisor,
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


# Restart with backoff


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


# Shutdown


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


# Control protocol


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


# Single mode


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


# ThreadedRole (how real server/client run in-process)


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


# Stopping: from a stop request until the role's task has really ended


class _SlowStop:
    """A role whose cancel handler takes `delay` seconds, like closing a big token pool."""

    def __init__(self, delay):
        self.delay = delay
        self.started = 0
        self.exited = False

    async def __call__(self):
        self.started += 1
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(self.delay)
            self.exited = True
            raise


def _quiet_status(st):
    return {k: st[k] for k in ("running", "status", "stopping") if k in st}


def test_stop_reports_stopping_until_the_role_has_exited():
    async def main():
        role = _SlowStop(0.3)
        rs = RoleSupervisor("server", role, backoff=FAST, stop_timeout=5.0)
        rs.start()
        await _wait_until(lambda: role.started == 1)
        task = rs._task
        stop = asyncio.ensure_future(rs.handle({"cmd": "stop"}))
        await asyncio.sleep(0)
        seen = []
        while not stop.done():
            seen.append((role.exited, _quiet_status(rs.status())))
            await asyncio.sleep(0.02)
        assert len(seen) > 5
        # Never stopped while the role was still cancelling.
        assert all(
            st == {"running": True, "status": "stopping", "stopping": True}
            for exited, st in seen
            if not exited
        )
        assert role.exited
        assert await stop == {"ok": True, "status": "stopped", "code": None}
        assert rs.status()["running"] is False and "status" not in rs.status()
        assert task.done() and role.started == 1

    asyncio.run(main())


def test_start_while_stopping_is_refused():
    async def main():
        role = _SlowStop(0.3)
        rs = RoleSupervisor("server", role, backoff=FAST, stop_timeout=5.0)
        rs.start()
        await _wait_until(lambda: role.started == 1)
        task = rs._task
        stop = asyncio.ensure_future(rs.stop())
        await asyncio.sleep(0.05)
        r = await rs.handle({"cmd": "start"})
        assert r["ok"] is False and r["status"] == "stopping"
        assert "still stopping" in r["error"]
        assert rs._task is task
        await stop
        assert (await rs.handle({"cmd": "start"}))["status"] == "started"
        await _wait_until(lambda: role.started == 2)
        await rs.stop()

    asyncio.run(main())


def test_stop_past_its_timeout_keeps_reporting_stopping_until_the_task_ends(
    monkeypatch, caplog
):
    monkeypatch.setattr(logging.getLogger("copycord"), "propagate", True)

    async def main():
        role = _SlowStop(0.5)
        rs = RoleSupervisor("server", role, backoff=FAST, stop_timeout=0.1)
        rs.start()
        await _wait_until(lambda: role.started == 1)
        with caplog.at_level("WARNING", logger="copycord.supervisor"):
            r = await rs.stop()
        assert r == {"ok": True, "status": "stopping"}
        assert "still stopping after" in caplog.text
        assert not role.exited
        assert _quiet_status(rs.status()) == {
            "running": True, "status": "stopping", "stopping": True,
        }
        # A second stop joins the first instead of cancelling again.
        assert (await rs.stop())["status"] == "stopping"
        await _wait_until(lambda: not rs.is_running(), timeout=3.0)
        assert role.exited
        st = rs.status()
        assert st["running"] is False and "status" not in st
        # A start before the done callback ran is not left stopping.
        assert (await rs.handle({"cmd": "start"}))["status"] == "started"
        assert not rs.is_stopping() and rs.status()["running"] is True
        await asyncio.sleep(0)
        assert not rs.is_stopping()
        await _wait_until(lambda: role.started == 2)
        rs.stop_timeout = 5.0
        await rs.stop()

    asyncio.run(main())


class _SlowShutdownRole(_BlockingRole):
    """_shutdown takes a while, like the server closing its token pool."""

    def __init__(self):
        super().__init__()
        self.finished = False

    async def _shutdown(self):
        self.shutdown_called = True
        await asyncio.sleep(0.4)
        self.finished = True
        self._stop.set()


def test_threaded_role_over_its_budget_is_waited_for_not_abandoned(monkeypatch, caplog):
    monkeypatch.setattr(logging.getLogger("copycord"), "propagate", True)
    _BlockingRole.instances.clear()

    async def main():
        rs = RoleSupervisor(
            "server",
            ThreadedRole("server", _SlowShutdownRole, stop_timeout=0.1, abandon_grace=5.0),
            backoff=FAST,
            stop_timeout=10.0,
        )
        rs.start()
        await _wait_until(lambda: _BlockingRole.instances and _BlockingRole.instances[0].thread)
        inst = _BlockingRole.instances[0]
        stop = asyncio.ensure_future(rs.stop())
        await asyncio.sleep(0)
        states = []
        with caplog.at_level("WARNING", logger="copycord.supervisor"):
            while not stop.done():
                states.append((inst.finished, rs.status().get("status")))
                await asyncio.sleep(0.02)
        assert all(st == "stopping" for finished, st in states if not finished)
        assert (await stop)["status"] == "stopped"
        assert inst.finished
        assert "still shutting down after its" in caplog.text
        # Ended during the grace: not abandoned, and no wait for the rest of the grace.
        assert "abandoning it" not in caplog.text
        assert len(states) < 100

    asyncio.run(main())


def test_threaded_role_within_its_budget_logs_nothing(monkeypatch, caplog):
    monkeypatch.setattr(logging.getLogger("copycord"), "propagate", True)
    _BlockingRole.instances.clear()

    async def main():
        rs = RoleSupervisor(
            "server",
            ThreadedRole("server", _BlockingRole, stop_timeout=5.0, abandon_grace=5.0),
            backoff=FAST,
            stop_timeout=15.0,
        )
        rs.start()
        await _wait_until(lambda: _BlockingRole.instances and _BlockingRole.instances[0].thread)
        with caplog.at_level("WARNING", logger="copycord.supervisor"):
            assert (await rs.stop())["status"] == "stopped"
        assert "still shutting down" not in caplog.text
        assert "abandoning it" not in caplog.text

    asyncio.run(main())


class _HungShutdownRole(_BlockingRole):
    """_shutdown never lets run() return, like a session close that hangs."""

    async def _shutdown(self):
        self.shutdown_called = True
        self.loop = asyncio.get_running_loop()


def test_threaded_role_that_never_exits_is_abandoned_after_budget_and_grace(
    monkeypatch, caplog
):
    monkeypatch.setattr(logging.getLogger("copycord"), "propagate", True)
    _BlockingRole.instances.clear()

    def build():
        return _HungShutdownRole() if not _BlockingRole.instances else _BlockingRole()

    async def main():
        rs = RoleSupervisor(
            "server",
            ThreadedRole("server", build, stop_timeout=0.1, abandon_grace=0.3),
            backoff=FAST,
            stop_timeout=5.0,
        )
        rs.start()
        await _wait_until(lambda: _BlockingRole.instances and _BlockingRole.instances[0].thread)
        hung = _BlockingRole.instances[0]
        try:
            stop = asyncio.ensure_future(rs.stop())
            await asyncio.sleep(0)
            states = []
            with caplog.at_level("WARNING", logger="copycord.supervisor"):
                while not stop.done():
                    states.append(rs.status().get("status"))
                    await asyncio.sleep(0.02)
                r = await stop
            # Stopping for the whole budget and grace, then a startable state.
            assert len(states) > 5 and all(st == "stopping" for st in states)
            assert r["status"] == "stopped"
            assert hung.shutdown_called and hung.thread.is_alive()
            assert not rs.is_running() and not rs.is_stopping()
            assert "status" not in rs.status()
            assert caplog.text.count("abandoning it, state reset so it can be started again") == 1
            assert (await rs.handle({"cmd": "start"}))["status"] == "started"
            await _wait_until(
                lambda: len(_BlockingRole.instances) == 2 and _BlockingRole.instances[1].thread
            )
            assert rs.is_running()
            assert (await rs.stop())["status"] == "stopped"
            assert caplog.text.count("abandoning it") == 1
        finally:
            hung.loop.call_soon_threadsafe(hung._stop.set)
        await _wait_until(lambda: not hung.thread.is_alive())

    asyncio.run(main())


def test_server_stop_budget_is_built_from_the_pool_close_cap():
    from copycord.common.control_protocol import (
        ROLE_ABANDON_GRACE,
        SERVER_STOP_BUDGET,
        TOKEN_POOL_CLOSE_CAP,
    )

    assert SERVER_STOP_BUDGET > TOKEN_POOL_CLOSE_CAP
    role = sup_mod.server_factory()
    assert role.stop_timeout == SERVER_STOP_BUDGET
    assert role.abandon_grace == ROLE_ABANDON_GRACE > 0
    sup = build_supervisor(_fakes(), control_ports={"server": 0, "client": 0}, backoff=FAST)
    # The supervisor outwaits the role's budget plus its grace, so the role's own lines come first.
    assert sup.roles["server"].stop_timeout > SERVER_STOP_BUDGET + ROLE_ABANDON_GRACE


# Busy ports at startup


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


def test_import_error_logs_the_doctor_hint_once_and_backs_off(caplog):
    delays = []

    async def fake_sleep(d):
        delays.append(d)
        await asyncio.sleep(0)

    async def main():
        state = {"n": 0}

        async def broken():
            state["n"] += 1
            if state["n"] <= 3:
                raise ImportError("cannot import name 'ArgsKwargs' from "
                                  "'pydantic_core._pydantic_core' (unknown location)",
                                  name="pydantic_core._pydantic_core")
            await asyncio.Event().wait()

        rs = RoleSupervisor("admin", broken, backoff=FAST, sleep=fake_sleep)
        rs.start()
        await _wait_until(lambda: state["n"] == 4)
        await rs.stop()
        return rs

    with caplog.at_level(logging.INFO, logger="copycord.supervisor"):
        rs = asyncio.run(main())
    assert rs.crashes == 3
    assert delays == [sup_mod.IMPORT_RETRY_DELAY] * 3
    hints = [r for r in caplog.records if "copycord doctor --fix" in r.getMessage()]
    assert len(hints) == 1
    assert "pydantic_core._pydantic_core" in hints[0].getMessage()
    assert hints[0].levelno == logging.ERROR
