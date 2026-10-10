# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Single-process supervisor: admin, server and client as asyncio tasks.

Each role is a RoleSupervisor that owns one task, restarts it on crash with
capped exponential backoff and can be started/stopped on demand. Server and
client also get a ControlEndpoint speaking the control.py websocket protocol,
so the admin drives them over the same control ports as in split mode.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional

from common.control_protocol import ALLOWED_ENV, serve_json, status_payload

logger = logging.getLogger("copycord.supervisor")

RoleFactory = Callable[[], Awaitable[None]]

ROLES = ("admin", "server", "client")
# Shutdown runs client first so it stops forwarding before the server goes,
# and admin last so the UI stays up while the others drain.
SHUTDOWN_ORDER = ("client", "server", "admin")

DEFAULT_SERVER_CONTROL_PORT = 9101
DEFAULT_CLIENT_CONTROL_PORT = 9102

CODE_DIR = Path(__file__).resolve().parent.parent


@dataclass
class Backoff:
    """Capped exponential backoff between restarts of a crashed role."""

    initial: float = 1.0
    maximum: float = 60.0
    factor: float = 2.0
    # A run that lasted this long counts as healthy and resets the delay.
    reset_after: float = 60.0

    def delays(self):
        delay = self.initial
        while True:
            yield delay
            delay = min(delay * self.factor, self.maximum)


class RoleSupervisor:
    """
    Runs one role as an asyncio task and restarts it when it crashes.

    A crash (any exception from the role coroutine) never propagates out of
    the task, so other roles are unaffected. A role that returns on its own is
    left stopped unless restart_on_exit is set, which matches split mode where
    control.py does not respawn a child that exited.
    """

    def __init__(
        self,
        name: str,
        factory: RoleFactory,
        *,
        backoff: Optional[Backoff] = None,
        restart_on_exit: bool = False,
        stop_timeout: float = 20.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        before_start: Optional[Callable[[], None]] = None,
    ):
        self.name = name
        self.factory = factory
        self.backoff = backoff or Backoff()
        self.restart_on_exit = restart_on_exit
        self.stop_timeout = stop_timeout
        self._sleep = sleep
        self._before_start = before_start

        self._task: Optional[asyncio.Task] = None
        self._started_wall: Optional[float] = None
        self._started_mono: Optional[float] = None
        self.runs = 0
        self.crashes = 0
        self.restart_delays: List[float] = []

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _supervise(self) -> None:
        delays = self.backoff.delays()
        while True:
            if self._before_start is not None:
                try:
                    self._before_start()
                except Exception:
                    logger.exception("[%s] pre-start hook failed", self.name)
            self.runs += 1
            began = time.monotonic()
            try:
                await self.factory()
            except asyncio.CancelledError:
                raise
            except (Exception, SystemExit) as e:
                self.crashes += 1
                logger.error(
                    "[%s] role crashed: %r", self.name, e, exc_info=True
                )
            else:
                if not self.restart_on_exit:
                    logger.info("[%s] role exited", self.name)
                    return
                logger.warning("[%s] role exited unexpectedly", self.name)

            if time.monotonic() - began >= self.backoff.reset_after:
                delays = self.backoff.delays()
            delay = next(delays)
            self.restart_delays.append(delay)
            logger.info("[%s] restarting in %.1fs", self.name, delay)
            await self._sleep(delay)

    def start(self) -> dict:
        if self.is_running():
            return {"ok": True, "status": "already-running", "pid": os.getpid()}
        self._task = asyncio.get_running_loop().create_task(
            self._supervise(), name=f"copycord-{self.name}"
        )
        self._started_wall = time.time()
        self._started_mono = time.monotonic()
        logger.info("[%s] started", self.name)
        return {"ok": True, "status": "started", "pid": os.getpid()}

    async def stop(self) -> dict:
        task = self._task
        if task is None or task.done():
            self._clear()
            return {"ok": True, "status": "already-stopped"}

        task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(task), self.stop_timeout)
        except asyncio.CancelledError:
            if not task.done():
                raise
        except asyncio.TimeoutError:
            logger.warning(
                "[%s] did not stop within %.0fs", self.name, self.stop_timeout
            )
        except Exception:
            logger.debug("[%s] error while stopping", self.name, exc_info=True)
        self._clear()
        logger.info("[%s] stopped", self.name)
        return {"ok": True, "status": "stopped", "code": None}

    def _clear(self) -> None:
        self._task = None
        self._started_wall = None
        self._started_mono = None

    def status(self) -> dict:
        running = self.is_running()
        return status_payload(
            running, os.getpid(), self._started_wall, self._started_mono
        )

    async def handle(self, msg: dict) -> dict:
        """Same commands and reply shapes as ControlService._handle_ws_message."""
        cmd = (msg or {}).get("cmd")
        if cmd == "status":
            return self.status()
        if cmd == "start":
            return self.start()
        if cmd == "stop":
            return await self.stop()
        return {"ok": False, "error": "unknown-cmd"}


class ControlEndpoint:
    """Control websocket for one role, answering like control.py does."""

    def __init__(self, role: RoleSupervisor, host: str, port: int):
        self.role = role
        self.host = host
        self.port = int(port)
        self._server = None

    async def _ws_handler(self, ws):
        await serve_json(ws, self.role.handle)

    async def start(self) -> None:
        import websockets

        self._server = await websockets.serve(
            self._ws_handler, self.host, self.port, ping_interval=20, ping_timeout=20
        )
        if self.port == 0:
            self.port = self._server.sockets[0].getsockname()[1]
        logger.info(
            "[%s] control listening on %s:%d", self.role.name, self.host, self.port
        )

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None


@dataclass
class Supervisor:
    """All roles of one process, their control endpoints and shutdown order."""

    roles: Dict[str, RoleSupervisor]
    controls: List[ControlEndpoint] = field(default_factory=list)
    autostart: List[str] = field(default_factory=list)

    async def start(self) -> None:
        # Control ports first: the admin's own auto-start asks them for status.
        for ctl in self.controls:
            await ctl.start()
        for name in self.autostart:
            self.roles[name].start()

    async def shutdown(self) -> None:
        order = [n for n in SHUTDOWN_ORDER if n in self.roles]
        order += [n for n in self.roles if n not in order]
        for name in order:
            try:
                await self.roles[name].stop()
            except Exception:
                logger.exception("[%s] stop failed", name)
        for ctl in self.controls:
            await ctl.close()

    async def run(self, stop: asyncio.Event) -> None:
        try:
            await self.start()
            await stop.wait()
        finally:
            await self.shutdown()


# --------------------------------------------------------------------------
# Real role factories
# --------------------------------------------------------------------------


def _apply_db_config() -> None:
    """
    In-process counterpart of ControlService._load_env_for_child: copy the
    whitelisted DB.app_config keys into os.environ before a role starts.
    """
    try:
        from common.db import DBManager

        cfg = DBManager(os.environ["DB_PATH"]).get_all_config() or {}
    except Exception as e:
        logger.warning("DB config read failed: %s", e)
        return
    for k, v in cfg.items():
        if k in ALLOWED_ENV and v is not None:
            os.environ[k] = str(v)


class ThreadedRole:
    """
    Runs a role's blocking entry (ServerReceiver().run() / ClientListener().run())
    on its own thread with its own event loop, awaited as an asyncio task.

    Both roles drive their loop with run_until_complete, so they cannot share
    the supervisor's running loop; a thread keeps them in-process unchanged.
    Cancelling the awaiting task schedules the role's _shutdown() on its loop
    and waits for the thread to finish.
    """

    def __init__(self, name: str, build: Callable[[], object], stop_timeout: float = 15.0):
        self.name = name
        self.build = build
        self.stop_timeout = stop_timeout

    async def __call__(self) -> None:
        outer = asyncio.get_running_loop()
        done: asyncio.Future = outer.create_future()
        state: dict = {}
        stopping = threading.Event()

        def _resolve(exc: Optional[BaseException]) -> None:
            if done.done():
                return
            if exc is None:
                done.set_result(None)
            elif isinstance(exc, Exception):
                done.set_exception(exc)
            else:
                done.set_exception(RuntimeError(f"{self.name} exited: {exc!r}"))

        def _target() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            state["loop"] = loop
            exc: Optional[BaseException] = None
            inst = None
            try:
                inst = self.build()
                state["inst"] = inst
                if not stopping.is_set():
                    inst.run()
            except BaseException as e:
                exc = e
                # Release ports/sessions so the restart can bind again.
                if inst is not None and not loop.is_closed() and not loop.is_running():
                    with contextlib.suppress(BaseException):
                        loop.run_until_complete(inst._shutdown())
            finally:
                with contextlib.suppress(Exception):
                    if not loop.is_closed():
                        loop.close()
                asyncio.set_event_loop(None)
                with contextlib.suppress(RuntimeError):
                    outer.call_soon_threadsafe(_resolve, exc)

        thread = threading.Thread(
            target=_target, name=f"copycord-{self.name}", daemon=True
        )
        thread.start()
        try:
            await asyncio.shield(done)
        except asyncio.CancelledError:
            stopping.set()
            inst, loop = state.get("inst"), state.get("loop")
            if inst is not None and loop is not None:
                with contextlib.suppress(RuntimeError):
                    asyncio.run_coroutine_threadsafe(inst._shutdown(), loop)
            try:
                await asyncio.wait_for(asyncio.shield(done), self.stop_timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "[%s] thread still running after %.0fs; abandoning it",
                    self.name,
                    self.stop_timeout,
                )
            except Exception:
                pass
            raise


def server_factory() -> RoleFactory:
    def build():
        from server.server import ServerReceiver

        return ServerReceiver()

    return ThreadedRole("server", build)


def client_factory() -> RoleFactory:
    def build():
        from client.client import ClientListener

        return ClientListener()

    return ThreadedRole("client", build)


def admin_factory(host: str, port: int, log_level: str) -> RoleFactory:
    """uvicorn admin.app:app driven in the supervisor's loop (entrypoint.sh flags)."""

    async def run_admin() -> None:
        import uvicorn

        class _EmbeddedServer(uvicorn.Server):
            # The supervisor owns SIGINT/SIGTERM.
            @contextlib.contextmanager
            def capture_signals(self):
                yield

            def install_signal_handlers(self) -> None:
                pass

        config = uvicorn.Config(
            "admin.app:app",
            host=host,
            port=port,
            log_level=log_level,
            timeout_keep_alive=2,
            timeout_graceful_shutdown=1,
        )
        server = _EmbeddedServer(config)

        async def _serve() -> None:
            try:
                await server.serve()
            except SystemExit as e:
                # uvicorn exits the process on bind/startup failure.
                raise RuntimeError(f"admin server exited (code {e.code})") from None

        task = asyncio.ensure_future(_serve())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            server.should_exit = True
            try:
                await asyncio.wait_for(asyncio.shield(task), 10)
            except asyncio.TimeoutError:
                server.force_exit = True
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
            except Exception:
                pass
            raise

    return run_admin


# --------------------------------------------------------------------------
# Wiring
# --------------------------------------------------------------------------


def _set_runtime_env(split: bool) -> None:
    """
    Environment control.py would hand a child, plus loopback addresses for
    the single-process mode so roles find each other without compose DNS.
    """
    data = Path(os.getenv("DATA_DIR", str(CODE_DIR.parent / "data"))).resolve()
    with contextlib.suppress(Exception):
        data.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("DB_PATH", str(data / "data.db"))
    if split:
        return
    sport = os.getenv("CONTROL_PORT_SERVER", str(DEFAULT_SERVER_CONTROL_PORT))
    cport = os.getenv("CONTROL_PORT_CLIENT", str(DEFAULT_CLIENT_CONTROL_PORT))
    for key, value in (
        ("ADMIN_HOST", "127.0.0.1"),
        ("SERVER_WS_HOST", "127.0.0.1"),
        ("CLIENT_WS_HOST", "127.0.0.1"),
        ("WS_SERVER_CTRL_URL", f"ws://127.0.0.1:{sport}"),
        ("WS_CLIENT_CTRL_URL", f"ws://127.0.0.1:{cport}"),
        ("WS_SERVER_URL", f"ws://127.0.0.1:{os.getenv('SERVER_WS_PORT', '8765')}"),
        ("WS_CLIENT_URL", f"ws://127.0.0.1:{os.getenv('CLIENT_WS_PORT', '8766')}"),
    ):
        os.environ.setdefault(key, value)


def _control_port(role: str, split: bool) -> int:
    if split and os.getenv("CONTROL_PORT"):
        return int(os.environ["CONTROL_PORT"])
    if role == "server":
        return int(os.getenv("CONTROL_PORT_SERVER", DEFAULT_SERVER_CONTROL_PORT))
    return int(os.getenv("CONTROL_PORT_CLIENT", DEFAULT_CLIENT_CONTROL_PORT))


def build_supervisor(
    role: Optional[str] = None,
    factories: Optional[Dict[str, RoleFactory]] = None,
    *,
    control_host: Optional[str] = None,
    control_ports: Optional[Dict[str, int]] = None,
    backoff: Optional[Backoff] = None,
) -> Supervisor:
    """
    role=None: all three roles in this process; the admin is autostarted and
    drives server/client through loopback control ports, as it does in split
    mode. role="admin"|"server"|"client": just that role, the split-mode
    equivalent of one container (server/client wait for the admin's start).

    factories/control_* overrides exist for tests.
    """
    if role is not None and role not in ROLES:
        raise ValueError(f"unknown role {role!r}")
    split = role is not None
    names = [role] if split else list(ROLES)

    real = factories is None
    if real:
        factories = {}
        if "admin" in names:
            factories["admin"] = admin_factory(
                host="0.0.0.0",
                port=int(os.getenv("ADMIN_PORT", "8080")),
                log_level=os.getenv("LOG_LEVEL", "info").lower(),
            )
        if "server" in names:
            factories["server"] = server_factory()
        if "client" in names:
            factories["client"] = client_factory()

    roles: Dict[str, RoleSupervisor] = {}
    for name in names:
        is_admin = name == "admin"
        roles[name] = RoleSupervisor(
            name,
            factories[name],
            backoff=backoff,
            restart_on_exit=is_admin,
            # Real server/client read their config from os.environ, like a
            # control.py child does.
            before_start=_apply_db_config if real and not is_admin else None,
        )

    host = control_host or ("0.0.0.0" if split else "127.0.0.1")
    controls = []
    for name in names:
        if name == "admin":
            continue
        port = (control_ports or {}).get(name)
        if port is None:
            port = _control_port(name, split)
        controls.append(ControlEndpoint(roles[name], host, port))

    autostart = ["admin"] if "admin" in roles else []
    return Supervisor(roles=roles, controls=controls, autostart=autostart)


async def run(role: Optional[str] = None) -> None:
    _set_runtime_env(split=role is not None)
    sup = build_supervisor(role)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            # Windows: asyncio.run turns Ctrl+C into cancellation of this
            # task, and Supervisor.run's finally still shuts down in order.
            pass

    await sup.run(stop)
