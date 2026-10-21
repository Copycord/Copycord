# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
Runs admin, server and client as asyncio tasks in one process, restarting a
crashed role with backoff. The admin drives server and client over their
loopback control ports.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import logging
import os
import secrets
import signal
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from copycord.common.config import CURRENT_VERSION
from copycord.common.control_protocol import ALLOWED_ENV, serve_json, status_payload
from copycord.common.paths import data_dir
from copycord.common.ports import (
    DEFAULT_ADMIN_PORT,
    DEFAULT_CLIENT_CONTROL_PORT,
    DEFAULT_CLIENT_WS_PORT,
    DEFAULT_SERVER_CONTROL_PORT,
    DEFAULT_SERVER_WS_PORT,
    agent_host,
    dashboard_host,
)
from copycord.common.websockets import INTERNAL_TOKEN_ENV
from copycord import ui
from copycord.logging_setup import apply_level, setup_logging

logger = logging.getLogger("copycord.supervisor")

RoleFactory = Callable[[], Awaitable[None]]

ROLES = ("admin", "server", "client")
# Client stops first so it stops forwarding before the server; admin last.
SHUTDOWN_ORDER = ("client", "server", "admin")

# How long a busy port is retried at startup: a Copycord that just stopped can
# hold its ports for a few seconds.
PORT_WAIT = 15.0
PORT_RETRY = 0.5


class PortBusyError(Exception):
    """A port Copycord needs stayed in use for PORT_WAIT seconds."""

    def __init__(self, port: int):
        super().__init__(f"port {port} is in use")
        self.port = port


def port_in_use(e: OSError) -> bool:
    return e.errno in (errno.EADDRINUSE, 10048) or getattr(e, "winerror", None) == 10048


def port_free(host: str, port: int) -> bool:
    """Whether host:port can be bound now, the way uvicorn binds it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # On Windows SO_REUSEADDR would share a live port.
        if sys.platform != "win32":
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
        except OSError as e:
            if port_in_use(e):
                return False
            raise
    return True


async def wait_for_port(host: str, port: int, timeout: float = PORT_WAIT) -> None:
    """Return once host:port is free; PortBusyError after timeout seconds."""
    deadline = time.monotonic() + timeout
    warned = False
    while not port_free(host, port):
        if time.monotonic() >= deadline:
            raise PortBusyError(port)
        if not warned:
            logger.warning("Port %d is in use, waiting up to %.0f seconds for it", port, timeout)
            warned = True
        await asyncio.sleep(PORT_RETRY)

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
    """Runs one role as an asyncio task and restarts it when it crashes."""

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
                    logger.exception("%s role pre-start hook failed", self.name)
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
                    logger.info("%s role exited", self.name)
                    return
                logger.warning("%s role exited unexpectedly", self.name)

            if time.monotonic() - began >= self.backoff.reset_after:
                delays = self.backoff.delays()
            delay = next(delays)
            self.restart_delays.append(delay)
            logger.info("%s role restarting in %.1fs", self.name, delay)
            await self._sleep(delay)

    def start(self) -> dict:
        if self.is_running():
            return {"ok": True, "status": "already-running", "pid": os.getpid()}
        self._task = asyncio.get_running_loop().create_task(
            self._supervise(), name=f"copycord-{self.name}"
        )
        self._started_wall = time.time()
        self._started_mono = time.monotonic()
        logger.info("%s role started", self.name)
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
            logger.debug("%s role error while stopping", self.name, exc_info=True)
        self._clear()
        logger.info("%s role stopped", self.name)
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
        """Commands: status, start, stop."""
        cmd = (msg or {}).get("cmd")
        if cmd == "status":
            return self.status()
        if cmd == "start":
            return self.start()
        if cmd == "stop":
            return await self.stop()
        return {"ok": False, "error": "unknown-cmd"}


class ControlEndpoint:
    """Control websocket for one role, speaking common.control_protocol."""

    def __init__(self, role: RoleSupervisor, host: str, port: int,
                 port_wait: float = PORT_WAIT):
        self.role = role
        self.host = host
        self.port = int(port)
        self.port_wait = port_wait
        self._server = None

    async def _ws_handler(self, ws):
        await serve_json(ws, self.role.handle)

    async def start(self) -> None:
        import websockets

        deadline = time.monotonic() + self.port_wait
        warned = False
        while True:
            try:
                self._server = await websockets.serve(
                    self._ws_handler, self.host, self.port, ping_interval=20, ping_timeout=20
                )
                break
            except OSError as e:
                if not port_in_use(e):
                    raise
                if time.monotonic() >= deadline:
                    raise PortBusyError(self.port) from None
                if not warned:
                    logger.warning("Port %d is in use, waiting up to %.0f seconds for it",
                                   self.port, self.port_wait)
                    warned = True
                await asyncio.sleep(PORT_RETRY)
        if self.port == 0:
            self.port = self._server.sockets[0].getsockname()[1]
        logger.info(
            "%s control listening on %s:%d", self.role.name, self.host, self.port
        )

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None


@dataclass
class Supervisor:
    """All roles, their control endpoints and shutdown order."""

    roles: Dict[str, RoleSupervisor]
    controls: List[ControlEndpoint] = field(default_factory=list)
    autostart: List[str] = field(default_factory=list)
    # (host, port) the auto-started roles bind, waited for before they start.
    ports: List[Tuple[str, int]] = field(default_factory=list)
    port_wait: float = PORT_WAIT

    async def start(self) -> None:
        # Control ports first: the admin's own auto-start asks them for status.
        for ctl in self.controls:
            await ctl.start()
        for host, port in self.ports:
            await wait_for_port(host, port, self.port_wait)
        for name in self.autostart:
            self.roles[name].start()

    async def shutdown(self) -> None:
        order = [n for n in SHUTDOWN_ORDER if n in self.roles]
        order += [n for n in self.roles if n not in order]
        for name in order:
            try:
                await self.roles[name].stop()
            except Exception:
                logger.exception("%s role stop failed", name)
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


def _apply_db_log_level() -> None:
    """Let the DB LOG_LEVEL win over .env before logging is installed."""
    try:
        from copycord.common.db import DBManager

        db = DBManager(os.environ["DB_PATH"])
        try:
            level = (db.get_all_config() or {}).get("LOG_LEVEL")
        finally:
            db.close()
    except Exception:
        return
    if level:
        os.environ["LOG_LEVEL"] = str(level)


def _apply_db_config() -> None:
    """Copy the allowed app config keys into os.environ before a role starts.

    The DB LOG_LEVEL (the dashboard's Log Level) also re-levels the loggers.
    """
    try:
        from copycord.common.db import DBManager

        db = DBManager(os.environ["DB_PATH"])
        try:
            cfg = db.get_all_config() or {}
        finally:
            db.close()
    except Exception:
        logger.warning(
            "DB config read failed; the role starts with .env values only", exc_info=True
        )
        return
    for k, v in cfg.items():
        if k in ALLOWED_ENV and v is not None:
            os.environ[k] = str(v)
    apply_level()


class ThreadedRole:
    """
    Runs a role's blocking run() on its own thread and event loop, awaited as a task.

    The roles call run_until_complete, so they cannot share the supervisor's loop.
    Cancelling the task runs the role's _shutdown() and joins the thread.
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
        from copycord.server.server import ServerReceiver

        return ServerReceiver()

    return ThreadedRole("server", build)


def client_factory() -> RoleFactory:
    def build():
        from copycord.client.client import ClientListener

        return ClientListener()

    return ThreadedRole("client", build)


def admin_factory(host: str, port: int, log_level: str) -> RoleFactory:
    """The admin app under uvicorn in the supervisor's loop."""

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
            "copycord.admin.app:app",
            host=host,
            port=port,
            # No uvicorn log config: its records go through logging_setup like
            # every other logger, held at WARNING there unless LOG_LEVEL=DEBUG.
            log_config=None,
            log_level=log_level if log_level == "debug" else None,
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


def _set_runtime_env() -> None:
    """Data paths plus loopback addresses so roles find each other in-process."""
    data = data_dir()
    with contextlib.suppress(Exception):
        data.mkdir(parents=True, exist_ok=True)
    # copycord.admin.app reads DATA_DIR itself; keep it on the same directory as DB_PATH.
    os.environ.setdefault("DATA_DIR", str(data))
    os.environ.setdefault("DB_PATH", str(data / "data.db"))
    sport = os.getenv("CONTROL_PORT_SERVER", str(DEFAULT_SERVER_CONTROL_PORT))
    cport = os.getenv("CONTROL_PORT_CLIENT", str(DEFAULT_CLIENT_CONTROL_PORT))
    for key, value in (
        # ADMIN_HOST is where the roles connect to the dashboard, not where it listens.
        ("ADMIN_HOST", agent_host(dashboard_host())),
        ("SERVER_WS_HOST", "127.0.0.1"),
        ("CLIENT_WS_HOST", "127.0.0.1"),
        ("WS_SERVER_CTRL_URL", f"ws://127.0.0.1:{sport}"),
        ("WS_CLIENT_CTRL_URL", f"ws://127.0.0.1:{cport}"),
        ("WS_SERVER_URL", f"ws://127.0.0.1:{os.getenv('SERVER_WS_PORT', str(DEFAULT_SERVER_WS_PORT))}"),
        ("WS_CLIENT_URL", f"ws://127.0.0.1:{os.getenv('CLIENT_WS_PORT', str(DEFAULT_CLIENT_WS_PORT))}"),
    ):
        os.environ.setdefault(key, value)
    # Shared by the roles for the admin's internal sockets (/bus, /ws/out).
    os.environ.setdefault(INTERNAL_TOKEN_ENV, secrets.token_urlsafe(32))


def _control_port(role: str) -> int:
    if role == "server":
        return int(os.getenv("CONTROL_PORT_SERVER", DEFAULT_SERVER_CONTROL_PORT))
    return int(os.getenv("CONTROL_PORT_CLIENT", DEFAULT_CLIENT_CONTROL_PORT))


def build_supervisor(
    factories: Optional[Dict[str, RoleFactory]] = None,
    *,
    control_host: Optional[str] = None,
    control_ports: Optional[Dict[str, int]] = None,
    backoff: Optional[Backoff] = None,
    inprocess_transport: Optional[bool] = None,
) -> Supervisor:
    """All three roles in this process, using the in-process transport by default."""
    real = factories is None
    ports = []
    if inprocess_transport is None:
        inprocess_transport = real
    if inprocess_transport:
        from copycord.common import inprocess

        inprocess.enable()
    if real:
        ports.append((dashboard_host(), int(os.getenv("ADMIN_PORT") or DEFAULT_ADMIN_PORT)))
        factories = {
            "admin": admin_factory(
                host=ports[0][0],
                port=ports[0][1],
                log_level=(os.getenv("LOG_LEVEL") or "info").lower(),
            ),
            "server": server_factory(),
            "client": client_factory(),
        }

    roles: Dict[str, RoleSupervisor] = {}
    for name in ROLES:
        is_admin = name == "admin"
        roles[name] = RoleSupervisor(
            name,
            factories[name],
            backoff=backoff,
            restart_on_exit=is_admin,
            # Real server/client read their config from os.environ.
            before_start=_apply_db_config if real and not is_admin else None,
        )

    host = control_host or "127.0.0.1"
    controls = []
    for name in ("server", "client"):
        port = (control_ports or {}).get(name)
        if port is None:
            port = _control_port(name)
        controls.append(ControlEndpoint(roles[name], host, port))

    return Supervisor(roles=roles, controls=controls, autostart=["admin"],
                      ports=ports)


async def _auto_update() -> None:
    """Start the auto-update loop when AUTO_UPDATE=true."""
    from copycord import update_check

    try:
        if update_check.auto_update_enabled() and not ui.is_interactive():
            logger.info("[⬆️] Auto-update is on, checking every 6 hours")
            await update_check.auto_update_loop()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("auto-update stopped")


async def run() -> None:
    _set_runtime_env()
    _apply_db_log_level()
    setup_logging()
    # The role banners are INFO on role threads, so only this one reaches the console.
    logger.info("Starting Copycord %s", CURRENT_VERSION)
    sup = build_supervisor()

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            # On Windows Ctrl+C arrives as cancellation; run's finally still shuts down in order.
            pass

    auto_update = asyncio.create_task(_auto_update())
    try:
        await sup.run(stop)
    finally:
        auto_update.cancel()
        _close_admin_db()
        # The rest (commands cog, module-level configs) so SQLite drops data.db-wal/-shm.
        from copycord.common.db import close_all

        close_all()


def _close_admin_db() -> None:
    """Close the admin app's module-level DB once every role is down; admin restarts reuse it."""
    admin = sys.modules.get("copycord.admin.app")
    db = getattr(admin, "db", None)
    if db is None:
        return
    try:
        db.close()
    except Exception:
        logger.debug("admin DB close failed", exc_info=True)
