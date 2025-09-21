# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Optional
import time
from datetime import datetime, timezone
from common.db import DBManager

# ------------------- Defaults & wiring -------------------
ROLE = os.getenv("ROLE", "server").strip().lower()  # "server" or "client"
DEFAULT_PORT = 9101 if ROLE == "server" else 9102
PORT = int(os.getenv("CONTROL_PORT", str(DEFAULT_PORT)))
ROOT = Path("/app")
DATA = Path("/data")
DATA.mkdir(parents=True, exist_ok=True)

DB_PATH = os.getenv("DB_PATH", "/data/data.db")
PYTHONPATH = "/app"
MODULE = "server.server" if ROLE == "server" else "client.client"

PIDFILE = DATA / f"{ROLE}.pid"
LOG_OUT = DATA / f"{ROLE}.out"


class ControlService:
    """
    Central Control hub for Client/Server.
    """

    ALLOWED_ENV = {
        "SERVER_TOKEN",
        "CLONE_GUILD_ID",
        "COMMAND_USERS",
        "DELETE_CHANNELS",
        "DELETE_THREADS",
        "DELETE_ROLES",
        "CLONE_EMOJI",
        "CLONE_STICKER",
        "CLONE_ROLES",
        "MIRROR_ROLE_PERMISSIONS",
        "CLIENT_TOKEN",
        "HOST_GUILD_ID",
        "ENABLE_CLONING",
        "LOG_LEVEL",
    }

    def __init__(
        self,
        role: str,
        module: str,
        port: int,
        db_path: str,
        root: Path,
        pidfile: Path,
        log_out: Path,
        pythonpath: str = "/app",
    ):
        self.role = role
        self.module = module
        self.port = int(port)
        self.db_path = db_path
        self.root = root
        self.pidfile = pidfile
        self.log_out = log_out
        self.pythonpath = pythonpath

        self._db = DBManager(self.db_path)
        self._child: Optional[subprocess.Popen] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._started_wall: Optional[float] = None   
        self._started_mono: Optional[float] = None   

    # ----------------- DB-backed environment -----------------
    def _load_env_for_child(self) -> dict:
        """
        Merge current ENV with config values from DB.app_config,
        but only pass through whitelisted keys.
        """
        env = dict(os.environ)
        env["PYTHONPATH"] = self.pythonpath
        env["DB_PATH"] = self.db_path  # let the child know where the DB is

        try:
            all_cfg = self._db.get_all_config()  # {key: value}
        except Exception as e:
            # If DB is not ready for any reason, proceed with current env
            sys.stdout.write(f"[control] DB read failed: {e}\n")
            sys.stdout.flush()
            all_cfg = {}

        for k, v in (all_cfg or {}).items():
            if k in self.ALLOWED_ENV and v is not None:
                env[k] = str(v)
        return env

    # ----------------- Child process control -----------------
    def is_running(self) -> bool:
        return self._child is not None and self._child.poll() is None

    @staticmethod
    def _tee_stdout(proc: subprocess.Popen, logfile: Path) -> None:
        """
        Read child's stdout line-by-line and tee to container stdout AND a file.
        """
        try:
            logfile.parent.mkdir(parents=True, exist_ok=True)
            with open(logfile, "a", encoding="utf-8") as lf:
                for line in iter(proc.stdout.readline, ""):
                    # stream to docker logs
                    sys.stdout.write(line)
                    sys.stdout.flush()
                    # stream to file
                    lf.write(line)
                    lf.flush()
        except Exception as e:
            sys.stdout.write(f"[control] tee error: {e}\n")
            sys.stdout.flush()
        finally:
            try:
                if proc.stdout:
                    proc.stdout.close()
            except Exception:
                pass

    def start(self) -> dict:
        if self.is_running():
            return {"ok": True, "status": "already-running", "pid": self._child.pid}

        env = self._load_env_for_child()

        # Spawn the real bot process as a module so package imports work.
        self._child = subprocess.Popen(
            [sys.executable, "-u", "-m", self.module],
            cwd=str(self.root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1,
        )

        # Record PID and start the tee thread
        self.pidfile.write_text(str(self._child.pid), encoding="utf-8")
        self._reader_thread = threading.Thread(
            target=self._tee_stdout, args=(self._child, self.log_out), daemon=True
        )
        self._reader_thread.start()
        
        self._started_wall = time.time()
        self._started_mono = time.monotonic()

        return {"ok": True, "status": "started", "pid": self._child.pid}

    def stop(self) -> dict:
        if not self.is_running():
            try:
                self.pidfile.unlink()
            except Exception:
                pass
            self._child = None
            self._started_wall = None
            self._started_mono = None
            return {"ok": True, "status": "already-stopped"}

        try:
            self._child.send_signal(signal.SIGTERM)
            self._child.wait(timeout=15)
        except Exception:
            try:
                self._child.kill()
            except Exception:
                pass
        finally:
            try:
                self.pidfile.unlink()
            except Exception:
                pass
            code = self._child.poll()
            self._child = None
            self._started_wall = None
            self._started_mono = None
            return {"ok": True, "status": "stopped", "code": code}

    # ----------------- WebSocket protocol -----------------
    async def _handle_ws_message(self, msg: dict) -> dict:
        """
        Handle incoming WebSocket messages and respond based on the command type.
        Supported: status | start | stop
        """
        cmd = (msg or {}).get("cmd")
        if cmd == "status":
            running = self.is_running()
            started_at = (
                datetime.fromtimestamp(self._started_wall, tz=timezone.utc).isoformat()
                if running and self._started_wall else None
            )
            uptime_sec = (
                max(0.0, time.monotonic() - self._started_mono)
                if running and self._started_mono else None
            )
            return {
                "ok": True,
                "running": running,
                "pid": (self._child.pid if running else None),
                "started_at": started_at,
                "uptime_sec": uptime_sec,
            }
        if cmd == "start":
            return self.start()
        if cmd == "stop":
            return self.stop()
        return {"ok": False, "error": "unknown-cmd"}

    async def ws_handler(self, ws, path):
        """
        Accept JSON messages like:
          {"cmd": "status" | "start" | "stop"}
        """
        try:
            async for raw in ws:
                try:
                    data = json.loads(raw) if isinstance(raw, (str, bytes)) else {}
                except Exception:
                    data = {}
                resp = await self._handle_ws_message(data)
                await ws.send(json.dumps(resp))
        except Exception as e:
            try:
                await ws.send(json.dumps({"ok": False, "error": str(e)}))
            except Exception:
                pass

    # ----------------- Server lifecycle -----------------
    async def run(self):
        import websockets

        server = await websockets.serve(
            self.ws_handler, "0.0.0.0", self.port, ping_interval=20, ping_timeout=20
        )

        loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

        def _on_term():
            try:
                self.stop()
            except Exception:
                pass
            if self._stop_event:
                self._stop_event.set()

        # SIG handlers (no-op on Windows)
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _on_term)
            except NotImplementedError:
                pass

        await self._stop_event.wait()
        server.close()
        await server.wait_closed()


# ----------------- Entrypoint -----------------
if __name__ == "__main__":
    service = ControlService(
        role=ROLE,
        module=MODULE,
        port=PORT,
        db_path=DB_PATH,
        root=ROOT,
        pidfile=PIDFILE,
        log_out=LOG_OUT,
        pythonpath=PYTHONPATH,
    )
    asyncio.run(service.run())
