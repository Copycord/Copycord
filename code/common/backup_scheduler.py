# =============================================================================
#  Copycord
#  Copyright (C) 2021 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import tarfile
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Awaitable, Callable, Iterable, Optional
from zoneinfo import ZoneInfo


@dataclass
class BackupConfig:
    """
    Configuration for the SQLite backup scheduler.

    - db_path: path to the live SQLite DB (e.g., /data/data.db)
    - backup_dir: directory to store archives (e.g., /data/backups)
    - retain: number of archives (*.tar.gz) to keep (by newest mtime)
    - run_at: "HH:MM" 24h clock, local to `timezone` (used if interval_minutes is None)
    - timezone: IANA TZ name (e.g., "UTC", "America/New_York")
    - interval_minutes: if set (e.g. 60), run every N minutes instead of once daily
    """

    db_path: Path
    backup_dir: Path
    retain: int = 14
    run_at: str = "03:17"
    timezone: str = "UTC"
    interval_minutes: Optional[int] = None


class DailySQLiteBackupScheduler:
    """
    Creates compressed SQLite snapshot archives in:
        <backup_dir>/<YYYY-MM-DD>_<HH-MM-SS>[ -NN ].tar.gz
    (contains a single file named 'data.db')

    Supports:
      - Daily run at `run_at` (default), OR
      - Repeating runs every `interval_minutes` (multiple times per day)

    Robust behavior:
      - (Re)creates backup_dir if missing
      - Retries archive write once if backup_dir vanished mid-write
      - Prunes by count (`retain`) using newest mtime first
    """

    def __init__(
        self,
        cfg: BackupConfig,
        logger=None,
        on_complete: Optional[Callable[[Path], Awaitable[None] | None]] = None,
    ) -> None:
        self.cfg = cfg
        self.log = logger
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self.on_complete = on_complete

        self._dbg("init: cfg=%s", self.cfg)
        self._ensure_backup_dir()

        self._tz = None
        if ZoneInfo is not None:
            try:
                self._tz = ZoneInfo(self.cfg.timezone)
                self._dbg("timezone resolved: %s", self.cfg.timezone)
            except Exception:
                self._tz = ZoneInfo("UTC")
                self._warn("Invalid TZ %r; falling back to UTC", self.cfg.timezone)
        else:
            self._dbg("zoneinfo not available; using naive UTC")

    def start(self) -> None:
        """Start the scheduler as a background asyncio task."""
        if self._task and not self._task.done():
            self._dbg("start: scheduler already running")
            return
        self._stop_evt.clear()
        self._task = asyncio.create_task(self._run_loop(), name="db-backup-scheduler")
        if self.cfg.interval_minutes and self.cfg.interval_minutes > 0:
            self._info(
                "backup scheduler started (every=%dm, retain=%d, dir=%s, tz=%s)",
                self.cfg.interval_minutes,
                self.cfg.retain,
                self.cfg.backup_dir,
                self.cfg.timezone,
            )
        else:
            self._info(
                "backup scheduler started (run_at=%s %s, retain=%d, dir=%s)",
                self.cfg.run_at,
                self.cfg.timezone,
                self.cfg.retain,
                self.cfg.backup_dir,
            )

    async def stop(self) -> None:
        """Signal the scheduler to stop and await the task."""
        self._info("backup scheduler stopping...")
        self._stop_evt.set()
        if self._task:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._info("backup scheduler stopped")

    async def run_now(self) -> Path:
        """Run a backup immediately (manual trigger)."""
        self._info("manual backup requested")
        out = await self._backup_once()
        try:
            if self.on_complete:
                if asyncio.iscoroutinefunction(self.on_complete):
                    await self.on_complete(out)
                else:
                    self.on_complete(out)
        except Exception as e:
            self._exception("on_complete failed after manual backup: %s", e)
        self._info("manual backup finished: %s", out.name)
        return out

    async def _run_loop(self) -> None:

        try:
            if self.cfg.interval_minutes and self.cfg.interval_minutes > 0:
                first_delay = max(1.0, float(self.cfg.interval_minutes * 60))
                self._dbg(
                    "run_loop: sleeping %.2fs until first interval run", first_delay
                )
            else:
                first_delay = self._seconds_until_next_run()
                self._dbg("run_loop: sleeping %.2fs until next daily run", first_delay)

            await asyncio.wait_for(self._stop_evt.wait(), timeout=first_delay)
            self._dbg("run_loop: stop signaled before first run")
            return
        except asyncio.TimeoutError:
            pass

        while not self._stop_evt.is_set():
            try:
                out = await self._backup_once()
                self._info("DB backup complete: %s", out.name)

                if self.on_complete:
                    try:
                        if asyncio.iscoroutinefunction(self.on_complete):
                            await self.on_complete(out)
                        else:
                            self.on_complete(out)
                    except Exception as e:
                        self._exception(
                            "on_complete failed after scheduled backup: %s", e
                        )

            except Exception as e:
                self._exception("DB backup failed: %s", e)

            try:
                if self.cfg.interval_minutes and self.cfg.interval_minutes > 0:
                    sleep_s = max(1, int(self.cfg.interval_minutes * 60))
                else:
                    sleep_s = 24 * 3600
                self._dbg("run_loop: sleeping %ds before next run", sleep_s)
                await asyncio.wait_for(self._stop_evt.wait(), timeout=sleep_s)
                self._dbg("run_loop: stop signaled during wait")
                return
            except asyncio.TimeoutError:
                self._dbg("run_loop: waking for next run")
                continue

    def _seconds_until_next_run(self) -> float:
        """Compute seconds until the next daily run_at in configured timezone."""
        hh, mm = self.cfg.run_at.split(":")
        hh_i, mm_i = int(hh), int(mm)

        if self._tz:
            now = datetime.now(self._tz)
            run_time = dtime(
                hour=hh_i, minute=mm_i, second=0, microsecond=0, tzinfo=self._tz
            )
            run_at = datetime.combine(now.date(), run_time)
            if run_at <= now:
                run_at += timedelta(days=1)
            delta = (run_at - now).total_seconds()
            self._dbg(
                "next_run (local %s): now=%s run_at=%s delta=%.2fs",
                self.cfg.timezone,
                now.isoformat(),
                run_at.isoformat(),
                delta,
            )
        else:
            now = datetime.utcnow()
            run_at = now.replace(hour=hh_i, minute=mm_i, second=0, microsecond=0)
            if run_at <= now:
                run_at += timedelta(days=1)
            delta = (run_at - now).total_seconds()
            self._dbg(
                "next_run (UTC naive): now=%s run_at=%s delta=%.2fs",
                now.isoformat() + "Z",
                run_at.isoformat() + "Z",
                delta,
            )

        return max(1.0, float(delta))

    def _ensure_backup_dir(self) -> None:
        """(Re)create the backup directory if missing."""
        try:
            if not self.cfg.backup_dir.exists():
                self._info("creating backup_dir: %s", self.cfg.backup_dir)
            self.cfg.backup_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._warn("Unable to create backup_dir %s: %s", self.cfg.backup_dir, e)
            raise

    def _now(self) -> datetime:
        return datetime.now(self._tz) if self._tz else datetime.utcnow()

    def _unique_archive_path(self, when: datetime) -> Path:
        """
        Build a unique archive path like:
        """
        stamp = when.strftime("%Y-%m-%d_%H-%M-%S")
        base = f"{stamp}.tar.gz"
        candidate = self.cfg.backup_dir / base
        if not candidate.exists():
            return candidate

        for i in range(1, 100):
            candidate = self.cfg.backup_dir / f"{stamp}-{i:02d}.tar.gz"
            if not candidate.exists():
                return candidate

        ticks = int(time.monotonic() * 1000)
        return self.cfg.backup_dir / f"{stamp}-{ticks}.tar.gz"

    async def _backup_once(self) -> Path:
        self._ensure_backup_dir()

        now = self._now()
        final_tgz = self._unique_archive_path(now)
        tmp_tgz = self.cfg.backup_dir / f".{final_tgz.stem}.tmp.tar.gz"

        t0 = time.monotonic()
        self._info("backup start: db=%s -> %s", self.cfg.db_path, final_tgz.name)

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
            tmp_db_path = Path(tf.name)
        self._dbg("snapshot temp file: %s", tmp_db_path)

        try:
            snap_t0 = time.monotonic()
            self._dbg("opening live db for backup: %s", self.cfg.db_path)
            src = sqlite3.connect(str(self.cfg.db_path))
            try:
                self._dbg("creating snapshot db: %s", tmp_db_path)
                with sqlite3.connect(str(tmp_db_path)) as dest:
                    src.backup(dest)
            finally:
                src.close()
            self._dbg("snapshot completed in %.2fs", time.monotonic() - snap_t0)

            def _write_archive_once() -> None:
                self._dbg("opening tar for write: %s", tmp_tgz)
                with tarfile.open(tmp_tgz, "w:gz") as tar:
                    tar.add(str(tmp_db_path), arcname="data.db")
                size_tmp = tmp_tgz.stat().st_size if tmp_tgz.exists() else 0
                self._dbg("tar written (tmp): %s bytes", f"{size_tmp:,}")
                tmp_tgz.replace(final_tgz)
                size_final = final_tgz.stat().st_size if final_tgz.exists() else 0
                self._info(
                    "archive ready: %s (%s bytes)", final_tgz.name, f"{size_final:,}"
                )

            try:
                _write_archive_once()
            except FileNotFoundError:
                self._warn(
                    "backup_dir missing during write; recreating and retrying..."
                )
                self._ensure_backup_dir()
                _write_archive_once()

        finally:
            with contextlib.suppress(Exception):
                if tmp_db_path.exists():
                    sz = tmp_db_path.stat().st_size
                    self._dbg(
                        "removing temp snapshot %s (size=%s bytes)",
                        tmp_db_path,
                        f"{sz:,}",
                    )
                tmp_db_path.unlink(missing_ok=True)

        self._prune_old_archives()

        self._info(
            "backup finished in %.2fs -> %s", time.monotonic() - t0, final_tgz.name
        )
        return final_tgz

    def _prune_old_archives(self) -> None:
        if not self.cfg.backup_dir.exists():
            self._warn(
                "backup_dir disappeared before pruning; recreating and skipping prune"
            )
            self._ensure_backup_dir()
            return

        archives = sorted(
            self.cfg.backup_dir.glob("*.tar.gz"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        total = len(archives)
        self._dbg("prune: found %d archives; retain=%d", total, self.cfg.retain)

        to_delete: Iterable[Path] = archives[self.cfg.retain :]
        count = 0
        bytes_freed = 0
        for old in to_delete:
            try:
                size = old.stat().st_size
            except Exception:
                size = 0
            with contextlib.suppress(Exception):
                old.unlink()
                count += 1
                bytes_freed += size
                self._dbg("prune: deleted %s (%s bytes)", old.name, f"{size:,}")

        if count:
            self._info(
                "prune: removed %d old archives, freed %s bytes",
                count,
                f"{bytes_freed:,}",
            )
        else:
            self._dbg("prune: nothing to delete")

    def _dbg(self, msg: str, *args) -> None:
        if self.log:
            try:
                self.log.debug(msg, *args)
                return
            except Exception:
                pass
        print("[backup:debug] " + (msg % args))

    def _info(self, msg: str, *args) -> None:
        if self.log:
            try:
                self.log.info(msg, *args)
                return
            except Exception:
                pass
        print("[backup] " + (msg % args))

    def _warn(self, msg: str, *args) -> None:
        if self.log:
            try:
                self.log.warning(msg, *args)
                return
            except Exception:
                pass
        print("[backup:warn] " + (msg % args))

    def _exception(self, msg: str, *args) -> None:
        if self.log:
            try:
                self.log.exception(msg, *args)
                return
            except Exception:
                pass
        print("[backup:exception] " + (msg % args))
