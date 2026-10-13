# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

from __future__ import annotations

import asyncio
import contextlib
import gzip
import io
import json
import logging
import os
import posixpath
import shutil
import sqlite3
import stat
import tarfile
import tempfile
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone, time as dtime, timedelta
from pathlib import Path
from typing import Awaitable, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

_log = logging.getLogger("copycord.backup")

# Written next to the archives when a backup fails its check; the next good backup removes it.
CHECK_FAILED = ".last-backup-failed.json"


class BackupCheckError(RuntimeError):
    """The copy of the database failed PRAGMA integrity_check; no archive was kept."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(
            f"the copy of the database failed its integrity check ({reason}); "
            "no backup was kept"
        )


def integrity_problems(db_path: Path) -> list:
    """What PRAGMA integrity_check reports for db_path; [] when it is sound.

    integrity_check rather than quick_check: it also checks that every index
    matches its table. It runs on the private copy in a worker thread, once a
    day or on request, so its extra time on a large database costs nothing.
    """
    try:
        conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
        try:
            rows = [str(r[0]) for r in conn.execute("PRAGMA integrity_check(20)")]
        finally:
            conn.close()
    except sqlite3.Error as e:
        return [str(e) or type(e).__name__]
    return [] if rows == ["ok"] else rows


def record_check_failure(backup_dir: Path, reason: str) -> None:
    """Note a failed check for `copycord doctor`."""
    now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
    (Path(backup_dir) / CHECK_FAILED).write_text(
        json.dumps({"at": now_iso, "reason": reason}) + "\n", encoding="utf-8"
    )


def clear_check_failure(backup_dir: Path) -> None:
    with contextlib.suppress(OSError):
        (Path(backup_dir) / CHECK_FAILED).unlink()


def last_check_failure(backup_dir: Path) -> Optional[dict]:
    """{"at", "reason", "mtime"} of the last failed check, or None."""
    path = Path(backup_dir) / CHECK_FAILED
    try:
        mtime = path.stat().st_mtime
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        data = {}
    return {"at": str(data.get("at") or ""), "reason": str(data.get("reason") or ""),
            "mtime": mtime}


DB_FILE = "data.db"
KEY_FILE = "secret.key"
MANIFEST = "manifest.json"
# manifest.json "format"; an archive without a manifest holds only data.db.
ARCHIVE_FORMAT = 1


def _copycord_version() -> str:
    try:
        from copycord.common.config import CURRENT_VERSION

        return CURRENT_VERSION.lstrip("v")
    except Exception:
        return "unknown"


def _add_bytes(tar: tarfile.TarFile, name: str, data: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mtime = int(time.time())
    info.mode = 0o600
    tar.addfile(info, io.BytesIO(data))


def write_archive(db_copy: Path, dest: Path, key_path: Optional[Path] = None) -> list:
    """Write dest as a tar.gz of db_copy (as data.db), key_path (as secret.key)
    when it exists and manifest.json; owner-only on POSIX since it holds the key.
    Returns the names of the files in it."""
    from copycord.common.db import stored_schema_version

    files = [DB_FILE]
    key = None
    if key_path is not None:
        try:
            key = Path(key_path).read_bytes()
            files.append(KEY_FILE)
        except FileNotFoundError:
            pass
    try:
        schema = stored_schema_version(str(db_copy))
    except sqlite3.Error:
        schema = None
    manifest = {
        "format": ARCHIVE_FORMAT,
        "copycord_version": _copycord_version(),
        "schema_version": schema,
        "created_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z",
        "files": files,
    }
    fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0), 0o600)
    if os.name != "nt":
        os.fchmod(fd, 0o600)
    with os.fdopen(fd, "wb") as fh, tarfile.open(fileobj=fh, mode="w:gz") as tar:
        tar.add(str(db_copy), arcname=DB_FILE)
        if key is not None:
            _add_bytes(tar, KEY_FILE, key)
        _add_bytes(tar, MANIFEST, (json.dumps(manifest, indent=2) + "\n").encode("utf-8"))
    return files


def archive_names(archive: Path) -> list:
    """The names of the files in a backup archive."""
    with tarfile.open(archive, "r:*") as tar:
        return tar.getnames()


# pre-restore-<UTC timestamp>.tar.gz, written by `copycord restore`; never pruned.
PRE_RESTORE_PREFIX = "pre-restore-"


class ArchiveError(ValueError):
    """A file that is not a readable Copycord backup."""


def list_backups(backup_dir: Path) -> list:
    """Restorable backups in backup_dir (*.tar.gz archives and *.db copies), newest first."""
    d = Path(backup_dir)
    if not d.is_dir():
        return []
    found = []
    for pattern in ("*.tar.gz", "*.db"):
        for p in d.glob(pattern):
            if p.name.startswith("."):
                continue
            try:
                if p.is_file():
                    found.append((p.stat().st_mtime, p))
            except OSError:
                pass
    return [p for _, p in sorted(found, key=lambda t: t[0], reverse=True)]


@dataclass
class Unpacked:
    db: Path
    # None for an older archive or a .db copy.
    key: Optional[Path]
    # {} when the backup has no manifest.json.
    manifest: dict


def _write_private(dest: Path, src) -> None:
    fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0), 0o600)
    with os.fdopen(fd, "wb") as out:
        shutil.copyfileobj(src, out)


def unpack_backup(src: Path, into: Path) -> Unpacked:
    """Copy the database, secret.key and manifest.json of a backup into the folder into.

    src is a .tar.gz archive (older ones hold only data.db) or a plain .db copy.
    Members are read by name and written here, never extracted by their own path.
    """
    src, into = Path(src), Path(into)
    if src.name.endswith(".db"):
        with open(src, "rb") as fh:
            _write_private(into / DB_FILE, fh)
        return Unpacked(into / DB_FILE, None, {})
    found = {}
    try:
        with tarfile.open(src, "r:*") as tar:
            for member in tar.getmembers():
                name = posixpath.normpath(member.name)
                if name not in (DB_FILE, KEY_FILE, MANIFEST) or name in found or not member.isfile():
                    continue
                fh = tar.extractfile(member)
                if fh is None:
                    continue
                with fh:
                    _write_private(into / name, fh)
                found[name] = into / name
    except (tarfile.TarError, EOFError, zlib.error, gzip.BadGzipFile) as e:
        raise ArchiveError(f"{src.name} is not a readable backup archive ({e})") from None
    if DB_FILE not in found:
        raise ArchiveError(f"{src.name} has no {DB_FILE} in it")
    manifest = {}
    if MANIFEST in found:
        try:
            manifest = json.loads(found[MANIFEST].read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifest = {}
        if not isinstance(manifest, dict):
            manifest = {}
    return Unpacked(found[DB_FILE], found.get(KEY_FILE), manifest)


def snapshot_database(db_path: Path, dest: Path) -> None:
    """Copy db_path to dest with SQLite's online backup; a file SQLite cannot
    read is copied byte for byte, so even a damaged database is kept."""
    try:
        src = sqlite3.connect(str(db_path))
        try:
            out = sqlite3.connect(str(dest))
            try:
                src.backup(out)
            finally:
                out.close()
        finally:
            src.close()
    except sqlite3.DatabaseError:
        Path(dest).unlink(missing_ok=True)
        shutil.copyfile(db_path, dest)


def pre_restore_backup(db_path: Path, backup_dir: Path, key_path: Optional[Path]) -> Optional[Path]:
    """Archive the current database and key as pre-restore-<UTC timestamp>.tar.gz
    in backup_dir; None when there is no database."""
    db_path, backup_dir = Path(db_path), Path(backup_dir)
    if not db_path.is_file() or db_path.stat().st_size == 0:
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = backup_dir / f"{PRE_RESTORE_PREFIX}{stamp}.tar.gz"
    n = 1
    while dest.exists():
        dest = backup_dir / f"{PRE_RESTORE_PREFIX}{stamp}-{n}.tar.gz"
        n += 1
    tmp = backup_dir / f".{dest.name}.tmp"
    with tempfile.TemporaryDirectory() as work:
        copy = Path(work) / DB_FILE
        snapshot_database(db_path, copy)
        try:
            write_archive(copy, tmp, key_path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
    tmp.replace(dest)
    return dest


def _match_owner(new: Path, old: Path, mode: bool) -> None:
    """Give new the owner (when run as root) and, with mode, the permissions of old."""
    try:
        st = old.stat()
    except OSError:
        return
    if mode:
        os.chmod(new, stat.S_IMODE(st.st_mode))
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        with contextlib.suppress(OSError):
            os.chown(new, st.st_uid, st.st_gid)


def put_in_place(unpacked: Unpacked, db_path: Path, key_path: Path) -> tuple:
    """Move a checked backup over the database and, when it has one, the key.

    unpacked must be on the same drive as db_path, so each move is atomic.
    Returns (key result, where the previous key was kept or None); the result
    is "restored", "same" or "none" (the backup has no key).
    """
    db_path, key_path = Path(db_path), Path(key_path)
    _match_owner(unpacked.db, db_path, mode=True)
    if unpacked.key is not None:
        _match_owner(unpacked.key, key_path, mode=False)
    # A journal left by the old database would be applied to the restored one.
    for suffix in ("-wal", "-shm", "-journal"):
        Path(f"{db_path}{suffix}").unlink(missing_ok=True)
    os.replace(unpacked.db, db_path)
    if unpacked.key is None:
        return "none", None
    new = unpacked.key.read_bytes()
    try:
        old = key_path.read_bytes()
    except FileNotFoundError:
        old = None
    if old == new:
        return "same", None
    kept = None
    if old is not None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        kept = key_path.with_name(f"{key_path.name}.bak-{stamp}")
        os.replace(key_path, kept)
    os.replace(unpacked.key, key_path)
    return "restored", kept


def record_backup_stats(set_config: Callable[[str, str], None], archive_path: Path) -> None:
    """Store the DB_LAST_BACKUP_* app config values the dashboard shows."""
    size = archive_path.stat().st_size if archive_path.exists() else 0
    now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
    set_config("DB_LAST_BACKUP_AT", now_iso)
    set_config("DB_LAST_BACKUP_FILE", archive_path.name)
    set_config("DB_LAST_BACKUP_SIZE", str(size))


@dataclass
class BackupConfig:
    """Settings for the SQLite backup scheduler; run_at is HH:MM in ``timezone`` unless interval_minutes is set."""

    db_path: Path
    backup_dir: Path
    retain: int = 14
    run_at: str = "03:17"
    timezone: str = "UTC"
    interval_minutes: Optional[int] = None
    # secret.key, archived with the database; None means next to db_path.
    key_path: Optional[Path] = None


class DailySQLiteBackupScheduler:
    """Writes timestamped tar.gz snapshots of the SQLite DB daily or every N minutes and prunes to ``retain``."""

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

            except BackupCheckError:
                pass  # _backup_once has logged it
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
            now = datetime.now(timezone.utc).replace(tzinfo=None)
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
        return datetime.now(self._tz) if self._tz else datetime.now(timezone.utc).replace(tzinfo=None)

    def _unique_archive_path(self, when: datetime) -> Path:
        """Return an unused archive path, adding a numeric suffix on collision."""
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

            check_t0 = time.monotonic()
            problems = await asyncio.to_thread(integrity_problems, tmp_db_path)
            if problems:
                reason = "; ".join(problems[:3])
                self._warn("backup failed its integrity check, no archive kept: %s", reason)
                with contextlib.suppress(OSError):
                    record_check_failure(self.cfg.backup_dir, reason)
                raise BackupCheckError(reason)
            self._dbg("integrity check passed in %.2fs", time.monotonic() - check_t0)

            def _write_archive_once() -> None:
                self._dbg("opening tar for write: %s", tmp_tgz)
                key_path = self.cfg.key_path or Path(self.cfg.db_path).parent / KEY_FILE
                files = write_archive(tmp_db_path, tmp_tgz, key_path)
                self._dbg("archived: %s", ", ".join(files))
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

        clear_check_failure(self.cfg.backup_dir)
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
            (p for p in self.cfg.backup_dir.glob("*.tar.gz")
             if not p.name.startswith(PRE_RESTORE_PREFIX)),
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

    def _emit(self, level: str, msg: str, *args) -> None:
        # Use the caller's logger when given; fall back to the module logger, never print().
        if self.log:
            try:
                getattr(self.log, level)(msg, *args)
                return
            except Exception:
                _log.debug("backup: caller logger failed", exc_info=True)
        getattr(_log, level)(msg, *args)

    def _dbg(self, msg: str, *args) -> None:
        self._emit("debug", msg, *args)

    def _info(self, msg: str, *args) -> None:
        self._emit("info", msg, *args)

    def _warn(self, msg: str, *args) -> None:
        self._emit("warning", msg, *args)

    def _exception(self, msg: str, *args) -> None:
        self._emit("exception", msg, *args)
