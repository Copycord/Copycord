"""Append-only log file per task run, kept out of SQLite so live logging never
contends with message writes.

One line per entry: "seq<TAB>iso<TAB>level<TAB>step<TAB>msg" in
<data dir>/task-logs/<run id>.log, capped per run; past the cap one final
"log truncated" line is written and later lines are dropped.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import threading
from typing import Optional, Union

from copycord.common.paths import data_dir

LOG_CAP_BYTES = 5 * 1024 * 1024
TRUNCATED_MSG = "log truncated"


def default_log_dir() -> Path:
    """<data dir>/task-logs, next to data.db and backups/."""
    return data_dir() / "task-logs"


def _clean(value) -> str:
    """One field of a line: no tabs or newlines."""
    text = "" if value is None else str(value)
    return text.replace("\r", " ").replace("\n", " ").replace("\t", " ")


def _iso(ts: Union[datetime, str, None]) -> str:
    if ts is None:
        ts = datetime.now(timezone.utc)
    if isinstance(ts, datetime):
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        return ts.isoformat() + "Z"
    return _clean(ts)


class LogStore:
    def __init__(self, directory: Union[str, Path, None] = None, cap_bytes: int = LOG_CAP_BYTES):
        self.dir = Path(directory) if directory is not None else default_log_dir()
        self.cap = int(cap_bytes)
        self._lock = threading.Lock()
        self._truncated: set[int] = set()

    def path(self, run_id: int) -> Path:
        return self.dir / f"{int(run_id)}.log"

    def _ends_truncated(self, path: Path) -> bool:
        """True when the file already ends with the truncation line (survives a restart)."""
        with path.open("rb") as f:
            f.seek(max(0, path.stat().st_size - 256))
            tail = f.read().decode("utf-8", "replace").rstrip("\n")
        return tail.endswith("\t" + TRUNCATED_MSG)

    def append(self, run_id: int, seq: int, ts, level: str, step: Optional[str], msg) -> bool:
        """Write one line; False when the run's log is past the cap and the line was dropped."""
        run_id = int(run_id)
        line = "\t".join(
            (str(int(seq)), _iso(ts), _clean(level or "info"), _clean(step), _clean(msg))
        ) + "\n"
        data = line.encode("utf-8")
        with self._lock:
            if run_id in self._truncated:
                return False
            path = self.path(run_id)
            self.dir.mkdir(parents=True, exist_ok=True)
            size = path.stat().st_size if path.exists() else 0
            if size + len(data) > self.cap:
                if size and self._ends_truncated(path):
                    self._truncated.add(run_id)
                    return False
                marker = "\t".join((str(int(seq)), _iso(ts), "warn", "", TRUNCATED_MSG)) + "\n"
                with path.open("ab") as f:
                    f.write(marker.encode("utf-8"))
                self._truncated.add(run_id)
                return False
            with path.open("ab") as f:
                f.write(data)
            return True

    def read(
        self,
        run_id: int,
        after: int = 0,
        step: Optional[str] = None,
        limit: int = 500,
    ) -> tuple[list[dict], int, bool]:
        """Lines with seq > after (and of one step when given), at most limit.
        Returns (lines, next, complete): next is the seq to pass as after for the
        following page, complete is True when nothing past it matched yet."""
        limit = max(1, int(limit))
        after = int(after or 0)
        path = self.path(run_id)
        lines: list[dict] = []
        nxt = after
        if not path.exists():
            return lines, nxt, True
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                parts = raw.rstrip("\n").split("\t", 4)
                if len(parts) != 5:
                    continue
                try:
                    seq = int(parts[0])
                except ValueError:
                    continue
                if seq <= after:
                    continue
                line_step = parts[3] or None
                is_marker = parts[4] == TRUNCATED_MSG and line_step is None
                if step is not None and line_step != step and not is_marker:
                    continue
                if len(lines) >= limit:
                    return lines, nxt, False
                lines.append(
                    {"seq": seq, "ts": parts[1], "level": parts[2], "step": line_step, "msg": parts[4]}
                )
                nxt = seq
        return lines, nxt, True

    def delete(self, run_id: int) -> None:
        with self._lock:
            self._truncated.discard(int(run_id))
            try:
                os.unlink(self.path(run_id))
            except FileNotFoundError:
                pass
