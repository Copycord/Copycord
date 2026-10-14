# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""RunContext: what a task runner gets to log, report steps and see cancels.

Every change bumps the run's seq. Transitions (run status, step state or
label) write the repo and publish task.run.updated at once; progress, logs and
summary write at most once a second and publish at most every 500 ms, with a
trailing publish so the last value always goes out."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

if TYPE_CHECKING:
    from .service import RunRepo, TaskRun

log = logging.getLogger("copycord.tasks")

WRITE_INTERVAL = 1.0
LOG_KEEP = 500
PUBLISH_INTERVAL = 0.5
STEP_STATES = ("queued", "running", "success", "failed", "skipped", "cancelled")
STEP_FINAL = ("success", "failed", "skipped", "cancelled")


class TaskCancelled(Exception):
    """Raised by ctx.check_cancelled() once a cancel was requested."""


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _clean(text: Any) -> str:
    return str(text).replace("\t", " ").replace("\r", " ").replace("\n", " ")


class Step:
    def __init__(self, ctx: "RunContext", key: str, title: str):
        self._ctx = ctx
        self.key = key
        self.data: dict = {
            "key": key, "title": title, "state": "queued", "label": None,
            "done": None, "total": None, "queue_position": None,
            "started_at": None, "finished_at": None, "note": None,
        }

    async def queued(self, position: Optional[int] = None) -> None:
        transition = self.data["state"] != "queued"
        self.data.update(state="queued", queue_position=position)
        await self._ctx._changed(transition, self.key)

    async def start(self) -> None:
        self.data.update(state="running", queue_position=None,
                         started_at=iso(self._ctx._clock()))
        await self._ctx._changed(True, self.key)

    async def progress(self, done: int, total: Optional[int] = None) -> None:
        self.data["done"] = done
        if total is not None:
            self.data["total"] = total
        await self._ctx._changed(False, self.key)

    async def state(self, label: Optional[str]) -> None:
        transition = self.data["label"] != label
        self.data["label"] = label
        await self._ctx._changed(transition, self.key)

    async def finish(self, outcome: str = "success", note: Optional[str] = None) -> None:
        if outcome not in STEP_FINAL:
            raise ValueError(f"step outcome must be one of {STEP_FINAL}")
        self.data.update(state=outcome, note=note, label=None, queue_position=None,
                         finished_at=iso(self._ctx._clock()))
        await self._ctx._changed(True, self.key)


class RunContext:
    def __init__(
        self,
        run: "TaskRun",
        repo: "RunRepo",
        publish: Callable[[dict], Awaitable[None]],
        clock: Callable[[], float],
    ):
        self.run = run
        self._repo = repo
        self._publish = publish
        self._clock = clock
        self.cancelled = asyncio.Event()
        self.failure: Optional[str] = None
        self._steps: dict[str, Step] = {}
        self._dirty_steps: set[str] = set()
        self._last_write = float("-inf")
        self._last_publish = float("-inf")
        self._write_pending = False
        self._trailing: Optional[asyncio.Task] = None
        for data in run.steps:
            step = Step(self, data["key"], data.get("title", data["key"]))
            step.data.update(data)
            self._steps[step.key] = step
        run.steps = [s.data for s in self._steps.values()]

    # runner API

    async def log(self, msg: Any, level: str = "info", step: Optional[str] = None) -> None:
        """Append a friendly line to progress_json.log_lines; the newest LOG_KEEP stay."""
        self.run.log_count += 1
        self.run.log_lines.append({
            "n": self.run.log_count, "ts": iso(self._clock()), "level": _clean(level),
            "step": step or None, "msg": _clean(msg),
        })
        del self.run.log_lines[:-LOG_KEEP]
        await self._changed(False)

    def step(self, key: str, title: Optional[str] = None) -> Step:
        existing = self._steps.get(key)
        if existing is not None:
            if title:
                existing.data["title"] = title
            return existing
        step = Step(self, key, title or key)
        self._steps[key] = step
        self.run.steps.append(step.data)
        self._dirty_steps.add(key)
        return step

    async def summary(self, **counts: Any) -> None:
        self.run.summary.update(counts)
        await self._changed(False)

    async def report(self, data: Optional[dict] = None, /, **fields: Any) -> None:
        """Set the run's JSON report (report_json): ctx.report({...}) or ctx.report(k=v)."""
        report = dict(data or {})
        report.update(fields)
        self.run.report = report
        await self._changed(False)

    def check_cancelled(self) -> None:
        if self.cancelled.is_set():
            raise TaskCancelled()

    def fail(self, msg: str) -> None:
        """End the run as failed once the runner returns, without raising."""
        self.failure = str(msg)

    # service API

    def steps_dict(self) -> list[dict]:
        return [s.data for s in self._steps.values()]

    async def _changed(self, transition: bool, step_key: Optional[str] = None) -> None:
        if step_key is not None:
            self._dirty_steps.add(step_key)
        now = self._clock()
        self.run.seq += 1
        self.run.updated_at = iso(now)
        self._write_pending = True
        if transition or now - self._last_write >= WRITE_INTERVAL:
            self._write(now)
        if transition or now - self._last_publish >= PUBLISH_INTERVAL:
            await self._emit(now)
        elif self._trailing is None or self._trailing.done():
            delay = max(0.0, PUBLISH_INTERVAL - (now - self._last_publish))
            self._trailing = asyncio.ensure_future(self._trailing_flush(delay))

    async def flush(self) -> None:
        """Write and publish now (run status transitions, end of run)."""
        now = self._clock()
        self.run.seq += 1
        self.run.updated_at = iso(now)
        self._write(now)
        await self._emit(now)

    def _settle_steps(self, state: str) -> None:
        """Steps still queued or running when the run ends take the run's outcome."""
        stamp = iso(self._clock())
        for step in self._steps.values():
            if step.data["state"] in ("queued", "running"):
                step.data.update(state=state, label=None, queue_position=None, finished_at=stamp)
                self._dirty_steps.add(step.key)

    def close(self) -> None:
        if self._trailing is not None and not self._trailing.done():
            self._trailing.cancel()
        self._trailing = None

    async def _trailing_flush(self, delay: float) -> None:
        await asyncio.sleep(delay)
        now = self._clock()
        if self._write_pending and now - self._last_write >= WRITE_INTERVAL:
            self._write(now)
        await self._emit(now)

    def _write(self, now: float) -> None:
        self._last_write = now
        self._write_pending = False
        try:
            self._repo.update_run(self.run)
        except Exception:
            log.exception("task run %s: repo write failed", self.run.id)

    async def _emit(self, now: float) -> None:
        self._last_publish = now
        if self._trailing is not None and self._trailing is not asyncio.current_task():
            self._trailing.cancel()
            self._trailing = None
        steps = [self._steps[k].data for k in self._dirty_steps if k in self._steps]
        self._dirty_steps.clear()
        event = {
            "type": "task.run.updated",
            "run": self.run.to_wire(include_steps=False, include_inputs=False),
            "steps": [dict(s) for s in steps],
        }
        try:
            await self._publish(event)
        except Exception:
            log.exception("task run %s: publish failed", self.run.id)
