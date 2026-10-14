# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""TaskService: start, cancel, list, recover and prune task runs.

Storage sits behind RunRepo (task_runs rows) and LogSink (per run log
files); both are Protocols so the SQLite and file implementations live with
the DB code and tests use the in-memory ones below."""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, Protocol

from pydantic import BaseModel, ValidationError

from . import registry
from .context import RunContext, TaskCancelled, iso
from .registry import TaskType

log = logging.getLogger("copycord.tasks")

RUN_STATES = ("queued", "running", "success", "failed", "cancelled")
ACTIVE = ("queued", "running")
FINAL = ("success", "failed", "cancelled")
CANCEL_GRACE = 30.0
INTERRUPTED = "Interrupted by restart"
KEEP_PER_TYPE = 500


def _ids_to_str(value: Any, key: str = "") -> Any:
    """Every id on the wire is a string: snowflakes pass 2**53."""
    is_id = key == "id" or key.endswith("_id")
    if isinstance(value, dict):
        return {k: _ids_to_str(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        inner = key if key.endswith("_ids") or key == "ids" else ""
        return [_ids_to_str(v, "id" if inner else "") for v in value]
    if is_id and isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return value


@dataclass
class TaskRun:
    type: str
    number: int
    status: str
    title: str
    trigger: str
    actor: str
    inputs: dict
    context: dict = field(default_factory=dict)
    id: Optional[int] = None
    steps: list = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    log_lines: int = 0
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    updated_at: Optional[str] = None
    error: Optional[str] = None
    seq: int = 0

    def progress(self) -> dict:
        """What goes in progress_json."""
        return {"steps": [dict(s) for s in self.steps], "summary": dict(self.summary),
                "log_lines": self.log_lines, "seq": self.seq}

    def load_progress(self, data: Optional[dict]) -> None:
        data = data or {}
        self.steps = [dict(s) for s in data.get("steps") or []]
        self.summary = dict(data.get("summary") or {})
        self.log_lines = int(data.get("log_lines") or 0)
        self.seq = int(data.get("seq") or 0)

    def to_wire(self, include_steps: bool = True, include_inputs: bool = True) -> dict:
        wire = {
            "id": None if self.id is None else str(self.id),
            "type": self.type,
            "number": self.number,
            "title": self.title,
            "context": _ids_to_str(self.context),
            "status": self.status,
            "trigger": self.trigger,
            "actor": self.actor,
            "inputs": _ids_to_str(self.inputs),
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "summary": _ids_to_str(self.summary),
            "log_lines": self.log_lines,
            "seq": self.seq,
        }
        if not include_inputs:
            del wire["inputs"]
        if include_steps:
            wire["steps"] = [dict(s) for s in self.steps]
        return wire


class RunRepo(Protocol):
    """task_runs storage. list_runs filters: type, status (str or list), actor,
    context (context label), q (title, context label or "#number"); newest id
    first; page is 1 based; returns (runs, total matching)."""

    def insert_run(self, run: TaskRun) -> int: ...
    def update_run(self, run: TaskRun) -> None: ...
    def get_run(self, run_id: int) -> Optional[TaskRun]: ...
    def list_runs(self, filters: dict, page: int = 1, per_page: int = 25) -> tuple[list[TaskRun], int]: ...
    def next_number(self, type_key: str) -> int: ...
    def delete_run(self, run_id: int) -> None: ...
    def list_types(self) -> list[str]: ...


class LogSink(Protocol):
    """Per run log. line is "seq<TAB>iso<TAB>level<TAB>step<TAB>msg" without a newline."""

    def append(self, run_id: int, line: str) -> None: ...
    def delete(self, run_id: int) -> None: ...


class RunFinished(Exception):
    """cancel() on a run that already ended."""

    def __init__(self, run: TaskRun):
        super().__init__(f"run {run.id} already {run.status}")
        self.run = run


class MemoryRunRepo:
    def __init__(self):
        self.rows: dict[int, TaskRun] = {}
        self.writes = 0
        self._next_id = 1

    def insert_run(self, run: TaskRun) -> int:
        run_id = self._next_id
        self._next_id += 1
        row = copy.deepcopy(run)
        row.id = run_id
        self.rows[run_id] = row
        return run_id

    def update_run(self, run: TaskRun) -> None:
        self.writes += 1
        self.rows[run.id] = copy.deepcopy(run)

    def get_run(self, run_id: int) -> Optional[TaskRun]:
        row = self.rows.get(int(run_id))
        return copy.deepcopy(row) if row else None

    def list_runs(self, filters: Optional[dict] = None, page: int = 1, per_page: int = 25):
        f = filters or {}
        statuses = f.get("status")
        if isinstance(statuses, str):
            statuses = [statuses]
        q = (f.get("q") or "").strip().lower()

        def match(r: TaskRun) -> bool:
            label = str((r.context or {}).get("label") or "")
            if f.get("type") and r.type != f["type"]:
                return False
            if statuses and r.status not in statuses:
                return False
            if f.get("actor") and r.actor != f["actor"]:
                return False
            if f.get("context") and label != f["context"]:
                return False
            if q and q not in r.title.lower() and q not in label.lower() and q != f"#{r.number}":
                return False
            return True

        hits = sorted((r for r in self.rows.values() if match(r)), key=lambda r: r.id, reverse=True)
        start = (max(1, page) - 1) * per_page
        return [copy.deepcopy(r) for r in hits[start:start + per_page]], len(hits)

    def next_number(self, type_key: str) -> int:
        return 1 + max((r.number for r in self.rows.values() if r.type == type_key), default=0)

    def delete_run(self, run_id: int) -> None:
        self.rows.pop(int(run_id), None)

    def list_types(self) -> list[str]:
        return sorted({r.type for r in self.rows.values()})


class MemoryLogSink:
    def __init__(self):
        self.lines: dict[int, list[str]] = {}

    def append(self, run_id: int, line: str) -> None:
        self.lines.setdefault(run_id, []).append(line)

    def delete(self, run_id: int) -> None:
        self.lines.pop(run_id, None)


def _input_errors(exc: ValidationError) -> dict:
    errors: dict[str, str] = {}
    for err in exc.errors():
        name = ".".join(str(p) for p in err.get("loc") or ()) or "inputs"
        errors.setdefault(name, err.get("msg") or "Invalid value")
    return errors


async def _noop_publish(event: dict) -> None:
    return None


class TaskService:
    def __init__(
        self,
        repo: RunRepo,
        log_sink: LogSink,
        publish: Optional[Callable[[dict], Awaitable[None]]] = None,
        clock: Callable[[], float] = time.time,
        lookups: Any = None,
        cancel_grace: float = CANCEL_GRACE,
    ):
        self.repo = repo
        self.log_sink = log_sink
        self._publish = publish or _noop_publish
        self._clock = clock
        self._lookups = lookups
        self._grace = cancel_grace
        self._active: dict[int, tuple[RunContext, asyncio.Task]] = {}
        self._watchdogs: dict[int, asyncio.Task] = {}
        self._slots: dict[str, asyncio.Semaphore] = {}

    # starting

    async def start(self, type_key: str, raw_inputs: Optional[dict], actor: str = "admin",
                    trigger: str = "manual") -> TaskRun:
        """Validate, insert a queued row and schedule the run. Bad inputs raise
        ValueError({field: message})."""
        task_type = registry.get(type_key)
        if task_type is None:
            raise ValueError({"type": f"Unknown task type {type_key!r}"})
        try:
            inputs = task_type.inputs.model_validate(raw_inputs or {})
        except ValidationError as exc:
            raise ValueError(_input_errors(exc)) from None
        title, context = task_type.describe_run(inputs, self._lookups)
        stamp = iso(self._clock())
        run = TaskRun(
            type=task_type.key, number=self.repo.next_number(task_type.key), status="queued",
            title=title, trigger=trigger, actor=actor, inputs=inputs.model_dump(mode="json"),
            context=context, created_at=stamp, updated_at=stamp,
        )
        run.id = self.repo.insert_run(run)
        ctx = RunContext(run, self.repo, self.log_sink, self._publish, self._clock)
        await ctx.flush()
        task = asyncio.ensure_future(self._execute(task_type, ctx, inputs))
        self._active[run.id] = (ctx, task)
        return run

    def _slot(self, task_type: TaskType) -> Optional[asyncio.Semaphore]:
        if task_type.concurrency is None:
            return None
        slot = self._slots.get(task_type.key)
        if slot is None:
            slot = self._slots[task_type.key] = asyncio.Semaphore(task_type.concurrency)
        return slot

    @staticmethod
    async def _acquire(slot: asyncio.Semaphore, cancelled: asyncio.Event) -> bool:
        """Wait for a concurrency slot; False when the run is cancelled first."""
        if cancelled.is_set():
            return False
        acquire = asyncio.ensure_future(slot.acquire())
        waiter = asyncio.ensure_future(cancelled.wait())
        try:
            await asyncio.wait({acquire, waiter}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            waiter.cancel()
        if acquire.done() and not cancelled.is_set():
            return True
        acquire.cancel()
        try:
            await acquire
        except asyncio.CancelledError:
            return False
        slot.release()
        return False

    async def _execute(self, task_type: TaskType, ctx: RunContext, inputs: BaseModel) -> None:
        run = ctx.run
        slot = self._slot(task_type)
        acquired = False
        try:
            if slot is not None:
                acquired = await self._acquire(slot, ctx.cancelled)
                if not acquired:
                    await self._finish(ctx, "cancelled", None)
                    return
            if ctx.cancelled.is_set():
                await self._finish(ctx, "cancelled", None)
                return
            run.status = "running"
            run.started_at = iso(self._clock())
            await ctx.flush()
            outcome, error = "success", None
            try:
                await task_type.run(ctx, inputs)
                if ctx.failure is not None:
                    outcome, error = "failed", ctx.failure
                elif ctx.cancelled.is_set():
                    outcome = "cancelled"
            except TaskCancelled:
                outcome = "cancelled"
            except asyncio.CancelledError:
                outcome = "cancelled"
            except Exception as exc:
                log.exception("task run %s (%s #%s) failed", run.id, run.type, run.number)
                outcome, error = "failed", str(exc) or type(exc).__name__
            await self._finish(ctx, outcome, error)
        finally:
            if acquired:
                slot.release()
            self._active.pop(run.id, None)
            ctx.close()

    async def _finish(self, ctx: RunContext, outcome: str, error: Optional[str]) -> None:
        run = ctx.run
        if run.status in FINAL:
            return
        if outcome != "success":
            ctx._settle_steps(outcome)
        run.status = outcome
        run.error = error
        run.finished_at = iso(self._clock())
        await ctx.flush()

    # cancelling

    async def cancel(self, run_id: Any) -> TaskRun:
        """Ask the runner to stop; a run still going after the grace period is
        cancelled by force. KeyError for an unknown run, RunFinished if it ended."""
        run_id = int(run_id)
        active = self._active.get(run_id)
        if active is None:
            run = self.repo.get_run(run_id)
            if run is None:
                raise KeyError(run_id)
            raise RunFinished(run)
        ctx, task = active
        if not ctx.cancelled.is_set():
            ctx.cancelled.set()
            if ctx.run.status == "running":
                await ctx.log("Cancel requested", level="warning")
                self._watchdogs[run_id] = asyncio.ensure_future(self._grace_period(ctx, task))
        return ctx.run

    async def _grace_period(self, ctx: RunContext, task: asyncio.Task) -> None:
        try:
            done, _ = await asyncio.wait({task}, timeout=self._grace)
            if done:
                return
            task.cancel()
            await asyncio.wait({task}, timeout=1.0)
            if ctx.run.status not in FINAL:
                await ctx.log(f"Runner did not stop within {self._grace:g} s; marked cancelled",
                              level="warning")
                await self._finish(ctx, "cancelled", None)
        finally:
            self._watchdogs.pop(ctx.run.id, None)

    # reading

    def get(self, run_id: Any) -> Optional[TaskRun]:
        run_id = int(run_id)
        active = self._active.get(run_id)
        if active is not None:
            return active[0].run
        return self.repo.get_run(run_id)

    def list(self, filters: Optional[dict] = None, page: int = 1,
             per_page: int = 25) -> tuple[list[TaskRun], int]:
        runs, total = self.repo.list_runs(filters or {}, page, per_page)
        live = [self._active[r.id][0].run if r.id in self._active else r for r in runs]
        return live, total

    def active_runs(self, type_key: Optional[str] = None) -> list[TaskRun]:
        return [ctx.run for ctx, _ in self._active.values()
                if type_key is None or ctx.run.type == type_key]

    # housekeeping

    def recover(self) -> int:
        """Boot: queued and running rows left by a dead process become failed."""
        stamp = iso(self._clock())
        fixed = 0
        while True:
            runs, _ = self.repo.list_runs({"status": list(ACTIVE)}, 1, 200)
            stale = [r for r in runs if r.id not in self._active]
            if not stale:
                return fixed
            for run in stale:
                for step in run.steps:
                    if step.get("state") in ("queued", "running"):
                        step.update(state="failed", label=None, queue_position=None, finished_at=stamp)
                run.status = "failed"
                run.error = INTERRUPTED
                run.finished_at = stamp
                run.updated_at = stamp
                run.seq += 1
                self.repo.update_run(run)
                fixed += 1

    def prune(self, keep: int = KEEP_PER_TYPE) -> int:
        """Keep the newest `keep` runs per type; older rows and their logs go."""
        if keep < 1:
            raise ValueError("keep must be at least 1")
        removed = 0
        for type_key in self.repo.list_types():
            while True:
                older, _ = self.repo.list_runs({"type": type_key}, 2, keep)
                victims = [r for r in older if r.id not in self._active]
                if not victims:
                    break
                for run in victims:
                    self.repo.delete_run(run.id)
                    try:
                        self.log_sink.delete(run.id)
                    except Exception:
                        log.exception("task run %s: log delete failed", run.id)
                    removed += 1
        return removed

    async def shutdown(self) -> None:
        """Stop every active run (app shutdown); recover() marks them on next boot otherwise."""
        tasks = [task for _, task in self._active.values()] + list(self._watchdogs.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


__all__ = (
    "TaskRun", "RunRepo", "LogSink", "RunFinished", "MemoryRunRepo", "MemoryLogSink",
    "TaskService", "TaskCancelled", "INTERRUPTED", "CANCEL_GRACE", "KEEP_PER_TYPE",
)
