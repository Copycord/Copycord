# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""TaskService: start, cancel, list and recover task runs.

Storage sits behind RunRepo (task_runs rows), a Protocol so the SQLite
implementation lives with the DB code and tests use the in-memory one below.
The run's short log rides in progress_json (newest LOG_KEEP lines); nothing
is pruned, the user deletes runs."""

from __future__ import annotations

import asyncio
import copy
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, Protocol

from pydantic import BaseModel, ValidationError

from . import registry
from .context import LOG_KEEP, RunContext, TaskCancelled, iso
from .registry import TaskType

log = logging.getLogger("copycord.tasks")

RUN_STATES = ("queued", "running", "success", "failed", "cancelled")
ACTIVE = ("queued", "running")
FINAL = ("success", "failed", "cancelled")
CANCEL_GRACE = 30.0
INTERRUPTED = "Interrupted by restart"
BOTS_STOPPED = "Bots stopped"
# App shutdown: how long stopped runners get to write their report before they are cancelled.
SHUTDOWN_WAIT = 1.0


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
    id: Optional[str] = None
    steps: list = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    log_lines: list = field(default_factory=list)
    log_count: int = 0
    report: Optional[dict] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    updated_at: Optional[str] = None
    error: Optional[str] = None
    seq: int = 0
    # Queued behind a concurrency slot: the oldest running run of the type holding it, and this run's
    # place in line when more than one waits. Live only (the service sets them), never stored.
    waiting_for: Optional[dict] = None
    queue_position: Optional[int] = None

    def progress(self) -> dict:
        """What goes in progress_json."""
        return {"steps": [dict(s) for s in self.steps], "summary": dict(self.summary),
                "log_lines": [dict(ln) for ln in self.log_lines], "log_count": self.log_count,
                "seq": self.seq}

    def load_progress(self, data: Optional[dict]) -> None:
        data = data or {}
        self.steps = [dict(s) for s in data.get("steps") or []]
        self.summary = dict(data.get("summary") or {})
        lines = data.get("log_lines")
        self.log_lines = [dict(ln) for ln in lines if isinstance(ln, dict)] if isinstance(lines, list) else []
        self.log_count = int(data.get("log_count") or len(self.log_lines))
        self.seq = int(data.get("seq") or 0)

    def to_wire(self, include_steps: bool = True, include_inputs: bool = True,
                detail: bool = False) -> dict:
        """List rows and events carry log_count; detail adds log_lines and report."""
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
            # List rows and events leave the steps out; the count still says how many the run has.
            "step_count": len(self.steps),
            "log_count": self.log_count,
            "seq": self.seq,
            "waiting_for": None if self.waiting_for is None else dict(self.waiting_for),
            "queue_position": self.queue_position,
        }
        if not include_inputs:
            del wire["inputs"]
        if include_steps:
            wire["steps"] = [dict(s) for s in self.steps]
        if detail:
            wire["log_lines"] = [dict(ln) for ln in self.log_lines]
            wire["report"] = None if self.report is None else _ids_to_str(self.report)
        return wire


class RunRepo(Protocol):
    """task_runs storage. list_runs filters: type, status (str or list), actor,
    context (context label), q (title, context label or "#number"); newest created
    first; page is 1 based; returns (runs, total matching)."""

    def insert_run(self, run: TaskRun) -> str: ...
    def update_run(self, run: TaskRun) -> None: ...
    def get_run(self, run_id: str) -> Optional[TaskRun]: ...
    def list_runs(self, filters: dict, page: int = 1, per_page: int = 25) -> tuple[list[TaskRun], int]: ...
    def next_number(self, type_key: str) -> int: ...
    def delete_run(self, run_id: str) -> None: ...
    def list_types(self) -> list[str]: ...


class RunFinished(Exception):
    """cancel() on a run that already ended."""

    def __init__(self, run: TaskRun):
        super().__init__(f"run {run.id} already {run.status}")
        self.run = run


class MemoryRunRepo:
    def __init__(self):
        self.rows: dict[str, TaskRun] = {}
        self.writes = 0
        # insert order breaks created_at ties, as rowid does in SQLite
        self._order: dict[str, int] = {}

    def insert_run(self, run: TaskRun) -> str:
        run_id = uuid.uuid4().hex
        row = copy.deepcopy(run)
        row.id = run_id
        self.rows[run_id] = row
        self._order[run_id] = len(self._order)
        return run_id

    def update_run(self, run: TaskRun) -> None:
        self.writes += 1
        self.rows[run.id] = copy.deepcopy(run)

    def get_run(self, run_id: str) -> Optional[TaskRun]:
        row = self.rows.get(str(run_id))
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

        hits = sorted((r for r in self.rows.values() if match(r)),
                      key=lambda r: (r.created_at or "", self._order.get(r.id, 0)), reverse=True)
        start = (max(1, page) - 1) * per_page
        return [copy.deepcopy(r) for r in hits[start:start + per_page]], len(hits)

    def next_number(self, type_key: str) -> int:
        return 1 + max((r.number for r in self.rows.values() if r.type == type_key), default=0)

    def delete_run(self, run_id: str) -> None:
        self.rows.pop(str(run_id), None)

    def list_types(self) -> list[str]:
        return sorted({r.type for r in self.rows.values()})


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
        publish: Optional[Callable[[dict], Awaitable[None]]] = None,
        clock: Callable[[], float] = time.time,
        lookups: Any = None,
        cancel_grace: float = CANCEL_GRACE,
    ):
        self.repo = repo
        self._publish = publish or _noop_publish
        self._clock = clock
        self._lookups = lookups
        self._grace = cancel_grace
        self._active: dict[str, tuple[RunContext, asyncio.Task]] = {}
        self._watchdogs: dict[str, asyncio.Task] = {}
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
        ctx = RunContext(run, self.repo, self._publish, self._clock)
        await ctx.flush()
        task = asyncio.ensure_future(self._execute(task_type, ctx, inputs))
        self._active[run.id] = (ctx, task)
        await self._refresh_queue(task_type)
        return run

    async def _refresh_queue(self, task_type: TaskType) -> None:
        """Queued runs of a type whose slots are all taken say which run they wait for and, when more
        than one waits, their place in line; a change publishes. While a slot is free the next run
        is about to start, so nothing changes (no flicker between the blocker ending and it starting)."""
        if task_type.concurrency is None:
            return
        mine = [ctx for ctx, _ in self._active.values() if ctx.run.type == task_type.key]
        running = sorted((c.run for c in mine if c.run.status == "running"),
                         key=lambda r: (r.started_at or "", r.number))
        if len(running) < task_type.concurrency:
            return
        blocker = running[0]
        waiting_for = {"id": str(blocker.id), "number": blocker.number, "type": blocker.type,
                       "title": blocker.title}
        queued = sorted((c for c in mine if c.run.status == "queued" and not c.cancelled.is_set()),
                        key=lambda c: c.run.number)
        for i, ctx in enumerate(queued):
            position = i + 1 if len(queued) > 1 else None
            if ctx.run.waiting_for != waiting_for or ctx.run.queue_position != position:
                ctx.run.waiting_for = waiting_for
                ctx.run.queue_position = position
                await ctx.flush()

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
            run.waiting_for = None
            run.queue_position = None
            await ctx.flush()
            await self._refresh_queue(task_type)
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
            await self._refresh_queue(task_type)

    async def _finish(self, ctx: RunContext, outcome: str, error: Optional[str],
                      note: Optional[str] = None) -> None:
        run = ctx.run
        if run.status in FINAL:
            # Ended early (stop_all): what the runner wrote since, its report, still lands.
            if ctx._write_pending:
                await ctx.flush()
            return
        if outcome != "success":
            ctx._settle_steps(outcome, note)
        run.status = outcome
        run.error = error
        run.waiting_for = None
        run.queue_position = None
        run.finished_at = iso(self._clock())
        await ctx.flush()

    # cancelling

    async def cancel(self, run_id: Any) -> TaskRun:
        """Ask the runner to stop; a run still going after the grace period is
        cancelled by force. KeyError for an unknown run, RunFinished if it ended."""
        run_id = str(run_id)
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

    async def stop_all(self, note: str = BOTS_STOPPED, bots_only: bool = True) -> list[TaskRun]:
        """The bots went down: every queued and running run of a type that needs them ends
        cancelled now, its open steps noted, instead of waiting on frames that will never come.
        Runs of a type with requires_bots=False (a scrape) go on, unless bots_only=False
        (app shutdown) ends them too. Each runner still sees the
        cancel and unwinds (a report it writes lands on the ended run); one still going after
        the grace period is cancelled by force."""
        stopped: list[TaskRun] = []
        for run_id, (ctx, task) in list(self._active.items()):
            if ctx.run.status in FINAL:
                continue
            task_type = registry.get(ctx.run.type)
            if bots_only and task_type is not None and not task_type.requires_bots:
                continue
            ctx.cancel_note = note
            ctx.cancelled.set()
            await ctx.log(note, level="warning")
            await self._finish(ctx, "cancelled", None, note)
            if run_id not in self._watchdogs and not task.done():
                self._watchdogs[run_id] = asyncio.ensure_future(self._grace_period(ctx, task))
            stopped.append(ctx.run)
        return stopped

    # reading

    def get(self, run_id: Any) -> Optional[TaskRun]:
        run_id = str(run_id)
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

    async def shutdown(self) -> None:
        """App shutdown: the bots go with it, so every active run ends cancelled (stop_all), its
        runner gets SHUTDOWN_WAIT to finish and is then cancelled."""
        await self.stop_all(bots_only=False)
        runners = [task for _, task in self._active.values()]
        if runners:
            await asyncio.wait(runners, timeout=SHUTDOWN_WAIT)
        tasks = [task for _, task in self._active.values()] + list(self._watchdogs.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


__all__ = (
    "TaskRun", "RunRepo", "RunFinished", "MemoryRunRepo",
    "TaskService", "TaskCancelled", "INTERRUPTED", "BOTS_STOPPED", "CANCEL_GRACE", "LOG_KEEP",
)
