"""SQLite task-run repository: the RunRepo TaskService expects, over the
DBManager task_runs methods. TaskRun in, TaskRun out; ids are uuid hex
strings end to end."""

from __future__ import annotations

import json
from typing import Optional

from copycord.common.db import DBManager

from .service import TaskRun


def _loads(text, default):
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


def _row_fields(run: TaskRun) -> dict:
    """Columns written on insert and on every update (id, type and number are fixed)."""
    return {
        "status": run.status,
        "title": run.title,
        "context_json": dict(run.context or {}),
        "trigger": run.trigger,
        "actor": run.actor,
        "inputs_json": dict(run.inputs or {}),
        "progress_json": run.progress(),
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "updated_at": run.updated_at,
        "error": run.error,
        "report_json": run.report,
    }


def run_from_row(row: dict) -> TaskRun:
    run = TaskRun(
        type=row["type"],
        number=int(row["number"]),
        status=row["status"],
        title=row.get("title") or "",
        trigger=row.get("trigger") or "manual",
        actor=row.get("actor") or "admin",
        inputs=_loads(row.get("inputs_json"), {}),
        context=_loads(row.get("context_json"), {}) or {},
        id=str(row["id"]),
        created_at=row.get("created_at"),
        started_at=row.get("started_at"),
        finished_at=row.get("finished_at"),
        updated_at=row.get("updated_at"),
        error=row.get("error"),
    )
    run.load_progress(_loads(row.get("progress_json"), {}))
    report = _loads(row.get("report_json"), None)
    run.report = report if isinstance(report, dict) else None
    return run


class SqliteTaskRepo:
    def __init__(self, db: DBManager):
        self.db = db

    def insert_run(self, run: TaskRun) -> str:
        """Insert and return the id. The DB assigns number atomically (MAX+1 in
        the INSERT); run.number is set to what was stored."""
        fields = {k: v for k, v in _row_fields(run).items() if v is not None}
        fields["type"] = run.type
        run_id = self.db.task_run_insert(fields)
        row = self.db.task_run_get(run_id)
        if row is not None:
            run.number = int(row["number"])
        return run_id

    def update_run(self, run: TaskRun) -> None:
        fields = _row_fields(run)
        if not fields["created_at"]:
            fields.pop("created_at")
        if not fields["updated_at"]:
            fields.pop("updated_at")
        self.db.task_run_update(str(run.id), **fields)

    def get_run(self, run_id: str) -> Optional[TaskRun]:
        row = self.db.task_run_get(str(run_id))
        return run_from_row(row) if row else None

    def list_runs(self, filters: Optional[dict] = None, page: int = 1,
                  per_page: int = 25) -> tuple[list[TaskRun], int]:
        f = filters or {}
        rows, total = self.db.task_runs_list(
            type=f.get("type") or None,
            status=f.get("status") or None,
            actor=f.get("actor") or None,
            context_label=f.get("context") or None,
            q=f.get("q") or None,
            page=page,
            per_page=per_page,
        )
        return [run_from_row(r) for r in rows], total

    def next_number(self, type_key: str) -> int:
        return self.db.task_run_next_number(type_key)

    def delete_run(self, run_id: str) -> None:
        self.db.task_run_delete(str(run_id))

    def list_types(self) -> list[str]:
        return self.db.task_runs_types()

    def actors(self) -> list[str]:
        return self.db.task_runs_actors()

    def contexts(self, task_type: Optional[str] = None) -> list[str]:
        return self.db.task_runs_contexts(task_type)
