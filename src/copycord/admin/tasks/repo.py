"""SQLite task-run repository: a thin adapter over the DBManager task_runs
methods, shaped for TaskService's repository interface. Ids are ints here;
the wire layer turns them into strings."""

from __future__ import annotations

from typing import Optional

from copycord.common.db import DBManager


class SqliteTaskRepo:
    def __init__(self, db: DBManager):
        self.db = db

    def insert_run(self, fields: dict) -> int:
        return self.db.task_run_insert(fields)

    def update_run(self, run_id: int, **fields) -> bool:
        return self.db.task_run_update(run_id, **fields)

    def get_run(self, run_id: int) -> Optional[dict]:
        return self.db.task_run_get(run_id)

    def list_runs(self, **filters) -> tuple[list[dict], int]:
        return self.db.task_runs_list(**filters)

    def next_number(self, task_type: str) -> int:
        return self.db.task_run_next_number(task_type)

    def recover(self) -> list[int]:
        return self.db.task_runs_recover()

    def prune(self, keep_per_type: int = 500) -> list[int]:
        return self.db.task_runs_prune(keep_per_type)

    def actors(self) -> list[str]:
        return self.db.task_runs_actors()

    def contexts(self, task_type: Optional[str] = None) -> list[str]:
        return self.db.task_runs_contexts(task_type)
