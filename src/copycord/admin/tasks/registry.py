# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Task type registry: every long job registers a TaskType once at import."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from pydantic import BaseModel


Describe = Callable[[BaseModel, Any], "tuple[str, dict]"]
Runner = Callable[[Any, BaseModel], Awaitable[None]]
Options = Callable[[dict], Awaitable[Any]]


@dataclass(frozen=True)
class TaskType:
    """One kind of job. run(ctx, inputs) does the work; describe(inputs,
    lookups) returns the (title, context) stored on the run row; options(params)
    returns data the start form needs; concurrency caps active runs (None = no cap);
    requires_bots refuses a start while the server and client bots are not both running."""

    key: str
    title: str
    run: Runner
    inputs: type[BaseModel]
    description: str = ""
    describe: Optional[Describe] = None
    options: Optional[Options] = None
    concurrency: Optional[int] = None
    requires_bots: bool = False

    def describe_run(self, inputs: BaseModel, lookups: Any) -> tuple[str, dict]:
        if self.describe is None:
            return self.title, {}
        title, context = self.describe(inputs, lookups)
        return title or self.title, dict(context or {})

    def inputs_schema(self) -> dict:
        return self.inputs.model_json_schema()


_TYPES: dict[str, TaskType] = {}


def register(task_type: TaskType) -> TaskType:
    if task_type.key in _TYPES:
        raise ValueError(f"task type {task_type.key!r} is already registered")
    if task_type.concurrency is not None and task_type.concurrency < 1:
        raise ValueError("concurrency must be at least 1 or None")
    _TYPES[task_type.key] = task_type
    return task_type


def unregister(key: str) -> None:
    """Tests only: drop a throwaway type."""
    _TYPES.pop(key, None)


def get(key: str) -> Optional[TaskType]:
    return _TYPES.get(key)


def all() -> list[TaskType]:  # noqa: A001 - registry API name from the plan
    return list(_TYPES.values())
