# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Tasks framework: long jobs register a TaskType and run through TaskService.

Task types register here at import once they exist (backfill_channels next)."""

from .context import RunContext, Step, TaskCancelled
from .registry import TaskType, register
from .service import (
    MemoryRunRepo,
    RunFinished,
    RunRepo,
    TaskRun,
    TaskService,
)

__all__ = (
    "MemoryRunRepo", "RunContext", "RunFinished", "RunRepo",
    "Step", "TaskCancelled", "TaskRun", "TaskService", "TaskType", "register",
)
