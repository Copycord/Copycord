# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Tasks framework: long jobs register a TaskType and run through TaskService.

Task types register here at import."""

from .context import RunContext, Step, TaskCancelled
from .registry import TaskType, register
from .service import (
    MemoryRunRepo,
    RunFinished,
    RunRepo,
    TaskRun,
    TaskService,
)
from . import backfill  # noqa: F401,E402  registers backfill_channels
from . import purge  # noqa: F401,E402  registers purge_messages
from . import purge_assets  # noqa: F401,E402  registers purge_assets
from . import export_dms  # noqa: F401,E402  registers export_dms
from . import export_messages  # noqa: F401,E402  registers export_messages
from . import avatars  # noqa: F401,E402  registers download_avatars
from . import export_assets  # noqa: F401,E402  registers export_assets
from . import sync_assets  # noqa: F401,E402  registers sync_assets
from . import scrape  # noqa: F401,E402  registers scrape_members

__all__ = (
    "MemoryRunRepo", "RunContext", "RunFinished", "RunRepo",
    "Step", "TaskCancelled", "TaskRun", "TaskService", "TaskType", "register",
)
