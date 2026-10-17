# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""Per item callbacks the role, emoji and sticker syncs take.

The regular sitemap sync passes none and gets this no-op base; the Sync assets task
(server/asset_sync.py) passes a subclass that streams each item to the admin and asks
for a stop between items."""

from __future__ import annotations

import asyncio
from typing import Optional

# What happened to one asset.
OUTCOMES = ("created", "updated", "deleted", "unchanged", "skipped", "failed")


class ItemHooks:
    # Set when the caller wants a stop; rate limit waits wake on it.
    stop_event: Optional[asyncio.Event] = None

    async def begin(self, total: int) -> None:
        """The number of items this pass will report."""

    async def item(self, name: str, outcome: str, note: Optional[str] = None) -> None:
        """One asset is done: outcome is one of OUTCOMES; note says why it failed or was
        skipped, or what was left out of it."""

    def stop(self) -> bool:
        """True once the caller wants the pass to end before the next item."""
        return False

    def check(self) -> None:
        """Right before a Discord change and after a download or rate limit wait: once a stop
        was asked, raise CancelledError so the item ends cancelled with nothing changed (it
        is not caught by the passes' except Exception blocks)."""
        if self.stop():
            raise asyncio.CancelledError()
