# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

import inspect

import discord


class CursorPaginator(discord.ui.View):
    """Prev/Next buttons over keyset pages, usable only by the user who ran the command.

    `fetch(cursor)` (sync or async) returns (rows, next_cursor), with next_cursor None on the
    last page; the first page is fetched with cursor None. `render(rows, page)` builds the
    embed, page counting from 1. Prev refetches with the cursor that fetched the earlier page,
    so no page is held in memory and deep pages cost the same as the first.
    """

    def __init__(self, owner_id: int, fetch, render, *, timeout: float = 600):
        super().__init__(timeout=timeout, disable_on_timeout=True)
        self.owner_id = int(owner_id)
        self._fetch = fetch
        self._render = render
        self.cursors: list = [None]
        self.page = 0
        self.rows: list = []
        self.next_cursor = None

        self.prev_button = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary)
        self.next_button = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary)

        async def _prev(interaction):
            await self.turn(interaction, -1)

        async def _next(interaction):
            await self.turn(interaction, 1)

        self.prev_button.callback = _prev
        self.next_button.callback = _next
        self.add_item(self.prev_button)
        self.add_item(self.next_button)
        self._sync_buttons()

    async def _get(self, cursor):
        out = self._fetch(cursor)
        if inspect.isawaitable(out):
            out = await out
        return out

    def _sync_buttons(self):
        self.prev_button.disabled = self.page == 0
        self.next_button.disabled = self.next_cursor is None

    def embed(self) -> discord.Embed:
        return self._render(self.rows, self.page + 1)

    async def first_page(self) -> discord.Embed | None:
        """Load page 1; None when there are no rows at all."""
        self.rows, self.next_cursor = await self._get(None)
        self.cursors, self.page = [None], 0
        self._sync_buttons()
        return self.embed() if self.rows else None

    async def turn(self, interaction, step: int):
        if step > 0:
            if self.next_cursor is None:
                return
            target, cursor = self.page + 1, self.next_cursor
        else:
            if self.page == 0:
                return
            target = self.page - 1
            cursor = self.cursors[target]
        rows, nxt = await self._get(cursor)
        self.cursors = self.cursors[:target] + [cursor]
        self.page, self.rows, self.next_cursor = target, rows, nxt
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.embed(), view=self)

    async def interaction_check(self, interaction) -> bool:
        user = getattr(interaction, "user", None)
        if user is not None and int(user.id) == self.owner_id:
            return True
        await interaction.response.send_message(
            "Only the person who ran this command can use these buttons.", ephemeral=True
        )
        return False
