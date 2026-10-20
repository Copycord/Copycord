# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""/search: find cloned messages in a clone server by original author, channel, date and
attachments or embeds. Clone messages are posted by webhooks, so Discord's own `from:`
filter can't find them; the messages table keeps who wrote each one. No message text is
stored or searched."""

import re
from datetime import datetime, timedelta, timezone

import discord

from copycord.server.paginator import CursorPaginator

PAGE_SIZE = 10
HAS_CHOICES = ("attachments", "embeds")
DATE_HELP = "Use YYYY-MM-DD, or 7d, 24h, 30m, 2w, today or yesterday (UTC)."

_RELATIVE = re.compile(r"^(\d{1,6})\s*([mhdw])$")
_UNITS = {"m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


class SearchInputError(ValueError):
    """Bad /search input; the message is shown to the user as is."""


def parse_when(text: str, *, end: bool = False, now: datetime | None = None) -> int:
    """Epoch seconds for a /search date. A whole day (YYYY-MM-DD, today, yesterday) means
    its first second, or its last one when `end` is set (for `before`). 7d, 24h, 30m and 2w
    count back from now."""
    now = now or datetime.now(timezone.utc)
    s = (text or "").strip().lower()
    day = None
    if s == "today":
        day = now.date()
    elif s == "yesterday":
        day = now.date() - timedelta(days=1)
    else:
        m = _RELATIVE.match(s)
        if m:
            try:
                return int((now - timedelta(**{_UNITS[m.group(2)]: int(m.group(1))})).timestamp())
            except OverflowError:
                raise SearchInputError(f"`{text}` is too far back. {DATE_HELP}") from None
        try:
            day = datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            raise SearchInputError(f"Couldn't read `{text}` as a date. {DATE_HELP}") from None
    start = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
    return start + 86399 if end else start


def build_filters(
    *,
    author_id: int | None = None,
    channel_id: int | None = None,
    after: str | None = None,
    before: str | None = None,
    has: str | None = None,
    now: datetime | None = None,
) -> dict:
    """The db.search_messages filters for the command's options. Raises SearchInputError."""
    after = (after or "").strip() or None
    before = (before or "").strip() or None
    has = (has or "").strip().lower() or None
    if author_id is None and channel_id is None and after is None and before is None and has is None:
        raise SearchInputError("Pick at least one filter: author, channel, after, before or has.")
    filters: dict = {}
    if author_id is not None:
        filters["author_id"] = int(author_id)
    if channel_id is not None:
        filters["cloned_channel_id"] = int(channel_id)
    if after is not None:
        filters["created_from"] = parse_when(after, now=now)
    if before is not None:
        filters["created_to"] = parse_when(before, end=True, now=now)
    if after is not None and before is not None and filters["created_from"] > filters["created_to"]:
        raise SearchInputError("`after` is later than `before`.")
    if has == "attachments":
        filters["has_attachments"] = True
    elif has == "embeds":
        filters["has_embeds"] = True
    elif has is not None:
        raise SearchInputError("`has` must be attachments or embeds.")
    return filters


def author_label(author: dict) -> str:
    """'Name (@username)' for an author row, falling back to whichever part exists, then the id."""
    name = (author.get("author_name") or "").strip()
    username = (author.get("author_username") or "").strip()
    if name and username:
        label = f"{name} (@{username})"
    elif username:
        label = f"@{username}"
    else:
        label = name or str(author.get("author_id") or "Unknown")
    return label[:100]


def author_choices(db, guild_id: int, prefix: str) -> list[discord.OptionChoice]:
    return [
        discord.OptionChoice(name=author_label(a), value=str(a["author_id"]))
        for a in db.distinct_authors(guild_id, prefix or "", limit=25)
    ]


def resolve_author(db, guild_id: int, text: str | None) -> dict | None:
    """The author for the `author` option: an id picked from the list, or typed text that
    matches exactly one author. Raises SearchInputError when it doesn't."""
    text = (text or "").strip()
    if not text:
        return None
    matches = db.distinct_authors(guild_id, text, limit=25)
    if text.isdigit():
        for a in matches:
            if str(a["author_id"]) == text:
                return a
        return {"author_id": int(text), "author_name": None, "author_username": None}
    needle = text.lstrip("@").lower()
    exact = [
        a for a in matches
        if needle in ((a["author_name"] or "").lower(), (a["author_username"] or "").lower())
    ]
    if len(exact) == 1:
        return exact[0]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise SearchInputError(f"No cloned messages from an author matching `{text}` in this server.")
    raise SearchInputError(f"More than one author matches `{text}`. Pick one from the list.")


def jump_url(row: dict) -> str:
    return (
        f"https://discord.com/channels/{row['cloned_guild_id']}"
        f"/{row['cloned_channel_id']}/{row['cloned_message_id']}"
    )


def result_line(row: dict) -> str:
    name = row.get("author_name") or row.get("author_username") or str(row.get("author_id") or "Unknown")
    name = discord.utils.escape_markdown(name[:40])
    marks = ""
    if row.get("attachment_count"):
        marks += f" 📎{int(row['attachment_count'])}"
    if row.get("embed_count"):
        marks += f" 🔗{int(row['embed_count'])}"
    ts = int(row.get("message_ts") or 0)
    return f"**{name}** in <#{row['cloned_channel_id']}> <t:{ts}:R>{marks} [Jump]({jump_url(row)})"


def filters_summary(*, author: dict | None, channel_name: str | None, after, before, has) -> str:
    parts = []
    if author:
        parts.append(f"from {author_label(author)}")
    if channel_name:
        parts.append(f"in #{channel_name}")
    if after:
        parts.append(f"after {after.strip()}")
    if before:
        parts.append(f"before {before.strip()}")
    if has:
        parts.append(f"has {has.strip().lower()}")
    return ", ".join(parts)


def render_page(rows: list[dict], page: int, summary: str) -> discord.Embed:
    e = discord.Embed(
        title="Search results",
        description="\n".join(result_line(r) for r in rows) or "No more results.",
        color=discord.Color.blurple(),
    )
    e.set_footer(text=f"Page {page} | {summary}"[:2048])
    return e


def page_fetcher(db, guild_id: int, filters: dict):
    """fetch(cursor) for CursorPaginator: one page of rows, and the cursor for the next page
    (None on the last one). Reads one extra row to know whether a next page exists."""

    def fetch(cursor):
        rows = db.search_messages(guild_id, limit=PAGE_SIZE + 1, after=cursor, **filters)
        if len(rows) <= PAGE_SIZE:
            return rows, None
        rows = rows[:PAGE_SIZE]
        return rows, (rows[-1]["message_ts"], rows[-1]["original_message_id"])

    return fetch


async def run_search(db, ctx, *, author=None, channel=None, after=None, before=None, has=None, now=None):
    """Answer /search in ctx's guild with an ephemeral, paged embed."""
    guild = ctx.guild
    if guild is None:
        return await ctx.respond("This command must be run inside a server.", ephemeral=True)
    try:
        who = resolve_author(db, guild.id, author)
        filters = build_filters(
            author_id=who["author_id"] if who else None,
            channel_id=channel.id if channel is not None else None,
            after=after,
            before=before,
            has=has,
            now=now,
        )
    except SearchInputError as e:
        return await ctx.respond(str(e), ephemeral=True)

    summary = filters_summary(
        author=who,
        channel_name=getattr(channel, "name", None),
        after=after,
        before=before,
        has=has,
    )
    view = CursorPaginator(
        ctx.user.id,
        page_fetcher(db, guild.id, filters),
        lambda rows, page: render_page(rows, page, summary),
    )
    embed = await view.first_page()
    if embed is None:
        view.stop()
        return await ctx.respond("No cloned messages match these filters.", ephemeral=True)
    if view.next_cursor is None:
        view.stop()
        return await ctx.respond(embed=embed, ephemeral=True)
    return await ctx.respond(embed=embed, view=view, ephemeral=True)
