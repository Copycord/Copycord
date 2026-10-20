"""/search on the server bot: the db search and author autocomplete, date and option parsing,
the paginator, and the COMMAND_USERS gate."""
import importlib
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from copycord.server import search
from copycord.server.paginator import CursorPaginator

G = 900  # the clone guild /search runs in
OTHER = 901
CH_A, CH_B = 501, 502
BASE = 1_760_000_000
NOW = datetime(2026, 10, 20, 12, 0, 0, tzinfo=timezone.utc)


def _add(db, mid, *, guild=G, chan=CH_A, author=11, name="Ann", username="ann", ts=None, att=0, emb=0, cloned=True):
    db.upsert_message_mapping(
        1, 100 + chan, mid, chan, mid + 10_000 if cloned else None,
        cloned_guild_id=guild, author_id=author, author_name=name, author_username=username,
        attachment_count=att, embed_count=emb, message_ts=BASE + mid if ts is None else ts,
    )


@pytest.fixture()
def seeded(db):
    _add(db, 1, author=11, name="Ann", username="ann")
    _add(db, 2, author=12, name="Bob", username="bobby", chan=CH_B, att=2)
    _add(db, 3, author=11, name="Ann B", username="ann", emb=1)
    _add(db, 4, author=13, name="Carl", username="c_100%")
    _add(db, 5, author=14, name="Dee", username="dee", guild=OTHER)
    _add(db, 8, author=17, name="Gus", username="gus", cloned=False)
    return db


def _ids(rows):
    return [r["original_message_id"] for r in rows]


# ---- db ---------------------------------------------------------------------


def test_search_scoped_to_guild_newest_first_and_skips_uncloned(seeded):
    assert _ids(seeded.search_messages(G)) == [4, 3, 2, 1]
    assert _ids(seeded.search_messages(OTHER)) == [5]


def test_search_filters(seeded):
    assert _ids(seeded.search_messages(G, author_id=11)) == [3, 1]
    assert _ids(seeded.search_messages(G, cloned_channel_id=CH_B)) == [2]
    assert _ids(seeded.search_messages(G, has_attachments=True)) == [2]
    assert _ids(seeded.search_messages(G, has_embeds=True)) == [3]
    assert _ids(seeded.search_messages(G, created_from=BASE + 2, created_to=BASE + 2)) == [2]
    assert _ids(seeded.search_messages(G, author_id=12, cloned_channel_id=CH_A)) == []


def test_search_keyset_paging_no_gaps_with_ties(db):
    for mid in range(1, 24):
        _add(db, mid, ts=BASE + mid // 3)  # three rows share most timestamps
    seen, cursor = [], None
    while True:
        rows = db.search_messages(G, limit=5, after=cursor)
        if not rows:
            break
        seen += _ids(rows)
        cursor = (rows[-1]["message_ts"], rows[-1]["original_message_id"])
    assert seen == sorted(range(1, 24), key=lambda m: (BASE + m // 3, m), reverse=True)


def test_list_messages_still_takes_the_new_filters(seeded):
    assert _ids(seeded.list_messages(cloned_channel_id=CH_B)) == [2]
    assert set(_ids(seeded.list_messages(has_embeds=False))) == {1, 2, 4, 5, 8}


def test_distinct_authors(seeded):
    _add(seeded, 6, author=15, name="annie", username="zz")
    assert [a["author_id"] for a in seeded.distinct_authors(G)] == [15, 13, 11, 12]
    assert [a["author_id"] for a in seeded.distinct_authors(G, "AN")] == [15, 11]
    assert [a["author_id"] for a in seeded.distinct_authors(G, "@bob")] == [12]
    assert [a["author_id"] for a in seeded.distinct_authors(G, "1")] == [15, 13, 11, 12]
    assert seeded.distinct_authors(G, "14") == []  # other guild
    assert seeded.distinct_authors(G, "gus") == []  # no clone message to jump to
    assert seeded.distinct_authors(G, "ann")[1]["author_name"] == "Ann B"  # latest name
    assert [a["author_id"] for a in seeded.distinct_authors(G, "c_100%")] == [13]
    assert seeded.distinct_authors(G, "c%") == []  # wildcards are literal
    assert seeded.distinct_authors(G, "_") == []
    assert len(seeded.distinct_authors(G, "", limit=2)) == 2


def test_search_indexes_exist(db):
    names = {r[0] for r in db.conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"idx_messages_clone_guild_ts", "idx_messages_clone_guild_author"} <= names


# ---- parsing ----------------------------------------------------------------


def test_parse_when_dates_and_words():
    day = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp())
    assert search.parse_when("2026-10-01", now=NOW) == day
    assert search.parse_when(" 2026-10-01 ", end=True, now=NOW) == day + 86399
    today = int(datetime(2026, 10, 20, tzinfo=timezone.utc).timestamp())
    assert search.parse_when("Today", now=NOW) == today
    assert search.parse_when("yesterday", now=NOW) == today - 86400
    assert search.parse_when("yesterday", end=True, now=NOW) == today - 1


def test_parse_when_relative():
    now = int(NOW.timestamp())
    assert search.parse_when("7d", now=NOW) == now - 7 * 86400
    assert search.parse_when("24h", now=NOW) == now - 86400
    assert search.parse_when("30m", end=True, now=NOW) == now - 1800
    assert search.parse_when("2W", now=NOW) == now - 14 * 86400


@pytest.mark.parametrize("bad", ["", "soon", "2026-13-01", "2026/10/01", "7y", "-3d", "999999w"])
def test_parse_when_rejects(bad):
    with pytest.raises(search.SearchInputError):
        search.parse_when(bad, now=NOW)


def test_build_filters():
    with pytest.raises(search.SearchInputError, match="at least one filter"):
        search.build_filters(after="  ", has="")
    f = search.build_filters(author_id=11, channel_id=CH_A, after="2026-10-01", before="2026-10-01", has="Embeds", now=NOW)
    day = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp())
    assert f == {
        "author_id": 11, "cloned_channel_id": CH_A, "created_from": day,
        "created_to": day + 86399, "has_embeds": True,
    }
    assert search.build_filters(has="attachments") == {"has_attachments": True}
    with pytest.raises(search.SearchInputError, match="later than"):
        search.build_filters(after="2026-10-02", before="2026-10-01", now=NOW)
    with pytest.raises(search.SearchInputError):
        search.build_filters(has="stickers")
    with pytest.raises(search.SearchInputError, match="date"):
        search.build_filters(before="nope")


def test_resolve_author(seeded):
    assert search.resolve_author(seeded, G, None) is None
    assert search.resolve_author(seeded, G, "12")["author_name"] == "Bob"
    assert search.resolve_author(seeded, G, "777")["author_id"] == 777
    assert search.resolve_author(seeded, G, "bob")["author_id"] == 12
    assert search.resolve_author(seeded, G, "@ann")["author_id"] == 11  # exact username beats "Ann B"
    _add(seeded, 7, author=16, name="Annabel", username="bel")
    with pytest.raises(search.SearchInputError, match="More than one"):
        search.resolve_author(seeded, G, "an")
    with pytest.raises(search.SearchInputError, match="No cloned messages"):
        search.resolve_author(seeded, G, "zed")


def test_author_label_and_choices(seeded):
    assert search.author_label({"author_id": 1, "author_name": "Ann", "author_username": "ann"}) == "Ann (@ann)"
    assert search.author_label({"author_id": 1, "author_name": None, "author_username": "ann"}) == "@ann"
    assert search.author_label({"author_id": 1, "author_name": None, "author_username": None}) == "1"
    choices = search.author_choices(seeded, G, "bo")
    assert [(c.name, c.value) for c in choices] == [("Bob (@bobby)", "12")]


def test_result_line_and_page():
    row = {
        "original_message_id": 1, "cloned_guild_id": G, "cloned_channel_id": CH_A, "cloned_message_id": 77,
        "author_id": 11, "author_name": "A*nn", "author_username": "ann",
        "attachment_count": 2, "embed_count": 0, "message_ts": BASE,
    }
    line = search.result_line(row)
    assert f"<#{CH_A}>" in line and f"<t:{BASE}:R>" in line and "📎2" in line and "🔗" not in line
    assert f"[Jump](https://discord.com/channels/{G}/{CH_A}/77)" in line
    assert "A\\*nn" in line
    e = search.render_page([row], 3, "from Ann (@ann)")
    assert e.footer.text == "Page 3 | from Ann (@ann)"


# ---- paginator --------------------------------------------------------------


def _inter(user_id):
    return SimpleNamespace(
        user=SimpleNamespace(id=user_id),
        response=SimpleNamespace(edit_message=AsyncMock(), send_message=AsyncMock()),
    )


def _list_fetch(items, size, calls):
    def fetch(cursor):
        calls.append(cursor)
        start = 0 if cursor is None else cursor
        page = items[start:start + size]
        return page, (start + size if start + size < len(items) else None)
    return fetch


@pytest.mark.asyncio
async def test_paginator_owner_only():
    view = CursorPaginator(5, lambda c: ([], None), lambda rows, page: discord.Embed())
    assert view.timeout == 600
    assert await view.interaction_check(_inter(5)) is True
    other = _inter(6)
    assert await view.interaction_check(other) is False
    other.response.send_message.assert_awaited_once()
    assert other.response.send_message.call_args.kwargs.get("ephemeral") is True


@pytest.mark.asyncio
async def test_paginator_cursor_paging_and_ends():
    calls = []
    view = CursorPaginator(
        5, _list_fetch(list(range(25)), 10, calls),
        lambda rows, page: discord.Embed(title=f"p{page}", description=",".join(map(str, rows))),
    )
    first = await view.first_page()
    assert first.title == "p1" and view.prev_button.disabled and not view.next_button.disabled

    inter = _inter(5)
    await view.turn(inter, 1)
    assert inter.response.edit_message.call_args.kwargs["embed"].title == "p2"
    await view.turn(inter, 1)
    emb = inter.response.edit_message.call_args.kwargs["embed"]
    assert emb.title == "p3" and emb.description == "20,21,22,23,24"
    assert view.next_button.disabled and not view.prev_button.disabled

    await view.turn(inter, 1)  # at the end: nothing happens
    assert inter.response.edit_message.await_count == 2

    await view.turn(inter, -1)
    await view.turn(inter, -1)
    assert inter.response.edit_message.call_args.kwargs["embed"].title == "p1"
    assert view.prev_button.disabled and not view.next_button.disabled
    assert calls == [None, 10, 20, 10, None]  # each page refetched from its own cursor


@pytest.mark.asyncio
async def test_paginator_async_fetch_and_empty():
    async def fetch(cursor):
        return [], None

    view = CursorPaginator(5, fetch, lambda rows, page: discord.Embed())
    assert await view.first_page() is None


# ---- the command ------------------------------------------------------------


def _ctx(user_id=1, guild_id=G):
    return SimpleNamespace(
        guild=SimpleNamespace(id=guild_id) if guild_id else None,
        user=SimpleNamespace(id=user_id),
        respond=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_run_search_pages_through_every_result(db):
    for mid in range(1, 26):
        _add(db, mid)
    _add(db, 99, author=12, name="Bob")
    ctx = _ctx()
    await search.run_search(db, ctx, author="11", after="2020-01-01")
    kw = ctx.respond.call_args.kwargs
    view = kw["view"]
    assert kw["ephemeral"] is True and kw["embed"].footer.text.startswith("Page 1 | from Ann (@ann), after 2020-01-01")
    descs = [kw["embed"].description]
    inter = _inter(1)
    while not view.next_button.disabled:
        await view.turn(inter, 1)
        descs.append(inter.response.edit_message.call_args.kwargs["embed"].description)
    lines = "\n".join(descs).splitlines()
    assert len(lines) == 25 and len(descs) == 3
    assert lines[0].endswith(f"/{CH_A}/{25 + 10_000})") and lines[-1].endswith(f"/{CH_A}/{1 + 10_000})")


@pytest.mark.asyncio
async def test_run_search_single_page_has_no_buttons(seeded):
    ctx = _ctx()
    channel = SimpleNamespace(id=CH_B, name="general")
    await search.run_search(seeded, ctx, channel=channel)
    kw = ctx.respond.call_args.kwargs
    assert "view" not in kw and "in #general" in kw["embed"].footer.text


@pytest.mark.asyncio
async def test_run_search_errors_and_no_results(seeded):
    ctx = _ctx()
    await search.run_search(seeded, ctx)
    assert "at least one filter" in ctx.respond.call_args.args[0]
    await search.run_search(seeded, ctx, before="someday")
    assert "Couldn't read" in ctx.respond.call_args.args[0]
    await search.run_search(seeded, ctx, author="12", has="embeds")
    assert ctx.respond.call_args.args[0] == "No cloned messages match these filters."
    assert ctx.respond.call_args.kwargs["ephemeral"] is True
    no_guild = _ctx(guild_id=None)
    await search.run_search(seeded, no_guild, has="embeds")
    assert "inside a server" in no_guild.respond.call_args.args[0]


def _commands_module():
    # importing the commands module can retune library log levels; put them back
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    levels = {n: lg.level for n, lg in logging.root.manager.loggerDict.items() if isinstance(lg, logging.Logger)}
    try:
        return importlib.import_module("copycord.server.commands")
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        for n, lg in list(logging.root.manager.loggerDict.items()):
            if isinstance(lg, logging.Logger):
                lg.setLevel(levels.get(n, logging.NOTSET))


def test_search_command_registered_with_options():
    mod = _commands_module()
    # Cog.__new__ copies the commands onto the instance and binds them, which drops self and ctx
    cog = mod.CloneCommands.__new__(mod.CloneCommands)
    cmd = next(c for c in cog.get_commands() if c.name == "search")
    assert isinstance(cmd, discord.SlashCommand)
    opts = {o.name: o for o in cmd.options}
    block = next(c for c in cog.get_commands() if c.name == "block_add")
    assert [o.name for o in block.options] == ["keyword"]
    assert list(opts) == ["author", "channel", "after", "before", "has"]
    assert not any(o.required for o in opts.values())
    assert opts["author"].autocomplete is not None
    assert [c.value for c in opts["has"].choices] == ["attachments", "embeds"]
    assert discord.ChannelType.public_thread in opts["channel"].channel_types
    assert discord.ChannelType.forum not in opts["channel"].channel_types


@pytest.mark.asyncio
async def test_command_users_gate():
    mod = _commands_module()
    cog_check = vars(mod.CloneCommands)["cog_check"]
    cog = SimpleNamespace(allowed_users=[1])
    cmd = SimpleNamespace(name="search", qualified_name="search")
    stranger = SimpleNamespace(command=cmd, guild=SimpleNamespace(name="Clone"),
                               user=SimpleNamespace(id=2, name="x"), respond=AsyncMock())
    assert await cog_check(cog, stranger) is False
    assert stranger.respond.call_args.kwargs["ephemeral"] is True
    admin = SimpleNamespace(command=cmd, guild=SimpleNamespace(name="Clone"),
                            user=SimpleNamespace(id=1, name="a"), respond=AsyncMock())
    assert await cog_check(cog, admin) is True
    admin.respond.assert_not_awaited()


@pytest.mark.asyncio
async def test_author_autocomplete_is_gated(seeded):
    mod = _commands_module()
    cog = SimpleNamespace(allowed_users=[1], db=seeded)

    def actx(user_id):
        return SimpleNamespace(
            command=SimpleNamespace(cog=cog),
            interaction=SimpleNamespace(guild_id=G, user=SimpleNamespace(id=user_id)),
            value="b",
        )

    assert [c.value for c in await mod._search_author_autocomplete(actx(1))] == ["12"]
    assert await mod._search_author_autocomplete(actx(2)) == []
