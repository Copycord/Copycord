"""Role reorder after a sync creates roles: the cache's positions are stale, so
RoleManager._rearrange_roles plans from roles fetched over REST, ranked into
Discord's order, and never sends a position at or above the bot's top role."""
import logging
from types import SimpleNamespace

import discord
import pytest

from copycord.server.roles import RoleManager, plan_role_order, rank_role_facts

CG = 1234567890123456790
BOT = 99


class _Role:
    def __init__(self, rid, position, name=None, managed=False):
        self.id, self.position, self.managed = rid, position, managed
        self.name = name or f"r{rid}"

    def is_default(self):
        return self.id == CG


class _Guild:
    """Cached roles say one thing, fetch_roles says what Discord really has."""

    def __init__(self, cached, fresh, fetch_error=None, edit_errors=()):
        self.id = CG
        self.roles = cached
        self._fresh = fresh
        self._fetch_error = fetch_error
        self._edit_errors = list(edit_errors)
        top = next(r for r in cached if r.id == BOT)
        self.me = SimpleNamespace(roles=[top], top_role=top)
        self.fetches = 0
        self.moves = []

    async def fetch_roles(self):
        self.fetches += 1
        if self._fetch_error:
            raise self._fetch_error
        return list(self._fresh)

    async def edit_role_positions(self, positions, reason=None):
        self.moves.append({int(o.id): p for o, p in positions.items()})
        if self._edit_errors:
            raise self._edit_errors.pop(0)


class _DB:
    def __init__(self, mapped):
        self.rows = [{"original_guild_id": 1, "cloned_guild_id": CG, "original_role_id": h,
                      "cloned_role_id": c} for h, c in mapped.items()]

    def get_all_role_mappings(self):
        return self.rows


def _http_error(status, code, message):
    resp = SimpleNamespace(status=status, reason=message)
    return discord.HTTPException(resp, {"code": code, "message": message})


# Host order bottom to top: 201, 202, 203, then the managed 150.
HOST = [{"id": 1, "position": 1}, {"id": 2, "position": 2}, {"id": 3, "position": 3},
        {"id": 4, "position": 4}]
MAPPED = {1: 201, 2: 202, 3: 203, 4: 150}


def _fresh():
    return [_Role(CG, 0, "@everyone"), _Role(150, 1, "Bots", managed=True),
            _Role(203, 2), _Role(202, 3), _Role(201, 4, "· Boost Bots"),
            _Role(BOT, 5, "Copycord", managed=True)]


def _stale():
    # Rapid creation leaves new roles duplicated and high, the bot's role low.
    return [_Role(CG, 0, "@everyone"), _Role(150, 1, "Bots", managed=True),
            _Role(203, 7), _Role(202, 7), _Role(201, 9, "· Boost Bots"),
            _Role(BOT, 2, "Copycord", managed=True)]


def _manager():
    return RoleManager(bot=None, db=_DB(MAPPED), guild_resolver=None, ratelimit=None,
                       clone_guild_id=CG)


async def _rearrange(guild):
    return await _manager()._rearrange_roles(guild=guild, incoming=HOST, host_id=1,
                                             clone_id=CG)


def _sent_ok(move, bot_position):
    assert {BOT, CG, 150}.isdisjoint(move)
    assert all(0 < p < bot_position for p in move.values())


@pytest.mark.asyncio
async def test_stale_cache_plans_from_the_fetched_roles(caplog):
    guild = _Guild(_stale(), _fresh())
    with caplog.at_level(logging.DEBUG, logger="server.roles"):
        moved = await _rearrange(guild)
    assert "above my role" not in caplog.text
    assert guild.moves == [{201: 1, 202: 2, 203: 3}]
    _sent_ok(guild.moves[0], 5)
    assert moved == 3


@pytest.mark.asyncio
async def test_fetch_failure_falls_back_to_the_cache(caplog):
    cached = _fresh()
    cached[4].position = 6  # the cache alone says 201 sits above the bot
    guild = _Guild(cached, [], fetch_error=_http_error(500, 0, "Server Error"))
    with caplog.at_level(logging.DEBUG, logger="server.roles"):
        await _rearrange(guild)
    assert "Fetching roles failed" in caplog.text
    assert "'· Boost Bots' is above my role" in caplog.text
    assert guild.moves == [{202: 1, 203: 2}]


@pytest.mark.asyncio
async def test_missing_permissions_refetches_and_retries_once(caplog):
    guild = _Guild(_stale(), _fresh(), edit_errors=[_http_error(400, 50013, "Missing Permissions")])
    with caplog.at_level(logging.DEBUG, logger="server.roles"):
        moved = await _rearrange(guild)
    assert guild.fetches == 2 and len(guild.moves) == 2 and moved == 3
    assert "retrying once" in caplog.text


@pytest.mark.asyncio
async def test_a_second_missing_permissions_gives_up_with_a_warning(caplog):
    err = _http_error(400, 50013, "Missing Permissions")
    guild = _Guild(_stale(), _fresh(), edit_errors=[err, err])
    with caplog.at_level(logging.DEBUG, logger="server.roles"):
        moved = await _rearrange(guild)
    assert moved == 0 and len(guild.moves) == 2
    assert "Failed to rearrange roles after refetching them" in caplog.text


def test_rank_breaks_ties_by_id_with_the_older_role_higher():
    roles = [{"id": CG, "position": 0, "default": True},
             {"id": 202, "position": 1}, {"id": 201, "position": 2},
             {"id": BOT, "position": 2}, {"id": 50, "position": 2}]
    facts, bot_pos, bot_id = rank_role_facts(roles, {BOT})
    assert [f["id"] for f in facts] == [CG, 202, 201, BOT, 50]
    assert [f["position"] for f in facts] == [0, 1, 2, 3, 4]
    assert (bot_pos, bot_id) == (3, BOT)


def test_rank_without_my_roles_is_none():
    assert rank_role_facts([{"id": CG, "position": 0, "default": True}], {BOT}) is None


def test_plan_never_sends_a_position_at_or_above_the_bot():
    clone = [{"id": CG, "name": "@everyone", "position": 0, "default": True},
             {"id": 11, "name": "a", "position": 1}, {"id": 12, "name": "b", "position": 1},
             {"id": 13, "name": "c", "position": 1}, {"id": BOT, "name": "Copycord", "position": 2}]
    host = [{"id": 1, "position": 1}, {"id": 2, "position": 2}, {"id": 3, "position": 3}]
    plan = plan_role_order(host, {1: 11, 2: 12, 3: 13}, clone, 2, BOT)
    assert plan["positions"] == {11: 1}
    assert {u["id"] for u in plan["unmovable"]} == {12, 13}
