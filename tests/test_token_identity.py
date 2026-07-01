"""
Unit tests for the sticky-author token selection / rotation logic.

These cover ``TokenIdentityManager._select_token`` — a pure function of the
persisted assignment state — plus ``prepare`` idempotency using lightweight
fakes (no real bot or database).
"""

import types

import pytest

from server.token_identity import TokenIdentityManager


def _ident(author_id, token_id, assigned_at):
    return {
        "author_id": author_id,
        "token_id": token_id,
        "assigned_at": assigned_at,
    }


select = TokenIdentityManager._select_token


class TestSelectToken:

    def test_new_author_gets_least_used_free_token(self):
        chosen, reset = select([], "X", ["a", "b", "c"], 3600, 100)
        assert chosen == "a"
        assert reset is None

    def test_distinct_authors_get_distinct_tokens(self):
        identities = []
        picks = []
        now = 1000
        for author in ("X", "Y", "Z"):
            chosen, _ = select(identities, author, ["a", "b", "c"], 3600, now)
            picks.append(chosen)
            identities.append(_ident(author, chosen, now))
        assert sorted(picks) == ["a", "b", "c"]

    def test_author_keeps_live_token(self):
        identities = [_ident("X", "b", 900)]
        chosen, reset = select(identities, "X", ["a", "b", "c"], 3600, 1000)
        assert chosen == "b"
        assert reset is None

    def test_expired_hold_rotates_to_free_token(self):
        # X's hold on "a" expired; Y actively holds "b" → X moves to "c".
        identities = [_ident("X", "a", 0), _ident("Y", "b", 9000)]
        chosen, reset = select(identities, "X", ["a", "b", "c"], 3600, 10000)
        assert chosen == "c"
        assert reset == "a"

    def test_no_free_token_keeps_current(self):
        # Only one token and the hold expired → stay put, no reset.
        identities = [_ident("X", "a", 0)]
        chosen, reset = select(identities, "X", ["a"], 3600, 10000)
        assert chosen == "a"
        assert reset is None

    def test_all_busy_no_current_shares_least_used(self):
        identities = [_ident("Y", "a", 9000), _ident("Z", "b", 9000)]
        chosen, reset = select(identities, "X", ["a", "b"], 3600, 10000)
        assert chosen in ("a", "b")
        assert reset is None

    def test_ttl_zero_never_expires(self):
        identities = [_ident("X", "a", 0)]
        chosen, reset = select(identities, "X", ["a", "b"], 0, 10_000_000)
        assert chosen == "a"
        assert reset is None

    def test_disabled_token_forces_reassignment(self):
        # X was on "a" but "a" is no longer enabled → rotate + reset "a".
        identities = [_ident("X", "a", 9000)]
        chosen, reset = select(identities, "X", ["b", "c"], 3600, 10000)
        assert chosen == "b"
        assert reset == "a"

    def test_no_tokens_returns_none(self):
        chosen, reset = select([], "X", [], 3600, 100)
        assert chosen is None
        assert reset is None


# ── prepare() idempotency with fakes ─────────────────────────────────────────


class _Role:
    def __init__(self, rid, position=1, managed=False, default=False):
        self.id = rid
        self.position = position
        self.managed = managed
        self._default = default

    def is_default(self):
        return self._default

    def __lt__(self, other):
        return self.position < other.position

    def __eq__(self, other):
        return isinstance(other, _Role) and other.id == self.id

    def __hash__(self):
        return hash(self.id)


class _Member:
    def __init__(self, mid, nick=None, roles=None):
        self.id = mid
        self.nick = nick
        self.roles = list(roles or [])
        self.edits = []
        self.added = []
        self.removed = []

    async def edit(self, *, nick=None, reason=None):
        self.nick = nick
        self.edits.append(nick)

    async def add_roles(self, *roles, reason=None):
        self.added.append([r.id for r in roles])
        self.roles.extend(roles)

    async def remove_roles(self, *roles, reason=None):
        self.removed.append([r.id for r in roles])
        self.roles = [r for r in self.roles if r not in roles]


class _Guild:
    def __init__(self, gid, members, roles):
        self.id = gid
        self._members = members
        self._roles = roles
        self.me = types.SimpleNamespace(top_role=_Role(9999, position=9999))

    def get_member(self, uid):
        return self._members.get(int(uid))

    async def fetch_member(self, uid):
        m = self._members.get(int(uid))
        if m is None:
            raise RuntimeError("not found")
        return m

    def get_role(self, rid):
        return self._roles.get(int(rid))


class _Bot:
    def __init__(self, guild):
        self._guild = guild

    def get_guild(self, gid):
        return self._guild if int(gid) == self._guild.id else None


class _DB:
    def __init__(self, role_map=None):
        self.identities = {}
        self.role_map = role_map or {}
        self.upserts = 0

    def get_token_identity(self, mapping_id, author_id):
        return self.identities.get((str(mapping_id), str(author_id)))

    def list_token_identities(self, mapping_id):
        return [v for k, v in self.identities.items() if k[0] == str(mapping_id)]

    def get_mapping_token(self, token_id):
        return None

    def get_role_mapping_for_clone(self, host_id, clone_gid):
        crid = self.role_map.get(int(host_id))
        return {"cloned_role_id": crid} if crid else None

    def upsert_token_identity(
        self, *, mapping_id, author_id, token_id, cloned_guild_id,
        applied_nick, applied_role_ids, assigned_at,
    ):
        self.upserts += 1
        self.identities[(str(mapping_id), str(author_id))] = {
            "author_id": str(author_id),
            "token_id": str(token_id),
            "cloned_guild_id": cloned_guild_id,
            "applied_nick": applied_nick,
            "applied_role_ids": sorted(int(x) for x in (applied_role_ids or [])),
            "assigned_at": int(assigned_at),
        }


def _silent():
    return types.SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )


class TestPrepareIdempotency:

    @pytest.mark.asyncio
    async def test_applies_nick_then_fast_path_is_noop(self):
        member = _Member(500)
        bot = _Bot(_Guild(2, {500: member}, {}))
        db = _DB()
        mgr = TokenIdentityManager(bot=bot, db=db, logger=_silent())
        tokens = [{"token_id": "a", "user_id": "500"}]
        settings = {
            "USER_TOKEN_STICKY_NICKNAME": True,
            "USER_TOKEN_STICKY_ROLES": False,
            "USER_TOKEN_IDENTITY_TTL_MIN": 60,
        }
        kw = dict(
            mapping_id="m", cloned_guild_id=2, author_id="777",
            author_display_name="Alice", author_role_ids=[],
            settings=settings, tokens=tokens,
        )

        assert await mgr.prepare(**kw) == "a"
        assert member.nick == "Alice"
        assert db.upserts == 1

        # Already in sync → no second edit, no second write.
        assert await mgr.prepare(**kw) == "a"
        assert len(member.edits) == 1
        assert db.upserts == 1

    @pytest.mark.asyncio
    async def test_blocked_nick_is_not_retried_every_message(self):
        class _Blocked(_Member):
            async def edit(self, *, nick=None, reason=None):
                self.edits.append(nick)
                import discord

                raise discord.Forbidden.__new__(discord.Forbidden)

        member = _Blocked(500)
        bot = _Bot(_Guild(2, {500: member}, {}))
        db = _DB()
        mgr = TokenIdentityManager(bot=bot, db=db, logger=_silent())
        tokens = [{"token_id": "a", "user_id": "500"}]
        settings = {
            "USER_TOKEN_STICKY_NICKNAME": True,
            "USER_TOKEN_STICKY_ROLES": False,
            "USER_TOKEN_IDENTITY_TTL_MIN": 60,
        }
        kw = dict(
            mapping_id="m", cloned_guild_id=2, author_id="777",
            author_display_name="Alice", author_role_ids=[],
            settings=settings, tokens=tokens,
        )
        await mgr.prepare(**kw)
        await mgr.prepare(**kw)
        await mgr.prepare(**kw)
        # The intent was recorded on the first (failed) attempt, so the doomed
        # edit is tried once — not on every message.
        assert len(member.edits) == 1

    @pytest.mark.asyncio
    async def test_mirrors_roles_and_is_idempotent(self):
        clone_role = _Role(20, position=5)
        member = _Member(500, roles=[])
        bot = _Bot(_Guild(2, {500: member}, {20: clone_role}))
        db = _DB(role_map={10: 20})
        mgr = TokenIdentityManager(bot=bot, db=db, logger=_silent())
        tokens = [{"token_id": "a", "user_id": "500"}]
        settings = {
            "USER_TOKEN_STICKY_NICKNAME": False,
            "USER_TOKEN_STICKY_ROLES": True,
            "USER_TOKEN_IDENTITY_TTL_MIN": 0,
        }
        kw = dict(
            mapping_id="m", cloned_guild_id=2, author_id="777",
            author_display_name="Alice", author_role_ids=[10],
            settings=settings, tokens=tokens,
        )
        await mgr.prepare(**kw)
        assert 20 in [r.id for r in member.roles]
        added_after_first = list(member.added)

        # Author's roles unchanged → fast path, no more add/remove.
        await mgr.prepare(**kw)
        assert member.added == added_after_first
        assert member.removed == []
