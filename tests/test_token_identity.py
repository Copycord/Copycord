"""
Unit tests for the sticky-author token selection / rotation logic.

These cover ``TokenIdentityManager._select_token``, a pure function of the
persisted assignment state, plus ``prepare`` idempotency using lightweight
fakes (no real bot or database).
"""

import types

import pytest

from copycord.server import token_identity
from copycord.server.token_identity import TokenIdentityManager


def _ident(author_id, token_id, assigned_at):
    return {
        "author_id": author_id,
        "token_id": token_id,
        "assigned_at": assigned_at,
    }


select = TokenIdentityManager._select_token


class TestSelectToken:

    def test_new_author_gets_first_free_token(self):
        chosen, reset = select([], "X", ["a", "b", "c"])
        assert chosen == "a"
        assert reset is None

    def test_distinct_authors_get_distinct_tokens(self):
        identities = []
        picks = []
        for author in ("X", "Y", "Z"):
            chosen, _ = select(identities, author, ["a", "b", "c"])
            picks.append(chosen)
            identities.append(_ident(author, chosen, 0))
        assert sorted(picks) == ["a", "b", "c"]

    def test_author_keeps_assigned_token(self):
        # Assignment is permanent, the token stays while it's still enabled.
        identities = [_ident("X", "b", 900)]
        chosen, reset = select(identities, "X", ["a", "b", "c"])
        assert chosen == "b"
        assert reset is None

    def test_bad_token_swaps_to_free_token(self):
        # X's token "a" went bad and is disabled; Y holds "b" → X → "c".
        identities = [_ident("X", "a", 0), _ident("Y", "b", 0)]
        chosen, reset = select(identities, "X", ["b", "c"])
        assert chosen == "c"
        assert reset == "a"

    def test_no_free_token_returns_none(self):
        # X's token is bad and the only other token is taken by Y → exhausted.
        identities = [_ident("X", "a", 0), _ident("Y", "b", 0)]
        chosen, reset = select(identities, "X", ["b"])
        assert chosen is None
        assert reset is None

    def test_disabled_token_forces_reassignment(self):
        # X was on "a" but "a" is disabled → swap + reset "a".
        identities = [_ident("X", "a", 0)]
        chosen, reset = select(identities, "X", ["b", "c"])
        assert chosen == "b"
        assert reset == "a"

# prepare() idempotency with fakes


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
        uid = getattr(self, "token_users", {}).get(str(token_id))
        return {"user_id": uid} if uid else None

    def delete_token_identity(self, mapping_id, author_id):
        self.deleted = getattr(self, "deleted", []) + [str(author_id)]
        return self.identities.pop((str(mapping_id), str(author_id)), None) is not None

    def get_role_mapping_for_clone(self, host_id, clone_gid):
        crid = self.role_map.get(int(host_id))
        return {"cloned_role_id": crid} if crid else None

    def upsert_token_identity(
        self, *, mapping_id, author_id, token_id, cloned_guild_id,
        applied_nick, applied_role_ids, assigned_at,
        applied_avatar_hash=None,
    ):
        self.upserts += 1
        self.identities[(str(mapping_id), str(author_id))] = {
            "author_id": str(author_id),
            "token_id": str(token_id),
            "cloned_guild_id": cloned_guild_id,
            "applied_nick": applied_nick,
            "applied_role_ids": sorted(int(x) for x in (applied_role_ids or [])),
            "applied_avatar_hash": applied_avatar_hash,
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
        }
        kw = dict(
            mapping_id="m", cloned_guild_id=2, author_id="777",
            author_display_name="Alice", author_role_ids=[],
            settings=settings, tokens=tokens,
        )

        assert await mgr.prepare(**kw) == ("a", None)
        assert member.nick == "Alice"
        assert db.upserts == 1

        # Already in sync → no second edit, no second write.
        assert await mgr.prepare(**kw) == ("a", None)
        assert len(member.edits) == 1
        assert db.upserts == 1

    @pytest.mark.asyncio
    async def test_blocked_nick_is_not_retried_every_message(self):
        class _Blocked(_Member):
            async def edit(self, *, nick=None, reason=None):
                self.edits.append(nick)
                # The server role's own Forbidden, the class token_identity
                # catches; reached through the module so this test does not
                # import a Discord library itself.
                forbidden = token_identity.discord.Forbidden
                raise forbidden.__new__(forbidden)

        member = _Blocked(500)
        bot = _Bot(_Guild(2, {500: member}, {}))
        db = _DB()
        mgr = TokenIdentityManager(bot=bot, db=db, logger=_silent())
        tokens = [{"token_id": "a", "user_id": "500"}]
        settings = {
            "USER_TOKEN_STICKY_NICKNAME": True,
            "USER_TOKEN_STICKY_ROLES": False,
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
        # edit is tried once, not on every message.
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


class TestPrepareSwapAndExhaust:

    def _kw(self, tokens, fallback_webhook=True):
        return dict(
            mapping_id="m", cloned_guild_id=2, author_id="777",
            author_display_name="Alice", author_role_ids=[],
            settings={
                "USER_TOKEN_STICKY_NICKNAME": True,
                "USER_TOKEN_FALLBACK_WEBHOOK": fallback_webhook,
            },
            tokens=tokens,
        )

    @pytest.mark.asyncio
    async def test_new_identity_no_free_token_webhook(self):
        # The only token is already held by another author → exhausted → webhook.
        db = _DB()
        db.identities[("m", "999")] = {
            "author_id": "999", "token_id": "a", "cloned_guild_id": 2,
            "applied_nick": "Bob", "applied_role_ids": [], "assigned_at": 0,
        }
        mgr = TokenIdentityManager(
            bot=_Bot(_Guild(2, {500: _Member(500)}, {})), db=db, logger=_silent()
        )
        token_id, action = await mgr.prepare(
            **self._kw([{"token_id": "a", "user_id": "500"}])
        )
        assert token_id is None
        assert action == "webhook"

    @pytest.mark.asyncio
    async def test_new_identity_no_free_token_skip(self):
        db = _DB()
        db.identities[("m", "999")] = {
            "author_id": "999", "token_id": "a", "cloned_guild_id": 2,
            "applied_nick": "Bob", "applied_role_ids": [], "assigned_at": 0,
        }
        mgr = TokenIdentityManager(
            bot=_Bot(_Guild(2, {500: _Member(500)}, {})), db=db, logger=_silent()
        )
        token_id, action = await mgr.prepare(
            **self._kw([{"token_id": "a", "user_id": "500"}], fallback_webhook=False)
        )
        assert token_id is None
        assert action == "skip"

    @pytest.mark.asyncio
    async def test_failed_token_excluded_swaps_and_resets_previous(self):
        m_a = _Member(500)
        m_b = _Member(600)
        db = _DB()
        db.token_users = {"a": "500", "b": "600"}
        guild = _Guild(2, {500: m_a, 600: m_b}, {})
        mgr = TokenIdentityManager(bot=_Bot(guild), db=db, logger=_silent())
        tokens = [
            {"token_id": "a", "user_id": "500"},
            {"token_id": "b", "user_id": "600"},
        ]
        kw = self._kw(tokens)

        # Permanent assignment: first send picks "a" and mirrors the nickname.
        assert await mgr.prepare(**kw) == ("a", None)
        assert m_a.nick == "Alice"

        # "a" failed to send this message → exclude it → swap to "b", clear "a".
        assert await mgr.prepare(**kw, exclude={"a"}) == ("b", None)
        assert m_b.nick == "Alice"
        assert m_a.nick is None

    @pytest.mark.asyncio
    async def test_failure_does_not_bench_token_for_future_messages(self):
        # A token that fails once (excluded for one message) must
        # still be available for the next message, no persistent benching.
        m_a = _Member(500)
        db = _DB()
        db.token_users = {"a": "500"}
        guild = _Guild(2, {500: m_a}, {})
        mgr = TokenIdentityManager(bot=_Bot(guild), db=db, logger=_silent())
        tokens = [{"token_id": "a", "user_id": "500"}]

        # This message excludes "a" (it just failed) and there's no other token
        # → exhausted → webhook.
        assert await mgr.prepare(**self._kw(tokens), exclude={"a"}) == (None, "webhook")

        # Next message (no exclusion) can still use "a".
        assert await mgr.prepare(**self._kw(tokens)) == ("a", None)


class TestBadTokens:
    """A token confirmed dead (Discord revoked it → 401) is benched for the rest
    of the session and never handed to an identity again."""

    def _kw(self, tokens, author="777", fallback_webhook=True):
        return dict(
            mapping_id="m", cloned_guild_id=2, author_id=author,
            author_display_name="Alice", author_role_ids=[],
            settings={
                "USER_TOKEN_STICKY_NICKNAME": True,
                "USER_TOKEN_FALLBACK_WEBHOOK": fallback_webhook,
            },
            tokens=tokens,
        )

    @pytest.mark.asyncio
    async def test_benched_token_is_not_assigned_to_new_author(self):
        db = _DB()
        db.token_users = {"a": "500", "b": "600"}
        guild = _Guild(2, {500: _Member(500), 600: _Member(600)}, {})
        mgr = TokenIdentityManager(bot=_Bot(guild), db=db, logger=_silent())
        mgr.mark_token_bad("a")

        tokens = [
            {"token_id": "a", "user_id": "500"},
            {"token_id": "b", "user_id": "600"},
        ]
        token_id, _ = await mgr.prepare(**self._kw(tokens))
        # "a" is benched → the new identity lands on "b".
        assert token_id == "b"

    @pytest.mark.asyncio
    async def test_benching_current_token_swaps_and_resets_it(self):
        m_a = _Member(500)
        m_b = _Member(600)
        db = _DB()
        db.token_users = {"a": "500", "b": "600"}
        guild = _Guild(2, {500: m_a, 600: m_b}, {})
        mgr = TokenIdentityManager(bot=_Bot(guild), db=db, logger=_silent())
        tokens = [
            {"token_id": "a", "user_id": "500"},
            {"token_id": "b", "user_id": "600"},
        ]
        kw = self._kw(tokens)

        # Assigned "a" first.
        assert await mgr.prepare(**kw) == ("a", None)
        assert m_a.nick == "Alice"

        # Discord revoked "a" mid-session → bench it → next message swaps to "b"
        # and clears "a" (its nickname is reset).
        mgr.mark_token_bad("a")
        assert await mgr.prepare(**kw) == ("b", None)
        assert m_b.nick == "Alice"
        assert m_a.nick is None

    @pytest.mark.asyncio
    async def test_all_tokens_benched_exhausts_to_webhook(self):
        db = _DB()
        db.token_users = {"a": "500"}
        guild = _Guild(2, {500: _Member(500)}, {})
        mgr = TokenIdentityManager(bot=_Bot(guild), db=db, logger=_silent())
        mgr.mark_token_bad("a")
        # The only token is dead → no free token to switch to → webhook.
        assert await mgr.prepare(**self._kw([{"token_id": "a", "user_id": "500"}])) == (
            None,
            "webhook",
        )

    def test_clear_bad_tokens_reenables(self):
        mgr = TokenIdentityManager(bot=None, db=_DB(), logger=_silent())
        mgr.mark_token_bad("a")
        assert mgr.is_token_bad("a")
        mgr.clear_bad_tokens()
        assert not mgr.is_token_bad("a")


class TestResetIdentity:

    @pytest.mark.asyncio
    async def test_reset_strips_all_removable_roles_and_clears_nick(self):
        # The previous token must come back clean when the identity changes:
        # every removable role is stripped (even ones we never recorded), the
        # managed role stays (can't be removed), and the nickname is cleared.
        mirrored = _Role(20, position=5)
        untracked = _Role(21, position=6)
        managed = _Role(22, position=7, managed=True)
        member = _Member(500, nick="Alice", roles=[mirrored, untracked, managed])
        guild = _Guild(2, {500: member}, {20: mirrored, 21: untracked, 22: managed})
        mgr = TokenIdentityManager(bot=_Bot(guild), db=_DB(), logger=_silent())

        await mgr._reset_identity(
            guild, member, applied_nick="Alice", applied_role_ids=[20]
        )

        remaining = [r.id for r in member.roles]
        assert 20 not in remaining
        assert 21 not in remaining
        assert 22 in remaining
        assert member.nick is None

    @pytest.mark.asyncio
    async def test_reset_leaves_roles_when_none_were_mirrored(self):
        # Nickname-only identity (no roles applied) must not touch the account's
        # roles, only clear the nickname.
        base = _Role(30, position=3)
        member = _Member(500, nick="Bob", roles=[base])
        guild = _Guild(2, {500: member}, {30: base})
        mgr = TokenIdentityManager(bot=_Bot(guild), db=_DB(), logger=_silent())

        await mgr._reset_identity(
            guild, member, applied_nick="Bob", applied_role_ids=[]
        )

        assert [r.id for r in member.roles] == [30]
        assert member.nick is None


class TestNoFreeAccountSetting:

    @pytest.mark.parametrize(
        "settings,policy,action",
        [
            ({}, "webhook", "webhook"),
            ({"USER_TOKEN_FALLBACK_WEBHOOK": True}, "webhook", "webhook"),
            ({"USER_TOKEN_FALLBACK_WEBHOOK": False}, "drop", "skip"),
            ({"USER_TOKEN_NO_FREE_ACCOUNT": "reuse", "USER_TOKEN_FALLBACK_WEBHOOK": False}, "reuse", "webhook"),
            ({"USER_TOKEN_NO_FREE_ACCOUNT": "drop", "USER_TOKEN_FALLBACK_WEBHOOK": True}, "drop", "skip"),
            ({"USER_TOKEN_NO_FREE_ACCOUNT": "webhook", "USER_TOKEN_FALLBACK_WEBHOOK": False}, "webhook", "webhook"),
            ({"USER_TOKEN_NO_FREE_ACCOUNT": " Reuse "}, "reuse", "webhook"),
            ({"USER_TOKEN_NO_FREE_ACCOUNT": "nope", "USER_TOKEN_FALLBACK_WEBHOOK": False}, "drop", "skip"),
            ({"USER_TOKEN_NO_FREE_ACCOUNT": None}, "webhook", "webhook"),
            ({"USER_TOKEN_NO_FREE_ACCOUNT": 3}, "webhook", "webhook"),
        ],
    )
    def test_resolution(self, settings, policy, action):
        assert token_identity.no_free_account_policy(settings) == policy
        assert token_identity.exhausted_action(settings) == action

    def test_server_uses_the_same_helper(self):
        from copycord.server.server import ServerReceiver

        assert ServerReceiver._identity_exhausted_action({"USER_TOKEN_FALLBACK_WEBHOOK": False}) == "skip"
        assert ServerReceiver._identity_exhausted_action({"USER_TOKEN_NO_FREE_ACCOUNT": "reuse"}) == "webhook"
        assert ServerReceiver._identity_exhausted_action({"USER_TOKEN_NO_FREE_ACCOUNT": "drop"}) == "skip"

    @pytest.mark.parametrize(
        "raw,minutes", [(None, 30), (45, 45), ("10", 10), (0, 1), (-5, 1), ("x", 30)]
    )
    def test_idle_minutes(self, raw, minutes):
        settings = {} if raw is None else {"USER_TOKEN_REUSE_IDLE_MINUTES": raw}
        assert token_identity.reuse_idle_minutes(settings) == minutes

    def test_config_defaults_leave_the_policy_unset(self):
        from copycord.common.config import Config

        defaults = Config.default_mapping_settings(object())
        assert "USER_TOKEN_NO_FREE_ACCOUNT" not in defaults
        assert defaults["USER_TOKEN_REUSE_IDLE_MINUTES"] == 30
        assert defaults["USER_TOKEN_FALLBACK_WEBHOOK"] is True


NOW = 1_000_000


class TestReuseIdleAccount:

    def _setup(self, holders, tokens=None, now=NOW):
        """holders: {author: (token, assigned_at, applied_nick, applied_role_ids)}."""
        token_identity.time = types.SimpleNamespace(time=lambda: now)
        db = _DB()
        members = {}
        for author, (tid, assigned_at, nick, roles) in holders.items():
            db.identities[("m", author)] = {
                "author_id": author, "token_id": tid, "cloned_guild_id": 2,
                "applied_nick": nick, "applied_role_ids": roles,
                "applied_avatar_hash": None, "assigned_at": assigned_at,
            }
        tokens = tokens or []
        db.token_users = {t["token_id"]: t["user_id"] for t in tokens}
        for t in tokens:
            members[int(t["user_id"])] = _Member(int(t["user_id"]))
        guild = _Guild(2, members, {})
        logs = []
        log = _silent()
        log.info = lambda msg, *a, **k: logs.append(msg % a)
        mgr = TokenIdentityManager(bot=_Bot(guild), db=db, logger=log)
        return mgr, db, members, logs

    def _kw(self, tokens, author="new", minutes=30, policy="reuse"):
        return dict(
            mapping_id="m", cloned_guild_id=2, author_id=author,
            author_display_name="Newbie", author_role_ids=[],
            settings={
                "USER_TOKEN_STICKY_NICKNAME": True,
                "USER_TOKEN_NO_FREE_ACCOUNT": policy,
                "USER_TOKEN_REUSE_IDLE_MINUTES": minutes,
            },
            tokens=tokens,
        )

    @pytest.fixture(autouse=True)
    def _restore_time(self):
        import time as real_time

        yield
        token_identity.time = real_time

    @pytest.mark.asyncio
    async def test_free_token_preferred_over_eviction(self):
        tokens = [
            {"token_id": "a", "user_id": "500", "last_used": 0},
            {"token_id": "b", "user_id": "600", "last_used": None},
        ]
        mgr, db, members, _ = self._setup({"old": ("a", 0, "Old", [])}, tokens)
        assert await mgr.prepare(**self._kw(tokens)) == ("b", None)
        assert ("m", "old") in db.identities
        assert getattr(db, "deleted", []) == []

    @pytest.mark.asyncio
    async def test_evicts_the_longest_idle_holder(self):
        tokens = [
            {"token_id": "a", "user_id": "500", "last_used": NOW - 40 * 60},
            {"token_id": "b", "user_id": "600", "last_used": NOW - 90 * 60},
        ]
        mgr, db, members, logs = self._setup(
            {"x": ("a", 0, "Xan", []), "y": ("b", 0, "Yve", [])}, tokens
        )
        members[600].nick = "Yve"
        resets = []
        orig = mgr._reset_identity

        async def spy(guild, member, **kw):
            resets.append((member.id, kw))
            await orig(guild, member, **kw)

        mgr._reset_identity = spy
        assert await mgr.prepare(**self._kw(tokens)) == ("b", None)
        assert resets == [(600, {"applied_nick": "Yve", "applied_role_ids": []})]
        assert db.deleted == ["y"]
        assert ("m", "y") not in db.identities
        assert db.identities[("m", "new")]["token_id"] == "b"
        assert db.identities[("m", "new")]["applied_nick"] == "Newbie"
        assert members[600].nick == "Newbie"
        assert len(logs) == 1 and "'reuse'" in logs[0] and "90 min" in logs[0]

    @pytest.mark.asyncio
    async def test_nobody_idle_enough_gives_webhook(self):
        tokens = [{"token_id": "a", "user_id": "500", "last_used": NOW - 29 * 60}]
        mgr, db, _, _ = self._setup({"x": ("a", 0, "Xan", [])}, tokens)
        assert await mgr.prepare(**self._kw(tokens)) == (None, "webhook")
        assert ("m", "x") in db.identities
        assert getattr(db, "deleted", []) == []

    @pytest.mark.asyncio
    async def test_recent_assignment_counts_over_old_last_used(self):
        # Last send was long ago, but the token changed hands 5 minutes ago.
        tokens = [{"token_id": "a", "user_id": "500", "last_used": NOW - 999 * 60}]
        mgr, db, _, _ = self._setup({"x": ("a", NOW - 5 * 60, "Xan", [])}, tokens)
        assert await mgr.prepare(**self._kw(tokens)) == (None, "webhook")
        assert ("m", "x") in db.identities

    @pytest.mark.asyncio
    async def test_never_used_token_counts_from_assignment(self):
        tokens = [{"token_id": "a", "user_id": "500", "last_used": None}]
        mgr, db, _, _ = self._setup({"x": ("a", NOW - 10 * 60, "Xan", [])}, tokens)
        assert await mgr.prepare(**self._kw(tokens)) == (None, "webhook")
        mgr2, db2, _, _ = self._setup({"x": ("a", NOW - 31 * 60, "Xan", [])}, tokens)
        assert await mgr2.prepare(**self._kw(tokens)) == ("a", None)
        assert ("m", "x") not in db2.identities

    @pytest.mark.asyncio
    async def test_benched_token_is_not_evicted(self):
        tokens = [
            {"token_id": "a", "user_id": "500", "last_used": 0},
            {"token_id": "b", "user_id": "600", "last_used": NOW - 60 * 60},
        ]
        mgr, db, _, _ = self._setup(
            {"x": ("a", 0, "Xan", []), "y": ("b", 0, "Yve", [])}, tokens
        )
        mgr.mark_token_bad("a")
        assert await mgr.prepare(**self._kw(tokens)) == ("b", None)
        assert ("m", "x") in db.identities
        assert db.deleted == ["y"]

    @pytest.mark.asyncio
    async def test_excluded_token_is_not_evicted(self):
        tokens = [{"token_id": "a", "user_id": "500", "last_used": 0}]
        mgr, db, _, _ = self._setup({"x": ("a", 0, "Xan", [])}, tokens)
        assert await mgr.prepare(**self._kw(tokens), exclude={"a"}) == (None, "webhook")
        assert ("m", "x") in db.identities

    @pytest.mark.asyncio
    @pytest.mark.parametrize("policy,action", [("webhook", "webhook"), ("drop", "skip")])
    async def test_other_policies_never_evict(self, policy, action):
        tokens = [{"token_id": "a", "user_id": "500", "last_used": 0}]
        mgr, db, _, _ = self._setup({"x": ("a", 0, "Xan", [])}, tokens)
        assert await mgr.prepare(**self._kw(tokens, policy=policy)) == (None, action)
        assert ("m", "x") in db.identities

    @pytest.mark.asyncio
    async def test_two_new_authors_do_not_take_the_same_token(self):
        import asyncio

        tokens = [{"token_id": "a", "user_id": "500", "last_used": 0}]
        mgr, db, _, _ = self._setup({"x": ("a", 0, "Xan", [])}, tokens)
        r1, r2 = await asyncio.gather(
            mgr.prepare(**self._kw(tokens, author="n1")),
            mgr.prepare(**self._kw(tokens, author="n2")),
        )
        assert sorted([r1, r2], key=str) == sorted([("a", None), (None, "webhook")], key=str)
        holders = [k[1] for k, v in db.identities.items() if v["token_id"] == "a"]
        assert len(holders) == 1

    @pytest.mark.asyncio
    async def test_evicted_author_coming_back(self):
        tokens = [
            {"token_id": "a", "user_id": "500", "last_used": 0},
            {"token_id": "b", "user_id": "600", "last_used": 0},
        ]
        mgr, db, _, _ = self._setup(
            {"x": ("a", 0, "Xan", []), "y": ("b", NOW - 60 * 60, "Yve", [])}, tokens
        )
        # "new" takes x's account (idle since 0, the longest).
        assert await mgr.prepare(**self._kw(tokens)) == ("a", None)
        # x comes back: y's account is idle 60 min, so x takes it.
        assert await mgr.prepare(**self._kw(tokens, author="x")) == ("b", None)
        assert ("m", "y") not in db.identities
        # y comes back: "new" and x were just assigned, nobody is idle enough.
        assert await mgr.prepare(**self._kw(tokens, author="y")) == (None, "webhook")
