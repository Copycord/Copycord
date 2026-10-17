"""Server side of the Purge assets task (lane L65): purge_assets_list and purge_assets_delete through
ServerReceiver._on_ws against a fake clone guild and a real DB. Only mapped assets go in a cloned
purge, the scope is checked again on delete, a delete drops its mapping row, and Forbidden, a
missing asset and a 429 come back as skipped, skipped and rate_limited."""
import asyncio
import importlib
import logging
from types import SimpleNamespace

import discord
import pytest

HOST, CLONE = 1000000000000000001, 1000000000000000002
BOT_ID = 999


@pytest.fixture
def ServerReceiver():
    # importing the server module retunes library log levels; put them back (as test_server_backfill_jobs does)
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    levels = {n: lg.level for n, lg in logging.root.manager.loggerDict.items() if isinstance(lg, logging.Logger)}
    try:
        yield importlib.import_module("copycord.server.server").ServerReceiver
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        for n, lg in list(logging.root.manager.loggerDict.items()):
            if isinstance(lg, logging.Logger):
                lg.setLevel(levels.get(n, logging.NOTSET))


def _http(cls, status, headers=None):
    return cls(SimpleNamespace(status=status, reason="nope", headers=headers or {}), "nope")


class Asset:
    def __init__(self, guild, bucket, id_, name, error=None):
        self.guild, self.bucket, self.id, self.name, self.error = guild, bucket, id_, name, error

    async def delete(self, reason=None):
        if self.error is not None:
            raise self.error
        getattr(self.guild, self.bucket).remove(self)
        self.guild.deleted.append(self.id)


class Role(Asset):
    def __init__(self, guild, id_, name, position, default=False, managed=False, booster=False, error=None):
        super().__init__(guild, "roles", id_, name, error)
        self.position, self.managed = position, managed
        self._default, self._booster = default, booster

    def is_default(self):
        return self._default

    def is_premium_subscriber(self):
        return self._booster

    def __lt__(self, other):
        return self.position < other.position


class Guild:
    def __init__(self, manage_roles=True):
        self.id, self.name = CLONE, "Alpha clone"
        self.deleted = []
        self.emojis = [Asset(self, "emojis", 11, "blob"), Asset(self, "emojis", 12, "wave"),
                       Asset(self, "emojis", 13, "local")]
        self.stickers = [Asset(self, "stickers", 21, "hello")]
        self.roles = [
            Role(self, CLONE, "@everyone", 0, default=True),
            Role(self, 31, "Member", 1),
            Role(self, 32, "Staff", 2),
            Role(self, 33, "Booster", 3, booster=True),
            Role(self, 34, "Integration", 4, managed=True),
        ]
        bot_role = Role(self, 40, "Copycord", 5, managed=True)
        self.roles += [bot_role, Role(self, 41, "Owner", 6)]
        self.me = SimpleNamespace(id=BOT_ID, top_role=bot_role,
                                  guild_permissions=SimpleNamespace(manage_roles=manage_roles))

    async def fetch_roles(self):
        return list(self.roles)

    async def fetch_stickers(self):
        return list(self.stickers)

    async def fetch_emoji(self, emoji_id):
        raise _http(discord.NotFound, 404)

    def get_role(self, role_id):
        return next((r for r in self.roles if r.id == role_id), None)


@pytest.fixture
def guild():
    return Guild()


@pytest.fixture
def rx(ServerReceiver, db, guild):
    db.upsert_emoji_mapping(501, "blob", 11, "blob", original_guild_id=HOST, cloned_guild_id=CLONE)
    db.upsert_emoji_mapping(502, "wave", 12, "wave", original_guild_id=HOST, cloned_guild_id=CLONE)
    db.upsert_sticker_mapping(521, "hello", 21, "hello", original_guild_id=HOST, cloned_guild_id=CLONE)
    db.upsert_role_mapping(531, "Member", 31, "Member", original_guild_id=HOST, cloned_guild_id=CLONE)
    db.upsert_role_mapping(541, "Owner", 41, "Owner", original_guild_id=HOST, cloned_guild_id=CLONE)
    r = ServerReceiver.__new__(ServerReceiver)
    r._shutting_down = False
    r._resolve_guild_label = lambda typ, data: None
    r.db = db
    r.bot = SimpleNamespace(user=SimpleNamespace(id=BOT_ID),
                            get_guild=lambda gid: guild if gid == CLONE else None)
    return r


def _ws(rx, typ, **data):
    return asyncio.run(rx._on_ws({"type": typ, "data": data}))


def _mapped(db, table, col, cloned_id):
    return db.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = ?", (cloned_id,)).fetchone()[0]


def test_list_names_assets_with_whether_copycord_cloned_them(rx):
    out = _ws(rx, "purge_assets_list", guild_id=str(CLONE), kinds=["emojis", "stickers", "roles"])
    assert out["ok"] and out["guild_name"] == "Alpha clone"
    kinds = out["kinds"]
    assert kinds["emojis"]["items"] == [
        {"id": "11", "name": "blob", "mapped": True},
        {"id": "12", "name": "wave", "mapped": True},
        {"id": "13", "name": "local", "mapped": False},
    ]
    assert kinds["stickers"]["items"] == [{"id": "21", "name": "hello", "mapped": True}]
    # @everyone, booster and managed roles never show; Owner sits above the bot's role
    assert [i["name"] for i in kinds["roles"]["items"]] == ["Member", "Staff"]
    assert kinds["roles"]["above_bot"] == 1


def test_list_without_manage_roles_reports_it_on_roles_only(rx, guild):
    guild.me.guild_permissions.manage_roles = False
    out = _ws(rx, "purge_assets_list", guild_id=str(CLONE), kinds=["emojis", "roles"])
    assert out["kinds"]["roles"] == {"items": [], "error": "The bot needs the Manage Roles permission"}
    assert len(out["kinds"]["emojis"]["items"]) == 3 and "stickers" not in out["kinds"]


def test_list_for_a_guild_the_bot_is_not_in(rx):
    out = _ws(rx, "purge_assets_list", guild_id="123", kinds=["emojis"])
    assert out == {"ok": False, "error": "The server bot is not in the clone guild"}


def test_delete_removes_the_asset_and_its_mapping(rx, guild, db):
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="emojis", scope="cloned", id="11")
    assert out == {"ok": True, "outcome": "deleted"}
    assert guild.deleted == [11] and _mapped(db, "emoji_mappings", "cloned_emoji_id", 11) == 0
    assert _mapped(db, "emoji_mappings", "cloned_emoji_id", 12) == 1
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="roles", scope="cloned", id="31")
    assert out["outcome"] == "deleted" and _mapped(db, "role_mappings", "cloned_role_id", 31) == 0


def test_cloned_scope_never_deletes_an_unmapped_asset(rx, guild):
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="emojis", scope="cloned", id="13")
    assert out == {"ok": True, "outcome": "skipped", "error": "Not cloned by Copycord"}
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="emojis", scope="unmapped", id="11")
    assert out["outcome"] == "skipped" and guild.deleted == []
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="emojis", scope="all", id="13")
    assert out["outcome"] == "deleted" and guild.deleted == [13]


def test_roles_the_bot_cannot_delete_are_skipped(rx, guild):
    for rid in ("41", str(CLONE)):
        out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="roles", scope="all", id=rid)
        assert out == {"ok": True, "outcome": "skipped", "error": "The bot cannot delete this role"}
    assert guild.deleted == []


def test_missing_asset_is_skipped_and_its_stale_mapping_dropped(rx, guild, db):
    guild.stickers.clear()
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="stickers", scope="cloned", id="21")
    assert out == {"ok": True, "outcome": "skipped", "error": "Already gone"}
    assert _mapped(db, "sticker_mappings", "cloned_sticker_id", 21) == 0


def test_forbidden_is_skipped_and_errors_fail_keeping_the_mapping(rx, guild, db):
    guild.emojis[0].error = _http(discord.Forbidden, 403)
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="emojis", scope="cloned", id="11")
    assert out["outcome"] == "skipped" and out["error"].startswith("Missing permission")
    guild.emojis[1].error = _http(discord.HTTPException, 500)
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="emojis", scope="cloned", id="12")
    assert out["outcome"] == "failed" and out["error"]
    assert _mapped(db, "emoji_mappings", "cloned_emoji_id", 11) == 1
    assert _mapped(db, "emoji_mappings", "cloned_emoji_id", 12) == 1


def test_rate_limit_comes_back_with_retry_after(rx, guild):
    guild.emojis[0].error = _http(discord.HTTPException, 429, {"Retry-After": "2.5"})
    out = _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="emojis", scope="cloned", id="11")
    assert out["outcome"] == "rate_limited" and out["retry_after"] == 2.5


def test_bad_delete_frames_fail(rx):
    assert _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="channels", id="11")["outcome"] == "failed"
    assert _ws(rx, "purge_assets_delete", guild_id=str(CLONE), kind="emojis")["error"] == "Missing asset id"
    assert _ws(rx, "purge_assets_delete", guild_id="5", kind="emojis", id="11")["ok"] is False
