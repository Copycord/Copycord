"""Discord obfuscates channels the account cannot view (flags 1 << 17, name
"___hidden___"). The sitemap keeps them, and the server decides: an obfuscated
channel with no clone is skipped, a mapped one keeps its clone and real name.
"""
import asyncio
from types import SimpleNamespace

import pytest

from copycord.selfclient.models import Guild
from copycord.server import server as srv

GUILD = 800000000000000001
HIDDEN = "___hidden___"
OBFUSCATED_FLAG = 1 << 17


def _guild_with(*channels):
    return Guild(
        {
            "id": str(GUILD),
            "name": "Source",
            "roles": [{"id": str(GUILD), "name": "@everyone", "position": 0, "permissions": "0"}],
            "channels": list(channels),
        }
    )


def test_channel_wrapper_reads_the_obfuscated_flag_not_the_name():
    g = _guild_with(
        {"id": "1", "type": 0, "name": HIDDEN, "flags": OBFUSCATED_FLAG},
        {"id": "2", "type": 0, "name": HIDDEN, "flags": 0},
        {"id": "3", "type": 0, "name": "general"},
        {"id": "4", "type": 0, "name": "pinned", "flags": 2 | OBFUSCATED_FLAG},
    )
    got = {c.id: c.obfuscated for c in g.channels}
    assert got == {1: True, 2: False, 3: False, 4: True}


@pytest.mark.parametrize(
    "item, expected",
    [
        ({"id": 1, "name": HIDDEN, "obfuscated": True}, True),
        ({"id": 1, "name": "real", "obfuscated": True}, True),
        ({"id": 1, "name": HIDDEN}, True),
        ({"id": 1, "name": f"  {HIDDEN} "}, True),
        ({"id": 1, "name": "real", "obfuscated": False}, False),
        ({"id": 1, "name": "real"}, False),
        ({"id": 1}, False),
        (None, False),
        ({}, False),
    ],
)
def test_sitemap_item_is_obfuscated(item, expected):
    assert srv.is_obfuscated_item(item) is expected


class _Category:
    def __init__(self, cid):
        self.id = cid


class _CloneGuild:
    def __init__(self, existing=()):
        self.id = 900000000000000001
        self.name = "Clone"
        self.created = []
        self._by_id = {c.id: c for c in existing}

    def get_channel(self, cid):
        return self._by_id.get(cid)

    async def create_category(self, name):
        cat = _Category(900000000000000100 + len(self.created))
        self.created.append(name)
        self._by_id[cat.id] = cat
        return cat


class _CategoryDB:
    def __init__(self, row=None):
        self.row = row
        self.upserts = []

    def get_category_mapping_by_original_and_clone(self, original_id, clone_id):
        return self.row

    def upsert_category_mapping(self, **kw):
        self.upserts.append(kw)


def _receiver(db):
    rx = srv.ServerReceiver.__new__(srv.ServerReceiver)
    rx.db = db
    rx.cat_map_by_clone = {}
    rx.cat_map = {}

    async def _no_event(*a, **kw):
        return None

    rx._emit_event_log = _no_event
    return rx


def _ensure(rx, guild, name, *, obfuscated):
    return asyncio.run(
        rx._ensure_category(guild, 5, name, 7, obfuscated=obfuscated)
    )


def test_unmapped_obfuscated_category_is_not_created():
    guild, db = _CloneGuild(), _CategoryDB()
    cat, created = _ensure(_receiver(db), guild, HIDDEN, obfuscated=True)
    assert (cat, created) == (None, False)
    assert guild.created == [] and db.upserts == []


def test_unmapped_placeholder_named_category_is_not_created_without_the_flag():
    guild, db = _CloneGuild(), _CategoryDB()
    cat, created = _ensure(_receiver(db), guild, HIDDEN, obfuscated=False)
    assert (cat, created) == (None, False)
    assert guild.created == []


def test_mapped_obfuscated_category_keeps_its_clone():
    clone = _Category(900000000000000050)
    guild = _CloneGuild([clone])
    db = _CategoryDB(
        {
            "cloned_category_id": clone.id,
            "original_category_name": "Staff",
            "cloned_category_name": None,
            "original_guild_id": 7,
        }
    )
    cat, created = _ensure(_receiver(db), guild, HIDDEN, obfuscated=True)
    assert (cat, created) == (clone, False)
    assert guild.created == []


def test_mapped_obfuscated_category_with_deleted_clone_is_rebuilt_under_its_real_name():
    guild = _CloneGuild()
    db = _CategoryDB(
        {
            "cloned_category_id": 900000000000000999,
            "original_category_name": "Staff",
            "cloned_category_name": None,
            "original_guild_id": 7,
        }
    )
    cat, created = _ensure(_receiver(db), guild, HIDDEN, obfuscated=True)
    assert created is True and cat is not None
    assert guild.created == ["Staff"]
    assert db.upserts[0]["orig_name"] == "Staff"


def test_mapped_category_whose_stored_name_is_the_placeholder_is_not_rebuilt():
    guild = _CloneGuild()
    db = _CategoryDB(
        {
            "cloned_category_id": 900000000000000999,
            "original_category_name": HIDDEN,
            "cloned_category_name": None,
            "original_guild_id": 7,
        }
    )
    cat, created = _ensure(_receiver(db), guild, HIDDEN, obfuscated=True)
    assert (cat, created) == (None, False)
    assert guild.created == []


class _CloneChannel:
    def __init__(self, name):
        self.id = 900000000000000321
        self.name = name
        self.guild = SimpleNamespace(id=900000000000000001)
        self.edits = []
        self.type = SimpleNamespace(value=0)

    async def edit(self, **kw):
        self.edits.append(kw)
        self.name = kw.get("name", self.name)


class _ChannelDB:
    def __init__(self):
        self.upserts = []

    def get_channel_mapping_by_original_and_clone(self, orig, clone):
        return None

    def upsert_channel_mapping(self, orig, name, *args, **kw):
        self.upserts.append(name)


def _rename(obfuscated, upstream, mapping, monkeypatch):
    monkeypatch.setattr(
        srv, "resolve_mapping_settings", lambda *a, **kw: {"RENAME_CHANNELS": True}
    )
    rx = srv.ServerReceiver.__new__(srv.ServerReceiver)
    rx.db = _ChannelDB()
    rx.config = SimpleNamespace(default_mapping_settings=lambda: {"RENAME_CHANNELS": True})
    rx.chan_map_by_clone = {900000000000000001: {5: mapping}}
    ch = _CloneChannel("staff-chat")

    async def _no_event(*a, **kw):
        return None

    rx._emit_event_log = _no_event
    result = asyncio.run(rx._maybe_rename_channel(ch, upstream, 5, obfuscated=obfuscated))
    return result, ch, rx.db


def _mapping(stored):
    return {
        "original_channel_id": 5,
        "original_channel_name": stored,
        "cloned_channel_id": 900000000000000321,
        "clone_channel_name": None,
        "original_guild_id": 7,
        "cloned_guild_id": 900000000000000001,
    }


def test_obfuscated_channel_is_not_renamed_and_keeps_its_stored_name(monkeypatch):
    result, ch, db = _rename(True, HIDDEN, _mapping("staff-chat"), monkeypatch)
    assert result == (False, "skipped_hidden_placeholder")
    assert ch.edits == [] and ch.name == "staff-chat"
    assert db.upserts == ["staff-chat"]


def test_flag_alone_blocks_the_rename_even_with_a_real_looking_name(monkeypatch):
    result, ch, db = _rename(True, "something-else", _mapping("staff-chat"), monkeypatch)
    assert result == (False, "skipped_hidden_placeholder")
    assert ch.edits == []


def test_a_real_name_still_renames_the_clone(monkeypatch):
    result, ch, db = _rename(False, "renamed-upstream", _mapping("staff-chat"), monkeypatch)
    assert result[0] is True
    assert ch.name == "renamed-upstream"
    assert db.upserts == ["renamed-upstream"]
