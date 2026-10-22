"""Convert to orphan (Channels page): the unlink_channel verify action unlinks one cloned channel
in one clone guild. The mapping row, the thread rows under it and the message rows of the channel
and its threads go; the server's mapping caches and queued webhook create for it are dropped; the
channel stays in the clone guild and shows as an orphan; the next sync clones the source again."""
import asyncio
import importlib
import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from copycord.server.helpers import VerifyController

HOST, CLONE, OTHER_CLONE = 1000000000000000001, 1000000000000000002, 1000000000000000003
SRC, SRC_KEPT = 1521898853337403501, 1521898853337403502
OLD, KEPT, NEW = 1521898853337403601, 1521898853337403602, 1521898853337403603
THREAD_SRC, THREAD = 1521898853337403701, 1521898853337403702
ELSEWHERE = 1521898853337403801


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


class Channel:
    def __init__(self, cid, name):
        self.id, self.name = cid, name
        self.touched = []

    async def edit(self, **kw):
        self.touched.append(("edit", kw))

    async def delete(self, **kw):
        self.touched.append(("delete", kw))


class Guild:
    def __init__(self):
        self.id, self.name = CLONE, "Alpha clone"
        self.features = []
        self.categories = []
        self.chans = {OLD: Channel(OLD, "general"), KEPT: Channel(KEPT, "rules")}
        self.created = []

    @property
    def channels(self):
        return list(self.chans.values())

    def get_channel(self, cid):
        return self.chans.get(int(cid)) if cid else None


class Backfill:
    def __init__(self, running=()):
        self.running = set(running)
        self.invalidated = []

    def is_backfilling(self, original_id):
        return int(original_id) in self.running

    def invalidate_rotation(self, clone_channel_id):
        self.invalidated.append(int(clone_channel_id))


def _seed(db):
    for src, clone, name in ((SRC, OLD, "general"), (SRC_KEPT, KEPT, "rules")):
        db.upsert_channel_mapping(
            src, name, clone, f"https://discord.com/api/webhooks/{clone}/tok", None, None, 0,
            original_guild_id=HOST, cloned_guild_id=CLONE,
        )
    # the same source in a second clone guild keeps its own mapping
    db.upsert_channel_mapping(
        SRC, "general", ELSEWHERE, None, None, None, 0, original_guild_id=HOST, cloned_guild_id=OTHER_CLONE
    )
    db.upsert_forum_thread_mapping(
        THREAD_SRC, "help", THREAD, SRC, OLD, original_guild_id=HOST, cloned_guild_id=CLONE
    )
    for mid, ocid, ccid, cg in (
        (1, SRC, OLD, CLONE),
        (2, THREAD_SRC, THREAD, CLONE),
        (3, SRC_KEPT, KEPT, CLONE),
        (1, SRC, ELSEWHERE, OTHER_CLONE),
    ):
        db.upsert_message_mapping(
            HOST, ocid, mid, ccid, 9000 + mid, f"https://discord.com/api/webhooks/{ccid}/tok", cloned_guild_id=cg
        )


def _count(db, sql, *args):
    return db.conn.execute(sql, args).fetchone()[0]


@pytest.fixture
def guild():
    return Guild()


@pytest.fixture
def rx(ServerReceiver, db, guild):
    _seed(db)
    r = ServerReceiver.__new__(ServerReceiver)
    r._shutting_down = False
    r.db = db
    r.bot = SimpleNamespace(get_guild=lambda gid: guild if gid == CLONE else None)
    r.backfill = Backfill()
    r._guild_sync_locks = {}
    r._pending_webhook_channels = {
        CLONE: [
            {"original_id": SRC, "cloned_channel_id": OLD, "cloned_guild_id": CLONE},
            {"original_id": SRC_KEPT, "cloned_channel_id": KEPT, "cloned_guild_id": CLONE},
        ]
    }
    r._wh_meta = {SRC: {"custom": True}}
    r._wh_identity_state = {SRC: True}
    r._unmapped_warned = set()
    r._schedule_flush = lambda **kw: None
    r._load_mappings()
    return r


def _unlink(rx, orig=SRC, gid=CLONE):
    return asyncio.run(rx.unlink_channel_to_orphan(orig, gid))


def test_db_unlink_drops_the_row_its_threads_and_their_messages_in_that_clone_only(db):
    _seed(db)
    out = db.unlink_channel_mapping(SRC, CLONE)
    assert out["cloned_channel_id"] == OLD
    assert out["thread_ids"] == [THREAD]
    assert (out["threads_removed"], out["messages_removed"]) == (1, 2)
    assert db.get_channel_mapping_by_original_and_clone(SRC, CLONE) is None
    assert db.get_channel_mapping_by_clone_id(OLD) is None
    assert _count(db, "SELECT COUNT(*) FROM threads WHERE cloned_guild_id=?", CLONE) == 0
    assert _count(db, "SELECT COUNT(*) FROM messages WHERE cloned_channel_id IN (?, ?)", OLD, THREAD) == 0
    # other channels and the other clone guild are untouched
    assert db.get_channel_mapping_by_original_and_clone(SRC_KEPT, CLONE) is not None
    assert db.get_channel_mapping_by_original_and_clone(SRC, OTHER_CLONE)["cloned_channel_id"] == ELSEWHERE
    assert _count(db, "SELECT COUNT(*) FROM messages WHERE cloned_channel_id IN (?, ?)", KEPT, ELSEWHERE) == 2
    assert db.unlink_channel_mapping(SRC, CLONE) is None


def test_unlink_drops_the_caches_and_the_queued_webhook_and_leaves_the_channel(rx, guild):
    assert SRC in rx.chan_map_by_clone[CLONE]
    res = _unlink(rx)
    assert res == {"ok": True, "id": str(SRC), "cloned_channel_id": str(OLD), "name": "general"}
    assert SRC not in rx.chan_map_by_clone[CLONE]
    assert SRC_KEPT in rx.chan_map_by_clone[CLONE]
    assert rx.chan_map_by_clone[OTHER_CLONE][SRC]["cloned_channel_id"] == ELSEWHERE
    assert [e["cloned_channel_id"] for e in rx._pending_webhook_channels[CLONE]] == [KEPT]
    assert SRC not in rx._wh_meta and SRC not in rx._wh_identity_state
    assert rx.backfill.invalidated == [OLD]
    assert guild.get_channel(OLD) is not None and guild.chans[OLD].touched == []


def test_unlink_rejects_unmapped_missing_backfilling_and_unknown_guild(rx, guild):
    assert _unlink(rx, orig=424242) == {"ok": False, "id": "424242", "reason": "not_mapped"}
    assert _unlink(rx, gid=OTHER_CLONE)["reason"] == "guild_not_found"

    rx.backfill.running.add(SRC)
    assert _unlink(rx)["reason"] == "backfilling"
    assert rx.db.get_channel_mapping_by_original_and_clone(SRC, CLONE) is not None
    rx.backfill.running.clear()

    del guild.chans[KEPT]
    assert _unlink(rx, orig=SRC_KEPT)["reason"] == "not_found"
    assert rx.db.get_channel_mapping_by_original_and_clone(SRC_KEPT, CLONE) is not None

    assert _unlink(rx)["ok"] is True
    assert _unlink(rx) == {"ok": False, "id": str(SRC), "reason": "not_mapped"}


def _orphan_ids(rx, guild):
    """Orphan channel ids as the Channels page's verify list computes them."""
    vc = VerifyController(
        bus=MagicMock(), admin_base_url="http://admin", bot=rx.bot, guild_id=CLONE, db=rx.db,
        ratelimit=MagicMock(), get_protected_channel_ids=lambda g: [], action_type_delete_channel=None,
    )
    return [c["id"] for c in vc._compute_orphans(guild)[1]]


def test_the_unlinked_channel_shows_as_an_orphan(rx, guild):
    assert _orphan_ids(rx, guild) == []
    _unlink(rx)
    assert _orphan_ids(rx, guild) == [str(OLD)]


def test_the_next_sync_creates_a_new_channel_instead_of_reusing_the_orphan(rx, guild):
    async def create(g, kind, name, category, original_channel_id=None):
        ch = Channel(NEW, name)
        g.chans[NEW] = ch
        g.created.append((kind, name, original_channel_id))
        return ch

    rx._create_channel = create
    ensure = lambda: asyncio.run(rx._ensure_channel_and_webhook(HOST, guild, SRC, "general", None, None, 0))

    # still mapped: the sync reuses the clone
    assert ensure()[1] == OLD and guild.created == []

    _unlink(rx)
    assert ensure()[1] == NEW
    assert guild.created == [("text", "general", SRC)]
    row = rx.db.get_channel_mapping_by_original_and_clone(SRC, CLONE)
    assert row["cloned_channel_id"] == NEW and row["channel_webhook_url"] is None
    assert rx.chan_map_by_clone[CLONE][SRC]["cloned_channel_id"] == NEW
    assert guild.chans[OLD].touched == []
    assert _orphan_ids(rx, guild) == [str(OLD)]


def test_a_webhook_queued_before_the_unlink_does_not_map_the_orphan_again(rx, guild, monkeypatch):
    queued = list(rx._pending_webhook_channels[CLONE])
    _unlink(rx)
    made = []

    async def make(ch, name, avatar):
        made.append(ch.id)
        return SimpleNamespace(id=ch.id + 1, token="t")

    async def avatar():
        return None

    async def no_wait(_):
        return None

    # the creator spaces its creates by 1.5 s
    monkeypatch.setattr(asyncio, "sleep", no_wait)
    rx._create_webhook_safely = make
    rx._get_default_avatar_bytes = avatar
    entries = [
        {**e, "original_name": "x", "channel_type": 0, "host_guild_id": HOST} for e in queued
    ]
    asyncio.run(rx._create_pending_webhooks(entries))
    assert made == [KEPT]
    assert rx.db.get_channel_mapping_by_clone_id(OLD) is None
    assert rx.db.get_channel_mapping_by_original_and_clone(SRC, CLONE) is None


class FakeBus:
    def __init__(self):
        self.sent = []

    async def publish(self, kind, payload):
        self.sent.append({"kind": kind, "role": "server", "payload": payload})


def _verify(unlink, mapping_row):
    bus = FakeBus()
    db = MagicMock()
    db.get_mapping_by_id.return_value = mapping_row
    vc = VerifyController(
        bus=bus, admin_base_url="http://admin", bot=SimpleNamespace(get_guild=lambda gid: None),
        guild_id=OTHER_CLONE, db=db, ratelimit=MagicMock(), get_protected_channel_ids=lambda g: [],
        action_type_delete_channel=None, unlink_channel=unlink,
    )
    return vc, bus


def _reply(bus):
    frame = json.loads(json.dumps(bus.sent[-1]))
    assert frame["kind"] == "verify"
    return frame["payload"]


def test_verify_action_unlinks_in_the_mappings_clone_guild_and_replies():
    calls = []

    async def unlink(orig, gid):
        calls.append((orig, gid))
        return {"ok": True, "id": str(orig), "cloned_channel_id": str(OLD), "name": "general"}

    vc, bus = _verify(unlink, {"cloned_guild_id": CLONE})
    asyncio.run(vc._handle({"action": "unlink_channel", "mapping_id": "m1", "original_channel_id": str(SRC)}, req_id="r1"))
    assert calls == [(SRC, CLONE)]
    assert _reply(bus) == {
        "type": "unlinked", "req_id": "r1", "mapping_id": "m1", "id": str(SRC),
        "ok": True, "cloned_channel_id": str(OLD), "name": "general",
    }


def test_verify_action_never_falls_back_to_the_default_guild_and_checks_the_id():
    calls = []

    async def unlink(orig, gid):
        calls.append((orig, gid))
        return {"ok": True}

    vc, bus = _verify(unlink, None)
    asyncio.run(vc._handle({"action": "unlink_channel", "mapping_id": "gone", "original_channel_id": str(SRC)}, req_id="r2"))
    assert _reply(bus)["reason"] == "no_mapping" and _reply(bus)["ok"] is False

    vc, bus = _verify(unlink, {"cloned_guild_id": CLONE})
    asyncio.run(vc._handle({"action": "unlink_channel", "mapping_id": "m1", "original_channel_id": "abc"}, req_id="r3"))
    assert _reply(bus)["reason"] == "bad_id"
    assert calls == []
