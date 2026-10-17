"""Cancel stops every task type at once (lane L77): each run is held mid-way on a fake Discord or
agent call that never returns (an asyncio.Event never set), cancelled, and must end cancelled
within 1 s with nothing changed after the cancel. Sync assets runs end to end, the admin task
wired to the server's AssetSync and the real StickerManager, as in the "Created sticker 6 s
after Cancelling" report."""
import asyncio
import time

import pytest

from copycord.admin.tasks import avatars, backfill, export_dms, purge, purge_assets, sync_assets
from copycord.admin.tasks.service import MemoryRunRepo, TaskService
from copycord.server.asset_sync import AssetSync
from copycord.server.item_hooks import ItemHooks
from copycord.server.rate_limiter import RateLimiter, RateLimitManager
from copycord.server.stickers import StickerManager

OG = 1234567890123456789
CG = 1234567890123456790
FINAL = ("success", "failed", "cancelled")


@pytest.fixture()
def svc():
    return TaskService(MemoryRunRepo(), cancel_grace=5.0)


async def _until(cond, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


async def _cancel_and_time(svc, run_id):
    """Cancel, wait for the run to end; (run, seconds from the click to the end)."""
    t0 = time.monotonic()
    await svc.cancel(run_id)
    await _until(lambda: svc.get(run_id).status in FINAL and run_id not in svc._active)
    return svc.get(run_id), time.monotonic() - t0


def _mapping(db):
    return db.upsert_guild_mapping(
        mapping_id=None, mapping_name="Main", original_guild_id=OG,
        original_guild_name="Host", original_guild_icon_url=None,
        cloned_guild_id=CG, cloned_guild_name="Clone",
    )


# the rate limiter

@pytest.mark.asyncio
async def test_rate_limit_wait_wakes_on_the_stop_event():
    lim = RateLimiter(1, 60.0)
    await lim.acquire()
    stop = asyncio.Event()
    waiting = asyncio.ensure_future(lim.acquire(stop))
    await asyncio.sleep(0.05)
    assert not waiting.done()
    t0 = time.monotonic()
    stop.set()
    await asyncio.wait_for(waiting, 1.0)
    assert time.monotonic() - t0 < 0.5


def test_item_hooks_check_raises_once_stopped():
    class _Stopped(ItemHooks):
        def stop(self):
            return True

    ItemHooks().check()
    with pytest.raises(asyncio.CancelledError):
        _Stopped().check()


# sync assets: the admin task, the server job and the real sticker pass

class _StickerDb:
    def __init__(self):
        self.upserts = []

    def get_all_sticker_mappings(self):
        return []

    def upsert_sticker_mapping(self, *args, **kwargs):
        self.upserts.append(args[1] if len(args) > 1 else kwargs)

    def delete_sticker_mapping_for_clone(self, *args, **kwargs):
        pass


class _Resp:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def read(self):
        return b"png"


class _Session:
    closed = False

    def get(self, url):
        return _Resp()


class _Created:
    def __init__(self, name):
        self.id, self.name = abs(hash(name)) % 10**18, name


class _CloneGuild:
    id = CG
    name = "Clone"
    sticker_limit = 60
    features = []

    def __init__(self, block_on=None):
        self.block_on = block_on
        self.started = []
        self.created = []
        self.never = asyncio.Event()

    async def fetch_stickers(self):
        return []

    async def create_sticker(self, *, name, **kwargs):
        self.started.append(name)
        if name == self.block_on:
            # The Discord call that never answers.
            await self.never.wait()
        self.created.append(name)
        return _Created(name)


class _Bus:
    async def publish(self, kind, payload):
        await sync_assets.dispatch(payload)


class _WS:
    def __init__(self, stickers):
        self.stickers = stickers

    async def request(self, payload, timeout=None):
        return {"ok": True, "roles": [], "emojis": [], "stickers": self.stickers}


class _Bot:
    def __init__(self, guild, stickers):
        self.guild, self.ws_manager = guild, _WS(stickers)

    def get_guild(self, gid):
        return self.guild if gid == self.guild.id else None


class _Server:
    def __init__(self, guild, stickers):
        self.bot = _Bot(guild, stickers)
        self.bus = _Bus()
        self.sticker_db = _StickerDb()
        self.stickers = StickerManager(self.bot, self.sticker_db, None, RateLimitManager(),
                                       session=_Session())
        self.roles = self.emojis = None


def _host_stickers(*names):
    return [{"id": 9000 + i, "name": n, "url": f"https://cdn/{n}.png", "format_type": 1,
             "tags": "x", "description": f"{n} sticker"} for i, n in enumerate(names)]


@pytest.fixture()
def wired(db, monkeypatch):
    """The admin's sync_assets sends straight to a server AssetSync; its frames come back
    through dispatch, as the bus watcher hands them over."""
    holder = {}

    async def send(frame):
        holder["frames"].append(frame)
        sync = holder["sync"]
        if frame["type"] == "sync_assets":
            return await sync.start(frame["data"])
        if frame["type"] == "sync_assets_cancel":
            return sync.cancel(frame["data"]["job_id"])
        return {"ok": True}

    def make(guild, stickers):
        server = _Server(guild, stickers)
        holder.update(sync=AssetSync(server), server=server, frames=[])
        return server

    monkeypatch.setattr(sync_assets, "_wiring", sync_assets._Wiring(db=db, send=send))
    sync_assets._jobs.clear()
    yield make, holder
    sync_assets._jobs.clear()


@pytest.mark.asyncio
async def test_sync_assets_cancel_during_the_sticker_rate_limit_wait_creates_nothing_more(svc, db, wired):
    """Mac's log: Cancelling job ...:assets at 02:30:16, Created sticker benny_wizard at 02:30:22.
    The first sticker takes the one slot a minute; benny_wizard waits out the limit when the
    cancel lands and must never be created."""
    make, holder = wired
    guild = _CloneGuild()
    server = make(guild, _host_stickers("first", "benny_wizard"))
    mid = _mapping(db)
    run = await svc.start("sync_assets", {"mapping_id": mid, "kinds": ["stickers"]})
    await _until(lambda: guild.created == ["first"])
    await asyncio.sleep(0.1)  # benny_wizard is downloaded and waiting on the limiter
    done, took = await _cancel_and_time(svc, run.id)
    assert took < 1.0
    assert done.status == "cancelled"
    await asyncio.sleep(0.2)
    assert guild.started == ["first"] and guild.created == ["first"]
    assert server.sticker_db.upserts == ["first"]
    assert done.summary["created"] == 1
    assert {s["key"]: s["state"] for s in done.steps} == {"stickers": "cancelled"}
    assert not holder["sync"]._jobs


@pytest.mark.asyncio
async def test_sync_assets_cancel_during_a_create_that_never_answers(svc, db, wired, monkeypatch):
    make, holder = wired
    guild = _CloneGuild(block_on="benny_wizard")
    server = make(guild, _host_stickers("benny_wizard", "after"))
    end_frames = []
    real_dispatch = sync_assets.dispatch

    async def spy(frame):
        if frame.get("type") == "sync_assets_end":
            end_frames.append(frame["data"])
        return await real_dispatch(frame)

    monkeypatch.setattr(sync_assets, "dispatch", spy)
    mid = _mapping(db)
    run = await svc.start("sync_assets", {"mapping_id": mid, "kinds": ["stickers"]})
    await _until(lambda: guild.started == ["benny_wizard"])
    done, took = await _cancel_and_time(svc, run.id)
    assert took < 1.0
    assert done.status == "cancelled"
    await asyncio.sleep(0.2)
    # Never reported created, never mapped, and the next sticker never started.
    assert guild.created == [] and guild.started == ["benny_wizard"]
    assert server.sticker_db.upserts == []
    assert done.summary["created"] == 0
    assert len(end_frames) == 1 and end_frames[0]["cancelled"] is True and end_frames[0]["ok"] is False
    assert end_frames[0]["totals"]["created"] == 0
    assert not holder["sync"]._jobs


@pytest.mark.asyncio
async def test_server_job_cancelled_before_its_first_step_still_sends_a_cancelled_end(db, wired):
    make, holder = wired
    guild = _CloneGuild()
    server = make(guild, _host_stickers("one"))
    ends = []

    class _Spy:
        async def publish(self, kind, payload):
            if payload["type"] == "sync_assets_end":
                ends.append(payload["data"])

    server.bus = _Spy()
    sync = holder["sync"]
    await sync.start({"job_id": "j:assets", "original_guild_id": str(OG), "cloned_guild_id": str(CG),
                      "kinds": ["stickers"]})
    sync.cancel("j:assets")
    await _until(lambda: ends)
    assert ends[0]["cancelled"] is True and guild.started == []


# purge assets

class _PurgeServer:
    def __init__(self):
        self.deletes = []
        self.never = asyncio.Event()

    async def __call__(self, frame, timeout):
        data = frame["data"]
        if frame["type"] == "purge_assets_list":
            return {"ok": True, "kinds": {"emojis": {"items": [
                {"id": "11", "name": "blob", "mapped": True},
                {"id": "12", "name": "wave", "mapped": True}], "error": None}}}
        self.deletes.append(data["id"])
        await self.never.wait()


@pytest.mark.asyncio
async def test_purge_assets_cancel_during_a_delete_that_never_answers(svc, db, monkeypatch):
    server = _PurgeServer()
    monkeypatch.setattr(purge_assets, "_wiring", purge_assets._Wiring(db=db, send=server))
    monkeypatch.setattr(purge_assets, "DELETE_GAP", 0)
    mid = _mapping(db)
    run = await svc.start("purge_assets", {"mapping_id": mid, "kinds": ["emojis"]})
    await _until(lambda: server.deletes == ["11"])
    done, took = await _cancel_and_time(svc, run.id)
    assert took < 1.0
    assert done.status == "cancelled"
    await asyncio.sleep(0.2)
    assert server.deletes == ["11"]
    step = {s["key"]: s for s in done.steps}["emojis"]
    assert step["state"] == "cancelled" and done.summary["deleted"] == 0


# avatars

@pytest.mark.asyncio
async def test_avatars_cancel_during_a_download_that_never_answers(svc, tmp_path, monkeypatch):
    never = asyncio.Event()
    urls = []
    members = [{"id": str(100 + i), "username": f"u{i}", "global_name": None, "discriminator": "0",
                "avatar": f"h{i}"} for i in range(3)]

    async def client(frame):
        typ, data = frame["type"], frame["data"]
        if typ == "avatars_members":
            async def stream():
                await avatars.dispatch({"type": "avatars_members_end", "data": {
                    "job_id": data["job_id"], "ok": True, "found": 3, "member_count": 3}})
            asyncio.ensure_future(stream())
            return {"ok": True}
        if typ == "avatars_members_page":
            off, lim = data["offset"], data["limit"]
            return {"ok": True, "state": "done", "found": 3, "member_count": 3,
                    "members": members[off:off + lim] if lim else []}
        return {"ok": True}

    async def fetch(url):
        urls.append(url)
        if len(urls) == 2:
            await never.wait()
        return 200, b"img", {}

    monkeypatch.setattr(avatars, "_wiring", avatars._Wiring(
        send=client, data_dir=tmp_path, fetch=fetch, clock=time.time))
    monkeypatch.setattr(avatars, "DOWNLOAD_INTERVAL", 0)
    monkeypatch.setattr(avatars, "STATUS_POLL", 0.05)
    avatars._jobs.clear()
    run = await svc.start("download_avatars", {"guild_id": "1234567890123456789"})
    await _until(lambda: len(urls) == 2)
    done, took = await _cancel_and_time(svc, run.id)
    assert took < 1.0
    assert done.status == "cancelled"
    assert len(urls) == 2
    assert done.summary["downloaded"] == 1 and done.summary["failed"] == 0
    assert done.summary["skipped"] == 2
    avatars._jobs.clear()


# export DMs

@pytest.mark.asyncio
async def test_export_dms_cancel_during_a_forward_that_never_answers(svc, monkeypatch):
    """The client's exporter is held on a webhook forward; the cancel cancels its task, which
    sends export_dm_done cancelled as the real DmHistoryExporter does."""
    never = asyncio.Event()
    forwards = []
    holder = {}

    async def exporter(job_id):
        try:
            for n in range(5):
                forwards.append(n)
                await never.wait()
        except asyncio.CancelledError:
            await export_dms.dispatch({"type": "export_dm_done", "data": {
                "job_id": job_id, "error": "cancelled", "read": 5, "sent": 0}})
            raise

    async def client(frame):
        data = frame["data"]
        if frame["type"] == "export_dm_history":
            holder["task"] = asyncio.ensure_future(exporter(data["job_id"]))
        elif frame["type"] == "export_dm_cancel":
            task = holder["task"]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return {"ok": True}

    monkeypatch.setattr(export_dms, "_wiring", export_dms._Wiring(send=client))
    export_dms._jobs.clear()
    run = await svc.start("export_dms", {"user_id": "123456789012345678",
                                         "webhook_url": "https://discord.com/api/webhooks/111/secret-token"})
    await _until(lambda: forwards == [0])
    done, took = await _cancel_and_time(svc, run.id)
    assert took < 1.0
    assert done.status == "cancelled"
    await asyncio.sleep(0.1)
    assert forwards == [0]
    assert all(s["state"] in ("cancelled", "success", "skipped") for s in done.steps)
    export_dms._jobs.clear()


# backfill

@pytest.mark.asyncio
async def test_backfill_cancel_with_a_client_that_never_ends(svc, db, monkeypatch):
    cat, ch = 1111111111111111111, 2222222222222222221
    mid = _mapping(db)
    db.upsert_category_mapping(cat, "News", 3333333333333333333, original_guild_id=OG, cloned_guild_id=CG)
    db.upsert_channel_mapping(
        original_channel_id=ch, original_channel_name="chan", cloned_channel_id=4444444444444444440,
        channel_webhook_url=None, original_parent_category_id=cat,
        cloned_parent_category_id=3333333333333333333, channel_type=0,
        original_guild_id=OG, cloned_guild_id=CG,
    )
    frames = []

    async def client(frame):
        frames.append(frame)
        return {"ok": True}

    monkeypatch.setattr(backfill, "_wiring", backfill._Wiring(db=db, send=client))
    backfill._jobs.clear()
    backfill._late.clear()
    run = await svc.start("backfill_channels", {"mapping_id": mid, "category_ids": [str(cat)]})
    await _until(lambda: any(f["type"] == "backfill_run" for f in frames))
    job_id = f"{run.id}:{ch}"
    await backfill.dispatch({"type": "backfill_job", "data": {"job_id": job_id, "state": "running"}})
    done, took = await _cancel_and_time(svc, run.id)
    # CANCEL_WAIT (1 s) for an end that never comes, then the step ends cancelled.
    assert took < backfill.CANCEL_WAIT + 0.5
    assert done.status == "cancelled"
    assert [f["data"]["job_id"] for f in frames if f["type"] == "backfill_cancel"] == [job_id]
    assert {s["state"] for s in done.steps} == {"cancelled"}
    backfill._jobs.clear()
    backfill._late.clear()


# purge messages (the DB only, no Discord)

@pytest.mark.asyncio
async def test_purge_messages_cancel_stops_before_the_next_mapping(svc, db, monkeypatch):
    monkeypatch.setattr(purge, "_wiring", purge._Wiring(db=db))
    scopes = [{"key": f"m{i}", "name": f"Map {i}", "mapping_id": f"m{i}", "pair": (i, i), "skip": []}
              for i in range(3)]
    deleted = []
    holder = {}

    def fake_delete(db_, seconds, scope):
        deleted.append(scope["key"])
        # The cancel lands while this mapping's delete runs.
        svc._active[holder["id"]][0].cancelled.set()
        return 4

    monkeypatch.setattr(purge, "_scopes", lambda *a, **k: scopes)
    monkeypatch.setattr(purge, "_count", lambda *a, **k: 4)
    monkeypatch.setattr(purge, "_delete", fake_delete)
    run = await svc.start("purge_messages", {"older_than": 30, "unit": "days"})
    holder["id"] = run.id
    t0 = time.monotonic()
    await _until(lambda: svc.get(run.id).status in FINAL and run.id not in svc._active)
    done = svc.get(run.id)
    assert time.monotonic() - t0 < 1.0
    assert done.status == "cancelled"
    assert deleted == ["m0"]
    states = [s["state"] for s in done.steps]
    assert states == ["success", "cancelled", "cancelled"]
