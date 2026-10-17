"""Sync assets (lane L66): the task with a fake server sender and job frames handed straight
to the dispatcher, plan_role_order, and the server's sync_assets job with a fake guild."""
import asyncio

import pytest

from copycord.admin.tasks import sync_assets
from copycord.admin.tasks.service import MemoryRunRepo, TaskService
from copycord.server import asset_sync
from copycord.server.asset_sync import AssetSync
from copycord.server.roles import plan_role_order

OG = 1234567890123456789
CG = 1234567890123456790


@pytest.fixture()
def mapping(db):
    return db.upsert_guild_mapping(
        mapping_id=None, mapping_name="Main", original_guild_id=OG,
        original_guild_name="Host", original_guild_icon_url=None,
        cloned_guild_id=CG, cloned_guild_name="Clone",
    )


@pytest.fixture()
def sent(db, monkeypatch):
    frames = []

    async def send(frame):
        frames.append(frame)
        return {"ok": True}

    monkeypatch.setattr(sync_assets, "_wiring", sync_assets._Wiring(db=db, send=send))
    monkeypatch.setattr(sync_assets, "CANCEL_WAIT", 0.2)
    sync_assets._jobs.clear()
    yield frames
    sync_assets._jobs.clear()


@pytest.fixture()
def svc():
    return TaskService(MemoryRunRepo(), cancel_grace=2.0)


def _of(frames, typ):
    return [f["data"] for f in frames if f["type"] == typ]


async def _until(cond, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


async def _settled(svc, run_id):
    await _until(lambda: svc.get(run_id).status in ("success", "failed", "cancelled")
                 and run_id not in svc._active)
    return svc.get(run_id)


def _steps(svc, run_id):
    return {s["key"]: s for s in svc.get(run_id).steps}


async def _frame(typ, **data):
    return await sync_assets.dispatch({"type": typ, "data": data})


# the task

def test_inputs_need_a_kind_and_keep_the_kind_order():
    assert sync_assets.SyncAssetsInputs(mapping_id="m", kinds=["stickers", "roles"]).kinds == \
        ["roles", "stickers"]
    with pytest.raises(Exception):
        sync_assets.SyncAssetsInputs(mapping_id="m", kinds=[])
    with pytest.raises(Exception):
        sync_assets.SyncAssetsInputs(mapping_id="m", kinds=["channels"])


def test_role_order_step_follows_roles():
    assert sync_assets.step_keys(["emojis", "roles", "stickers"]) == \
        ["roles", "role_order", "emojis", "stickers"]
    assert sync_assets.step_keys(["stickers"]) == ["stickers"]


@pytest.mark.asyncio
async def test_run_streams_items_and_ends_with_summary_and_report(svc, sent, mapping):
    run = await svc.start("sync_assets", {"mapping_id": mapping, "kinds": ["roles", "emojis"]})
    assert run.title == "Sync roles and emojis"
    assert run.context["label"] == "Main"
    await _until(lambda: _of(sent, "sync_assets"))
    req = _of(sent, "sync_assets")[0]
    job = req["job_id"]
    assert job == f"{run.id}:assets"
    assert req["kinds"] == ["roles", "emojis"]
    assert req["original_guild_id"] == str(OG) and req["cloned_guild_id"] == str(CG)
    assert req["delete_extras"] is False
    assert list(_steps(svc, run.id)) == ["roles", "role_order", "emojis"]
    assert _steps(svc, run.id)["role_order"]["title"] == "Role order"

    await _frame("sync_assets_step", job_id=job, step="roles", state="running", label=None)
    await _frame("sync_assets_progress", job_id=job, step="roles", done=1, total=3, label="Mods",
                 totals={"created": 1, "updated": 0, "reordered": 0, "skipped": 0, "failed": 0})
    step = _steps(svc, run.id)["roles"]
    assert step["state"] == "running" and step["done"] == 1 and step["total"] == 3
    assert step["label"] == "Mods"
    assert svc.get(run.id).summary["created"] == 1
    await _frame("sync_assets_step", job_id=job, step="roles", state="success",
                 note="1 created, 2 unchanged", done=3, total=3)
    await _frame("sync_assets_step", job_id=job, step="role_order", state="running",
                 label="Reading the clone's roles")
    assert _steps(svc, run.id)["role_order"]["label"] == "Reading the clone's roles"
    await _frame("sync_assets_log", job_id=job, level="warning", step="role_order",
                 msg="Could not move Bots: Managed by an integration")
    await _frame("sync_assets_step", job_id=job, step="role_order", state="success",
                 note="Moved 2 roles, 1 could not move", done=3, total=3)
    await _frame("sync_assets_step", job_id=job, step="emojis", state="running")
    await _frame("sync_assets_progress", job_id=job, step="emojis", done=1, total=1, label="pepe",
                 totals={"created": 1, "updated": 1, "reordered": 2, "skipped": 0, "failed": 1})
    await _frame("sync_assets_step", job_id=job, step="emojis", state="failed",
                 note="1 failed", done=1, total=1)
    result = {
        "kinds": {
            "roles": {"created": 1, "unchanged": 2, "items": [], "error": None},
            "emojis": {"failed": 1, "items": [{"name": "pepe", "outcome": "failed",
                                               "note": "Too big"}], "error": None},
        },
        "role_order": {"moved": 2, "error": None,
                       "unmovable": [{"id": 5, "name": "Bots", "reason": "Managed by an integration"}]},
    }
    await _frame("sync_assets_end", job_id=job, ok=True, error=None, cancelled=False, result=result,
                 totals={"created": 1, "updated": 1, "deleted": 0, "unchanged": 2, "reordered": 2,
                         "skipped": 0, "failed": 1})

    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert done.summary == {"created": 1, "updated": 1, "deleted": 0, "reordered": 2,
                            "skipped": 0, "failed": 1}
    steps = _steps(svc, run.id)
    assert steps["roles"]["state"] == "success" and steps["roles"]["note"] == "1 created, 2 unchanged"
    assert steps["role_order"]["note"] == "Moved 2 roles, 1 could not move"
    assert steps["emojis"]["state"] == "failed"
    assert done.report["failures"] == [{"kind": "emojis", "name": "pepe", "outcome": "failed",
                                        "note": "Too big"}]
    assert done.report["unmovable"][0]["name"] == "Bots"
    assert done.report["mapping"]["cloned_guild_id"] == str(CG)
    assert any(ln["msg"].startswith("Could not move Bots") and ln["step"] == "role_order"
               for ln in done.log_lines)


@pytest.mark.asyncio
async def test_refused_job_fails_every_step(svc, db, mapping, monkeypatch):
    async def send(frame):
        return {"ok": False, "error": "The server bot is not in the clone server"}

    monkeypatch.setattr(sync_assets, "_wiring", sync_assets._Wiring(db=db, send=send))
    run = await svc.start("sync_assets", {"mapping_id": mapping, "kinds": ["stickers"]})
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert _steps(svc, run.id)["stickers"]["state"] == "failed"
    assert _steps(svc, run.id)["stickers"]["note"] == "The server bot is not in the clone server"
    assert done.report["error"] == "The server bot is not in the clone server"
    assert not sync_assets._jobs


@pytest.mark.asyncio
async def test_cancel_asks_the_server_to_stop_and_ends_cancelled(svc, sent, mapping):
    run = await svc.start("sync_assets", {"mapping_id": mapping, "kinds": ["emojis", "stickers"]})
    await _until(lambda: _of(sent, "sync_assets"))
    job = _of(sent, "sync_assets")[0]["job_id"]
    await _frame("sync_assets_step", job_id=job, step="emojis", state="running")
    await _frame("sync_assets_progress", job_id=job, step="emojis", done=2, total=9, label="kek")
    await svc.cancel(run.id)
    await _until(lambda: _of(sent, "sync_assets_cancel"))
    assert _of(sent, "sync_assets_cancel")[0]["job_id"] == job
    await _frame("sync_assets_step", job_id=job, step="emojis", state="cancelled",
                 note="2 unchanged, then cancelled", done=2, total=9)
    await _frame("sync_assets_end", job_id=job, ok=False, error=None, cancelled=True,
                 result={"kinds": {}, "role_order": None}, totals={})
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    steps = _steps(svc, run.id)
    assert steps["emojis"]["state"] == "cancelled" and steps["emojis"]["done"] == 2
    assert steps["stickers"]["state"] == "cancelled"


@pytest.mark.asyncio
async def test_frames_of_other_jobs_are_not_ours():
    assert await sync_assets.dispatch({"type": "sync_assets_progress", "data": {"job_id": "x"}}) is False
    assert await sync_assets.dispatch({"type": "backfill_job", "data": {"job_id": "x"}}) is False


# plan_role_order

def _role(rid, pos, name=None, managed=False, default=False):
    return {"id": rid, "name": name or f"r{rid}", "position": pos, "managed": managed,
            "default": default}


def test_plan_reverses_to_the_host_order():
    host = [{"id": 1, "position": 3}, {"id": 2, "position": 2}, {"id": 3, "position": 1}]
    clone = [_role(100, 0, default=True), _role(11, 1), _role(12, 2), _role(13, 3), _role(99, 10)]
    plan = plan_role_order(host, {1: 11, 2: 12, 3: 13}, clone, 10, 99)
    assert plan["order"] == [13, 12, 11]
    assert plan["positions"] == {13: 1, 12: 2, 11: 3}
    assert plan["moved"] == 2 and plan["changed"] is True


def test_plan_keeps_unmapped_clone_roles_above_the_cloned_ones():
    host = [{"id": 1, "position": 1}, {"id": 2, "position": 2}]
    clone = [_role(100, 0, default=True), _role(50, 1, "Local"), _role(12, 2), _role(11, 3),
             _role(99, 10)]
    plan = plan_role_order(host, {1: 11, 2: 12}, clone, 10, 99)
    assert plan["order"] == [11, 12, 50]
    assert plan["cloned"] == 2


def test_plan_caps_at_the_bot_top_role_and_lists_what_cannot_move():
    host = [{"id": 1, "position": 1}, {"id": 2, "position": 2}, {"id": 3, "position": 3},
            {"id": 4, "position": 4}]
    clone = [_role(100, 0, default=True), _role(12, 1), _role(11, 2), _role(14, 3, "Bots", managed=True),
             _role(99, 4, "Copycord"), _role(13, 5, "Admin")]
    plan = plan_role_order(host, {1: 11, 2: 12, 3: 13, 4: 14}, clone, 4, 99)
    assert plan["order"] == [11, 12]
    reasons = {u["name"]: u["reason"] for u in plan["unmovable"]}
    assert reasons == {"Admin": "Above my top role", "Bots": "Managed by an integration"}
    assert 99 not in plan["positions"] and 13 not in plan["positions"]


def test_plan_skips_host_roles_without_a_clone():
    host = [{"id": 1, "position": 1}, {"id": 2, "position": 2}, {"id": 3, "position": 3}]
    clone = [_role(100, 0, default=True), _role(13, 1), _role(11, 2), _role(99, 9)]
    plan = plan_role_order(host, {1: 11, 3: 13, 2: 777}, clone, 9, 99)
    assert plan["order"] == [11, 13]
    assert plan["moved"] == 2


# the server's sync_assets job

class _Role:
    def __init__(self, rid, name, position, managed=False, default=False):
        self.id, self.name, self.position, self.managed = rid, name, position, managed
        self._default = default

    def is_default(self):
        return self._default


class _Me:
    def __init__(self, top):
        self.top_role = top


class _Guild:
    def __init__(self, roles, bot_role):
        self.id = CG
        self.features = []
        self._roles = roles
        self.me = _Me(bot_role)
        self.moves = []

    async def fetch_roles(self):
        return list(self._roles)

    async def edit_role_positions(self, positions, reason=None):
        self.moves.append({int(o.id): p for o, p in positions.items()})


class _Manager:
    """A role, emoji or sticker manager whose _sync reports a fixed list of items."""

    def __init__(self, items, mapped=None):
        self.items = items
        self.mapped = mapped or {}
        self.calls = []
        self._lock = asyncio.Lock()

    def _get_lock_for_clone(self, clone_id):
        return self._lock

    async def _sync(self, *args, hooks=None, **kwargs):
        self.calls.append(kwargs)
        await hooks.begin(len(self.items))
        for name, outcome, note in self.items:
            if hooks.stop():
                return
            await hooks.item(name, outcome, note)

    def set_last_sitemap(self, *args):
        pass

    async def refresh_cache(self, *args):
        pass

    def mapped_roles(self, host_id, clone_id):
        return self.mapped


class _WS:
    def __init__(self, snap):
        self.snap = snap
        self.requests = []

    async def request(self, payload, timeout=None):
        self.requests.append(payload)
        return self.snap


class _Bus:
    def __init__(self):
        self.frames = []

    async def publish(self, kind, payload):
        self.frames.append((kind, payload))


class _Bot:
    def __init__(self, guild, ws):
        self.guild, self.ws_manager = guild, ws

    def get_guild(self, gid):
        return self.guild if self.guild and gid == self.guild.id else None


class _Server:
    def __init__(self, guild, snap, roles, emojis=None, stickers=None):
        self.bot = _Bot(guild, _WS(snap))
        self.bus = _Bus()
        self.roles = roles
        self.emojis = emojis or _Manager([])
        self.stickers = stickers or _Manager([])


def _frames(server, typ):
    return [p["data"] for kind, p in server.bus.frames if p["type"] == typ and kind == "client"]


def _host_and_clone():
    snap = {"ok": True,
            "roles": [{"id": 1, "name": "Admin", "position": 2}, {"id": 2, "name": "Mods", "position": 1}],
            "emojis": [{"id": 7, "name": "pepe"}], "stickers": []}
    bot = _Role(99, "Copycord", 5)
    guild = _Guild([_Role(CG, "@everyone", 0, default=True), _Role(11, "Admin", 1),
                    _Role(12, "Mods", 2), bot], bot)
    return snap, guild


@pytest.mark.asyncio
async def test_ws_job_streams_items_moves_roles_and_ends(monkeypatch):
    monkeypatch.setattr(asset_sync, "PROGRESS_INTERVAL", 0.0)
    snap, guild = _host_and_clone()
    roles = _Manager([("Admin", "updated", None), ("Mods", "unchanged", None)], mapped={1: 11, 2: 12})
    emojis = _Manager([("pepe", "failed", "Too big")])
    server = _Server(guild, snap, roles, emojis)
    sync = AssetSync(server)
    res = await sync.start({"job_id": "r1:assets", "original_guild_id": str(OG),
                            "cloned_guild_id": str(CG), "kinds": ["emojis", "roles"]})
    assert res == {"ok": True, "job_id": "r1:assets", "state": "accepted"}
    await _until(lambda: _frames(server, "sync_assets_end"))

    assert server.bot.ws_manager.requests[0]["type"] == "asset_snapshot"
    assert roles.calls[0]["delete_roles"] is False and roles.calls[0]["rearrange_roles"] is False
    labels = [f["label"] for f in _frames(server, "sync_assets_progress") if f["step"] == "roles"]
    assert "Admin" in labels and "Mods" in labels
    steps = [(f["step"], f["state"]) for f in _frames(server, "sync_assets_step")]
    assert steps == [("roles", "running"), ("roles", "success"), ("role_order", "running"),
                     ("role_order", "success"), ("emojis", "running"), ("emojis", "failed")]
    assert guild.moves == [{12: 1, 11: 2}]
    end = _frames(server, "sync_assets_end")[0]
    assert end["ok"] is True and end["cancelled"] is False
    assert end["totals"]["updated"] == 1 and end["totals"]["failed"] == 1
    assert end["totals"]["reordered"] == 2
    assert end["result"]["kinds"]["emojis"]["items"] == [{"name": "pepe", "outcome": "failed",
                                                          "note": "Too big"}]
    assert not sync._jobs


@pytest.mark.asyncio
async def test_ws_job_refuses_without_the_clone_or_kinds():
    snap, guild = _host_and_clone()
    sync = AssetSync(_Server(None, snap, _Manager([])))
    res = await sync.start({"job_id": "j", "original_guild_id": "1", "cloned_guild_id": str(CG),
                            "kinds": ["roles"]})
    assert res["ok"] is False and "not in the clone server" in res["error"]
    sync = AssetSync(_Server(guild, snap, _Manager([])))
    res = await sync.start({"job_id": "j", "original_guild_id": "1", "cloned_guild_id": str(CG),
                            "kinds": ["channels"]})
    assert res == {"ok": False, "error": "No asset kinds to sync"}


@pytest.mark.asyncio
async def test_ws_job_cancel_stops_between_items(monkeypatch):
    monkeypatch.setattr(asset_sync, "PROGRESS_INTERVAL", 0.0)
    snap, guild = _host_and_clone()
    gate = asyncio.Event()

    class _Slow(_Manager):
        async def _sync(self, *args, hooks=None, **kwargs):
            await hooks.begin(3)
            for name in ("a", "b", "c"):
                if hooks.stop():
                    return
                await hooks.item(name, "created")
                await gate.wait()

    server = _Server(guild, snap, _Manager([]), _Slow([]))
    sync = AssetSync(server)
    await sync.start({"job_id": "j2", "original_guild_id": "1", "cloned_guild_id": str(CG),
                      "kinds": ["emojis", "stickers"]})
    await _until(lambda: any(f["label"] == "a" for f in _frames(server, "sync_assets_progress")))
    assert sync.cancel("j2")["state"] == "cancelling"
    gate.set()
    await _until(lambda: _frames(server, "sync_assets_end"))
    end = _frames(server, "sync_assets_end")[0]
    assert end["cancelled"] is True and end["ok"] is False
    assert end["totals"]["created"] == 1
    emoji_end = [f for f in _frames(server, "sync_assets_step") if f["step"] == "emojis"][-1]
    assert emoji_end["state"] == "cancelled"
    assert not [f for f in _frames(server, "sync_assets_step") if f["step"] == "stickers"]
