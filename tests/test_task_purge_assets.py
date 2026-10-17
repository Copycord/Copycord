"""Purge assets task type: dry run, cloned-only deletes, partial failure, a rate limit
wait, a kind the bot cannot purge, cancel and the preview, with a fake server bot sender, run
through TaskService like the other tasks."""
import asyncio
import itertools

import pytest

from copycord.admin.tasks import purge_assets
from copycord.admin.tasks.service import MemoryRunRepo, TaskService

HOST, CLONE = 1000000000000000001, 1000000000000000002


def _listing():
    return {
        "emojis": {"items": [
            {"id": "11", "name": "blob", "mapped": True},
            {"id": "12", "name": "wave", "mapped": True},
            {"id": "13", "name": "local", "mapped": False},
        ], "error": None},
        "stickers": {"items": [{"id": "21", "name": "hello", "mapped": True}], "error": None},
        "roles": {"items": [
            {"id": "31", "name": "Member", "mapped": True},
            {"id": "32", "name": "Staff", "mapped": False},
        ], "error": None, "above_bot": 1},
    }


class FakeServer:
    """The server agent: purge_assets_list answers from a listing, purge_assets_delete from
    per-id outcomes (default deleted). hold makes the delete of that id wait for release."""

    def __init__(self, listing=None, outcomes=None):
        self.listing = listing or _listing()
        self.outcomes = dict(outcomes or {})
        self.frames = []
        self.hold = None
        self.held = asyncio.Event()
        self.release = asyncio.Event()
        self.down = False

    @property
    def deletes(self):
        return [f["data"]["id"] for f in self.frames if f["type"] == "purge_assets_delete"]

    async def __call__(self, frame, timeout):
        self.frames.append(frame)
        if self.down:
            raise ConnectionError("server agent unreachable")
        data = frame["data"]
        if frame["type"] == "purge_assets_list":
            assert data["guild_id"] == str(CLONE)
            return {"ok": True, "guild_name": "Alpha clone",
                    "kinds": {k: v for k, v in self.listing.items() if k in data["kinds"]}}
        assert frame["type"] == "purge_assets_delete" and data["guild_id"] == str(CLONE)
        if self.hold == data["id"]:
            self.held.set()
            await self.release.wait()
        out = self.outcomes.get(data["id"], {"ok": True, "outcome": "deleted"})
        if isinstance(out, list):
            out = out.pop(0) if len(out) > 1 else out[0]
        return out


@pytest.fixture()
def mapping_id(db):
    return db.upsert_guild_mapping(
        mapping_id=None, mapping_name="Alpha", original_guild_id=HOST,
        original_guild_name="Alpha host", original_guild_icon_url=None,
        cloned_guild_id=CLONE, cloned_guild_name="Alpha clone",
    )


@pytest.fixture()
def server(db, monkeypatch):
    fake = FakeServer()
    monkeypatch.setattr(purge_assets, "_wiring", purge_assets._Wiring(db=db, send=fake))
    monkeypatch.setattr(purge_assets, "DELETE_GAP", 0)
    return fake


@pytest.fixture()
def svc():
    return TaskService(MemoryRunRepo(), cancel_grace=2.0)


async def _settled(svc, run_id, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        run = svc.get(run_id)
        if run.status in ("success", "failed", "cancelled") and run_id not in svc._active:
            return run
        await asyncio.sleep(0.01)
    raise AssertionError("run did not settle")


def _steps(run):
    return {s["key"]: s for s in run.steps}


@pytest.mark.asyncio
@pytest.mark.parametrize("raw, field", [
    ({"kinds": ["emojis"]}, "mapping_id"),
    ({"mapping_id": "m", "kinds": []}, "kinds"),
    ({"mapping_id": "m", "kinds": ["channels"]}, "kinds.0"),
    ({"mapping_id": "m", "scope": "host"}, "scope"),
])
async def test_rejects_bad_inputs(svc, server, raw, field):
    with pytest.raises(ValueError) as exc:
        await svc.start("purge_assets", raw)
    assert field in exc.value.args[0]


@pytest.mark.asyncio
async def test_dry_run_counts_cloned_assets_and_deletes_nothing(svc, server, mapping_id):
    run = await svc.start("purge_assets", {"mapping_id": mapping_id, "dry_run": True,
                                           "kinds": ["roles", "emojis"]})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert done.title == "Count cloned emojis and roles"
    assert done.context["label"] == "Alpha"
    assert server.deletes == []
    steps = _steps(done)
    assert list(steps) == ["emojis", "roles"]
    assert (steps["emojis"]["done"], steps["emojis"]["total"]) == (2, 2)
    assert steps["emojis"]["note"] == "2 would be deleted"
    assert done.summary == {"matched": 3, "deleted": 0, "failed": 0, "skipped": 0}
    emojis = done.report["kinds"][0]
    assert [i["name"] for i in emojis["items"]] == ["blob", "wave"] and emojis["kept"] == 1
    assert done.report["dry_run"] is True


@pytest.mark.asyncio
async def test_cloned_scope_deletes_only_mapped_assets_with_live_progress(server, mapping_id):
    seen = []
    # A clock a second on per read: every change publishes, so each delete's progress shows.
    svc = TaskService(MemoryRunRepo(), publish=lambda event: _record(seen, event),
                      clock=itertools.count(1_700_000_000).__next__, cancel_grace=2.0)
    run = await svc.start("purge_assets", {"mapping_id": mapping_id})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert done.title == "Purge cloned emojis, stickers and roles"
    assert server.deletes == ["11", "12", "21", "31"]
    assert all(f["data"]["scope"] == "cloned" for f in server.frames[1:])
    steps = _steps(done)
    assert [s["state"] for s in steps.values()] == ["success"] * 3
    assert (steps["emojis"]["done"], steps["emojis"]["total"], steps["emojis"]["note"]) == (2, 2, "2 deleted")
    assert done.summary == {"matched": 4, "deleted": 4, "failed": 0, "skipped": 0}
    # each delete moved its step on, labelled with the asset's name
    emoji_frames = [(s["done"], s["label"]) for e in seen for s in e["steps"] if s["key"] == "emojis"]
    assert (1, "blob") in emoji_frames and (2, "wave") in emoji_frames
    assert seen[-1]["run"]["summary"]["deleted"] == 4
    assert done.report["failures"] == []


async def _record(seen, event):
    seen.append(event)


@pytest.mark.asyncio
async def test_all_scope_takes_unmapped_assets_too(svc, server, mapping_id):
    run = await svc.start("purge_assets", {"mapping_id": mapping_id, "scope": "all", "kinds": ["emojis"]})
    done = await _settled(svc, run.id)
    assert done.title == "Purge all emojis"
    assert server.deletes == ["11", "12", "13"]


@pytest.mark.asyncio
async def test_partial_failure_lists_failures_and_fails_the_run(svc, server, mapping_id):
    server.outcomes = {
        "12": {"ok": True, "outcome": "failed", "error": "500 Internal Server Error"},
        "21": {"ok": True, "outcome": "skipped", "error": "Missing permission: Forbidden"},
    }
    run = await svc.start("purge_assets", {"mapping_id": mapping_id})
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert done.error == "1 of 4 assets failed"
    steps = _steps(done)
    assert steps["emojis"]["state"] == "failed" and steps["emojis"]["note"] == "1 deleted, 1 failed"
    assert steps["stickers"]["state"] == "success" and steps["stickers"]["note"] == "0 deleted, 1 skipped"
    assert steps["roles"]["state"] == "success"
    assert done.summary == {"matched": 4, "deleted": 2, "failed": 1, "skipped": 1}
    assert done.report["failures"] == [{"kind": "emojis", "id": "12", "name": "wave",
                                        "outcome": "failed", "error": "500 Internal Server Error"}]


@pytest.mark.asyncio
async def test_rate_limit_waits_and_retries(svc, server, mapping_id):
    server.outcomes = {"11": [{"ok": True, "outcome": "rate_limited", "retry_after": 0.01},
                              {"ok": True, "outcome": "deleted"}]}
    run = await svc.start("purge_assets", {"mapping_id": mapping_id, "kinds": ["emojis"]})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert server.deletes == ["11", "11", "12"]
    assert any("Rate limited" in line["msg"] for line in done.log_lines)


@pytest.mark.asyncio
async def test_kind_the_bot_cannot_purge_fails_alone(svc, server, mapping_id):
    server.listing["roles"] = {"items": [], "error": "The bot needs the Manage Roles permission"}
    run = await svc.start("purge_assets", {"mapping_id": mapping_id})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "1 of 3 asset kinds failed"
    steps = _steps(done)
    assert steps["roles"]["state"] == "failed"
    assert steps["roles"]["note"] == "The bot needs the Manage Roles permission"
    assert steps["emojis"]["state"] == "success" and server.deletes == ["11", "12", "21"]


@pytest.mark.asyncio
async def test_server_bot_not_answering_fails_the_run(svc, server, mapping_id):
    server.down = True
    run = await svc.start("purge_assets", {"mapping_id": mapping_id})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and "unreachable" in done.error
    assert all(s["state"] == "failed" for s in done.steps)


@pytest.mark.asyncio
async def test_cancel_stops_before_the_next_asset(svc, server, mapping_id):
    server.hold = "12"
    run = await svc.start("purge_assets", {"mapping_id": mapping_id})
    await asyncio.wait_for(server.held.wait(), 2.0)
    await svc.cancel(run.id)
    server.release.set()
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    # the delete in flight finishes; nothing after it is sent
    assert server.deletes == ["11", "12"]
    steps = _steps(done)
    assert steps["emojis"]["state"] == "success" and steps["emojis"]["done"] == 2
    assert steps["stickers"]["state"] == "cancelled" and steps["roles"]["state"] == "cancelled"
    assert done.summary["deleted"] == 2


@pytest.mark.asyncio
async def test_cancel_mid_kind_marks_the_step_cancelled(svc, server, mapping_id):
    server.hold = "11"
    run = await svc.start("purge_assets", {"mapping_id": mapping_id, "kinds": ["emojis"]})
    await asyncio.wait_for(server.held.wait(), 2.0)
    await svc.cancel(run.id)
    server.release.set()
    done = await _settled(svc, run.id)
    assert done.status == "cancelled" and server.deletes == ["11"]
    step = _steps(done)["emojis"]
    assert step["state"] == "cancelled" and step["note"] == "Cancelled after 1 deleted"
    assert (step["done"], step["total"]) == (1, 2)


@pytest.mark.asyncio
async def test_unknown_mapping_fails_the_run(svc, server, mapping_id):
    run = await svc.start("purge_assets", {"mapping_id": "nope"})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and server.frames == []


@pytest.mark.asyncio
async def test_options_preview(server, mapping_id, db):
    base = await purge_assets.options({})
    assert [m["name"] for m in base["mappings"]] == ["Alpha"]
    assert base["counts"] == {"emojis": None, "stickers": None, "roles": None}

    live = await purge_assets.options({"mapping_id": mapping_id})
    assert live["live"] is True and live["counts"] == {"emojis": 2, "stickers": 1, "roles": 1}
    assert live["notes"]["roles"] == "1 role at or above the bot's role is left alone"
    everything = await purge_assets.options({"mapping_id": mapping_id, "scope": "all"})
    assert everything["counts"] == {"emojis": 3, "stickers": 1, "roles": 2}
    unmapped = await purge_assets.options({"mapping_id": mapping_id, "scope": "unmapped"})
    assert unmapped["counts"] == {"emojis": 1, "stickers": 0, "roles": 1}

    # without the server bot, a cloned scope counts Copycord's mapping rows
    server.down = True
    db.upsert_emoji_mapping(501, "blob", 11, "blob", original_guild_id=HOST, cloned_guild_id=CLONE)
    db.upsert_role_mapping(601, "Member", 31, "Member", original_guild_id=HOST, cloned_guild_id=CLONE)
    fallback = await purge_assets.options({"mapping_id": mapping_id})
    assert fallback["live"] is False and fallback["error"]
    assert fallback["counts"] == {"emojis": 1, "stickers": 0, "roles": 1}
    other = await purge_assets.options({"mapping_id": mapping_id, "scope": "all"})
    assert other["counts"] == {"emojis": None, "stickers": None, "roles": None}

    assert (await purge_assets.options({"mapping_id": "nope"}))["error"] == "Unknown mapping"
