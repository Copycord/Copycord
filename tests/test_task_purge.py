"""Purge messages task type (lane L59): dry run, delete, mapping scope, validation and cancel
against a real DB, run through TaskService like backfill."""
import asyncio
import time

import pytest

from copycord.admin.tasks import purge
from copycord.admin.tasks.service import MemoryRunRepo, TaskService

DAY = 86400
A = (1000000000000000001, 1000000000000000002)
B = (2000000000000000001, 2000000000000000002)
STRAY = (3000000000000000001, 3000000000000000002)


def _msg(db, mid, pair, age_days):
    db.conn.execute(
        "INSERT INTO messages (original_message_id, original_guild_id, cloned_guild_id, created_at)"
        " VALUES (?, ?, ?, ?)",
        (mid, pair[0], pair[1], int(time.time()) - int(age_days * DAY)),
    )
    db.conn.commit()


def _left(db, pair=None):
    if pair is None:
        return db.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    return db.conn.execute(
        "SELECT COUNT(*) FROM messages WHERE original_guild_id = ? AND cloned_guild_id = ?", pair
    ).fetchone()[0]


@pytest.fixture()
def maps(db, monkeypatch):
    ids = {}
    for name, pair in (("Alpha", A), ("Beta", B)):
        ids[name] = db.upsert_guild_mapping(
            mapping_id=None, mapping_name=name, original_guild_id=pair[0],
            original_guild_name=f"{name} host", original_guild_icon_url=None,
            cloned_guild_id=pair[1], cloned_guild_name=f"{name} clone",
        )
    # 8 records. Alpha: 3 old (40, 50, 60 days), 1 new; Beta: 2 old, 1 new; a stray pair: 1 old.
    n = 1
    for pair, ages in ((A, (40, 50, 60, 1)), (B, (35, 90, 2)), (STRAY, (100,))):
        for age in ages:
            _msg(db, n, pair, age)
            n += 1
    monkeypatch.setattr(purge, "_wiring", purge._Wiring(db=db))
    return ids


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


def test_registered_with_a_schema():
    from copycord.admin.tasks import registry
    t = registry.get("purge_messages")
    assert t is not None and t.title == "Purge messages"
    props = t.inputs_schema()["properties"]
    assert set(props) == {"older_than", "unit", "mapping_id", "dry_run", "all_records"}
    assert props["unit"]["default"] == "days" and props["older_than"]["default"] == 30
    assert props["all_records"]["default"] is False


def test_old_inputs_still_parse():
    old = purge.PurgeInputs(**{"older_than": 900, "unit": "weeks", "mapping_id": None, "dry_run": False})
    assert old.all_records is False and old.seconds == 900 * 7 * DAY
    assert purge.PurgeInputs(all_records=True).seconds is None


def test_count_and_delete_any_age(db, maps):
    assert db.count_old_messages(None) == 8
    assert db.count_old_messages(None, pair=A) == 4
    assert db.count_old_messages(None, skip_pairs=[A, B]) == 1
    assert db.delete_old_messages(None, pair=A) == 4
    assert db.delete_old_messages(None, skip_pairs=[B]) == 1
    assert _left(db) == 3 and _left(db, B) == 3


def test_count_and_delete_by_pair(db, maps):
    assert db.count_old_messages(30 * DAY) == 6
    assert db.count_old_messages(30 * DAY, pair=A) == 3
    assert db.count_old_messages(30 * DAY, skip_pairs=[A, B]) == 1
    assert db.delete_old_messages(30 * DAY, pair=B) == 2
    assert _left(db, B) == 1 and _left(db, A) == 4


@pytest.mark.asyncio
async def test_dry_run_counts_and_deletes_nothing(svc, db, maps):
    run = await svc.start("purge_messages", {"older_than": 30, "unit": "days", "dry_run": True})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert done.title == "Count messages older than 30 days"
    assert done.summary == {"matched": 6, "deleted": 0}
    assert _left(db) == 8
    by = {r["name"]: r for r in done.report["mappings"]}
    assert by["Alpha"]["matched"] == 3 and by["Beta"]["matched"] == 2
    assert by["Not under a mapping"]["matched"] == 1
    assert done.report["dry_run"] is True
    assert {s["note"] for s in done.steps} >= {"3 would be deleted", "2 would be deleted"}


@pytest.mark.asyncio
async def test_delete_all_mappings(svc, db, maps):
    run = await svc.start("purge_messages", {"older_than": 30})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert done.title == "Purge messages older than 30 days"
    assert done.context["label"] == "All mappings"
    assert done.summary == {"matched": 6, "deleted": 6}
    assert _left(db) == 2 and _left(db, STRAY) == 0
    assert [r["deleted"] for r in done.report["mappings"]] == [3, 2, 1]


@pytest.mark.asyncio
async def test_delete_all_records(svc, db, maps):
    # 900 weeks finds nothing; All records ignores the age and takes the 1 and 2 day old ones too.
    run = await svc.start("purge_messages", {"older_than": 900, "unit": "weeks", "all_records": True})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert done.title == "Delete all message records"
    assert done.summary == {"matched": 8, "deleted": 8}
    assert _left(db) == 0
    assert [r["deleted"] for r in done.report["mappings"]] == [4, 3, 1]
    assert done.report["all_records"] is True and done.report["older_than_seconds"] is None
    assert done.report["inputs"]["all_records"] is True


@pytest.mark.asyncio
async def test_all_records_dry_run(svc, db, maps):
    run = await svc.start("purge_messages", {"all_records": True, "dry_run": True})
    done = await _settled(svc, run.id)
    assert done.title == "Count all message records"
    assert done.summary == {"matched": 8, "deleted": 0} and _left(db) == 8


@pytest.mark.asyncio
async def test_all_records_one_mapping(svc, db, maps):
    run = await svc.start("purge_messages", {"all_records": True, "mapping_id": maps["Beta"]})
    done = await _settled(svc, run.id)
    assert done.title == "Delete all message records" and done.context["label"] == "Beta"
    assert done.summary == {"matched": 3, "deleted": 3}
    assert _left(db, B) == 0 and _left(db, A) == 4 and _left(db, STRAY) == 1


@pytest.mark.asyncio
async def test_no_unmapped_step_when_there_are_none(svc, db, maps):
    db.conn.execute("DELETE FROM messages WHERE original_guild_id = ?", (STRAY[0],))
    db.conn.commit()
    run = await svc.start("purge_messages", {"older_than": 30})
    done = await _settled(svc, run.id)
    assert [s["title"] for s in done.steps] == ["Alpha", "Beta"]


@pytest.mark.asyncio
async def test_scope_to_one_mapping(svc, db, maps):
    run = await svc.start("purge_messages", {"older_than": 6, "unit": "weeks", "mapping_id": maps["Beta"]})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert done.context["label"] == "Beta"
    assert done.summary == {"matched": 1, "deleted": 1}
    assert _left(db, B) == 2 and _left(db, A) == 4 and _left(db, STRAY) == 1
    assert [s["key"] for s in done.steps] == [maps["Beta"]]


@pytest.mark.asyncio
async def test_hours_unit(svc, db, maps):
    run = await svc.start("purge_messages", {"older_than": 36, "unit": "hours", "mapping_id": maps["Alpha"]})
    done = await _settled(svc, run.id)
    # 40, 50 and 60 days are past 36 hours; the 1 day old record is not.
    assert done.summary == {"matched": 3, "deleted": 3} and _left(db, A) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [
    {"older_than": 0}, {"older_than": -3}, {"older_than": "abc"}, {"older_than": 1.5},
    {"older_than": True}, {"older_than": 5, "unit": "months"},
])
async def test_rejects_bad_inputs(svc, maps, raw):
    with pytest.raises(ValueError) as exc:
        await svc.start("purge_messages", raw)
    errors = exc.value.args[0]
    assert set(errors) & {"older_than", "unit"}


@pytest.mark.asyncio
async def test_unknown_mapping_fails_the_run(svc, db, maps):
    run = await svc.start("purge_messages", {"older_than": 1, "mapping_id": "nope"})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and _left(db) == 8


@pytest.mark.asyncio
async def test_cancel_stops_before_the_next_mapping(svc, db, maps, monkeypatch):
    real = purge._delete

    def cancel_after_first(db_, seconds, scope):
        n = real(db_, seconds, scope)
        for ctx, _ in svc._active.values():
            ctx.cancelled.set()
        return n

    monkeypatch.setattr(purge, "_delete", cancel_after_first)
    run = await svc.start("purge_messages", {"older_than": 30})
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    states = [s["state"] for s in done.steps]
    assert states == ["success", "cancelled", "cancelled"]  # Alpha, Beta, the stray record
    assert done.summary["deleted"] == 3 and _left(db, B) == 3


@pytest.mark.asyncio
async def test_options_preview(maps):
    out = await purge.options({"older_than": "30", "unit": "days"})
    assert out["matched"] == 6
    assert [r["matched"] for r in out["mappings"]] == [3, 2, 1]
    scoped = await purge.options({"older_than": "30", "mapping_id": maps["Alpha"]})
    assert scoped["matched"] == 3 and len(scoped["mappings"]) == 1
    bad = await purge.options({"older_than": "0"})
    assert "older_than" in bad["errors"]
    every = await purge.options({"all_records": True})
    assert every["matched"] == 8 and [r["matched"] for r in every["mappings"]] == [4, 3, 1]
    one = await purge.options({"all_records": "true", "mapping_id": maps["Alpha"]})
    assert one["matched"] == 4
