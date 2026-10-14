"""A non-resume backfill_started cancels earlier running backfill_runs rows for
the channel but keeps the row the admin inserted for this job (run_id = job_id)."""
from types import SimpleNamespace

import pytest

from copycord.server.backfill import BackfillManager

OG = 1234567890123456789
CG = 1234567890123456790
CID = 2222222222222222221
OTHER_CID = 2222222222222222229
JOB = "41:2222222222222222221"


def _status(db, run_id):
    return db.conn.execute(
        "SELECT status FROM backfill_runs WHERE run_id=?", (run_id,)
    ).fetchone()[0]


def _manager(db):
    rx = SimpleNamespace(db=db, bot=None, ratelimit=object(), chan_map={})
    return BackfillManager(rx)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_clone", [True, False])
async def test_non_resume_start_keeps_the_admin_row(db, with_clone):
    gids = {"original_guild_id": OG, "cloned_guild_id": CG}
    db.backfill_create_run(CID, {}, run_id="stale", **gids)
    db.backfill_create_run(OTHER_CID, {}, run_id="elsewhere", **gids)
    db.backfill_create_run(CID, {"mode": "all"}, run_id=JOB, task_run_id=41, **gids)

    meta = {"job_id": JOB, "resume": False}
    if with_clone:
        meta.update(cloned_guild_id=CG, original_guild_id=OG)
    mgr = _manager(db)
    await mgr.on_started(CID, meta=meta)

    assert _status(db, JOB) == "running"
    assert _status(db, "stale") == "cancelled"
    assert _status(db, "elsewhere") == "running"
    assert mgr._progress[CID]["run_id"] == JOB
    assert mgr._progress[CID]["task_id"] == JOB
    assert mgr.is_backfilling(CID)


@pytest.mark.asyncio
async def test_non_resume_start_without_job_id_still_cancels_every_running_row(db):
    db.backfill_create_run(CID, {}, run_id="stale", original_guild_id=OG, cloned_guild_id=CG)
    mgr = _manager(db)
    await mgr.on_started(CID, meta={"cloned_guild_id": CG})
    assert _status(db, "stale") == "cancelled"
    new_id = mgr._progress[CID]["run_id"]
    assert new_id != "stale" and _status(db, new_id) == "running"
