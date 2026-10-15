"""Seed the throwaway e2e DB with rows whose API routes need a live Discord account."""

import os
import sys
from pathlib import Path

home = Path(os.environ["COPYCORD_HOME"])
db_path = os.environ.get("DB_PATH") or str(home / "data" / "data.db")

from copycord.common.db import DBManager  # noqa: E402

db = DBManager(db_path, init_schema=True)
if not db.list_guild_mappings():
    db.upsert_guild_mapping(
        mapping_id=None,
        mapping_name="E2E Mapping",
        original_guild_id=111111111111111111,
        original_guild_name="E2E Host",
        original_guild_icon_url=None,
        cloned_guild_id=222222222222222222,
        cloned_guild_name="E2E Clone",
        settings={},
    )
if not db.list_backup_tokens():
    db.add_backup_token("e2e.fake.backup-token-value", "E2E backup")

# One finished Backfill channels run like a real failed one, so /tasks/runs/<id> renders every card.
from copycord.admin.tasks.repo import SqliteTaskRepo  # noqa: E402
from copycord.admin.tasks.service import TaskRun  # noqa: E402

repo = SqliteTaskRepo(db)
runs, _ = repo.list_runs({"type": "backfill_channels"})
if runs:
    run_id = str(runs[0].id)
else:
    mapping = db.list_guild_mappings()[0]
    mapping_id = str(mapping["mapping_id"])
    inputs = {
        "mapping_id": mapping_id,
        "channel_ids": [str(333333333333333301 + i) for i in range(6)],
        "category_ids": [],
        "range": {"mode": "all", "since": None, "until": None, "last_n": None},
        "ignore_cloned": False,
        "mode": "resume",
    }
    outcomes = [
        ("general", "success", 120, None),
        ("announcements", "success", 48, None),
        ("rules", "success", 12, None),
        ("media", "success", 44, None),
        ("support", "failed", 0, "Missing access to the source channel"),
        ("off-topic", "cancelled", 0, "Cancelled after 0 messages"),
    ]
    steps, channels = [], []
    counts = {"success": 0, "failed": 0, "skipped": 0, "cancelled": 0}
    for i, (name, state, delivered, note) in enumerate(outcomes):
        cid = inputs["channel_ids"][i]
        started = f"2026-10-14T18:0{i}:00.000Z"
        finished = f"2026-10-14T18:0{i}:40.000Z"
        if state == "success":
            note = f"{delivered} messages"
        steps.append({
            "key": cid, "title": f"#{name}", "state": state, "label": None,
            "done": delivered, "total": delivered if state == "success" else None,
            "queue_position": None, "started_at": started, "finished_at": finished, "note": note,
        })
        channels.append({
            "channel_id": cid, "name": name, "job_id": f"e2e-{cid}", "outcome": state,
            "delivered": delivered, "error": note if state == "failed" else None,
            "started_at": started, "finished_at": finished, "checkpoint": None, "note": note,
        })
        counts[state] += 1
    delivered = sum(c["delivered"] for c in channels)
    summary = {**counts, "delivered": delivered}
    run = TaskRun(
        type="backfill_channels",
        number=0,
        status="failed",
        title="Backfill 6 channels",
        trigger="manual",
        actor="admin",
        inputs=inputs,
        context={"label": mapping["mapping_name"], "mapping_id": mapping_id,
                 "guild_id": str(mapping["original_guild_id"])},
        created_at="2026-10-14T18:00:00.000Z",
        started_at="2026-10-14T18:00:00.000Z",
        finished_at="2026-10-14T18:05:40.000Z",
        updated_at="2026-10-14T18:05:40.000Z",
        error=f"{counts['failed']} of {len(channels)} channels failed",
    )
    run.steps = steps
    run.summary = summary
    run.log_lines = [
        {"n": 1, "ts": "2026-10-14T18:00:00.000Z", "level": "info", "step": None,
         "msg": f"Backfilling {len(channels)} channels in {mapping['mapping_name']}"},
        {"n": 2, "ts": "2026-10-14T18:04:40.000Z", "level": "error", "step": channels[4]["channel_id"],
         "msg": f"Failed #support: {channels[4]['error']}"},
        {"n": 3, "ts": "2026-10-14T18:05:40.000Z", "level": "error", "step": None,
         "msg": run.error},
    ]
    run.log_count = len(run.log_lines)
    run.report = {
        "inputs": inputs,
        "mapping": {"mapping_id": mapping_id, "name": mapping["mapping_name"],
                    "original_guild_id": str(mapping["original_guild_id"]),
                    "cloned_guild_id": str(mapping["cloned_guild_id"])},
        "channels": channels,
        "summary": summary,
    }
    run_id = repo.insert_run(run)
print("seed_db: mappings", len(db.list_guild_mappings()), "backup tokens", len(db.list_backup_tokens()),
      "task run", run_id)
sys.exit(0)
