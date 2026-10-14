"""task_runs table and the DBManager task-run methods (Tasks framework, lane L2)."""

import sqlite3
import threading

import pytest

from copycord.admin.tasks.repo import SqliteTaskRepo
from copycord.common.db import DBManager


def _run(db, type_="backfill_channels", **extra):
    fields = {
        "type": type_,
        "title": extra.pop("title", "Backfill 3 channels"),
        "inputs_json": {"channels": ["1"]},
        "trigger": "manual",
        "actor": "admin",
    }
    fields.update(extra)
    return db.task_run_insert(fields)


def _indexes(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA index_list({table})").fetchall()}


def test_fresh_db_has_task_runs_and_indexes(db):
    cols = db._table_columns("task_runs")
    assert {
        "id", "type", "number", "status", "title", "context_json", "trigger", "actor",
        "inputs_json", "progress_json", "created_at", "started_at", "finished_at",
        "updated_at", "error",
    } <= cols
    assert {"idx_task_runs_type_id", "idx_task_runs_status", "idx_task_runs_created"} <= _indexes(
        db.conn, "task_runs"
    )
    assert "task_run_id" in db._table_columns("backfill_runs")


def test_v3_shaped_db_gains_task_runs_and_keeps_backfill_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path / "backups"))
    path = tmp_path / "v3.db"
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE backfill_runs (
            run_id TEXT PRIMARY KEY, original_guild_id INTEGER, cloned_guild_id INTEGER,
            original_channel_id INTEGER NOT NULL, clone_channel_id INTEGER,
            status TEXT NOT NULL DEFAULT 'running', range_json TEXT,
            started_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            delivered INTEGER NOT NULL DEFAULT 0, expected_total INTEGER,
            last_orig_message_id TEXT, last_orig_timestamp TEXT, error TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO backfill_runs (run_id, original_guild_id, cloned_guild_id, original_channel_id,"
        " clone_channel_id, status, started_at, updated_at, delivered)"
        " VALUES ('r1', 111, 222, 333, 444, 'completed', 't0', 't1', 7)"
    )
    conn.commit()
    conn.close()

    db = DBManager(str(path), init_schema=True)
    try:
        assert db._table_exists("task_runs")
        row = dict(db.conn.execute("SELECT * FROM backfill_runs WHERE run_id='r1'").fetchone())
        assert row["original_guild_id"] == 111
        assert row["cloned_guild_id"] == 222
        assert row["delivered"] == 7
        assert row["task_run_id"] is None
        rid = _run(db)
        assert db.task_run_get(rid)["number"] == 1
    finally:
        db.close()

    # reopening an up to date DB leaves everything in place
    db = DBManager(str(path), init_schema=True)
    try:
        assert db.task_run_get(rid)["type"] == "backfill_channels"
    finally:
        db.close()


def test_insert_get_update(db):
    rid = _run(db, context_json={"label": "Main > general", "mapping_id": "m1"})
    assert isinstance(rid, int)
    row = db.task_run_get(rid)
    assert row["id"] == rid
    assert row["status"] == "queued"
    assert row["number"] == 1
    assert row["inputs_json"] == '{"channels":["1"]}'
    assert row["created_at"].endswith("Z") and row["updated_at"] == row["created_at"]

    assert db.task_run_update(rid, status="running", started_at="2026-10-14T00:00:00Z") is True
    row = db.task_run_get(rid)
    assert row["status"] == "running"
    assert row["started_at"] == "2026-10-14T00:00:00Z"
    assert db.task_run_update(rid, progress_json={"steps": []}, status="success") is True
    assert db.task_run_get(rid)["progress_json"] == '{"steps":[]}'

    assert db.task_run_update(999999, status="failed") is False
    assert db.task_run_get(999999) is None
    with pytest.raises(ValueError):
        db.task_run_update(rid, number=5)
    with pytest.raises(ValueError):
        db.task_run_insert({"title": "no type"})


def test_numbering_is_per_type(db):
    a1 = _run(db, "a")
    a2 = _run(db, "a")
    b1 = _run(db, "b")
    assert [db.task_run_get(i)["number"] for i in (a1, a2, b1)] == [1, 2, 1]
    assert db.task_run_next_number("a") == 3
    assert db.task_run_next_number("b") == 2
    assert db.task_run_next_number("never") == 1


def test_concurrent_inserts_never_share_a_number(db):
    ids, errors = [], []

    def worker():
        try:
            for _ in range(20):
                ids.append(_run(db, "race"))
        except Exception as e:  # pragma: no cover - surfaced by the assert below
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    numbers = sorted(db.task_run_get(i)["number"] for i in ids)
    assert numbers == list(range(1, 41))


def test_list_filters_paging_and_q(db):
    ids = []
    for i in range(30):
        ids.append(
            _run(
                db,
                "backfill_channels" if i % 2 == 0 else "export",
                title=f"Run {i}",
                actor="admin" if i % 3 else "system",
                context_json={"label": "Guild A" if i < 10 else "Guild B"},
            )
        )
    db.task_run_update(ids[5], status="failed")

    rows, total = db.task_runs_list()
    assert total == 30
    assert len(rows) == 25
    assert [r["id"] for r in rows] == sorted(ids, reverse=True)[:25]
    rows, total = db.task_runs_list(page=2, per_page=25)
    assert total == 30 and len(rows) == 5

    rows, total = db.task_runs_list(type="export")
    assert total == 15 and all(r["type"] == "export" for r in rows)
    rows, total = db.task_runs_list(status="failed")
    assert [r["id"] for r in rows] == [ids[5]]
    rows, total = db.task_runs_list(actor="system")
    assert total == 10
    rows, total = db.task_runs_list(context_label="Guild A")
    assert total == 10
    rows, total = db.task_runs_list(type="export", context_label="Guild A", actor="admin")
    assert {r["title"] for r in rows} == {"Run 1", "Run 5", "Run 7"}

    rows, total = db.task_runs_list(q="Run 2")
    assert {r["title"] for r in rows} == {"Run 2", "Run 20", "Run 21", "Run 22", "Run 23",
                                          "Run 24", "Run 25", "Run 26", "Run 27", "Run 28", "Run 29"}
    rows, total = db.task_runs_list(q="guild b", type="export")
    assert total == 10
    # "#3" is the third export run (number 3), not a substring hit
    rows, total = db.task_runs_list(q="#3", type="export")
    assert [r["number"] for r in rows] == [3]
    rows, total = db.task_runs_list(q="100%")
    assert total == 0


def test_recover_marks_queued_and_running_failed(db):
    q = _run(db)
    r = _run(db)
    s = _run(db)
    db.task_run_update(r, status="running")
    db.task_run_update(s, status="success")
    assert db.task_runs_recover() == [q, r]
    for i in (q, r):
        row = db.task_run_get(i)
        assert row["status"] == "failed"
        assert row["error"] == "Interrupted by restart"
        assert row["finished_at"]
    assert db.task_run_get(s)["status"] == "success"
    assert db.task_runs_recover() == []


def test_prune_keeps_newest_500_per_type(db):
    a = [_run(db, "a") for _ in range(503)]
    b = [_run(db, "b") for _ in range(3)]
    for i in a + b:
        db.task_run_update(i, status="success")
    # an old run still going is never pruned
    db.task_run_update(a[0], status="running")
    deleted = db.task_runs_prune()
    assert deleted == a[1:3]
    assert db.task_runs_list(type="a")[1] == 501
    assert db.task_runs_list(type="b")[1] == 3
    assert db.task_run_get(a[0]) is not None
    assert db.task_run_get(a[-500]) is not None
    assert db.task_runs_prune(keep_per_type=2) == sorted(a[-500:-2] + b[:1])


def test_actors_and_contexts(db):
    _run(db, actor="system", context_json={"label": "Zeta"})
    _run(db, actor="admin", context_json={"label": "Alpha"})
    _run(db, "other", actor="admin", context_json={"label": "Other"})
    _run(db, actor="admin")
    assert db.task_runs_actors() == ["admin", "system"]
    assert db.task_runs_contexts() == ["Alpha", "Other", "Zeta"]
    assert db.task_runs_contexts("backfill_channels") == ["Alpha", "Zeta"]


def test_repo_adapter_delegates(db):
    repo = SqliteTaskRepo(db)
    rid = repo.insert_run({"type": "noop", "title": "Noop"})
    assert repo.next_number("noop") == 2
    assert repo.update_run(rid, status="running")
    assert repo.get_run(rid)["status"] == "running"
    rows, total = repo.list_runs(type="noop")
    assert total == 1 and rows[0]["id"] == rid
    assert repo.recover() == [rid]
    assert repo.actors() == ["admin"]
    assert repo.prune(keep_per_type=0) == [rid]
