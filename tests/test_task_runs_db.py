"""task_runs table and the DBManager task-run methods (Tasks framework, lane L2)."""

import json
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
        "updated_at", "error", "report_json",
    } <= cols
    assert {"idx_task_runs_type_created", "idx_task_runs_status", "idx_task_runs_created"} <= _indexes(
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
    assert isinstance(rid, str) and len(rid) == 32 and int(rid, 16) >= 0
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

    assert db.task_run_update("0" * 32, status="failed") is False
    assert db.task_run_get("0" * 32) is None
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
    assert [r["id"] for r in rows] == ids[::-1][:25]
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


def test_report_json_round_trips(db):
    rid = _run(db)
    assert db.task_run_get(rid)["report_json"] is None
    db.task_run_update(rid, report_json={"channels": [{"id": "1", "delivered": 5}]})
    assert json.loads(db.task_run_get(rid)["report_json"]) == {"channels": [{"id": "1", "delivered": 5}]}
    db.task_run_update(rid, report_json=None)
    assert db.task_run_get(rid)["report_json"] is None


def test_task_runs_without_report_json_gains_it_and_keeps_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path / "backups"))
    path = tmp_path / "l2.db"
    db = DBManager(str(path), init_schema=True)
    rid = _run(db, status="success", progress_json={"seq": 3})
    db.close()
    # the task_runs shape before report_json existed
    conn = sqlite3.connect(path)
    conn.execute("ALTER TABLE task_runs DROP COLUMN report_json")
    conn.commit()
    conn.close()

    db = DBManager(str(path), init_schema=True)
    try:
        assert "report_json" in db._table_columns("task_runs")
        row = db.task_run_get(rid)
        assert row["status"] == "success" and row["number"] == 1
        assert json.loads(row["progress_json"]) == {"seq": 3}
        assert row["report_json"] is None
        assert {"idx_task_runs_type_created", "idx_task_runs_status"} <= _indexes(db.conn, "task_runs")
    finally:
        db.close()


def test_integer_run_ids_migrate_to_uuid_hex_and_backfill_links_follow(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path / "backups"))
    path = tmp_path / "int-ids.db"
    conn = sqlite3.connect(path)
    # the task_runs and backfill_runs shapes from when run ids were integers
    conn.execute(
        """
        CREATE TABLE task_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, number INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued', title TEXT NOT NULL DEFAULT '', context_json TEXT,
            trigger TEXT NOT NULL DEFAULT 'manual', actor TEXT NOT NULL DEFAULT 'admin',
            inputs_json TEXT NOT NULL DEFAULT '{}', progress_json TEXT, created_at TEXT NOT NULL,
            started_at TEXT, finished_at TEXT, updated_at TEXT NOT NULL, error TEXT, report_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX idx_task_runs_type_id ON task_runs(type, id DESC)")
    conn.execute(
        """
        CREATE TABLE backfill_runs (
            run_id TEXT PRIMARY KEY, original_guild_id INTEGER, cloned_guild_id INTEGER,
            original_channel_id INTEGER NOT NULL, clone_channel_id INTEGER,
            status TEXT NOT NULL DEFAULT 'running', range_json TEXT,
            started_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            delivered INTEGER NOT NULL DEFAULT 0, expected_total INTEGER,
            last_orig_message_id TEXT, last_orig_timestamp TEXT, error TEXT, task_run_id INTEGER
        )
        """
    )
    steps = {"steps": [{"key": "333", "state": "success"}], "seq": 4}
    for rid, number, inputs, report, created in (
        (1, 1, {"channel_ids": ["333"]}, {"delivered": 5}, "2026-10-01T00:00:00Z"),
        (2, 2, {"channel_ids": ["444"]}, None, "2026-10-02T00:00:00Z"),
    ):
        conn.execute(
            "INSERT INTO task_runs (id, type, number, status, title, inputs_json, progress_json,"
            " created_at, updated_at, report_json) VALUES (?, 'backfill_channels', ?, 'success',"
            " ?, ?, ?, ?, ?, ?)",
            (rid, number, f"Run {rid}", json.dumps(inputs), json.dumps(steps), created, created,
             json.dumps(report) if report else None),
        )
    conn.execute(
        "INSERT INTO backfill_runs (run_id, original_guild_id, cloned_guild_id, original_channel_id,"
        " status, started_at, updated_at, delivered, task_run_id)"
        " VALUES ('2:444', 111, 222, 444, 'completed', 't0', 't1', 9, 2)"
    )
    conn.commit()
    conn.close()

    db = DBManager(str(path), init_schema=True)
    try:
        assert db._column_type("task_runs", "id") == "TEXT"
        assert db._column_type("backfill_runs", "task_run_id") == "TEXT"
        rows, total = db.task_runs_list()
        assert total == 2
        newer, older = rows
        for row in rows:
            assert isinstance(row["id"], str) and len(row["id"]) == 32 and int(row["id"], 16) >= 0
        assert newer["id"] != older["id"]
        assert (older["number"], older["title"]) == (1, "Run 1")
        assert (newer["number"], newer["title"]) == (2, "Run 2")
        assert json.loads(older["inputs_json"]) == {"channel_ids": ["333"]}
        assert json.loads(newer["inputs_json"]) == {"channel_ids": ["444"]}
        assert json.loads(older["progress_json"]) == steps
        assert json.loads(older["report_json"]) == {"delivered": 5}
        assert newer["report_json"] is None
        assert older["created_at"] == "2026-10-01T00:00:00Z"
        assert db.task_run_get(older["id"])["number"] == 1

        bf = dict(db.conn.execute("SELECT * FROM backfill_runs WHERE run_id='2:444'").fetchone())
        assert bf["task_run_id"] == newer["id"]
        assert (bf["original_guild_id"], bf["cloned_guild_id"], bf["delivered"]) == (111, 222, 9)

        assert "idx_task_runs_type_id" not in _indexes(db.conn, "task_runs")
        assert "idx_task_runs_type_created" in _indexes(db.conn, "task_runs")
        assert db.task_run_next_number("backfill_channels") == 3
        fresh = _run(db)
        assert db.task_run_get(fresh)["number"] == 3
    finally:
        db.close()

    # a second open leaves the migrated ids alone
    db = DBManager(str(path), init_schema=True)
    try:
        assert {r["id"] for r in db.task_runs_list()[0]} == {older["id"], newer["id"], fresh}
        bf = db.conn.execute("SELECT task_run_id FROM backfill_runs WHERE run_id='2:444'").fetchone()
        assert bf[0] == newer["id"]
    finally:
        db.close()


def test_actors_and_contexts(db):
    _run(db, actor="system", context_json={"label": "Zeta"})
    _run(db, actor="admin", context_json={"label": "Alpha"})
    _run(db, "other", actor="admin", context_json={"label": "Other"})
    _run(db, actor="admin")
    assert db.task_runs_actors() == ["admin", "system"]
    assert db.task_runs_contexts() == ["Alpha", "Other", "Zeta"]
    assert db.task_runs_contexts("backfill_channels") == ["Alpha", "Zeta"]


def test_delete_types_and_status_list(db):
    a = _run(db, "noop")
    b = _run(db, "other", status="running")
    assert db.task_runs_types() == ["noop", "other"]
    rows, total = db.task_runs_list(status=["queued", "running"])
    assert total == 2
    rows, total = db.task_runs_list(status=["running"])
    assert [r["id"] for r in rows] == [b]
    assert db.task_run_delete(a) is True
    assert db.task_run_delete(a) is False
    assert db.task_runs_types() == ["other"]


def test_list_has_no_per_page_cap(db):
    for _ in range(205):
        _run(db, "noop")
    rows, total = db.task_runs_list(type="noop", per_page=500)
    assert total == 205 and len(rows) == 205


def test_repo_adapter_delegates(db):
    repo = SqliteTaskRepo(db)
    assert repo.actors() == []
    _run(db, "noop")
    assert repo.next_number("noop") == 2
    assert repo.list_types() == ["noop"]
    assert repo.actors() == ["admin"]
