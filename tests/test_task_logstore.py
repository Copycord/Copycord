"""Per-run task log files (admin/tasks/logstore.py)."""

from datetime import datetime, timezone

from copycord.admin.tasks import logstore
from copycord.admin.tasks.logstore import TRUNCATED_MSG, LogStore


def test_default_dir_is_under_the_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    assert LogStore().dir == tmp_path.resolve() / "task-logs"
    assert logstore.default_log_dir() == tmp_path.resolve() / "task-logs"


def test_append_and_read(tmp_path):
    store = LogStore(tmp_path / "task-logs")
    ts = datetime(2026, 10, 14, 12, 0, 0, tzinfo=timezone.utc)
    assert store.append(7, 1, ts, "info", None, "Started")
    assert store.append(7, 2, "2026-10-14T12:00:01Z", "warn", "c1", "tab\there\nnewline")
    text = (tmp_path / "task-logs" / "7.log").read_text()
    assert text == (
        "1\t2026-10-14T12:00:00Z\tinfo\t\tStarted\n"
        "2\t2026-10-14T12:00:01Z\twarn\tc1\ttab here newline\n"
    )
    lines, nxt, complete = store.read(7)
    assert lines == [
        {"seq": 1, "ts": "2026-10-14T12:00:00Z", "level": "info", "step": None, "msg": "Started"},
        {"seq": 2, "ts": "2026-10-14T12:00:01Z", "level": "warn", "step": "c1", "msg": "tab here newline"},
    ]
    assert nxt == 2 and complete is True
    assert store.read(404) == ([], 0, True)


def test_after_step_and_limit(tmp_path):
    store = LogStore(tmp_path)
    for seq in range(1, 11):
        store.append(1, seq, None, "info", "a" if seq % 2 else "b", f"line {seq}")

    lines, nxt, complete = store.read(1, after=6)
    assert [l["seq"] for l in lines] == [7, 8, 9, 10] and nxt == 10 and complete

    lines, nxt, complete = store.read(1, step="b")
    assert [l["seq"] for l in lines] == [2, 4, 6, 8, 10]

    lines, nxt, complete = store.read(1, limit=3)
    assert [l["seq"] for l in lines] == [1, 2, 3] and nxt == 3 and complete is False
    lines, nxt, complete = store.read(1, after=nxt, step="a", limit=2)
    assert [l["seq"] for l in lines] == [5, 7] and nxt == 7 and complete is False
    lines, nxt, complete = store.read(1, after=nxt, step="a", limit=2)
    assert [l["seq"] for l in lines] == [9] and nxt == 9 and complete is True
    assert store.read(1, after=10) == ([], 10, True)


def test_cap_writes_one_truncation_line(tmp_path):
    store = LogStore(tmp_path, cap_bytes=200)
    written = 0
    seq = 0
    while True:
        seq += 1
        if not store.append(3, seq, "t", "info", "s", "x" * 30):
            break
        written += 1
    for extra in range(5):
        assert store.append(3, seq + 1 + extra, "t", "info", "s", "more") is False
    path = tmp_path / "3.log"
    lines = path.read_text().splitlines()
    assert len(lines) == written + 1
    assert lines[-1] == f"{seq}\tt\twarn\t\t{TRUNCATED_MSG}"
    assert sum(len(l) + 1 for l in lines[:-1]) <= 200

    # the marker shows even when reading one step, and a fresh store (restart) keeps the cap
    got, _, _ = store.read(3, step="s")
    assert got[-1]["msg"] == TRUNCATED_MSG
    again = LogStore(tmp_path, cap_bytes=200)
    assert again.append(3, 999, "t", "info", "s", "x" * 30) is False
    assert path.read_text().splitlines() == lines


def test_default_cap_is_5_mb():
    assert LogStore("unused").cap == 5 * 1024 * 1024


def test_delete(tmp_path):
    store = LogStore(tmp_path, cap_bytes=60)
    store.append(9, 1, "t", "info", None, "x" * 40)
    store.append(9, 2, "t", "info", None, "x" * 40)
    assert (tmp_path / "9.log").exists()
    store.delete(9)
    assert not (tmp_path / "9.log").exists()
    store.delete(9)  # missing file is fine
    # the cap state goes with the file
    assert store.append(9, 3, "t", "info", None, "fresh")
