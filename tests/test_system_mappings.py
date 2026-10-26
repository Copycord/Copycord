"""GET /api/system/senders: each item's `mapping_ids` and the `mappings` the senders table filters by."""
from unittest.mock import AsyncMock

import pytest

from copycord.admin import app as app_module
from tests.test_system_senders import _entry, _pool, _row, _status, tc  # noqa: F401


def _m(mid, *, name=None, original="", cloned="", status="active", tokens=True):
    return {
        "mapping_id": mid,
        "mapping_name": name,
        "original_guild_name": original,
        "cloned_guild_name": cloned,
        "status": status,
        "settings": {"USE_USER_TOKENS": tokens},
    }


def _ids(rows, pool=None):
    return {i["id"]: i["mapping_ids"] for i in app_module._merge_senders(rows, pool)}


def test_a_token_shared_by_two_mappings_lists_both_sorted():
    rows = [
        _row("x1", "SHARED", mapping_id="m2"),
        _row("x2", "SHARED", mapping_id="m1"),
        _row("x3", "SHARED", mapping_id="m2"),
        _row("y", "OWN", mapping_id="m2"),
    ]
    assert _ids(rows, _pool(x2=_entry("online"))) == {"x1": ["m1", "m2"], "y": ["m2"]}


def test_rows_without_a_mapping_give_an_empty_list():
    assert _ids([_row("n", "N"), _row("e", "E", mapping_id="")]) == {"e": [], "n": []}


def test_paused_mapping_tokens_keep_their_mapping_and_state():
    rows = [
        _row("p1", "P1", mapping_id="mp"),
        _row("p2", "P2", mapping_id="mp", enabled=0),
        _row("p3", "P3", mapping_id="mp"),
    ]
    pool = _pool(p3=_entry("offline", dead=True))
    items = {i["id"]: (i["state"], i["mapping_ids"]) for i in app_module._merge_senders(rows, pool)}
    assert items == {"p1": ("offline", ["mp"]), "p2": ("disabled", ["mp"]), "p3": ("revoked", ["mp"])}


def test_name_fallbacks():
    items = app_module._senders_mappings([
        _m("named", name="  Named  ", original="Orig"),
        _m("orig", name="", original="Origin Guild", cloned="Clone"),
        _m("clone", cloned="Clone Guild"),
        _m("abcdef1234567890"),
    ])
    assert {i["id"]: i["name"] for i in items} == {
        "named": "Named", "orig": "Origin Guild", "clone": "Clone Guild",
        "abcdef1234567890": "Mapping abcdef12",
    }
    assert all(set(i) == {"id", "name", "status"} for i in items)


def test_order_is_active_first_then_by_name_and_stable():
    mappings = [
        _m("1", name="zulu"),
        _m("2", name="Bravo", status="paused"),
        _m("3", name="alpha", status="paused"),
        _m("4", name="Charlie"),
        _m("5", name="charlie"),
        _m("4", name="duplicate"),
        _m(""),
    ]
    first = [(i["id"], i["status"]) for i in app_module._senders_mappings(mappings)]
    assert first == [("4", "active"), ("5", "active"), ("1", "active"), ("3", "paused"), ("2", "paused")]
    assert [(i["id"], i["status"]) for i in app_module._senders_mappings(list(reversed(mappings[:5])))] == first


class CountingDB:
    def __init__(self, rows, mappings):
        self.rows, self.mappings = rows, mappings
        self.calls = []

    def list_all_user_tokens(self):
        self.calls.append("tokens")
        return [dict(r) for r in self.rows]

    def list_guild_mappings(self):
        self.calls.append("mappings")
        return [dict(m) for m in self.mappings]

    def __getattr__(self, name):
        raise AssertionError(f"unexpected DB call {name}")


def test_endpoint_adds_mapping_ids_and_mappings_and_keeps_items_and_enabled(tc, monkeypatch):  # noqa: F811
    rows = [
        _row("t1", "SECRET-ONE", username="a", mapping_id="m1"),
        _row("t2", "SECRET-ONE", username="a", mapping_id="m2"),
        _row("t3", "SECRET-TWO", username="b", enabled=0, mapping_id="m1"),
    ]
    fake = CountingDB(rows, [_m("m2", name="Two", status="paused"), _m("m1", name="One")])
    monkeypatch.setattr(app_module, "db", fake)
    monkeypatch.setattr(app_module, "_collect_status",
                        AsyncMock(return_value=_status("running", _pool(t1=_entry("online")))))
    resp = tc.get("/api/system/senders")
    assert "SECRET" not in resp.text and "token_value" not in resp.text
    body = resp.json()
    assert set(body) == {"ok", "enabled", "items", "mappings"}
    assert body["ok"] is True and body["enabled"] is True
    assert [(i["id"], i["state"], i["mapping_ids"]) for i in body["items"]] == [
        ("t1", "online", ["m1", "m2"]),
        ("t3", "disabled", ["m1"]),
    ]
    assert set(body["items"][0]) == {"id", "username", "display_name", "avatar_url", "state",
                                     "presence", "since", "last_used", "use_count", "mapping_ids"}
    assert body["mappings"] == [
        {"id": "m1", "name": "One", "status": "active"},
        {"id": "m2", "name": "Two", "status": "paused"},
    ]
    assert sorted(fake.calls) == ["mappings", "tokens"]


@pytest.mark.parametrize("n_mappings", [1, 40])
def test_endpoint_with_1000_tokens_reads_the_db_once(tc, monkeypatch, n_mappings):  # noqa: F811
    rows = [_row(f"t{i}", f"V{i}", mapping_id=f"m{i % n_mappings}") for i in range(1000)]
    pool = _pool(**{f"t{i}": _entry("online") for i in range(0, 1000, 2)})
    fake = CountingDB(rows, [_m(f"m{j}", name=f"Map {j:02d}") for j in range(n_mappings)])
    monkeypatch.setattr(app_module, "db", fake)
    monkeypatch.setattr(app_module, "_collect_status", AsyncMock(return_value=_status("running", pool)))
    body = tc.get("/api/system/senders").json()
    assert sorted(fake.calls) == ["mappings", "tokens"]
    assert len(body["items"]) == 1000
    assert sum(i["state"] == "online" for i in body["items"]) == 500
    assert all(i["mapping_ids"] == [f"m{int(i['id'][1:]) % n_mappings}"] for i in body["items"])
    assert len(body["mappings"]) == n_mappings
