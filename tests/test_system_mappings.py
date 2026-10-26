"""GET /api/system/senders `mappings`: per mapping token counts from the same states the senders table shows."""
from unittest.mock import AsyncMock

import pytest

from copycord.admin import app as app_module
from tests.test_system_senders import _entry, _pool, _row, _status, tc  # noqa: F401


def _trow(tid, value, mapping_id, **kw):
    return dict(_row(tid, value, **kw), mapping_id=mapping_id)


def _m(mid, *, name=None, original="", cloned="", status="active", tokens=True):
    return {
        "mapping_id": mid,
        "mapping_name": name,
        "original_guild_name": original,
        "cloned_guild_name": cloned,
        "status": status,
        "settings": {"USE_USER_TOKENS": tokens},
    }


def _counts(item):
    return {k: item[k] for k in ("total", "online", "connecting", "offline", "disabled", "revoked")}


def _mappings(rows, pool, mappings):
    return app_module._mapping_senders(app_module._group_senders(rows, pool), mappings)


def test_counts_per_mapping_for_a_mix_of_states():
    rows = [
        _trow("a1", "A1", "ma"),
        _trow("a2", "A2", "ma"),
        _trow("a3", "A3", "ma"),
        _trow("a4", "A4", "ma"),
        _trow("a5", "A5", "ma", enabled=0),
        _trow("a6", "A6", "ma"),
    ]
    pool = _pool(a1=_entry("online"), a2=_entry("online"), a3=_entry("connecting"),
                 a6=_entry("offline", dead=True))
    (item,) = _mappings(rows, pool, [_m("ma", name="Alpha")])
    assert item["id"] == "ma" and item["name"] == "Alpha" and item["status"] == "active"
    assert item["tokens_enabled"] is True
    assert _counts(item) == {"total": 6, "online": 2, "connecting": 1, "offline": 1,
                             "disabled": 1, "revoked": 1}


def test_a_shared_token_counts_under_both_mappings():
    rows = [_trow("x1", "SHARED", "m1"), _trow("x2", "SHARED", "m2"), _trow("y", "OWN", "m2")]
    pool = _pool(x2=_entry("online"))
    by_id = {i["id"]: i for i in _mappings(rows, pool, [_m("m1", name="One"), _m("m2", name="Two")])}
    assert _counts(by_id["m1"]) == {"total": 1, "online": 1, "connecting": 0, "offline": 0,
                                    "disabled": 0, "revoked": 0}
    assert by_id["m2"]["total"] == 2 and by_id["m2"]["online"] == 1 and by_id["m2"]["offline"] == 1


def test_a_paused_mapping_shows_its_tokens_as_offline_but_keeps_disabled_and_revoked():
    rows = [
        _trow("p1", "P1", "mp"),
        _trow("p2", "P2", "mp"),
        _trow("p3", "P3", "mp", enabled=0),
        _trow("p4", "P4", "mp"),
        # Shared with an active mapping that keeps it online.
        _trow("s1", "S", "mp"),
        _trow("s2", "S", "ma"),
    ]
    pool = _pool(p1=_entry("online"), p2=_entry("connecting"), p4=_entry("offline", dead=True),
                 s2=_entry("online"))
    by_id = {i["id"]: i for i in _mappings(rows, pool, [_m("mp", name="P", status="paused"), _m("ma", name="A")])}
    assert by_id["mp"]["status"] == "paused"
    assert _counts(by_id["mp"]) == {"total": 5, "online": 0, "connecting": 0, "offline": 3,
                                    "disabled": 1, "revoked": 1}
    assert by_id["ma"]["online"] == 1


def test_a_row_disabled_in_one_mapping_only():
    rows = [_trow("d1", "D", "m1", enabled=0), _trow("d2", "D", "m2")]
    by_id = {i["id"]: i for i in _mappings(rows, _pool(d2=_entry("online")), [_m("m1", name="a"), _m("m2", name="b")])}
    assert by_id["m1"]["disabled"] == 1 and by_id["m1"]["online"] == 0
    assert by_id["m2"]["online"] == 1


def test_name_fallbacks_and_empty_mappings():
    items = _mappings([], None, [
        _m("named", name="  Named  ", original="Orig"),
        _m("orig", name="", original="Origin Guild", cloned="Clone"),
        _m("clone", cloned="Clone Guild"),
        _m("abcdef1234567890"),
    ])
    names = {i["id"]: i["name"] for i in items}
    assert names == {"named": "Named", "orig": "Origin Guild", "clone": "Clone Guild",
                     "abcdef1234567890": "Mapping abcdef12"}
    assert all(_counts(i) == dict.fromkeys(_counts(i), 0) for i in items)


def test_order_is_active_first_then_by_name_and_stable():
    mappings = [
        _m("1", name="zulu"),
        _m("2", name="Bravo", status="paused"),
        _m("3", name="alpha", status="paused"),
        _m("4", name="Charlie"),
        _m("5", name="charlie"),
    ]
    first = [i["id"] for i in _mappings([], None, mappings)]
    assert first == ["4", "5", "1", "3", "2"]
    assert [i["id"] for i in _mappings([], None, list(reversed(mappings)))] == first


def test_tokens_enabled_resolves_over_the_defaults(monkeypatch):
    inherit = dict(_m("i", name="i"), settings={})
    off = _m("o", name="o", tokens=False)
    assert {i["id"]: i["tokens_enabled"] for i in _mappings([], None, [inherit, off])} == {"i": False, "o": False}
    monkeypatch.setitem(app_module.DEFAULTS, "USE_USER_TOKENS", True)
    assert {i["id"]: i["tokens_enabled"] for i in _mappings([], None, [inherit, off])} == {"i": True, "o": False}


def test_rows_of_an_unknown_mapping_are_ignored():
    items = _mappings([_trow("g", "G", "gone")], None, [_m("m", name="m")])
    assert [(i["id"], i["total"]) for i in items] == [("m", 0)]


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


def test_endpoint_adds_mappings_and_keeps_items_and_enabled(tc, monkeypatch):  # noqa: F811
    rows = [_trow("t1", "SECRET-ONE", "m1", username="a"), _trow("t2", "SECRET-ONE", "m2", username="a"),
            _trow("t3", "SECRET-TWO", "m1", username="b", enabled=0)]
    fake = CountingDB(rows, [_m("m1", name="One"), _m("m2", name="Two", status="paused")])
    monkeypatch.setattr(app_module, "db", fake)
    monkeypatch.setattr(app_module, "_collect_status",
                        AsyncMock(return_value=_status("running", _pool(t1=_entry("online")))))
    resp = tc.get("/api/system/senders")
    assert "SECRET" not in resp.text and "token_value" not in resp.text
    body = resp.json()
    assert set(body) == {"ok", "enabled", "items", "mappings"}
    assert body["enabled"] is True
    assert [(i["id"], i["state"]) for i in body["items"]] == [("t1", "online"), ("t3", "disabled")]
    assert set(body["items"][0]) == {"id", "username", "display_name", "avatar_url", "state",
                                     "presence", "since", "last_used", "use_count"}
    assert body["mappings"] == [
        {"id": "m1", "name": "One", "status": "active", "tokens_enabled": True, "total": 2,
         "online": 1, "connecting": 0, "offline": 0, "disabled": 1, "revoked": 0},
        {"id": "m2", "name": "Two", "status": "paused", "tokens_enabled": True, "total": 1,
         "online": 0, "connecting": 0, "offline": 1, "disabled": 0, "revoked": 0},
    ]
    assert sorted(fake.calls) == ["mappings", "tokens"]


@pytest.mark.parametrize("n_mappings", [1, 40])
def test_endpoint_with_1000_tokens_reads_the_db_once(tc, monkeypatch, n_mappings):  # noqa: F811
    rows = [_trow(f"t{i}", f"V{i}", f"m{i % n_mappings}") for i in range(1000)]
    pool = _pool(**{f"t{i}": _entry("online") for i in range(0, 1000, 2)})
    fake = CountingDB(rows, [_m(f"m{j}", name=f"Map {j:02d}") for j in range(n_mappings)])
    monkeypatch.setattr(app_module, "db", fake)
    monkeypatch.setattr(app_module, "_collect_status", AsyncMock(return_value=_status("running", pool)))
    body = tc.get("/api/system/senders").json()
    assert sorted(fake.calls) == ["mappings", "tokens"]
    assert len(body["items"]) == 1000
    assert sum(m["total"] for m in body["mappings"]) == 1000
    assert sum(m["online"] for m in body["mappings"]) == 500
    assert len(body["mappings"]) == n_mappings
