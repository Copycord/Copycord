"""GET /api/system/senders: DB token rows merged with the server's live token_pool status (no Discord)."""
from unittest.mock import AsyncMock

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin.security import CSRF_HEADER

UID = "175928847299117063"  # (UID >> 22) % 6 == 2


def _row(tid, value, *, username=None, display_name=None, avatar_url=None, user_id=None,
         enabled=1, last_used=None, use_count=0):
    return {
        "token_id": tid,
        "token_value": value,
        "username": username,
        "display_name": display_name,
        "avatar_url": avatar_url,
        "user_id": user_id,
        "enabled": enabled,
        "last_used": last_used,
        "use_count": use_count,
    }


def _entry(state, *, presence="online", since=1000.5, dead=False):
    return {"state": state, "presence": presence, "since": since, "dead": dead, "session": "k"}


def _pool(**tokens):
    return {"keep_online": 2, "live": 1, "max_loop_lag_ms": 0.0, "tokens": tokens}


def test_shared_token_is_one_item_with_max_last_used_and_summed_count():
    rows = [
        _row("t1", "SECRET-A", username="relay.alpha", display_name="Ava", user_id=UID,
             last_used=100, use_count=3),
        _row("t2", "SECRET-A", username="relay.alpha", last_used=250, use_count=4),
    ]
    items = app_module._merge_senders(rows, _pool(t2=_entry("online", presence="dnd")))
    assert items == [
        {
            "id": "t1",
            "username": "relay.alpha",
            "display_name": "Ava",
            "avatar_url": "https://cdn.discordapp.com/embed/avatars/2.png",
            "state": "online",
            "presence": "dnd",
            "since": 1000,
            "last_used": 250,
            "use_count": 7,
        }
    ]


def test_revoked_beats_disabled_and_disabled_needs_every_row_off():
    rows = [
        _row("d1", "DEAD", username="dead", enabled=0),
        _row("x1", "OFF", username="off1", enabled=0),
        _row("x2", "OFF", username="off1", enabled=0),
        _row("h1", "HALF", username="half", enabled=0),
        _row("h2", "HALF", username="half", enabled=1),
    ]
    pool = _pool(d1=_entry("offline", dead=True), h2=_entry("connecting"))
    by_name = {i["username"]: i for i in app_module._merge_senders(rows, pool)}
    assert by_name["dead"]["state"] == "revoked"
    assert by_name["off1"]["state"] == "disabled"
    assert by_name["half"]["state"] == "connecting"
    # presence and since only while online
    assert all(i["presence"] is None and i["since"] is None for i in by_name.values())


def test_offline_when_pool_absent_or_no_entry():
    rows = [_row("a", "A", username="a"), _row("b", "B", username="b")]
    for pool in (None, _pool(), {"tokens": None}, _pool(b=_entry("offline"))):
        items = app_module._merge_senders(rows, pool)
        assert [i["state"] for i in items] == ["offline", "offline"]


def test_identity_fallbacks():
    rows = [
        _row("s", "S", username="stored", display_name="Stored Name",
             avatar_url="https://cdn.discordapp.com/avatars/1/h.png?size=1024", user_id="1"),
        _row("u", "U", username="plain", user_id=UID),
        _row("n", "N"),
    ]
    by_id = {i["id"]: i for i in app_module._merge_senders(rows, None)}
    assert by_id["s"]["display_name"] == "Stored Name"
    assert by_id["s"]["avatar_url"] == "https://cdn.discordapp.com/avatars/1/h.png?size=1024"
    assert by_id["u"]["display_name"] == "plain"
    assert by_id["u"]["avatar_url"] == "https://cdn.discordapp.com/embed/avatars/2.png"
    assert by_id["n"]["display_name"] is None
    assert by_id["n"]["avatar_url"] is None
    assert by_id["n"]["username"] is None


def test_sort_online_first_then_username():
    rows = [
        _row("1", "A", username="Charlie"),
        _row("2", "B", username="bravo"),
        _row("3", "C", username="alpha"),
        _row("4", "D", username="delta"),
        _row("5", "E"),
    ]
    pool = _pool(**{"4": _entry("online"), "2": _entry("online")})
    items = app_module._merge_senders(rows, pool)
    assert [i["username"] for i in items] == ["bravo", "delta", "alpha", "Charlie", None]


class FakeDB:
    def __init__(self, rows):
        self.rows = rows

    def list_all_user_tokens(self):
        return [dict(r) for r in self.rows]


@pytest.fixture()
def tc(tmp_path, monkeypatch):
    monkeypatch.setattr(app_module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    with TestClient(app_module.app) as client:
        client.headers.update({"origin": "http://testserver", CSRF_HEADER: "1",
                               "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
        yield client


def _status(state, token_pool):
    return {"server": {"state": state, "token_pool": token_pool}, "client": {}}


def test_endpoint_merges_live_pool_and_never_returns_token_values(tc, monkeypatch):
    rows = [
        _row("t1", "SECRET-ONE", username="relay.alpha", user_id=UID, use_count=5, last_used=99),
        _row("t2", "SECRET-ONE", username="relay.alpha", use_count=1),
        _row("t3", "SECRET-TWO", username="relay.bravo", enabled=0),
    ]
    monkeypatch.setattr(app_module, "db", FakeDB(rows))
    monkeypatch.setattr(
        app_module, "_collect_status",
        AsyncMock(return_value=_status("running", _pool(t1=_entry("online", since=1700000000.9)))),
    )
    resp = tc.get("/api/system/senders")
    assert resp.status_code == 200
    assert "SECRET" not in resp.text and "token_value" not in resp.text
    body = resp.json()
    assert body["ok"] is True
    assert [(i["id"], i["state"], i["use_count"]) for i in body["items"]] == [
        ("t1", "online", 6),
        ("t3", "disabled", 0),
    ]
    assert body["items"][0]["since"] == 1700000000
    assert body["items"][0]["presence"] == "online"


def test_endpoint_ignores_a_stale_pool_when_the_server_is_stopped(tc, monkeypatch):
    monkeypatch.setattr(app_module, "db", FakeDB([_row("t1", "V", username="a")]))
    monkeypatch.setattr(
        app_module, "_collect_status",
        AsyncMock(return_value=_status("stopped", _pool(t1=_entry("online")))),
    )
    items = tc.get("/api/system/senders").json()["items"]
    assert [(i["state"], i["presence"], i["since"]) for i in items] == [("offline", None, None)]


def test_endpoint_needs_login(tmp_path, monkeypatch):
    monkeypatch.setattr(app_module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    monkeypatch.setattr(app_module, "db", FakeDB([_row("t1", "V", username="a")]))
    with TestClient(app_module.app) as client:
        resp = client.get("/api/system/senders", headers={"origin": "http://testserver"})
    assert resp.status_code == 401


def test_verify_route_stores_identity(tc, monkeypatch):
    stored = []

    class DB:
        def get_mapping_by_id(self, mid):
            return {"mapping_id": mid, "cloned_guild_id": "5"}

        def list_mapping_tokens(self, mid):
            return [{"token_id": "t1", "token_value": "VAL-1", "username": "old"},
                    {"token_id": "t2", "token_value": "VAL-2", "username": "bad"}]

        def update_token_identity(self, **kw):
            stored.append(kw)
            return 1

    monkeypatch.setattr(app_module, "db", DB())
    monkeypatch.setattr(app_module, "_selfbot_in_guild",
                        AsyncMock(side_effect=lambda tok, gid: tok == "VAL-1"))
    monkeypatch.setattr(app_module, "_selfbot_identity", AsyncMock(return_value={
        "id": UID, "username": "new", "global_name": "New Name",
        "avatar_url": f"https://cdn.discordapp.com/avatars/{UID}/a_h.gif?size=1024",
    }))
    body = tc.post("/api/guild-mappings/m1/user-tokens/verify").json()
    assert [r["username"] for r in body["results"]] == ["new", "bad"]
    assert stored == [{
        "token_value": "VAL-1", "username": "new", "display_name": "New Name",
        "avatar_url": f"https://cdn.discordapp.com/avatars/{UID}/a_h.gif?size=1024", "user_id": UID,
    }]


def test_avatar_url_helpers():
    assert app_module._user_avatar_url(UID, "abc") == (
        f"https://cdn.discordapp.com/avatars/{UID}/abc.png?size=1024"
    )
    assert app_module._user_avatar_url(UID, "a_abc").endswith("/a_abc.gif?size=1024")
    assert app_module._user_avatar_url(UID, None) == "https://cdn.discordapp.com/embed/avatars/2.png"
    assert app_module._default_avatar_url(None) is None
    assert app_module._default_avatar_url("nope") is None
