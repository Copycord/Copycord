"""Messages page backend (lane L60a): message metadata columns, /api/messages list, facets and live preview."""
import importlib
import logging

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin import messages as messages_api
from copycord.admin.app import app
from copycord.admin.security import CSRF_HEADER
from copycord.common.db import DBManager, snowflake_ts

ORIGIN = "http://testserver"
OG, CG = 111, 222  # original / cloned guild
OG2, CG2 = 333, 444
# snowflakes 1 s apart (1000 ms << 22) so message_ts orders them
BASE = 1_200_000_000_000_000_000


def _mid(i):
    return BASE + i * (1000 << 22)


def _seed(db):
    db.upsert_guild_mapping(
        mapping_id=None, mapping_name="Alpha", original_guild_id=OG, original_guild_name="Orig",
        original_guild_icon_url=None, cloned_guild_id=CG, cloned_guild_name="Clone",
    )
    db.upsert_guild_mapping(
        mapping_id=None, mapping_name="Beta", original_guild_id=OG2, original_guild_name="Orig2",
        original_guild_icon_url=None, cloned_guild_id=CG2, cloned_guild_name="Clone2",
    )
    db.upsert_channel_mapping(10, "general", 20, None, None, None, 0,
                              clone_name="general-clone", original_guild_id=OG, cloned_guild_id=CG)
    db.upsert_channel_mapping(11, "random", 21, None, None, None, 0,
                              original_guild_id=OG, cloned_guild_id=CG)
    authors = [(1, "Zed", "zed"), (2, "amy", "amy_x"), (3, "Bob", "bobby")]
    for i in range(12):
        aid, name, user = authors[i % 3]
        g, c, cc = (OG2, 12, 22) if i == 11 else (OG, 10 + i % 2, 20 + i % 2)
        db.upsert_message_mapping(
            original_guild_id=g, original_channel_id=c, original_message_id=_mid(i),
            cloned_channel_id=cc, cloned_message_id=9_000 + i,
            cloned_guild_id=CG2 if i == 11 else CG,
            author_id=aid, author_name=name, author_username=user,
            attachment_count=i % 4 == 0 and 2 or 0, embed_count=1,
        )


@pytest.fixture()
def sent():
    return []


@pytest.fixture()
def client(db, monkeypatch, sent):
    _seed(db)
    replies = {"res": {"ok": True, "source": "clone", "message": {"id": "9000", "content": "hello"}}}

    async def send(payload, timeout):
        sent.append(payload)
        r = replies["res"]
        if isinstance(r, Exception):
            raise r
        return r

    messages_api.configure(db=db, send=send)
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    with TestClient(app) as tc:
        tc.headers.update({"origin": ORIGIN, CSRF_HEADER: "1",
                           "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
        tc.replies = replies
        yield tc
    messages_api.configure(db=app_module.db, send=lambda p, t: None)


def _ids(res):
    return [it["original_message_id"] for it in res.json()["items"]]


# --- schema ---

def test_migration_adds_columns_on_old_schema(tmp_db_path):
    db1 = DBManager(tmp_db_path, init_schema=True)
    db1.conn.execute("DROP TABLE messages")
    db1.conn.execute(
        """CREATE TABLE messages (
            original_message_id INTEGER NOT NULL, original_guild_id INTEGER, original_channel_id INTEGER,
            cloned_guild_id INTEGER, cloned_channel_id INTEGER, cloned_message_id INTEGER,
            webhook_url TEXT, sent_token_id TEXT,
            created_at INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (original_message_id, cloned_guild_id))"""
    )
    db1.conn.execute(
        "INSERT INTO messages (original_message_id, original_guild_id, original_channel_id, cloned_guild_id,"
        " cloned_channel_id, cloned_message_id, webhook_url, created_at, updated_at)"
        " VALUES (?, 1, 2, 3, 4, 5, 'https://wh', 7, 7)", (_mid(0),),
    )
    db1.conn.commit()
    db1.conn.close()

    db2 = DBManager(tmp_db_path, init_schema=True)
    cols = db2._table_columns("messages")
    for c in ("author_id", "author_name", "author_username", "attachment_count", "embed_count", "message_ts"):
        assert c in cols
    r = dict(db2.conn.execute("SELECT * FROM messages").fetchone())
    assert r["webhook_url"] == "https://wh" and r["cloned_message_id"] == 5 and r["created_at"] == 7
    assert r["author_id"] is None and r["author_name"] is None and r["attachment_count"] is None
    assert r["message_ts"] == snowflake_ts(_mid(0))
    idx = {x[1] for x in db2.conn.execute("PRAGMA index_list(messages)").fetchall()}
    assert {"idx_messages_author", "idx_messages_message_ts", "idx_messages_author_name"} <= idx


def test_upsert_fills_and_keeps_new_columns(db):
    db.upsert_message_mapping(1, 2, _mid(1), 4, 5, cloned_guild_id=3, author_id=77, author_name="Ann",
                              author_username="ann", attachment_count=2, embed_count=1)
    # a later upsert without the fields (e.g. an edit path) keeps them
    db.upsert_message_mapping(1, 2, _mid(1), 4, 6, cloned_guild_id=3)
    r = dict(db.conn.execute("SELECT * FROM messages").fetchone())
    assert (r["author_id"], r["author_name"], r["author_username"]) == (77, "Ann", "ann")
    assert (r["attachment_count"], r["embed_count"], r["cloned_message_id"]) == (2, 1, 6)
    assert r["message_ts"] == snowflake_ts(_mid(1))


@pytest.fixture
def ServerReceiver():
    # importing the server module retunes library log levels; put them back (as test_server_backfill_jobs does)
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    levels = {n: lg.level for n, lg in logging.root.manager.loggerDict.items() if isinstance(lg, logging.Logger)}
    try:
        yield importlib.import_module("copycord.server.server").ServerReceiver
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        for n, lg in list(logging.root.manager.loggerDict.items()):
            if isinstance(lg, logging.Logger):
                lg.setLevel(levels.get(n, logging.NOTSET))


def test_server_message_meta(ServerReceiver):
    meta = ServerReceiver._message_meta({
        "author": "ann", "author_id": "77", "author_display_name": "Ann",
        "attachments": [{}, {}], "embeds": [{}], "content": "secret",
    })
    assert meta == {"author_id": 77, "author_name": "Ann", "author_username": "ann",
                    "attachment_count": 2, "embed_count": 1}


# --- list ---

def test_list_default_sort_and_row_shape(client):
    res = client.get("/api/messages")
    assert res.status_code == 200
    body = res.json()
    assert body["next_cursor"] is None
    assert _ids(res) == [str(_mid(i)) for i in range(11, -1, -1)]
    it = next(x for x in body["items"] if x["original_message_id"] == str(_mid(0)))
    assert it["mapping_name"] == "Alpha" and it["mapping_id"]
    assert it["original_channel_name"] == "general" and it["cloned_channel_name"] == "general-clone"
    assert it["author_id"] == "1" and it["author_name"] == "Zed" and it["author_username"] == "zed"
    assert it["attachment_count"] == 2 and it["embed_count"] == 1
    assert it["created_at"] == snowflake_ts(_mid(0))
    assert it["original_url"] == f"https://discord.com/channels/{OG}/10/{_mid(0)}"
    assert it["cloned_url"] == f"https://discord.com/channels/{CG}/20/9000"
    assert "content" not in it


def test_search_by_name_username_and_ids(client):
    assert len(_ids(client.get("/api/messages", params={"q": "AMY"}))) == 4
    assert len(_ids(client.get("/api/messages", params={"q": "bobb"}))) == 4
    assert _ids(client.get("/api/messages", params={"q": str(_mid(5))})) == [str(_mid(5))]
    assert _ids(client.get("/api/messages", params={"q": "9007"})) == [str(_mid(7))]
    assert len(_ids(client.get("/api/messages", params={"q": "3"}))) == 4  # author id 3
    assert len(_ids(client.get("/api/messages", params={"q": "11"}))) == 5  # channel 11
    assert _ids(client.get("/api/messages", params={"q": "%"})) == []


def test_filters(client):
    g = lambda **p: _ids(client.get("/api/messages", params=p))  # noqa: E731
    mapping = client.get("/api/messages/facets").json()["mappings"]
    beta = next(m for m in mapping if m["mapping_name"] == "Beta")["mapping_id"]
    assert g(mapping_id=beta) == [str(_mid(11))]
    assert g(mapping_id="nope") == []
    assert g(original_guild_id=OG2) == [str(_mid(11))]
    assert len(g(cloned_guild_id=CG)) == 11
    assert len(g(channel_id=10)) == 6
    assert len(g(author_id=2)) == 4
    t3, t5 = snowflake_ts(_mid(3)), snowflake_ts(_mid(5))
    assert g(created_from=t3, created_to=t5, order="asc") == [str(_mid(i)) for i in (3, 4, 5)]
    assert g(has_attachments="true") == [str(_mid(i)) for i in (8, 4, 0)]
    assert len(g(has_attachments="false")) == 9


def test_sorts(client):
    items = client.get("/api/messages", params={"sort": "author", "order": "asc"}).json()["items"]
    names = [it["author_name"].lower() for it in items]
    assert names == sorted(names)
    items = client.get("/api/messages", params={"sort": "channel", "order": "desc"}).json()["items"]
    chans = [(it["original_channel_name"] or "").lower() for it in items]
    assert chans == sorted(chans, reverse=True)
    assert _ids(client.get("/api/messages", params={"order": "asc"})) == [str(_mid(i)) for i in range(12)]
    assert client.get("/api/messages", params={"sort": "content"}).status_code == 400
    assert client.get("/api/messages", params={"order": "up"}).status_code == 400


@pytest.mark.parametrize("sort,order", [("created_at", "desc"), ("created_at", "asc"),
                                        ("author", "asc"), ("author", "desc"), ("channel", "asc")])
def test_cursor_pagination_no_gaps_or_duplicates(client, sort, order):
    full = _ids(client.get("/api/messages", params={"sort": sort, "order": order, "limit": 200}))
    seen, cursor, pages = [], None, 0
    while True:
        p = {"sort": sort, "order": order, "limit": 5}
        if cursor:
            p["cursor"] = cursor
        body = client.get("/api/messages", params=p).json()
        seen += [it["original_message_id"] for it in body["items"]]
        pages += 1
        cursor = body["next_cursor"]
        if not cursor:
            break
    assert pages == 3 and seen == full and len(set(seen)) == 12


def test_cursor_and_limit_validation(client):
    assert client.get("/api/messages", params={"cursor": "garbage"}).status_code == 400
    cur = client.get("/api/messages", params={"limit": 2}).json()["next_cursor"]
    assert client.get("/api/messages", params={"cursor": cur, "sort": "author"}).status_code == 400
    assert client.get("/api/messages", params={"limit": 201}).status_code == 422
    assert client.get("/api/messages", params={"limit": 0}).status_code == 422
    assert client.get("/api/messages", params={"author_id": "x"}).status_code == 400


def test_facets(client):
    body = client.get("/api/messages/facets").json()
    assert [m["mapping_name"] for m in body["mappings"]] == ["Alpha", "Beta"]
    chans = {c["original_channel_id"]: c for c in body["channels"]}
    assert chans["10"]["name"] == "general" and chans["10"]["count"] == 6
    assert chans["12"]["name"] is None and chans["12"]["count"] == 1


# --- live content ---

def test_content_ok_and_cached(client, sent):
    url = f"/api/messages/{_mid(0)}/content"
    res = client.get(url, params={"cloned_guild_id": CG})
    assert res.status_code == 200
    body = res.json()
    assert body["message"]["content"] == "hello" and body["cached"] is False
    assert body["row"]["original_message_id"] == str(_mid(0))
    targets = sent[0]["data"]["targets"]
    assert sent[0]["type"] == "fetch_message"
    assert targets[0] == {"channel_id": "20", "message_id": "9000", "source": "clone"}
    assert targets[1]["source"] == "original"
    assert client.get(url, params={"cloned_guild_id": CG}).json()["cached"] is True
    assert len(sent) == 1


def test_content_bot_not_running(client):
    client.replies["res"] = {"ok": False, "running": False, "error": "Connect call failed"}
    res = client.get(f"/api/messages/{_mid(1)}/content")
    assert res.status_code == 503 and res.json()["detail"] == "bot-not-running"
    client.replies["res"] = {"ok": False, "error": "bot_not_ready", "status": 503}
    assert client.get(f"/api/messages/{_mid(1)}/content").status_code == 503


def test_content_deleted_and_forbidden(client):
    client.replies["res"] = {"ok": False, "error": "not_found", "status": 404}
    res = client.get(f"/api/messages/{_mid(2)}/content")
    assert res.status_code == 404 and res.json()["detail"] == "message-deleted"
    client.replies["res"] = {"ok": False, "error": "forbidden", "status": 403}
    res = client.get(f"/api/messages/{_mid(3)}/content")
    assert res.status_code == 403 and res.json()["detail"] == "no-access"


def test_content_rate_limited_not_cached(client, sent):
    client.replies["res"] = {"ok": False, "error": "rate_limited", "status": 429, "retry_after": 2.5}
    res = client.get(f"/api/messages/{_mid(4)}/content")
    assert res.status_code == 429
    assert res.json()["retry_after"] == 2.5 and res.headers["retry-after"] == "3"
    client.replies["res"] = {"ok": True, "source": "clone", "message": {"content": "x"}}
    assert client.get(f"/api/messages/{_mid(4)}/content").status_code == 200
    assert len(sent) == 2


def test_content_unknown_message(client):
    assert client.get("/api/messages/42/content").status_code == 404
    assert client.get("/api/messages/abc/content").status_code == 400


def test_auth_required(client):
    anon = TestClient(app)
    anon.headers.update({"origin": ORIGIN})
    for path in ("/api/messages", "/api/messages/facets", f"/api/messages/{_mid(0)}/content"):
        assert anon.get(path).status_code == 401, path
