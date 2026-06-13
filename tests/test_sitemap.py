import importlib
import sys
import types


def _import_sitemap(monkeypatch):
    if "client.sitemap" in sys.modules:
        return sys.modules["client.sitemap"]

    monkeypatch.setitem(sys.modules, "discord", types.SimpleNamespace())
    return importlib.import_module("client.sitemap")


def test_row_get_supports_sqlite_row(db, monkeypatch):
    sitemap = _import_sitemap(monkeypatch)

    db.upsert_category_mapping(10, "Cat", 20, "Cat-C", 1, 2)
    db.upsert_channel_mapping(
        100,
        "forum",
        200,
        None,
        10,
        20,
        15,
        original_guild_id=1,
        cloned_guild_id=2,
    )
    db.upsert_forum_thread_mapping(
        orig_thread_id=3000,
        orig_thread_name="Thread 1",
        clone_thread_id=4000,
        forum_orig_id=100,
        forum_clone_id=200,
        original_guild_id=1,
        cloned_guild_id=2,
    )

    row = db.get_all_threads()[0]

    assert not hasattr(row, "get")
    assert sitemap._row_get(row, "original_guild_id") == 1
    assert sitemap._row_get(row, "missing_column", "fallback") == "fallback"
