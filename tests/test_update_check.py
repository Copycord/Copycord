"""
Tests for the update check on start and the auto-update
(src/copycord/update_check.py). The network is mocked.
"""
import sqlite3
import time

import pytest

from copycord import update, update_check as uc


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv(uc.ENV_OPT_OUT, raising=False)
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    monkeypatch.setattr(update, "installed_commit", lambda: "a" * 40)


def set_config(tmp_path, key, value):
    d = tmp_path / "data"
    d.mkdir(exist_ok=True)
    conn = sqlite3.connect(d / "data.db")
    conn.execute("CREATE TABLE IF NOT EXISTS app_config (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT OR REPLACE INTO app_config VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()


def fake_release(version):
    return update.Release(f"v{version}", version, False, None)


def test_hint_text_release():
    r = uc.Result(True, "4.0.0", "4.0.1", 0)
    assert uc.hint_text(r) == "Update available: v4.0.1 (you have v4.0.0). Run copycord update"


def test_hint_text_dev():
    r = uc.Result(True, "a" * 40, "b" * 40, 0, True, "main")
    assert uc.hint_text(r) == "Update available: main is at bbbbbbbbbbbb (you have aaaaaaaaaaaa). Run copycord update"


def test_release_newer_and_up_to_date(monkeypatch):
    monkeypatch.setattr(update, "fetch_latest_release", lambda **kw: fake_release("4.0.1"))
    assert uc.fetch(update.Channel(update.STABLE)).available
    monkeypatch.setattr(update, "fetch_latest_release", lambda **kw: fake_release("4.0.0"))
    assert not uc.fetch(update.Channel(update.STABLE)).available


def test_beta_asks_for_prereleases(monkeypatch):
    seen = {}
    monkeypatch.setattr(update, "fetch_latest_release",
                        lambda prerelease, timeout: seen.setdefault("p", prerelease) and fake_release("4.0.0"))
    uc.fetch(update.Channel(update.BETA))
    assert seen["p"] is True


def test_dev_sha_comparison(monkeypatch):
    head = update.Commit("b" * 40, "", "")
    monkeypatch.setattr(update, "fetch_branch_head", lambda *a, **kw: head)
    ch = update.Channel(update.DEV, "main")
    assert uc.fetch(ch).available
    monkeypatch.setattr(update, "installed_commit", lambda: "b" * 40)
    assert not uc.fetch(ch).available
    monkeypatch.setattr(update, "installed_commit", lambda: None)
    assert not uc.fetch(ch).available


def test_cache_used_within_24h_and_refreshed_after(monkeypatch):
    calls = []

    def fetch_release(**kw):
        calls.append(1)
        return fake_release("4.0.1")

    monkeypatch.setattr(update, "fetch_latest_release", fetch_release)
    ch = update.Channel(update.STABLE)
    uc.check_cached(ch)
    uc.check_cached(ch)
    assert len(calls) == 1
    assert uc.load_cache(ch, now=time.time() + uc.CACHE_TTL + 5) is None
    uc.check_cached(ch, force=True)
    assert len(calls) == 2


def test_cache_ignored_for_other_channel_or_version(monkeypatch):
    monkeypatch.setattr(update, "fetch_latest_release", lambda **kw: fake_release("4.0.1"))
    uc.check_cached(update.Channel(update.STABLE))
    assert uc.load_cache(update.Channel(update.BETA)) is None
    monkeypatch.setattr(update, "current_version", lambda: "4.0.1")
    assert uc.load_cache(update.Channel(update.STABLE)) is None


def test_network_error_never_raises_from_start(monkeypatch, capsys):
    def boom(**kw):
        raise OSError("offline")

    monkeypatch.setattr(update, "fetch_latest_release", boom)
    t = uc.start()
    t.join(5)
    assert capsys.readouterr().out == ""


def test_start_prints_one_hint(monkeypatch, capsys):
    monkeypatch.setattr(update, "fetch_latest_release", lambda **kw: fake_release("4.0.1"))
    uc.start().join(5)
    out = capsys.readouterr().out
    assert out.count("Update available: v4.0.1 (you have v4.0.0). Run copycord update") == 1


def test_env_opt_out(monkeypatch):
    monkeypatch.setenv(uc.ENV_OPT_OUT, "1")
    assert uc.start() is None


def test_config_opt_out(tmp_path, monkeypatch):
    assert uc.check_enabled()
    set_config(tmp_path, uc.KEY_ON_START, "false")
    assert not uc.check_enabled()
    assert uc.start() is None


def test_auto_update_off_by_default(tmp_path):
    assert not uc.auto_update_enabled()
    set_config(tmp_path, uc.KEY_AUTO_UPDATE, "true")
    assert uc.auto_update_enabled()


def test_auto_update_skips_terminal_and_applies_in_service(tmp_path, monkeypatch):
    set_config(tmp_path, uc.KEY_AUTO_UPDATE, "true")
    monkeypatch.setattr(update, "fetch_latest_release", lambda **kw: fake_release("4.0.1"))
    monkeypatch.setattr(update, "detect_installer", lambda *a, **kw: update.UV_TOOL)
    applied = []
    monkeypatch.setattr(update, "update", lambda **kw: applied.append(1) or 0)
    monkeypatch.setattr(uc.ui, "is_interactive", lambda: True)
    assert not uc.auto_update_once() and not applied
    monkeypatch.setattr(uc.ui, "is_interactive", lambda: False)
    assert uc.auto_update_once() and applied == [1]


def test_auto_update_does_nothing_when_disabled(monkeypatch):
    monkeypatch.setattr(update, "update", lambda **kw: pytest.fail("must not update"))
    assert not uc.auto_update_once()
