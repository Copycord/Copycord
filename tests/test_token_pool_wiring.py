"""Server wiring of the warm token pool, driven by a fake pool, a fake DB and a fake proxy pool (no Discord)."""

import asyncio
import importlib
import logging
import types
from types import SimpleNamespace

import pytest

from copycord.server.token_sender import SEND_TRANSIENT
from tests.test_token_sender_pool import FakePool as SenderFakePool
from tests.test_token_sender_pool import _DB as SenderDB
from tests.test_token_sender_pool import _record_sends, _send, _sender


@pytest.fixture
def srv():
    # importing the server module retunes library log levels; put them back (as test_server_purge_assets does)
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    levels = {n: lg.level for n, lg in logging.root.manager.loggerDict.items() if isinstance(lg, logging.Logger)}
    try:
        yield importlib.import_module("copycord.server.server")
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
        for n, lg in list(logging.root.manager.loggerDict.items()):
            if isinstance(lg, logging.Logger):
                lg.setLevel(levels.get(n, logging.NOTSET))


class FakePool:
    """The slice of TokenSessionPool the server drives."""

    def __init__(self, online=()):
        self.online = set(online)
        self.ensured = []
        self.closed = []
        self.close_all_calls = 0
        self.gate = None

    async def ensure(self, token_id, token_value, *, keep_online, presence):
        if self.gate is not None:
            await self.gate.wait()
        self.ensured.append((token_id, token_value, keep_online, presence))

    async def close(self, token_id):
        self.closed.append(token_id)

    async def close_all(self):
        self.close_all_calls += 1

    def online_ids(self):
        return set(self.online)

    def snapshot(self):
        return {"keep_online": 0, "live": len(self.online), "max_loop_lag_ms": 0.0, "tokens": {}}


class FakeDB:
    """Mappings as (mapping_id, settings, [(token_id, token_value, enabled)])."""

    def __init__(self, mappings):
        self.mappings = mappings
        self.loads = 0

    def list_guild_mappings(self):
        self.loads += 1
        return [
            {
                "mapping_id": mid,
                "original_guild_id": str(100 + i),
                "cloned_guild_id": str(200 + i),
                "settings": settings,
            }
            for i, (mid, settings, _toks) in enumerate(self.mappings)
        ]

    def get_enabled_mapping_tokens(self, mapping_id):
        for mid, _settings, toks in self.mappings:
            if mid == mapping_id:
                return [
                    {"token_id": tid, "token_value": value}
                    for tid, value, enabled in toks
                    if enabled
                ]
        return []

    def settings_for(self, original_guild_id):
        return dict(self.mappings[original_guild_id - 100][1])


class FakeProxies:
    enabled = True

    def __init__(self):
        self.leases = {}

    async def lease(self, token):
        return self.leases.setdefault(token, f"http://proxy-{len(self.leases)}")

    def current(self, token):
        return self.leases.get(token)


def _settings(keep=5, presence="random", proxy=False, use=True):
    return {
        "USE_USER_TOKENS": use,
        "USER_TOKEN_KEEP_ONLINE": keep,
        "USER_TOKEN_PRESENCE": presence,
        "USER_TOKEN_USE_PROXIES": proxy,
    }


@pytest.fixture
def make_rx(srv, monkeypatch):
    proxies = FakeProxies()
    monkeypatch.setattr(srv, "get_proxy_pool", lambda: proxies)

    def build(mappings, online=()):
        db = FakeDB(mappings)
        monkeypatch.setattr(
            srv,
            "resolve_mapping_settings",
            lambda db_, config, *, original_guild_id=None, cloned_guild_id=None, mapping_id=None: db.settings_for(
                original_guild_id
            ),
        )
        r = srv.ServerReceiver.__new__(srv.ServerReceiver)
        r._shutting_down = False
        r.db = db
        r.config = SimpleNamespace()
        r.token_pool = FakePool(online)
        r.user_token_sender = SimpleNamespace(_dead_tokens=set())
        r._pool_proxied = set()
        r._pool_ids = set()
        r._pool_lock = asyncio.Lock()
        r._pool_again = False
        r._pool_debounce_task = None
        r._pool_loop_task = None
        r._pool_online_seen = None
        r._pool_identity_saved = {}
        r.proxies = proxies
        return r

    return build


# plan_token_pool


def _m(mid, settings, *toks):
    return {
        "mapping_id": mid,
        "settings": settings,
        "tokens": [{"token_id": tid, "token_value": v} for tid, v in toks],
    }


def test_plan_unions_mappings_and_always_proxies(srv):
    plan = srv.plan_token_pool(
        [
            _m("m1", _settings(keep=2, presence="busy", proxy=False), ("1", "X"), ("2", "Y")),
            _m("m2", _settings(keep=1, presence="online", proxy=True), ("3", "X")),
        ]
    )
    x = plan["tokens"]["X"]
    assert x["ids"] == ["1", "3"]
    assert x["mappings"] == ["m1", "m2"]
    assert x["use_proxy"] is True
    # the stored USER_TOKEN_USE_PROXIES=False is ignored
    assert plan["tokens"]["Y"]["use_proxy"] is True
    # The largest keep_online and the first mapping's presence win.
    assert x["keep_online"] == 2
    assert x["presence"] == "busy"


def test_plan_counts_a_shared_token_once(srv):
    plan = srv.plan_token_pool(
        [
            _m("m1", _settings(keep=1), ("1", "X"), ("2", "Y")),
            _m("m2", _settings(keep=1), ("3", "Z"), ("4", "X")),
        ]
    )
    # m2 picks the token m1 already wants instead of starting a second session.
    assert plan["wanted"] == ["X"]
    assert plan["keep_online"] == 1


def test_plan_caps_keep_online_at_enabled_tokens(srv):
    plan = srv.plan_token_pool([_m("m1", _settings(keep=5), ("1", "A"), ("2", "B"))])
    assert plan["wanted"] == ["A", "B"]
    assert plan["keep_online"] == 2

    plan = srv.plan_token_pool(
        [_m("m1", _settings(keep=2), ("1", "A"), ("2", "B"), ("3", "C"))],
        online_values={"C"},
    )
    # A token already online keeps its slot.
    assert plan["wanted"] == ["C", "A"]


def test_plan_skips_dead_tokens_and_mappings_without_user_tokens(srv):
    plan = srv.plan_token_pool(
        [
            _m("m1", _settings(keep=5), ("1", "A"), ("2", "B")),
            _m("m2", _settings(keep=5, use=False), ("3", "C")),
        ],
        skip_values={"A"},
    )
    assert plan["wanted"] == ["B"]
    assert "C" not in plan["tokens"]


# reconcile


@pytest.mark.asyncio
async def test_reconcile_ensures_wanted_tokens_with_presence_and_global_keep(make_rx):
    rx = make_rx(
        [
            ("m1", _settings(keep=1, presence="invisible"), [("1", "X", True), ("2", "Y", True)]),
            ("m2", _settings(keep=2, presence="busy"), [("3", "Z", True), ("4", "W", True)]),
        ]
    )
    await rx._reconcile_token_pool()

    assert rx.token_pool.ensured == [
        ("1", "X", 3, "invisible"),
        ("3", "Z", 3, "busy"),
        ("4", "W", 3, "busy"),
    ]
    assert rx._pool_ids == {"1", "3", "4"}
    assert rx.token_pool.close_all_calls == 0


@pytest.mark.asyncio
async def test_reconcile_shared_token_is_one_proxied_session(make_rx):
    rx = make_rx(
        [
            ("m1", _settings(keep=1, proxy=False), [("1", "X", True)]),
            ("m2", _settings(keep=1, proxy=True), [("2", "X", True)]),
            ("m3", _settings(keep=1, proxy=False), [("3", "Y", True)]),
        ]
    )
    await rx._reconcile_token_pool()

    # Both ids of the shared token go to the pool, which keys the session by value.
    assert [e[:3] for e in rx.token_pool.ensured] == [("1", "X", 2), ("2", "X", 2), ("3", "Y", 2)]
    # Proxies are mandatory: every wanted token is proxied, whatever the stored flag says.
    assert rx._pool_proxied == {"X", "Y"}
    assert list(rx.proxies.leases) == ["X", "Y"]
    # proxy_for hands the gateway the lease REST reads.
    assert rx._token_pool_proxy_for("X") == rx.proxies.current("X")
    assert rx._token_pool_proxy_for("Y") == rx.proxies.current("Y")
    # A token with no lease gets None, which the pool takes as "do not connect".
    assert rx._token_pool_proxy_for("Z") is None


@pytest.mark.asyncio
async def test_reconcile_without_proxies_leases_nothing(make_rx):
    rx = make_rx([("m1", _settings(keep=1), [("1", "X", True)])])
    rx.proxies.enabled = False
    await rx._reconcile_token_pool()
    assert rx.proxies.leases == {}
    assert rx._token_pool_proxy_for("X") is None


@pytest.mark.asyncio
async def test_keep_online_zero_everywhere_closes_all(make_rx):
    rx = make_rx(
        [
            ("m1", _settings(keep=2), [("1", "X", True)]),
            ("m2", _settings(keep=1), [("2", "Y", True)]),
        ]
    )
    await rx._reconcile_token_pool()
    assert rx._pool_ids == {"1", "2"}

    for i, (mid, settings, toks) in enumerate(rx.db.mappings):
        rx.db.mappings[i] = (mid, dict(settings, USER_TOKEN_KEEP_ONLINE=0), toks)
    await rx._reconcile_token_pool()

    assert sorted(rx.token_pool.closed) == ["1", "2"]
    assert rx.token_pool.close_all_calls == 1
    assert rx._pool_ids == set()


@pytest.mark.asyncio
async def test_disabled_or_deleted_token_is_closed(make_rx):
    rx = make_rx([("m1", _settings(keep=5), [("1", "X", True), ("2", "Y", True), ("3", "Z", True)])])
    await rx._reconcile_token_pool()
    assert rx._pool_ids == {"1", "2", "3"}

    rx.db.mappings[0] = ("m1", rx.db.mappings[0][1], [("1", "X", True), ("2", "Y", False)])
    rx.token_pool.ensured.clear()
    await rx._reconcile_token_pool()

    assert sorted(rx.token_pool.closed) == ["2", "3"]
    assert rx.token_pool.ensured == [("1", "X", 1, "random")]
    assert rx._pool_ids == {"1"}


@pytest.mark.asyncio
async def test_sender_dead_token_is_skipped_and_closed(make_rx):
    rx = make_rx([("m1", _settings(keep=5), [("1", "X", True), ("2", "Y", True)])])
    await rx._reconcile_token_pool()

    rx.user_token_sender._dead_tokens.add("Y")
    rx.token_pool.ensured.clear()
    await rx._reconcile_token_pool()

    assert rx.token_pool.closed == ["2"]
    assert rx.token_pool.ensured == [("1", "X", 1, "random")]


@pytest.mark.asyncio
async def test_overlapping_reconciles_coalesce_into_one_rerun(make_rx):
    rx = make_rx([("m1", _settings(keep=1), [("1", "X", True)])])
    rx.token_pool.gate = asyncio.Event()

    first = asyncio.create_task(rx._reconcile_token_pool())
    await asyncio.sleep(0)
    others = [asyncio.create_task(rx._reconcile_token_pool()) for _ in range(5)]
    await asyncio.gather(*others)
    assert rx.db.loads == 1

    rx.token_pool.gate.set()
    await first
    # Five overlapping triggers cost one extra pass, not five.
    assert rx.db.loads == 2


@pytest.mark.asyncio
async def test_debounced_triggers_run_one_reconcile(make_rx):
    rx = make_rx([("m1", _settings(keep=1), [("1", "X", True)])])
    for _ in range(4):
        rx._schedule_token_pool_reconcile(delay=0.05)
    await asyncio.sleep(0.2)
    assert rx.db.loads == 1


@pytest.mark.asyncio
async def test_status_publish_keeps_the_server_fields(make_rx):
    rx = make_rx([], online={"1"})
    sent = []

    async def status(**fields):
        sent.append(fields)

    rx.bus = SimpleNamespace(status=status)
    rx.bot = SimpleNamespace(
        is_ready=lambda: True, is_closed=lambda: False, user=SimpleNamespace(name="bot")
    )
    await rx._publish_token_pool_status()
    await rx._publish_token_pool_status()

    # Unchanged online set: one publish.
    assert len(sent) == 1
    assert sent[0]["running"] is True
    assert sent[0]["discord"] == {"ready": True}
    assert sent[0]["token_pool"]["live"] == 1


def test_status_persists_identity_only_on_change(make_rx):
    rx = make_rx([])
    ident = {"username": "relay.alpha", "display_name": "Ava", "avatar_url": "https://x/a.png", "user_id": "7"}
    tokens = {
        "1": {"state": "online", "session": "k1", "identity": dict(ident)},
        "2": {"state": "online", "session": "k1", "identity": dict(ident)},
        "3": {"state": "connecting", "session": "k3", "identity": None},
    }
    rx.token_pool.snapshot = lambda: {"live": 1, "tokens": tokens}
    saved = []
    rx.db.update_token_identity = lambda **kw: saved.append(kw) or 1

    rx._token_pool_status()
    rx._token_pool_status()
    # Two ids on one session (one token value) and two publishes: one write.
    assert saved == [dict(token_id="1", **ident)]

    tokens["1"]["identity"] = dict(ident, display_name="Ava R")
    tokens["2"]["identity"] = dict(ident, display_name="Ava R")
    rx._token_pool_status()
    assert len(saved) == 2 and saved[1]["display_name"] == "Ava R"

    def boom(**kw):
        raise RuntimeError("db locked")

    rx.db.update_token_identity = boom
    tokens["1"]["identity"] = dict(ident, avatar_url="https://x/b.png")
    tokens["2"]["identity"] = dict(ident, avatar_url="https://x/b.png")
    # A failed write is retried on the next publish and never breaks the status.
    assert rx._token_pool_status()["live"] == 1
    rx.db.update_token_identity = lambda **kw: saved.append(kw) or 1
    rx._token_pool_status()
    assert saved[-1]["avatar_url"] == "https://x/b.png"


# round robin over online only


@pytest.mark.asyncio
async def test_round_robin_never_repeats_an_online_account(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = SenderFakePool(online={"a", "c", "e"})
    s = _sender(SenderDB(["a", "b", "c", "d", "e"]), pool)

    firsts = []
    for _ in range(9):
        tried.clear()
        await _send(s)
        firsts.append(tried[0])
        assert sorted(tried[3:]) == ["tb", "td"]

    assert firsts == ["ta", "tc", "te"] * 3


@pytest.mark.asyncio
async def test_round_robin_without_online_tokens_rotates_over_all(monkeypatch):
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    s = _sender(SenderDB(["a", "b", "c"]), SenderFakePool())

    firsts = []
    for _ in range(3):
        tried.clear()
        await _send(s)
        firsts.append(tried[0])
    assert firsts == ["ta", "tb", "tc"]
