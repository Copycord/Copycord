"""Server wiring of the warm token pool, driven by a fake pool, a fake DB and a fake proxy pool (no Discord)."""

import asyncio
import importlib
import logging
import types
from types import SimpleNamespace

import pytest

from copycord.common import token_proxy
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
        # token_id -> the rotate flag of its last ensure
        self.rotate = {}
        # token_id -> the use_proxy flag of its last ensure
        self.use_proxy = {}
        self.closed = []
        self.close_all_calls = 0
        self.gate = None

    async def ensure(self, token_id, token_value, *, keep_online, presence, rotate=False, use_proxy=True):
        if self.gate is not None:
            await self.gate.wait()
        self.ensured.append((token_id, token_value, keep_online, presence))
        self.rotate[token_id] = rotate
        self.use_proxy[token_id] = use_proxy

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


def _settings(presence="random", proxy=True, use=True, rotate=None, legacy_keep=None):
    settings = {
        "USE_USER_TOKENS": use,
        "USER_TOKEN_PRESENCE": presence,
        "USER_TOKEN_USE_PROXIES": proxy,
    }
    if rotate is not None:
        settings["USER_TOKEN_PRESENCE_ROTATE"] = rotate
    # The removed Keep online setting, as an old database may still hold it.
    if legacy_keep is not None:
        settings["USER_TOKEN_KEEP_ONLINE"] = legacy_keep
    return settings


@pytest.fixture
def make_rx(srv, monkeypatch):
    proxies = FakeProxies()
    monkeypatch.setattr(srv, "get_proxy_pool", lambda: proxies)
    monkeypatch.setattr(token_proxy, "_tokens_disabled_noticed", False)
    monkeypatch.setattr(srv, "logger", logging.getLogger("test.server_pool"))

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


def test_plan_unions_mappings_and_proxies_a_token_any_of_them_proxies(srv):
    plan = srv.plan_token_pool(
        [
            _m("m1", _settings(presence="busy", proxy=False), ("1", "X"), ("2", "Y")),
            _m("m2", _settings(presence="online", proxy=True), ("3", "X")),
        ]
    )
    x = plan["tokens"]["X"]
    assert x["ids"] == ["1", "3"]
    assert x["mappings"] == ["m1", "m2"]
    # shared with a mapping that wants proxies: proxied
    assert x["use_proxy"] is True
    # only used by the mapping with proxies off: direct
    assert plan["tokens"]["Y"]["use_proxy"] is False


def test_plan_proxies_by_default_when_the_setting_is_missing(srv):
    settings = {"USE_USER_TOKENS": True}
    plan = srv.plan_token_pool([_m("m1", settings, ("1", "X")), _m("m2", {**settings, "USER_TOKEN_USE_PROXIES": None}, ("2", "Y"))])
    assert plan["tokens"]["X"]["use_proxy"] is True
    assert plan["tokens"]["Y"]["use_proxy"] is True
    # The first mapping's presence wins; there is no per-token cap any more.
    assert x["presence"] == "busy"
    assert "keep_online" not in x


def test_plan_rotate_is_on_if_any_mapping_sharing_the_token_has_it_on(srv):
    plan = srv.plan_token_pool(
        [
            _m("m1", _settings(rotate=False), ("1", "X"), ("2", "Y")),
            _m("m2", _settings(presence="busy", rotate=True), ("3", "X"), ("4", "Z")),
            _m("m3", _settings(), ("5", "W")),
        ]
    )
    x = plan["tokens"]["X"]
    assert x["rotate"] is True
    # presence still follows the first mapping
    assert x["presence"] == "random"
    assert plan["tokens"]["Y"]["rotate"] is False
    assert plan["tokens"]["Z"]["rotate"] is True
    # missing setting means off
    assert plan["tokens"]["W"]["rotate"] is False


@pytest.mark.parametrize(
    "value,on",
    [(True, True), (1, True), ("true", True), ("True", True), ("1", True),
     (False, False), (0, False), ("false", False), ("", False), (None, False), ("0", False)],
)
def test_plan_reads_rotate_bool_ish(srv, value, on):
    plan = srv.plan_token_pool([_m("m1", _settings(rotate=value), ("1", "X"))])
    assert plan["tokens"]["X"]["rotate"] is on


def test_plan_counts_a_shared_token_once(srv):
    plan = srv.plan_token_pool(
        [
            _m("m1", _settings(), ("1", "X"), ("2", "Y")),
            _m("m2", _settings(), ("3", "Z"), ("4", "X")),
        ]
    )
    # Every token is wanted; X, used by both mappings, is one session.
    assert plan["wanted"] == ["X", "Y", "Z"]
    assert plan["keep_online"] == 3
    assert plan["tokens"]["X"]["ids"] == ["1", "4"]


def test_plan_three_mappings_one_shared_token(srv):
    plan = srv.plan_token_pool(
        [
            _m("m1", _settings(), ("1", "A"), ("2", "S")),
            _m("m2", _settings(), ("3", "B"), ("4", "S"), ("5", "C")),
            _m("m3", _settings(), ("6", "S"), ("7", "D")),
        ]
    )
    assert plan["wanted"] == ["A", "S", "B", "C", "D"]
    # The global count equals the distinct wanted set, so the pool cap trims no one.
    assert plan["keep_online"] == len(set(plan["wanted"])) == 5
    assert plan["tokens"]["S"]["mappings"] == ["m1", "m2", "m3"]


@pytest.mark.parametrize("legacy", [0, 2, "3", "junk", None, -1])
def test_plan_ignores_a_stored_keep_online(srv, legacy):
    settings = _settings()
    settings["USER_TOKEN_KEEP_ONLINE"] = legacy
    plan = srv.plan_token_pool(
        [
            _m("m1", settings, ("1", "A"), ("2", "B"), ("3", "C")),
            _m("m2", _settings(legacy_keep=1), ("4", "C"), ("5", "D")),
        ]
    )
    assert plan["wanted"] == ["A", "B", "C", "D"]
    assert plan["keep_online"] == 4


def test_plan_skips_dead_disabled_tokens_and_mappings_without_user_tokens(srv):
    m1 = _m("m1", _settings(), ("1", "A"), ("2", "B"), ("3", "E"))
    # A row the caller did not filter still counts as disabled when it says so.
    m1["tokens"][2]["enabled"] = 0
    plan = srv.plan_token_pool(
        [
            m1,
            _m("m2", _settings(use=False), ("4", "C")),
            _m("m3", _settings(), ("5", "  "), ("6", "F")),
        ],
        skip_values={"A"},
    )
    assert plan["wanted"] == ["B", "F"]
    assert plan["keep_online"] == 2
    assert {"A", "C", "E"}.isdisjoint(plan["tokens"])


def test_plan_reads_old_stored_settings_without_error(srv):
    """A mapping row saved with USER_TOKEN_KEEP_ONLINE resolves and every token is wanted."""
    from copycord.common.common_helpers import resolve_mapping_settings
    from copycord.common.config import Config

    defaults = Config.default_mapping_settings(None)
    assert "USER_TOKEN_KEEP_ONLINE" not in defaults
    cfg = SimpleNamespace(default_mapping_settings=lambda: dict(defaults))
    stored = '{"USE_USER_TOKENS": true, "USER_TOKEN_KEEP_ONLINE": 0, "USER_TOKEN_PRESENCE": "idle"}'
    db = SimpleNamespace(get_mapping_by_id=lambda mid: {"settings": stored, "status": "active"})
    eff = resolve_mapping_settings(db, cfg, mapping_id=1)
    assert eff["USER_TOKEN_PRESENCE"] == "idle"

    plan = srv.plan_token_pool([_m("m1", eff, ("1", "A"), ("2", "B"))])
    assert plan["wanted"] == ["A", "B"]
    assert plan["keep_online"] == 2
    assert plan["tokens"]["A"]["presence"] == "idle"


# reconcile


@pytest.mark.asyncio
async def test_reconcile_ensures_wanted_tokens_with_presence_and_global_keep(make_rx):
    rx = make_rx(
        [
            ("m1", _settings(presence="idle", legacy_keep=1), [("1", "X", True), ("2", "Y", True)]),
            ("m2", _settings(presence="busy", legacy_keep=0), [("3", "Z", True), ("4", "W", True)]),
        ]
    )
    await rx._reconcile_token_pool()

    # Every enabled token is ensured with the total; the stored cap is ignored.
    assert rx.token_pool.ensured == [
        ("1", "X", 4, "idle"),
        ("2", "Y", 4, "idle"),
        ("3", "Z", 4, "busy"),
        ("4", "W", 4, "busy"),
    ]
    assert rx._pool_ids == {"1", "2", "3", "4"}
    # rotation is off unless a mapping turns it on
    assert rx.token_pool.rotate == {"1": False, "2": False, "3": False, "4": False}


@pytest.mark.asyncio
async def test_reconcile_passes_rotate_ored_across_mappings(make_rx):
    rx = make_rx(
        [
            ("m1", _settings(rotate=False), [("1", "X", True)]),
            ("m2", _settings(rotate="true"), [("2", "X", True), ("3", "Y", True)]),
            ("m3", _settings(), [("4", "Z", True)]),
        ]
    )
    await rx._reconcile_token_pool()
    assert rx.token_pool.rotate == {"1": True, "2": True, "3": True, "4": False}

    # turning it off in m2 turns it off for the shared token on the next reconcile
    rx.db.mappings[1] = ("m2", _settings(rotate=False), rx.db.mappings[1][2])
    await rx._reconcile_token_pool()
    assert rx.token_pool.rotate == {"1": False, "2": False, "3": False, "4": False}
    assert rx.token_pool.close_all_calls == 0


@pytest.mark.asyncio
async def test_reconcile_shared_token_is_one_proxied_session(make_rx):
    rx = make_rx(
        [
            ("m1", _settings(proxy=False), [("1", "X", True)]),
            ("m2", _settings(proxy=True), [("2", "X", True)]),
            ("m3", _settings(proxy=False), [("3", "Y", True)]),
        ]
    )
    await rx._reconcile_token_pool()

    # Both ids of the shared token go to the pool, which keys the session by value.
    assert [e[:3] for e in rx.token_pool.ensured] == [("1", "X", 2), ("2", "X", 2), ("3", "Y", 2)]
    # X is shared with m2, which wants proxies, so it stays proxied; Y is only m3's (off).
    assert rx._pool_proxied == {"X"}
    assert rx.token_pool.use_proxy == {"1": True, "2": True, "3": False}
    assert list(rx.proxies.leases) == ["X"]
    # Sends read the same rule: only Y may go direct.
    assert token_proxy.resolve_use_proxy("X", False) is True
    assert token_proxy.resolve_use_proxy("Y", False) is False
    assert token_proxy.resolve_use_proxy("Y", True) is True
    # proxy_for hands the gateway the lease REST reads.
    assert rx._token_pool_proxy_for("X") == rx.proxies.current("X")
    # A token with no lease gets None, which the pool takes as "do not connect".
    assert rx._token_pool_proxy_for("Z") is None


@pytest.mark.asyncio
async def test_zero_proxies_with_every_mapping_off_connects_direct_and_says_nothing(make_rx, caplog):
    caplog.set_level(logging.INFO, logger="test.server_pool")
    rx = make_rx([("m1", _settings(proxy=False), [("1", "X", True), ("2", "Y", True)])])
    rx.proxies.enabled = False
    await rx._reconcile_token_pool()
    assert _notices(caplog) == []
    assert [e[:2] for e in rx.token_pool.ensured] == [("1", "X"), ("2", "Y")]
    assert rx.token_pool.use_proxy == {"1": False, "2": False}
    assert rx.proxies.leases == {}
    assert rx.token_pool.close_all_calls == 0


@pytest.mark.asyncio
async def test_zero_proxies_keeps_proxied_tokens_offline_and_direct_ones_online(make_rx, caplog):
    caplog.set_level(logging.INFO, logger="test.server_pool")
    rx = make_rx(
        [
            ("m1", _settings(proxy=False), [("1", "X", True), ("2", "Y", True)]),
            ("m2", _settings(proxy=True), [("3", "Y", True)]),
        ]
    )
    rx.proxies.enabled = False
    await rx._reconcile_token_pool()
    # Y is shared with a mapping that wants proxies: offline, and the notice says why
    assert _notices(caplog) == [token_proxy.TOKENS_DISABLED_NOTICE]
    assert [e[:3] for e in rx.token_pool.ensured] == [("1", "X", 1)]
    assert rx._pool_ids == {"1"}
    assert rx.proxies.leases == {}


@pytest.mark.asyncio
async def test_reconcile_without_proxies_leases_nothing(make_rx):
    rx = make_rx([("m1", _settings(), [("1", "X", True)])])
    rx.proxies.enabled = False
    await rx._reconcile_token_pool()
    assert rx.proxies.leases == {}
    assert rx._token_pool_proxy_for("X") is None


def _notices(caplog):
    return [
        r.getMessage()
        for r in caplog.records
        if r.getMessage() in (token_proxy.TOKENS_DISABLED_NOTICE, token_proxy.TOKENS_ENABLED_NOTICE)
    ]


@pytest.mark.asyncio
async def test_zero_proxies_closes_the_pool_and_notices_once_per_state_change(make_rx, caplog):
    caplog.set_level(logging.INFO, logger="test.server_pool")
    rx = make_rx([("m1", _settings(), [("1", "X", True), ("2", "Y", True)])])
    await rx._reconcile_token_pool()
    assert rx._pool_ids == {"1", "2"}
    rx.token_pool.ensured.clear()

    rx.proxies.enabled = False
    for _ in range(3):
        await rx._reconcile_token_pool()
    assert rx.token_pool.ensured == []
    assert sorted(rx.token_pool.closed) == ["1", "2"]
    assert rx.token_pool.close_all_calls == 3
    assert rx._pool_ids == set()
    assert rx._pool_proxied == set()
    assert _notices(caplog) == [token_proxy.TOKENS_DISABLED_NOTICE]
    # no token ids or values in it
    assert all(s not in token_proxy.TOKENS_DISABLED_NOTICE for s in ("X", "Y"))

    # proxies come back: one info line, then the tokens connect
    rx.proxies.enabled = True
    await rx._reconcile_token_pool()
    await rx._reconcile_token_pool()
    assert [e[:2] for e in rx.token_pool.ensured][:2] == [("1", "X"), ("2", "Y")]
    assert rx._pool_ids == {"1", "2"}
    assert _notices(caplog) == [token_proxy.TOKENS_DISABLED_NOTICE, token_proxy.TOKENS_ENABLED_NOTICE]
    enabled = [r for r in caplog.records if r.getMessage() == token_proxy.TOKENS_ENABLED_NOTICE]
    assert enabled[0].levelno == logging.INFO

    # and gone again: the notice once more
    rx.proxies.enabled = False
    await rx._reconcile_token_pool()
    await rx._reconcile_token_pool()
    assert _notices(caplog) == [
        token_proxy.TOKENS_DISABLED_NOTICE,
        token_proxy.TOKENS_ENABLED_NOTICE,
        token_proxy.TOKENS_DISABLED_NOTICE,
    ]


@pytest.mark.asyncio
async def test_zero_proxies_without_wanted_tokens_says_nothing(make_rx, caplog):
    caplog.set_level(logging.INFO, logger="test.server_pool")
    rx = make_rx(
        [
            ("m1", _settings(), [("1", "X", False)]),
            ("m2", _settings(use=False), [("2", "Y", True)]),
        ]
    )
    rx.proxies.enabled = False
    await rx._reconcile_token_pool()
    assert rx.token_pool.ensured == []
    assert rx.token_pool.close_all_calls == 1
    assert _notices(caplog) == []


@pytest.mark.asyncio
async def test_proxy_reload_message_reloads_and_schedules_a_reconcile(make_rx):
    rx = make_rx([("m1", _settings(), [("1", "X", True)])])
    reloads, scheduled = [], []
    rx.proxies.reload = lambda: reloads.append(1)
    rx._resolve_guild_label = lambda typ, data: None
    rx._schedule_token_pool_reconcile = lambda *a, **k: scheduled.append(1)
    await rx._on_ws({"type": "reload_proxies", "data": {}})
    assert reloads == [1]
    assert scheduled == [1]


@pytest.mark.asyncio
async def test_stored_keep_online_zero_keeps_tokens_online(make_rx):
    rx = make_rx(
        [
            ("m1", _settings(), [("1", "X", True)]),
            ("m2", _settings(), [("2", "Y", True)]),
        ]
    )
    await rx._reconcile_token_pool()
    assert rx._pool_ids == {"1", "2"}

    # An old database may still hold Keep online = 0: it no longer closes anything.
    for i, (mid, settings, toks) in enumerate(rx.db.mappings):
        rx.db.mappings[i] = (mid, dict(settings, USER_TOKEN_KEEP_ONLINE=0), toks)
    rx.token_pool.ensured.clear()
    await rx._reconcile_token_pool()

    assert rx.token_pool.closed == []
    assert rx.token_pool.close_all_calls == 0
    assert rx.token_pool.ensured == [("1", "X", 2, "random"), ("2", "Y", 2, "random")]
    assert rx._pool_ids == {"1", "2"}


@pytest.mark.asyncio
async def test_user_tokens_off_everywhere_closes_all(make_rx):
    rx = make_rx(
        [
            ("m1", _settings(), [("1", "X", True)]),
            ("m2", _settings(), [("2", "Y", True)]),
        ]
    )
    await rx._reconcile_token_pool()
    assert rx._pool_ids == {"1", "2"}

    for i, (mid, settings, toks) in enumerate(rx.db.mappings):
        rx.db.mappings[i] = (mid, dict(settings, USE_USER_TOKENS=False), toks)
    rx.token_pool.ensured.clear()
    await rx._reconcile_token_pool()

    assert sorted(rx.token_pool.closed) == ["1", "2"]
    assert rx.token_pool.close_all_calls == 1
    assert rx.token_pool.ensured == []
    assert rx._pool_ids == set()


@pytest.mark.asyncio
async def test_disabled_or_deleted_token_is_closed(make_rx):
    rx = make_rx([("m1", _settings(), [("1", "X", True), ("2", "Y", True), ("3", "Z", True)])])
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
    rx = make_rx([("m1", _settings(), [("1", "X", True), ("2", "Y", True)])])
    await rx._reconcile_token_pool()

    rx.user_token_sender._dead_tokens.add("Y")
    rx.token_pool.ensured.clear()
    await rx._reconcile_token_pool()

    assert rx.token_pool.closed == ["2"]
    assert rx.token_pool.ensured == [("1", "X", 1, "random")]


@pytest.mark.asyncio
async def test_overlapping_reconciles_coalesce_into_one_rerun(make_rx):
    rx = make_rx([("m1", _settings(), [("1", "X", True)])])
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
    rx = make_rx([("m1", _settings(), [("1", "X", True)])])
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


# sticky per author is the only strategy


async def _author_firsts(s, tried, authors):
    firsts = []
    for author in authors:
        tried.clear()
        await _send(s, author_id=author)
        firsts.append(tried[0])
    return firsts


@pytest.mark.asyncio
async def test_sticky_assignment_is_used_with_no_strategy_setting(srv, monkeypatch):
    from copycord.common.config import Config

    assert "USER_TOKEN_STRATEGY" not in Config.default_mapping_settings(None)
    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    s = _sender(SenderDB(["a", "b", "c"]), SenderFakePool(online={"a", "b", "c"}))

    firsts = await _author_firsts(s, tried, ["u1", "u2", "u3", "u1", "u2", "u3", "u1"])
    # Each author keeps one account; no rotation between their messages.
    assert firsts == ["ta", "tb", "tc", "ta", "tb", "tc", "ta"]
    assert s._sticky_author_token == {("m", "u1"): "a", ("m", "u2"): "b", ("m", "u3"): "c"}
    # The identity manager follows the sticky options alone.
    assert srv.ServerReceiver._identity_active({"USER_TOKEN_STICKY_NICKNAME": True}) is True
    assert srv.ServerReceiver._identity_active({}) is False


@pytest.mark.asyncio
async def test_stale_round_robin_value_changes_nothing(srv, monkeypatch):
    from copycord.common.common_helpers import resolve_mapping_settings
    from copycord.common.config import Config

    defaults = Config.default_mapping_settings(None)
    cfg = SimpleNamespace(default_mapping_settings=lambda: dict(defaults))
    stored = '{"USE_USER_TOKENS": true, "USER_TOKEN_STRATEGY": "round_robin", "USER_TOKEN_STICKY_ROLES": true}'
    db = SimpleNamespace(get_mapping_by_id=lambda mid: {"settings": stored, "status": "active"})
    eff = resolve_mapping_settings(db, cfg, mapping_id=1)
    assert eff["USER_TOKEN_STICKY_ROLES"] is True

    for sticky in ({}, {"USER_TOKEN_STICKY_NICKNAME": True}, {"USER_TOKEN_STICKY_ROLES": True}, {"USER_TOKEN_STICKY_AVATAR": True}):
        for stale in ("round_robin", "sticky_author", "junk"):
            assert srv.ServerReceiver._identity_active(
                dict(sticky, USER_TOKEN_STRATEGY=stale)
            ) == srv.ServerReceiver._identity_active(sticky)
    assert srv.ServerReceiver._identity_active(eff) is True

    tried = _record_sends(monkeypatch, status=SEND_TRANSIENT)
    pool = SenderFakePool(online={"a", "b", "c"})
    authors = ["u1", "u2", "u1", "u2", "u1"]
    plain = await _author_firsts(_sender(SenderDB(["a", "b", "c"]), pool), tried, authors)
    assert plain == ["ta", "tb", "ta", "tb", "ta"]
    # Author-less sends always start from the same first token.
    s = _sender(SenderDB(["a", "b", "c"]), pool)
    assert await _author_firsts(s, tried, [None, None, None]) == ["ta", "ta", "ta"]
