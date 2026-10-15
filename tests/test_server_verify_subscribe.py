"""Bot start subscribes to the admin bus for verify requests once, through VerifyController.

The receiver used to start its own listen loop as well, which called a _admin_base() that never
existed and raised AttributeError on every start.
"""
import asyncio
import importlib
import logging
from types import SimpleNamespace

import pytest

GUILD = 1442034337590677588


@pytest.fixture
def ServerReceiver():
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


class FakeBus:
    def __init__(self):
        self.subscribed = []
        self.sent = []

    async def subscribe(self, url, handler):
        self.subscribed.append((url, handler))

    async def publish(self, kind, payload):
        self.sent.append((kind, payload))


class FakeDB:
    def __init__(self, clone_ids):
        self.clone_ids = clone_ids

    def get_all_clone_guild_ids(self):
        return self.clone_ids

    def get_mapping_by_id(self, mapping_id):
        return None


def _rx(ServerReceiver, guilds, clone_ids):
    rx = ServerReceiver.__new__(ServerReceiver)
    rx.bus = FakeBus()
    rx.config = SimpleNamespace(ADMIN_WS_URL="ws://admin:8080/")
    rx.bot = SimpleNamespace(guilds=guilds, get_guild=lambda gid: None)
    rx.db = FakeDB(clone_ids)
    rx.ratelimit = None
    return rx


@pytest.mark.parametrize(
    "guilds, clone_ids, guild_id",
    [([SimpleNamespace(id=GUILD)], [GUILD], GUILD), ([], [], None)],
    ids=["mapped guild", "no guild yet"],
)
def test_start_subscribes_once_without_error(ServerReceiver, guilds, clone_ids, guild_id):
    assert not hasattr(ServerReceiver, "_verify_listen_loop")

    async def go():
        rx = _rx(ServerReceiver, guilds, clone_ids)
        rx._start_verify()
        rx._start_verify()
        task = rx.verify._task
        await asyncio.wait_for(task, 1.0)
        assert task.exception() is None
        return rx

    rx = asyncio.run(go())

    assert [url for url, _ in rx.bus.subscribed] == ["ws://admin:8080"]
    assert rx.verify.guild_id == guild_id


def test_verify_request_without_a_guild_answers_instead_of_raising(ServerReceiver):
    async def go():
        rx = _rx(ServerReceiver, [], [])
        rx._start_verify()
        await asyncio.wait_for(rx.verify._task, 1.0)
        (_, handler), = rx.bus.subscribed
        await handler({"kind": "verify", "role": "ui", "payload": {"action": "list", "req_id": "r1"}})
        return rx

    rx = asyncio.run(go())

    assert [(kind, p["type"], p["req_id"]) for kind, p in rx.bus.sent] == [("verify", "orphans", "r1")]
    assert rx.bus.sent[0][1]["channels"] == [] and rx.bus.sent[0][1]["categories"] == []
