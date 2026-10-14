"""VerifyController delete results carry string ids.

Discord snowflakes run past 2**53, so an int id loses digits in the browser's
JSON.parse and the admin SPA cannot match the result to the row it deleted.
"""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from copycord.server.helpers import VerifyController

GUILD = 1442034337590677588
GONE = 1521898853337403511
KEPT = 1521898853337403512


class FakeChannel:
    def __init__(self, cid, name):
        self.id = cid
        self.name = name
        self.deleted = False

    async def delete(self):
        self.deleted = True


class FakeBus:
    def __init__(self):
        self.sent = []

    async def publish(self, kind, payload):
        self.sent.append({"kind": kind, "role": "server", "payload": payload})


def _controller():
    chans = {GONE: FakeChannel(GONE, "old-news"), KEPT: FakeChannel(KEPT, "12367686")}
    guild = SimpleNamespace(
        id=GUILD,
        categories=[],
        channels=list(chans.values()),
        get_channel=chans.get,
    )
    bus = FakeBus()
    db = MagicMock()
    db.get_mapping_by_id.return_value = None
    vc = VerifyController(
        bus=bus,
        admin_base_url="http://admin",
        bot=SimpleNamespace(get_guild=lambda gid: guild if gid == GUILD else None),
        guild_id=GUILD,
        db=db,
        ratelimit=MagicMock(),
        get_protected_channel_ids=lambda g: [KEPT],
        action_type_delete_channel=None,
    )
    return vc, bus, chans


def _results(bus):
    frame = json.loads(json.dumps(bus.sent[-1]))
    assert frame["kind"] == "verify"
    assert frame["payload"]["type"] == "deleted"
    return frame["payload"]["results"]


def test_delete_all_results_carry_string_ids():
    vc, bus, chans = _controller()
    asyncio.run(
        vc._handle({"action": "delete_all", "ids": [str(GONE), str(KEPT)]}, req_id="r1")
    )
    results = _results(bus)
    assert results == [
        {"id": str(GONE), "kind": "channel", "name": "old-news", "deleted": True},
        {
            "id": str(KEPT),
            "kind": "channel",
            "name": "12367686",
            "deleted": False,
            "reason": "protected",
        },
    ]
    assert chans[GONE].deleted and not chans[KEPT].deleted


def test_delete_one_and_not_found_carry_string_ids():
    vc, bus, _ = _controller()
    asyncio.run(vc._handle({"action": "delete_one", "kind": "channel", "id": str(GONE)}, req_id="r2"))
    assert [r["id"] for r in _results(bus)] == [str(GONE)]
    asyncio.run(vc._handle({"action": "delete_one", "kind": "channel", "id": "99"}, req_id="r3"))
    assert _results(bus) == [
        {"id": "99", "kind": "channel", "name": "#99", "deleted": False, "reason": "not_found"}
    ]
