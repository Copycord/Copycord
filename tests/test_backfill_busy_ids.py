"""backfill_busy carries the channel id as a string.

Discord snowflakes run past 2**53, so an int id loses digits in the browser's
JSON.parse and the admin SPA cannot match the busy notice to the row it holds.
"""
import asyncio
import json

from copycord.server.server import ServerReceiver

CID = 1521898853337403511


class FakeBus:
    def __init__(self):
        self.sent = []

    async def publish(self, kind, payload):
        self.sent.append({"kind": kind, "role": "server", "payload": payload})


def test_backfill_busy_round_trips_string_channel_id():
    rx = ServerReceiver.__new__(ServerReceiver)
    rx._shutting_down = False
    rx._resolve_guild_label = lambda typ, data: None
    rx._task_for_channel = {}
    rx._active_backfills = {CID}
    rx.bus = FakeBus()

    asyncio.run(rx._on_ws({"type": "backfill_started", "data": {"channel_id": str(CID)}}))

    assert len(rx.bus.sent) == 1
    frame = json.loads(json.dumps(rx.bus.sent[0]))
    assert frame["kind"] == "client"
    assert frame["payload"]["type"] == "backfill_busy"
    assert frame["payload"]["data"]["channel_id"] == str(CID)
