"""StandaloneScraper's bio phase: the whole scrape() with the token check, guild info and
gateway stubbed, and a fake Discord in place of the token's curl_cffi session. Nothing
calls Discord."""
import asyncio

import pytest
from curl_cffi.requests.exceptions import RequestException

from copycord.admin import standalone_scraper as ss
from copycord.admin.standalone_scraper import ScraperConfig, StandaloneScraper


class _Resp:
    """A curl_cffi response: status_code, a synchronous json(), headers."""

    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self.body = body
        self.headers = headers or {}

    def json(self):
        if isinstance(self.body, BaseException):
            raise self.body
        return self.body


def ok(bio):
    return _Resp(200, {"user": {"bio": bio}})


def limited(after):
    return _Resp(429, {"retry_after": after})


class FakeDiscord:
    """GET /users/{id}/profile answers per user id, served in turn with the last one
    repeated; records (user id, loop time) per GET. With a gate, answers wait for it."""

    def __init__(self, profiles, gate=None):
        self.profiles = {uid: list(items) for uid, items in profiles.items()}
        self.gate = gate
        self.calls = []
        self.closed = False

    async def get(self, url, **kwargs):
        uid = url.rstrip("/").split("/")[-2]
        self.calls.append((uid, asyncio.get_running_loop().time()))
        items = self.profiles[uid]
        item = items.pop(0) if len(items) > 1 else items[0]
        if self.gate is not None:
            await self.gate.wait()
        if isinstance(item, BaseException):
            raise item
        return item

    def count(self, uid):
        return sum(1 for u, _ in self.calls if u == uid)

    def times(self, uid):
        return [t for u, t in self.calls if u == uid]

    async def close(self):
        self.closed = True


def _scraper(discord, reports, monkeypatch):
    uids = list(discord.profiles)
    config = ScraperConfig(guild_id=1, tokens=["tok"], include_bio=True,
                           bio_progress_callback=lambda done, total, msg="": reports.append((done, total)))
    s = StandaloneScraper(config)

    async def validate(token):
        return True, "user", "1"

    async def guild_info(token, guild_id):
        return len(uids), "Guild"

    async def gateway(**kwargs):
        async with s._members_lock:
            for uid in uids:
                if uid not in s._members:
                    s._members[uid] = {"id": uid, "bio": None}
                    s._queue_bio(kwargs["bio_queue"], uid)

    s._validate_token = validate
    s._get_guild_info = guild_info
    s._run_gateway_session = gateway
    monkeypatch.setattr(ss, "_CurlAsyncSession", lambda **kwargs: discord)
    return s


def _fake_time(scraper):
    """The scraper's waits jump the loop's clock forward instead of sleeping, so every
    timer (a wait_for cap included) sees the skipped time. Returns (skipped, undo). Under
    it a wait_for guard around scrape() would fire too, so the tests await it bare."""
    loop = asyncio.get_running_loop()
    real = loop.time
    skipped = [0.0]
    loop.time = lambda: real() + skipped[0]
    scraper._clock = loop.time

    async def pause(seconds):
        skipped[0] += seconds
        await asyncio.sleep(0)
        return scraper._stop_event.is_set()

    scraper._pause = pause

    def undo():
        del loop.time

    return skipped, undo


def _bios(result):
    return {m["id"]: m["bio"] for m in result.members}


@pytest.mark.asyncio
async def test_bio_wait_has_no_time_cap(monkeypatch):
    # Discord holds each profile back 45 s once: well over the old 60 s cap in all.
    discord = FakeDiscord({uid: [limited(45), ok(f"bio {uid}")] for uid in ("1", "2", "3")})
    reports = []
    s = _scraper(discord, reports, monkeypatch)
    skipped, undo = _fake_time(s)
    try:
        result = await s.scrape()
    finally:
        undo()
    assert skipped[0] > 120
    assert result.success and not result.metadata["cancelled"]
    assert _bios(result) == {"1": "bio 1", "2": "bio 2", "3": "bio 3"}
    assert result.metadata["bios"] == {"queued": 3, "fetched": 3, "skipped": 0}
    assert discord.closed


@pytest.mark.asyncio
async def test_progress_reports_bios_done_of_queued(monkeypatch):
    # Discord answers once the bio phase has begun (the worker runs from the start of the
    # scrape), so the first report is none of the three.
    gate = asyncio.Event()
    discord = FakeDiscord({uid: [limited(5), ok("x")] for uid in ("1", "2", "3")}, gate)
    reports = []
    s = _scraper(discord, reports, monkeypatch)

    def report(done, total, msg=""):
        reports.append((done, total))
        gate.set()

    s.config.bio_progress_callback = report
    _skipped, undo = _fake_time(s)
    try:
        await s.scrape()
    finally:
        undo()
    assert reports[0] == (0, 3) and reports[-1] == (3, 3)
    assert all(total == 3 for _, total in reports)
    dones = [done for done, _ in reports]
    assert dones == sorted(dones)
    assert any(0 < d < 3 for d in dones)


@pytest.mark.asyncio
async def test_429_waits_out_retry_after_however_often(monkeypatch):
    discord = FakeDiscord({
        "1": [limited(7.5), ok("one")],
        # No JSON body: the Retry-After header says when.
        "2": [_Resp(429, ValueError("no body"), {"Retry-After": "3"}), ok("two")],
        # Many more 429s than BIO_ATTEMPTS: still not skipped.
        "3": [limited(2)] * (ss.BIO_ATTEMPTS * 3) + [ok("three")],
    })
    s = _scraper(discord, [], monkeypatch)
    _skipped, undo = _fake_time(s)
    try:
        result = await s.scrape()
    finally:
        undo()
    t1, t2 = discord.times("1"), discord.times("2")
    assert len(t1) == 2 and t1[1] - t1[0] >= 7.5 + 0.25
    assert len(t2) == 2 and t2[1] - t2[0] >= 3 + 0.25
    assert discord.count("3") == ss.BIO_ATTEMPTS * 3 + 1
    assert _bios(result) == {"1": "one", "2": "two", "3": "three"}
    assert result.metadata["bios"] == {"queued": 3, "fetched": 3, "skipped": 0}


@pytest.mark.asyncio
async def test_failing_profiles_are_skipped_after_their_tries(monkeypatch):
    discord = FakeDiscord({
        "ok": [ok("hello")],
        "500": [_Resp(500)],
        "network": [RequestException("proxy refused the connection")],
        "timeout": [asyncio.TimeoutError()],
        "bad json": [_Resp(200, ValueError("not json"))],
        "not a dict": [_Resp(200, ["?"])],
        "404": [_Resp(404)],
        "403": [_Resp(403)],
        "401": [_Resp(401)],
        "flaky": [_Resp(502)] * (ss.BIO_ATTEMPTS - 1) + [ok("late")],
    })
    s = _scraper(discord, [], monkeypatch)
    _skipped, undo = _fake_time(s)
    try:
        result = await s.scrape()
    finally:
        undo()
    assert result.success
    for uid in ("500", "network", "timeout", "bad json", "not a dict"):
        assert discord.count(uid) == ss.BIO_ATTEMPTS, uid
    for uid in ("404", "403", "401", "ok"):
        assert discord.count(uid) == 1, uid
    assert discord.count("flaky") == ss.BIO_ATTEMPTS
    bios = _bios(result)
    assert bios.pop("ok") == "hello" and bios.pop("flaky") == "late"
    # Skipped members are still saved, without a bio.
    assert set(bios.values()) == {None} and len(bios) == 8
    assert result.metadata["bios"] == {"queued": 10, "fetched": 2, "skipped": 8}


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_task", [True, False])
async def test_cancel_during_the_bio_phase_stops_within_a_second(cancel_task, monkeypatch):
    # The worker sits in a 1000 s retry_after (a real wait) when the cancel lands. The task
    # runner both stops the scraper and cancels its task; stop() alone must do too.
    discord = FakeDiscord({"1": [limited(1000)], "2": [ok("never")]})
    reports = []
    s = _scraper(discord, reports, monkeypatch)
    task = asyncio.ensure_future(s.scrape())
    for _ in range(300):
        if discord.calls and reports:
            break
        await asyncio.sleep(0.01)
    assert discord.calls and reports == [(0, 2)]
    await asyncio.sleep(0.05)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    s.stop()
    if cancel_task:
        task.cancel()
    result = await asyncio.wait_for(task, timeout=1.0)
    assert loop.time() - t0 < 1.0
    if cancel_task:
        assert result.error == "Cancelled"
    else:
        assert result.success and result.metadata["cancelled"] is True
    assert len(result.members) == 2
    assert result.metadata["bios"]["fetched"] == 0
    assert discord.closed
    calls = len(discord.calls)
    await asyncio.sleep(0.1)
    assert len(discord.calls) == calls == 1
    assert not [t for t in asyncio.all_tasks()
                if "_bio_worker" in getattr(t.get_coro(), "__qualname__", "")]
