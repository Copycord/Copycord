"""Download avatars task type (lane L68): a fake client agent answering the member frames,
a fake CDN, the zip it writes and the expiring download route."""
import asyncio
import csv
import io
import json
import zipfile
from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin.security import CSRF_HEADER
from copycord.admin.tasks import avatars, registry
from copycord.admin.tasks.service import MemoryRunRepo, TaskService
from copycord.client import avatar_members

GID = "1234567890123456789"
NOW = 1_800_000_000.0
ORIGIN = "http://testserver"


def _member(uid, name, avatar=None, disc="0", global_name=None):
    return {"id": str(uid), "username": name, "global_name": global_name, "discriminator": disc, "avatar": avatar}


MEMBERS = [
    _member(111111111111111111, "alice", "abc123", global_name="Alice"),
    _member(222222222222222222, "bob", "a_anim9"),
    _member(333333333333333333, "carol"),
    _member(444444444444444444, "dave"),
]


class FakeClient:
    """The client agent: takes avatars_members, streams its frames, serves the pages."""

    def __init__(self, members=MEMBERS, member_count=None, partial=False, stream=True, accept=True,
                 duplicate=False):
        self.members = list(members)
        if duplicate:
            self.members.append(dict(self.members[0]))
        self.member_count = member_count
        self.partial = partial
        self.stream = stream
        self.accept = accept
        self.frames = []
        self.job_id = None

    def status(self):
        return {"job_id": self.job_id, "found": len(self.members), "member_count": self.member_count,
                "partial": self.partial, "error": None}

    async def send(self, frame):
        self.frames.append(frame)
        typ, data = frame["type"], frame["data"]
        if typ == "avatars_members":
            if not self.accept:
                return {"ok": False, "error": "The client account is not in this server"}
            self.job_id = data["job_id"]
            if self.stream:
                asyncio.ensure_future(self._stream())
            return {"ok": True}
        if typ == "avatars_members_page":
            off, lim = data["offset"], data["limit"]
            return {**self.status(), "ok": True, "state": "done", "members": self.members[off:off + lim] if lim else []}
        return {"ok": True}

    async def _stream(self):
        await avatars.dispatch({"type": "avatars_members_progress",
                                "data": {"job_id": self.job_id, "found": 2, "member_count": self.member_count}})
        await avatars.dispatch({"type": "avatars_members_end",
                                "data": {**self.status(), "ok": True}})

    def of(self, typ):
        return [f["data"] for f in self.frames if f["type"] == typ]


class FakeCdn:
    def __init__(self, missing=(), on_get=None):
        self.urls = []
        self.missing = set(missing)
        self.on_get = on_get

    async def fetch(self, url):
        self.urls.append(url)
        if self.on_get:
            await self.on_get(len(self.urls))
        if any(m in url for m in self.missing):
            return 404, b"", {}
        return 200, f"img:{url}".encode(), {}


@pytest.fixture()
def wire(tmp_path, monkeypatch):
    def _wire(client=None, cdn=None, db=None, user_guilds=None):
        client = client or FakeClient()
        cdn = cdn or FakeCdn()
        monkeypatch.setattr(avatars, "_wiring", avatars._Wiring(
            db=db, send=client.send, user_guilds=user_guilds, data_dir=tmp_path,
            fetch=cdn.fetch, clock=lambda: NOW))
        return client, cdn

    monkeypatch.setattr(avatars, "DOWNLOAD_INTERVAL", 0)
    monkeypatch.setattr(avatars, "STATUS_POLL", 0.05)
    avatars._jobs.clear()
    avatars._guild_names.clear()
    yield _wire
    avatars._jobs.clear()
    avatars._guild_names.clear()


@pytest.fixture()
def svc():
    return TaskService(MemoryRunRepo(), cancel_grace=2.0)


async def _until(cond, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


async def _settled(svc, run_id):
    await _until(lambda: svc.get(run_id).status in ("success", "failed", "cancelled")
                 and run_id not in svc._active)
    return svc.get(run_id)


def _steps(run):
    return {s["key"]: s for s in run.steps}


def _zip(tmp_path, report):
    token = report["download"]["url"].rsplit("/", 1)[1]
    return token, zipfile.ZipFile(tmp_path / avatars.DIRNAME / f"{token}.zip")


def test_registered_with_bots_required():
    t = registry.get("download_avatars")
    assert t.title == "Download avatars" and t.requires_bots is True and t.concurrency == 1
    assert set(t.inputs_schema()["properties"]) == {"guild_id", "guild_name", "expires_hours"}


@pytest.mark.parametrize("raw, field", [
    ({}, "guild_id"),
    ({"guild_id": "abc"}, "guild_id"),
    ({"guild_id": GID, "expires_hours": 0}, "expires_hours"),
    ({"guild_id": GID, "expires_hours": 169}, "expires_hours"),
    ({"guild_id": GID, "expires_hours": 1.5}, "expires_hours"),
])
@pytest.mark.asyncio
async def test_bad_inputs_are_refused(svc, wire, raw, field):
    wire()
    with pytest.raises(ValueError) as exc:
        await svc.start("download_avatars", raw)
    assert field in exc.value.args[0]


def test_avatar_url_original_format_and_defaults():
    assert avatars.avatar_url(MEMBERS[0]) == (
        f"{avatars.CDN}/avatars/111111111111111111/abc123.png?size=1024", "png")
    assert avatars.avatar_url(MEMBERS[1]) == (
        f"{avatars.CDN}/avatars/222222222222222222/a_anim9.gif?size=1024", "gif")
    uid = 333333333333333333
    assert avatars.avatar_url(MEMBERS[2]) == (f"{avatars.CDN}/embed/avatars/{(uid >> 22) % 6}.png", "png")
    legacy = _member(5, "old", disc="1337")
    assert avatars.avatar_url(legacy)[0] == f"{avatars.CDN}/embed/avatars/{1337 % 5}.png"


@pytest.mark.asyncio
async def test_run_lists_members_downloads_and_zips(svc, wire, tmp_path, monkeypatch):
    monkeypatch.setattr(avatars, "PAGE_SIZE", 2)
    client, cdn = wire(FakeClient(member_count=4, duplicate=True))
    avatars._guild_names[GID] = "Host Server"
    run = await svc.start("download_avatars", {"guild_id": GID, "expires_hours": 6})
    assert run.title == "Download avatars from Host Server"
    assert run.context == {"label": "Host Server", "guild_id": GID}
    run = await _settled(svc, run.id)
    assert run.status == "success", run.error

    start = client.of("avatars_members")
    assert start == [{"job_id": f"{run.id}:members", "guild_id": GID}]
    # Pages of two until a short page; the duplicate of alice is dropped by user id.
    assert [p["offset"] for p in client.of("avatars_members_page") if p["limit"]] == [0, 2, 4]
    assert client.of("avatars_members_cancel") == [{"job_id": f"{run.id}:members"}]

    steps = _steps(run)
    assert [s["title"] for s in run.steps] == ["Members", "Avatars"]
    assert steps["members"]["state"] == "success" and steps["members"]["note"] == "4 found"
    assert steps["avatars"]["state"] == "success" and steps["avatars"]["note"] == "4 downloaded"
    assert steps["avatars"]["done"] == 4 and steps["avatars"]["total"] == 4
    assert run.summary == {"members": 4, "downloaded": 4, "failed": 0, "skipped": 0}

    # carol and dave share no default avatar unless their ids say so; each default is fetched once.
    defaults = {avatars.avatar_url(m)[0] for m in MEMBERS[2:]}
    assert len(cdn.urls) == 2 + len(defaults)

    report = run.report
    assert report["members"] == {"found": 4, "member_count": 4, "partial": False}
    dl = report["download"]
    assert dl["url"].startswith("/api/tasks/avatars/") and dl["files"] == 4
    assert dl["filename"] == f"avatars-Host_Server-{run.number}.zip"
    assert dl["expires_at"] == avatars.iso(NOW + 6 * 3600)
    token, zf = _zip(tmp_path, report)
    names = sorted(zf.namelist())
    assert names == sorted(["alice_111111111111111111.png", "bob_222222222222222222.gif",
                            "carol_333333333333333333.png", "dave_444444444444444444.png", "members.csv"])
    assert zf.read("bob_222222222222222222.gif") == f"img:{avatars.avatar_url(MEMBERS[1])[0]}".encode()
    rows = list(csv.reader(io.StringIO(zf.read("members.csv").decode())))
    assert rows[0] == ["user_id", "username", "global_name", "file", "status"]
    assert rows[1] == ["111111111111111111", "alice", "Alice", "alice_111111111111111111.png", "downloaded"]
    assert rows[3][4] == "default avatar"
    meta = json.loads((tmp_path / avatars.DIRNAME / f"{token}.json").read_text())
    assert meta["expires_at"] == NOW + 6 * 3600 and meta["run_id"] == run.id
    assert not list((tmp_path / avatars.DIRNAME).glob("*.part"))


@pytest.mark.asyncio
async def test_progress_is_live_per_file(svc, wire):
    seen = []
    holder = {}

    async def on_get(n):
        steps = _steps(holder["svc"].get(holder["id"]))
        seen.append((steps["avatars"]["done"], steps["avatars"]["total"], steps["avatars"]["label"]))

    wire(cdn=FakeCdn(on_get=on_get))
    holder["svc"] = svc
    run = await svc.start("download_avatars", {"guild_id": GID})
    holder["id"] = run.id
    run = await _settled(svc, run.id)
    assert run.status == "success"
    # Before each fetch the step shows the files so far and the last name.
    assert seen[0] == (0, 4, None)
    assert seen[1] == (1, 4, "alice")
    assert seen[2] == (2, 4, "bob")


@pytest.mark.asyncio
async def test_partial_member_list_is_exported_and_said(svc, wire):
    wire(FakeClient(member_count=10, partial=True))
    run = await _settled(svc, (await svc.start("download_avatars", {"guild_id": GID})).id)
    assert run.status == "success"
    assert _steps(run)["members"]["note"] == "4 of 10 visible"
    assert run.report["members"] == {"found": 4, "member_count": 10, "partial": True}
    assert any("Only part of the member list" in ln["msg"] for ln in run.log_lines)


@pytest.mark.asyncio
async def test_a_failed_avatar_is_counted_and_named(svc, wire, tmp_path):
    wire(cdn=FakeCdn(missing=("abc123",)))
    run = await _settled(svc, (await svc.start("download_avatars", {"guild_id": GID})).id)
    assert run.status == "success"
    assert run.summary == {"members": 4, "downloaded": 3, "failed": 1, "skipped": 0}
    assert run.report["failures"] == [{"user_id": "111111111111111111", "username": "alice",
                                       "error": "Not found on Discord"}]
    assert _steps(run)["avatars"]["note"] == "3 downloaded, 1 failed"
    _, zf = _zip(tmp_path, run.report)
    assert "alice_111111111111111111.png" not in zf.namelist()


@pytest.mark.asyncio
async def test_rate_limit_waits_and_retries(svc, wire, monkeypatch):
    waits = []
    calls = {"n": 0}

    async def fetch(url):
        calls["n"] += 1
        if calls["n"] == 1:
            return 429, b"", {"Retry-After": "0.01"}
        return 200, b"x", {}

    async def fake_sleep(ctx, seconds):
        waits.append(seconds)

    monkeypatch.setattr(avatars, "_sleep", fake_sleep)
    client, _ = wire(FakeClient(members=MEMBERS[:1]))
    avatars._wiring.fetch = fetch
    run = await _settled(svc, (await svc.start("download_avatars", {"guild_id": GID})).id)
    assert run.status == "success" and run.summary["downloaded"] == 1
    assert waits == [0.01]


@pytest.mark.asyncio
async def test_cancel_between_files_keeps_what_was_downloaded(svc, wire, tmp_path):
    holder = {}

    async def on_get(n):
        if n == 2:
            await holder["svc"].cancel(holder["id"])

    wire(cdn=FakeCdn(on_get=on_get))
    holder["svc"] = svc
    run = await svc.start("download_avatars", {"guild_id": GID})
    holder["id"] = run.id
    run = await _settled(svc, run.id)
    assert run.status == "cancelled"
    assert run.summary == {"members": 4, "downloaded": 2, "failed": 0, "skipped": 2}
    assert _steps(run)["avatars"]["state"] == "cancelled"
    assert _steps(run)["avatars"]["note"] == "Cancelled after 2 of 4"
    _, zf = _zip(tmp_path, run.report)
    assert len([n for n in zf.namelist() if n != "members.csv"]) == 2


@pytest.mark.asyncio
async def test_a_lost_end_frame_is_found_by_asking(svc, wire):
    client, _ = wire(FakeClient(stream=False))
    run = await _settled(svc, (await svc.start("download_avatars", {"guild_id": GID})).id)
    assert run.status == "success" and run.summary["downloaded"] == 4
    assert client.of("avatars_members_page")[0] == {"job_id": f"{run.id}:members", "offset": 0, "limit": 0}


@pytest.mark.asyncio
async def test_client_refusal_fails_the_run(svc, wire):
    wire(FakeClient(accept=False))
    run = await _settled(svc, (await svc.start("download_avatars", {"guild_id": GID})).id)
    assert run.status == "failed"
    assert run.error == "The client account is not in this server"
    steps = _steps(run)
    assert steps["members"]["state"] == "failed" and steps["avatars"]["state"] == "skipped"


@pytest.mark.asyncio
async def test_no_members_fails_without_a_zip(svc, wire, tmp_path):
    wire(FakeClient(members=[]))
    run = await _settled(svc, (await svc.start("download_avatars", {"guild_id": GID})).id)
    assert run.status == "failed" and run.report["download"] is None
    assert not (tmp_path / avatars.DIRNAME).exists() or not list((tmp_path / avatars.DIRNAME).iterdir())


@pytest.mark.asyncio
async def test_cancel_while_listing(svc, wire):
    client, _ = wire(FakeClient(stream=False))
    client.send_page = client.send

    async def running_page(frame):
        if frame["type"] == "avatars_members_page":
            client.frames.append(frame)
            return {**client.status(), "ok": True, "state": "running", "members": []}
        return await client.send_page(frame)

    avatars._wiring.send = running_page
    run = await svc.start("download_avatars", {"guild_id": GID})
    await _until(lambda: client.job_id is not None)
    await svc.cancel(run.id)
    run = await _settled(svc, run.id)
    assert run.status == "cancelled"
    assert client.of("avatars_members_cancel") == [{"job_id": f"{run.id}:members"}]
    assert run.report["download"] is None


@pytest.mark.asyncio
async def test_bus_listener_routes_client_frames(wire):
    wire()
    q = asyncio.Queue()
    bus = SimpleNamespace(subscribe=lambda: q, unsubscribe=lambda _q: None)
    step = SimpleNamespace(calls=[])

    async def progress(done, total=None, label=None):
        step.calls.append((done, total, label))

    step.progress = progress
    job = avatars._MemberJob("r:members", step)
    avatars._jobs["r:members"] = job
    task = asyncio.ensure_future(avatars._listen(bus))
    q.put_nowait(json.dumps({"kind": "server", "payload": {"type": "avatars_members_end", "data": {"job_id": "r:members"}}}))
    q.put_nowait(json.dumps({"kind": "client", "payload": {"type": "avatars_members_progress",
                                                            "data": {"job_id": "r:members", "found": 7, "member_count": 9}}}))
    q.put_nowait(json.dumps({"kind": "client", "payload": {"type": "avatars_members_end",
                                                            "data": {"job_id": "r:members", "ok": True, "found": 9}}}))
    await asyncio.wait_for(job.done.wait(), 2)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert step.calls[0] == (7, 9, "7 of 9 found") and job.end["found"] == 9


# options

class _Db:
    def __init__(self, token):
        self.token = token

    def get_config(self, key, default=""):
        return self.token if key == "CLIENT_TOKEN" else default


@pytest.mark.asyncio
async def test_options_lists_the_client_guilds_by_name(wire):
    asked = []

    async def user_guilds(token):
        asked.append(token)
        return [{"id": "2", "name": "zeta"}, {"id": "1", "name": "Alpha"}, {"id": "x", "name": "bad"}]

    wire(db=_Db(" tok "), user_guilds=user_guilds)
    assert await avatars.options({}) == {"guilds": [{"id": "1", "name": "Alpha"}, {"id": "2", "name": "zeta"}]}
    assert asked == ["tok"]
    assert avatars.describe(avatars.AvatarInputs(guild_id="2"))[0] == "Download avatars from zeta"


@pytest.mark.asyncio
async def test_options_errors(wire):
    class RateLimited(Exception):
        detail = "discord-rate-limited"

    async def limited(token):
        raise RateLimited()

    wire(db=_Db(""), user_guilds=limited)
    assert (await avatars.options({}))["error"] == "Set the client account token first."
    avatars._wiring.db = _Db("tok")
    res = await avatars.options({})
    assert res["guilds"] == [] and "rate limiting" in res["error"]


# files and the download route

def _seed(root, token, expires_at, body=b"PK"):
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{token}.zip").write_bytes(body)
    (root / f"{token}.json").write_text(json.dumps({"expires_at": expires_at, "filename": "avatars-x-1.zip"}))


TOKEN = "A" * 43
OTHER = "B" * 43


def test_sweep_deletes_expired_orphans_and_partials(tmp_path, monkeypatch):
    monkeypatch.setattr(avatars, "_wiring", avatars._Wiring(clock=lambda: NOW))
    root = tmp_path / avatars.DIRNAME
    _seed(root, TOKEN, NOW + 60)
    _seed(root, OTHER, NOW - 1)
    (root / ("C" * 43 + ".zip")).write_bytes(b"orphan")
    (root / ("D" * 43 + ".zip.part")).write_bytes(b"half")
    avatars.configure(data_dir=tmp_path)
    assert sorted(p.name for p in root.iterdir()) == [f"{TOKEN}.json", f"{TOKEN}.zip"]


@pytest.fixture()
def http(tmp_path, monkeypatch):
    clock = {"now": NOW}
    monkeypatch.setattr(avatars, "_wiring", avatars._Wiring(data_dir=tmp_path, clock=lambda: clock["now"]))
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    with TestClient(app_module.app) as tc:
        tc.headers.update({"origin": ORIGIN, CSRF_HEADER: "1",
                           "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
        yield tc, clock


def test_route_serves_a_valid_link(http, tmp_path):
    tc, _ = http
    _seed(tmp_path / avatars.DIRNAME, TOKEN, NOW + 60, body=b"PKzip")
    resp = tc.get(f"/api/tasks/avatars/{TOKEN}")
    assert resp.status_code == 200 and resp.content == b"PKzip"
    assert resp.headers["content-type"] == "application/zip"
    assert 'filename="avatars-x-1.zip"' in resp.headers["content-disposition"]


def test_route_wrong_or_malformed_token_is_404(http, tmp_path):
    tc, _ = http
    _seed(tmp_path / avatars.DIRNAME, TOKEN, NOW + 60)
    assert tc.get(f"/api/tasks/avatars/{OTHER}").status_code == 404
    assert tc.get("/api/tasks/avatars/short").status_code == 404
    assert tc.get("/api/tasks/avatars/" + "A" * 42 + "%21").status_code == 404
    assert (tmp_path / avatars.DIRNAME / f"{TOKEN}.zip").exists()


def test_route_expired_link_is_410_and_deleted(http, tmp_path):
    tc, clock = http
    root = tmp_path / avatars.DIRNAME
    _seed(root, TOKEN, NOW + 60)
    assert tc.get(f"/api/tasks/avatars/{TOKEN}").status_code == 200
    clock["now"] = NOW + 61
    resp = tc.get(f"/api/tasks/avatars/{TOKEN}")
    assert resp.status_code == 410 and resp.json() == {"detail": "This download link expired"}
    assert not (root / f"{TOKEN}.zip").exists() and not (root / f"{TOKEN}.json").exists()
    assert tc.get(f"/api/tasks/avatars/{TOKEN}").status_code == 404


def test_route_needs_a_session(http, tmp_path):
    tc, _ = http
    _seed(tmp_path / avatars.DIRNAME, TOKEN, NOW + 60)
    tc.headers.pop("cookie")
    assert tc.get(f"/api/tasks/avatars/{TOKEN}", follow_redirects=False).status_code == 401


# the client half

def _m(uid, name, avatar=None):
    return SimpleNamespace(id=uid, name=name, global_name=None, discriminator="0",
                           avatar=SimpleNamespace(key=avatar) if avatar else None)


class FakeBot:
    def __init__(self, cached, searchable, member_count):
        self.guild = SimpleNamespace(members=cached, member_count=member_count)
        self.searchable = searchable
        self.gateway = object()
        self.queries = []

    def get_guild(self, gid):
        return self.guild if gid == int(GID) else None

    async def fetch_members(self, gid, *, query="", limit=0, timeout=0):
        self.queries.append(query)
        return [m for m in self.searchable if m.name.startswith(query)][:limit]


async def _listing(bot):
    frames = []

    async def publish(frame):
        frames.append(frame)

    am = avatar_members.AvatarMembers(lambda: bot, publish, interval=0)
    assert (await am.handle("avatars_members", {"job_id": "j", "guild_id": GID})) == {"ok": True}
    await asyncio.wait_for(am._jobs["j"].task, 3)
    return am, frames


@pytest.mark.asyncio
async def test_client_lists_cached_and_searched_members(monkeypatch):
    monkeypatch.setattr(avatar_members, "QUERY_LIMIT", 2)
    cached = [_m(1, "alice", "h1")]
    searchable = [_m(1, "alice", "h1"), _m(2, "aaron"), _m(3, "abby"), _m(4, "bob", "a_x")]
    bot = FakeBot(cached, searchable, member_count=4)
    am, frames = await _listing(bot)
    # "a" came back full (two), so "aa", "ab" ... were searched after the single letters; the listing
    # stops once all 4 are found.
    assert bot.queries[:2] == ["a", "b"] and bot.queries[-1] == "ab"
    end = frames[-1]
    assert end["type"] == "avatars_members_end"
    assert end["data"] == {"job_id": "j", "found": 4, "member_count": 4, "partial": False, "error": None, "ok": True}
    page = await am.handle("avatars_members_page", {"job_id": "j", "offset": 1, "limit": 2})
    assert page["state"] == "done" and page["found"] == 4
    # In the order found: alice (cached), aaron ("a"), bob ("b"), abby ("ab").
    assert page["members"] == [
        {"id": "2", "username": "aaron", "global_name": None, "discriminator": "0", "avatar": None},
        {"id": "4", "username": "bob", "global_name": None, "discriminator": "0", "avatar": "a_x"},
    ]
    assert (await am.handle("avatars_members_page", {"job_id": "j", "offset": 0, "limit": 10}))["members"][0]["avatar"] == "h1"
    assert await am.handle("avatars_members_cancel", {"job_id": "j"}) == {"ok": True}
    assert (await am.handle("avatars_members_page", {"job_id": "j"}))["state"] == "missing"


@pytest.mark.asyncio
async def test_client_marks_a_short_list_partial():
    bot = FakeBot([_m(1, "alice")], [_m(1, "alice"), _m(2, "zed")], member_count=500)
    _, frames = await _listing(bot)
    assert len(bot.queries) == len(avatar_members.PREFIXES)
    assert frames[-1]["data"]["partial"] is True and frames[-1]["data"]["found"] == 2
    assert frames[0]["type"] == "avatars_members_progress" and frames[0]["data"]["found"] == 1


@pytest.mark.asyncio
async def test_client_not_in_the_server_fails_the_listing():
    bot = FakeBot([], [], member_count=1)
    bot.get_guild = lambda gid: None
    _, frames = await _listing(bot)
    assert frames[-1]["data"]["ok"] is False
    assert frames[-1]["data"]["error"] == "The client account is not in this server"
