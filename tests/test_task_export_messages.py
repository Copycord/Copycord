"""Export messages task type: a fake client agent that answers export_messages and
sends export_messages_* frames straight to the dispatcher, as the admin bus watcher
does, plus the client's ExportMessagesRunner on a fake bot."""
import asyncio
from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin.security import CSRF_HEADER
from copycord.admin.tasks import export_messages
from copycord.admin.tasks import routes as task_routes
from copycord.admin.tasks.service import MemoryRunRepo, TaskRun, TaskService
from copycord.client.export_runners import ExportMessagesRunner

GID = "223456789012345678"
HOOK = "https://discord.com/api/webhooks/111/secret-token"
FINAL = ("success", "failed", "cancelled")


class FakeDb:
    def __init__(self, rows=None):
        self.rows = rows if rows is not None else [
            {"guild_id": int(GID), "name": "Zeta", "icon_url": "https://cdn/z.png"},
            {"guild_id": 323456789012345678, "name": "alpha", "icon_url": None},
            {"guild_id": None, "name": "broken"},
        ]

    def get_all_guilds(self):
        return list(self.rows)


class FakeClient:
    """The client agent: export_messages starts a script of frames, export_messages_cancel
    cancels it and the script ends with export_messages_done cancelled, as the runner does."""

    def __init__(self):
        self.frames = []
        self.replies = {}
        self.script = None
        self.task = None
        self.release = asyncio.Event()

    async def __call__(self, frame):
        self.frames.append(frame)
        data = frame["data"]
        reply = self.replies.get(frame["type"])
        if reply is not None:
            return reply
        if frame["type"] == "export_messages" and self.script is not None:
            self.task = asyncio.ensure_future(self.script(self, data["job_id"]))
        elif frame["type"] == "export_messages_cancel" and self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        return {"ok": True, "accepted": True}

    def of(self, typ):
        return [f["data"] for f in self.frames if f["type"] == typ]


async def _frame(typ, job_id, **data):
    return await export_messages.dispatch({"type": typ, "data": {"job_id": str(job_id), **data}})


@pytest.fixture()
def client(monkeypatch, tmp_path):
    fake = FakeClient()
    monkeypatch.setattr(export_messages, "_wiring",
                        export_messages._Wiring(send=fake, data_dir=tmp_path, db=FakeDb()))
    monkeypatch.setattr(export_messages, "CANCEL_WAIT", 0.2)
    export_messages._jobs.clear()
    export_messages._guild_names.clear()
    yield fake
    export_messages._jobs.clear()
    export_messages._guild_names.clear()


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
    await _until(lambda: svc.get(run_id).status in FINAL and run_id not in svc._active)
    return svc.get(run_id)


def _steps(svc, run_id):
    return {s["key"]: s for s in svc.get(run_id).steps}


# inputs

@pytest.mark.asyncio
async def test_inputs_are_validated(svc, client):
    for bad, key in (({}, "guild_id"),
                     ({"guild_id": "abc"}, "guild_id"),
                     ({"guild_id": GID, "channel_id": "general"}, "channel_id"),
                     ({"guild_id": GID, "user_id": "bob"}, "user_id"),
                     ({"guild_id": GID, "webhook_url": "discord.com/x"}, "webhook_url"),
                     ({"guild_id": GID, "after_iso": "yesterday"}, "after_iso"),
                     ({"guild_id": GID, "filters": {"min_length": -1}}, "filters.min_length"),
                     ({"guild_id": GID, "filters": {"min_reactions": "lots"}}, "filters.min_reactions")):
        with pytest.raises(ValueError) as exc:
            await svc.start("export_messages", bad)
        assert key in exc.value.args[0], bad
    assert client.frames == []


def test_inputs_mirror_the_dialog_body():
    body = {
        "guild_id": int(GID), "channel_id": " ", "user_id": None, "webhook_url": "",
        "has_attachments": False, "after_iso": "2026-10-01T00:00:00.000Z", "before_iso": None,
        "filters": {"has_content": True, "embeds": False, "attachments": True,
                    "att_types": {"images": True, "videos": False, "audio": False, "other": False},
                    "links": True, "emojis": True, "word_on": True, "word": "  hello ",
                    "replies": True, "bots": False, "system": True, "min_length": "3",
                    "min_reactions": "", "pinned": True, "stickers": True, "mentions": True,
                    "threads": False, "forum_threads": False, "private_threads": False,
                    "download_media": {"images": True, "videos": False, "audio": False, "other": False}},
    }
    inputs = export_messages.ExportMessagesInputs.model_validate(body)
    assert (inputs.guild_id, inputs.channel_id, inputs.user_id, inputs.webhook_url) == (GID, None, None, None)
    assert inputs.after_iso == "2026-10-01T00:00:00.000Z"
    assert inputs.filters.word == "hello" and inputs.filters.min_length == 3
    assert inputs.filters.min_reactions == 0
    assert inputs.filters.download_media.any()
    sent = export_messages._command(inputs, "job")["filters"]
    assert set(sent) == set(body["filters"])
    assert sent["att_types"] == body["filters"]["att_types"]

    plain = export_messages.ExportMessagesInputs.model_validate({"guild_id": GID, "filters": None})
    filters = plain.filters.model_dump()
    assert filters["att_types"] == {"images": True, "videos": True, "audio": True, "other": True}
    assert filters["download_media"] == {"images": False, "videos": False, "audio": False, "other": False}
    assert filters["word_on"] is False and filters["threads"] is True
    assert not plain.filters.download_media.any()


def test_type_is_registered():
    from copycord.admin.tasks import registry
    t = registry.get("export_messages")
    assert t.title == "Export messages" and t.requires_bots is True and t.options is not None
    assert set(t.inputs_schema()["properties"]) == {
        "guild_id", "channel_id", "user_id", "webhook_url", "has_attachments",
        "after_iso", "before_iso", "filters"}
    assert t.inputs_schema()["required"] == ["guild_id"]


@pytest.mark.asyncio
async def test_options_list_the_guilds(client):
    out = await export_messages.options({})
    assert out == {"guilds": [
        {"id": "323456789012345678", "name": "alpha", "icon_url": None},
        {"id": GID, "name": "Zeta", "icon_url": "https://cdn/z.png"},
    ]}


@pytest.mark.asyncio
async def test_options_without_a_db(monkeypatch):
    monkeypatch.setattr(export_messages, "_wiring", export_messages._Wiring())
    assert (await export_messages.options({}))["guilds"] == []


# a run

async def _full_export(fake, job_id):
    await _frame("export_messages_progress", job_id, phase="scan", done=0, total=2,
                 scanned=0, matched=0, channel="#general")
    await _frame("export_messages_progress", job_id, phase="scan", done=1, total=2,
                 scanned=200, matched=40, channel="#memes")
    await _frame("export_messages_progress", job_id, phase="scan", done=2, total=2,
                 scanned=250, matched=50)
    await _frame("export_messages_progress", job_id, phase="json", matched=50,
                 json_path=f"/data/exports/{GID}/20261017-101010/messages.json")
    await _frame("export_messages_progress", job_id, phase="media", done=0, total=4)
    await _frame("export_messages_progress", job_id, phase="media", done=4, total=4,
                 media={"images": 3, "videos": 0, "audio": 0, "other": 0, "errors": 1})
    await _frame("export_messages_progress", job_id, phase="forward", done=0, total=50)
    await _frame("export_messages_progress", job_id, phase="forward", done=10, total=50)
    fake.mid = True
    await fake.release.wait()
    await _frame("export_messages_done", job_id, guild_id=int(GID), scanned=250, matched=50,
                 forwarded=50, json_path=f"/data/exports/{GID}/20261017-101010/messages.json",
                 media_download={"images": 3, "videos": 0, "audio": 0, "other": 0, "errors": 1})


@pytest.mark.asyncio
async def test_run_follows_the_client_and_reports(svc, client):
    client.script = _full_export
    client.mid = False
    inputs = {"guild_id": GID, "channel_id": "", "user_id": "42", "webhook_url": HOOK,
              "after_iso": "2026-10-01T00:00:00Z",
              "filters": {"download_media": {"images": True}}}
    run = await svc.start("export_messages", inputs)
    assert run.title == "Export messages from Zeta"
    assert run.context == {"label": "Zeta", "guild_id": GID}
    await _until(lambda: client.mid)

    req = client.of("export_messages")[0]
    assert req["job_id"] == str(run.id)
    assert {k: req[k] for k in ("guild_id", "channel_id", "user_id", "webhook_url", "has_attachments",
                                "after_iso", "before_iso")} == {
        "guild_id": GID, "channel_id": None, "user_id": "42", "webhook_url": HOOK,
        "has_attachments": False, "after_iso": "2026-10-01T00:00:00Z", "before_iso": None}
    assert req["filters"]["download_media"] == {"images": True, "videos": False, "audio": False, "other": False}

    steps = _steps(svc, run.id)
    assert list(steps) == ["scan", "json", "media", "forward"]
    assert steps["scan"]["state"] == "success" and steps["scan"]["note"] == "250 scanned, 50 messages matched"
    assert steps["json"]["state"] == "success" and steps["json"]["note"] == "50 messages saved"
    assert steps["media"]["state"] == "running" and (steps["media"]["done"], steps["media"]["total"]) == (4, 4)
    assert steps["forward"]["state"] == "running" and steps["forward"]["label"] == "10 of 50 forwarded"
    assert svc.get(run.id).summary == {"scanned": 250, "exported": 50, "failed": 0, "forwarded": 10,
                                       "downloaded": 3, "download_errors": 1}

    client.release.set()
    done = await _settled(svc, run.id)
    assert done.status == "success"
    steps = _steps(svc, run.id)
    assert steps["media"]["state"] == "success" and steps["media"]["note"] == "3 files downloaded, 1 failed"
    assert steps["forward"]["state"] == "success" and steps["forward"]["note"] == "50 messages forwarded"
    assert done.summary == {"scanned": 250, "exported": 50, "failed": 0, "forwarded": 50,
                            "downloaded": 3, "download_errors": 1}
    report = done.report
    assert report["guild"] == {"id": GID, "name": "Zeta"}
    assert report["output"] == {
        "json_path": f"/data/exports/{GID}/20261017-101010/messages.json",
        "media_dir": f"/data/exports/{GID}/20261017-101010/media",
        "webhook": "https://discord.com/api/webhooks/111/...",
    }
    assert report["media"] == {"images": 3, "videos": 0, "audio": 0, "other": 0, "errors": 1}
    assert report["summary"] == done.summary and report["error"] is None
    assert report["inputs"]["user_id"] == "42"
    assert "secret-token" not in str(report)
    assert all("/data/exports" not in str(s) for s in done.steps)
    assert export_messages._jobs == {}


@pytest.mark.asyncio
async def test_scan_progress_label(svc, client):
    run = await svc.start("export_messages", {"guild_id": GID})
    await _until(lambda: client.of("export_messages"))
    assert list(_steps(svc, run.id)) == ["scan", "json"]
    await _frame("export_messages_progress", run.id, phase="scan", done=1, total=3,
                 scanned=400, matched=12, channel="#memes")
    step = _steps(svc, run.id)["scan"]
    assert (step["done"], step["total"]) == (1, 3)
    assert step["label"] == "1 of 3 channels (#memes), 400 scanned, 12 matched"
    await _frame("export_messages_done", run.id, scanned=500, matched=20,
                 json_path=f"/x/exports/{GID}/t/messages.json")
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert done.summary == {"scanned": 500, "exported": 20, "failed": 0}
    assert done.report["output"]["media_dir"] is None and done.report["media"] is None


# cancel

async def _held_scan(fake, job_id):
    try:
        await _frame("export_messages_progress", job_id, phase="scan", done=0, total=5,
                     scanned=300, matched=9)
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await _frame("export_messages_done", job_id, error="cancelled", scanned=300, matched=9, forwarded=0)
        raise


@pytest.mark.asyncio
async def test_cancel_stops_the_client_runner(svc, client):
    client.script = _held_scan
    run = await svc.start("export_messages", {"guild_id": GID, "webhook_url": HOOK})
    await _until(lambda: (svc.get(run.id).summary or {}).get("scanned") == 300)
    await svc.cancel(run.id)
    done = await _settled(svc, run.id)
    assert client.of("export_messages_cancel") == [{"guild_id": int(GID), "job_id": str(run.id)}]
    assert client.task.cancelled()
    assert done.status == "cancelled"
    steps = _steps(svc, run.id)
    assert steps["scan"]["state"] == "cancelled" and steps["scan"]["note"] == "Cancelled after 300 scanned"
    assert steps["json"]["state"] == "cancelled" and steps["forward"]["state"] == "cancelled"
    assert done.summary == {"scanned": 300, "exported": 9, "failed": 0, "forwarded": 0}
    assert done.report["error"] is None and done.report["output"]["json_path"] is None


@pytest.mark.asyncio
async def test_cancel_without_an_end_from_the_client(svc, client):
    run = await svc.start("export_messages", {"guild_id": GID})
    await _until(lambda: client.of("export_messages"))
    await _frame("export_messages_progress", run.id, phase="scan", done=1, total=4, scanned=40, matched=2)
    await svc.cancel(run.id)
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    assert client.of("export_messages_cancel")
    steps = _steps(svc, run.id)
    assert steps["scan"]["state"] == "cancelled" and steps["scan"]["note"] == "Cancelled after 40 scanned"
    assert steps["json"]["state"] == "cancelled"
    # A frame after the run ended belongs to no job.
    assert await _frame("export_messages_done", run.id, scanned=40) is False


# failures

@pytest.mark.asyncio
async def test_client_refuses_the_export(svc, client):
    client.replies["export_messages"] = {"ok": False, "error": "Guild not found"}
    run = await svc.start("export_messages", {"guild_id": GID, "webhook_url": HOOK})
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    steps = _steps(svc, run.id)
    assert steps["scan"]["state"] == "failed"
    assert steps["scan"]["note"] == "The client account is not in this server"
    assert steps["json"]["state"] == "skipped" and steps["forward"]["state"] == "skipped"
    assert done.report["error"] == "The client account is not in this server"


@pytest.mark.asyncio
async def test_no_readable_channels(svc, client):
    run = await svc.start("export_messages", {"guild_id": GID})
    await _until(lambda: client.of("export_messages"))
    await _frame("export_messages_done", run.id, error="no-readable-channels", scanned=0, forwarded=0)
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert done.error == "The client account cannot read any channel in this server"


@pytest.mark.asyncio
async def test_json_write_failure_fails_its_step(svc, client):
    run = await svc.start("export_messages", {"guild_id": GID})
    await _until(lambda: client.of("export_messages"))
    await _frame("export_messages_progress", run.id, phase="json", matched=7, json_error="disk full")
    await _frame("export_messages_done", run.id, scanned=10, matched=7)
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert _steps(svc, run.id)["json"]["note"] == "disk full"
    assert done.summary == {"scanned": 10, "exported": 7, "failed": 7}


@pytest.mark.asyncio
async def test_forward_cut_short_fails_its_step(svc, client):
    run = await svc.start("export_messages", {"guild_id": GID, "webhook_url": HOOK})
    await _until(lambda: client.of("export_messages"))
    await _frame("export_messages_done", run.id, scanned=10, matched=8, forwarded=5,
                 json_path=f"/x/exports/{GID}/t/messages.json")
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert _steps(svc, run.id)["forward"]["note"] == "Stopped after 5 of 8"
    assert done.summary["failed"] == 3


# the export file route

STAMP = "20261017-022246"


@pytest.fixture()
def http(tmp_path, monkeypatch):
    repo = MemoryRunRepo()
    service = TaskService(repo)
    monkeypatch.setattr(export_messages, "_wiring", export_messages._Wiring(data_dir=tmp_path))
    monkeypatch.setattr(auth, "ADMIN_PASSWORD", "pw")
    monkeypatch.setattr(app_module, "_startup_hooks", [])
    monkeypatch.setattr(app_module, "_shutdown_hooks", [])
    task_routes.set_service(service)
    try:
        with TestClient(app_module.app) as tc:
            tc.headers.update({"origin": "http://testserver", CSRF_HEADER: "1",
                               "cookie": f"{auth.ADMIN_COOKIE_NAME}={auth.make_session_token()}"})
            yield tc, repo, tmp_path
    finally:
        task_routes.set_service(None)


def _finished_run(repo, json_path, type_key="export_messages", guild_id=GID):
    run = TaskRun(type=type_key, number=4, status="success", title="Export messages from Zeta",
                  trigger="manual", actor="admin", inputs={"guild_id": guild_id},
                  context={"label": "Zeta", "guild_id": guild_id},
                  summary={"scanned": 80, "exported": 62, "failed": 0},
                  report={"guild": {"id": guild_id, "name": "Zeta"},
                          "output": {"json_path": json_path, "media_dir": None, "webhook": None},
                          "summary": {"scanned": 80, "exported": 62, "failed": 0}})
    return repo.insert_run(run)


def _saved(tmp_path, guild_id=GID, body=b'{"count": 1}'):
    path = tmp_path / "exports" / guild_id / STAMP / "messages.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(body)
    return path


def test_route_serves_the_export_file(http):
    tc, repo, tmp_path = http
    rid = _finished_run(repo, str(_saved(tmp_path)))
    info = tc.get(f"/api/tasks/export-messages/{rid}")
    assert info.status_code == 200
    assert info.json() == {"file": {"filename": f"messages-{GID}-{STAMP}.json", "size": 12, "messages": 62}}
    resp = tc.get(f"/api/tasks/export-messages/{rid}/file")
    assert resp.status_code == 200 and resp.content == b'{"count": 1}'
    assert resp.headers["content-type"].startswith("application/json")
    assert f'filename="messages-{GID}-{STAMP}.json"' in resp.headers["content-disposition"]


def test_route_file_gone(http):
    tc, repo, tmp_path = http
    path = _saved(tmp_path)
    rid = _finished_run(repo, str(path))
    path.unlink()
    resp = tc.get(f"/api/tasks/export-messages/{rid}")
    assert resp.status_code == 404 and resp.json() == {"detail": "File no longer on disk"}


def test_route_unknown_run_is_404(http):
    tc, repo, tmp_path = http
    other = _finished_run(repo, str(_saved(tmp_path)), type_key="export_dms")
    for rid in ("0" * 32, "nope", other):
        for suffix in ("", "/file"):
            resp = tc.get(f"/api/tasks/export-messages/{rid}{suffix}")
            assert resp.status_code == 404 and resp.json() == {"detail": "Unknown run"}


def test_route_never_serves_outside_the_guild_exports_dir(http):
    tc, repo, tmp_path = http
    _saved(tmp_path)
    other_guild = _saved(tmp_path, guild_id="999")
    dm = tmp_path / "exports" / "dm" / "1" / STAMP / "messages.json"
    dm.parent.mkdir(parents=True)
    dm.write_text("{}")
    secret = tmp_path / "secret.json"
    secret.write_text("{}")
    for bad in (str(secret), str(other_guild), str(dm),
                f"{tmp_path}/exports/{GID}/{STAMP}/../../../secret.json", "/etc/passwd",
                str(tmp_path / "exports" / GID)):
        rid = _finished_run(repo, bad)
        resp = tc.get(f"/api/tasks/export-messages/{rid}/file")
        assert resp.status_code == 404 and resp.json() == {"detail": "No export file"}, bad


def test_route_needs_a_session(http):
    tc, repo, tmp_path = http
    rid = _finished_run(repo, str(_saved(tmp_path)))
    tc.headers.pop("cookie")
    assert tc.get(f"/api/tasks/export-messages/{rid}/file", follow_redirects=False).status_code == 401


# the client's runner

def _msg(i):
    return SimpleNamespace(
        id=i, content=f"hello {i}", system_content="", attachments=[], embeds=[], stickers=[],
        reactions=[], mentions=[], role_mentions=[], channel_mentions=[], pinned=False,
        reference=None, flags=SimpleNamespace(value=0), type=None,
        author=SimpleNamespace(id=7, bot=False),
    )


class FakeBot:
    def __init__(self, per_channel, hold=None):
        self.per_channel = per_channel
        self.hold = hold
        self.user = SimpleNamespace(id=1)

    async def history(self, ch, raise_on_error=True, **kwargs):
        for i in range(self.per_channel):
            yield _msg(ch.id * 1000 + i)
        if self.hold is not None:
            await self.hold.wait()


def _guild(n_channels):
    chans = [SimpleNamespace(id=i + 1, name=f"c{i + 1}") for i in range(n_channels)]
    return SimpleNamespace(id=int(GID), name="Zeta", text_channels=chans, forum_channels=[],
                           get_member=lambda uid: None)


class FakeWs:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)

    def of(self, typ):
        return [p["data"] for p in self.sent if p["type"] == typ]


def _runner(bot, ws, tmp_path):
    runner = ExportMessagesRunner(bot=bot, ws=ws, msg_serializer=lambda m: {"id": str(m.id)},
                                  send_sleep=0, out_root=str(tmp_path))
    runner.PROGRESS_EVERY = 3
    return runner


@pytest.mark.asyncio
async def test_runner_sends_progress_and_done_with_the_job_id(tmp_path):
    ws = FakeWs()
    runner = _runner(FakeBot(per_channel=4), ws, tmp_path)
    await runner.run({"guild_id": GID, "webhook_url": HOOK, "job_id": "j1"}, _guild(2))
    phases = [d["phase"] for d in ws.of("export_messages_progress")]
    assert phases[0] == "scan" and "json" in phases and phases[-1] == "forward"
    assert all(d["job_id"] == "j1" for d in ws.of("export_messages_progress"))
    last_scan = [d for d in ws.of("export_messages_progress") if d["phase"] == "scan"][-1]
    assert (last_scan["done"], last_scan["total"], last_scan["scanned"], last_scan["matched"]) == (2, 2, 8, 8)
    json_frame = next(d for d in ws.of("export_messages_progress") if d["phase"] == "json")
    assert json_frame["json_path"].startswith(str(tmp_path / GID))
    forwards = [d for d in ws.of("export_messages_progress") if d["phase"] == "forward"]
    assert (forwards[-1]["done"], forwards[-1]["total"]) == (8, 8)
    assert len(ws.of("export_message")) == 8
    (done,) = ws.of("export_messages_done")
    assert done["job_id"] == "j1" and done["matched"] == 8 and done["forwarded"] == 8
    assert done["json_path"] == json_frame["json_path"] and "error" not in done
    assert runner._active_guilds == set() and runner._tasks == {}


@pytest.mark.asyncio
async def test_runner_without_a_job_id_sends_no_progress(tmp_path):
    ws = FakeWs()
    await _runner(FakeBot(per_channel=2), ws, tmp_path).run({"guild_id": GID}, _guild(1))
    assert ws.of("export_messages_progress") == []
    (done,) = ws.of("export_messages_done")
    assert "job_id" not in done and done["matched"] == 2


@pytest.mark.asyncio
async def test_runner_cancel_sends_a_cancelled_done(tmp_path):
    ws = FakeWs()
    hold = asyncio.Event()
    runner = _runner(FakeBot(per_channel=5, hold=hold), ws, tmp_path)
    guild = _guild(1)
    assert await runner.try_begin(guild.id)
    task = asyncio.ensure_future(runner.run({"guild_id": GID, "job_id": "j2"}, guild, acquired=True))
    runner.register_task(guild.id, task, "j2")
    await _until(lambda: any(d.get("scanned") == 3 for d in ws.of("export_messages_progress")))
    assert await runner.cancel(guild.id, "other-job") is False
    assert await runner.cancel(guild.id, "j2") is True
    assert task.cancelled()
    (done,) = ws.of("export_messages_done")
    assert done == {"guild_id": guild.id, "forwarded": 0, "scanned": 5, "matched": 5,
                    "error": "cancelled", "job_id": "j2"}
    assert runner._active_guilds == set() and runner._tasks == {}
    assert await runner.cancel(guild.id) is False
