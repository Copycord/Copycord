"""Export assets task type: a fake client sender and the client's export_assets_* frames
handed straight to the dispatcher, as the admin bus watcher does; the client's
AssetExportRunner on fake guild assets; the archive route; /pull_assets is gone."""
import ast
import asyncio
import tarfile
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin.security import CSRF_HEADER
from copycord.admin.tasks import avatars, export_assets, registry
from copycord.admin.tasks import routes as task_routes
from copycord.admin.tasks.service import MemoryRunRepo, TaskRun, TaskService
from copycord.client.export_runners import AssetExportRunner

GID = "123456789012345678"
OTHER = "223456789012345678"
SRC = Path(__file__).resolve().parents[1] / "src" / "copycord"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(avatars, "_wiring", avatars._Wiring())
    monkeypatch.setattr(export_assets, "_wiring", export_assets._Wiring())
    monkeypatch.setattr(export_assets, "CANCEL_WAIT", 0.2)
    export_assets._jobs.clear()
    export_assets._guild_names.clear()
    yield
    export_assets._jobs.clear()
    export_assets._guild_names.clear()


@pytest.fixture()
def sent(monkeypatch):
    frames = []
    replies = {}

    async def send(frame):
        frames.append(frame)
        return replies.get(frame["type"], {"ok": True, "accepted": True})

    export_assets.configure(send=send)
    return frames, replies


@pytest.fixture()
def svc():
    return TaskService(MemoryRunRepo(), cancel_grace=2.0)


def _of(frames, typ):
    return [f["data"] for f in frames if f["type"] == typ]


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


def _steps(svc, run_id):
    return {s["key"]: s for s in svc.get(run_id).steps}


async def _frame(typ, job_id, **data):
    return await export_assets.dispatch({"type": typ, "data": {"job_id": str(job_id), **data}})


async def _started(svc, sent, inputs):
    frames, _ = sent
    run = await svc.start("export_assets", inputs)
    await _until(lambda: _of(frames, "export_assets"))
    return run


# type, inputs, describe, options

def test_registered_with_the_contract():
    t = registry.get("export_assets")
    assert t is not None and t.title == "Export assets"
    assert t.requires_bots is True and t.options is not None
    props = t.inputs_schema()["properties"]
    assert set(props) == {"guild_id", "asset"}
    assert props["asset"]["default"] == "both"
    assert set(props["asset"]["enum"]) == {"both", "emojis", "stickers"}


@pytest.mark.asyncio
async def test_inputs_are_validated(svc, sent):
    for bad, key in (({}, "guild_id"), ({"guild_id": "abc"}, "guild_id"),
                     ({"guild_id": GID, "asset": "gifs"}, "asset")):
        with pytest.raises(ValueError) as exc:
            await svc.start("export_assets", bad)
        assert key in exc.value.args[0], bad
    inputs = export_assets.ExportAssetsInputs.model_validate({"guild_id": f" {GID} ", "asset": "Emojis"})
    assert inputs.guild_id == GID and inputs.asset == "emojis" and inputs.kinds() == ("emojis",)
    assert export_assets.ExportAssetsInputs.model_validate({"guild_id": GID}).kinds() == ("emojis", "stickers")


@pytest.mark.asyncio
async def test_a_server_the_client_is_not_in_is_a_field_error(svc, sent):
    export_assets._guild_names[OTHER] = "Other"
    with pytest.raises(ValueError) as exc:
        await svc.start("export_assets", {"guild_id": GID})
    assert "The client account is not in this server" in exc.value.args[0]["guild_id"]


def test_describe_names_the_server():
    inputs = export_assets.ExportAssetsInputs(guild_id=GID)
    assert export_assets.describe(inputs) == (f"Export assets from {GID}", {"label": GID, "guild_id": GID})
    export_assets._guild_names[GID] = "Alpha"
    assert export_assets.describe(inputs) == ("Export assets from Alpha", {"label": "Alpha", "guild_id": GID})


class _Db:
    def __init__(self, token="tok"):
        self.token = token

    def get_config(self, key, default=""):
        return self.token if key == "CLIENT_TOKEN" else default


@pytest.mark.asyncio
async def test_options_list_the_client_servers():
    seen = []

    async def user_guilds(token):
        seen.append(token)
        return [{"id": OTHER, "name": "beta", "icon": "abc"}, {"id": GID, "name": "Alpha", "icon": None},
                {"id": "nope", "name": "Bad"}]

    export_assets.configure(user_guilds=user_guilds, db=_Db())
    res = await export_assets.options({})
    assert seen == ["tok"]
    assert res == {"guilds": [{"id": GID, "name": "Alpha"}, {"id": OTHER, "name": "beta", "icon": "abc"}]}
    assert export_assets._guild_names == {GID: "Alpha", OTHER: "beta"}


@pytest.mark.asyncio
async def test_options_errors():
    assert (await export_assets.options({}))["error"] == "Server list not configured"

    async def boom(token):
        raise RuntimeError("discord-rate-limited")

    export_assets.configure(user_guilds=boom, db=_Db(""))
    assert await export_assets.options({}) == {"guilds": [], "error": "Set the client account token first."}
    export_assets.configure(db=_Db())
    res = await export_assets.options({})
    assert res["guilds"] == [] and "rate limiting" in res["error"]


@pytest.mark.asyncio
async def test_falls_back_to_the_avatars_wiring(svc):
    frames = []

    async def send(frame):
        frames.append(frame)
        return {"ok": True}

    avatars.configure(send=send)
    run = await svc.start("export_assets", {"guild_id": GID, "asset": "stickers"})
    await _until(lambda: _of(frames, "export_assets"))
    await _frame("export_assets_done", run.id, ok=True, saved_stickers=0, total_stickers=0,
                 archive="/data/assets/x.tar.gz")
    assert (await _settled(svc, run.id)).status == "success"


# runs with a fake client

@pytest.mark.asyncio
async def test_both_kinds_then_done(svc, sent):
    frames, _ = sent
    export_assets._guild_names[GID] = "Alpha"
    run = await _started(svc, sent, {"guild_id": GID})
    assert run.title == "Export assets from Alpha" and run.context["label"] == "Alpha"
    assert _of(frames, "export_assets")[0] == {"job_id": str(run.id), "guild_id": int(GID), "asset": "both"}
    steps = _steps(svc, run.id)
    assert list(steps) == ["emojis", "stickers"]
    assert steps["emojis"]["title"] == "Emojis" and steps["stickers"]["title"] == "Stickers"
    assert steps["emojis"]["state"] == "running" and steps["stickers"]["state"] == "queued"

    await _frame("export_assets_progress", run.id, kind="emojis", done=0, total=2, saved=0, failed=0)
    await _frame("export_assets_progress", run.id, kind="emojis", done=1, total=2, saved=1, failed=0)
    steps = _steps(svc, run.id)
    assert (steps["emojis"]["done"], steps["emojis"]["total"]) == (1, 2)
    assert steps["emojis"]["label"] == "1 of 2 saved"
    await _frame("export_assets_progress", run.id, kind="emojis", done=2, total=2, saved=2, failed=0)
    assert _steps(svc, run.id)["emojis"]["state"] == "success"
    await _frame("export_assets_progress", run.id, kind="stickers", done=0, total=3, saved=0, failed=0)
    assert _steps(svc, run.id)["stickers"]["state"] == "running"
    await _frame("export_assets_progress", run.id, kind="stickers", done=3, total=3, saved=2, failed=1)
    archive = "/data/assets/Alpha_1/Alpha_1__20261019-120000.tar.gz"
    await _frame("export_assets_done", run.id, ok=True, error=None, archive=archive,
                 saved_emojis=2, total_emojis=2, failed_emojis=0,
                 saved_stickers=2, total_stickers=3, failed_stickers=1)
    done = await _settled(svc, run.id)
    assert done.status == "success"
    steps = _steps(svc, run.id)
    assert steps["emojis"]["note"] == "2 of 2 saved"
    assert steps["stickers"]["state"] == "success" and steps["stickers"]["note"] == "2 of 3 saved"
    assert done.summary == {"emojis_saved": 2, "emojis_total": 2, "stickers_saved": 2, "stickers_total": 3}
    assert done.report["output"] == {"archive_path": archive}
    assert done.report["guild"] == {"guild_id": GID, "name": "Alpha"}
    assert done.report["failed"] == {"emojis": 0, "stickers": 1}
    assert done.report["error"] is None
    assert any("Done: 2 emojis and 2 stickers saved" in line["msg"] for line in done.log_lines)


@pytest.mark.asyncio
async def test_emojis_only(svc, sent):
    frames, _ = sent
    run = await _started(svc, sent, {"guild_id": GID, "asset": "emojis"})
    assert _of(frames, "export_assets")[0]["asset"] == "emojis"
    assert list(_steps(svc, run.id)) == ["emojis"]
    await _frame("export_assets_progress", run.id, kind="emojis", done=1, total=1, saved=1, failed=0)
    # A stickers frame belongs to no step of this run.
    await _frame("export_assets_progress", run.id, kind="stickers", done=0, total=4)
    await _frame("export_assets_done", run.id, ok=True, archive="/data/assets/a.tar.gz",
                 saved_emojis=1, total_emojis=1, failed_emojis=0,
                 saved_stickers=0, total_stickers=0, failed_stickers=0)
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert list(_steps(svc, run.id)) == ["emojis"]
    assert done.summary == {"emojis_saved": 1, "emojis_total": 1}


@pytest.mark.asyncio
async def test_done_without_progress_and_a_kind_with_none(svc, sent):
    run = await _started(svc, sent, {"guild_id": GID})
    await _frame("export_assets_done", run.id, ok=True, archive="/data/assets/a.tar.gz",
                 saved_emojis=3, total_emojis=3, failed_emojis=0,
                 saved_stickers=0, total_stickers=0, failed_stickers=0)
    done = await _settled(svc, run.id)
    assert done.status == "success"
    steps = _steps(svc, run.id)
    assert steps["emojis"]["state"] == "success" and steps["emojis"]["note"] == "3 of 3 saved"
    assert steps["stickers"]["state"] == "success" and steps["stickers"]["note"] == "No stickers in this server"


@pytest.mark.asyncio
async def test_no_asset_saved_fails_the_step(svc, sent):
    run = await _started(svc, sent, {"guild_id": GID, "asset": "emojis"})
    await _frame("export_assets_progress", run.id, kind="emojis", done=2, total=2, saved=0, failed=2)
    await _frame("export_assets_done", run.id, ok=True, archive="/data/assets/a.tar.gz",
                 saved_emojis=0, total_emojis=2, failed_emojis=2)
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert done.error == "None of the 2 emojis could be saved"


@pytest.mark.asyncio
async def test_no_archive_fails_the_run(svc, sent):
    run = await _started(svc, sent, {"guild_id": GID, "asset": "emojis"})
    await _frame("export_assets_done", run.id, ok=True, archive=None, saved_emojis=1, total_emojis=1)
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "The archive could not be written"


@pytest.mark.asyncio
async def test_cancel_stops_the_client_and_keeps_the_archive(svc, sent):
    frames, _ = sent
    run = await _started(svc, sent, {"guild_id": GID})
    await _frame("export_assets_progress", run.id, kind="emojis", done=3, total=10, saved=3, failed=0)
    await svc.cancel(run.id)
    await _until(lambda: _of(frames, "export_assets_cancel"))
    assert _of(frames, "export_assets_cancel")[0] == {"job_id": str(run.id)}
    archive = "/data/assets/a.tar.gz"
    await _frame("export_assets_done", run.id, ok=False, error="cancelled", archive=archive,
                 saved_emojis=3, total_emojis=10, failed_emojis=0)
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    steps = _steps(svc, run.id)
    assert steps["emojis"]["state"] == "cancelled" and steps["emojis"]["note"] == "Cancelled after 3 of 10 saved"
    assert steps["stickers"]["state"] == "cancelled" and steps["stickers"]["note"] == "Cancelled"
    assert done.summary == {"emojis_saved": 3, "emojis_total": 10, "stickers_saved": 0, "stickers_total": 0}
    assert done.report["output"] == {"archive_path": archive}
    assert done.report["error"] is None


@pytest.mark.asyncio
async def test_cancel_without_an_end_from_the_client(svc, sent):
    frames, _ = sent
    run = await _started(svc, sent, {"guild_id": GID, "asset": "stickers"})
    await svc.cancel(run.id)
    done = await _settled(svc, run.id)
    assert done.status == "cancelled"
    assert _of(frames, "export_assets_cancel")
    assert _steps(svc, run.id)["stickers"]["state"] == "cancelled"
    # A frame after the run ended belongs to no job.
    assert await _frame("export_assets_done", run.id, ok=True) is False


@pytest.mark.asyncio
async def test_client_refuses_the_export(svc, sent):
    _, replies = sent
    replies["export_assets"] = {"ok": False, "error": "not-in-guild"}
    run = await svc.start("export_assets", {"guild_id": GID})
    done = await _settled(svc, run.id)
    assert done.status == "failed"
    assert done.error == "The client account is not in this server"
    steps = _steps(svc, run.id)
    assert steps["emojis"]["state"] == "failed" and steps["stickers"]["state"] == "skipped"
    assert done.report["error"] == "The client account is not in this server"


@pytest.mark.asyncio
async def test_one_run_per_server(svc, sent):
    frames, replies = sent
    first = await _started(svc, sent, {"guild_id": GID})
    second = await svc.start("export_assets", {"guild_id": GID, "asset": "emojis"})
    done = await _settled(svc, second.id)
    assert done.status == "failed" and done.error == "An asset export for this server is already running"
    assert len(_of(frames, "export_assets")) == 1
    # Another server runs alongside.
    third = await svc.start("export_assets", {"guild_id": OTHER})
    await _until(lambda: len(_of(frames, "export_assets")) == 2)
    # The client's own refusal reads the same.
    replies["export_assets"] = {"ok": False, "error": "asset-export-in-progress"}
    fourth = await svc.start("export_assets", {"guild_id": "323456789012345678"})
    assert (await _settled(svc, fourth.id)).error == "An asset export for this server is already running"
    for run in (first, third):
        await _frame("export_assets_done", run.id, ok=True, archive="/data/assets/a.tar.gz")
        assert (await _settled(svc, run.id)).status == "success"


# the client's AssetExportRunner

class _Url:
    def __init__(self, name, body, gate=None):
        self.name, self.body, self.gate = name, body, gate

    def __str__(self):
        return f"https://cdn.discordapp.com/{self.name}"

    async def read(self):
        if self.gate is not None:
            await self.gate.wait()
        if self.body is None:
            raise RuntimeError("404")
        return self.body


class _Asset:
    def __init__(self, aid, name, ext, body=b"x", gate=None):
        self.id, self.name = aid, name
        self.url = _Url(f"{aid}.{ext}", body, gate)


class _Guild:
    def __init__(self, emojis, stickers):
        self.id, self.name = int(GID), "Alpha Server"
        self.emojis, self.stickers = emojis, stickers


def _members(archive):
    with tarfile.open(archive, "r:gz") as tf:
        return sorted(m.name.split("/", 1)[1] for m in tf.getmembers() if m.isfile())


@pytest.mark.asyncio
async def test_client_runner_sends_progress_and_done(tmp_path):
    out = []

    async def publish(frame):
        out.append(frame)

    guild = _Guild([_Asset(1, "wave", "png"), _Asset(2, "broken", "png", body=None)],
                   [_Asset(3, "cat", "gif")])
    runner = AssetExportRunner(None, None, out_root=str(tmp_path), job_id="job1", publish=publish)
    assert AssetExportRunner._jobs["job1"] is runner
    res = await runner.run(guild)
    assert "job1" not in AssetExportRunner._jobs
    assert (res["saved_emojis"], res["failed_emojis"], res["saved_stickers"]) == (1, 1, 1)
    progress = [f["data"] for f in out if f["type"] == "export_assets_progress"]
    assert [(p["kind"], p["done"], p["total"], p["saved"], p["failed"]) for p in progress] == [
        ("emojis", 0, 2, 0, 0), ("emojis", 1, 2, 1, 0), ("emojis", 2, 2, 1, 1),
        ("stickers", 0, 1, 0, 0), ("stickers", 1, 1, 1, 0)]
    assert all(p["job_id"] == "job1" for p in progress)
    end = out[-1]
    assert end["type"] == "export_assets_done"
    assert end["data"]["ok"] is True and end["data"]["error"] is None
    assert end["data"]["saved_emojis"] == 1 and end["data"]["total_emojis"] == 2
    archive = Path(end["data"]["archive"])
    assert archive.parent == tmp_path / f"AlphaServer_{GID}" and archive.name.endswith(".tar.gz")
    assert _members(archive) == ["emoji/wave_1.png", "stickers/cat_3.gif"]
    # The working directory is gone, the archive stays.
    assert [p.name for p in archive.parent.iterdir()] == [archive.name]


@pytest.mark.asyncio
async def test_client_runner_cancel_between_assets_keeps_what_was_saved(tmp_path):
    out = []
    gate = asyncio.Event()

    async def publish(frame):
        out.append(frame)

    guild = _Guild([_Asset(1, "one", "png"), _Asset(2, "two", "png", gate=gate), _Asset(4, "four", "png")],
                   [_Asset(3, "cat", "gif")])
    runner = AssetExportRunner(None, None, out_root=str(tmp_path), job_id="job2", publish=publish)
    task = asyncio.ensure_future(runner.run(guild, include_stickers=True))
    await _until(lambda: any(f["data"].get("done") == 1 for f in out))
    assert AssetExportRunner.cancel("job2") is True
    assert AssetExportRunner.cancel("nope") is False
    res = await asyncio.wait_for(task, 3.0)
    assert res["saved_emojis"] == 1 and res["saved_stickers"] == 0
    end = out[-1]["data"]
    assert out[-1]["type"] == "export_assets_done" and end["error"] == "cancelled" and end["ok"] is False
    assert not any(f["data"].get("kind") == "stickers" for f in out)
    assert _members(end["archive"]) == ["emoji/one_1.png"]
    # The guild is free again.
    assert await AssetExportRunner._try_begin(int(GID)) is True
    await AssetExportRunner._end(int(GID))


@pytest.mark.asyncio
async def test_client_runner_refuses_a_second_export_of_a_guild(tmp_path):
    out = []

    async def publish(frame):
        out.append(frame)

    assert await AssetExportRunner._try_begin(int(GID)) is True
    try:
        runner = AssetExportRunner(None, None, out_root=str(tmp_path), job_id="job3", publish=publish)
        await runner.run(_Guild([], []))
    finally:
        await AssetExportRunner._end(int(GID))
    assert out == [{"type": "export_assets_done", "data": {
        "job_id": "job3", "ok": False, "error": "asset-export-in-progress", "archive": None,
        "saved_emojis": 0, "total_emojis": 0, "failed_emojis": 0,
        "saved_stickers": 0, "total_stickers": 0, "failed_stickers": 0}}]


# the archive route

STAMP = "20261019-120000"


@pytest.fixture()
def http(tmp_path, monkeypatch):
    repo = MemoryRunRepo()
    service = TaskService(repo)
    export_assets.configure(data_dir=tmp_path)
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


def _finished_run(repo, archive, type_key="export_assets", summary=None):
    summary = summary if summary is not None else {
        "emojis_saved": 5, "emojis_total": 6, "stickers_saved": 2, "stickers_total": 2}
    run = TaskRun(type=type_key, number=3, status="success", title="Export assets from Alpha", trigger="manual",
                  actor="admin", inputs={"guild_id": GID, "asset": "both"}, context={"label": "Alpha", "guild_id": GID},
                  summary=summary, report={"output": {"archive_path": archive}, "summary": summary})
    return repo.insert_run(run)


def _saved(tmp_path, body=b"tar-bytes"):
    path = tmp_path / "assets" / f"Alpha_{GID}" / f"Alpha_{GID}__{STAMP}.tar.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


def test_route_serves_the_archive(http):
    tc, repo, tmp_path = http
    rid = _finished_run(repo, str(_saved(tmp_path)))
    name = f"Alpha_{GID}__{STAMP}.tar.gz"
    info = tc.get(f"/api/tasks/export-assets/{rid}")
    assert info.status_code == 200
    assert info.json() == {"file": {"filename": name, "size": 9, "emojis": 5, "stickers": 2}}
    resp = tc.get(f"/api/tasks/export-assets/{rid}/file")
    assert resp.status_code == 200 and resp.content == b"tar-bytes"
    assert resp.headers["content-type"].startswith("application/gzip")
    assert f'filename="{name}"' in resp.headers["content-disposition"]


def test_route_counts_only_the_chosen_kinds(http):
    tc, repo, tmp_path = http
    rid = _finished_run(repo, str(_saved(tmp_path)), summary={"emojis_saved": 4, "emojis_total": 4})
    assert tc.get(f"/api/tasks/export-assets/{rid}").json()["file"]["stickers"] is None


def test_route_file_gone_and_no_file(http):
    tc, repo, tmp_path = http
    path = _saved(tmp_path)
    rid = _finished_run(repo, str(path))
    path.unlink()
    for suffix in ("", "/file"):
        resp = tc.get(f"/api/tasks/export-assets/{rid}{suffix}")
        assert resp.status_code == 404 and resp.json() == {"detail": "File no longer on disk"}
    none = _finished_run(repo, None)
    resp = tc.get(f"/api/tasks/export-assets/{none}")
    assert resp.status_code == 404 and resp.json() == {"detail": "No export file"}


def test_route_unknown_run_is_404(http):
    tc, repo, tmp_path = http
    other = _finished_run(repo, str(_saved(tmp_path)), type_key="export_dms")
    for rid in ("0" * 32, "nope", other):
        for suffix in ("", "/file"):
            resp = tc.get(f"/api/tasks/export-assets/{rid}{suffix}")
            assert resp.status_code == 404 and resp.json() == {"detail": "Unknown run"}


def test_route_never_serves_outside_the_assets_dir(http):
    tc, repo, tmp_path = http
    _saved(tmp_path)
    secret = tmp_path / "secret.tar.gz"
    secret.write_text("x")
    plain = tmp_path / "assets" / "notes.txt"
    plain.write_text("x")
    for bad in (str(secret), str(tmp_path / "assets" / ".." / "secret.tar.gz"), "/etc/passwd",
                str(plain), str(tmp_path / "assets")):
        rid = _finished_run(repo, bad)
        resp = tc.get(f"/api/tasks/export-assets/{rid}/file")
        assert resp.status_code == 404 and resp.json() == {"detail": "No export file"}, bad


def test_route_needs_a_session(http):
    tc, repo, tmp_path = http
    rid = _finished_run(repo, str(_saved(tmp_path)))
    tc.headers.pop("cookie")
    assert tc.get(f"/api/tasks/export-assets/{rid}/file", follow_redirects=False).status_code == 401


# /pull_assets is gone

def _command_names(path):
    names = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call):
                    for kw in dec.keywords:
                        if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                            names.add(kw.value.value)
    return names


def test_pull_assets_is_no_longer_a_slash_command():
    names = _command_names(SRC / "server" / "commands.py")
    assert {"ping_server", "onjoin_role"} <= names
    assert "pull_assets" not in names
    assert "pull_assets" not in (SRC / "client" / "client.py").read_text(encoding="utf-8")
