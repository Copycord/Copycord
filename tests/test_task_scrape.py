"""Scrape members task type: a fake scraper in place of StandaloneScraper, a fake
token store and per token membership check, files under tmp_path."""
import asyncio
import json

import pytest
from starlette.testclient import TestClient

from copycord.admin import app as app_module
from copycord.admin import auth
from copycord.admin.security import CSRF_HEADER
from copycord.admin.standalone_scraper import ScraperResult
from copycord.admin.tasks import registry, scrape
from copycord.admin.tasks import routes as task_routes
from copycord.admin.tasks.service import MemoryRunRepo, TaskRun, TaskService

GID = "222333444555666777"
DASH = chr(0x2014)
TOKENS = ["tok-one", "tok-two"]


def _save_tokens(directory, values):
    (directory / "scraper_tokens.txt").write_text("".join(f"{v}\n" for v in values), encoding="utf-8")


class FakeScraper:
    """Finds `count` members, one per tick, then waits for `release` (when given) and
    returns `result` (or raises `raise_exc`). With endless=True it keeps finding members.
    Like the real one it turns a cancel into a Cancelled result unless stubborn, which
    keeps reporting after the cancel and never ends."""

    def __init__(self, config, *, count=3, hint=10, endless=False, release=None, result=None,
                 raise_exc=None, stubborn=False):
        self.config = config
        self._members = {}
        self.count, self.hint, self.endless = count, hint, endless
        self.release, self.result, self.raise_exc = release, result, raise_exc
        self.stubborn = stubborn
        self.stopped = False
        self.cancelled = False
        self.ticks = 0
        self.ended = False

    def _find(self):
        self.ticks += 1
        n = len(self._members) + 1
        self._members[str(n)] = {"id": str(n), "username": f"user{n}"}
        self.config.progress_callback(n, self.hint, f"Found {n:,} members")
        self.config.log_callback(f"{scrape.PROGRESS_MARK} Scraping {DASH} found {n:,} members so far", "info")

    async def scrape(self):
        self.config.log_callback(f"\U0001f680 Starting scraper for guild {self.config.guild_id}", "info")
        self.config.log_callback(f"Token 2 is invalid {DASH} skipping", "warn")
        try:
            while self.endless or len(self._members) < self.count:
                await asyncio.sleep(0.01)
                self._find()
            if self.release is not None:
                await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            if self.stubborn:
                # Keeps going for a while whatever it is told, then gives up (so the loop can close).
                for _ in range(80):
                    try:
                        await asyncio.sleep(0.01)
                        self._find()
                    except asyncio.CancelledError:
                        continue
            return ScraperResult(success=False, error="Cancelled", members=list(self._members.values()),
                                 total_count=len(self._members), elapsed_seconds=0.5,
                                 metadata={"guild_name": "Guild"})
        finally:
            self.ended = True
        if self.raise_exc is not None:
            raise self.raise_exc
        if self.result is not None:
            return self.result
        members = list(self._members.values())
        return ScraperResult(members=members, total_count=len(members), success=True, elapsed_seconds=1.25,
                             metadata={"method": "gateway", "tokens_used": len(self.config.tokens),
                                       "member_count_hint": self.hint, "guild_name": "Guild",
                                       "cancelled": False})

    def stop(self):
        self.stopped = True
        self.config.log_callback("\U0001f6d1 Cancelling scrape...", "info")


@pytest.fixture()
def env(tmp_path, monkeypatch):
    _save_tokens(tmp_path, TOKENS)
    built = []
    opts = {}
    # with: token ids in the server; fail: token values Discord refuses (the check raises).
    access = {"gate": None, "with": {"tok-one"}, "fail": set(), "calls": 0}

    def make(config):
        s = FakeScraper(config, **opts)
        built.append(s)
        return s

    async def in_guild(token_value, gid):
        access["calls"] += 1
        if access["gate"] is not None:
            await access["gate"].wait()
        if token_value in access["fail"]:
            raise app_module.DiscordGuildsError("client-token-invalid")
        access.setdefault("guilds", []).append(gid)
        return token_value in access["with"]

    async def user_guilds(token):
        if token in access["fail"]:
            raise app_module.DiscordGuildsError("client-token-invalid")
        if token == "tok-one":
            return [{"id": GID, "name": "Zeta"}, {"id": "111", "name": "alpha"}]
        if token == "tok-two":
            return [{"id": GID, "name": "Zeta"}, {"id": "nope", "name": "skip"}]
        return []

    monkeypatch.setattr(scrape, "_wiring", scrape._Wiring(
        data_dir=tmp_path, in_guild=in_guild, user_guilds=user_guilds,
        log_path=tmp_path / "logs" / "scraper.out", make_scraper=make))
    monkeypatch.setattr(scrape, "CANCEL_WAIT", 1.0)
    scrape._guild_names.clear()
    yield {"built": built, "opts": opts, "access": access, "dir": tmp_path}
    scrape._guild_names.clear()


@pytest.fixture()
def svc():
    return TaskService(MemoryRunRepo(), cancel_grace=5.0)


async def _until(cond, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not reached")


async def _settled(svc, run_id, timeout=3.0):
    await _until(lambda: svc.get(run_id).status in ("success", "failed", "cancelled")
                 and run_id not in svc._active, timeout)
    return svc.get(run_id)


def _steps(svc, run_id):
    return {s["key"]: s for s in svc.get(run_id).steps}


def _found(svc, run_id):
    """The Scrape members step's count (0 until the runner made the step)."""
    return _steps(svc, run_id).get("scrape", {}).get("done") or 0


def _msgs(run):
    return [ln["msg"] for ln in run.log_lines]


def test_registered_one_at_a_time_without_bots():
    t = registry.get("scrape_members")
    assert t is not None and t.title == "Scrape members"
    assert t.concurrency == 1
    assert t.requires_bots is False
    assert t.options is scrape.options
    assert set(t.inputs_schema()["properties"]) == {
        "guild_id", "include_username", "include_avatar_url", "include_bio", "include_roles", "use_proxies"}


@pytest.mark.asyncio
async def test_bad_guild_id_is_refused(svc, env):
    for bad in ("", "abc", None, "12a"):
        with pytest.raises(ValueError):
            await svc.start("scrape_members", {"guild_id": bad})
    assert env["built"] == []


@pytest.mark.asyncio
async def test_progress_then_success_saves_the_file(svc, env):
    (env["dir"] / "proxies.txt").write_text("1.2.3.4:80\n\n user:pw@5.6.7.8:3128 \n")
    release = asyncio.Event()
    env["opts"].update(count=3, hint=10, release=release)
    await scrape.options({})  # learns the server's name
    run = await svc.start("scrape_members", {"guild_id": GID, "include_bio": True})
    assert run.title == "Scrape members of Zeta"
    assert run.context == {"label": "Zeta", "guild_id": GID}

    await _until(lambda: _found(svc, run.id) == 3)
    await _until(lambda: _steps(svc, run.id)["scrape"]["label"] == "3 of about 10 members found")
    steps = _steps(svc, run.id)
    assert steps["tokens"]["state"] == "success"
    assert steps["tokens"]["note"] == "1 of 2 tokens used, 1 skipped"
    assert steps["scrape"]["state"] == "running" and steps["scrape"]["total"] == 10
    assert steps["save"]["state"] == "queued"
    cfg = env["built"][0].config
    assert cfg.guild_id == int(GID) and cfg.tokens == ["tok-one"]
    assert cfg.proxies == ["1.2.3.4:80", "user:pw@5.6.7.8:3128"]
    assert (cfg.include_username, cfg.include_avatar_url, cfg.include_bio, cfg.include_roles) == (
        True, True, True, False)

    release.set()
    done = await _settled(svc, run.id)
    assert done.status == "success" and done.error is None
    steps = _steps(svc, run.id)
    assert steps["scrape"]["state"] == "success" and steps["scrape"]["note"] == "3 members found"
    assert steps["save"]["state"] == "success" and steps["save"]["note"] == "Saved 3 members"
    assert done.summary == {"members": 3, "tokens": 1, "tokens_skipped": 1}

    out = done.report["output"]
    assert out["path"].startswith(f"{GID}/scrape_{GID}_") and out["path"].endswith(".json")
    saved = env["dir"] / "scrapes" / out["path"]
    data = json.loads(saved.read_text())
    assert data["guild_id"] == GID and data["guild_name"] == "Guild" and data["total_count"] == 3
    assert [m["id"] for m in data["members"]] == ["1", "2", "3"]
    assert out["size"] == saved.stat().st_size
    assert done.report["result"] == {"method": "gateway", "elapsed_seconds": 1.25,
                                     "member_count_hint": 10, "cancelled": False}
    assert done.report["guild"] == {"id": GID, "name": "Guild"}
    # Proxy strings stay out of the run; only their count is kept.
    assert done.report["inputs"]["proxies"] == 2
    assert "user:pw" not in json.dumps(done.to_wire())

    msgs = _msgs(done)
    assert f"\U0001f680 Starting scraper for guild {GID}" in msgs
    assert "Token 2 is invalid: skipping" in msgs
    assert not any(scrape.PROGRESS_MARK in m or DASH in m for m in msgs)
    assert msgs[-1] == "Done: 3 members from Guild"
    log_file = (env["dir"] / "logs" / "scraper.out").read_text()
    assert f"[Guild {GID}]" in log_file and "found 3 members so far" in log_file


@pytest.mark.asyncio
async def test_use_proxies_off_sends_none(svc, env):
    (env["dir"] / "proxies.txt").write_text("1.2.3.4:80\n")
    run = await svc.start("scrape_members", {"guild_id": GID, "use_proxies": False})
    assert (await _settled(svc, run.id)).status == "success"
    assert env["built"][0].config.proxies == []
    assert svc.get(run.id).report["inputs"]["proxies"] == 0


@pytest.mark.asyncio
async def test_scraper_failure_fails_the_run(svc, env):
    env["opts"].update(count=0, result=ScraperResult(success=False, error="No valid tokens with guild access"))
    run = await svc.start("scrape_members", {"guild_id": GID})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "No valid tokens with guild access"
    steps = _steps(svc, run.id)
    assert steps["scrape"]["state"] == "failed" and steps["scrape"]["note"] == "No valid tokens with guild access"
    assert steps["save"]["state"] == "skipped"
    assert done.report["error"] == "No valid tokens with guild access"
    assert not (env["dir"] / "scrapes").exists()


@pytest.mark.asyncio
async def test_scraper_exception_fails_the_run(svc, env):
    env["opts"].update(count=2, raise_exc=RuntimeError("gateway exploded"))
    run = await svc.start("scrape_members", {"guild_id": GID})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "gateway exploded"
    assert _steps(svc, run.id)["scrape"]["state"] == "failed"
    assert any(m == "Scrape failed: gateway exploded" for m in _msgs(done))


@pytest.mark.asyncio
async def test_no_members_writes_no_file(svc, env):
    env["opts"].update(count=0, hint=None)
    run = await svc.start("scrape_members", {"guild_id": GID})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    save = _steps(svc, run.id)["save"]
    assert save["state"] == "skipped" and save["note"] == "No members found, no file written"
    assert done.report["output"]["path"] is None


@pytest.mark.asyncio
async def test_no_token_can_see_the_server(svc, env):
    # Zero usable tokens: one out of the server, one Discord refuses. The only case that fails.
    env["access"]["with"] = {"tok-two"}
    env["access"]["fail"] = {"tok-two"}
    run = await svc.start("scrape_members", {"guild_id": GID})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "None of the 2 scraper tokens work in this server"
    steps = _steps(svc, run.id)
    assert steps["tokens"]["state"] == "failed"
    assert steps["tokens"]["note"] == "None of the 2 scraper tokens work in this server"
    assert steps["scrape"]["state"] == "skipped" and steps["save"]["state"] == "skipped"
    assert done.summary == {"members": 0, "tokens": 0, "tokens_skipped": 2}
    assert done.report["error"] == done.error
    assert env["access"]["calls"] == 2 and env["built"] == []


@pytest.mark.asyncio
async def test_bad_and_out_of_server_tokens_are_skipped(svc, env):
    # No validation: an unvalidated token is used, a refused one and one out of the server
    # are skipped with a note, and the run goes on with the rest.
    _save_tokens(env["dir"], ["tok-one", "tok-two", "tok-bad", "tok-four", "tok-out"])
    env["access"]["with"] = {"tok-one", "tok-bad", "tok-four"}
    env["access"]["fail"] = {"tok-bad"}
    run = await svc.start("scrape_members", {"guild_id": GID})
    done = await _settled(svc, run.id)
    assert done.status == "success" and done.error is None
    steps = _steps(svc, run.id)
    assert steps["tokens"]["state"] == "success"
    assert steps["tokens"]["note"] == "2 of 5 tokens used, 3 skipped"
    # File order, no usage sorting.
    assert env["built"][0].config.tokens == ["tok-one", "tok-four"]
    assert done.summary == {"members": 3, "tokens": 2, "tokens_skipped": 3}
    msgs = _msgs(done)
    assert "2 of 5 tokens used, 3 skipped" in msgs
    assert any(ln["msg"] == "3 tokens skipped: failed or not in this server" and ln["level"] == "warning"
               for ln in done.log_lines)
    assert not any(ln["level"] == "error" for ln in done.log_lines)


@pytest.mark.asyncio
async def test_all_tokens_used_has_no_skip_note(svc, env):
    env["access"]["with"] = {"tok-one", "tok-two"}
    run = await svc.start("scrape_members", {"guild_id": GID})
    done = await _settled(svc, run.id)
    assert done.status == "success"
    assert _steps(svc, run.id)["tokens"]["note"] == "2 of 2 tokens used"
    assert not any("skipped" in m for m in _msgs(done))


@pytest.mark.asyncio
async def test_no_tokens_saved(svc, env):
    _save_tokens(env["dir"], [])
    run = await svc.start("scrape_members", {"guild_id": GID})
    done = await _settled(svc, run.id)
    assert done.status == "failed" and done.error == "No scraper tokens saved"
    assert env["access"]["calls"] == 0 and env["built"] == []


@pytest.mark.asyncio
async def test_bots_stopping_leaves_a_scrape_running(svc, env):
    release = asyncio.Event()
    env["opts"].update(count=1, release=release)
    run = await svc.start("scrape_members", {"guild_id": GID})
    await _until(lambda: _found(svc, run.id) == 1)
    assert await svc.stop_all() == []
    assert svc.get(run.id).status == "running"
    release.set()
    assert (await _settled(svc, run.id)).status == "success"


@pytest.mark.asyncio
async def test_cancel_stops_the_scraper_within_a_second(svc, env):
    env["opts"].update(endless=True, hint=None)
    run = await svc.start("scrape_members", {"guild_id": GID})
    await _until(lambda: _found(svc, run.id) >= 5)
    scraper = env["built"][0]
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await svc.cancel(run.id)
    done = await _settled(svc, run.id, timeout=1.0)
    assert loop.time() - t0 < 1.0
    assert done.status == "cancelled"
    assert scraper.stopped and scraper.cancelled and scraper.ended
    # No further work once the cancel landed.
    ticks = scraper.ticks
    found = len(scraper._members)
    await asyncio.sleep(0.2)
    assert scraper.ticks == ticks
    steps = _steps(svc, run.id)
    assert steps["scrape"]["state"] == "cancelled"
    assert steps["scrape"]["note"] == f"Cancelled after {found} members found"
    assert steps["scrape"]["done"] == found
    # What was found before the cancel is kept, as the old page did.
    assert steps["save"]["state"] == "success"
    assert steps["save"]["note"] == f"Saved {found} members found before the cancel"
    saved = env["dir"] / "scrapes" / done.report["output"]["path"]
    assert json.loads(saved.read_text())["total_count"] == found
    assert done.report["result"]["cancelled"] is True
    assert done.summary["members"] == found
    assert not any("Cancelling scrape" in m for m in _msgs(done))


@pytest.mark.asyncio
async def test_cancel_does_not_wait_on_a_scraper_that_ignores_it(svc, env, monkeypatch):
    monkeypatch.setattr(scrape, "CANCEL_WAIT", 0.3)
    env["opts"].update(endless=True, stubborn=True, hint=None)
    run = await svc.start("scrape_members", {"guild_id": GID})
    await _until(lambda: _found(svc, run.id) >= 2)
    scraper = env["built"][0]
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await svc.cancel(run.id)
    done = await _settled(svc, run.id, timeout=1.0)
    assert loop.time() - t0 < 1.0
    assert done.status == "cancelled"
    seq = done.seq
    step = dict(_steps(svc, run.id)["scrape"])
    await asyncio.sleep(0.2)
    # The scraper still reports, but none of it reaches the ended run.
    assert scraper.ticks > 0 and svc.get(run.id).seq == seq
    assert _steps(svc, run.id)["scrape"] == step
    assert any(m == "The scraper did not stop within 0.3 s" for m in _msgs(done))
    await _until(lambda: scraper.ended)


@pytest.mark.asyncio
async def test_cancel_during_the_token_check(svc, env):
    env["access"]["gate"] = asyncio.Event()
    run = await svc.start("scrape_members", {"guild_id": GID})
    await _until(lambda: env["access"]["calls"] == 1)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await svc.cancel(run.id)
    done = await _settled(svc, run.id, timeout=1.0)
    assert loop.time() - t0 < 1.0
    assert done.status == "cancelled"
    assert env["built"] == []
    assert all(s["state"] == "cancelled" for s in done.steps)


@pytest.mark.asyncio
async def test_one_run_at_a_time_later_runs_wait(svc, env):
    release = asyncio.Event()
    env["opts"].update(count=1, release=release)
    first = await svc.start("scrape_members", {"guild_id": GID})
    await _until(lambda: len(env["built"]) == 1)
    second = await svc.start("scrape_members", {"guild_id": "111"})
    third = await svc.start("scrape_members", {"guild_id": GID})
    await asyncio.sleep(0.1)
    assert svc.get(second.id).status == "queued" and svc.get(third.id).status == "queued"
    assert len(env["built"]) == 1

    # A queued run cancels without ever building a scraper.
    await svc.cancel(third.id)
    assert (await _settled(svc, third.id)).status == "cancelled"
    assert len(env["built"]) == 1

    release.set()
    assert (await _settled(svc, first.id)).status == "success"
    assert (await _settled(svc, second.id)).status == "success"
    assert [s.config.guild_id for s in env["built"]] == [int(GID), 111]


@pytest.mark.asyncio
async def test_options(env):
    (env["dir"] / "proxies.txt").write_text("a:1\nb:2\n")
    out = await scrape.options({})
    assert out == {
        "guilds": [{"id": "111", "name": "alpha", "tokens": 1}, {"id": GID, "name": "Zeta", "tokens": 2}],
        "tokens": {"total": 2, "skipped": 0},
        "proxies": 2,
    }
    # No validation gate: a guild_id adds nothing and checks no token.
    out = await scrape.options({"guild_id": GID})
    assert "setup" not in out and env["access"]["calls"] == 0
    assert [g["id"] for g in out["guilds"]] == ["111", GID]

    # Unvalidated tokens list their servers; one Discord refuses is skipped, not an error.
    _save_tokens(env["dir"], TOKENS + ["tok-bad"])
    env["access"]["fail"] = {"tok-bad"}
    out = await scrape.options({})
    assert out["tokens"] == {"total": 3, "skipped": 1} and "error" not in out
    assert [g["id"] for g in out["guilds"]] == ["111", GID]

    # Only when no token lists anything is it an error.
    _save_tokens(env["dir"], ["tok-bad"])
    out = await scrape.options({})
    assert out["guilds"] == [] and out["tokens"] == {"total": 1, "skipped": 1}
    assert out["error"] == "Discord did not accept a scraper token."

    (env["dir"] / "scraper_tokens.txt").unlink()
    out = await scrape.options({"guild_id": GID})
    assert out["guilds"] == [] and out["tokens"] == {"total": 0, "skipped": 0}
    assert out["error"] == "No scraper tokens. Add one first." and "setup" not in out


def test_save_scrape_file(tmp_path):
    assert scrape.save_scrape_file(tmp_path, 5, [], {}, 0, 0) is None
    rel = scrape.save_scrape_file(tmp_path, 5, [{"id": "1"}], {"guild_name": "G"}, 1, 2.345)
    assert rel.startswith("5/scrape_5_")
    data = json.loads((tmp_path / "scrapes" / rel).read_text())
    assert {k: data[k] for k in ("guild_id", "guild_name", "total_count", "elapsed_seconds")} == {
        "guild_id": "5", "guild_name": "G", "total_count": 1, "elapsed_seconds": 2.35}


# the download route

@pytest.fixture()
def http(tmp_path, monkeypatch):
    repo = MemoryRunRepo()
    service = TaskService(repo)
    monkeypatch.setattr(scrape, "_wiring", scrape._Wiring(data_dir=tmp_path))
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
        task_routes.set_bots_check(None)


def _finished_run(repo, path, type_key="scrape_members"):
    run = TaskRun(type=type_key, number=2, status="success", title="Scrape members of Zeta", trigger="manual",
                  actor="admin", inputs={"guild_id": GID}, context={"label": "Zeta", "guild_id": GID},
                  summary={"members": 3, "tokens": 1, "tokens_skipped": 0},
                  report={"output": {"path": path, "size": None},
                          "summary": {"members": 3, "tokens": 1, "tokens_skipped": 0}})
    return repo.insert_run(run)


def _saved(tmp_path, body=b'{"members": []}'):
    path = tmp_path / "scrapes" / GID / f"scrape_{GID}_20261016_101010.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(body)
    return f"{GID}/{path.name}", path


def test_route_serves_the_scrape_file(http):
    tc, repo, tmp_path = http
    rel, _path = _saved(tmp_path)
    rid = _finished_run(repo, rel)
    info = tc.get(f"/api/tasks/scrape-members/{rid}")
    assert info.status_code == 200
    assert info.json() == {"file": {"filename": f"scrape_{GID}_20261016_101010.json", "path": rel,
                                    "size": 15, "members": 3}}
    resp = tc.get(f"/api/tasks/scrape-members/{rid}/file")
    assert resp.status_code == 200 and resp.content == b'{"members": []}'
    assert resp.headers["content-type"].startswith("application/json")
    assert f'filename="scrape_{GID}_20261016_101010.json"' in resp.headers["content-disposition"]


def test_route_unknown_run_is_404(http):
    tc, repo, tmp_path = http
    rel, _path = _saved(tmp_path)
    other = _finished_run(repo, rel, type_key="export_dms")
    for rid in ("0" * 32, "nope", other):
        for suffix in ("", "/file"):
            resp = tc.get(f"/api/tasks/scrape-members/{rid}{suffix}")
            assert resp.status_code == 404 and resp.json() == {"detail": "Unknown run"}


def test_route_missing_file_is_404(http):
    tc, repo, tmp_path = http
    rel, path = _saved(tmp_path)
    rid = _finished_run(repo, rel)
    path.unlink()
    for suffix in ("", "/file"):
        resp = tc.get(f"/api/tasks/scrape-members/{rid}{suffix}")
        assert resp.status_code == 404 and resp.json() == {"detail": "File no longer on disk"}
    no_file = _finished_run(repo, None)
    resp = tc.get(f"/api/tasks/scrape-members/{no_file}/file")
    assert resp.status_code == 404 and resp.json() == {"detail": "No scrape file"}


def test_route_never_serves_outside_the_scrapes_dir(http):
    tc, repo, tmp_path = http
    _saved(tmp_path)
    secret = tmp_path / "secret.json"
    secret.write_text("{}")
    for bad in ("../secret.json", f"{GID}/../../secret.json", str(secret), "/etc/passwd", ".", "",
                f"{GID}/../../../../etc/passwd"):
        rid = _finished_run(repo, bad)
        for suffix in ("", "/file"):
            resp = tc.get(f"/api/tasks/scrape-members/{rid}{suffix}")
            assert resp.status_code == 404 and resp.json() == {"detail": "No scrape file"}, bad


def test_route_needs_a_session(http):
    tc, repo, tmp_path = http
    rel, _path = _saved(tmp_path)
    rid = _finished_run(repo, rel)
    tc.headers.pop("cookie")
    assert tc.get(f"/api/tasks/scrape-members/{rid}/file", follow_redirects=False).status_code == 401


def test_start_does_not_need_the_bots(http):
    tc, repo, tmp_path = http

    async def bots_down():
        return False

    task_routes.set_bots_check(bots_down)
    resp = tc.post("/api/tasks/runs", json={"type": "scrape_members", "inputs": {"guild_id": "abc"}})
    assert resp.status_code == 422
    resp = tc.post("/api/tasks/runs", json={"type": "scrape_members", "inputs": {"guild_id": GID}})
    assert resp.status_code == 201


# the scraper settings routes that stay (tokens, proxies) and the ones that went

def test_old_token_routes_are_gone(http):
    tc, _repo, _tmp = http
    assert tc.post("/api/scraper/tokens/add", data={"token_value": "x"}).status_code in (404, 405)
    assert tc.post("/api/scraper/tokens/id1/update", data={"label": "x"}).status_code in (404, 405)
    assert tc.post("/api/scraper/tokens/id1/validate").status_code in (404, 405)
    assert tc.delete("/api/scraper/tokens/id1").status_code in (404, 405)


@pytest.mark.parametrize("method,path", [
    ("post", "/api/scraper/validate-setup"),
    ("post", "/api/scraper/start"),
    ("post", "/api/scraper/queue/add"),
    ("get", "/api/scraper/queue"),
    ("delete", "/api/scraper/queue/1"),
    ("post", "/api/scraper/queue/clear"),
    ("post", "/api/scraper/queue/start"),
    ("get", "/api/scraper/status"),
    ("post", "/api/scraper/cancel"),
    ("post", "/api/scraper/cancel-all"),
    ("get", "/api/scraper/results"),
    ("post", "/api/scraper/logs/clear"),
])
def test_old_scraper_routes_are_gone(http, method, path):
    tc, _repo, _tmp = http
    assert getattr(tc, method)(path).status_code in (404, 405)
    assert not hasattr(app_module, "scraper_state") and not hasattr(app_module, "_run_single_scrape")
    assert not hasattr(app_module, "_scraper_token_access")
