"""Tests for the update channels of `copycord update`. No network, no real uv/pipx/npm."""
import io
import json
import os
import subprocess
import urllib.error

import pytest

from copycord import __main__ as cli
from copycord import node_runtime, service, update, winupdate

GIT_SPEC = "git+https://github.com/Copycord/Copycord@{}"
RELEASES = "https://api.github.com/repos/Copycord/Copycord/releases"
LIST_URL = RELEASES + "?per_page=100"


def wheel_url(tag):
    return (
        f"https://github.com/Copycord/Copycord/releases/download/{tag}/"
        f"copycord-{tag.lstrip('v')}-py3-none-any.whl"
    )


def release(tag, prerelease=False, draft=False, wheel=True):
    assets = [{"name": f"copycord-{tag.lstrip('v')}.tar.gz",
               "browser_download_url": wheel_url(tag)[:-len("-py3-none-any.whl")] + ".tar.gz"}]
    if wheel:
        assets.append({"name": wheel_url(tag).rsplit("/", 1)[1],
                       "browser_download_url": wheel_url(tag)})
    return {"tag_name": tag, "prerelease": prerelease, "draft": draft, "assets": assets}


# Newest first, like the API: a draft, two pre-releases, a non-version tag.
RELEASE_LIST = [
    release("v4.2.0", draft=True),
    release("nightly", prerelease=True),
    release("v4.1.0b2", prerelease=True),
    release("v4.1.0b1", prerelease=True),
    release("v4.0.0"),
    release("v3.28.0"),
]


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    d = tmp_path / "data"
    monkeypatch.setenv("DATA_DIR", str(d))
    return d


def _saved(data_dir):
    return json.loads((data_dir / "update.json").read_text(encoding="utf-8"))


# saved channel


def test_default_channel_is_stable(data_dir):
    assert update.load_channel() == update.Channel("stable")
    assert not (data_dir / "update.json").exists()


@pytest.mark.parametrize(
    "channel",
    [update.Channel("stable"), update.Channel("beta"), update.Channel("dev", "feature/x")],
)
def test_saved_channel_round_trip(data_dir, channel):
    path = update.save_channel(channel)
    assert path == data_dir / "update.json"
    assert update.load_channel() == channel


@pytest.mark.parametrize("text", ["not json", "[]", '{"channel": "nightly"}', ""])
def test_bad_saved_channel_falls_back_to_stable(data_dir, text):
    data_dir.mkdir()
    (data_dir / "update.json").write_text(text)
    assert update.load_channel() == update.Channel("stable")


# release resolution (GitHub releases API)


def test_stable_skips_prereleases_and_drafts():
    r = update.latest_from_releases(RELEASE_LIST, prerelease=False)
    assert r == update.Release("v4.0.0", "4.0.0", False, wheel_url("v4.0.0"))


def test_beta_includes_prereleases_but_not_drafts():
    r = update.latest_from_releases(RELEASE_LIST, prerelease=True)
    assert r == update.Release("v4.1.0b2", "4.1.0b2", True, wheel_url("v4.1.0b2"))


def test_beta_takes_a_newer_final_release():
    r = update.latest_from_releases([release("v4.1.0")] + RELEASE_LIST, prerelease=True)
    assert r.tag == "v4.1.0"


def test_drafts_only_is_an_error():
    with pytest.raises(update.ReleaseError, match="no published stable release"):
        update.latest_from_releases([release("v4.0.0", draft=True)], prerelease=False)


def test_missing_wheel_asset_is_a_clear_error():
    # The release still has its sdist; that is not taken for the wheel.
    r = update.latest_from_releases([release("v4.0.0", wheel=False)], prerelease=False)
    assert r.wheel_url is None
    with pytest.raises(update.ReleaseError, match=r"release v4\.0\.0 has no wheel asset"):
        r.wheel()


# command per installer

WHEEL = wheel_url("v4.0.0")


@pytest.mark.parametrize(
    "kind,spec,expected",
    [
        (update.UV_TOOL, WHEEL, ["uv", "tool", "install", "--force", "--python", "3.12", WHEEL]),
        (update.UV_TOOL, GIT_SPEC.format("v4-dev"),
         ["uv", "tool", "install", "--force", "--python", "3.12", GIT_SPEC.format("v4-dev")]),
        (update.PIPX, WHEEL, ["pipx", "install", "--force", WHEEL]),
        (update.PIPX, GIT_SPEC.format("main"),
         ["pipx", "install", "--force", GIT_SPEC.format("main")]),
    ],
)
def test_install_command(kind, spec, expected):
    cmd, hint = update.install_command(kind, spec)
    assert cmd == expected
    assert hint.startswith(expected[0])


def test_git_spec_defaults_to_main():
    assert update.git_spec() == GIT_SPEC.format("main")
    assert update.git_spec("v4-dev") == GIT_SPEC.format("v4-dev")


# fakes


@pytest.fixture()
def fake_net(monkeypatch):
    """urlopen serving responses[url]; an Exception value is raised."""
    responses, seen = {LIST_URL: RELEASE_LIST}, []
    for r in RELEASE_LIST:
        if not r["draft"]:
            responses[f"{RELEASES}/tags/{r['tag_name']}"] = r

    def urlopen(req, timeout=None):
        seen.append((req.full_url, timeout))
        value = responses.get(req.full_url)
        if value is None:
            raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {}, None)
        if isinstance(value, Exception):
            raise value
        return io.BytesIO(json.dumps(value).encode())

    monkeypatch.setattr(update.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    return responses, seen


class Recorder:
    def __init__(self, rc=0):
        self.calls = []
        self.envs = []
        self.rc = rc

    def __call__(self, cmd, capture=True, env=None):
        self.calls.append(list(cmd))
        self.envs.append(env)
        return subprocess.CompletedProcess(list(cmd), self.rc, "", "")


@pytest.fixture()
def env(data_dir, fake_net, monkeypatch):
    """uv tool install, uv and npm on PATH, service not installed, faked GitHub."""
    monkeypatch.setattr(update, "detect_installer", lambda: update.UV_TOOL)
    on_path = {"uv", "pipx", "npm"}
    monkeypatch.setattr(
        update.shutil, "which", lambda name: f"/usr/bin/{name}" if name in on_path else None
    )
    rec = Recorder()
    monkeypatch.setattr(service, "run_cmd", rec)
    restarts = []
    monkeypatch.setattr(service, "restart_service", lambda: restarts.append(1))
    monkeypatch.setattr(
        service, "service_state", lambda: service.State(False, False, "test")
    )
    monkeypatch.setattr(update, "installed_version", lambda: ("4.0.0", "abcdef1234567"))
    # The up-to-date check has its own tests below; here every update installs.
    monkeypatch.setattr(update, "nothing_new", lambda target: (False, None))
    return rec, on_path, restarts


def uv(spec):
    return ["uv", "tool", "install", "--force", "--python", "3.12", spec]


# copycord update


def test_plain_update_installs_latest_stable_wheel(env, data_dir, fake_net):
    rec, _, _ = env
    _, seen = fake_net
    assert cli.main(["update"]) == 0
    assert rec.calls == [uv(wheel_url("v4.0.0"))]
    assert seen == [(LIST_URL, 5.0)]
    assert not (data_dir / "update.json").exists()


def test_channel_switch_is_saved_and_followed(env, data_dir):
    rec, _, _ = env
    assert cli.main(["update", "--channel", "beta"]) == 0
    assert _saved(data_dir) == {"channel": "beta"}
    assert cli.main(["update"]) == 0
    assert rec.calls == [uv(wheel_url("v4.1.0b2"))] * 2


def test_dev_channel_with_branch(env, data_dir, fake_net):
    rec, _, _ = env
    _, seen = fake_net
    assert cli.main(["update", "--channel", "dev", "--ref", "v4-dev"]) == 0
    assert rec.calls == [uv(GIT_SPEC.format("v4-dev"))]
    assert _saved(data_dir) == {"channel": "dev", "branch": "v4-dev"}
    # Plain update stays on the saved branch; --ref alone switches it.
    assert cli.main(["update"]) == 0
    assert rec.calls[-1][-1] == GIT_SPEC.format("v4-dev")
    assert cli.main(["update", "--ref", "main"]) == 0
    assert rec.calls[-1][-1] == GIT_SPEC.format("main")
    assert _saved(data_dir) == {"channel": "dev", "branch": "main"}
    assert seen == []


def test_dev_channel_defaults_to_main(env, data_dir):
    rec, _, _ = env
    assert cli.main(["update", "--channel", "dev"]) == 0
    assert rec.calls == [uv(GIT_SPEC.format("main"))]
    assert _saved(data_dir) == {"channel": "dev", "branch": "main"}


def test_switching_back_to_stable_is_saved(env, data_dir):
    update.save_channel(update.Channel("dev", "main"))
    assert cli.main(["update", "--channel", "stable"]) == 0
    assert _saved(data_dir) == {"channel": "stable"}


@pytest.mark.parametrize("tag", ["3.28.0", "v3.28.0"])
def test_tag_installs_that_release_and_keeps_the_channel(env, data_dir, fake_net, tag):
    rec, _, _ = env
    _, seen = fake_net
    update.save_channel(update.Channel("beta"))
    assert cli.main(["update", "--tag", tag, "--yes"]) == 0
    assert rec.calls == [uv(wheel_url("v3.28.0"))]
    assert seen == [(f"{RELEASES}/tags/v3.28.0", 5.0)]
    assert _saved(data_dir) == {"channel": "beta"}


DOWNGRADE_PROMPT = (
    "Downgrade from v4.0.0 to v3.28.0? Your data is backed up first; a newer "
    "database schema will block the older version until you restore the backup. (y/N)"
)


@pytest.fixture()
def tty(monkeypatch):
    """A terminal whose answers are given per test; the prompts asked are kept."""
    state = {"answers": [], "asked": []}

    def ask(prompt=""):
        state["asked"].append(prompt)
        if not state["answers"]:
            pytest.fail(f"asked {prompt!r}")
        return state["answers"].pop(0)

    monkeypatch.setattr(update.ui, "is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", ask)
    return state


def test_older_tag_asks_and_installs_on_yes(env, tty):
    rec, _, _ = env
    tty["answers"] = ["y"]
    assert cli.main(["update", "--tag", "3.28.0"]) == 0
    assert len(tty["asked"]) == 1 and DOWNGRADE_PROMPT in tty["asked"][0]
    assert rec.calls == [uv(wheel_url("v3.28.0"))]


@pytest.mark.parametrize("answer", ["n", ""])
def test_older_tag_declined_installs_nothing(env, data_dir, tty, answer, capsys):
    rec, _, _ = env
    tty["answers"] = [answer]
    assert cli.main(["update", "--tag", "3.28.0"]) == 1
    assert rec.calls == []
    assert "update cancelled" in capsys.readouterr().err
    assert not (data_dir / "backups").exists()


@pytest.mark.parametrize("flag", ["--yes", "-y"])
def test_yes_skips_the_downgrade_prompt(env, tty, flag):
    rec, _, _ = env
    assert cli.main(["update", "--tag", "3.28.0", flag]) == 0
    assert tty["asked"] == []
    assert rec.calls == [uv(wheel_url("v3.28.0"))]


def test_downgrade_without_a_terminal_refuses(env, monkeypatch, capsys):
    rec, _, _ = env
    monkeypatch.setattr(update.ui, "is_interactive", lambda: False)
    assert cli.main(["update", "--tag", "3.28.0"]) == 1
    assert rec.calls == []
    err = capsys.readouterr().err
    assert "would downgrade from v4.0.0 to v3.28.0" in err
    assert "rerun with --yes" in err


def test_channel_switch_to_an_older_stable_asks(env, tty, monkeypatch):
    rec, _, _ = env
    monkeypatch.setattr(update, "current_version", lambda: "4.1.0b2")
    tty["answers"] = ["n"]
    assert cli.main(["update", "--channel", "stable"]) == 1
    assert "Downgrade from v4.1.0b2 to v4.0.0?" in tty["asked"][0]
    assert rec.calls == []


@pytest.mark.parametrize("argv, prompt", [
    (["update", "--tag", "4.1.0b1"], "Update Copycord now? (Y/n)"),
    (["update", "--channel", "beta"], "Update Copycord now? (Y/n)"),
    (["update", "--tag", "4.0.0"], "Reinstall Copycord 4.0.0? (Y/n)"),
])
def test_newer_or_same_version_asks_once_and_enter_installs(env, tty, argv, prompt):
    rec, _, _ = env
    tty["answers"] = [""]
    assert cli.main(argv) == 0
    assert len(tty["asked"]) == 1 and prompt in tty["asked"][0]
    assert len(rec.calls) == 1


def test_dev_ref_does_not_ask(env, monkeypatch):
    rec, _, _ = env
    # A dev ref has no comparable version, so even a far newer install goes ahead
    # without a terminal and without --yes.
    monkeypatch.setattr(update, "current_version", lambda: "99.0.0")
    monkeypatch.setattr(update.ui, "is_interactive", lambda: False)
    assert cli.main(["update", "--channel", "dev", "--ref", "1a2b3c4"]) == 0
    assert rec.calls == [uv(GIT_SPEC.format("1a2b3c4"))]


def test_tag_without_saved_channel_saves_nothing(env, data_dir):
    assert cli.main(["update", "--tag", "4.1.0b1"]) == 0
    assert not (data_dir / "update.json").exists()


def test_unknown_tag_is_a_clear_error(env, capsys):
    rec, _, _ = env
    assert cli.main(["update", "--tag", "9.9.9"]) == 1
    assert rec.calls == []
    assert "no release v9.9.9" in capsys.readouterr().err


def test_tag_on_dev_channel_needs_no_npm(env, data_dir):
    rec, on_path, _ = env
    on_path.discard("npm")
    update.save_channel(update.Channel("dev", "main"))
    assert cli.main(["update", "--tag", "4.0.0"]) == 0
    assert rec.calls == [uv(wheel_url("v4.0.0"))]
    assert _saved(data_dir) == {"channel": "dev", "branch": "main"}


def test_missing_wheel_asset_stops_the_update(env, fake_net, capsys):
    rec, _, _ = env
    responses, _ = fake_net
    responses[LIST_URL] = [release("v4.0.0", wheel=False)]
    assert cli.main(["update"]) == 1
    assert rec.calls == []
    assert "release v4.0.0 has no wheel asset" in capsys.readouterr().err


def test_network_failure_stops_the_update(env, data_dir, fake_net, capsys):
    rec, _, _ = env
    responses, _ = fake_net
    responses[LIST_URL] = OSError("Network is unreachable")
    assert cli.main(["update", "--channel", "beta"]) == 1
    assert rec.calls == []
    assert "Network is unreachable" in capsys.readouterr().err
    assert not (data_dir / "update.json").exists()


def test_tag_and_channel_are_exclusive(env):
    with pytest.raises(SystemExit):
        cli.main(["update", "--tag", "4.0.0", "--channel", "beta"])


def test_branch_needs_dev_channel(env, capsys):
    rec, _, _ = env
    assert cli.main(["update", "--channel", "beta", "--ref", "main"]) == 2
    assert cli.main(["update", "--ref", "main"]) == 2
    assert rec.calls == []
    assert "dev channel" in capsys.readouterr().err


def test_dev_without_npm_and_no_node_stops(env, data_dir, monkeypatch, capsys):
    rec, on_path, _ = env
    on_path.discard("npm")

    def no_node(env=None):
        raise node_runtime.NodeError("no official Node build for plan9 mips; install Node.js")

    monkeypatch.setattr(node_runtime, "ensure_npm", no_node)
    assert cli.main(["update", "--channel", "dev"]) == 1
    assert rec.calls == []
    assert "plan9 mips" in capsys.readouterr().err
    assert not (data_dir / "update.json").exists()


def test_dev_without_npm_builds_with_the_home_node(env, data_dir, tmp_path, monkeypatch):
    rec, on_path, _ = env
    on_path.discard("npm")
    node_bin = tmp_path / "node" / "node-v22.12.0-linux-x64" / "bin"
    monkeypatch.setattr(node_runtime, "ensure_npm", lambda env=None: node_bin)
    monkeypatch.setenv("PATH", "/usr/bin")
    update.save_channel(update.Channel("dev", "main"))
    assert cli.main(["update"]) == 0
    assert rec.calls == [uv(GIT_SPEC.format("main"))]
    # The uv subprocess gets the Node bin dir first on PATH, so hatch_build.py
    # finds npm.
    path = rec.envs[0]["PATH"].split(os.pathsep)
    assert path == [str(node_bin), "/usr/bin"]


def test_release_update_never_sets_up_node(env, data_dir, monkeypatch):
    rec, on_path, _ = env
    on_path.discard("npm")
    monkeypatch.setattr(node_runtime, "ensure_npm", lambda env=None: pytest.fail("Node set up"))
    assert cli.main(["update"]) == 0
    assert rec.envs == [None]


def test_dev_with_npm_on_path_passes_its_dir(env, data_dir):
    rec, _, _ = env
    assert cli.main(["update", "--channel", "dev"]) == 0
    assert rec.envs[0]["PATH"].split(os.pathsep)[0] == "/usr/bin"


def test_failed_install_does_not_switch_channel(env, data_dir):
    rec, _, _ = env
    rec.rc = 3
    assert cli.main(["update", "--channel", "beta"]) == 3
    assert not (data_dir / "update.json").exists()


def test_channel_switch_restarts_running_service(env, monkeypatch):
    rec, _, restarts = env
    monkeypatch.setattr(service, "service_state", lambda: service.State(True, True, "test"))
    assert cli.main(["update", "--channel", "beta"]) == 0
    assert restarts == [1]


def test_channel_switch_leaves_stopped_service(env, monkeypatch):
    rec, _, restarts = env
    monkeypatch.setattr(service, "service_state", lambda: service.State(True, False, "test"))
    assert cli.main(["update", "--channel", "dev"]) == 0
    assert restarts == []


def test_pipx_beta(env, monkeypatch):
    rec, _, _ = env
    monkeypatch.setattr(update, "detect_installer", lambda: update.PIPX)
    assert cli.main(["update", "--channel", "beta"]) == 0
    assert rec.calls == [["pipx", "install", "--force", wheel_url("v4.1.0b2")]]


@pytest.mark.parametrize("kind", [update.DOCKER, update.SOURCE])
def test_docker_and_source_need_no_network(env, monkeypatch, fake_net, capsys, kind):
    rec, _, _ = env
    _, seen = fake_net
    monkeypatch.setattr(update, "detect_installer", lambda: kind)
    assert cli.main(["update"]) == 1
    assert rec.calls == [] and seen == []
    assert f"installed with {kind}" in capsys.readouterr().out


# --no-install (used by install.sh / install.ps1)


def test_no_install_only_saves_the_channel(env, data_dir, fake_net):
    rec, _, _ = env
    _, seen = fake_net
    assert cli.main(["update", "--channel", "beta", "--no-install"]) == 0
    assert _saved(data_dir) == {"channel": "beta"}
    assert rec.calls == [] and seen == []


def test_no_install_dev_branch(env, data_dir):
    assert cli.main(["update", "--channel", "dev", "--ref", "v4-dev", "--no-install"]) == 0
    assert _saved(data_dir) == {"channel": "dev", "branch": "v4-dev"}


# version


# update status

GITHUB_HEAD = {
    "sha": "0123456789abcdef0123456789abcdef01234567",
    "commit": {
        "message": "client: fix reconnect\n\nlonger body",
        "committer": {"date": "2026-10-11T12:00:00Z"},
    },
}


@pytest.fixture()
def no_install(monkeypatch):
    monkeypatch.setattr(service, "run_cmd", lambda *a, **k: pytest.fail("ran a command"))


def test_check_stable_ignores_prereleases_and_drafts(data_dir, fake_net, no_install, capsys):
    _, seen = fake_net
    assert cli.main(["update", "status"]) == 0
    out = capsys.readouterr().out
    assert "current  4.0.0" in out
    assert "stable channel" in out
    assert "latest   4.0.0 (v4.0.0)" in out
    assert "up to date" in out
    assert seen == [(LIST_URL, 5.0)]


def test_check_beta_considers_prereleases(data_dir, fake_net, no_install, capsys):
    update.save_channel(update.Channel("beta"))
    assert cli.main(["update", "status"]) == 0
    out = capsys.readouterr().out
    assert "beta channel" in out
    assert "latest   4.1.0b2 (v4.1.0b2)" in out
    assert "Update available" in out


def test_check_with_channel_does_not_save(data_dir, fake_net, no_install, capsys):
    assert cli.main(["update", "status", "--channel", "beta"]) == 0
    assert "latest   4.1.0b2" in capsys.readouterr().out
    assert not (data_dir / "update.json").exists()


def test_check_network_failure_is_a_warning(data_dir, fake_net, no_install, capsys):
    responses, _ = fake_net
    responses[LIST_URL] = OSError("timed out")
    assert cli.main(["update", "status"]) == 0
    out = capsys.readouterr().out
    assert "current  4.0.0" in out and "stable channel" in out
    assert "! " in out and "timed out" in out


def test_check_dev_shows_branch_head(data_dir, fake_net, no_install, monkeypatch, capsys):
    responses, seen = fake_net
    url = "https://api.github.com/repos/Copycord/Copycord/commits/v4-dev"
    responses[url] = GITHUB_HEAD
    monkeypatch.setattr(update, "installed_commit", lambda: "fedcba9876543210")
    update.save_channel(update.Channel("dev", "v4-dev"))
    assert cli.main(["update", "status"]) == 0
    out = capsys.readouterr().out
    assert "dev channel · v4-dev" in out
    assert "latest   v4-dev @ 0123456789ab 2026-10-11T12:00:00Z client: fix reconnect" in out
    assert "Update available" in out
    assert seen == [(url, 5.0)]


# copycord update when already up to date

REAL_NOTHING_NEW = update.nothing_new
HEAD_URL = "https://api.github.com/repos/Copycord/Copycord/commits/{}"


@pytest.fixture()
def current(env, fake_net, monkeypatch):
    """env with the real up-to-date check, a wheel install and recorded backups."""
    monkeypatch.setattr(update, "nothing_new", REAL_NOTHING_NEW)
    monkeypatch.setattr(update, "git_source", lambda: None)
    monkeypatch.setattr(update, "installed_commit", lambda: None)
    backups = []
    monkeypatch.setattr(update, "backup_database", lambda skip=False: backups.append(1) or True)
    responses, seen = fake_net
    responses[HEAD_URL.format("v4-dev")] = GITHUB_HEAD
    return env[0], backups, responses, seen


def _on_dev_head(monkeypatch, sha=GITHUB_HEAD["sha"]):
    monkeypatch.setattr(update, "installed_commit", lambda: sha)
    update.save_channel(update.Channel("dev", "v4-dev"))


def test_dev_at_the_branch_head_is_up_to_date(current, monkeypatch, capsys):
    rec, backups, _, seen = current
    _on_dev_head(monkeypatch)
    assert cli.main(["update"]) == 0
    assert rec.calls == [] and backups == []
    assert seen == [(HEAD_URL.format("v4-dev"), 5.0)]
    out = capsys.readouterr().out
    assert "Copycord is up to date (4.0.0, 0123456)" in out
    assert "Updating" not in out


def test_dev_with_a_new_commit_installs(current, monkeypatch):
    rec, backups, _, _ = current
    _on_dev_head(monkeypatch, "f" * 40)
    assert cli.main(["update"]) == 0
    assert rec.calls == [uv(GIT_SPEC.format("v4-dev"))] and backups == [1]


def test_stable_at_the_latest_release_is_up_to_date(current, capsys):
    rec, backups, _, seen = current
    assert cli.main(["update"]) == 0
    assert rec.calls == [] and backups == []
    assert seen == [(LIST_URL, 5.0)]
    assert "Copycord is up to date (4.0.0)" in capsys.readouterr().out


def test_stable_with_a_newer_release_installs_and_asks_github_once(current, monkeypatch):
    rec, _, _, seen = current
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    assert cli.main(["update"]) == 0
    assert rec.calls == [uv(wheel_url("v4.0.0"))]
    assert seen == [(LIST_URL, 5.0)]


@pytest.mark.parametrize("argv", [
    ["update", "--ref", "main"],
    ["update", "--channel", "beta"],
    ["update", "--channel", "stable"],
])
def test_a_channel_switch_installs_even_when_up_to_date(current, monkeypatch, argv):
    rec, backups, _, _ = current
    _on_dev_head(monkeypatch)
    assert cli.main(argv) == 0
    assert len(rec.calls) == 1 and backups == [1]


def test_reinstall_installs_even_when_up_to_date(current, monkeypatch):
    rec, _, _, _ = current
    _on_dev_head(monkeypatch)
    assert update.update(reinstall=True) == 0
    assert rec.calls == [["uv", "tool", "install", "--reinstall", "--force", "--python", "3.12",
                          GIT_SPEC.format("v4-dev")]]


def test_github_failure_in_the_check_installs_anyway(current, monkeypatch, capsys):
    rec, _, responses, _ = current
    _on_dev_head(monkeypatch)
    responses[HEAD_URL.format("v4-dev")] = OSError("Network is unreachable")
    assert cli.main(["update"]) == 0
    assert rec.calls == [uv(GIT_SPEC.format("v4-dev"))]
    err = capsys.readouterr().err
    assert "! " in err and "Network is unreachable" in err


@pytest.mark.parametrize("dev", [True, False])
def test_force_installs_even_when_up_to_date(current, monkeypatch, dev):
    rec, backups, _, _ = current
    if dev:
        _on_dev_head(monkeypatch)
    assert cli.main(["update", "--force"]) == 0
    assert len(rec.calls) == 1 and backups == [1]


# version comparator


def test_version_ordering():
    ordered = [
        "3.28.0",
        "4.0.0.dev1",
        "4.0.0a1",
        "4.0.0b1",
        "4.0.0b2",
        "4.0.0rc1",
        "4.0.0",
        "4.0.0.post1",
        "4.0.0.1",
        "4.0.0.2",
        "4.0.0.10",
        "4.0.1",
        "4.0.1.1",
        "4.10.0",
    ]
    for lower, higher in zip(ordered, ordered[1:]):
        assert update.is_newer(higher, lower), (higher, lower)
        assert not update.is_newer(lower, higher), (lower, higher)


def test_build_number_versions():
    assert update.parse_version("v4.0.0.1") == update.parse_version("4.0.0.1")
    assert not update.is_newer("4.0.0.0", "4.0.0")
    assert not update.is_newer("4.0.0", "4.0.0.0")
    # A v4.0.0b1 install moves up to the first build-number release.
    assert update.is_newer("4.0.0.1", "4.0.0b1")
    assert update.is_older("4.0.0b1", "4.0.0.1")


def test_beta_is_the_prerelease_flag_not_the_tag():
    data = [release("v4.0.0.3", prerelease=True), release("v4.0.0.2"), release("v4.0.0b1")]
    assert update.latest_from_releases(data, prerelease=False).tag == "v4.0.0.2"
    beta = update.latest_from_releases(data, prerelease=True)
    assert beta.tag == "v4.0.0.3" and beta.prerelease
    # A b1 tag published as a full release counts as stable.
    assert update.latest_from_releases([release("v4.0.0b1")], prerelease=False).tag == "v4.0.0b1"


# installed_commit


class _Dist:
    def __init__(self, direct_url):
        self._raw = json.dumps(direct_url)

    def read_text(self, name):
        return self._raw if name == "direct_url.json" else None


def _fake_dist(monkeypatch, direct_url):
    from importlib import metadata

    monkeypatch.setattr(metadata, "distribution", lambda name: _Dist(direct_url))


def test_installed_commit_from_vcs_info(monkeypatch):
    _fake_dist(monkeypatch, {"url": "https://github.com/Copycord/Copycord", "vcs_info": {"vcs": "git", "commit_id": "a" * 40}})
    assert update.installed_commit() == "a" * 40


def test_installed_commit_from_a_local_checkout(monkeypatch, tmp_path):
    monkeypatch.setattr(update, "build_commit", lambda: None)
    repo = tmp_path / "src"
    repo.mkdir()
    git = ["git", "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(git + ["commit", "-q", "--allow-empty", "-m", "x"], check=True)
    sha = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    _fake_dist(monkeypatch, {"url": repo.as_uri(), "dir_info": {}})
    assert update.installed_commit() == sha


def test_installed_commit_is_none_for_a_wheel_without_a_stamp(monkeypatch):
    monkeypatch.setattr(update, "build_commit", lambda: None)
    _fake_dist(monkeypatch, {"url": "https://github.com/Copycord/Copycord/releases/download/v4.0.0/copycord-4.0.0-py3-none-any.whl", "archive_info": {}})
    assert update.installed_commit() is None


def test_installed_commit_from_the_build_stamp_of_a_release_wheel(monkeypatch):
    monkeypatch.setattr(update, "build_commit", lambda: "c" * 40)
    _fake_dist(monkeypatch, {"url": "https://github.com/Copycord/Copycord/releases/download/v4.0.0/copycord-4.0.0-py3-none-any.whl", "archive_info": {}})
    assert update.installed_commit() == "c" * 40


# reinstall (copycord doctor --fix)


def test_reinstall_installs_this_exact_release_again(env, data_dir, fake_net):
    rec, _, _ = env
    _, seen = fake_net
    update.save_channel(update.Channel("beta"))
    assert update.update(reinstall=True) == 0
    assert rec.calls == [["uv", "tool", "install", "--reinstall", "--force", "--python", "3.12",
                          wheel_url("v4.0.0")]]
    assert seen == [(f"{RELEASES}/tags/v4.0.0", 5.0)]
    assert _saved(data_dir) == {"channel": "beta"}


def test_reinstall_on_dev_rebuilds_the_saved_ref(env, data_dir, fake_net):
    rec, _, _ = env
    _, seen = fake_net
    update.save_channel(update.Channel("dev", "v4-dev"))
    assert update.update(reinstall=True) == 0
    assert rec.calls == [["uv", "tool", "install", "--reinstall", "--force", "--python", "3.12",
                          GIT_SPEC.format("v4-dev")]]
    assert seen == []
    assert _saved(data_dir) == {"channel": "dev", "branch": "v4-dev"}


GIT_INSTALL = {
    "url": "https://github.com/Copycord/Copycord",
    "vcs_info": {"vcs": "git", "requested_revision": "v4/doctor-fix", "commit_id": "d" * 40},
}


def test_a_git_install_with_no_saved_channel_reads_as_dev_at_its_ref(data_dir, monkeypatch):
    _fake_dist(monkeypatch, GIT_INSTALL)
    assert update.load_channel() == update.Channel("dev", "v4/doctor-fix")
    assert update.load_channel().describe() == "dev channel · v4/doctor-fix"


def test_a_saved_channel_wins_over_a_git_install(data_dir, monkeypatch):
    _fake_dist(monkeypatch, GIT_INSTALL)
    update.save_channel(update.Channel("beta"))
    assert update.load_channel() == update.Channel("beta")


def test_a_git_install_from_a_fork_reads_as_dev_on_that_repo(data_dir, monkeypatch):
    _fake_dist(monkeypatch, {**GIT_INSTALL, "url": "https://github.com/you/Copycord.git"})
    assert update.load_channel() == update.Channel("dev", "v4/doctor-fix", "you/Copycord")


@pytest.mark.parametrize("saved", [None, update.Channel("stable")])
def test_reinstall_of_a_git_install_builds_the_installed_commit_not_a_release(
    env, data_dir, fake_net, monkeypatch, saved
):
    rec, _, _ = env
    _, seen = fake_net
    _fake_dist(monkeypatch, GIT_INSTALL)
    monkeypatch.setattr(update, "backup_database", lambda skip=False: True)
    if saved:
        update.save_channel(saved)
    assert update.update(reinstall=True) == 0
    assert rec.calls == [["uv", "tool", "install", "--reinstall", "--force", "--python", "3.12",
                          "git+https://github.com/Copycord/Copycord@" + "d" * 40]]
    assert seen == []


def test_reinstall_with_pipx_forces_a_new_venv():
    cmd, _ = update.install_command(update.PIPX, "spec", reinstall=True)
    assert cmd == ["pipx", "install", "--force", "spec"]


# Windows: the install runs after copycord exits

PREFIX = r"C:\Users\u\AppData\Roaming\uv\tools\copycord"


def proc(pid, ppid, name, path=None):
    return {"ProcessId": pid, "ParentProcessId": ppid, "Name": name, "ExecutablePath": path}


SHELL = proc(10, 1, "powershell.exe", r"C:\Windows\powershell.exe")
LAUNCHER = proc(20, 10, "copycord.exe", r"C:\Users\u\.local\bin\copycord.exe")
ME = proc(30, 20, "python.exe", PREFIX + r"\Scripts\python.exe")


@pytest.fixture()
def win(env, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(update.sys, "platform", "win32")
    monkeypatch.setattr(winupdate, "running", lambda: ([r"C:\bin\copycord.exe"], None))
    monkeypatch.setattr(winupdate, "clean_old", lambda home, launchers: calls.append("clean"))

    def move_aside(prefix, launchers, home, failed=None):
        calls.append(("move", launchers))
        return [("src", "dest")]

    monkeypatch.setattr(winupdate, "move_aside", move_aside)
    monkeypatch.setattr(winupdate, "restore", lambda moved: calls.append(("restore", moved)))
    rec = env[0]
    real = rec.__call__

    def run(cmd, capture=True, env=None):
        calls.append("install")
        return real(cmd, capture, env)

    monkeypatch.setattr(service, "run_cmd", run)
    return env, calls


def test_windows_moves_the_locked_files_aside_and_installs_inline(win, capsys):
    (rec, _, restarts), calls = win
    assert update.update() == 0
    assert rec.calls == [uv(wheel_url("v4.0.0"))]
    assert calls == ["clean", ("move", [r"C:\bin\copycord.exe"]), "install"]
    assert restarts == []
    out = capsys.readouterr().out
    assert "Copycord updated to 4.0.0 (abcdef1)" in out
    assert "$ uv" not in out


def test_windows_failed_install_puts_the_files_back(win, monkeypatch, data_dir, capsys):
    (rec, _, _), calls = win
    rec.rc = 2
    assert update.update(channel="beta") == 2
    assert calls[-1] == ("restore", [("src", "dest")])
    assert not update.settings_path().exists()


def test_windows_stops_and_restarts_a_running_service(win, monkeypatch):
    (_, _, restarts), calls = win
    monkeypatch.setattr(service, "service_state", lambda: service.State(True, True, "t"))

    class Svc:
        def stop(self):
            calls.append("stop")

    monkeypatch.setattr(service, "get_service", lambda: Svc())
    assert update.update() == 0
    assert calls.index("stop") < calls.index("install")
    assert restarts == [1]


def test_windows_channel_switch_is_saved(win, data_dir):
    assert update.update(channel="beta") == 0
    assert update.load_channel(update.settings_path()) == update.Channel("beta")


def test_windows_refuses_while_copycord_runs_elsewhere(win, monkeypatch, capsys):
    (rec, _, _), calls = win
    backups = []
    monkeypatch.setattr(update, "backup_database", lambda skip=False: backups.append(1))
    monkeypatch.setattr(winupdate, "running", lambda: ([], 99))
    assert update.update() == 1
    assert rec.calls == [] and calls == [] and backups == []
    assert "stop it first" in capsys.readouterr().err


@pytest.mark.parametrize("platform", ["linux", "darwin"])
def test_other_platforms_install_inline_and_quietly(env, monkeypatch, platform, capsys):
    rec, _, _ = env
    monkeypatch.setattr(update.sys, "platform", platform)
    monkeypatch.setattr(winupdate, "running", lambda: pytest.fail("checked processes"))
    monkeypatch.setattr(winupdate, "move_aside", lambda *a: pytest.fail("moved files"))
    assert update.update() == 0
    assert rec.calls == [uv(wheel_url("v4.0.0"))]
    out = capsys.readouterr().out
    assert "Installing Copycord v4.0.0..." in out
    assert "Copycord updated to 4.0.0 (abcdef1)" in out
    assert "$ uv" not in out


def test_move_aside_renames_executables_and_launchers(tmp_path, monkeypatch):
    env = tmp_path / "tools" / "copycord"
    (env / "Scripts").mkdir(parents=True)
    (env / "Lib").mkdir()
    for name in ("Scripts/python.exe", "Scripts/pythonw.exe", "Lib/x.pyd", "Lib/x.py"):
        (env / name).write_text("x")
    shim = tmp_path / "bin" / "copycord.exe"
    shim.parent.mkdir()
    shim.write_text("x")
    home = tmp_path / "home"
    moved = winupdate.move_aside(str(env), [str(shim)], home)
    assert len(moved) == 4
    assert not (env / "Lib" / "x.pyd").exists() and not shim.exists()
    assert (env / "Lib" / "x.py").exists()
    winupdate.restore(moved)
    assert (env / "Scripts" / "python.exe").exists() and shim.exists()
    winupdate.move_aside(str(env), [str(shim)], home)
    winupdate.clean_old(home, [str(shim)])
    assert not (home / winupdate.ASIDE).exists()


def _fake_env(tmp_path):
    env = tmp_path / "tools" / "copycord"
    (env / "Scripts").mkdir(parents=True)
    (env / "Lib").mkdir()
    for name in ("python.exe", "pythonw.exe", "copycord.exe"):
        (env / "Scripts" / name).write_bytes(b"MZ " + name.encode())
    (env / "Lib" / "x.pyd").write_bytes(b"pyd")
    return env


def test_move_aside_copies_the_interpreters_back(tmp_path):
    env = _fake_env(tmp_path)
    home = tmp_path / "home"
    failed: list = []
    moved = dict(winupdate.move_aside(str(env), [], home, failed))
    assert failed == []
    for name in ("python.exe", "pythonw.exe"):
        src = env / "Scripts" / name
        # uv needs Scripts\python.exe to treat the folder as an environment.
        assert src.read_bytes() == b"MZ " + name.encode()
        assert moved[src].read_bytes() == src.read_bytes()
        assert winupdate.ASIDE in moved[src].parts
    assert not (env / "Scripts" / "copycord.exe").exists()
    assert not (env / "Lib" / "x.pyd").exists()


def test_restore_puts_the_original_interpreters_back(tmp_path):
    env = _fake_env(tmp_path)
    moved = winupdate.move_aside(str(env), [], tmp_path / "home")
    winupdate.restore(moved)
    for src, dest in moved:
        assert src.is_file() and not dest.exists()
    assert (env / "Scripts" / "python.exe").read_bytes() == b"MZ python.exe"
    assert (env / "Lib" / "x.pyd").read_bytes() == b"pyd"


def test_restore_keeps_a_new_interpreter_and_new_files(tmp_path):
    env = _fake_env(tmp_path)
    moved = winupdate.move_aside(str(env), [], tmp_path / "home")
    (env / "Scripts" / "python.exe").write_bytes(b"new")
    (env / "Lib" / "x.pyd").write_bytes(b"new")
    winupdate.restore(moved)
    assert (env / "Scripts" / "python.exe").read_bytes() == b"new"
    assert (env / "Lib" / "x.pyd").read_bytes() == b"new"
    assert (env / "Scripts" / "copycord.exe").exists()


def test_a_failed_copy_back_is_reported_and_does_not_stop(tmp_path, monkeypatch):
    env = _fake_env(tmp_path)

    def copy2(src, dst):
        raise OSError("denied")

    monkeypatch.setattr(winupdate.shutil, "copy2", copy2)
    failed: list = []
    moved = winupdate.move_aside(str(env), [], tmp_path / "home", failed)
    assert len(moved) == 4
    assert len(failed) == 2 and all("(copy back): denied" in f for f in failed)
    assert not (env / "Scripts" / "python.exe").exists()
    winupdate.restore(moved)
    assert (env / "Scripts" / "python.exe").read_bytes() == b"MZ python.exe"


def test_scan_finds_own_chain_and_ignores_the_service():
    rows = [
        SHELL, LAUNCHER, ME,
        proc(40, 2, "svchost.exe"),
        proc(41, 40, "copycord.exe", r"C:\Users\u\.local\bin\copycord.exe"),
        proc(42, 41, "python.exe", PREFIX + r"\Scripts\python.exe"),
    ]
    assert winupdate.scan(rows, 30, PREFIX) == ([30, 20], None)


def test_scan_reports_a_copycord_started_from_a_shell():
    rows = [
        SHELL, LAUNCHER, ME,
        proc(50, 1, "cmd.exe"),
        proc(51, 50, "copycord.exe", r"C:\Users\u\.local\bin\copycord.exe"),
        proc(52, 51, "python.exe", PREFIX + r"\Scripts\python.exe"),
    ]
    assert winupdate.scan(rows, 30, PREFIX)[1] in (51, 52)


def test_scan_ignores_a_copycord_of_another_install():
    rows = [
        SHELL, LAUNCHER, ME,
        proc(60, 1, "cmd.exe"),
        proc(61, 60, "copycord.exe", r"D:\other\bin\copycord.exe"),
        proc(62, 61, "python.exe", r"D:\other\tools\copycord\Scripts\python.exe"),
    ]
    assert winupdate.scan(rows, 30, PREFIX) == ([30, 20], None)


def test_service_leftovers_finds_what_the_task_started():
    BIN = r"C:\Users\u\.local\bin"
    rows = [
        SHELL, LAUNCHER, ME,
        # The service: Task Scheduler, the windowless launcher, its Python.
        proc(40, 2, "svchost.exe"),
        proc(41, 40, "copycordw.exe", BIN + r"\copycordw.exe"),
        proc(42, 41, "pythonw.exe", PREFIX + r"\Scripts\pythonw.exe"),
        # Left behind by schtasks /End: its launcher is gone.
        proc(45, 44, "python.exe", PREFIX + r"\Scripts\python.exe"),
        # The base interpreter the venv's pythonw.exe started, left behind too.
        dict(proc(46, 43, "pythonw.exe", r"C:\Users\u\AppData\Roaming\uv\python\pythonw.exe"),
             CommandLine=f'"{PREFIX}\\Scripts\\pythonw.exe" "{BIN}\\copycordw.exe" run'),
        # copycord run in a terminal and another install: not the service.
        proc(50, 1, "cmd.exe"),
        proc(51, 50, "copycord.exe", BIN + r"\copycord.exe"),
        proc(52, 51, "python.exe", PREFIX + r"\Scripts\python.exe"),
        proc(60, 40, "copycord.exe", r"D:\other\bin\copycord.exe"),
        proc(61, 60, "python.exe", r"D:\other\tools\copycord\Scripts\python.exe"),
    ]
    assert winupdate.service_leftovers(rows, 30, PREFIX, BIN) == [41, 42, 45, 46]
    # This command and its launcher are never in the list.
    assert winupdate.service_leftovers([LAUNCHER, ME], 30, PREFIX, BIN) == []
    # Without the launcher folder only the environment's processes count.
    assert winupdate.service_leftovers(rows, 30, PREFIX) == [42, 45, 46]


def test_running_without_powershell_does_not_block(monkeypatch):
    def boom(*a, **k):
        raise OSError("no powershell")

    monkeypatch.setattr(winupdate.subprocess, "run", boom)
    assert winupdate.running()[1] is None


# copycord update asks before it changes anything


def test_update_shows_the_versions_and_no_changes_nothing(current, tty, monkeypatch, capsys):
    rec, backups, _, _ = current
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    monkeypatch.setattr(update.node_runtime, "ensure_npm",
                        lambda: pytest.fail("npm checked before the answer"))
    tty["answers"] = ["n"]
    assert cli.main(["update"]) == 0
    assert len(tty["asked"]) == 1 and "Update Copycord now? (Y/n)" in tty["asked"][0]
    assert rec.calls == [] and backups == []
    out = capsys.readouterr().out
    assert "Update available" in out and "3.28.0 -> 4.0.0" in out
    assert "Update cancelled, nothing was changed" in out


@pytest.mark.parametrize("answer", ["", "y", "yes"])
def test_update_installs_when_the_answer_is_yes(current, tty, monkeypatch, answer):
    rec, backups, _, _ = current
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    tty["answers"] = [answer]
    assert cli.main(["update"]) == 0
    assert rec.calls == [uv(wheel_url("v4.0.0"))] and backups == [1]


@pytest.mark.parametrize("flag", ["--yes", "-y"])
def test_yes_updates_without_asking(current, tty, monkeypatch, flag):
    rec, backups, _, _ = current
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    assert cli.main(["update", flag]) == 0
    assert tty["asked"] == []
    assert rec.calls == [uv(wheel_url("v4.0.0"))] and backups == [1]


def test_no_terminal_updates_without_asking(current, monkeypatch):
    rec, backups, _, _ = current
    monkeypatch.setattr(update, "current_version", lambda: "3.28.0")
    monkeypatch.setattr(update.ui, "is_interactive", lambda: False)
    monkeypatch.setattr("builtins.input", lambda prompt="": pytest.fail("asked"))
    assert cli.main(["update"]) == 0
    assert rec.calls == [uv(wheel_url("v4.0.0"))] and backups == [1]


def test_auto_update_does_not_ask(monkeypatch):
    from copycord import update_check as uc

    seen = []
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")  # a release; a dev build never auto-updates
    monkeypatch.setattr(uc, "auto_update_enabled", lambda: True)
    monkeypatch.setattr(uc.ui, "is_interactive", lambda: False)
    monkeypatch.setattr(update, "detect_installer", lambda *a, **kw: update.UV_TOOL)
    monkeypatch.setattr(uc, "check_cached", lambda *a, **kw: type(
        "R", (), {"available": True, "dev": False, "latest": "4.0.1"})())
    monkeypatch.setattr(uc, "hint_text", lambda result: "")
    monkeypatch.setattr(update, "update", lambda **kw: seen.append(kw) or 0)
    assert uc.auto_update_once()
    assert seen == [{"yes": True, "auto": True}]


@pytest.mark.parametrize("argv", [["update", "--force"], ["update", "--force", "--channel", "dev"]])
def test_force_on_the_same_version_asks_to_reinstall(current, tty, monkeypatch, argv, capsys):
    rec, backups, _, _ = current
    _on_dev_head(monkeypatch)
    tty["answers"] = ["n"]
    assert cli.main(argv) == 0
    assert len(tty["asked"]) == 1 and "Reinstall Copycord 4.0.0? (Y/n)" in tty["asked"][0]
    assert rec.calls == [] and backups == []
    assert "Update cancelled, nothing was changed" in capsys.readouterr().out


def test_downgrade_asks_once_with_its_own_question(current, tty, monkeypatch):
    rec, backups, _, _ = current
    monkeypatch.setattr(update, "current_version", lambda: "4.1.0b2")
    tty["answers"] = ["y"]
    assert cli.main(["update", "--channel", "stable"]) == 0
    assert len(tty["asked"]) == 1 and "Downgrade from v4.1.0b2 to v4.0.0?" in tty["asked"][0]
    assert len(rec.calls) == 1 and backups == [1]
