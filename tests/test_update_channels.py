"""
Tests for the update channels of `copycord update` (code/copycord/update.py):
the saved channel in <data dir>/update.json, release resolution from faked
GitHub releases API responses (stable skips pre-releases, beta includes them,
drafts ignored, missing wheel asset, network failure), the install command
per installer, --tag, --no-install, the npm requirement of the dev channel
and --check. No network, no real uv/pipx/npm.
"""
import io
import json
import subprocess
import urllib.error

import pytest

from copycord import __main__ as cli
from copycord import service, update

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


# --------------------------------------------------------------------------
# saved channel
# --------------------------------------------------------------------------


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


def test_saved_dev_channel_defaults_to_main(data_dir):
    data_dir.mkdir()
    (data_dir / "update.json").write_text('{"channel": "dev"}')
    assert update.load_channel() == update.Channel("dev", "main")


@pytest.mark.parametrize("text", ["not json", "[]", '{"channel": "nightly"}', ""])
def test_bad_saved_channel_falls_back_to_stable(data_dir, text):
    data_dir.mkdir()
    (data_dir / "update.json").write_text(text)
    assert update.load_channel() == update.Channel("stable")


def test_saved_file_format(data_dir):
    update.save_channel(update.Channel("dev", "v4-dev"))
    assert _saved(data_dir) == {"channel": "dev", "branch": "v4-dev"}
    update.save_channel(update.Channel("beta"))
    assert _saved(data_dir) == {"channel": "beta"}


# --------------------------------------------------------------------------
# release resolution (GitHub releases API)
# --------------------------------------------------------------------------


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


# --------------------------------------------------------------------------
# command per installer
# --------------------------------------------------------------------------

WHEEL = wheel_url("v4.0.0")


@pytest.mark.parametrize(
    "kind,spec,expected",
    [
        (update.UV_TOOL, WHEEL, ["uv", "tool", "install", "--force", WHEEL]),
        (update.UV_TOOL, GIT_SPEC.format("v4-dev"),
         ["uv", "tool", "install", "--force", GIT_SPEC.format("v4-dev")]),
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


def test_pip_prints_command():
    cmd, hint = update.install_command(update.PIP, WHEEL)
    assert cmd is None
    assert f'-m pip install --upgrade "{WHEEL}"' in hint


def test_uv_pip_hint():
    cmd, hint = update.install_command(update.UV_PIP, WHEEL)
    assert cmd is None and f'uv pip install --upgrade --python' in hint and WHEEL in hint


# --------------------------------------------------------------------------
# fakes
# --------------------------------------------------------------------------


@pytest.fixture()
def fake_net(monkeypatch):
    """urlopen serving responses[url] (the releases list and the tags of
    RELEASE_LIST by default); an Exception value is raised."""
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
        self.rc = rc

    def __call__(self, cmd, capture=True):
        self.calls.append(list(cmd))
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
    return rec, on_path, restarts


def uv(spec):
    return ["uv", "tool", "install", "--force", spec]


# --------------------------------------------------------------------------
# copycord update
# --------------------------------------------------------------------------


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
    assert cli.main(["update", "--channel", "dev", "--branch", "v4-dev"]) == 0
    assert rec.calls == [uv(GIT_SPEC.format("v4-dev"))]
    assert _saved(data_dir) == {"channel": "dev", "branch": "v4-dev"}
    # Plain update stays on the saved branch; --branch alone switches it.
    assert cli.main(["update"]) == 0
    assert rec.calls[-1][-1] == GIT_SPEC.format("v4-dev")
    assert cli.main(["update", "--branch", "main"]) == 0
    assert rec.calls[-1][-1] == GIT_SPEC.format("main")
    assert _saved(data_dir) == {"channel": "dev", "branch": "main"}
    assert seen == []  # dev never asks the releases API


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
    assert cli.main(["update", "--tag", tag]) == 0
    assert rec.calls == [uv(wheel_url("v3.28.0"))]
    assert seen == [(f"{RELEASES}/tags/v3.28.0", 5.0)]
    assert _saved(data_dir) == {"channel": "beta"}


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


def test_tag_and_branch_are_exclusive(env, capsys):
    rec, _, _ = env
    assert cli.main(["update", "--tag", "4.0.0", "--branch", "main"]) == 2
    assert rec.calls == []


def test_branch_needs_dev_channel(env, capsys):
    rec, _, _ = env
    assert cli.main(["update", "--channel", "beta", "--branch", "main"]) == 2
    assert cli.main(["update", "--branch", "main"]) == 2  # saved channel is stable
    assert rec.calls == []
    assert "dev channel" in capsys.readouterr().err


def test_unknown_channel_is_rejected(env):
    with pytest.raises(SystemExit):
        cli.main(["update", "--channel", "nightly"])


def test_dev_refuses_without_npm(env, data_dir, capsys):
    rec, on_path, _ = env
    on_path.discard("npm")
    assert cli.main(["update", "--channel", "dev"]) == 1
    assert rec.calls == []
    assert "npm" in capsys.readouterr().err
    assert not (data_dir / "update.json").exists()


def test_saved_dev_refuses_without_npm(env, data_dir):
    rec, on_path, _ = env
    on_path.discard("npm")
    update.save_channel(update.Channel("dev", "main"))
    assert cli.main(["update"]) == 1
    assert rec.calls == []


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


def test_plain_pip_prints_the_wheel_command(env, monkeypatch, capsys):
    rec, _, _ = env
    monkeypatch.setattr(update, "detect_installer", lambda: update.PIP)
    assert cli.main(["update", "--channel", "beta"]) == 1
    assert rec.calls == []
    assert f'-m pip install --upgrade "{wheel_url("v4.1.0b2")}"' in capsys.readouterr().out


@pytest.mark.parametrize("kind", [update.DOCKER, update.SOURCE])
def test_docker_and_source_need_no_network(env, monkeypatch, fake_net, capsys, kind):
    rec, _, _ = env
    _, seen = fake_net
    monkeypatch.setattr(update, "detect_installer", lambda: kind)
    assert cli.main(["update"]) == 1
    assert rec.calls == [] and seen == []
    assert f"installed with {kind}" in capsys.readouterr().out


# --------------------------------------------------------------------------
# --no-install (used by install.sh / install.ps1)
# --------------------------------------------------------------------------


def test_no_install_only_saves_the_channel(env, data_dir, fake_net):
    rec, _, _ = env
    _, seen = fake_net
    assert cli.main(["update", "--channel", "beta", "--no-install"]) == 0
    assert _saved(data_dir) == {"channel": "beta"}
    assert rec.calls == [] and seen == []


def test_no_install_dev_branch(env, data_dir):
    assert cli.main(["update", "--channel", "dev", "--branch", "v4-dev", "--no-install"]) == 0
    assert _saved(data_dir) == {"channel": "dev", "branch": "v4-dev"}


def test_no_install_needs_a_channel(env, data_dir):
    rec, _, _ = env
    assert cli.main(["update", "--no-install"]) == 2
    assert cli.main(["update", "--tag", "4.0.0", "--no-install"]) == 2
    assert rec.calls == []
    assert not (data_dir / "update.json").exists()


# --------------------------------------------------------------------------
# version
# --------------------------------------------------------------------------


def test_version_prints_dev_branch(data_dir, capsys):
    update.save_channel(update.Channel("dev", "v4-dev"))
    assert cli.main(["version"]) == 0
    assert "channel dev (branch v4-dev)" in capsys.readouterr().out


# --------------------------------------------------------------------------
# --check
# --------------------------------------------------------------------------

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
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "current 4.0.0" in out
    assert "channel stable" in out
    assert "latest  4.0.0 (v4.0.0)" in out
    assert "up to date" in out
    assert seen == [(LIST_URL, 5.0)]


def test_check_beta_considers_prereleases(data_dir, fake_net, no_install, capsys):
    update.save_channel(update.Channel("beta"))
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "channel beta" in out
    assert "latest  4.1.0b2 (v4.1.0b2)" in out
    assert "update is available" in out


def test_check_with_channel_does_not_save(data_dir, fake_net, no_install, capsys):
    assert cli.main(["update", "--check", "--channel", "beta"]) == 0
    assert "latest  4.1.0b2" in capsys.readouterr().out
    assert not (data_dir / "update.json").exists()


def test_check_network_failure_is_a_warning(data_dir, fake_net, no_install, capsys):
    responses, _ = fake_net
    responses[LIST_URL] = OSError("timed out")
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "current 4.0.0" in out and "channel stable" in out
    assert "WARN" in out and "timed out" in out


def test_check_no_release_is_a_warning(data_dir, fake_net, no_install, capsys):
    responses, _ = fake_net
    responses[LIST_URL] = [release("v4.0.0", draft=True)]
    assert cli.main(["update", "--check"]) == 0
    assert "WARN  no published stable release" in capsys.readouterr().out


def test_check_warns_about_a_missing_wheel(data_dir, fake_net, no_install, capsys):
    responses, _ = fake_net
    responses[LIST_URL] = [release("v4.0.1", wheel=False)]
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "latest  4.0.1 (v4.0.1)" in out
    assert "WARN  release v4.0.1 has no wheel asset" in out


def test_check_dev_shows_branch_head(data_dir, fake_net, no_install, monkeypatch, capsys):
    responses, seen = fake_net
    url = "https://api.github.com/repos/Copycord/Copycord/commits/v4-dev"
    responses[url] = GITHUB_HEAD
    monkeypatch.setattr(update, "installed_commit", lambda: "fedcba9876543210")
    update.save_channel(update.Channel("dev", "v4-dev"))
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "channel dev (branch v4-dev)" in out
    assert "latest  v4-dev @ 0123456789ab 2026-10-11T12:00:00Z client: fix reconnect" in out
    assert "update is available" in out
    assert seen == [(url, 5.0)]


def test_check_dev_up_to_date(data_dir, fake_net, no_install, monkeypatch, capsys):
    responses, _ = fake_net
    responses["https://api.github.com/repos/Copycord/Copycord/commits/main"] = GITHUB_HEAD
    monkeypatch.setattr(update, "installed_commit", lambda: GITHUB_HEAD["sha"])
    assert cli.main(["update", "--check", "--channel", "dev"]) == 0
    assert "up to date" in capsys.readouterr().out


def test_check_dev_github_failure_is_a_warning(data_dir, fake_net, no_install, capsys):
    responses, _ = fake_net
    responses["https://api.github.com/repos/Copycord/Copycord/commits/main"] = OSError(
        "Network is unreachable"
    )
    update.save_channel(update.Channel("dev", "main"))
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "WARN" in out and "Network is unreachable" in out


# --------------------------------------------------------------------------
# version comparator
# --------------------------------------------------------------------------


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
        "4.0.1",
        "4.10.0",
    ]
    for lower, higher in zip(ordered, ordered[1:]):
        assert update.is_newer(higher, lower), (higher, lower)
        assert not update.is_newer(lower, higher), (lower, higher)


def test_version_equivalents():
    assert not update.is_newer("4.0", "4.0.0")
    assert not update.is_newer("v4.0.0", "4.0.0")
    assert not update.is_newer("4.0.0+local", "4.0.0")


def test_is_prerelease():
    assert update.is_prerelease("4.0.0b1")
    assert update.is_prerelease("4.0.0rc1")
    assert update.is_prerelease("4.0.0.dev2")
    assert not update.is_prerelease("4.0.0")
    assert not update.is_prerelease("4.0.0.post1")
