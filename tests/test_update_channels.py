"""
Tests for the update channels of `copycord update` (code/copycord/update.py):
the saved channel in <data dir>/update.json, the install command per channel
and installer, --tag, the npm requirement of the dev channel and --check
against faked PyPI and GitHub responses. No network, no real uv/pipx/npm.
"""
import io
import json
import subprocess

import pytest

from copycord import __main__ as cli
from copycord import service, update

GIT_SPEC = "copycord @ git+https://github.com/Copycord/Copycord@{}"


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
# command per channel x installer
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind,channel,expected",
    [
        (update.UV_TOOL, update.Channel("stable"),
         ["uv", "tool", "install", "--force", "copycord"]),
        (update.UV_TOOL, update.Channel("beta"),
         ["uv", "tool", "install", "--force", "--prerelease", "allow", "copycord"]),
        (update.UV_TOOL, update.Channel("dev", "main"),
         ["uv", "tool", "install", "--force", GIT_SPEC.format("main")]),
        (update.UV_TOOL, update.Channel("dev", "v4-dev"),
         ["uv", "tool", "install", "--force", GIT_SPEC.format("v4-dev")]),
        (update.PIPX, update.Channel("stable"),
         ["pipx", "install", "--force", "copycord"]),
        (update.PIPX, update.Channel("beta"),
         ["pipx", "install", "--force", "--pip-args=--pre", "copycord"]),
        (update.PIPX, update.Channel("dev", "main"),
         ["pipx", "install", "--force", GIT_SPEC.format("main")]),
    ],
)
def test_install_command(kind, channel, expected):
    cmd, hint = update.install_command(kind, channel)
    assert cmd == expected
    assert hint.startswith(expected[0])


@pytest.mark.parametrize(
    "channel,flag,spec",
    [
        (update.Channel("stable"), "", '"copycord"'),
        (update.Channel("beta"), " --pre", '"copycord"'),
        (update.Channel("dev", "main"), "", f'"{GIT_SPEC.format("main")}"'),
    ],
)
def test_pip_prints_command_per_channel(channel, flag, spec):
    cmd, hint = update.install_command(update.PIP, channel)
    assert cmd is None
    assert f"-m pip install --upgrade{flag} {spec}" in hint


def test_uv_pip_beta_hint():
    cmd, hint = update.install_command(update.UV_PIP, update.Channel("beta"))
    assert cmd is None and "uv pip install --upgrade --prerelease allow" in hint


@pytest.mark.parametrize(
    "kind,expected",
    [
        (update.UV_TOOL, ["uv", "tool", "install", "--force", "copycord==4.0.0b1"]),
        (update.PIPX, ["pipx", "install", "--force", "copycord==4.0.0b1"]),
    ],
)
@pytest.mark.parametrize(
    "channel", [update.Channel("stable"), update.Channel("beta"), update.Channel("dev", "main")]
)
def test_tag_command_pins_the_version(kind, expected, channel):
    assert update.install_command(kind, channel, tag="v4.0.0b1")[0] == expected


# --------------------------------------------------------------------------
# copycord update
# --------------------------------------------------------------------------


class Recorder:
    def __init__(self, rc=0):
        self.calls = []
        self.rc = rc

    def __call__(self, cmd, capture=True):
        self.calls.append(list(cmd))
        return subprocess.CompletedProcess(list(cmd), self.rc, "", "")


@pytest.fixture()
def env(data_dir, monkeypatch):
    """uv tool install, uv and npm on PATH, service not installed."""
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


def test_plain_update_uses_stable_by_default(env, data_dir):
    rec, _, _ = env
    assert cli.main(["update"]) == 0
    assert rec.calls == [["uv", "tool", "install", "--force", "copycord"]]
    assert not (data_dir / "update.json").exists()


def test_channel_switch_is_saved_and_followed(env, data_dir):
    rec, _, _ = env
    assert cli.main(["update", "--channel", "beta"]) == 0
    assert _saved(data_dir) == {"channel": "beta"}
    assert cli.main(["update"]) == 0
    beta = ["uv", "tool", "install", "--force", "--prerelease", "allow", "copycord"]
    assert rec.calls == [beta, beta]


def test_dev_channel_with_branch(env, data_dir):
    rec, _, _ = env
    assert cli.main(["update", "--channel", "dev", "--branch", "v4-dev"]) == 0
    assert rec.calls == [["uv", "tool", "install", "--force", GIT_SPEC.format("v4-dev")]]
    assert _saved(data_dir) == {"channel": "dev", "branch": "v4-dev"}
    # Plain update stays on the saved branch; --branch alone switches it.
    assert cli.main(["update"]) == 0
    assert rec.calls[-1][-1] == GIT_SPEC.format("v4-dev")
    assert cli.main(["update", "--branch", "main"]) == 0
    assert rec.calls[-1][-1] == GIT_SPEC.format("main")
    assert _saved(data_dir) == {"channel": "dev", "branch": "main"}


def test_dev_channel_defaults_to_main(env, data_dir):
    rec, _, _ = env
    assert cli.main(["update", "--channel", "dev"]) == 0
    assert rec.calls == [["uv", "tool", "install", "--force", GIT_SPEC.format("main")]]
    assert _saved(data_dir) == {"channel": "dev", "branch": "main"}


def test_switching_back_to_stable_is_saved(env, data_dir):
    update.save_channel(update.Channel("dev", "main"))
    assert cli.main(["update", "--channel", "stable"]) == 0
    assert _saved(data_dir) == {"channel": "stable"}


def test_tag_does_not_change_the_saved_channel(env, data_dir):
    rec, _, _ = env
    update.save_channel(update.Channel("beta"))
    assert cli.main(["update", "--tag", "3.28.0"]) == 0
    assert rec.calls == [["uv", "tool", "install", "--force", "copycord==3.28.0"]]
    assert _saved(data_dir) == {"channel": "beta"}
    assert update.load_channel() == update.Channel("beta")


def test_tag_without_saved_channel_saves_nothing(env, data_dir):
    assert cli.main(["update", "--tag", "4.0.0b1"]) == 0
    assert not (data_dir / "update.json").exists()


def test_tag_on_dev_channel_needs_no_npm(env, data_dir):
    rec, on_path, _ = env
    on_path.discard("npm")
    update.save_channel(update.Channel("dev", "main"))
    assert cli.main(["update", "--tag", "4.0.0"]) == 0
    assert rec.calls == [["uv", "tool", "install", "--force", "copycord==4.0.0"]]
    assert _saved(data_dir) == {"channel": "dev", "branch": "main"}


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
    assert rec.calls == [["pipx", "install", "--force", "--pip-args=--pre", "copycord"]]


def test_plain_pip_prints_channel_command(env, monkeypatch, data_dir, capsys):
    rec, _, _ = env
    monkeypatch.setattr(update, "detect_installer", lambda: update.PIP)
    assert cli.main(["update", "--channel", "beta"]) == 1
    assert rec.calls == []
    assert '-m pip install --upgrade --pre "copycord"' in capsys.readouterr().out


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

PYPI = {
    "info": {"version": "4.0.0"},
    "releases": {
        "3.28.0": [{"yanked": False}],
        "4.0.0": [{"yanked": False}],
        "4.1.0b2": [{"yanked": False}],
        "4.1.0b1": [{"yanked": False}],
        "4.2.0": [{"yanked": True}],
        "4.3.0": [],
    },
}

GITHUB_HEAD = {
    "sha": "0123456789abcdef0123456789abcdef01234567",
    "commit": {
        "message": "client: fix reconnect\n\nlonger body",
        "committer": {"date": "2026-10-11T12:00:00Z"},
    },
}


@pytest.fixture()
def fake_net(monkeypatch):
    """urlopen serving responses[url]; an Exception value is raised."""
    responses, seen = {}, []

    def urlopen(req, timeout=None):
        seen.append((req.full_url, timeout))
        value = responses.get(req.full_url, OSError(f"no fake for {req.full_url}"))
        if isinstance(value, Exception):
            raise value
        return io.BytesIO(json.dumps(value).encode())

    monkeypatch.setattr(update.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(update, "current_version", lambda: "4.0.0")
    monkeypatch.setattr(service, "run_cmd", lambda *a, **k: pytest.fail("ran a command"))
    return responses, seen


def test_check_stable_ignores_prereleases_and_yanked(data_dir, fake_net, capsys):
    responses, seen = fake_net
    responses[update.PYPI_URL] = PYPI
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "current 4.0.0" in out
    assert "channel stable" in out
    assert "latest  4.0.0" in out
    assert "up to date" in out
    assert seen == [("https://pypi.org/pypi/copycord/json", 5.0)]


def test_check_beta_considers_prereleases(data_dir, fake_net, capsys):
    responses, _ = fake_net
    responses[update.PYPI_URL] = PYPI
    update.save_channel(update.Channel("beta"))
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "channel beta" in out
    assert "latest  4.1.0b2" in out
    assert "update is available" in out


def test_check_with_channel_does_not_save(data_dir, fake_net, capsys):
    responses, _ = fake_net
    responses[update.PYPI_URL] = PYPI
    assert cli.main(["update", "--check", "--channel", "beta"]) == 0
    assert "latest  4.1.0b2" in capsys.readouterr().out
    assert not (data_dir / "update.json").exists()


def test_check_pypi_failure_is_a_warning(data_dir, fake_net, capsys):
    responses, _ = fake_net
    responses[update.PYPI_URL] = OSError("timed out")
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "current 4.0.0" in out and "channel stable" in out
    assert "WARN" in out and "timed out" in out


def test_check_dev_shows_branch_head(data_dir, fake_net, monkeypatch, capsys):
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


def test_check_dev_up_to_date(data_dir, fake_net, monkeypatch, capsys):
    responses, _ = fake_net
    responses["https://api.github.com/repos/Copycord/Copycord/commits/main"] = GITHUB_HEAD
    monkeypatch.setattr(update, "installed_commit", lambda: GITHUB_HEAD["sha"])
    assert cli.main(["update", "--check", "--channel", "dev"]) == 0
    assert "up to date" in capsys.readouterr().out


def test_check_dev_github_failure_is_a_warning(data_dir, fake_net, capsys):
    responses, _ = fake_net
    responses["https://api.github.com/repos/Copycord/Copycord/commits/main"] = OSError(
        "Network is unreachable"
    )
    update.save_channel(update.Channel("dev", "main"))
    assert cli.main(["update", "--check"]) == 0
    out = capsys.readouterr().out
    assert "WARN" in out and "Network is unreachable" in out


def test_check_falls_back_to_info_version(data_dir, fake_net, capsys):
    responses, _ = fake_net
    responses[update.PYPI_URL] = {"info": {"version": "4.0.1"}}
    assert cli.main(["update", "--check"]) == 0
    assert "latest  4.0.1" in capsys.readouterr().out


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
