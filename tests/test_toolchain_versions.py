"""The Node and Python versions must agree across every place that sets them."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))


def _read(name):
    return (ROOT / name).read_text(encoding="utf-8")


def _from_lines():
    return re.findall(r"^FROM\s+(\S+)", _read("Dockerfile"), re.M)


def test_nvmrc_matches_node_runtime_and_dockerfile():
    node = _read(".nvmrc").strip().lstrip("v")
    assert re.fullmatch(r"\d+\.\d+\.\d+", node)
    runtime = _read("src/copycord/node_runtime.py")
    assert re.search(r'^NODE_VERSION = "%s"' % re.escape(node), runtime, re.M)
    images = [f for f in _from_lines() if f.startswith("node:")]
    assert images, "Dockerfile has no node stage"
    for image in images:
        assert image.startswith("node:%s-" % node), image


def test_python_version_matches_dockerfile_and_pyproject():
    py = _read(".python-version").strip()
    assert re.fullmatch(r"\d+\.\d+", py)
    images = [f for f in _from_lines() if f.startswith("python:")]
    assert images, "Dockerfile has no python stage"
    for image in images:
        m = re.fullmatch(r"python:(\d+\.\d+)\.\d+-slim", image)
        assert m, "python base image must be pinned to an exact patch: " + image
        assert m.group(1) == py
    lo, hi = re.search(r'requires-python = ">=([\d.]+),<([\d.]+)"', _read("pyproject.toml")).groups()
    key = lambda v: tuple(int(x) for x in v.split("."))
    assert key(lo) <= key(py) < key(hi)


def test_node_is_not_end_of_life():
    assert int(_read(".nvmrc").strip().lstrip("v").split(".")[0]) >= 22


def test_workflows_read_versions_from_files():
    assert WORKFLOWS
    for wf in WORKFLOWS:
        text = wf.read_text(encoding="utf-8")
        for m in re.finditer(r"^\s*node-version:\s*(\S+)", text, re.M):
            raise AssertionError("%s hardcodes node-version %s" % (wf.name, m.group(1)))
        for m in re.finditer(r"^\s*python-version:\s*(\S+)", text, re.M):
            # Only the test matrix (explicit min and max Python) may list versions.
            assert m.group(1).startswith(("${{", "[")), "%s hardcodes python-version %s" % (wf.name, m.group(1))
        for m in re.finditer(r"node-version-file:\s*(\S+)", text):
            assert m.group(1) == ".nvmrc"
        for m in re.finditer(r"python-version-file:\s*(\S+)", text):
            assert m.group(1) == ".python-version"


def test_entrypoint_has_no_pipefail_under_sh():
    text = _read("entrypoint.sh")
    shebang = text.splitlines()[0]
    if "bash" not in shebang:
        assert "pipefail" not in text
    assert "/bin/sh" in _read("Dockerfile") or "bash" in shebang


def test_dockerfile_runs_as_non_root():
    users = re.findall(r"^USER\s+(\S+)", _read("Dockerfile"), re.M)
    assert users and users[-1] not in ("root", "0")
