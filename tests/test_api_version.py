"""
Every Discord call goes to API v9.

v9 is what the web client uses, and the whole point of the Chrome fingerprint
is that our traffic looks like that client's. A request identifying as Chrome
and carrying web super-properties, sent to v10, contradicts itself -- and the
codebase had drifted into using both, four call sites on v10 and two on v9.

discord.py-self is already on v9 (INTERNAL_API_VERSION), so this pins our own
code to the same version the library and the browser both use.
"""
import pathlib
import re

CODE = pathlib.Path(__file__).resolve().parent.parent / "code"

# discord.com/api/vN in any Python source under code/.
_API_VERSION_RE = re.compile(r"discord\.com/api/v(\d+)")
# The gateway carries its version as a query parameter instead.
_GATEWAY_VERSION_RE = re.compile(r"gateway\.discord\.gg/\?v=(\d+)")


def _python_sources():
    return [p for p in CODE.rglob("*.py") if "__pycache__" not in p.parts]


def _hits(pattern):
    found = []
    for path in _python_sources():
        text = path.read_text(encoding="utf-8", errors="replace")
        for m in pattern.finditer(text):
            line = text[: m.start()].count("\n") + 1
            found.append((path.relative_to(CODE).as_posix(), line, m.group(1)))
    return found


class TestEveryCallUsesV9:
    def test_no_rest_call_targets_another_version(self):
        wrong = [h for h in _hits(_API_VERSION_RE) if h[2] != "9"]
        assert not wrong, f"non-v9 REST bases: {wrong}"

    def test_no_gateway_url_targets_another_version(self):
        wrong = [h for h in _hits(_GATEWAY_VERSION_RE) if h[2] != "9"]
        assert not wrong, f"non-v9 gateway URLs: {wrong}"

    def test_the_pin_is_actually_watching_something(self):
        # A guard that matches nothing passes forever. If the URLs are ever
        # built some other way, this fails and asks for the pin to be updated
        # rather than quietly protecting an empty set.
        assert _hits(_API_VERSION_RE), "no discord.com/api/vN found to check"
        assert _hits(_GATEWAY_VERSION_RE), "no gateway URL found to check"


def test_the_library_agrees():
    # If discord.py-self ever moves to v10, the client would identify on a
    # different version than the token senders use, which is the split this
    # pin exists to prevent.
    import pytest

    http = pytest.importorskip("discord.http")
    version = getattr(http, "INTERNAL_API_VERSION", None)
    if version is None:
        pytest.skip("library does not expose INTERNAL_API_VERSION")
    assert int(version) == 9, f"discord library is on v{version}, we are on v9"
