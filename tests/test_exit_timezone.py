"""Unit tests for the exit-IP timezone resolver. The HTTP call is faked."""

import asyncio

import pytest

from copycord.common import exit_timezone as et
from copycord.common import selfbot_headers as sh

PROXY_A = "http://user:secret@10.0.0.1:8000"
PROXY_B = "http://user:secret@10.0.0.2:8000"


def _fake(monkeypatch, answers):
    """answers: proxy -> dict or Exception. Returns the list of proxies hit."""
    calls = []

    async def fetch(proxy, timeout):
        calls.append(proxy)
        a = answers[proxy]
        if isinstance(a, Exception):
            raise a
        return a

    monkeypatch.setattr(et, "_fetch_json", fetch)
    return calls


def run(coro):
    return asyncio.run(coro)


class TestResolve:
    def test_returns_the_reported_zone(self, monkeypatch):
        _fake(monkeypatch, {None: {"timezone": "Europe/Berlin"}})
        assert run(et.resolve_timezone(None)) == "Europe/Berlin"

    def test_none_on_error(self, monkeypatch):
        _fake(monkeypatch, {PROXY_A: OSError("boom")})
        assert run(et.resolve_timezone(PROXY_A)) is None

    @pytest.mark.parametrize("bad", ["Not/AReal_Zone", "", None, 5, "../etc/passwd"])
    def test_none_on_invalid_zone(self, monkeypatch, bad):
        _fake(monkeypatch, {None: {"timezone": bad}})
        assert run(et.resolve_timezone(None)) is None

    def test_none_on_non_dict_body(self, monkeypatch):
        _fake(monkeypatch, {None: ["x"]})
        assert run(et.resolve_timezone(None)) is None


class TestCache:
    def test_resolved_once_per_exit(self, monkeypatch):
        calls = _fake(monkeypatch, {PROXY_A: {"timezone": "Asia/Tokyo"}})

        async def go():
            return [await et.ensure_timezone(PROXY_A) for _ in range(3)]

        assert run(go()) == ["Asia/Tokyo"] * 3
        assert calls == [PROXY_A]

    def test_failure_falls_back_warns_once_and_is_not_retried_immediately(
        self, monkeypatch, caplog
    ):
        calls = _fake(monkeypatch, {None: OSError("down")})

        async def go():
            return [await et.ensure_timezone(None) for _ in range(3)]

        with caplog.at_level("WARNING", logger="common.exit_timezone"):
            assert run(go()) == [et.FALLBACK_TIMEZONE] * 3
        assert calls == [None]
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1

    def test_failure_is_retried_after_the_window(self, monkeypatch):
        calls = _fake(monkeypatch, {None: OSError("down")})
        monkeypatch.setattr(et, "RETRY_AFTER", -1.0)

        async def go():
            await et.ensure_timezone(None)
            await et.ensure_timezone(None)

        run(go())
        assert calls == [None, None]

    def test_logs_never_contain_proxy_credentials(self, monkeypatch, caplog):
        _fake(monkeypatch, {PROXY_A: OSError("down")})
        with caplog.at_level("DEBUG", logger="common.exit_timezone"):
            run(et.ensure_timezone(PROXY_A))
        assert "secret" not in caplog.text
        assert "10.0.0.1" in caplog.text

    @pytest.mark.parametrize(
        "exc, reason",
        [
            (asyncio.TimeoutError(), "no answer within 6s"),
            (OSError("connection refused"), "OSError: connection refused"),
            (OSError(f"cannot connect via {PROXY_A}"), "OSError: cannot connect via http://"),
        ],
    )
    def test_warning_names_the_reason_without_credentials(self, monkeypatch, caplog, exc, reason):
        _fake(monkeypatch, {PROXY_A: exc})
        with caplog.at_level("WARNING", logger="common.exit_timezone"):
            run(et.ensure_timezone(PROXY_A))
        assert reason in caplog.text
        assert "secret" not in caplog.text and "user:" not in caplog.text

    def test_warning_names_an_invalid_zone_answer(self, monkeypatch, caplog):
        _fake(monkeypatch, {None: {"timezone": "Not/AReal_Zone"}})
        with caplog.at_level("WARNING", logger="common.exit_timezone"):
            run(et.ensure_timezone(None))
        assert "no valid timezone in the answer ('Not/AReal_Zone')" in caplog.text


class TestHeaders:
    def test_unresolved_token_uses_the_fallback(self):
        assert sh.build_headers("t")["X-Discord-Timezone"] == sh.TIMEZONE

    def test_header_follows_the_tokens_proxy(self, monkeypatch):
        _fake(
            monkeypatch,
            {
                PROXY_A: {"timezone": "Asia/Tokyo"},
                PROXY_B: {"timezone": "Europe/Paris"},
            },
        )

        async def go():
            await et.note_token_proxy("tok", PROXY_A)
            first = sh.build_headers("tok")["X-Discord-Timezone"]
            await et.note_token_proxy("tok", PROXY_B)
            second = sh.build_headers("tok")["X-Discord-Timezone"]
            return first, second

        assert run(go()) == ("Asia/Tokyo", "Europe/Paris")

    def test_tokens_on_different_exits_differ(self, monkeypatch):
        _fake(
            monkeypatch,
            {PROXY_A: {"timezone": "Asia/Tokyo"}, None: {"timezone": "Europe/Berlin"}},
        )

        async def go():
            await et.note_token_proxy("a", PROXY_A)
            await et.note_token_proxy("b", None)

        run(go())
        assert sh.build_headers("a")["X-Discord-Timezone"] == "Asia/Tokyo"
        assert sh.build_headers("b")["X-Discord-Timezone"] == "Europe/Berlin"

    def test_forget_token_returns_to_fallback(self, monkeypatch):
        _fake(monkeypatch, {PROXY_A: {"timezone": "Asia/Tokyo"}})
        run(et.note_token_proxy("tok", PROXY_A))
        et.forget_token("tok")
        assert sh.build_headers("tok")["X-Discord-Timezone"] == sh.TIMEZONE
