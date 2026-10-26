"""selfclient.http: rate limits, paginators and per-method headers.

A fake AsyncSession replays canned responses and sleeping advances a fake clock.
"""
import json as _json

import pytest

from copycord.selfclient import http as sc_http
from copycord.selfclient.errors import Forbidden, RateLimited
from copycord.selfclient.http import HTTPClient, MESSAGE_PAGE, _route_key

CHANNEL = "111111111111111111"
GUILD = "999999999999999999"


class _Headers(dict):
    """Case-insensitive header lookup."""

    def get(self, name, default=None):
        for key, value in self.items():
            if key.lower() == name.lower():
                return value
        return default


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, text=None):
        self.status_code = status
        self._body = body
        self.headers = _Headers(headers or {})
        if text is not None:
            self.text = text
        elif body is not None:
            self.text = _json.dumps(body)
        else:
            self.text = ""
        self.content = self.text.encode()

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"unexpected extra request {method} {url}")
        return self.responses.pop(0)

    async def close(self):
        pass


class FakeClock:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def _client(responses, **kw):
    clock = FakeClock()
    session = FakeSession(responses)
    client = HTTPClient(
        "token-abc",
        session=session,
        sleep=clock.sleep,
        clock=clock,
        page_delay=0,
        **kw,
    )
    return client, session, clock


def _msgs(start, count):
    """Newest-first message dicts with descending ids."""
    return [{"id": str(start - i), "content": f"m{start - i}"} for i in range(count)]


# 429 / retry_after


class TestRateLimited:
    @pytest.mark.asyncio
    async def test_429_waits_body_retry_after_then_succeeds(self):
        client, session, clock = _client(
            [
                FakeResponse(429, {"retry_after": 2.5, "global": False}, {"X-RateLimit-Scope": "user"}),
                FakeResponse(200, {"ok": True}),
            ]
        )
        assert await client.get("/users/@me") == {"ok": True}
        assert len(session.calls) == 2
        assert clock.sleeps == [2.5]

    @pytest.mark.asyncio
    async def test_429_every_attempt_raises_rate_limited(self):
        client, session, clock = _client(
            [FakeResponse(429, {"retry_after": 1.0}) for _ in range(3)], max_retries=3
        )
        with pytest.raises(RateLimited) as info:
            await client.get("/users/@me")
        assert info.value.retry_after == 1.0
        assert info.value.status == 429
        assert len(session.calls) == 3
        assert clock.sleeps == [1.0, 1.0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("body", [{"retry_after": 0}, {"retry_after": 0.0, "global": False}, {}])
    async def test_429_without_a_wait_backs_off_from_one_second(self, body):
        client, session, clock = _client(
            [FakeResponse(429, body) for _ in range(5)] + [FakeResponse(200, {"ok": True})],
            max_retries=6,
        )
        assert await client.get("/users/@me") == {"ok": True}
        assert len(session.calls) == 6
        assert len(clock.sleeps) == 5
        for attempt, wait in enumerate(clock.sleeps, start=1):
            base = min(8.0, 2.0 ** (attempt - 1))
            assert base - 1e-6 <= wait <= min(8.0, base * 1.25) + 1e-6, (attempt, wait)
        # 1, 2, 4, then capped at 8.
        assert clock.sleeps[-2:] == pytest.approx([8.0, 8.0])

    @pytest.mark.asyncio
    async def test_429_with_a_real_wait_is_used_as_given(self):
        client, session, clock = _client(
            [FakeResponse(429, {"retry_after": 0.4}), FakeResponse(429, {"retry_after": 12.0}), FakeResponse(200, {})]
        )
        await client.get("/users/@me")
        assert clock.sleeps == pytest.approx([0.4, 12.0])

    @pytest.mark.asyncio
    async def test_global_429_holds_every_route(self):
        client, session, clock = _client(
            [FakeResponse(429, {"retry_after": 3.0, "global": True}), FakeResponse(200, {})],
            max_retries=1,
        )
        with pytest.raises(RateLimited):
            await client.get("/users/@me")
        assert clock.sleeps == []
        # A different route still has to wait out the global limit.
        await client.get(f"/channels/{CHANNEL}")
        assert clock.sleeps == [3.0]


# bucket pacing


class TestBucketPacing:
    @pytest.mark.asyncio
    async def test_empty_bucket_delays_the_next_request_on_that_route(self):
        empty = {
            "X-RateLimit-Bucket": "abc",
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset-After": "1.5",
        }
        client, session, clock = _client(
            [
                FakeResponse(200, [], empty),
                FakeResponse(200, [], {"X-RateLimit-Bucket": "abc", "X-RateLimit-Remaining": "4"}),
                FakeResponse(200, []),
            ]
        )
        path = f"/channels/{CHANNEL}/messages"
        await client.get(path)
        assert clock.sleeps == []
        await client.get(path)
        assert clock.sleeps == [1.5]
        await client.get(path)
        assert clock.sleeps == [1.5]

    @pytest.mark.asyncio
    async def test_bucket_with_requests_left_never_waits(self):
        ok = {"X-RateLimit-Bucket": "abc", "X-RateLimit-Remaining": "3", "X-RateLimit-Reset-After": "9"}
        client, session, clock = _client([FakeResponse(200, [], ok), FakeResponse(200, [], ok)])
        await client.get("/users/@me")
        await client.get("/users/@me")
        assert clock.sleeps == []

    @pytest.mark.asyncio
    async def test_other_routes_are_not_held_by_an_empty_bucket(self):
        empty = {"X-RateLimit-Bucket": "abc", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset-After": "5"}
        client, session, clock = _client([FakeResponse(200, [], empty), FakeResponse(200, {})])
        await client.get(f"/channels/{CHANNEL}/messages")
        await client.get("/users/@me")
        assert clock.sleeps == []

    def test_route_key_keeps_major_ids_and_collapses_the_rest(self):
        key = _route_key("DELETE", f"/channels/{CHANNEL}/messages/222222222222222222")
        assert key == f"DELETE /channels/{CHANNEL}/messages/{{id}}"
        assert _route_key("GET", f"/guilds/{GUILD}/members/333333333333333333") == (
            f"GET /guilds/{GUILD}/members/{{id}}"
        )


# refusals, headers, proxy


class TestRequests:
    @pytest.mark.asyncio
    async def test_403_raises_forbidden_with_discord_code(self):
        client, _, _ = _client([FakeResponse(403, {"code": 50013, "message": "Missing Permissions"})])
        with pytest.raises(Forbidden) as info:
            await client.get(f"/channels/{CHANNEL}/messages")
        assert info.value.status == 403
        assert info.value.code == 50013
        assert info.value.json["message"] == "Missing Permissions"

    @pytest.mark.asyncio
    async def test_get_carries_no_origin_or_content_type(self):
        client, session, _ = _client([FakeResponse(200, {})])
        await client.get("/users/@me")
        headers = session.calls[0]["headers"]
        assert "Origin" not in headers
        assert "Content-Type" not in headers
        assert headers["Authorization"] == "token-abc"
        assert session.calls[0]["method"] == "GET"
        assert session.calls[0]["url"] == "https://discord.com/api/v9/users/@me"

    @pytest.mark.asyncio
    async def test_post_carries_origin_and_content_type(self):
        client, session, _ = _client([FakeResponse(200, {"id": "5"})])
        await client.post("/users/@me/channels", json={"recipients": ["1"]})
        call = session.calls[0]
        assert call["headers"]["Origin"] == "https://discord.com"
        assert call["headers"]["Content-Type"] == "application/json"
        assert call["json"] == {"recipients": ["1"]}

    @pytest.mark.asyncio
    async def test_profile_headers_are_suppressed(self):
        client, session, _ = _client([FakeResponse(200, {})])
        await client.get("/users/@me")
        headers = session.calls[0]["headers"]
        assert headers["sec-fetch-user"] is None
        assert headers["upgrade-insecure-requests"] is None

    @pytest.mark.asyncio
    async def test_proxy_is_passed_through(self):
        client, session, _ = _client([FakeResponse(200, {}), FakeResponse(200, {})], proxy="http://p:1")
        await client.get("/users/@me")
        assert session.calls[0]["proxy"] == "http://p:1"
        client.set_proxy(None)
        await client.get("/users/@me")
        assert "proxy" not in session.calls[1]

    def test_secret_headers_are_redacted(self):
        out = sc_http.redact_request_headers(
            {"Authorization": "token-abc", "X-Super-Properties": "eyJ4", "Accept": "*/*"}
        )
        assert out["Authorization"] == "<redacted, 9 chars>"
        assert "eyJ4" not in out["X-Super-Properties"]
        assert out["Accept"] == "*/*"

    def test_failure_log_redacts_response_secrets(self):
        resp = FakeResponse(403, {"code": 1}, {"Set-Cookie": "__dcfduid=secret", "cf-ray": "abc"})
        dumped = sc_http._safe_headers(resp)
        assert "secret" not in dumped
        assert "cf-ray: abc" in dumped


def _refusals(caplog):
    return [r for r in caplog.records if r.name == "selfclient.http" and "refused" in r.getMessage()]


class TestRefusalLogLevels:
    @pytest.mark.asyncio
    async def test_unknown_channel_warns_once_then_debug(self, caplog):
        from copycord.selfclient.errors import NotFound

        other = "222222222222222222"
        client, _, _ = _client([
            FakeResponse(404, {"code": 10003, "message": "Unknown Channel"}),
            FakeResponse(404, {"code": 10003, "message": "Unknown Channel"}),
        ])
        with caplog.at_level("DEBUG", logger="selfclient.http"):
            with pytest.raises(NotFound):
                await client.get_channel(CHANNEL)
            with pytest.raises(NotFound):
                await client.get_channel(other)
        levels = [r.levelname for r in _refusals(caplog)]
        assert levels == ["WARNING", "DEBUG"]
        assert "full response at debug" in _refusals(caplog)[0].getMessage()
        assert any("full response:" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_login_401_is_not_an_error(self, caplog):
        from copycord.selfclient.errors import LoginFailure

        client, _, _ = _client([FakeResponse(401, {"code": 0, "message": "401: Unauthorized"})])
        with caplog.at_level("DEBUG", logger="selfclient.http"):
            with pytest.raises(LoginFailure):
                await client.login()
        assert not [r for r in caplog.records if r.levelname in ("ERROR", "WARNING")]
        assert [r.levelname for r in _refusals(caplog)] == ["DEBUG"]

    @pytest.mark.asyncio
    async def test_500_still_error(self, caplog):
        from copycord.selfclient.errors import HTTPException

        client, _, _ = _client([FakeResponse(500, {"code": 0, "message": "boom"})])
        with caplog.at_level("DEBUG", logger="selfclient.http"):
            with pytest.raises(HTTPException) as info:
                await client.get_channel(CHANNEL)
        assert info.value.status == 500
        assert [r.levelname for r in _refusals(caplog)] == ["ERROR"]


# messages() paginator


class TestMessagesPaginator:
    @pytest.mark.asyncio
    async def test_pages_backwards_newest_first_until_a_short_page(self):
        page1 = _msgs(1300, MESSAGE_PAGE)
        page2 = _msgs(1200, 50)
        client, session, _ = _client([FakeResponse(200, page1), FakeResponse(200, page2)])

        got = [m["id"] async for m in client.messages(CHANNEL)]

        assert got == [str(i) for i in range(1300, 1150, -1)]
        assert len(session.calls) == 2
        assert session.calls[0]["params"] == {"limit": str(MESSAGE_PAGE)}
        assert session.calls[1]["params"] == {"limit": str(MESSAGE_PAGE), "before": "1201"}

    @pytest.mark.asyncio
    async def test_after_id_is_a_floor_that_stops_the_walk(self):
        client, session, _ = _client([FakeResponse(200, _msgs(1300, MESSAGE_PAGE))])

        got = [m["id"] async for m in client.messages(CHANNEL, after_id=1250)]

        assert got == [str(i) for i in range(1300, 1250, -1)]
        assert "1250" not in got
        assert len(session.calls) == 1

    @pytest.mark.asyncio
    async def test_limit_caps_both_the_request_and_the_yield(self):
        client, session, _ = _client([FakeResponse(200, _msgs(1300, 5))])
        got = [m["id"] async for m in client.messages(CHANNEL, limit=5)]
        assert got == ["1300", "1299", "1298", "1297", "1296"]
        assert session.calls[0]["params"]["limit"] == "5"
        assert len(session.calls) == 1

    @pytest.mark.asyncio
    async def test_unreadable_channel_ends_quietly(self):
        client, _, _ = _client([FakeResponse(403, {"code": 50001, "message": "Missing Access"})])
        assert [m async for m in client.messages(CHANNEL)] == []

    @pytest.mark.asyncio
    async def test_unreadable_channel_raises_when_asked(self):
        client, _, _ = _client([FakeResponse(403, {"code": 50001, "message": "Missing Access"})])
        with pytest.raises(Forbidden):
            _ = [m async for m in client.messages(CHANNEL, raise_on_error=True)]

    @pytest.mark.asyncio
    async def test_referer_names_the_channel_page_when_guild_known(self):
        client, session, _ = _client([FakeResponse(200, [])], guild_id=GUILD)
        _ = [m async for m in client.messages(CHANNEL)]
        assert session.calls[0]["headers"]["Referer"] == (
            f"https://discord.com/channels/{GUILD}/{CHANNEL}"
        )


# archived_threads() paginator


def _threads(ids, stamps):
    return [
        {"id": str(i), "name": f"t{i}", "thread_metadata": {"archive_timestamp": ts}}
        for i, ts in zip(ids, stamps)
    ]


class TestArchivedThreadsPaginator:
    @pytest.mark.asyncio
    async def test_public_list_pages_on_archive_timestamp_until_has_more_false(self):
        page1 = _threads([30, 29], ["2026-03-03T00:00:00+00:00", "2026-03-02T00:00:00+00:00"])
        page2 = _threads([28], ["2026-03-01T00:00:00+00:00"])
        client, session, _ = _client(
            [
                FakeResponse(200, {"threads": page1, "has_more": True}),
                FakeResponse(200, {"threads": page2, "has_more": False}),
            ]
        )

        got = [t["id"] async for t in client.archived_threads(CHANNEL)]

        assert got == ["30", "29", "28"]
        assert len(session.calls) == 2
        assert session.calls[0]["url"].endswith(f"/channels/{CHANNEL}/threads/archived/public")
        assert "before" not in session.calls[0]["params"]
        assert session.calls[1]["params"]["before"] == "2026-03-02T00:00:00+00:00"

    @pytest.mark.asyncio
    async def test_joined_list_pages_on_thread_id(self):
        page1 = _threads([50, 40], ["x", "y"])
        page2 = _threads([30], ["z"])
        client, session, _ = _client(
            [
                FakeResponse(200, {"threads": page1, "has_more": True}),
                FakeResponse(200, {"threads": page2, "has_more": False}),
            ]
        )

        got = [t["id"] async for t in client.archived_threads(CHANNEL, private=True, joined=True)]

        assert got == ["50", "40", "30"]
        assert session.calls[0]["url"].endswith(
            f"/channels/{CHANNEL}/users/@me/threads/archived/private"
        )
        assert session.calls[1]["params"]["before"] == "40"

    @pytest.mark.asyncio
    async def test_full_page_without_has_more_is_the_last(self):
        page = _threads(range(100, 0, -1), ["2026-01-01T00:00:00+00:00"] * 100)
        client, session, _ = _client([FakeResponse(200, {"threads": page, "has_more": False})])
        got = [t async for t in client.archived_threads(CHANNEL)]
        assert len(got) == 100
        assert len(session.calls) == 1

    @pytest.mark.asyncio
    async def test_missing_cursor_stops_instead_of_looping(self):
        page = [{"id": "1", "thread_metadata": {}}]
        client, session, _ = _client([FakeResponse(200, {"threads": page, "has_more": True})])
        got = [t["id"] async for t in client.archived_threads(CHANNEL)]
        assert got == ["1"]
        assert len(session.calls) == 1

    @pytest.mark.asyncio
    async def test_private_refusal_ends_quietly(self):
        client, _, _ = _client([FakeResponse(403, {"code": 50013, "message": "Missing Permissions"})])
        assert [t async for t in client.archived_threads(CHANNEL, private=True)] == []
