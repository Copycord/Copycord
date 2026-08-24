"""
Mirroring a host author's profile picture onto the sending account.

Unlike nickname and roles, an avatar is set on the ACCOUNT, not per guild, so
it changes how that token looks everywhere. The hash Discord embeds in the
avatar URL is what makes "has it changed?" free.
"""
import pytest

from server.token_avatar import (
    avatar_hash_from_url,
    to_data_uri,
)

HASH = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
URL = f"https://cdn.discordapp.com/avatars/429748829756325889/{HASH}.png"


class TestAvatarHash:
    def test_reads_the_hash_out_of_a_cdn_url(self):
        # No download needed: an unchanged avatar must cost nothing.
        assert avatar_hash_from_url(URL) == HASH

    def test_handles_query_strings_and_sizes(self):
        assert avatar_hash_from_url(f"{URL}?size=128") == HASH
        assert avatar_hash_from_url(URL.replace(".png", ".webp")) == HASH

    def test_reads_an_animated_hash(self):
        animated = URL.replace(HASH, f"a_{HASH}").replace(".png", ".gif")
        assert avatar_hash_from_url(animated) == f"a_{HASH}"

    def test_unrecognised_urls_yield_nothing(self):
        # A default avatar or an odd CDN path must read as "nothing to do"
        # rather than as a change, or it would re-upload on every message.
        assert avatar_hash_from_url(None) is None
        assert avatar_hash_from_url("") is None
        assert avatar_hash_from_url("https://example.com/pic.png") is None
        assert (
            avatar_hash_from_url("https://cdn.discordapp.com/embed/avatars/3.png")
            is None
        )

    def test_two_avatars_are_distinguishable(self):
        other = URL.replace(HASH, "ffffffffffffffffffffffffffffffff")
        assert avatar_hash_from_url(other) != avatar_hash_from_url(URL)


class TestDataUri:
    def test_encodes_the_way_discord_expects(self):
        assert to_data_uri(b"hello", "image/png") == (
            "data:image/png;base64,aGVsbG8="
        )

    def test_carries_the_real_content_type(self):
        assert to_data_uri(b"x", "image/gif").startswith("data:image/gif;base64,")


class _Resp:
    def __init__(self, status, data=b"", ctype="image/png"):
        self.status = status
        self._data = data

    async def read(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, resp):
        self._resp = resp

    def get(self, url, **kw):
        return self._resp


class TestFetchAvatar:
    @pytest.mark.asyncio
    async def test_returns_bytes_and_type(self):
        from server.token_avatar import fetch_avatar

        got = await fetch_avatar(_Session(_Resp(200, b"\x89PNG rest")), URL)
        assert got is not None
        data, ctype = got
        assert data == b"\x89PNG rest"
        assert ctype == "image/png"

    @pytest.mark.asyncio
    async def test_detects_a_gif(self):
        from server.token_avatar import fetch_avatar

        got = await fetch_avatar(_Session(_Resp(200, b"GIF89a rest")), URL)
        assert got[1] == "image/gif"

    @pytest.mark.asyncio
    async def test_a_bad_response_is_not_an_avatar(self):
        from server.token_avatar import fetch_avatar

        assert await fetch_avatar(_Session(_Resp(404)), URL) is None

    @pytest.mark.asyncio
    async def test_empty_body_is_rejected(self):
        from server.token_avatar import fetch_avatar

        assert await fetch_avatar(_Session(_Resp(200, b"")), URL) is None

    @pytest.mark.asyncio
    async def test_oversized_image_is_rejected(self):
        from server.token_avatar import MAX_AVATAR_BYTES, fetch_avatar

        huge = b"\x89PNG" + b"x" * MAX_AVATAR_BYTES
        assert await fetch_avatar(_Session(_Resp(200, huge)), URL) is None


# --- the change-detection and skip logic in the identity manager -------------

import logging  # noqa: E402
import os  # noqa: E402
import tempfile  # noqa: E402

from common.db import DBManager  # noqa: E402
from server.token_identity import TokenIdentityManager  # noqa: E402

H2 = "b" * 32
URL2 = f"https://cdn.discordapp.com/avatars/429748829756325889/{H2}.png"
MAPPING, AUTHOR, CLONE_GUILD = "m1", "author-1", 5000
TOKENS = [{"token_id": "t1", "token_value": "TOK1", "user_id": "900"}]
ON = {"USER_TOKEN_STICKY_AVATAR": True, "USER_TOKEN_STRATEGY": "sticky_author"}


def _db():
    d = DBManager(os.path.join(tempfile.mkdtemp(), "t.db"), init_schema=True)
    d.conn.execute(
        "INSERT INTO guild_mappings (mapping_id, mapping_name, original_guild_id,"
        " cloned_guild_id) VALUES (?,?,?,?)",
        (MAPPING, "Clone-1", 7, CLONE_GUILD),
    )
    d.conn.commit()
    return d


def _manager(db, *, succeed=True):
    import server.token_identity as ti

    m = TokenIdentityManager(
        bot=None, db=db, logger=logging.getLogger("test"), session_provider=lambda: None
    )
    calls = []

    async def fake_set(token, url, **kw):
        calls.append(url)
        return succeed

    ti.set_avatar = fake_set
    return m, calls


async def _prepare(m, url, settings=ON):
    return await m.prepare(
        mapping_id=MAPPING,
        cloned_guild_id=CLONE_GUILD,
        author_id=AUTHOR,
        author_display_name="Bob",
        author_role_ids=[],
        author_avatar_url=url,
        settings=settings,
        tokens=TOKENS,
    )


class TestOnlyChangesWhenTheAuthorDoes:
    @pytest.mark.asyncio
    async def test_sets_it_and_records_the_hash(self):
        db = _db()
        m, calls = _manager(db)
        await _prepare(m, URL)
        assert calls == [URL]
        assert db.get_token_identity(MAPPING, AUTHOR)["applied_avatar_hash"] == HASH

    @pytest.mark.asyncio
    async def test_an_unchanged_avatar_costs_nothing(self):
        db = _db()
        m, calls = _manager(db)
        await _prepare(m, URL)
        calls.clear()
        await _prepare(m, URL)
        assert calls == []

    @pytest.mark.asyncio
    async def test_a_changed_avatar_is_re_uploaded(self):
        db = _db()
        m, calls = _manager(db)
        await _prepare(m, URL)
        calls.clear()
        await _prepare(m, URL2)
        assert calls == [URL2]
        assert db.get_token_identity(MAPPING, AUTHOR)["applied_avatar_hash"] == H2

    @pytest.mark.asyncio
    async def test_disabled_means_the_account_is_never_touched(self):
        db = _db()
        m, calls = _manager(db)
        await _prepare(m, URL, settings={"USER_TOKEN_STICKY_AVATAR": False})
        assert calls == []
        assert (
            db.get_token_identity(MAPPING, AUTHOR) or {}
        ).get("applied_avatar_hash") is None

    @pytest.mark.asyncio
    async def test_an_unrecognised_avatar_url_is_not_a_change(self):
        db = _db()
        m, calls = _manager(db)
        await _prepare(m, "https://example.com/x.png")
        assert calls == []


class TestFailureIsNotRetried:
    @pytest.mark.asyncio
    async def test_one_attempt_per_session_not_per_message(self):
        # A captcha will not clear by trying again, and the hash is never
        # recorded on failure, so without the skip this retries forever.
        db = _db()
        m, calls = _manager(db, succeed=False)
        for _ in range(4):
            await _prepare(m, URL)
        assert len(calls) == 1
        assert "t1" in m._avatar_skip

    @pytest.mark.asyncio
    async def test_a_failed_change_does_not_record_the_hash(self):
        db = _db()
        m, _ = _manager(db, succeed=False)
        await _prepare(m, URL)
        assert (
            db.get_token_identity(MAPPING, AUTHOR) or {}
        ).get("applied_avatar_hash") is None

    @pytest.mark.asyncio
    async def test_clearing_benched_tokens_also_clears_the_avatar_skip(self):
        db = _db()
        m, _ = _manager(db, succeed=False)
        await _prepare(m, URL)
        m.clear_bad_tokens()
        assert not m._avatar_skip
