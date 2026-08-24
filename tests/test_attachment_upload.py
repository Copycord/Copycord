"""
The client's attachment upload flow.

A real desktop client never posts file bytes inline. It reserves an upload
slot, PUTs the bytes to the signed URL it gets back, then posts a message
referencing the stored file. Captured 2026-08-21.
"""
import logging

import pytest

from server.token_sender import SEND_OK, SEND_UNDELIVERABLE, UserTokenSender

FILES = [("image.png", b"x" * 7671, "image/png")]


class _DB:
    def get_enabled_mapping_tokens(self, mapping_id):
        return [{"token_id": "t1", "token_value": "TOK"}]

    def increment_mapping_token_usage(self, tid):
        return None

    def get_cloned_emoji_ids(self, guild_id):
        return set()


def _sender(*, slots_ok=True, put_ok=True):
    s = UserTokenSender(
        db=_DB(),
        ratelimit=None,
        action_type=None,
        session_provider=lambda: None,
        logger=logging.getLogger("test"),
    )
    calls = {"reserve": None, "put": None, "message": None, "multipart": False}

    async def fake_request(token, url, *, json_body=None, mime_factory=None,
                           method="POST", **kw):
        if url.endswith("/attachments"):
            calls["reserve"] = json_body
            if not slots_ok:
                return SEND_UNDELIVERABLE, None
            return SEND_OK, {
                "attachments": [
                    {
                        "id": 0,
                        "upload_url": "https://storage/signed?sig=1",
                        "upload_filename": "0ecafebe-72dc/image.png",
                    }
                ]
            }
        if mime_factory is not None:
            calls["multipart"] = True
            mime_factory()
        calls["message"] = json_body
        return SEND_OK, {"id": "555"}

    class _Session:
        async def request(self, method, url, **kw):
            calls["put"] = {
                "method": method,
                "url": url,
                "len": len(kw.get("data") or b""),
                "headers": kw.get("headers") or {},
            }

            class _R:
                status_code = 200 if put_ok else 500

            return _R()

    async def fake_tls(token, *, use_proxy=False):
        return _Session()

    async def fake_prepare(session, attachments):
        return FILES, set()

    s._request_with_token = fake_request
    s._tls_session = fake_tls
    s._prepare_files = fake_prepare
    return s, calls


async def _send(s):
    return await s._send_with_token(
        "TOK", 42, "lol", [{"url": "http://x/image.png"}], guild_id=7
    )


class TestUploadFlow:
    @pytest.mark.asyncio
    async def test_reserves_an_upload_slot_for_each_file(self):
        s, calls = _sender()
        await _send(s)
        assert calls["reserve"] == {
            "files": [
                {
                    "filename": "image.png",
                    "file_size": 7671,
                    "id": "0",
                    "is_clip": False,
                    "original_content_type": "image/png",
                }
            ]
        }

    @pytest.mark.asyncio
    async def test_puts_the_bytes_to_the_signed_url(self):
        s, calls = _sender()
        await _send(s)
        assert calls["put"]["method"] == "PUT"
        assert calls["put"]["url"].startswith("https://storage/")
        assert calls["put"]["len"] == 7671
        assert calls["put"]["headers"].get("Content-Type") == "image/png"

    @pytest.mark.asyncio
    async def test_never_sends_the_token_to_storage(self):
        # The signed URL carries its own authorisation. Attaching the account
        # token would hand a credential to a third party.
        s, calls = _sender()
        await _send(s)
        assert not any(k.lower() == "authorization" for k in calls["put"]["headers"])

    @pytest.mark.asyncio
    async def test_message_references_the_upload_not_the_bytes(self):
        s, calls = _sender()
        await _send(s)
        assert calls["message"]["attachments"] == [
            {
                "id": "0",
                "filename": "image.png",
                # Discord answers with upload_filename and wants it back as
                # uploaded_filename. The names really do differ.
                "uploaded_filename": "0ecafebe-72dc/image.png",
                "original_content_type": "image/png",
            }
        ]
        assert list(calls["message"]) == [
            "mobile_network_type",
            "content",
            "nonce",
            "tts",
            "flags",
            "attachments",
        ]
        assert calls["multipart"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure", ["slots", "put"])
    async def test_falls_back_to_inline_when_the_flow_breaks(self, failure):
        # A delivered message beats a faithful failure.
        s, calls = _sender(
            slots_ok=failure != "slots", put_ok=failure != "put"
        )
        status, _mid = await _send(s)
        assert status == SEND_OK
        assert calls["multipart"] is True

    @pytest.mark.asyncio
    async def test_a_message_without_files_never_touches_the_upload_endpoints(self):
        s, calls = _sender()

        async def no_files(session, attachments):
            return [], set()

        s._prepare_files = no_files
        await s._send_with_token("TOK", 42, "plain", [], guild_id=7)
        assert calls["reserve"] is None
        assert calls["put"] is None
