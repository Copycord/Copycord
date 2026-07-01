# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================

"""
User-token ("self-bot") message sender.
"""

from __future__ import annotations

import asyncio
import json
import random
from typing import Callable, Optional

import aiohttp

from common.selfbot_headers import make_fingerprint

DISCORD_API_BASE = "https://discord.com/api/v10"

# Discord's message content hard limit and a conservative attachment size cap
# (bytes) above which we keep the source URL in the text instead of re-uploading.
MAX_CONTENT_LEN = 2000
MAX_UPLOAD_BYTES = 25 * 1024 * 1024

# How many times we retry the *same* token after a 429 before moving on.
MAX_429_RETRIES = 2


class UserTokenSender:
    def __init__(
        self,
        *,
        db,
        ratelimit,
        action_type,
        session_provider: Callable[[], aiohttp.ClientSession],
        logger,
    ):
        self._db = db
        self._ratelimit = ratelimit
        self._action = action_type
        self._session_provider = session_provider
        self._log = logger
        # One stable device fingerprint per token (keyed by token string).
        self._fingerprints: dict = {}
        # Rotating index per channel for round-robin selection.
        self._rr_index_by_channel: dict = {}
        # Sticky mode: (mapping_id, author_id) -> token_id assignment.
        self._sticky_author_token: dict = {}

    # ── public API ───────────────────────────────────────────────────────────

    async def send(
        self,
        *,
        mapping_id: Optional[str],
        target_channel_id: int,
        content: Optional[str],
        embeds: Optional[list] = None,
        attachments: Optional[list] = None,
        author_id=None,
        strategy: str = "round_robin",
        typing: bool = False,
        min_delay: float = 0.0,
        max_delay: float = 0.0,
        links_only: bool = False,
    ) -> bool:
        """
        Attempt to post a message into ``target_channel_id`` (a channel or thread
        id in the clone guild) as one of the mapping's user tokens.

        Returns True on success, False if there are no usable tokens or every
        token failed — in which case the caller should fall back to the webhook.
        """
        if not mapping_id or not target_channel_id:
            return False

        try:
            tokens = self._db.get_enabled_mapping_tokens(str(mapping_id)) or []
        except Exception:
            self._log.exception("[user-send] Failed to load tokens for mapping %s", mapping_id)
            return False

        if not tokens:
            return False

        text = self._compose_text(content, embeds)
        atts = [
            a
            for a in (attachments or [])
            if isinstance(a, dict) and a.get("url")
        ]

        # Nothing a user account can carry (e.g. sticker-only / custom-embed-only
        # message) → let the webhook path handle it.
        if not text and not atts:
            return False

        chan = int(target_channel_id)

        # Links-only mode: don't re-upload files; the source URLs are already in
        # the message text, so just skip the multipart upload.
        atts_to_upload = [] if links_only else atts

        # Pick the order of accounts to try, per the selected strategy.
        order = self._order_tokens(
            tokens, chan, strategy, author_id, mapping_id
        )

        # Optional spacing so bursts look less automated / ease rate limits.
        lo = max(0.0, float(min_delay or 0.0))
        hi = max(lo, float(max_delay or 0.0))
        if hi > 0:
            await asyncio.sleep(random.uniform(lo, min(hi, 30.0)))

        for tok in order:
            token_value = (tok.get("token_value") or "").strip()
            if not token_value:
                continue
            try:
                ok = await self._send_with_token(
                    token_value, chan, text, atts_to_upload, typing=typing
                )
            except Exception:
                self._log.exception(
                    "[user-send] Unexpected error sending to channel %s", target_channel_id
                )
                ok = False

            if ok:
                tid = tok.get("token_id")
                if tid:
                    try:
                        self._db.increment_mapping_token_usage(tid)
                    except Exception:
                        pass
                self._log.debug(
                    "[user-send] Sent message into channel %s as %s",
                    target_channel_id,
                    tok.get("username") or tok.get("token_id"),
                )
                return True

        self._log.debug(
            "[user-send] All %d token(s) failed for channel %s; falling back to webhook",
            len(order),
            target_channel_id,
        )
        return False

    # ── account selection ─────────────────────────────────────────────────────

    def _order_tokens(
        self, tokens: list, chan: int, strategy: str, author_id, mapping_id=None
    ):
        """Return the tokens in the order to try, per the selected strategy."""
        toks = list(tokens)
        if len(toks) <= 1:
            return toks

        if strategy == "sticky_author" and author_id:
            # Each source author is assigned one account and always uses it.
            # New authors are handed the least-used account so distinct authors
            # spread across distinct accounts until the accounts run out.
            ordered = sorted(toks, key=lambda t: str(t.get("token_id")))
            ids = [str(t.get("token_id")) for t in ordered]
            key = (str(mapping_id), str(author_id))
            assigned = self._sticky_author_token.get(key)
            if assigned not in ids:
                counts = {tid: 0 for tid in ids}
                for (mid, _aid), tid in self._sticky_author_token.items():
                    if mid == str(mapping_id) and tid in counts:
                        counts[tid] += 1
                assigned = min(ids, key=lambda tid: counts[tid])
                self._sticky_author_token[key] = assigned
            idx = ids.index(assigned)
            return ordered[idx:] + ordered[:idx]

        # Default / "round_robin" (and any legacy value): even rotation per
        # channel so every account sends an equal share.
        ordered = sorted(toks, key=lambda t: str(t.get("token_id")))
        idx = self._rr_index_by_channel.get(chan, 0) % len(ordered)
        self._rr_index_by_channel[chan] = (idx + 1) % len(ordered)
        return ordered[idx:] + ordered[:idx]

    # ── send implementation ──────────────────────────────────────────────────

    async def _send_with_token(
        self,
        token: str,
        channel_id: int,
        text: str,
        attachments: list,
        *,
        typing: bool = False,
    ) -> bool:
        session = self._session_provider()
        files, kept_urls = await self._prepare_files(session, attachments)

        # Any attachment that was uploaded as a file gets its URL stripped from
        # the text to avoid showing the link twice. Attachments we could not
        # download keep their URL in the text as a fallback.
        body_text = text
        for a in attachments:
            url = a.get("url")
            if url and url not in kept_urls and url in body_text:
                body_text = body_text.replace(url, "").strip()

        if len(body_text) > MAX_CONTENT_LEN:
            body_text = body_text[: MAX_CONTENT_LEN - 1] + "…"

        if not body_text and not files:
            return False

        url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
        headers = self._build_headers(token)

        if typing:
            await self._send_typing(session, channel_id, headers)

        attempts = 0
        while True:
            try:
                if files:
                    form = self._build_multipart(body_text, files)
                    resp_cm = session.post(url, data=form, headers=headers, timeout=60)
                else:
                    payload = {"content": body_text}
                    resp_cm = session.post(url, json=payload, headers=headers, timeout=30)

                async with resp_cm as resp:
                    status = resp.status
                    if status in (200, 201):
                        return True

                    if status == 429:
                        retry_after = await self._retry_after(resp)
                        attempts += 1
                        if attempts > MAX_429_RETRIES:
                            self._log.warning(
                                "[user-send] Rate limited on channel %s; giving up on this token",
                                channel_id,
                            )
                            return False
                        await asyncio.sleep(min(retry_after, 30.0))
                        continue

                    if status in (401, 403, 404):
                        # invalid/expired token, not in guild, missing perms, or
                        # channel gone → this token cannot deliver; try the next.
                        self._log.debug(
                            "[user-send] token rejected (HTTP %s) for channel %s", status, channel_id
                        )
                        return False

                    body = await self._safe_text(resp)
                    self._log.warning(
                        "[user-send] Discord returned HTTP %s for channel %s: %s",
                        status,
                        channel_id,
                        body[:300],
                    )
                    return False
            except asyncio.TimeoutError:
                self._log.warning("[user-send] Timeout posting to channel %s", channel_id)
                return False
            except aiohttp.ClientError as e:
                self._log.warning("[user-send] Network error posting to channel %s: %s", channel_id, e)
                return False

    async def _send_typing(self, session, channel_id: int, headers: dict):
        """Fire a typing indicator, then pause briefly to simulate typing.

        Best-effort — any failure is ignored and the message is still sent.
        """
        try:
            typing_url = f"{DISCORD_API_BASE}/channels/{channel_id}/typing"
            async with session.post(typing_url, headers=headers, timeout=10):
                pass
            await asyncio.sleep(random.uniform(1.0, 2.5))
        except Exception:
            pass

    async def _prepare_files(self, session: aiohttp.ClientSession, attachments: list):
        """Download attachments and return (files, kept_urls).

        ``files`` is a list of (filename, bytes, content_type) tuples that will
        be uploaded as multipart. ``kept_urls`` is the set of source URLs that
        were successfully downloaded (so the caller can strip them from text).
        """
        files: list = []
        kept_urls: set = set()

        for a in attachments:
            url = a.get("url")
            if not url:
                continue
            size = a.get("size")
            try:
                if size and int(size) > MAX_UPLOAD_BYTES:
                    continue
            except Exception:
                pass

            try:
                async with session.get(url, timeout=60) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.read()
                    if len(data) > MAX_UPLOAD_BYTES:
                        continue
                    ctype = (
                        a.get("content_type")
                        or resp.headers.get("Content-Type")
                        or "application/octet-stream"
                    )
            except Exception:
                self._log.debug("[user-send] Failed to download attachment %s", url, exc_info=True)
                continue

            filename = a.get("filename") or self._basename(url) or "file.bin"
            files.append((filename, data, ctype))
            kept_urls.add(url)

        return files, kept_urls

    def _build_multipart(self, content: str, files: list) -> aiohttp.FormData:
        form = aiohttp.FormData()
        payload = {
            "content": content or "",
            "attachments": [
                {"id": i, "filename": fn} for i, (fn, _data, _ct) in enumerate(files)
            ],
        }
        form.add_field(
            "payload_json", json.dumps(payload), content_type="application/json"
        )
        for i, (fn, data, ct) in enumerate(files):
            form.add_field(f"files[{i}]", data, filename=fn, content_type=ct)
        return form

    # ── content helpers ──────────────────────────────────────────────────────

    def _compose_text(self, content: Optional[str], embeds: Optional[list]) -> str:
        text = (content or "").strip()
        for e in embeds or []:
            flat = self._flatten_embed(e)
            if flat:
                text = f"{text}\n{flat}".strip() if text else flat
        return text

    @staticmethod
    def _flatten_embed(e) -> str:
        """Render a (py-cord) Embed object down to plain text/links."""
        parts: list[str] = []

        author = getattr(e, "author", None)
        if author is not None and getattr(author, "name", None):
            parts.append(str(author.name))

        title = getattr(e, "title", None)
        if title:
            parts.append(f"**{title}**")

        e_url = getattr(e, "url", None)
        if e_url:
            parts.append(str(e_url))

        desc = getattr(e, "description", None)
        if desc:
            parts.append(str(desc))

        for f in getattr(e, "fields", []) or []:
            name = getattr(f, "name", "") or ""
            value = getattr(f, "value", "") or ""
            joined = f"{name}\n{value}".strip()
            if joined:
                parts.append(joined)

        image = getattr(e, "image", None)
        if image is not None and getattr(image, "url", None):
            parts.append(str(image.url))

        thumb = getattr(e, "thumbnail", None)
        if thumb is not None and getattr(thumb, "url", None):
            parts.append(str(thumb.url))

        footer = getattr(e, "footer", None)
        if footer is not None and getattr(footer, "text", None):
            parts.append(str(footer.text))

        return "\n".join(p for p in parts if p)

    # ── misc helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _basename(url: str) -> str:
        try:
            path = url.split("?", 1)[0]
            return path.rsplit("/", 1)[-1]
        except Exception:
            return ""

    @staticmethod
    async def _retry_after(resp) -> float:
        try:
            data = await resp.json()
            return float(data.get("retry_after", 1.0))
        except Exception:
            hdr = resp.headers.get("Retry-After")
            try:
                return float(hdr) if hdr else 1.0
            except Exception:
                return 1.0

    @staticmethod
    async def _safe_text(resp) -> str:
        try:
            return await resp.text()
        except Exception:
            return ""

    def _build_headers(self, token: str) -> dict:
        """Realistic Discord desktop-client headers, unique & stable per token.
        """
        fp = self._fingerprints.get(token)
        if fp is None:
            fp = self._make_fingerprint(token)
            self._fingerprints[token] = fp
        return {
            **fp["headers"],
            "Authorization": token,
            "X-Super-Properties": fp["super_props_b64"],
        }

    @staticmethod
    def _make_fingerprint(token: str) -> dict:
        # Delegates to the shared builder so token *validation* (admin process)
        # and message *sending* (server process) present an identical device
        # fingerprint for the same account. See common.selfbot_headers.
        return make_fingerprint(token)
