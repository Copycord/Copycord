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
        # Send-delay pacing: serialize per channel so the configured delay
        # SPACES messages instead of each message jittering independently
        # (which would fire concurrent messages in a burst).
        self._pace_locks: dict = {}
        self._pace_last_by_channel: dict = {}

    # ── public API ───────────────────────────────────────────────────────────

    async def send(
        self,
        *,
        mapping_id: Optional[str],
        target_channel_id: int,
        content: Optional[str],
        embeds: Optional[list] = None,
        attachments: Optional[list] = None,
        sticker_ids: Optional[list] = None,
        author_id=None,
        strategy: str = "round_robin",
        typing: bool = False,
        min_delay: float = 0.0,
        max_delay: float = 0.0,
        links_only: bool = False,
        forced_token_id: Optional[str] = None,
    ) -> bool:
        """
        Attempt to post a message into ``target_channel_id`` (a channel or thread
        id in the clone guild) as one of the mapping's user tokens.

        When ``forced_token_id`` is given (the identity manager already decided
        which account should send), that token is tried first, bypassing the
        strategy ordering — this keeps the account that received the mirrored
        nickname/roles the same account that posts the message.

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
        stkr_ids = [str(s) for s in (sticker_ids or []) if s]

        # Nothing a user account can carry (e.g. custom-embed-only message) →
        # let the webhook path handle it.
        if not text and not atts and not stkr_ids:
            return False

        chan = int(target_channel_id)

        # Links-only mode: don't re-upload files; the source URLs are already in
        # the message text, so just skip the multipart upload.
        atts_to_upload = [] if links_only else atts

        # Pick the order of accounts to try. When the identity manager already
        # chose a token, try it first (then the rest as fallback); otherwise use
        # the configured strategy ordering.
        if forced_token_id is not None:
            forced = [
                t for t in tokens if str(t.get("token_id")) == str(forced_token_id)
            ]
            rest = [
                t for t in tokens if str(t.get("token_id")) != str(forced_token_id)
            ]
            order = forced + rest
        else:
            order = self._order_tokens(
                tokens, chan, strategy, author_id, mapping_id
            )

        # Optional spacing so messages don't arrive in a burst. Serialized per
        # channel so each message waits a random gap after the previous one.
        await self._pace_channel(chan, min_delay, max_delay)

        first_attempt = True
        for tok in order:
            token_value = (tok.get("token_value") or "").strip()
            if not token_value:
                continue
            # Only the first account fires the typing indicator — otherwise a
            # failing send would make every token "type" in a burst.
            fire_typing = typing and first_attempt
            first_attempt = False
            try:
                ok = await self._send_with_token(
                    token_value,
                    chan,
                    text,
                    atts_to_upload,
                    typing=fire_typing,
                    sticker_ids=stkr_ids,
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

    async def create_forum_thread(
        self,
        *,
        mapping_id: Optional[str],
        forum_channel_id: int,
        thread_name: str,
        content: Optional[str],
        embeds: Optional[list] = None,
        attachments: Optional[list] = None,
        sticker_ids: Optional[list] = None,
        applied_tag_ids: Optional[list] = None,
        auto_archive_duration: int = 60,
        author_id=None,
        strategy: str = "round_robin",
        min_delay: float = 0.0,
        max_delay: float = 0.0,
        links_only: bool = False,
        forced_token_id: Optional[str] = None,
    ) -> Optional[int]:
        """
        Create a forum thread whose starter message is authored by one of the
        mapping's user tokens.

        In a forum channel the thread and its first (starter) message are one
        atomic Discord operation, so this is the only way the starter post can
        be authored by a real user account instead of the channel webhook.

        Returns the new thread id on success (the thread id equals the starter
        message id), or None if there are no usable tokens or every token failed
        — in which case the caller should fall back to the webhook.
        """
        if not mapping_id or not forum_channel_id or not thread_name:
            return None

        try:
            tokens = self._db.get_enabled_mapping_tokens(str(mapping_id)) or []
        except Exception:
            self._log.exception(
                "[user-send] Failed to load tokens for mapping %s", mapping_id
            )
            return None
        if not tokens:
            return None

        text = self._compose_text(content, embeds)
        atts = [
            a for a in (attachments or []) if isinstance(a, dict) and a.get("url")
        ]
        stkr_ids = [str(s) for s in (sticker_ids or []) if s]

        # A forum thread must have a starter message; if a user account can
        # carry nothing (custom-embed-only, etc.) let the webhook create it.
        if not text and not atts and not stkr_ids:
            return None

        forum_id = int(forum_channel_id)
        atts_to_upload = [] if links_only else atts
        tag_ids = [str(t) for t in (applied_tag_ids or []) if t]

        # Same account ordering as send(): honour the identity manager's choice,
        # else fall back to the configured strategy.
        if forced_token_id is not None:
            forced = [
                t for t in tokens if str(t.get("token_id")) == str(forced_token_id)
            ]
            rest = [
                t for t in tokens if str(t.get("token_id")) != str(forced_token_id)
            ]
            order = forced + rest
        else:
            order = self._order_tokens(
                tokens, forum_id, strategy, author_id, mapping_id
            )

        # Pace like a normal send (keyed on the forum channel).
        await self._pace_channel(forum_id, min_delay, max_delay)

        for tok in order:
            token_value = (tok.get("token_value") or "").strip()
            if not token_value:
                continue
            try:
                new_id = await self._create_thread_with_token(
                    token_value,
                    forum_id,
                    thread_name,
                    text,
                    atts_to_upload,
                    sticker_ids=stkr_ids,
                    applied_tag_ids=tag_ids,
                    auto_archive_duration=auto_archive_duration,
                )
            except Exception:
                self._log.exception(
                    "[user-send] Unexpected error creating forum thread in %s",
                    forum_id,
                )
                new_id = None

            if new_id:
                tid = tok.get("token_id")
                if tid:
                    try:
                        self._db.increment_mapping_token_usage(tid)
                    except Exception:
                        pass
                self._log.debug(
                    "[user-send] Created forum thread %s in channel %s as %s",
                    new_id,
                    forum_id,
                    tok.get("username") or tok.get("token_id"),
                )
                return new_id

        self._log.debug(
            "[user-send] All %d token(s) failed to create forum thread in %s; "
            "falling back to webhook",
            len(order),
            forum_id,
        )
        return None

    async def create_text_thread(
        self,
        *,
        mapping_id: Optional[str],
        parent_channel_id: int,
        thread_name: str,
        starter_message_id: Optional[int] = None,
        auto_archive_duration: int = 60,
        author_id=None,
        strategy: str = "round_robin",
        min_delay: float = 0.0,
        max_delay: float = 0.0,
        forced_token_id: Optional[str] = None,
    ) -> Optional[int]:
        """
        Create a text-channel thread as one of the mapping's user tokens.

        When ``starter_message_id`` is given the thread is created *from* that
        message (its id equals the message id); otherwise a standalone public
        thread is created. Unlike a forum thread this posts no starter message —
        the thread's first message is forwarded separately.

        Returns the new thread id on success, or None if there are no usable
        tokens or every token failed — in which case the caller should fall back
        to the bot.
        """
        if not mapping_id or not parent_channel_id or not thread_name:
            return None

        try:
            tokens = self._db.get_enabled_mapping_tokens(str(mapping_id)) or []
        except Exception:
            self._log.exception(
                "[user-send] Failed to load tokens for mapping %s", mapping_id
            )
            return None
        if not tokens:
            return None

        parent_id = int(parent_channel_id)

        if forced_token_id is not None:
            forced = [
                t for t in tokens if str(t.get("token_id")) == str(forced_token_id)
            ]
            rest = [
                t for t in tokens if str(t.get("token_id")) != str(forced_token_id)
            ]
            order = forced + rest
        else:
            order = self._order_tokens(
                tokens, parent_id, strategy, author_id, mapping_id
            )

        await self._pace_channel(parent_id, min_delay, max_delay)

        for tok in order:
            token_value = (tok.get("token_value") or "").strip()
            if not token_value:
                continue
            try:
                new_id = await self._create_text_thread_with_token(
                    token_value,
                    parent_id,
                    thread_name,
                    starter_message_id=starter_message_id,
                    auto_archive_duration=auto_archive_duration,
                )
            except Exception:
                self._log.exception(
                    "[user-send] Unexpected error creating text thread in %s",
                    parent_id,
                )
                new_id = None

            if new_id:
                tid = tok.get("token_id")
                if tid:
                    try:
                        self._db.increment_mapping_token_usage(tid)
                    except Exception:
                        pass
                self._log.debug(
                    "[user-send] Created text thread %s in channel %s as %s",
                    new_id,
                    parent_id,
                    tok.get("username") or tok.get("token_id"),
                )
                return new_id

        self._log.debug(
            "[user-send] All %d token(s) failed to create text thread in %s; "
            "falling back to bot",
            len(order),
            parent_id,
        )
        return None

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

    async def _pace_channel(self, chan: int, min_delay, max_delay) -> None:
        """Space sends to a channel by a random gap.

        Unlike a bare per-message sleep (which lets concurrent messages fire at
        once), this serializes on a per-channel lock and waits until at least a
        random ``[min, max]`` seconds after the previous send — so bursts are
        spread out. When messages arrive slower than the gap, there is no wait.
        """
        lo = max(0.0, float(min_delay or 0.0))
        hi = max(lo, float(max_delay or 0.0))
        if hi <= 0:
            return

        lock = self._pace_locks.get(chan)
        if lock is None:
            lock = asyncio.Lock()
            self._pace_locks[chan] = lock

        async with lock:
            loop = asyncio.get_event_loop()
            now = loop.time()
            gap = random.uniform(lo, min(hi, 30.0))
            last = self._pace_last_by_channel.get(chan, 0.0)
            wait = (last + gap) - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._pace_last_by_channel[chan] = loop.time()

    async def _send_with_token(
        self,
        token: str,
        channel_id: int,
        text: str,
        attachments: list,
        *,
        typing: bool = False,
        sticker_ids: Optional[list] = None,
    ) -> bool:
        session = self._session_provider()
        files, uploaded_urls = await self._prepare_files(session, attachments)
        stkr_ids = [str(s) for s in (sticker_ids or []) if s]

        # Any attachment we uploaded as a real file gets its URL stripped from
        # the text so the link isn't shown alongside the upload. Attachments we
        # could not download keep their URL in the text as a fallback link.
        body_text = text
        for a in attachments:
            url = a.get("url")
            if url and url in uploaded_urls and url in body_text:
                body_text = body_text.replace(url, "").strip()

        if len(body_text) > MAX_CONTENT_LEN:
            body_text = body_text[: MAX_CONTENT_LEN - 1] + "…"

        if not body_text and not files and not stkr_ids:
            return False

        url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
        headers = self._build_headers(token)

        if typing:
            await self._send_typing(session, channel_id, headers)

        attempts = 0
        while True:
            try:
                if files:
                    form = self._build_multipart(body_text, files, stkr_ids)
                    resp_cm = session.post(url, data=form, headers=headers, timeout=60)
                else:
                    payload = {"content": body_text}
                    if stkr_ids:
                        payload["sticker_ids"] = stkr_ids
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

    async def _create_thread_with_token(
        self,
        token: str,
        forum_channel_id: int,
        thread_name: str,
        text: str,
        attachments: list,
        *,
        sticker_ids: Optional[list] = None,
        applied_tag_ids: Optional[list] = None,
        auto_archive_duration: int = 60,
    ) -> Optional[int]:
        """POST a forum thread + starter message as one account.

        Returns the new thread id on success, or None so the caller can try the
        next token.
        """
        session = self._session_provider()
        files, uploaded_urls = await self._prepare_files(session, attachments)
        stkr_ids = [str(s) for s in (sticker_ids or []) if s]

        body_text = text
        for a in attachments:
            url = a.get("url")
            if url and url in uploaded_urls and url in body_text:
                body_text = body_text.replace(url, "").strip()

        if len(body_text) > MAX_CONTENT_LEN:
            body_text = body_text[: MAX_CONTENT_LEN - 1] + "…"

        if not body_text and not files and not stkr_ids:
            return None

        message: dict = {"content": body_text}
        if stkr_ids:
            message["sticker_ids"] = stkr_ids

        thread_body: dict = {
            "name": (thread_name or "thread")[:100],
            "auto_archive_duration": int(auto_archive_duration or 60),
            "message": message,
        }
        if applied_tag_ids:
            thread_body["applied_tags"] = [str(t) for t in applied_tag_ids]

        url = f"{DISCORD_API_BASE}/channels/{forum_channel_id}/threads"
        headers = self._build_headers(token)

        attempts = 0
        while True:
            try:
                if files:
                    form = self._build_forum_multipart(thread_body, files)
                    resp_cm = session.post(url, data=form, headers=headers, timeout=60)
                else:
                    resp_cm = session.post(
                        url, json=thread_body, headers=headers, timeout=30
                    )

                async with resp_cm as resp:
                    status = resp.status
                    if status in (200, 201):
                        try:
                            data = await resp.json()
                            return int(data.get("id"))
                        except Exception:
                            self._log.warning(
                                "[user-send] Forum thread created in %s but response "
                                "had no usable id",
                                forum_channel_id,
                            )
                            return None

                    if status == 429:
                        retry_after = await self._retry_after(resp)
                        attempts += 1
                        if attempts > MAX_429_RETRIES:
                            self._log.warning(
                                "[user-send] Rate limited creating forum thread in %s; "
                                "giving up on this token",
                                forum_channel_id,
                            )
                            return None
                        await asyncio.sleep(min(retry_after, 30.0))
                        continue

                    if status in (401, 403, 404):
                        self._log.debug(
                            "[user-send] token rejected (HTTP %s) creating forum "
                            "thread in %s",
                            status,
                            forum_channel_id,
                        )
                        return None

                    body = await self._safe_text(resp)
                    self._log.warning(
                        "[user-send] Discord returned HTTP %s creating forum thread "
                        "in %s: %s",
                        status,
                        forum_channel_id,
                        body[:300],
                    )
                    return None
            except asyncio.TimeoutError:
                self._log.warning(
                    "[user-send] Timeout creating forum thread in %s", forum_channel_id
                )
                return None
            except aiohttp.ClientError as e:
                self._log.warning(
                    "[user-send] Network error creating forum thread in %s: %s",
                    forum_channel_id,
                    e,
                )
                return None

    async def _create_text_thread_with_token(
        self,
        token: str,
        parent_channel_id: int,
        thread_name: str,
        *,
        starter_message_id: Optional[int] = None,
        auto_archive_duration: int = 60,
    ) -> Optional[int]:
        """Create a text-channel thread as one account.

        With ``starter_message_id`` the thread is created from that message
        (POST .../messages/{id}/threads); otherwise a standalone public thread
        (type 11) is created. Returns the new thread id, or None so the caller
        can try the next token.
        """
        session = self._session_provider()

        body: dict = {
            "name": (thread_name or "thread")[:100],
            "auto_archive_duration": int(auto_archive_duration or 60),
        }
        if starter_message_id:
            url = (
                f"{DISCORD_API_BASE}/channels/{parent_channel_id}"
                f"/messages/{int(starter_message_id)}/threads"
            )
        else:
            url = f"{DISCORD_API_BASE}/channels/{parent_channel_id}/threads"
            body["type"] = 11  # public thread

        headers = self._build_headers(token)

        attempts = 0
        while True:
            try:
                async with session.post(
                    url, json=body, headers=headers, timeout=30
                ) as resp:
                    status = resp.status
                    if status in (200, 201):
                        try:
                            data = await resp.json()
                            return int(data.get("id"))
                        except Exception:
                            self._log.warning(
                                "[user-send] Text thread created in %s but response "
                                "had no usable id",
                                parent_channel_id,
                            )
                            return None

                    if status == 429:
                        retry_after = await self._retry_after(resp)
                        attempts += 1
                        if attempts > MAX_429_RETRIES:
                            self._log.warning(
                                "[user-send] Rate limited creating text thread in %s; "
                                "giving up on this token",
                                parent_channel_id,
                            )
                            return None
                        await asyncio.sleep(min(retry_after, 30.0))
                        continue

                    if status in (401, 403, 404):
                        self._log.debug(
                            "[user-send] token rejected (HTTP %s) creating text "
                            "thread in %s",
                            status,
                            parent_channel_id,
                        )
                        return None

                    body_txt = await self._safe_text(resp)
                    self._log.warning(
                        "[user-send] Discord returned HTTP %s creating text thread "
                        "in %s: %s",
                        status,
                        parent_channel_id,
                        body_txt[:300],
                    )
                    return None
            except asyncio.TimeoutError:
                self._log.warning(
                    "[user-send] Timeout creating text thread in %s", parent_channel_id
                )
                return None
            except aiohttp.ClientError as e:
                self._log.warning(
                    "[user-send] Network error creating text thread in %s: %s",
                    parent_channel_id,
                    e,
                )
                return None

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
        """Download attachments and return (files, uploaded_urls).

        ``files`` is a list of (filename, bytes, content_type) tuples that will
        be uploaded as multipart. ``uploaded_urls`` is the set of source URLs we
        downloaded successfully (so the caller can strip them from the text — the
        file replaces the link).
        """
        files: list = []
        uploaded_urls: set = set()

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
            uploaded_urls.add(url)

        return files, uploaded_urls

    def _build_multipart(
        self, content: str, files: list, sticker_ids: Optional[list] = None
    ) -> aiohttp.FormData:
        form = aiohttp.FormData()
        payload = {
            "content": content or "",
            "attachments": [
                {"id": i, "filename": fn} for i, (fn, _data, _ct) in enumerate(files)
            ],
        }
        if sticker_ids:
            payload["sticker_ids"] = [str(s) for s in sticker_ids]
        form.add_field(
            "payload_json", json.dumps(payload), content_type="application/json"
        )
        for i, (fn, data, ct) in enumerate(files):
            form.add_field(f"files[{i}]", data, filename=fn, content_type=ct)
        return form

    def _build_forum_multipart(
        self, thread_body: dict, files: list
    ) -> aiohttp.FormData:
        """Multipart body for creating a forum thread with file attachments.

        The starter message's ``attachments`` array lives inside ``message`` and
        references the uploaded ``files[i]`` parts.
        """
        form = aiohttp.FormData()
        body = dict(thread_body)
        message = dict(body.get("message") or {})
        message["attachments"] = [
            {"id": i, "filename": fn} for i, (fn, _data, _ct) in enumerate(files)
        ]
        body["message"] = message
        form.add_field(
            "payload_json", json.dumps(body), content_type="application/json"
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
