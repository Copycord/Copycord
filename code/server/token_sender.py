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
import time
from typing import Callable, Optional

import aiohttp

from common.selfbot_headers import make_fingerprint

DISCORD_API_BASE = "https://discord.com/api/v10"

# Discord's message content hard limit and a conservative attachment size cap
# (bytes) above which we keep the source URL in the text instead of re-uploading.
MAX_CONTENT_LEN = 2000
MAX_UPLOAD_BYTES = 25 * 1024 * 1024

# A 429 sleeps the account for its retry_after and retries — we respect the rate
# limit rather than moving on. Network/timeout/5xx errors are retried too (never
# webhooked). These bound how many times before giving up on one message.
MAX_429_RETRIES = 5
MAX_NET_RETRIES = 3

# Outcome of a single-account send, reported up so the sticky-identity loop can
# tell a genuinely dead account (bench it + swap) apart from a transient blip
# (keep the account, don't webhook) apart from "no token could deliver" (the one
# case that falls back to a webhook).
SEND_OK = "ok"                        # delivered
SEND_DEAD = "dead"                    # HTTP 401 — token revoked/invalid; bench it
SEND_UNDELIVERABLE = "undeliverable"  # 403/404/other 4xx — can't post here; try another
SEND_RATE_LIMITED = "rate_limited"    # 429 still after retries; don't swap, don't webhook
SEND_TRANSIENT = "transient"          # network/timeout/5xx after retries; don't swap/webhook
SEND_UNSUPPORTED = "unsupported"      # nothing a user account can carry → let the webhook do it
SEND_NO_TOKENS = "no_tokens"          # no usable tokens / bad target → webhook


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
        # Send-delay pacing: one lock per channel so each message waits its full
        # delay (showing the typing indicator, if enabled) before being sent,
        # and queued messages take turns instead of firing in a burst.
        self._pace_locks: dict = {}
        # Per-account rate-limit gating: one lock per token so an account's
        # messages take strict turns, plus a cooldown timestamp set from a 429's
        # retry_after. When one message from an account is rate limited the rest
        # wait out the cooldown instead of piling on more 429s (keyed by token).
        self._token_locks: dict = {}
        self._token_cooldown: dict = {}

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
        sticky_exclusive: bool = False,
    ) -> str:
        """
        Attempt to post a message into ``target_channel_id`` (a channel or thread
        id in the clone guild) as one of the mapping's user tokens.

        When ``forced_token_id`` is given (the identity manager already decided
        which account should send), that token is tried first, bypassing the
        strategy ordering — this keeps the account that received the mirrored
        nickname/roles the same account that posts the message.

        When ``sticky_exclusive`` is also set, ONLY the forced token is tried —
        never another account — so a sticky identity is never impersonated by the
        wrong token. If it fails the caller decides whether to swap the author's
        assignment, fall back to a webhook, or skip.

        Returns a ``SEND_*`` status: ``SEND_OK`` on success, otherwise the reason
        the (last) account couldn't deliver — ``SEND_DEAD`` (token revoked),
        ``SEND_UNDELIVERABLE`` (can't post here), ``SEND_RATE_LIMITED``,
        ``SEND_TRANSIENT`` (network), ``SEND_UNSUPPORTED`` (nothing carryable), or
        ``SEND_NO_TOKENS``. The caller decides swap/webhook/drop from that.
        """
        if not mapping_id or not target_channel_id:
            return SEND_NO_TOKENS

        try:
            tokens = self._db.get_enabled_mapping_tokens(str(mapping_id)) or []
        except Exception:
            self._log.exception("[user-send] Failed to load tokens for mapping %s", mapping_id)
            return SEND_NO_TOKENS

        if not tokens:
            return SEND_NO_TOKENS

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
            return SEND_UNSUPPORTED

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
            if sticky_exclusive:
                # Only the forced account may post — never fall back to another,
                # which would break the mirrored identity.
                order = forced
            else:
                rest = [
                    t for t in tokens if str(t.get("token_id")) != str(forced_token_id)
                ]
                order = forced + rest
        else:
            order = self._order_tokens(
                tokens, chan, strategy, author_id, mapping_id
            )

        # The typing indicator uses the first candidate account — the one about
        # to post.
        type_token = next(
            (
                tv
                for tv in ((t.get("token_value") or "").strip() for t in order)
                if tv
            ),
            None,
        )

        async def _deliver() -> str:
            # Wait this message's send-delay first (showing typing for the whole
            # time when enabled), then post it.
            await self._pace_channel(
                chan, min_delay, max_delay, typing=typing, token=type_token
            )
            last = SEND_NO_TOKENS
            for tok in order:
                token_value = (tok.get("token_value") or "").strip()
                if not token_value:
                    continue
                try:
                    status = await self._send_with_token(
                        token_value,
                        chan,
                        text,
                        atts_to_upload,
                        sticker_ids=stkr_ids,
                    )
                except Exception:
                    self._log.exception(
                        "[user-send] Unexpected error sending to channel %s",
                        target_channel_id,
                    )
                    status = SEND_TRANSIENT

                if status == SEND_OK:
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
                    return SEND_OK
                last = status

            self._log.debug(
                "[user-send] token(s) could not deliver to channel %s (%s)",
                target_channel_id,
                last,
            )
            return last

        # When there's a delay or a typing indicator to show, hold the per-channel
        # lock across BOTH the delay and the send so queued messages take strict
        # turns: type → send → next types → send. If the lock were released after
        # the delay (before the send), the next message would start typing and
        # then get its indicator cleared by this message's send — so it would
        # appear to send with no typing. With no delay/typing there's nothing to
        # serialize, so send concurrently.
        if (typing and type_token) or float(max_delay or 0.0) > 0:
            async with self._chan_lock(chan):
                return await _deliver()
        return await _deliver()

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
        sticky_exclusive: bool = False,
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
            if sticky_exclusive:
                order = forced
            else:
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
        sticky_exclusive: bool = False,
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
            if sticky_exclusive:
                order = forced
            else:
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

    def _chan_lock(self, chan: int) -> asyncio.Lock:
        """Per-channel lock. The caller holds it across the delay AND the send so
        queued messages take strict turns (type → send → next type → send)."""
        lock = self._pace_locks.get(chan)
        if lock is None:
            lock = asyncio.Lock()
            self._pace_locks[chan] = lock
        return lock

    def _token_lock(self, key: str) -> asyncio.Lock:
        """Per-account lock held across a request so one account's messages take
        strict turns — a rate-limited account holds back its own queued messages
        instead of firing them all and collecting more 429s."""
        lock = self._token_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._token_locks[key] = lock
        return lock

    async def _await_cooldown(self, key: str) -> None:
        """Block until this account's rate-limit cooldown (if any) elapses."""
        delay = self._token_cooldown.get(key, 0.0) - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)

    async def _request_with_token(
        self,
        token: str,
        url: str,
        *,
        json_body: Optional[dict] = None,
        form_factory: Optional[Callable[[], aiohttp.FormData]] = None,
        timeout: int = 30,
        ctx: str = "",
    ) -> tuple[str, Optional[dict]]:
        """POST as one account with per-account rate-limit gating and retries.

        Serializes on the account (``_token_lock``) so its queued messages wait
        their turn, sleeps a 429's ``retry_after`` (parking the account for that
        long so other messages wait too) and retries, and retries network /
        timeout / 5xx errors. Returns ``(status, data)`` where ``status`` is a
        ``SEND_*`` constant and ``data`` is the parsed JSON on success (or None).
        ``form_factory`` must rebuild the multipart body each attempt (an aiohttp
        ``FormData`` is single-use).
        """
        session = self._session_provider()
        r429 = 0
        rnet = 0
        async with self._token_lock(token):
            while True:
                await self._await_cooldown(token)
                try:
                    headers = self._build_headers(token)
                    if form_factory is not None:
                        resp_cm = session.post(
                            url, data=form_factory(), headers=headers, timeout=timeout
                        )
                    else:
                        resp_cm = session.post(
                            url, json=json_body, headers=headers, timeout=timeout
                        )

                    async with resp_cm as resp:
                        status = resp.status
                        if status in (200, 201):
                            try:
                                data = await resp.json()
                            except Exception:
                                data = None
                            return SEND_OK, data

                        if status == 429:
                            retry_after = await self._retry_after(resp)
                            wait = min(max(0.0, retry_after), 60.0)
                            # Park the whole account so queued messages wait too.
                            self._token_cooldown[token] = time.monotonic() + wait
                            r429 += 1
                            if r429 > MAX_429_RETRIES:
                                self._log.warning(
                                    "[user-send] Still rate limited %s after %d retries",
                                    ctx,
                                    MAX_429_RETRIES,
                                )
                                return SEND_RATE_LIMITED, None
                            await asyncio.sleep(wait)
                            continue

                        if status == 401:
                            # Token revoked/invalid → this account is dead.
                            self._log.debug(
                                "[user-send] token revoked (HTTP 401) %s", ctx
                            )
                            return SEND_DEAD, None

                        if status in (403, 404):
                            # Not in guild / missing perms / channel gone — this
                            # account can't deliver here, but it isn't dead.
                            self._log.debug(
                                "[user-send] token can't deliver (HTTP %s) %s",
                                status,
                                ctx,
                            )
                            return SEND_UNDELIVERABLE, None

                        if 500 <= status < 600:
                            rnet += 1
                            if rnet > MAX_NET_RETRIES:
                                self._log.warning(
                                    "[user-send] Discord HTTP %s %s; giving up after %d retries",
                                    status,
                                    ctx,
                                    MAX_NET_RETRIES,
                                )
                                return SEND_TRANSIENT, None
                            await asyncio.sleep(min(2.0 * rnet, 10.0))
                            continue

                        body = await self._safe_text(resp)
                        self._log.warning(
                            "[user-send] Discord returned HTTP %s %s: %s",
                            status,
                            ctx,
                            body[:300],
                        )
                        return SEND_UNDELIVERABLE, None
                except asyncio.TimeoutError:
                    rnet += 1
                    self._log.warning(
                        "[user-send] Timeout %s (attempt %d)", ctx, rnet
                    )
                    if rnet > MAX_NET_RETRIES:
                        return SEND_TRANSIENT, None
                    await asyncio.sleep(min(2.0 * rnet, 10.0))
                    continue
                except aiohttp.ClientError as e:
                    rnet += 1
                    self._log.warning(
                        "[user-send] Network error %s (attempt %d): %s", ctx, rnet, e
                    )
                    if rnet > MAX_NET_RETRIES:
                        return SEND_TRANSIENT, None
                    await asyncio.sleep(min(2.0 * rnet, 10.0))
                    continue

    async def _pace_channel(
        self,
        chan: int,
        min_delay,
        max_delay,
        *,
        typing: bool = False,
        token: Optional[str] = None,
    ) -> None:
        """Wait a message's send-delay before it goes out.

        The delay is a random ``[min, max]`` seconds and represents human
        composition time. When ``typing`` is set (and a ``token`` is given) the
        typing indicator is shown for the whole delay; otherwise we just wait.
        With no delay this only shows a brief typing blip (if enabled) or is a
        no-op. This does NOT lock — the caller holds ``_chan_lock`` across this
        and the send so a queued message can't start typing before the current
        one is actually posted (which would clear its indicator).
        """
        lo = max(0.0, float(min_delay or 0.0))
        hi = max(lo, float(max_delay or 0.0))
        do_type = bool(typing and token)
        if hi <= 0 and not do_type:
            return

        delay = random.uniform(lo, min(hi, 30.0)) if hi > 0 else 0.0
        if do_type:
            await self._type_for(chan, token, delay)
        elif delay > 0:
            await asyncio.sleep(delay)

    async def _type_for(self, chan: int, token: str, duration: float) -> None:
        """Show the typing indicator in ``chan`` for ``duration`` seconds.

        Discord clears the indicator ~10s after each trigger, so re-fire every
        ~8s for longer delays. Best-effort — a failed ping never blocks the send.
        """
        session = self._session_provider()
        headers = self._build_headers(token)
        url = f"{DISCORD_API_BASE}/channels/{chan}/typing"

        async def _fire():
            try:
                async with session.post(url, headers=headers, timeout=10):
                    pass
            except Exception:
                pass

        await _fire()
        remaining = max(0.0, float(duration))
        while remaining > 0:
            step = min(remaining, 8.0)
            await asyncio.sleep(step)
            remaining -= step
            if remaining > 0:
                await _fire()

    async def _send_with_token(
        self,
        token: str,
        channel_id: int,
        text: str,
        attachments: list,
        *,
        sticker_ids: Optional[list] = None,
    ) -> str:
        """Post a message as one account. Returns a ``SEND_*`` status."""
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
            return SEND_UNSUPPORTED

        url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
        ctx = f"posting to channel {channel_id}"

        if files:
            status, _ = await self._request_with_token(
                token,
                url,
                form_factory=lambda: self._build_multipart(
                    body_text, files, stkr_ids
                ),
                timeout=60,
                ctx=ctx,
            )
        else:
            payload = {"content": body_text}
            if stkr_ids:
                payload["sticker_ids"] = stkr_ids
            status, _ = await self._request_with_token(
                token, url, json_body=payload, timeout=30, ctx=ctx
            )
        return status

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
        ctx = f"creating forum thread in {forum_channel_id}"

        if files:
            status, data = await self._request_with_token(
                token,
                url,
                form_factory=lambda: self._build_forum_multipart(thread_body, files),
                timeout=60,
                ctx=ctx,
            )
        else:
            status, data = await self._request_with_token(
                token, url, json_body=thread_body, timeout=30, ctx=ctx
            )

        if status == SEND_OK:
            try:
                return int((data or {}).get("id"))
            except Exception:
                self._log.warning(
                    "[user-send] Forum thread created in %s but response had no "
                    "usable id",
                    forum_channel_id,
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

        status, data = await self._request_with_token(
            token,
            url,
            json_body=body,
            timeout=30,
            ctx=f"creating text thread in {parent_channel_id}",
        )

        if status == SEND_OK:
            try:
                return int((data or {}).get("id"))
            except Exception:
                self._log.warning(
                    "[user-send] Text thread created in %s but response had no "
                    "usable id",
                    parent_channel_id,
                )
        return None

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
