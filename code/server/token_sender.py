# =============================================================================
#  Copycord
#  Copyright (C) 2025 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
from __future__ import annotations

import asyncio
import json
import random
import time
from typing import Callable, Optional

import aiohttp
from curl_cffi import CurlMime
from curl_cffi.requests.exceptions import RequestException as CurlRequestException

from common.proxy_pool import get_pool as get_proxy_pool
from common.selfbot_headers import (
    SUPPRESSED_PROFILE_HEADERS,
    build_headers,
    channel_referer,
    close_tls_session,
    context_properties,
    get_tls_session,
)

DISCORD_API_BASE = "https://discord.com/api/v10"


_CHAT_INPUT_CONTEXT = context_properties("chat_input")


MAX_CONTENT_LEN = 2000
MAX_UPLOAD_BYTES = 25 * 1024 * 1024


MAX_429_RETRIES = 5
MAX_NET_RETRIES = 3


def _reply_bits(reply_to: Optional[dict]) -> dict:
    """Payload fields that turn a message POST into a native reply.

    ``fail_if_not_exists`` is False so a reference to a message that has since
    been deleted in the clone degrades to a plain message instead of failing
    the whole send with a 400.

    ``allowed_mentions`` is spelled out rather than left off: a reply pings the
    author it answers by default, which a clone should never do. ``parse``
    lists every type so mentions inside the content keep behaving exactly as
    they do on a non-reply send — omitting it would silently make them inert.
    """
    if not reply_to:
        return {}

    try:
        message_id = int(reply_to.get("message_id") or 0)
        channel_id = int(reply_to.get("channel_id") or 0)
    except (TypeError, ValueError):
        return {}
    if not message_id or not channel_id:
        return {}

    ref = {
        "message_id": str(message_id),
        "channel_id": str(channel_id),
        "fail_if_not_exists": False,
    }
    try:
        guild_id = int(reply_to.get("guild_id") or 0)
    except (TypeError, ValueError):
        guild_id = 0
    if guild_id:
        ref["guild_id"] = str(guild_id)

    return {
        "message_reference": ref,
        "allowed_mentions": {
            "parse": ["users", "roles", "everyone"],
            "replied_user": False,
        },
    }


SEND_OK = "ok"
SEND_NOT_FOUND = "not_found"
SEND_DEAD = "dead"
SEND_UNDELIVERABLE = "undeliverable"
SEND_RATE_LIMITED = "rate_limited"
SEND_TRANSIENT = "transient"
SEND_UNSUPPORTED = "unsupported"
SEND_NO_TOKENS = "no_tokens"


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
        self._rr_index_by_channel: dict = {}
        self._sticky_author_token: dict = {}
        self._pace_locks: dict = {}
        self._token_locks: dict = {}
        self._token_cooldown: dict = {}

        self._token_last_seen: dict = {}
        self._channel_last_seen: dict = {}
        self._author_last_seen: dict = {}

        self._dead_tokens: set = set()

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
        use_proxy: bool = False,
        sent_as: Optional[list] = None,
        sent_ids: Optional[list] = None,
        reply_to: Optional[dict] = None,
        guild_id=None,
    ) -> str:
        """
        Attempt to post a message into ``target_channel_id`` (a channel or thread
        id in the clone guild) as one of the mapping's user tokens.

        ``reply_to``: ``{"message_id", "channel_id", "guild_id"}`` of the cloned
        message this one answers. Tokens are real accounts, so this posts an
        actual Discord reply rather than the link header webhooks have to use.

        ``sent_as``: optional list the account label that delivered the
        message is appended to. An out-param rather than a richer return
        value because the ``SEND_*`` string is compared directly at every
        call site; this lets the caller fold "which account sent it" into
        its own log line instead of us emitting a near-duplicate one here.

        ``sent_ids``: same idea, receiving a ``(message_id, token_id)`` pair.
        The caller records it, which is what makes a later message able to
        reply to this one, and what lets an edit or delete go out as the same
        account Discord requires.
        """
        if not mapping_id or not target_channel_id:
            return SEND_NO_TOKENS

        try:
            tokens = self._filter_dead(
                self._db.get_enabled_mapping_tokens(str(mapping_id)) or []
            )
        except Exception:
            self._log.exception(
                "[user-send] Failed to load tokens for mapping %s", mapping_id
            )
            return SEND_NO_TOKENS

        if not tokens:
            return SEND_NO_TOKENS

        text = self._compose_text(content, embeds)
        atts = [a for a in (attachments or []) if isinstance(a, dict) and a.get("url")]
        stkr_ids = [str(s) for s in (sticker_ids or []) if s]

        if not text and not atts and not stkr_ids:
            return SEND_UNSUPPORTED

        chan = int(target_channel_id)

        atts_to_upload = [] if links_only else atts

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
            order = self._order_tokens(tokens, chan, strategy, author_id, mapping_id)

        type_token = next(
            (tv for tv in ((t.get("token_value") or "").strip() for t in order) if tv),
            None,
        )

        async def _deliver() -> str:
            await self._pace_channel(
                chan,
                min_delay,
                max_delay,
                typing=typing,
                token=type_token,
                use_proxy=use_proxy,
            )
            last = SEND_NO_TOKENS
            for tok in order:
                token_value = (tok.get("token_value") or "").strip()
                if not token_value:
                    continue
                cloned_mid = None
                try:
                    status, cloned_mid = await self._send_with_token(
                        token_value,
                        chan,
                        text,
                        atts_to_upload,
                        sticker_ids=stkr_ids,
                        use_proxy=use_proxy,
                        reply_to=reply_to,
                        guild_id=guild_id,
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
                    if sent_as is not None:
                        sent_as.append(tok.get("username") or tok.get("token_id"))
                    if sent_ids is not None and cloned_mid:

                        sent_ids.append((cloned_mid, tid))
                    return SEND_OK
                if status == SEND_DEAD:

                    self._mark_token_dead(tok)
                last = status

            self._log.debug(
                "[user-send] token(s) could not deliver to channel %s (%s)",
                target_channel_id,
                last,
            )
            return last

        if (typing and type_token) or float(max_delay or 0.0) > 0:
            async with self._chan_lock(chan):
                return await _deliver()
        return await _deliver()

    def _token_value_for(
        self, mapping_id: Optional[str], token_id: Optional[str]
    ) -> Optional[str]:
        """The token value behind a stored token id, if it is still usable."""
        if not mapping_id or not token_id:
            return None
        try:
            tokens = self._filter_dead(
                self._db.get_enabled_mapping_tokens(str(mapping_id)) or []
            )
        except Exception:
            self._log.exception(
                "[user-send] Failed to load tokens for mapping %s", mapping_id
            )
            return None
        for t in tokens:
            if str(t.get("token_id")) == str(token_id):
                return (t.get("token_value") or "").strip() or None
        return None

    async def edit_message(
        self,
        *,
        mapping_id: Optional[str],
        channel_id: int,
        message_id: int,
        token_id: Optional[str],
        content: Optional[str],
        embeds: Optional[list] = None,
        use_proxy: bool = False,
        guild_id=None,
    ) -> str:
        """Edit a message, as the account that posted it.

        Discord only lets an author edit their own message, so this is not a
        "pick any token" operation the way sending is — it needs the exact
        account recorded when the message went out.
        """
        token = self._token_value_for(mapping_id, token_id)
        if not token:
            return SEND_NO_TOKENS

        text = self._compose_text(content, embeds)
        if len(text) > MAX_CONTENT_LEN:
            text = text[: MAX_CONTENT_LEN - 1] + "…"

        status, _ = await self._request_with_token(
            token,
            f"{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}",
            method="PATCH",
            json_body={"content": text},
            timeout=30,
            ctx=f"editing message {message_id}",
            use_proxy=use_proxy,
            referer=channel_referer(channel_id, guild_id),
            not_found_status=SEND_NOT_FOUND,
        )
        return status

    async def delete_message(
        self,
        *,
        mapping_id: Optional[str],
        channel_id: int,
        message_id: int,
        token_id: Optional[str],
        use_proxy: bool = False,
        guild_id=None,
    ) -> str:
        """Delete a message, as the account that posted it.

        Returns ``SEND_NOT_FOUND`` when it is already gone, which the caller
        treats as success — the desired end state is the same.
        """
        token = self._token_value_for(mapping_id, token_id)
        if not token:
            return SEND_NO_TOKENS

        status, _ = await self._request_with_token(
            token,
            f"{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}",
            method="DELETE",
            timeout=30,
            ctx=f"deleting message {message_id}",
            use_proxy=use_proxy,
            referer=channel_referer(channel_id, guild_id),
            not_found_status=SEND_NOT_FOUND,
        )
        return status

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
        use_proxy: bool = False,
    ) -> Optional[int]:
        """
        Create a forum thread whose starter message is authored by one of the
        mapping's user tokens.
        """
        if not mapping_id or not forum_channel_id or not thread_name:
            return None

        try:
            tokens = self._filter_dead(
                self._db.get_enabled_mapping_tokens(str(mapping_id)) or []
            )
        except Exception:
            self._log.exception(
                "[user-send] Failed to load tokens for mapping %s", mapping_id
            )
            return None
        if not tokens:
            return None

        text = self._compose_text(content, embeds)
        atts = [a for a in (attachments or []) if isinstance(a, dict) and a.get("url")]
        stkr_ids = [str(s) for s in (sticker_ids or []) if s]

        if not text and not atts and not stkr_ids:
            return None

        forum_id = int(forum_channel_id)
        atts_to_upload = [] if links_only else atts
        tag_ids = [str(t) for t in (applied_tag_ids or []) if t]

        # Same account ordering as send(): honour the identity manager's choice,

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
                    use_proxy=use_proxy,
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
        use_proxy: bool = False,
    ) -> Optional[int]:
        """
        Create a text-channel thread as one of the mapping's user tokens.
        """
        if not mapping_id or not parent_channel_id or not thread_name:
            return None

        try:
            tokens = self._filter_dead(
                self._db.get_enabled_mapping_tokens(str(mapping_id)) or []
            )
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
                    use_proxy=use_proxy,
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

    def _filter_dead(self, tokens: list) -> list:
        """Drop tokens already known revoked this run.

        ``get_enabled_mapping_tokens`` normally excludes them anyway once
        ``_mark_token_dead`` has flipped ``enabled`` in the DB; this covers
        the window before that write lands (or if it failed).
        """
        if not self._dead_tokens:
            return tokens
        return [
            t
            for t in tokens
            if (t.get("token_value") or "").strip() not in self._dead_tokens
        ]

    def _mark_token_dead(self, tok: dict) -> None:
        """Bench a 401'd token for this run and disable it in the DB so it
        stops being loaded at all and shows as disabled in the admin UI."""
        token_value = (tok.get("token_value") or "").strip()
        if token_value:
            self._dead_tokens.add(token_value)
        token_id = tok.get("token_id")
        if not token_id:
            return
        try:
            self._db.set_mapping_token_enabled(token_id, False)
        except Exception:
            self._log.exception(
                "[user-send] Failed to disable revoked token %s", token_id
            )
            return
        self._log.warning(
            "[user-send] Token %s returned 401 (revoked); disabled it",
            tok.get("username") or token_id,
        )

    def _order_tokens(
        self, tokens: list, chan: int, strategy: str, author_id, mapping_id=None
    ):
        """Return the tokens in the order to try, per the selected strategy."""
        toks = list(tokens)
        if len(toks) <= 1:
            return toks

        if strategy == "sticky_author" and author_id:

            ordered = sorted(toks, key=lambda t: str(t.get("token_id")))
            ids = [str(t.get("token_id")) for t in ordered]
            key = (str(mapping_id), str(author_id))
            self._author_last_seen[key] = time.monotonic()
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

        ordered = sorted(toks, key=lambda t: str(t.get("token_id")))
        self._channel_last_seen[chan] = time.monotonic()
        idx = self._rr_index_by_channel.get(chan, 0) % len(ordered)
        self._rr_index_by_channel[chan] = (idx + 1) % len(ordered)
        return ordered[idx:] + ordered[:idx]

    def _chan_lock(self, chan: int) -> asyncio.Lock:
        """Per-channel lock."""
        self._channel_last_seen[chan] = time.monotonic()
        lock = self._pace_locks.get(chan)
        if lock is None:
            lock = asyncio.Lock()
            self._pace_locks[chan] = lock
        return lock

    def _token_lock(self, key: str) -> asyncio.Lock:
        """Per-account lock held across a request so one account's messages take
        strict turns — a rate-limited account holds back its own queued messages
        instead of firing them all and collecting more 429s."""
        self._token_last_seen[key] = time.monotonic()
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

    async def reap_idle_state(self, idle_ttl: float = 3600) -> dict:
        """Drop lock/cooldown/rotation bookkeeping for tokens, channels, and
        sticky-author assignments untouched for ``idle_ttl`` seconds.

        Every dict this class keeps (token locks/cooldowns, per-channel
        pace locks/round-robin cursors, sticky-author assignments) is keyed
        by something that grows without bound over the process lifetime —
        tokens added over time, channels ever paced, or distinct authors
        ever seen — with nothing to shrink it. That's fine at small scale;
        at thousands of tokens or a high-churn source server it isn't.

        Nothing is lost permanently: a token/channel/author that becomes
        active again after being reaped just gets fresh state, identical in
        effect to its first-ever use (a round-robin cursor resets to 0, a
        sticky author gets freshly reassigned to the least-used token).
        Returns a dict of how many entries were reaped per category.

        Safe by construction, not by locking: ``_token_lock``/``_chan_lock``
        synchronously stamp last-seen to "now" *before* returning the lock
        object, with no ``await`` in between — so any key genuinely in
        concurrent use can never show up as stale here. The ``.locked()``
        checks below are therefore a non-blocking belt-and-suspenders guard,
        not something this ever needs to wait on; blocking on a held lock
        (an earlier version of this did) risks stalling the whole periodic
        sweep on one busy token.
        """
        tokens = self._reap_idle_token_state(idle_ttl)
        channels = self._reap_idle_channel_state(idle_ttl)
        authors = self._reap_idle_author_state(idle_ttl)
        if tokens or channels or authors:
            self._log.debug(
                "[user-send] Reaped idle bookkeeping: %d token(s), %d channel(s), "
                "%d author(s)",
                tokens,
                channels,
                authors,
            )
        return {"tokens": tokens, "channels": channels, "authors": authors}

    def _reap_idle_token_state(self, idle_ttl: float) -> int:
        now = time.monotonic()
        reaped = 0
        for token in [
            k for k, t in list(self._token_last_seen.items()) if now - t >= idle_ttl
        ]:
            lock = self._token_locks.get(token)
            if lock is not None and lock.locked():
                continue
            self._token_locks.pop(token, None)
            self._token_cooldown.pop(token, None)
            self._token_last_seen.pop(token, None)
            reaped += 1
        return reaped

    def _reap_idle_channel_state(self, idle_ttl: float) -> int:
        now = time.monotonic()
        reaped = 0
        for chan in [
            k for k, t in list(self._channel_last_seen.items()) if now - t >= idle_ttl
        ]:
            lock = self._pace_locks.get(chan)
            if lock is not None and lock.locked():
                continue
            self._pace_locks.pop(chan, None)
            self._rr_index_by_channel.pop(chan, None)
            self._channel_last_seen.pop(chan, None)
            reaped += 1
        return reaped

    def _reap_idle_author_state(self, idle_ttl: float) -> int:

        now = time.monotonic()
        stale = [
            k for k, t in list(self._author_last_seen.items()) if now - t >= idle_ttl
        ]
        for key in stale:
            self._sticky_author_token.pop(key, None)
            self._author_last_seen.pop(key, None)
        return len(stale)

    async def _tls_session(self, token: str, *, use_proxy: bool = False):
        """The per-token curl_cffi session. Overridden in tests."""
        return await get_tls_session(token, use_proxy=use_proxy)

    async def _request_with_token(
        self,
        token: str,
        url: str,
        *,
        json_body: Optional[dict] = None,
        mime_factory: Optional[Callable[[], "CurlMime"]] = None,
        timeout: int = 30,
        ctx: str = "",
        extra_headers: Optional[dict] = None,
        use_proxy: bool = False,
        method: str = "POST",
        not_found_status: str = SEND_UNDELIVERABLE,
        referer: Optional[str] = None,
    ) -> tuple[str, Optional[dict]]:
        """Call the API as one account, with rate-limit gating and retries.

        ``not_found_status`` lets a caller separate 404 from 403, which matter
        differently once this is used for more than sending: a delete whose
        target is already gone is done, while one that is forbidden is not.
        Sends keep conflating them, which is the behaviour they had.
        """
        r429 = 0
        rnet = 0
        async with self._token_lock(token):
            while True:
                await self._await_cooldown(token)

                # this token's session was primed through a proxy (by any

                # prior iteration's report_failure() may have swapped it.
                proxy = get_proxy_pool().current(token)
                try:
                    session = await self._tls_session(token, use_proxy=use_proxy)
                    headers = self._build_headers(token, referer)
                    if extra_headers:
                        headers = {**headers, **extra_headers}

                    if mime_factory is not None:
                        kwargs = {"multipart": mime_factory()}
                    elif json_body is not None:
                        kwargs = {"json": json_body}
                    else:
                        # DELETE carries no body.
                        kwargs = {}
                    kwargs.update(headers=headers, timeout=timeout, proxy=proxy)

                    if method == "POST":
                        resp = await session.post(url, **kwargs)
                    else:
                        resp = await session.request(method, url, **kwargs)

                    status = resp.status_code

                    if status in (200, 201, 204):
                        if proxy:
                            get_proxy_pool().report_success(proxy)
                        try:
                            data = resp.json()
                        except Exception:
                            data = None
                        return SEND_OK, data

                    if status == 429:
                        retry_after = self._retry_after(resp)
                        wait = min(max(0.0, retry_after), 60.0)

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

                        self._log.debug("[user-send] token revoked (HTTP 401) %s", ctx)
                        await close_tls_session(token, reason="revoked-401")
                        return SEND_DEAD, None

                    if status == 404:
                        self._log.debug(
                            "[user-send] target does not exist (HTTP 404) %s", ctx
                        )
                        return not_found_status, None

                    if status == 403:

                        self._log.debug(
                            "[user-send] token can't deliver (HTTP 403) %s", ctx
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

                    body = self._safe_text(resp)
                    self._log.warning(
                        "[user-send] Discord returned HTTP %s %s: %s",
                        status,
                        ctx,
                        body[:300],
                    )
                    return SEND_UNDELIVERABLE, None
                except CurlRequestException as e:
                    rnet += 1
                    self._log.warning(
                        "[user-send] Network error %s (attempt %d): %s", ctx, rnet, e
                    )
                    if proxy:

                        await get_proxy_pool().report_failure(token)
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
        use_proxy: bool = False,
    ) -> None:
        """Wait a message's send-delay before it goes out."""
        lo = max(0.0, float(min_delay or 0.0))
        hi = max(lo, float(max_delay or 0.0))
        do_type = bool(typing and token)
        if hi <= 0 and not do_type:
            return

        delay = random.uniform(lo, min(hi, 30.0)) if hi > 0 else 0.0
        if do_type:
            await self._type_for(chan, token, delay, use_proxy=use_proxy)
        elif delay > 0:
            await asyncio.sleep(delay)

    async def _type_for(
        self, chan: int, token: str, duration: float, *, use_proxy: bool = False
    ) -> None:
        """Show the typing indicator in ``chan`` for ``duration`` seconds."""
        headers = self._build_headers(token)
        url = f"{DISCORD_API_BASE}/channels/{chan}/typing"

        async def _fire():
            self._token_last_seen[token] = time.monotonic()
            try:
                session = await self._tls_session(token, use_proxy=use_proxy)
                proxy = get_proxy_pool().current(token)
                await session.post(url, headers=headers, timeout=10, proxy=proxy)
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
        use_proxy: bool = False,
        reply_to: Optional[dict] = None,
        guild_id=None,
    ) -> str:
        """Post a message as one account. Returns a ``SEND_*`` status."""
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
            return SEND_UNSUPPORTED

        url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
        ctx = f"posting to channel {channel_id}"

        extra = {"X-Context-Properties": _CHAT_INPUT_CONTEXT}

        reply_bits = _reply_bits(reply_to)
        referer = channel_referer(channel_id, guild_id)

        if files:
            status, data = await self._request_with_token(
                token,
                url,
                mime_factory=lambda: self._build_multipart(
                    body_text, files, stkr_ids, reply_bits=reply_bits
                ),
                timeout=60,
                ctx=ctx,
                extra_headers=extra,
                use_proxy=use_proxy,
                referer=referer,
            )
        else:
            payload = {"content": body_text}
            if stkr_ids:
                payload["sticker_ids"] = stkr_ids
            payload.update(reply_bits)
            status, data = await self._request_with_token(
                token,
                url,
                json_body=payload,
                timeout=30,
                ctx=ctx,
                extra_headers=extra,
                use_proxy=use_proxy,
                referer=referer,
            )

        cloned_message_id = None
        if status == SEND_OK and isinstance(data, dict):
            try:
                cloned_message_id = int(data.get("id") or 0) or None
            except (TypeError, ValueError):
                cloned_message_id = None
        return status, cloned_message_id

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
        use_proxy: bool = False,
    ) -> Optional[int]:
        """POST a forum thread + starter message as one account."""
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
                mime_factory=lambda: self._build_forum_multipart(thread_body, files),
                timeout=60,
                ctx=ctx,
                use_proxy=use_proxy,
            )
        else:
            status, data = await self._request_with_token(
                token,
                url,
                json_body=thread_body,
                timeout=30,
                ctx=ctx,
                use_proxy=use_proxy,
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
        use_proxy: bool = False,
    ) -> Optional[int]:
        """Create a text-channel thread as one account."""
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
            body["type"] = 11

        status, data = await self._request_with_token(
            token,
            url,
            json_body=body,
            timeout=30,
            ctx=f"creating text thread in {parent_channel_id}",
            use_proxy=use_proxy,
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
        """Download attachments and return (files, uploaded_urls)."""
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
                self._log.debug(
                    "[user-send] Failed to download attachment %s", url, exc_info=True
                )
                continue

            filename = a.get("filename") or self._basename(url) or "file.bin"
            files.append((filename, data, ctype))
            uploaded_urls.add(url)

        return files, uploaded_urls

    def _build_multipart(
        self,
        content: str,
        files: list,
        sticker_ids: Optional[list] = None,
        *,
        reply_bits: Optional[dict] = None,
    ) -> "CurlMime":
        payload = {
            "content": content or "",
            "attachments": [
                {"id": i, "filename": fn} for i, (fn, _data, _ct) in enumerate(files)
            ],
        }
        if sticker_ids:
            payload["sticker_ids"] = [str(s) for s in sticker_ids]
        if reply_bits:
            payload.update(reply_bits)
        mime = CurlMime()
        mime.addpart(
            "payload_json",
            content_type="application/json",
            data=json.dumps(payload).encode(),
        )
        for i, (fn, data, ct) in enumerate(files):
            mime.addpart(f"files[{i}]", content_type=ct, filename=fn, data=data)
        return mime

    def _build_forum_multipart(self, thread_body: dict, files: list) -> "CurlMime":
        """Multipart body for creating a forum thread with file attachments."""
        body = dict(thread_body)
        message = dict(body.get("message") or {})
        message["attachments"] = [
            {"id": i, "filename": fn} for i, (fn, _data, _ct) in enumerate(files)
        ]
        body["message"] = message
        mime = CurlMime()
        mime.addpart(
            "payload_json",
            content_type="application/json",
            data=json.dumps(body).encode(),
        )
        for i, (fn, data, ct) in enumerate(files):
            mime.addpart(f"files[{i}]", content_type=ct, filename=fn, data=data)
        return mime

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

    @staticmethod
    def _basename(url: str) -> str:
        try:
            path = url.split("?", 1)[0]
            return path.rsplit("/", 1)[-1]
        except Exception:
            return ""

    @staticmethod
    def _retry_after(resp) -> float:
        try:
            data = resp.json()
            return float(data.get("retry_after", 1.0))
        except Exception:
            hdr = resp.headers.get("Retry-After")
            try:
                return float(hdr) if hdr else 1.0
            except Exception:
                return 1.0

    @staticmethod
    def _safe_text(resp) -> str:
        try:
            return resp.text
        except Exception:
            return ""

    def _build_headers(self, token: str, referer: str | None = None) -> dict:
        """Realistic Discord desktop-client headers, unique & stable per token."""
        # The suppression entries are curl_cffi-only (None deletes a
        # header); this is the curl_cffi path, so merge them here.
        return {
            **build_headers(token, referer=referer),
            **SUPPRESSED_PROFILE_HEADERS,
        }
