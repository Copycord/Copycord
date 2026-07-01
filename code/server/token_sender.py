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

When a guild mapping has ``USE_USER_TOKENS`` enabled and one or more user
tokens attached, cloned messages are posted into the clone guild by a randomly
chosen user account via the raw Discord REST API instead of a channel webhook.

Limitations (by design — see the guild-mappings feature notes):
  * User accounts cannot post rich embeds → embeds are flattened into text.
  * User accounts cannot set a per-message username/avatar → the message appears
    as whichever account posted it.
  * Attachments are re-downloaded and re-uploaded as real files (multipart).
  * The account must be a member of the clone guild with permission to post in
    the target channel, otherwise that token is skipped.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import random
from typing import Callable, Optional

import aiohttp

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
        # Last token id used per channel, to avoid back-to-back repeats.
        self._last_token_by_channel: dict = {}

    # ── public API ───────────────────────────────────────────────────────────

    async def send(
        self,
        *,
        mapping_id: Optional[str],
        target_channel_id: int,
        content: Optional[str],
        embeds: Optional[list] = None,
        attachments: Optional[list] = None,
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

        # Random pick per message, but avoid using the same account twice in a
        # row in the same channel. Otherwise a burst of messages can all land on
        # one token, and Discord groups consecutive same-author messages under a
        # single header — defeating the point of spreading sends across
        # accounts. With a single token there is nothing to spread (no-op).
        order = list(tokens)
        random.shuffle(order)

        chan = int(target_channel_id)
        last_tid = self._last_token_by_channel.get(chan)
        if (
            last_tid is not None
            and len(order) > 1
            and order[0].get("token_id") == last_tid
        ):
            for i in range(1, len(order)):
                if order[i].get("token_id") != last_tid:
                    order[0], order[i] = order[i], order[0]
                    break

        rl_key = f"channel:{target_channel_id}"

        for tok in order:
            token_value = (tok.get("token_value") or "").strip()
            if not token_value:
                continue
            try:
                await self._ratelimit.acquire(self._action, key=rl_key)
            except Exception:
                pass
            try:
                ok = await self._send_with_token(
                    token_value, int(target_channel_id), text, atts
                )
            except Exception:
                self._log.exception(
                    "[user-send] Unexpected error sending to channel %s", target_channel_id
                )
                ok = False

            if ok:
                tid = tok.get("token_id")
                if tid:
                    self._last_token_by_channel[chan] = tid
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

    # ── send implementation ──────────────────────────────────────────────────

    async def _send_with_token(
        self, token: str, channel_id: int, text: str, attachments: list
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
                        try:
                            self._ratelimit.penalize(
                                self._action, retry_after, key=f"channel:{channel_id}"
                            )
                        except Exception:
                            pass
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

        Each account presents a *distinct* but *consistent* device fingerprint:
        the super-properties and User-Agent are derived deterministically from
        the token, so one account always looks like the same machine across
        requests (and process restarts), while different accounts never share a
        fingerprint. That mirrors how real clients behave — a fleet of accounts
        all sending an identical fingerprint is itself a detection signal.
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
        # Deterministic per-token RNG → a stable, unique fingerprint per account.
        seed = int(hashlib.sha256(token.encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed)

        chrome_major = rng.choice([128, 130, 132, 134, 136, 139])
        chrome_version = (
            f"{chrome_major}.0.{rng.randint(6000, 7300)}.{rng.randint(30, 220)}"
        )
        client_version = rng.choice(
            ["1.0.9179", "1.0.9163", "1.0.9156", "1.0.9154"]
        )
        electron_version = rng.choice(
            ["32.2.5", "31.3.1", "28.2.10", "22.3.26"]
        )
        os_version = rng.choice(
            ["10.0.19045", "10.0.22621", "10.0.22631", "10.0.26100"]
        )
        build_number = rng.randint(330000, 366000)
        native_build = rng.randint(60000, 64000)
        locale = rng.choice(
            ["en-US", "en-GB", "de", "fr", "es-ES", "nl", "pt-BR"]
        )
        tz = rng.choice(
            [
                "America/New_York",
                "America/Chicago",
                "America/Los_Angeles",
                "Europe/London",
                "Europe/Berlin",
                "Europe/Amsterdam",
                "Asia/Tokyo",
            ]
        )

        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            f"discord/{client_version} Chrome/{chrome_version} "
            f"Electron/{electron_version} Safari/537.36"
        )

        # Shape matches discord.py-self's super-properties for the desktop app.
        super_props = {
            "os": "Windows",
            "browser": "Discord Client",
            "release_channel": "stable",
            "client_version": client_version,
            "os_version": os_version,
            "os_arch": "x64",
            "app_arch": "x64",
            "system_locale": locale,
            "browser_user_agent": user_agent,
            "browser_version": chrome_version,
            "client_build_number": build_number,
            "native_build_number": native_build,
            "client_event_source": None,
            "device": "",
            "referrer": "",
            "referring_domain": "",
            "referrer_current": "",
            "referring_domain_current": "",
        }
        super_props_b64 = base64.b64encode(
            json.dumps(super_props, separators=(",", ":")).encode()
        ).decode()

        headers = {
            "User-Agent": user_agent,
            "X-Discord-Locale": locale,
            "X-Discord-Timezone": tz,
            "Accept": "*/*",
            "Accept-Language": f"{locale},en;q=0.9",
            "Origin": "https://discord.com",
            "Referer": "https://discord.com/channels/@me",
            "Sec-CH-UA": (
                f'"Chromium";v="{chrome_major}", '
                f'"Not(A:Brand";v="24", '
                f'"Google Chrome";v="{chrome_major}"'
            ),
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
        }
        return {"headers": headers, "super_props_b64": super_props_b64}
