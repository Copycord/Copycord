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
import os
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import aiohttp
import discord
import re
import html
from client.message_utils import _resolve_forward, _resolve_forward_via_snapshot

log = logging.getLogger(__name__)

DISCORD_WEBHOOK_RE = re.compile(
    r"^https?://(canary\.|ptb\.)?discord(app)?\.com/api/webhooks/\d+/.+", re.I
)


class RetryableForwardingError(Exception):
    """
    Raised to signal the worker that this job should be retried.
    Optionally includes a server-provided delay (e.g., retry_after seconds).
    """

    def __init__(
        self,
        msg: str,
        *,
        delay: float | None = None,
        status: int | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(msg)
        self.delay = delay
        self.status = status
        self.body = body or ""


def _safe_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _extract_retry_after_from_body(text: str) -> float | None:
    """
    Supports:
    - Discord: {"retry_after": 1.23, ...}
    - Telegram: {"parameters": {"retry_after": 3}, ...}
    """
    data = _safe_json_loads(text)
    if not isinstance(data, dict):
        return None

    ra = data.get("retry_after")
    if isinstance(ra, (int, float)) and ra > 0:
        return float(ra)

    params = data.get("parameters")
    if isinstance(params, dict):
        ra2 = params.get("retry_after")
        if isinstance(ra2, (int, float)) and ra2 > 0:
            return float(ra2)

    return None


def _extract_retry_after_from_headers(
    headers: aiohttp.typedefs.LooseHeaders,
) -> float | None:
    """
    Discord provides:
    - Retry-After (seconds)
    - X-RateLimit-Reset-After (seconds)
    """
    try:

        ra = headers.get("Retry-After") if headers else None
        if ra:
            try:
                v = float(ra)
                return v if v > 0 else None
            except Exception:
                pass

        xra = headers.get("X-RateLimit-Reset-After") if headers else None
        if xra:
            try:
                v = float(xra)
                return v if v > 0 else None
            except Exception:
                pass
    except Exception:
        return None

    return None


def _clip(s: str, limit: int) -> str:
    s = s or ""
    return s if len(s) <= limit else (s[: limit - 3] + "...")


def _sanitize_discord_embed_for_webhook(e: dict) -> dict | None:
    """
    Keep only fields that Discord webhooks accept for outgoing embeds.
    Drops keys that are commonly present in incoming embeds but not useful/accepted.
    """
    if not isinstance(e, dict):
        return None

    out: dict = {}

    def _str(v: object) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    for k in ("title", "description", "url", "timestamp"):
        v = _str(e.get(k))
        if v:
            out[k] = v

    c = e.get("color")
    if isinstance(c, int):
        out["color"] = c

    footer = e.get("footer")
    if isinstance(footer, dict):
        ft = _str(footer.get("text"))
        fi = _str(footer.get("icon_url"))
        if ft or fi:
            out["footer"] = {}
            if ft:
                out["footer"]["text"] = ft
            if fi:
                out["footer"]["icon_url"] = fi

    author = e.get("author")
    if isinstance(author, dict):
        an = _str(author.get("name"))
        au = _str(author.get("url"))
        ai = _str(author.get("icon_url"))
        if an or au or ai:
            out["author"] = {}
            if an:
                out["author"]["name"] = an
            if au:
                out["author"]["url"] = au
            if ai:
                out["author"]["icon_url"] = ai

    for k in ("image", "thumbnail"):
        obj = e.get(k)
        if isinstance(obj, dict):
            u = _str(obj.get("url"))
            if u:
                out[k] = {"url": u}

    fields_in = e.get("fields")
    if isinstance(fields_in, list):
        fields_out: list[dict] = []
        for f in fields_in:
            if not isinstance(f, dict):
                continue
            n = _str(f.get("name"))
            v = _str(f.get("value"))
            if not (n and v):
                continue
            fo = {"name": n, "value": v}
            if isinstance(f.get("inline"), bool):
                fo["inline"] = f["inline"]
            fields_out.append(fo)
        if fields_out:
            out["fields"] = fields_out

    return out or None


def _extract_embed_image_urls(embeds: list[dict]) -> set[str]:
    urls: set[str] = set()
    for e in embeds or []:
        if not isinstance(e, dict):
            continue
        for k in ("image", "thumbnail"):
            obj = e.get(k)
            if isinstance(obj, dict):
                u = (obj.get("url") or "").strip()
                if u:
                    urls.add(u)
    return urls


@dataclass
class ForwardingFilters:
    include_channels: list[int]
    exclude_channels: list[int]
    include_users: list[int]
    exclude_users: list[int]
    include_roles: list[int]
    exclude_roles: list[int]

    include_keywords: list[str]
    require_all_keywords: list[str]
    exclude_keywords: list[str]

    case_sensitive: bool = False
    include_bots: bool = False
    include_embeds: bool = False
    has_attachments: bool = False

    @staticmethod
    def _parse_int_list(val: Any) -> list[int]:
        nums: list[int] = []

        if isinstance(val, str):
            tokens = re.split(r"[,\s]+", val.strip())
        elif isinstance(val, (list, tuple, set)):
            tokens = list(val)
        else:
            tokens = []

        for x in tokens:
            s = str(x).strip()
            if not s:
                continue
            if not s.isdigit():
                continue
            try:
                nums.append(int(s))
            except Exception:
                continue

        return nums

    @staticmethod
    def _parse_str_list(val: Any) -> list[str]:
        items: list[str] = []

        if isinstance(val, str):
            tokens = re.split(r"[,\n]+", val)
        elif isinstance(val, (list, tuple, set)):
            tokens = list(val)
        else:
            tokens = []

        for x in tokens:
            s = str(x).strip()
            if not s:
                continue
            items.append(s)

        return items

    @classmethod
    def from_dict(cls, data: dict) -> "ForwardingFilters":
        data = data or {}

        case_sensitive = bool(data.get("case_sensitive", False))
        include_bots = bool(data.get("include_bots", False))
        include_embeds = bool(data.get("include_embeds", False))
        has_attachments = bool(data.get("has_attachments", False))

        raw_channels = data.get("include_channels")
        if raw_channels is None:
            raw_channels = data.get("channel_ids", [])

        raw_users = data.get("include_users")
        if raw_users is None:
            raw_users = data.get("user_ids", [])

        raw_kw_any = data.get("include_keywords")
        if raw_kw_any is None:
            raw_kw_any = data.get("keywords_any", [])

        raw_kw_all = data.get("require_all_keywords")
        if raw_kw_all is None:
            raw_kw_all = data.get("keywords_all", [])

        raw_kw_excl = data.get("exclude_keywords", [])

        include_channels = cls._parse_int_list(raw_channels)
        exclude_channels = cls._parse_int_list(data.get("exclude_channels", []))
        include_users = cls._parse_int_list(raw_users)
        exclude_users = cls._parse_int_list(data.get("exclude_users", []))
        include_roles = cls._parse_int_list(data.get("include_roles", []))
        exclude_roles = cls._parse_int_list(data.get("exclude_roles", []))

        kw_any = cls._parse_str_list(raw_kw_any)
        kw_all = cls._parse_str_list(raw_kw_all)
        kw_excl = cls._parse_str_list(raw_kw_excl)

        if not case_sensitive:
            kw_any = [k.lower() for k in kw_any]
            kw_all = [k.lower() for k in kw_all]
            kw_excl = [k.lower() for k in kw_excl]

        return cls(
            include_channels=include_channels,
            exclude_channels=exclude_channels,
            include_users=include_users,
            exclude_users=exclude_users,
            include_roles=include_roles,
            exclude_roles=exclude_roles,
            include_keywords=kw_any,
            require_all_keywords=kw_all,
            exclude_keywords=kw_excl,
            case_sensitive=case_sensitive,
            include_bots=include_bots,
            include_embeds=include_embeds,
            has_attachments=has_attachments,
        )

    def apply(self, attrs: dict) -> bool:
        channel_id = attrs.get("channel_id")
        author_id = attrs.get("author_id")
        role_ids = attrs.get("role_ids") or []
        is_bot = bool(attrs.get("is_bot", False))

        has_attachments = bool(attrs.get("has_attachments", False))
        if not has_attachments:
            atts = attrs.get("attachments") or []
            has_attachments = any(
                isinstance(a, dict)
                and ((a.get("url") or "").strip() or (a.get("filename") or "").strip())
                for a in atts
            )

        if self.has_attachments and not has_attachments:
            return False

        if is_bot and not self.include_bots:
            return False

        content = attrs.get("content") or ""
        if self.include_embeds:
            for e in attrs.get("embeds") or []:
                if not isinstance(e, dict):
                    continue

                title = (e.get("title") or "").strip()
                desc = (e.get("description") or "").strip()

                if title:
                    content += "\n" + title
                if desc:
                    content += "\n" + desc

                for f in e.get("fields") or []:
                    if not isinstance(f, dict):
                        continue
                    n = (f.get("name") or "").strip()
                    v = (f.get("value") or "").strip()
                    if n and v:
                        content += f"\n{n}: {v}"
                    elif n:
                        content += "\n" + n
                    elif v:
                        content += "\n" + v

        attachments = attrs.get("attachments") or []
        for att in attachments:
            if not isinstance(att, dict):
                continue
            fname = att.get("filename") or ""
            if fname:
                content += f"\n{fname}"
            url = att.get("url") or ""
            if url:
                content += f"\n{url}"

        haystack = content if self.case_sensitive else content.lower()

        if self.include_channels and channel_id not in self.include_channels:
            return False
        if self.exclude_channels and channel_id in self.exclude_channels:
            return False

        if self.include_users and author_id not in self.include_users:
            return False
        if self.exclude_users and author_id in self.exclude_users:
            return False

        if self.include_roles and not any(r in role_ids for r in self.include_roles):
            return False
        if self.exclude_roles and any(r in role_ids for r in self.exclude_roles):
            return False

        if self.include_keywords and not any(
            k in haystack for k in self.include_keywords
        ):
            return False

        if self.require_all_keywords and not all(
            k in haystack for k in self.require_all_keywords
        ):
            return False

        if self.exclude_keywords and any(k in haystack for k in self.exclude_keywords):
            return False

        return True


@dataclass
class ForwardingRule:
    rule_id: str
    guild_id: int
    label: str
    provider: str
    enabled: bool
    config: dict
    filters: ForwardingFilters


@dataclass
class ForwardingJob:
    """
    A single forwarding "work item" that gets enqueued and processed by a provider worker.
    """

    provider_queue: str
    rule: ForwardingRule
    message_id: str
    attrs: dict
    attempts: int = 0
    created_monotonic: float = 0.0

    def __post_init__(self) -> None:
        if not self.created_monotonic:
            self.created_monotonic = time.monotonic()


class ForwardingManager:
    """
    Manages per-guild forwarding rules for Copycord.

    Async provider queues (Telegram / Pushover / Discord webhook).
    - Each provider has its own asyncio.Queue and its own worker task(s).
    - Queues drain concurrently.
    """

    def __init__(
        self,
        *,
        config: Any,
        db: Any,
        ws: Any,
        logger: Optional[logging.Logger] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.config = config
        self.db = db
        self.ws = ws
        self.log = logger or log
        self.loop = loop or asyncio.get_event_loop()

        self._rules_cache: Dict[int, List[ForwardingRule]] = {}
        self._cache_lock = asyncio.Lock()

        self._started = False
        self._closing = False
        self._start_lock = asyncio.Lock()

        self._queue_maxsize = int(os.getenv("FORWARDING_QUEUE_MAXSIZE", "2000"))
        self._max_attempts = int(os.getenv("FORWARDING_QUEUE_MAX_ATTEMPTS", "3"))
        self._retry_max_delay = float(os.getenv("FORWARDING_RETRY_MAX_DELAY", "60"))

        self._workers_per_provider: Dict[str, int] = {
            "telegram": int(os.getenv("FORWARDING_WORKERS_TELEGRAM", "1")),
            "pushover": int(os.getenv("FORWARDING_WORKERS_PUSHOVER", "1")),
            "discord": int(os.getenv("FORWARDING_WORKERS_DISCORD", "1")),
        }

        self._queues: Dict[str, asyncio.Queue] = {
            "telegram": asyncio.Queue(maxsize=self._queue_maxsize),
            "pushover": asyncio.Queue(maxsize=self._queue_maxsize),
            "discord": asyncio.Queue(maxsize=self._queue_maxsize),
        }

        self._worker_tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return
            self._started = True
            self._closing = False

            for provider, q in self._queues.items():
                n = max(1, int(self._workers_per_provider.get(provider, 1) or 1))
                for idx in range(n):
                    t = asyncio.create_task(
                        self._provider_worker(
                            provider=provider, worker_idx=idx, queue=q
                        ),
                        name=f"forwarding_worker:{provider}:{idx}",
                    )
                    self._worker_tasks.append(t)

    async def close(
        self, *, drain: bool = False, timeout: Optional[float] = 30.0
    ) -> None:
        if not self._started or self._closing:
            return

        self._closing = True

        if drain:
            try:
                await asyncio.wait_for(self.drain(), timeout=timeout)
            except Exception:
                self.log.warning(
                    "[⏩] Drain timed out or failed; stopping anyway",
                    exc_info=True,
                )

        for provider, q in self._queues.items():
            n = max(1, int(self._workers_per_provider.get(provider, 1) or 1))
            for _ in range(n):
                try:
                    q.put_nowait(None)
                except asyncio.QueueFull:
                    asyncio.create_task(q.put(None))

        try:
            await asyncio.wait_for(
                asyncio.gather(*self._worker_tasks, return_exceptions=True),
                timeout=timeout,
            )
        except Exception:
            self.log.warning("[⏩] Worker shutdown timed out", exc_info=True)

        self._worker_tasks.clear()
        self._started = False

    async def drain(self) -> None:
        await asyncio.gather(*(q.join() for q in self._queues.values()))

    def queue_sizes(self) -> dict[str, int]:
        return {k: q.qsize() for k, q in self._queues.items()}

    async def reload_config(self) -> None:
        try:
            rows = self.db.list_message_forwarding_rules(guild_id=None) or []
        except Exception:
            self.log.exception("[⏩] Failed to eager reload forwarding rules")
            return

        by_guild: Dict[int, List[ForwardingRule]] = {}

        for row in rows:
            item = dict(row)

            cfg = item.get("config")
            flt = item.get("filters")
            if isinstance(cfg, str):
                try:
                    item["config"] = json.loads(cfg)
                except Exception:
                    self.log.warning(
                        "[⏩] Invalid JSON in forwarding config",
                    )
                    item["config"] = {}
            if isinstance(flt, str):
                try:
                    item["filters"] = json.loads(flt)
                except Exception:
                    self.log.warning(
                        "[⏩] Invalid JSON in forwarding filters",
                    )
                    item["filters"] = {}

            rule = self._parse_rule(item)
            if not rule:
                continue

            by_guild.setdefault(rule.guild_id, []).append(rule)

        async with self._cache_lock:
            self._rules_cache = by_guild

    async def handle_new_message(
        self,
        *,
        discord_message: discord.Message,
        attrs_override: Optional[dict] = None,
        bot: Any = None,
    ) -> None:
        if not self._started:
            await self.start()
        await self.handle_message(
            discord_message, attrs_override=attrs_override, bot=bot
        )

    async def handle_message(
        self,
        message: discord.Message,
        *,
        attrs_override: Optional[dict] = None,
        bot: Any = None,
    ) -> None:
        guild = getattr(message, "guild", None)
        if not guild:
            return

        rules = await self._get_rules_for_guild(int(guild.id))
        if not rules:
            return

        src_msg = message
        if attrs_override is None:
            src_msg = await self._maybe_resolve_forward(bot, message)
            if src_msg is None:
                return
            attrs = self._get_message_attributes(src_msg)
        else:
            attrs = dict(attrs_override)

        for rule in rules:
            if rule.enabled and rule.filters.apply(attrs):
                await self._dispatch_forwarding(rule, attrs)

    def _looks_like_forward_wrapper(self, message: discord.Message) -> bool:
        raw = (getattr(message, "content", "") or "").strip()
        sys = (getattr(message, "system_content", "") or "").strip()
        if raw or sys:
            return False

        if (
            getattr(message, "attachments", None)
            or getattr(message, "embeds", None)
            or getattr(message, "stickers", None)
        ):
            return False

        forwarded_flag_val = 0
        try:
            forwarded_flag_val = int(
                getattr(getattr(message, "flags", 0), "value", 0) or 0
            )
        except Exception:
            pass

        return bool(getattr(message, "reference", None) or (forwarded_flag_val & 16384))

    async def _maybe_resolve_forward(
        self, bot: Any, wrapper_msg: discord.Message
    ) -> Optional[discord.Message]:
        if bot is None or not self._looks_like_forward_wrapper(wrapper_msg):
            return wrapper_msg

        resolved = await _resolve_forward(bot, wrapper_msg)
        if resolved is None:
            resolved = await _resolve_forward_via_snapshot(
                bot, wrapper_msg, logger=self.log
            )

        return resolved

        guild = message.guild
        if not guild:
            return

        guild_id = int(guild.id)
        rules = await self._get_rules_for_guild(guild_id)
        if not rules:
            return

        attrs = self._get_message_attributes(message)

        for rule in rules:
            if not rule.enabled:
                continue

            try:
                if rule.filters.apply(attrs):
                    await self._dispatch_forwarding(rule, attrs)
            except Exception:
                self.log.exception(
                    "[⏩] Error while applying rule %s for guild %s",
                    rule.rule_id,
                    guild_id,
                )

    async def _get_rules_for_guild(self, guild_id: int) -> List[ForwardingRule]:
        async with self._cache_lock:
            cached = self._rules_cache.get(guild_id)
            if cached is not None:
                return cached

        try:
            rows = self.db.list_message_forwarding_rules(guild_id=guild_id) or []
        except Exception:
            self.log.exception(
                "[⏩] Failed to load forwarding rules from DB for guild %s",
                guild_id,
            )
            rules: List[ForwardingRule] = []
        else:
            rules = []
            for row in rows:
                item = dict(row)

                cfg = item.get("config")
                flt = item.get("filters")
                if isinstance(cfg, str):
                    try:
                        item["config"] = json.loads(cfg)
                    except Exception:
                        self.log.warning("[⏩] Invalid JSON in forwarding config")
                        item["config"] = {}
                if isinstance(flt, str):
                    try:
                        item["filters"] = json.loads(flt)
                    except Exception:
                        self.log.warning(
                            "[⏩] Invalid JSON in forwarding filters",
                        )
                        item["filters"] = {}

                rule = self._parse_rule(item)
                if rule:
                    rules.append(rule)

        async with self._cache_lock:
            self._rules_cache[guild_id] = rules

        return rules

    def _parse_rule(self, item: dict) -> Optional[ForwardingRule]:
        try:
            rule_id = str(item.get("rule_id") or "").strip()
            guild_id = int(item.get("guild_id"))
            label = str(item.get("label") or "").strip()
            provider_raw = str(item.get("provider") or "").strip().lower()
            enabled = bool(item.get("enabled", True))
            config = item.get("config") or {}
            filters_raw = item.get("filters") or {}
        except Exception:
            self.log.exception("[⏩] Failed to parse forwarding rule %r", item)
            return None

        provider = "discord" if provider_raw == "webhook" else provider_raw

        if provider_raw not in ("pushover", "telegram", "discord", "webhook"):
            self.log.warning(
                "[⏩] Invalid rule skipped | rule_id=%s guild_id=%s provider=%s",
                rule_id,
                guild_id,
                provider_raw,
            )
            return None

        if not rule_id or not guild_id:
            self.log.warning(
                "[⏩] Invalid rule skipped | rule_id=%s guild_id=%s provider=%s",
                rule_id,
                guild_id,
                provider_raw,
            )
            return None

        if provider in ("discord",):
            url = (config.get("url") or "").strip()
            if not url or not DISCORD_WEBHOOK_RE.match(url):
                self.log.warning(
                    "[⏩] Non-Discord webhook is not supported; skipping rule | rule_id=%s provider=%s url=%s",
                    rule_id,
                    provider_raw,
                    (url[:80] + "...") if len(url) > 80 else url,
                )
                return None

        filters = ForwardingFilters.from_dict(filters_raw)

        return ForwardingRule(
            rule_id=rule_id,
            guild_id=guild_id,
            label=label or rule_id,
            provider=provider,
            enabled=enabled,
            config=config,
            filters=filters,
        )

    def _get_message_attributes(self, message: discord.Message) -> dict:
        author = message.author
        guild = message.guild

        try:
            author_id = int(author.id)
        except Exception:
            author_id = None

        try:
            channel_id = int(message.channel.id)
        except Exception:
            channel_id = None

        role_ids: Sequence[int] = []
        if (
            guild
            and isinstance(author, (discord.Member, discord.User))
            and hasattr(author, "roles")
        ):
            try:
                role_ids = [int(r.id) for r in getattr(author, "roles", [])]
            except Exception:
                role_ids = []

        try:
            is_bot = bool(getattr(author, "bot", False))
        except Exception:
            is_bot = False

        attachments = []
        try:
            for att in getattr(message, "attachments", []) or []:
                try:
                    attachments.append(
                        {
                            "url": att.url,
                            "filename": att.filename,
                            "size": getattr(att, "size", None),
                            "content_type": getattr(att, "content_type", None),
                        }
                    )
                except Exception:
                    continue
        except Exception:
            attachments = []

        has_attachments = any(
            isinstance(a, dict)
            and ((a.get("url") or "").strip() or (a.get("filename") or "").strip())
            for a in attachments
        )

        embeds = []
        try:
            for e in getattr(message, "embeds", []) or []:
                try:
                    embeds.append(e.to_dict())
                except Exception:
                    continue
        except Exception:
            embeds = []

        jump_url = getattr(message, "jump_url", None)

        author_name = None
        try:
            author_name = getattr(author, "display_name", None) or getattr(
                author, "name", None
            )
        except Exception:
            author_name = None

        channel_name = None
        try:
            channel_name = getattr(message.channel, "name", None) or str(channel_id)
        except Exception:
            channel_name = str(channel_id) if channel_id is not None else None

        msg_id = getattr(message, "id", None)
        msg_id_int = None
        try:
            msg_id_int = int(msg_id) if msg_id is not None else None
        except Exception:
            msg_id_int = None

        return {
            "message_id": msg_id_int,
            "guild_id": int(guild.id) if guild else None,
            "channel_id": channel_id,
            "channel_name": channel_name,
            "author_id": author_id,
            "author_name": author_name,
            "role_ids": role_ids,
            "is_bot": is_bot,
            "content": message.content or "",
            "attachments": attachments,
            "embeds": embeds,
            "has_attachments": has_attachments,
            "jump_url": jump_url,
        }

    async def _dispatch_forwarding(self, rule: ForwardingRule, attrs: dict) -> None:
        if self._closing:
            return

        queue_name = self._queue_for_rule(rule)
        if not queue_name:
            return

        msg_id = attrs.get("message_id") or "message"
        job = ForwardingJob(
            provider_queue=queue_name,
            rule=rule,
            message_id=str(msg_id),
            attrs=dict(attrs or {}),
        )

        q = self._queues.get(queue_name)
        if not q:
            self.log.warning(
                "[⏩] Unknown queue=%s for rule=%s", queue_name, rule.rule_id
            )
            return

        try:
            q.put_nowait(job)
        except asyncio.QueueFull:
            self.log.warning(
                "[⏩] Queue full; dropping job | queue=%s rule_id=%s message_id=%s qsize=%s",
                queue_name,
                rule.rule_id,
                job.message_id,
                q.qsize(),
            )

    def _queue_for_rule(self, rule: ForwardingRule) -> str:
        p = (rule.provider or "").lower().strip()

        if p == "telegram":
            return "telegram"
        if p == "pushover":
            return "pushover"
        if p == "discord":
            url = (rule.config.get("url") or "").strip()
            if url and DISCORD_WEBHOOK_RE.match(url):
                return "discord"
            self.log.warning(
                "[⏩] Non-Discord webhook is not supported; dropping job | rule_id=%s provider=%s url=%s",
                rule.rule_id,
                p,
                (url[:80] + "...") if len(url) > 80 else url,
            )
            return ""
        return ""

    async def _provider_worker(
        self,
        *,
        provider: str,
        worker_idx: int,
        queue: asyncio.Queue,
    ) -> None:
        timeout_total = int(os.getenv("FORWARDING_HTTP_TIMEOUT_TOTAL", "60"))
        request_timeout = aiohttp.ClientTimeout(total=float(timeout_total))

        try:
            async with aiohttp.ClientSession(timeout=request_timeout) as session:
                while True:
                    job = await queue.get()
                    try:
                        if job is None:
                            return
                        assert isinstance(job, ForwardingJob)

                        await self._execute_job(
                            session=session, provider=provider, job=job
                        )

                    except RetryableForwardingError as e:
                        self.log.warning(
                            "[⏩] Retryable forward failure | provider=%s idx=%s message_id=%s attempts=%s status=%s delay=%s body=%s",
                            provider,
                            worker_idx,
                            getattr(job, "message_id", None),
                            getattr(job, "attempts", None),
                            e.status,
                            e.delay,
                            (e.body or "")[:200],
                        )
                        if isinstance(job, ForwardingJob) and not self._closing:
                            await self._maybe_retry(
                                provider, job, delay_override=e.delay
                            )

                    except Exception:
                        self.log.exception(
                            "[⏩] Worker job error | provider=%s idx=%s message_id=%s attempts=%s",
                            provider,
                            worker_idx,
                            getattr(job, "message_id", None),
                            getattr(job, "attempts", None),
                        )
                        if isinstance(job, ForwardingJob) and not self._closing:
                            await self._maybe_retry(provider, job)

                    finally:
                        queue.task_done()

        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception(
                "[⏩] Worker crashed | provider=%s idx=%s", provider, worker_idx
            )

    async def _execute_job(
        self,
        *,
        session: aiohttp.ClientSession,
        provider: str,
        job: ForwardingJob,
    ) -> None:
        if provider == "telegram":
            await self._send_telegram(rule=job.rule, attrs=job.attrs, session=session)
            return
        if provider == "pushover":
            await self._send_pushover(
                rule=job.rule,
                message_id=job.message_id,
                attrs=job.attrs,
                session=session,
            )
            return
        if provider == "discord":
            await self._send_discord_webhook(
                rule=job.rule, attrs=job.attrs, session=session
            )
            return

        self.log.warning(
            "[⏩] Unknown provider worker; dropping job | provider=%s",
            provider,
        )

    async def _maybe_retry(
        self,
        provider: str,
        job: ForwardingJob,
        delay_override: float | None = None,
    ) -> None:
        max_attempts = max(0, int(self._max_attempts or 0))
        if max_attempts <= 0:
            return

        if job.attempts >= max_attempts:
            self.log.warning(
                "[⏩] Dropping job after max attempts | provider=%s message_id=%s attempts=%s",
                provider,
                job.message_id,
                job.attempts,
            )
            return

        job.attempts += 1

        if delay_override is not None:
            delay = max(0.0, float(delay_override))
        else:
            delay = 0.5 * (2 ** (job.attempts - 1))

        delay = min(float(self._retry_max_delay), delay)
        delay *= 0.8 + random.random() * 0.4

        self.log.warning(
            "[⏩] Requeueing job | provider=%s message_id=%s attempt=%s delay=%.2fs",
            provider,
            job.message_id,
            job.attempts,
            delay,
        )

        asyncio.create_task(self._requeue_later(provider, job, delay))

    async def _requeue_later(
        self, provider: str, job: ForwardingJob, delay: float
    ) -> None:
        try:
            await asyncio.sleep(max(0.0, float(delay)))
            if self._closing:
                return
            q = self._queues.get(provider)
            if not q:
                return
            try:
                q.put_nowait(job)
            except asyncio.QueueFull:
                self.log.warning(
                    "[⏩] Queue full; dropping retry | provider=%s message_id=%s",
                    provider,
                    job.message_id,
                )
        except Exception:
            self.log.exception(
                "[⏩] Retry scheduling failed | provider=%s",
                provider,
            )

    def _build_forwarding_lines(
        self,
        attrs: dict,
        rule: Optional[ForwardingRule] = None,
        *,
        as_html: bool = False,
        html_links: bool = False,
    ) -> list[str]:
        def esc(s: str) -> str:
            return html.escape(s, quote=True) if as_html else s

        lines: list[str] = []

        raw_content = (attrs.get("content") or "").strip()
        content = esc(raw_content)

        if content:
            lines.append(content)

        embeds = attrs.get("embeds") or []
        embed_dicts: list[dict] = [e for e in embeds if isinstance(e, dict)]

        if embed_dicts:
            if lines:
                lines.append("")

            visible_idx = 0
            for e in embed_dicts:
                desc_raw = (e.get("description") or "").strip()
                if not desc_raw:
                    continue

                visible_idx += 1
                lines.append(esc(f"Embed {visible_idx}:"))
                lines.append(esc(desc_raw))
                lines.append("")

            while lines and not lines[-1].strip():
                lines.pop()

        attachments = attrs.get("attachments") or []

        audio_exts = (".mp3", ".ogg", ".wav", ".m4a", ".flac", ".aac")
        video_exts = (".mp4", ".webm", ".mov", ".mpg", ".mpeg", ".avi", ".mkv")
        image_exts = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp")

        media_lines: list[str] = []
        file_lines: list[str] = []

        for att in attachments:
            if not isinstance(att, dict):
                continue

            url = (att.get("url") or "").strip()
            if not url:
                continue

            fname = (att.get("filename") or "") or url
            fname_lower = fname.lower()
            ct = (att.get("content_type") or "").lower()

            is_audio = ct.startswith("audio/") or fname_lower.endswith(audio_exts)
            is_video = ct.startswith("video/") or fname_lower.endswith(video_exts)
            is_image = ct.startswith("image/") or fname_lower.endswith(image_exts)

            if html_links and is_image:
                continue

            if html_links and as_html:
                href = html.escape(url, quote=True)
                link = f'<a href="{href}">{esc(fname)}</a>'
            elif html_links:
                link = f"{fname} ({url})"
            else:
                link = esc(fname)

            if is_audio or is_video:
                media_lines.append(link)
            else:
                file_lines.append(link)

        if media_lines or file_lines:
            if lines:
                lines.append("")

            lines.extend(media_lines)
            if media_lines and file_lines:
                lines.append("")
            lines.extend(file_lines)

        if not lines:
            lines.append("New message")

        return lines

    @staticmethod
    def _split_lines_to_chunks(lines: list[str], limit: int) -> list[str]:
        chunks: list[str] = []
        current = ""

        for line in lines:
            candidate = (current + "\n" + line) if current else line
            if len(candidate) > limit:
                if current:
                    chunks.append(current)
                    current = line
                else:
                    chunks.append(line[:limit])
                    current = ""
            else:
                current = candidate

        if current:
            chunks.append(current)

        return chunks

    @staticmethod
    def _split_caption_and_rest(lines: list[str], limit: int) -> tuple[str, list[str]]:
        caption_lines: list[str] = []
        remaining: list[str] = []
        used = 0
        finished_caption = False

        for line in lines:
            if finished_caption:
                remaining.append(line)
                continue

            extra = (1 if caption_lines else 0) + len(line)
            if used + extra <= limit:
                caption_lines.append(line)
                used += extra
            else:
                if not caption_lines:
                    truncated = line[:limit]
                    caption_lines.append(truncated)
                else:
                    remaining.append(line)
                finished_caption = True

        caption = "\n".join(caption_lines) if caption_lines else ""
        return caption, remaining

    @staticmethod
    def _is_image_att(att: dict) -> bool:
        ct = (att.get("content_type") or "").lower()
        fname = (att.get("filename") or "").lower()
        if ct.startswith("image/"):
            return True
        return fname.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"))

    @staticmethod
    def _extract_image_urls(attrs: dict) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()

        def add(url: str) -> None:
            url = (url or "").strip()
            if not url or url in seen:
                return
            seen.add(url)
            out.append(url)

        for att in attrs.get("attachments") or []:
            if not isinstance(att, dict):
                continue
            if ForwardingManager._is_image_att(att):
                add(att.get("url") or "")

        for e in attrs.get("embeds") or []:
            if not isinstance(e, dict):
                continue
            img = e.get("image") or {}
            thumb = e.get("thumbnail") or {}
            if isinstance(img, dict):
                add(img.get("url") or "")
            if isinstance(thumb, dict):
                add(thumb.get("url") or "")

        return out

    @staticmethod
    def _split_lines_to_pushover_chunks(lines: list[str], limit: int) -> list[str]:
        chunks: list[str] = []
        current = ""

        for line in lines:
            candidate = f"{current}\n{line}" if current else line

            if len(candidate) <= limit:
                current = candidate
                continue

            if current:
                chunks.append(current)
                current = line
            else:
                if limit > 3:
                    chunks.append(line[: limit - 3] + "...")
                else:
                    chunks.append(line[:limit])
                current = ""

            if current and len(current) > limit:
                if limit > 3:
                    chunks.append(current[: limit - 3] + "...")
                else:
                    chunks.append(current[:limit])
                current = ""

        if current:
            chunks.append(current)

        return chunks

    async def _post_text(
        self,
        session: aiohttp.ClientSession,
        url: str,
        *,
        json_payload: dict | None = None,
        data_payload: dict | None = None,
        timeout: float | None = None,
    ) -> tuple[int, str, Any]:
        """
        Returns (status, body_text, headers_obj). Raises RetryableForwardingError on network/timeout.
        """
        try:
            async with session.post(
                url, json=json_payload, data=data_payload, timeout=timeout
            ) as resp:
                body_txt = await resp.text()
                return resp.status, body_txt, resp.headers
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise RetryableForwardingError("Network/timeout error", status=None) from e

    async def _send_pushover(
        self,
        *,
        rule: ForwardingRule,
        message_id: str,
        attrs: dict,
        session: aiohttp.ClientSession,
    ) -> None:
        token = rule.config.get("app_token")
        user = rule.config.get("user_key")
        if not token or not user:
            self.log.debug("[⏩] Pushover rule %s missing token/user_key", rule.rule_id)
            return

        MAX_PUSHOVER_LEN = 1024

        attrs_no_atts = dict(attrs or {})
        attrs_no_atts["attachments"] = []

        lines = self._build_forwarding_lines(
            attrs_no_atts, rule, as_html=True, html_links=False
        )

        def _add_numbered_section(title: str, items: list[str]) -> None:
            if not items:
                return
            if lines:
                lines.append("")
            lines.append(f"{title}:")
            for i, item in enumerate(items, start=1):
                lines.append(f"{i}. {item}")

        attachments = attrs.get("attachments") or []

        audio_exts = (".mp3", ".ogg", ".wav", ".m4a", ".flac", ".aac")
        video_exts = (".mp4", ".webm", ".mov", ".mpg", ".mpeg", ".avi", ".mkv")

        media_items: list[str] = []
        file_items: list[str] = []

        for att in attachments:
            if not isinstance(att, dict):
                continue

            url = (att.get("url") or "").strip()
            if not url:
                continue

            if self._is_image_att(att):
                continue

            fname = (att.get("filename") or "").strip() or url
            fname_lower = fname.lower()
            ct = (att.get("content_type") or "").lower()

            is_audio = ct.startswith("audio/") or fname_lower.endswith(audio_exts)
            is_video = ct.startswith("video/") or fname_lower.endswith(video_exts)

            if is_audio or is_video:
                media_items.append(html.escape(url, quote=True))
            else:
                file_items.append(
                    f"{html.escape(fname, quote=True)} ({html.escape(url, quote=True)})"
                )

        _add_numbered_section("Media", media_items)
        _add_numbered_section("Files", file_items)

        image_urls = self._extract_image_urls(attrs)
        if image_urls:
            if lines:
                lines.append("")
            lines.append("Images:")
            for i, u in enumerate(image_urls, start=1):
                u = (u or "").strip()
                if not u:
                    continue
                lines.append(f"{i}. {html.escape(u, quote=True)}")

        if not lines:
            lines = ["New message"]

        chunks = self._split_lines_to_pushover_chunks(lines, MAX_PUSHOVER_LEN)
        total = max(1, len(chunks))

        msg_id_str = str(message_id or attrs.get("message_id") or "message")

        async def _send_part(payload: dict) -> tuple[int, str]:
            status, body_txt, headers = await self._post_text(
                session,
                "https://api.pushover.net/1/messages.json",
                data_payload=payload,
                timeout=10,
            )

            if status == 429:
                ra = (
                    _extract_retry_after_from_headers(headers)
                    or _extract_retry_after_from_body(body_txt)
                    or 0.0
                )

                if 0 < ra <= 10:
                    await asyncio.sleep(ra)
                    status, body_txt, headers = await self._post_text(
                        session,
                        "https://api.pushover.net/1/messages.json",
                        data_payload=payload,
                        timeout=10,
                    )
                if status == 429:
                    ra2 = _extract_retry_after_from_headers(
                        headers
                    ) or _extract_retry_after_from_body(body_txt)
                    raise RetryableForwardingError(
                        "Pushover 429 rate limited",
                        delay=ra2,
                        status=status,
                        body=body_txt,
                    )

            if status in (500, 502, 503, 504, 408):
                raise RetryableForwardingError(
                    "Pushover transient HTTP error",
                    status=status,
                    body=body_txt,
                )

            return status, body_txt

        for idx, chunk in enumerate(chunks, start=1):
            title = f"{msg_id_str} [Part {idx}/{total}]" if total > 1 else msg_id_str

            payload = {
                "token": token,
                "user": user,
                "title": title,
                "message": chunk,
                "html": 1,
            }

            status, body_txt = await _send_part(payload)

            if status != 200:

                self.log.warning(
                    "[⏩] Pushover forward failed | status=%s body=%s",
                    status,
                    (body_txt or "")[:300],
                )
                return

            self.log.info("[⏩] Pushover forward OK")

            try:
                self.db.record_forwarding_event(
                    provider="pushover",
                    rule_id=rule.rule_id,
                    guild_id=rule.guild_id,
                    source_message_id=attrs.get("message_id"),
                    part_index=idx,
                    part_total=total,
                )
            except Exception:
                self.log.debug("[⏩] failed to record pushover event", exc_info=True)

            if idx != total:
                await asyncio.sleep(0.15)

    async def _send_telegram(
        self,
        *,
        rule: ForwardingRule,
        attrs: dict,
        session: aiohttp.ClientSession,
    ) -> None:
        token = rule.config.get("bot_token")
        chat_id = rule.config.get("chat_id")
        if not token or not chat_id:
            self.log.debug(
                "[⏩] Telegram rule %s missing bot_token/chat_id", rule.rule_id
            )
            return

        lines = self._build_forwarding_lines(attrs, rule, as_html=True, html_links=True)

        MAX_TG_TEXT = 4096
        MAX_TG_CAPTION = 1024

        attachments = attrs.get("attachments") or []
        embeds = attrs.get("embeds") or []

        image_urls: list[str] = []
        seen: set[str] = set()

        def _maybe_add(url: Optional[str]) -> None:
            url = (url or "").strip()
            if not url or url in seen:
                return
            seen.add(url)
            image_urls.append(url)

        for att in attachments:
            if not isinstance(att, dict):
                continue
            url = att.get("url")
            ct = (att.get("content_type") or "").lower()
            fname = (att.get("filename") or "").lower()

            is_image = False
            if ct:
                if ct.startswith("image/"):
                    is_image = True
            else:
                if fname.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp")):
                    is_image = True

            if is_image:
                _maybe_add(url)

        for e in embeds:
            if not isinstance(e, dict):
                continue
            img = e.get("image") or {}
            thumb = e.get("thumbnail") or {}
            if isinstance(img, dict):
                _maybe_add(img.get("url"))
            if isinstance(thumb, dict):
                _maybe_add(thumb.get("url"))

        raw_content = (attrs.get("content") or "").strip()
        has_embed_desc = any(
            isinstance(e, dict) and (e.get("description") or "").strip()
            for e in (embeds or [])
        )
        has_non_image_att = any(
            isinstance(a, dict)
            and (a.get("url") or "").strip()
            and not self._is_image_att(a)
            for a in (attachments or [])
        )

        if image_urls and not (raw_content or has_embed_desc or has_non_image_att):
            lines = []

        async def _tg_call(
            method: str,
            payload: dict,
            *,
            log_prefix: str,
            part_idx: int,
            part_total: int,
        ) -> bool:
            url = f"https://api.telegram.org/bot{token}/{method}"

            status, body_txt, headers = await self._post_text(
                session, url, json_payload=payload
            )
            data = _safe_json_loads(body_txt)
            retry_after = _extract_retry_after_from_body(body_txt)

            is_rate_limited = (status == 429) or (
                status == 200
                and isinstance(data, dict)
                and (data.get("ok") is False)
                and isinstance(retry_after, (int, float))
                and retry_after > 0
            )

            if is_rate_limited:
                ra = float(retry_after or 0.0)
                if 0 < ra <= 10:
                    self.log.warning(
                        "[⏩] %s rate limited | part=%s/%s retry_after=%.2fs",
                        log_prefix,
                        part_idx,
                        part_total,
                        ra,
                    )
                    await asyncio.sleep(ra)
                    status, body_txt, headers = await self._post_text(
                        session, url, json_payload=payload
                    )
                    data = _safe_json_loads(body_txt)
                    retry_after = _extract_retry_after_from_body(body_txt)

                if status == 429 or (
                    status == 200
                    and isinstance(data, dict)
                    and (data.get("ok") is False)
                    and (_extract_retry_after_from_body(body_txt) or 0) > 0
                ):
                    raise RetryableForwardingError(
                        "Telegram rate limited",
                        delay=float(
                            _extract_retry_after_from_body(body_txt)
                            or retry_after
                            or 0.0
                        )
                        or None,
                        status=status,
                        body=body_txt,
                    )

            if status in (408, 500, 502, 503, 504):
                raise RetryableForwardingError(
                    "Telegram transient HTTP error",
                    status=status,
                    body=body_txt,
                )

            if status != 200 or (isinstance(data, dict) and data.get("ok") is False):
                self.log.warning(
                    "[⏩] %s failed | status=%s body=%s",
                    log_prefix,
                    status,
                    (body_txt or "")[:300],
                )
                return False

            self.log.info(
                "[⏩] %s OK",
                log_prefix,
            )
            return True

        if not image_urls:
            text_chunks = self._split_lines_to_chunks(lines, MAX_TG_TEXT)
            total = len(text_chunks)
            for idx, chunk in enumerate(text_chunks, start=1):
                payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
                ok = await _tg_call(
                    "sendMessage",
                    payload,
                    log_prefix="Telegram forward",
                    part_idx=idx,
                    part_total=total,
                )
                if ok:
                    try:
                        self.db.record_forwarding_event(
                            provider="telegram",
                            rule_id=rule.rule_id,
                            guild_id=rule.guild_id,
                            source_message_id=attrs.get("message_id"),
                            part_index=idx,
                            part_total=total,
                        )
                    except Exception:
                        self.log.debug(
                            "[⏩] failed to record telegram event", exc_info=True
                        )
            return

        caption_text, remaining_lines = self._split_caption_and_rest(
            lines, MAX_TG_CAPTION
        )
        if not caption_text and lines:
            caption_text = lines[0][:MAX_TG_CAPTION]
            remaining_lines = lines[1:]

        if len(image_urls) == 1:
            payload = {"chat_id": chat_id, "photo": image_urls[0]}
            if caption_text:
                payload["caption"] = caption_text
                payload["parse_mode"] = "HTML"

            ok = await _tg_call(
                "sendPhoto",
                payload,
                log_prefix="Telegram photo forward",
                part_idx=1,
                part_total=1,
            )
            if ok:
                try:
                    self.db.record_forwarding_event(
                        provider="telegram",
                        rule_id=rule.rule_id,
                        guild_id=rule.guild_id,
                        source_message_id=attrs.get("message_id"),
                        part_index=1,
                        part_total=1,
                    )
                except Exception:
                    self.log.debug(
                        "[⏩] failed to record telegram photo event", exc_info=True
                    )

        else:
            max_media = 10
            media_items = [
                {"type": "photo", "media": u} for u in image_urls[:max_media]
            ]
            if caption_text:
                media_items[0]["caption"] = caption_text
                media_items[0]["parse_mode"] = "HTML"

            payload_media = {"chat_id": chat_id, "media": media_items}

            ok = await _tg_call(
                "sendMediaGroup",
                payload_media,
                log_prefix="Telegram media group forward",
                part_idx=1,
                part_total=1,
            )
            if ok:
                try:
                    self.db.record_forwarding_event(
                        provider="telegram",
                        rule_id=rule.rule_id,
                        guild_id=rule.guild_id,
                        source_message_id=attrs.get("message_id"),
                        part_index=1,
                        part_total=1,
                    )
                except Exception:
                    self.log.debug(
                        "[⏩] failed to record telegram media event", exc_info=True
                    )

        if remaining_lines:
            text_chunks = self._split_lines_to_chunks(remaining_lines, MAX_TG_TEXT)
            total = len(text_chunks)
            for idx, chunk in enumerate(text_chunks, start=1):
                payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
                ok = await _tg_call(
                    "sendMessage",
                    payload,
                    log_prefix="Telegram overflow forward",
                    part_idx=idx,
                    part_total=total,
                )
                if ok:
                    try:
                        self.db.record_forwarding_event(
                            provider="telegram",
                            rule_id=rule.rule_id,
                            guild_id=rule.guild_id,
                            source_message_id=attrs.get("message_id"),
                            part_index=idx,
                            part_total=total,
                        )
                    except Exception:
                        self.log.debug(
                            "[⏩] failed to record telegram overflow event",
                            exc_info=True,
                        )

    async def _send_discord_webhook(
        self,
        *,
        rule: ForwardingRule,
        attrs: dict,
        session: aiohttp.ClientSession,
    ) -> None:
        url = (rule.config.get("url") or "").strip()
        if not url:
            self.log.debug("[⏩] Discord webhook rule %s missing url", rule.rule_id)
            return

        if not DISCORD_WEBHOOK_RE.match(url):
            self.log.warning(
                "[⏩] Non-Discord webhook is not supported; skipping forward | rule_id=%s url=%s",
                rule.rule_id,
                (url[:80] + "...") if len(url) > 80 else url,
            )
            return

        content = (attrs.get("content") or "").strip()

        non_image_links: list[str] = []
        for a in attrs.get("attachments") or []:
            if not isinstance(a, dict):
                continue
            if self._is_image_att(a):
                continue
            u = (a.get("url") or "").strip()
            fn = (a.get("filename") or "").strip()
            if u:
                non_image_links.append(f"{fn + ': ' if fn else ''}{u}")

        lines: list[str] = []
        if content:
            lines.append(content)

        if non_image_links:
            if lines:
                lines.append("")
            lines.append("Files:")
            lines.extend(non_image_links)

        text = _clip("\n".join(lines).strip(), 2000)

        raw_embeds = [e for e in (attrs.get("embeds") or []) if isinstance(e, dict)]
        forwarded_embeds: list[dict] = []
        for e in raw_embeds:
            se = _sanitize_discord_embed_for_webhook(e)
            if se:
                forwarded_embeds.append(se)

        forwarded_embeds = forwarded_embeds[:10]

        existing_img_urls = _extract_embed_image_urls(forwarded_embeds)

        att_image_urls: list[str] = []
        for a in attrs.get("attachments") or []:
            if not isinstance(a, dict):
                continue
            if not self._is_image_att(a):
                continue
            u = (a.get("url") or "").strip()
            if u and u not in existing_img_urls:
                att_image_urls.append(u)

        remaining = max(0, 10 - len(forwarded_embeds))
        if remaining > 0 and att_image_urls:
            forwarded_embeds.extend(
                {"image": {"url": u}} for u in att_image_urls[:remaining]
            )

        payload = {
            "allowed_mentions": {"parse": []},
        }

        if text:
            payload["content"] = text

        if forwarded_embeds:
            payload["embeds"] = forwarded_embeds

        if not payload.get("content") and not payload.get("embeds"):
            payload["content"] = "New message"

        uname = (
            (rule.config.get("username") or "")
            or (rule.config.get("bot_username") or "")
            or (rule.config.get("webhook_username") or "")
        ).strip()
        if uname:
            payload["username"] = _clip(uname, 80)

        avatar_url = (
            (rule.config.get("avatar_url") or "")
            or (rule.config.get("bot_avatar_url") or "")
            or (rule.config.get("bot_avatar") or "")
            or (rule.config.get("webhook_avatar_url") or "")
        ).strip()
        if avatar_url:
            if avatar_url.startswith("http://") or avatar_url.startswith("https://"):
                payload["avatar_url"] = avatar_url
            else:
                self.log.debug(
                    "[⏩] Discord webhook avatar_url ignored (not http/https) | rule_id=%s",
                    rule.rule_id,
                )

        status, body, retry_after = await _post_with_discord_429_retry(
            session, url, payload
        )

        if status == 429:
            raise RetryableForwardingError(
                "Discord 429 rate limited",
                delay=retry_after,
                status=status,
                body=body,
            )

        if status in (408, 500, 502, 503, 504):
            raise RetryableForwardingError(
                "Discord transient HTTP error",
                status=status,
                body=body,
            )

        if status >= 400:
            self.log.warning(
                "[⏩] Discord webhook forward failed | status=%s body=%s",
                status,
                (body or "")[:300],
            )
            return

        self.log.info("[⏩] Discord webhook forward OK")
        try:
            self.db.record_forwarding_event(
                provider="discord",
                rule_id=rule.rule_id,
                guild_id=rule.guild_id,
                source_message_id=attrs.get("message_id"),
                part_index=1,
                part_total=1,
            )
        except Exception:
            self.log.debug("[⏩] failed to record discord webhook event", exc_info=True)


async def _post_with_discord_429_retry(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict,
) -> tuple[int, str, float | None]:
    async def _once() -> tuple[int, str, float | None, Any]:
        async with session.post(url, json=payload) as resp:
            body = await resp.text()
            ra = _extract_retry_after_from_headers(
                resp.headers
            ) or _extract_retry_after_from_body(body)
            return resp.status, body, ra, resp.headers

    try:
        status, body, retry_after, headers = await _once()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:

        raise RetryableForwardingError("Discord network/timeout", status=None) from e

    if status == 429:
        ra = float(retry_after or 0.0)

        if 0 < ra <= 10:
            await asyncio.sleep(ra)
            status, body, retry_after, headers = await _once()
            if status != 429:
                return status, body, None

        retry_after2 = (
            _extract_retry_after_from_headers(headers)
            or _extract_retry_after_from_body(body)
            or retry_after
        )
        return status, body, retry_after2

    return status, body, None
