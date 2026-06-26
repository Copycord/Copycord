# Multiple Discord Webhook URLs per Forwarding Rule — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let one forwarding rule fan a matched message out to multiple Discord webhook URLs instead of just one.

**Architecture:** Rule config stores `config.urls: list[str]` (legacy `config.url` string still read as a one-element list). Shared URL helpers live in `common/common_helpers.py` and are used by both the admin save path and the client send path. The client keeps one queue job per `(rule, message)` and loops the URLs inside `_send_discord_webhook`, tracking delivered URLs on the job so retries only re-send the ones that failed. No DB schema change — `config_json` is opaque JSON.

**Tech Stack:** Python 3 (asyncio, aiohttp), FastAPI admin API, SQLite, vanilla-JS frontend bundled with Vite, pytest + pytest-asyncio + httpx for tests.

## Global Constraints

- `MAX_DISCORD_WEBHOOK_URLS = 10` — cap URLs per rule (truncate beyond, never error on overflow at parse/send; admin save truncates silently after validation).
- Discord webhook URL pattern (Python, case-insensitive): `^https?://(canary\.|ptb\.)?discord(app)?\.com/api/webhooks/\d+/.+`
- JS equivalent: `/^https?:\/\/(canary\.|ptb\.)?discord(app)?\.com\/api\/webhooks\/\d+\/.+/i`
- Scope is Discord-only. Do not touch Telegram/Pushover send or validation paths.
- Username/avatar stay single per rule (shared across all URLs). Do not add per-URL identity.
- New `.py` source files get the AGPL header block (copy from `code/common/common_helpers.py:1-8`). Test files follow the existing convention in `tests/` (module docstring, no license header).
- After editing any file under `code/admin/frontend/src/`, rebuild the bundle (`npm run build`) before manual verification — the template serves the built `dist/assets/main.js`.

---

### Task 1: Shared URL helpers in `common/common_helpers.py`

Pure, dependency-free helpers used by both admin and client. Fully unit-testable without constructing any manager.

**Files:**
- Modify: `code/common/common_helpers.py` (append after line 113)
- Test: `tests/test_forwarding_urls.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `DISCORD_WEBHOOK_RE: re.Pattern` — compiled, case-insensitive.
  - `MAX_DISCORD_WEBHOOK_URLS: int = 10`
  - `is_discord_webhook_url(url: str) -> bool`
  - `coerce_url_list(value) -> list[str]` — accept a list/tuple/set or a string (split on commas/whitespace); trim, drop empties, de-dup preserving first-seen order. No validity filtering.
  - `discord_urls_from_config(config: dict) -> list[str]` — return `coerce_url_list(config["urls"])`; if that is empty, fall back to `coerce_url_list(config["url"])`. No validity filtering.

- [ ] **Step 1: Write the failing test**

Create `tests/test_forwarding_urls.py`:

```python
"""
Unit tests for shared Discord webhook URL helpers in common.common_helpers.
"""
import os
import sys

CODE_DIR = os.path.join(os.path.dirname(__file__), "..", "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(CODE_DIR))

from common.common_helpers import (  # noqa: E402
    MAX_DISCORD_WEBHOOK_URLS,
    coerce_url_list,
    discord_urls_from_config,
    is_discord_webhook_url,
)

VALID_A = "https://discord.com/api/webhooks/111/aaa"
VALID_B = "https://ptb.discord.com/api/webhooks/222/bbb-_token"
VALID_C = "https://discordapp.com/api/webhooks/333/ccc"


class TestIsDiscordWebhookUrl:
    def test_accepts_canonical(self):
        assert is_discord_webhook_url(VALID_A) is True

    def test_accepts_ptb_and_canary_and_app(self):
        assert is_discord_webhook_url(VALID_B) is True
        assert is_discord_webhook_url(VALID_C) is True
        assert is_discord_webhook_url(
            "https://canary.discord.com/api/webhooks/4/d"
        ) is True

    def test_rejects_non_webhook(self):
        assert is_discord_webhook_url("https://example.com/hook") is False
        assert is_discord_webhook_url("not a url") is False
        assert is_discord_webhook_url("") is False


class TestCoerceUrlList:
    def test_csv_string(self):
        assert coerce_url_list(f"{VALID_A}, {VALID_B}") == [VALID_A, VALID_B]

    def test_whitespace_and_newlines(self):
        assert coerce_url_list(f"{VALID_A}\n{VALID_B}  {VALID_C}") == [
            VALID_A, VALID_B, VALID_C
        ]

    def test_list_input_trimmed(self):
        assert coerce_url_list([f"  {VALID_A}  ", "", VALID_B]) == [VALID_A, VALID_B]

    def test_dedup_preserves_order(self):
        assert coerce_url_list([VALID_B, VALID_A, VALID_B]) == [VALID_B, VALID_A]

    def test_non_iterable_returns_empty(self):
        assert coerce_url_list(None) == []
        assert coerce_url_list(123) == []


class TestDiscordUrlsFromConfig:
    def test_reads_urls_list(self):
        assert discord_urls_from_config({"urls": [VALID_A, VALID_B]}) == [VALID_A, VALID_B]

    def test_legacy_url_fallback(self):
        assert discord_urls_from_config({"url": VALID_A}) == [VALID_A]

    def test_urls_takes_precedence_over_legacy(self):
        assert discord_urls_from_config({"urls": [VALID_B], "url": VALID_A}) == [VALID_B]

    def test_empty_urls_falls_back_to_legacy(self):
        assert discord_urls_from_config({"urls": [], "url": VALID_A}) == [VALID_A]

    def test_missing_returns_empty(self):
        assert discord_urls_from_config({}) == []

    def test_max_constant(self):
        assert MAX_DISCORD_WEBHOOK_URLS == 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_forwarding_urls.py -v`
Expected: FAIL — `ImportError: cannot import name 'coerce_url_list' from 'common.common_helpers'`.

- [ ] **Step 3: Write minimal implementation**

Append to `code/common/common_helpers.py` (after line 113; add `import re` next to the existing `import json` at the top of the file):

```python
import re

DISCORD_WEBHOOK_RE = re.compile(
    r"^https?://(canary\.|ptb\.)?discord(app)?\.com/api/webhooks/\d+/.+", re.I
)

MAX_DISCORD_WEBHOOK_URLS = 10


def is_discord_webhook_url(url: str) -> bool:
    """True if `url` is a Discord webhook URL."""
    return bool(DISCORD_WEBHOOK_RE.match((url or "").strip()))


def coerce_url_list(value) -> list[str]:
    """Normalize a URL value into a clean, de-duplicated list of strings.

    Accepts a comma/whitespace-separated string, or a list/tuple/set.
    Trims each entry, drops empties, and removes duplicates while
    preserving first-seen order. Does NOT validate URL format.
    """
    if isinstance(value, str):
        tokens = re.split(r"[,\s]+", value.strip())
    elif isinstance(value, (list, tuple, set)):
        tokens = list(value)
    else:
        tokens = []

    out: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        s = str(tok).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def discord_urls_from_config(config: dict) -> list[str]:
    """Extract the webhook URL list from a Discord forwarding rule config.

    Prefers `config["urls"]`; falls back to legacy single `config["url"]`.
    Returns a cleaned list (no validity filtering).
    """
    config = config or {}
    urls = coerce_url_list(config.get("urls"))
    if not urls:
        urls = coerce_url_list(config.get("url"))
    return urls
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_forwarding_urls.py -v`
Expected: PASS (all tests green).

- [ ] **Step 5: Commit**

```bash
git add code/common/common_helpers.py tests/test_forwarding_urls.py
git commit -m "feat(forwarding): add shared Discord webhook URL helpers"
```

---

### Task 2: Multi-URL rule parsing and routing in `forwarding.py`

Teach the client to accept `config.urls` (and legacy `config.url`) when parsing/validating a Discord rule and when routing it to the discord queue.

**Files:**
- Modify: `code/client/forwarding.py` — imports (`11-26`), module regex (`30-32`), `_parse_rule` discord block (`761-770`), `_queue_for_rule` discord block (`962-972`)
- Test: `tests/test_forwarding_rules.py` (create)

**Interfaces:**
- Consumes (from Task 1): `discord_urls_from_config`, `is_discord_webhook_url`, `MAX_DISCORD_WEBHOOK_URLS`, `DISCORD_WEBHOOK_RE`.
- Produces:
  - `ForwardingManager._parse_rule(item)` now normalizes a valid Discord rule's `config["urls"]` to the cleaned, validity-filtered, capped list and removes the legacy `config["url"]`. Returns `None` (skips rule) when no valid URL exists.
  - `ForwardingManager._queue_for_rule(rule)` returns `"discord"` when the rule has ≥1 valid URL, else `""`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_forwarding_rules.py`:

```python
"""
Unit tests for ForwardingManager Discord multi-URL parsing and routing.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

CODE_DIR = os.path.join(os.path.dirname(__file__), "..", "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(CODE_DIR))

from client.forwarding import ForwardingManager  # noqa: E402

VALID_A = "https://discord.com/api/webhooks/111/aaa"
VALID_B = "https://discord.com/api/webhooks/222/bbb"


def _manager(db):
    return ForwardingManager(
        config=SimpleNamespace(),
        db=db,
        ws=SimpleNamespace(),
        loop=asyncio.new_event_loop(),
    )


def _item(config):
    return {
        "rule_id": "r1",
        "guild_id": 1,
        "label": "L",
        "provider": "discord",
        "enabled": True,
        "config": config,
        "filters": {},
    }


class TestParseRuleMultiUrl:
    def test_urls_list_parsed(self, db):
        rule = _manager(db)._parse_rule(_item({"urls": [VALID_A, VALID_B]}))
        assert rule is not None
        assert rule.config["urls"] == [VALID_A, VALID_B]

    def test_legacy_url_normalized_to_urls(self, db):
        rule = _manager(db)._parse_rule(_item({"url": VALID_A}))
        assert rule is not None
        assert rule.config["urls"] == [VALID_A]
        assert "url" not in rule.config

    def test_invalid_urls_dropped(self, db):
        rule = _manager(db)._parse_rule(
            _item({"urls": [VALID_A, "https://example.com/x"]})
        )
        assert rule.config["urls"] == [VALID_A]

    def test_no_valid_urls_skips_rule(self, db):
        assert _manager(db)._parse_rule(_item({"urls": ["https://example.com/x"]})) is None
        assert _manager(db)._parse_rule(_item({"urls": []})) is None

    def test_caps_at_max(self, db):
        many = [f"https://discord.com/api/webhooks/{i}/tok" for i in range(15)]
        rule = _manager(db)._parse_rule(_item({"urls": many}))
        assert len(rule.config["urls"]) == 10


class TestQueueForRule:
    def test_routes_discord_when_valid(self, db):
        rule = _manager(db)._parse_rule(_item({"urls": [VALID_A]}))
        assert _manager(db)._queue_for_rule(rule) == "discord"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_forwarding_rules.py -v`
Expected: FAIL — `test_legacy_url_normalized_to_urls` fails because `_parse_rule` currently leaves `config["url"]` and never sets `config["urls"]`.

- [ ] **Step 3: Write minimal implementation**

In `code/client/forwarding.py`:

a) Replace the local regex definition (lines 30-32) and add the helper import. Change the import area so the top reads (keep existing `from client.message_utils import ...`):

```python
from client.message_utils import _resolve_forward, _resolve_forward_via_snapshot
from common.common_helpers import (
    DISCORD_WEBHOOK_RE,
    MAX_DISCORD_WEBHOOK_URLS,
    discord_urls_from_config,
    is_discord_webhook_url,
)

log = logging.getLogger(__name__)
```

Delete the now-duplicated local `DISCORD_WEBHOOK_RE = re.compile(...)` block (old lines 30-32). `re` is still imported and used elsewhere, leave `import re`.

b) Replace the discord block in `_parse_rule` (old lines 761-770):

```python
        if provider == "discord":
            urls = [
                u
                for u in discord_urls_from_config(config)
                if is_discord_webhook_url(u)
            ][:MAX_DISCORD_WEBHOOK_URLS]
            if not urls:
                self.log.warning(
                    "[⏩] No valid Discord webhook URL; skipping rule | rule_id=%s provider=%s",
                    rule_id,
                    provider_raw,
                )
                return None
            config = dict(config)
            config["urls"] = urls
            config.pop("url", None)
```

c) Replace the discord block in `_queue_for_rule` (old lines 962-972):

```python
        if p == "discord":
            urls = [
                u
                for u in discord_urls_from_config(rule.config)
                if is_discord_webhook_url(u)
            ]
            if urls:
                return "discord"
            self.log.warning(
                "[⏩] Non-Discord webhook is not supported; dropping job | rule_id=%s provider=%s",
                rule.rule_id,
                p,
            )
            return ""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_forwarding_rules.py tests/test_forwarding_urls.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add code/client/forwarding.py tests/test_forwarding_rules.py
git commit -m "feat(forwarding): parse and route multiple Discord webhook URLs"
```

---

### Task 3: Fan-out send with per-URL retry in `forwarding.py`

Loop the URLs inside `_send_discord_webhook`, tracking delivered URLs on the job so retries only re-send failures.

**Files:**
- Modify: `code/client/forwarding.py` — `ForwardingJob` dataclass (`434-449`), `_execute_job` discord call (`1053-1058`), `_send_discord_webhook` (`1817-1990`)
- Test: `tests/test_forwarding_send.py` (create)

**Interfaces:**
- Consumes (from Task 1): `discord_urls_from_config`, `is_discord_webhook_url`. Reuses module-level `_post_with_discord_429_retry(session, url, payload) -> (status, body, retry_after)`.
- Produces:
  - `ForwardingJob.delivered_urls: set[str]` (default empty set).
  - `ForwardingManager._send_discord_webhook(*, rule, attrs, session, job, attempt=0)` — new required `job` parameter. POSTs the payload to every URL not already in `job.delivered_urls`; on `<400` adds the URL to `job.delivered_urls`; on `>=400` logs and drops it; on 429/transient/network leaves it pending. Raises `RetryableForwardingError` if any URL is left pending; otherwise records exactly one `record_forwarding_event`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_forwarding_send.py`:

```python
"""
Unit tests for ForwardingManager._send_discord_webhook fan-out behavior.
"""
import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

CODE_DIR = os.path.join(os.path.dirname(__file__), "..", "code")
if CODE_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(CODE_DIR))

import client.forwarding as fwd  # noqa: E402
from client.forwarding import (  # noqa: E402
    ForwardingManager,
    ForwardingRule,
    ForwardingJob,
    ForwardingFilters,
    RetryableForwardingError,
)

VALID_A = "https://discord.com/api/webhooks/111/aaa"
VALID_B = "https://discord.com/api/webhooks/222/bbb"


def _manager(db):
    return ForwardingManager(
        config=SimpleNamespace(),
        db=db,
        ws=SimpleNamespace(),
        loop=asyncio.get_event_loop(),
    )


def _rule(urls):
    return ForwardingRule(
        rule_id="r1",
        guild_id=1,
        label="L",
        provider="discord",
        enabled=True,
        config={"urls": list(urls)},
        filters=ForwardingFilters.from_dict({}),
    )


def _attrs():
    return {"content": "hello", "message_id": 9001, "channel_name": "general"}


def _job(rule):
    return ForwardingJob(
        provider_queue="discord", rule=rule, message_id="9001", attrs=_attrs()
    )


def _install_fake_post(monkeypatch, status_by_url):
    """Patch _post_with_discord_429_retry to a recorder driven by status_by_url."""
    calls = []

    async def fake(session, url, payload):
        calls.append(url)
        status = status_by_url[url]
        return status, "", None

    monkeypatch.setattr(fwd, "_post_with_discord_429_retry", fake)
    return calls


class TestFanOut:
    @pytest.mark.asyncio
    async def test_posts_to_every_url_and_records_one_event(self, db, monkeypatch):
        mgr = _manager(db)
        rule = _rule([VALID_A, VALID_B])
        job = _job(rule)
        calls = _install_fake_post(monkeypatch, {VALID_A: 204, VALID_B: 204})

        await mgr._send_discord_webhook(
            rule=rule, attrs=_attrs(), session=None, job=job, attempt=0
        )

        assert calls == [VALID_A, VALID_B]
        assert job.delivered_urls == {VALID_A, VALID_B}
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is True

    @pytest.mark.asyncio
    async def test_transient_failure_requeues_and_retry_skips_delivered(
        self, db, monkeypatch
    ):
        mgr = _manager(db)
        rule = _rule([VALID_A, VALID_B])
        job = _job(rule)

        # Attempt 1: A succeeds, B is a transient 500 -> job should requeue.
        _install_fake_post(monkeypatch, {VALID_A: 204, VALID_B: 500})
        with pytest.raises(RetryableForwardingError):
            await mgr._send_discord_webhook(
                rule=rule, attrs=_attrs(), session=None, job=job, attempt=0
            )
        assert job.delivered_urls == {VALID_A}
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is False

        # Attempt 2 (retry): only B is posted, then the event is recorded.
        calls = _install_fake_post(monkeypatch, {VALID_A: 204, VALID_B: 204})
        await mgr._send_discord_webhook(
            rule=rule, attrs=_attrs(), session=None, job=job, attempt=1
        )
        assert calls == [VALID_B]
        assert job.delivered_urls == {VALID_A, VALID_B}
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is True

    @pytest.mark.asyncio
    async def test_permanent_4xx_is_dropped_not_retried(self, db, monkeypatch):
        mgr = _manager(db)
        rule = _rule([VALID_A, VALID_B])
        job = _job(rule)
        _install_fake_post(monkeypatch, {VALID_A: 204, VALID_B: 404})

        # No exception: B is dropped, A delivered, event still recorded.
        await mgr._send_discord_webhook(
            rule=rule, attrs=_attrs(), session=None, job=job, attempt=0
        )
        assert job.delivered_urls == {VALID_A}
        assert db.has_forwarding_event(rule_id="r1", source_message_id=9001) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_forwarding_send.py -v`
Expected: FAIL — `_send_discord_webhook()` got an unexpected keyword argument `job` (and `ForwardingJob` has no `delivered_urls`).

- [ ] **Step 3: Write minimal implementation**

In `code/client/forwarding.py`:

a) Add `delivered_urls` to the `ForwardingJob` dataclass. Ensure `field` is imported (change line 19 to `from dataclasses import dataclass, field`), then add the attribute after `created_monotonic`:

```python
@dataclass
class ForwardingJob:
    provider_queue: str
    rule: ForwardingRule
    message_id: str
    attrs: dict
    attempts: int = 0
    created_monotonic: float = 0.0
    delivered_urls: set = field(default_factory=set)

    def __post_init__(self) -> None:
        if not self.created_monotonic:
            self.created_monotonic = time.monotonic()
```

b) Update the discord branch of `_execute_job` (old lines 1053-1058) to pass the job:

```python
        if provider == "discord":
            await self._send_discord_webhook(
                rule=job.rule,
                attrs=job.attrs,
                session=session,
                job=job,
                attempt=job.attempts,
            )
            return
```

c) Rewrite `_send_discord_webhook` (old lines 1817-1990). Keep the existing payload-building logic (content/files/embeds/username/avatar — old lines 1838-1924) **unchanged**; change only the signature, the URL source, and the send/dedup/record tail. Replace the method with:

```python
    async def _send_discord_webhook(
        self,
        *,
        rule: ForwardingRule,
        attrs: dict,
        session: aiohttp.ClientSession,
        job: "ForwardingJob",
        attempt: int = 0,
    ) -> None:
        urls = [
            u
            for u in discord_urls_from_config(rule.config)
            if is_discord_webhook_url(u)
        ]
        if not urls:
            self.log.debug("[⏩] Discord webhook rule %s has no valid url", rule.rule_id)
            return

        # ---- build payload once (UNCHANGED from previous single-URL logic) ----
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

        payload = {"allowed_mentions": {"parse": []}}
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

        # ---- DB dedup: skip if this rule already fully forwarded this message ----
        msg_id = attrs.get("message_id")
        if msg_id and self.db:
            try:
                if self.db.has_forwarding_event(
                    rule_id=rule.rule_id,
                    source_message_id=int(msg_id),
                ):
                    self.log.warning(
                        "[⏩] DB dedup blocked duplicate webhook | rule_id=%s label=%s message_id=%s channel=%s attempt=%s",
                        rule.rule_id,
                        rule.label,
                        msg_id,
                        attrs.get("channel_name"),
                        attempt,
                    )
                    return
            except Exception:
                self.log.debug("[⏩] DB dedup check failed, proceeding", exc_info=True)

        # ---- fan out to every URL, tracking per-URL outcome on the job ----
        pending = False
        pending_retry_after: float | None = None
        pending_status: int | None = None
        pending_body: str = ""

        for url in urls:
            if url in job.delivered_urls:
                continue

            status, body, retry_after = await _post_with_discord_429_retry(
                session, url, payload
            )

            if status == 429:
                pending = True
                pending_retry_after = retry_after
                pending_status = status
                pending_body = body
                continue

            if status in (408, 500, 502, 503, 504):
                pending = True
                pending_status = status
                pending_body = body
                continue

            if status >= 400:
                self.log.warning(
                    "[⏩] Discord webhook forward failed (dropping url) | rule_id=%s status=%s body=%s",
                    rule.rule_id,
                    status,
                    (body or "")[:300],
                )
                continue

            job.delivered_urls.add(url)
            self.log.info(
                "[⏩] Discord webhook forward OK | rule_id=%s label=%s message_id=%s channel=%s attempt=%s",
                rule.rule_id,
                rule.label,
                attrs.get("message_id"),
                attrs.get("channel_name"),
                attempt,
            )

        if pending:
            raise RetryableForwardingError(
                "Discord webhook(s) pending retry",
                delay=pending_retry_after,
                status=pending_status,
                body=pending_body,
            )

        # ---- all URLs delivered or dropped: record exactly one event ----
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_forwarding_send.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Run the full forwarding suite + commit**

Run: `python -m pytest tests/test_forwarding_urls.py tests/test_forwarding_rules.py tests/test_forwarding_send.py -v`
Expected: PASS.

```bash
git add code/client/forwarding.py tests/test_forwarding_send.py
git commit -m "feat(forwarding): fan out Discord forward to multiple webhook URLs"
```

---

### Task 4: Admin save normalization + validation in `app.py`

Normalize and validate the Discord `config.urls` on save; reject empty/invalid URL sets with HTTP 400.

**Files:**
- Modify: `code/admin/app.py` — imports near `67-69`, `api_save_forwarding` (`5591-5647`)
- Modify: `tests/test_api.py` — extend the `_clean_db` autouse fixture (`46-55`) and add a `TestForwardingAPI` class
- Test: covered in `tests/test_api.py`

**Interfaces:**
- Consumes (from Task 1): `discord_urls_from_config`, `is_discord_webhook_url`, `MAX_DISCORD_WEBHOOK_URLS`.
- Produces: `POST /api/forwarding` with `provider="discord"` persists `config.urls` (cleaned, capped at 10, legacy `url` dropped). Returns 400 (plain text) when zero URLs or any invalid URL is supplied.

- [ ] **Step 1: Write the failing test**

First, extend the `_clean_db` autouse fixture in `tests/test_api.py` so forwarding rows don't leak between tests. Replace the body (lines 46-55) with:

```python
@pytest.fixture(autouse=True)
def _clean_db():
    """Clean event_logs, guild_mappings, and forwarding rules between tests."""
    yield
    try:
        db.clear_event_logs()
        for m in db.list_guild_mappings():
            db.delete_guild_mapping(m["mapping_id"])
        for r in db.list_message_forwarding_rules():
            db.delete_message_forward_rule(r["rule_id"])
    except Exception:
        pass
```

Then append a new test class to `tests/test_api.py`:

```python
# ---------------------------------------------------------------------------
# Forwarding API — Discord multi-URL
# ---------------------------------------------------------------------------

class TestForwardingAPI:

    _VALID_A = "https://discord.com/api/webhooks/111/aaa"
    _VALID_B = "https://discord.com/api/webhooks/222/bbb"

    def _payload(self, **cfg_over):
        config = {"urls": [self._VALID_A, self._VALID_B]}
        config.update(cfg_over)
        return {
            "guild_id": "111",
            "label": "Multi",
            "provider": "discord",
            "enabled": True,
            "config": config,
            "filters": {},
        }

    async def _get_rule(self, client, rule_id):
        resp = await client.get("/api/forwarding")
        for item in resp.json()["items"]:
            if item["rule_id"] == rule_id:
                return item
        return None

    @pytest.mark.asyncio
    async def test_save_persists_urls(self, client):
        resp = await client.post("/api/forwarding", json=self._payload())
        assert resp.status_code == 200
        rule_id = resp.json()["rule_id"]

        item = await self._get_rule(client, rule_id)
        assert item is not None
        cfg = item["config"]
        if isinstance(cfg, str):
            import json
            cfg = json.loads(cfg)
        assert cfg["urls"] == [self._VALID_A, self._VALID_B]
        assert "url" not in cfg

    @pytest.mark.asyncio
    async def test_legacy_url_accepted_and_normalized(self, client):
        payload = self._payload()
        payload["config"] = {"url": self._VALID_A}
        resp = await client.post("/api/forwarding", json=payload)
        assert resp.status_code == 200
        rule_id = resp.json()["rule_id"]

        item = await self._get_rule(client, rule_id)
        cfg = item["config"]
        if isinstance(cfg, str):
            import json
            cfg = json.loads(cfg)
        assert cfg["urls"] == [self._VALID_A]

    @pytest.mark.asyncio
    async def test_zero_urls_rejected(self, client):
        payload = self._payload()
        payload["config"] = {"urls": []}
        resp = await client.post("/api/forwarding", json=payload)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_invalid_url_rejected(self, client):
        payload = self._payload()
        payload["config"] = {"urls": [self._VALID_A, "https://example.com/x"]}
        resp = await client.post("/api/forwarding", json=payload)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_caps_at_ten(self, client):
        many = [f"https://discord.com/api/webhooks/{i}/tok" for i in range(12)]
        payload = self._payload()
        payload["config"] = {"urls": many}
        resp = await client.post("/api/forwarding", json=payload)
        assert resp.status_code == 200
        rule_id = resp.json()["rule_id"]

        item = await self._get_rule(client, rule_id)
        cfg = item["config"]
        if isinstance(cfg, str):
            import json
            cfg = json.loads(cfg)
        assert len(cfg["urls"]) == 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_api.py::TestForwardingAPI -v`
Expected: FAIL — `test_invalid_url_rejected` and `test_zero_urls_rejected` get 200 (Discord saves are currently unvalidated); `test_save_persists_urls` may fail on `"url" not in cfg`.

- [ ] **Step 3: Write minimal implementation**

a) Add the import near the other `from common...` imports in `code/admin/app.py` (after line 69):

```python
from common.common_helpers import (
    discord_urls_from_config,
    is_discord_webhook_url,
    MAX_DISCORD_WEBHOOK_URLS,
)
```

b) In `api_save_forwarding`, insert a Discord normalization/validation block immediately after the existing telegram/pushover validation block (after old line 5618, before the `try:` that calls `db.upsert_message_forwarding_rule`):

```python
    if provider == "discord":
        raw_urls = discord_urls_from_config(config)
        if not raw_urls:
            return PlainTextResponse(
                "Discord: at least one webhook URL is required.", status_code=400
            )
        invalid = [u for u in raw_urls if not is_discord_webhook_url(u)]
        if invalid:
            return PlainTextResponse(
                "Discord: invalid webhook URL(s):\n" + "\n".join(invalid),
                status_code=400,
            )
        config = dict(config)
        config["urls"] = raw_urls[:MAX_DISCORD_WEBHOOK_URLS]
        config.pop("url", None)
```

(`PlainTextResponse` is already imported — it is used by the telegram/pushover branch.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_api.py -v`
Expected: PASS (existing tests + new `TestForwardingAPI`).

- [ ] **Step 5: Commit**

```bash
git add code/admin/app.py tests/test_api.py
git commit -m "feat(forwarding): validate and normalize Discord webhook URLs on save"
```

---

### Task 5: Frontend multi-URL chip input

Swap the single Discord URL input for a chip input, wire the JS to read/write `config.urls`, validate locally, and truncate long chips. No automated tests (repo has no JS test harness) — verify manually.

**Files:**
- Modify: `code/admin/templates/forwarding.html` (discord block `256-265`)
- Modify: `code/admin/frontend/src/js/forwarding.js` — `addChip` (`1256-1292`), `openEditModal` (`989-990`), `ensureDiscordOptionalFields` anchor (`1183`), `resetForm` id list (`1095-1106`), `validateProviderConfig` (`1316-1347`), `buildPayloadFromForm` discord branch (`1433-1444`)
- Modify: `code/admin/frontend/src/main.css` (append)

**Interfaces:**
- Consumes (from Task 4): `POST /api/forwarding` accepting `config.urls`.
- Produces: form reads/writes a `#discord_webhook_urls` hidden input populated by a `.chip-input-wrap[data-chip-input="discord_webhook_urls"]`.

- [ ] **Step 1: Replace the Discord field in the template**

In `code/admin/templates/forwarding.html`, replace the `#provider_discord` block (lines 256-265) with:

```html
                            <div id="provider_discord" class="provider-fields" hidden>
                                <div class="form-group">
                                    <label class="form-label" for="discord_webhook_urls">Discord webhook URLs</label>
                                    <input id="discord_webhook_urls" type="hidden" />
                                    <div class="chip-input-wrap" data-chip-input="discord_webhook_urls">
                                        <input type="text" class="chip-text-input"
                                            placeholder="Paste a webhook URL and press Enter..." />
                                    </div>
                                    <p class="help-text">
                                        Copycord forwards the matched message to every webhook URL listed here.
                                    </p>
                                </div>
                            </div>
```

- [ ] **Step 2: Update `forwarding.js`**

a) In `addChip` (after `chip.setAttribute("aria-label", ...)`, ~line 1269) add a hover title so truncated URLs stay readable:

```javascript
    chip.title = value;
```

b) In `openEditModal`, replace the single-URL population (lines 989-990):

```javascript
    const urlList =
      Array.isArray(cfg.urls) && cfg.urls.length
        ? cfg.urls
        : cfg.url
        ? [cfg.url]
        : [];
    const urlHidden = document.getElementById("discord_webhook_urls");
    if (urlHidden) urlHidden.value = urlList.join(", ");
    const urlWrap = document.querySelector(
      '[data-chip-input="discord_webhook_urls"]'
    );
    if (urlWrap) this.setChipsFromValue(urlWrap, urlList.join(", "));
```

c) In `ensureDiscordOptionalFields`, change the anchor lookup (line 1183) from `discord_webhook_url` to the new hidden input:

```javascript
    const urlInput = document.getElementById("discord_webhook_urls");
```

d) In `resetForm`, update the id list (lines 1095-1106): replace `"discord_webhook_url"` with `"discord_webhook_urls"`.

e) In `validateProviderConfig`, add a Discord branch **before** the `if (provider !== "telegram" && provider !== "pushover") return true;` line (line 1347):

```javascript
    if (provider === "discord") {
      const urls = this.splitCsv(
        document.getElementById("discord_webhook_urls")?.value
      );
      if (!urls.length) {
        this.showToast("Discord requires at least one webhook URL.", {
          type: "warning",
        });
        return false;
      }
      const re =
        /^https?:\/\/(canary\.|ptb\.)?discord(app)?\.com\/api\/webhooks\/\d+\/.+/i;
      const bad = urls.filter((u) => !re.test(u));
      if (bad.length) {
        this.showToast("Invalid Discord webhook URL(s):\n" + bad.join("\n"), {
          type: "error",
        });
        return false;
      }
      return true;
    }
```

f) In `buildPayloadFromForm`, replace the discord branch (lines 1433-1444) so it reads the URL list:

```javascript
    } else if (provider === "discord") {
      cfg.urls = this.splitCsv(
        document.getElementById("discord_webhook_urls")?.value
      );

      const uname =
        document.getElementById("discord_webhook_username")?.value.trim() || "";
      const avatar =
        document.getElementById("discord_webhook_avatar_url")?.value.trim() ||
        "";

      if (uname) cfg.username = uname;
      if (avatar) cfg.avatar_url = avatar;
    } else if (provider === "telegram") {
```

- [ ] **Step 3: Truncate long chips in CSS**

Append to `code/admin/frontend/src/main.css`:

```css
.chip-input-wrap[data-chip-input="discord_webhook_urls"] .chip {
  max-width: 320px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
```

- [ ] **Step 4: Build the frontend bundle**

Run (install deps once if `node_modules` is absent):

```bash
cd code/admin/frontend && npm install && npm run build
```

Expected: Vite writes `dist/assets/main.js` and `dist/assets/main.css` with no build errors.

- [ ] **Step 5: Manual verification**

Start the admin app (per the project's normal run command) and open the Forwarding page.

1. New rule → provider **Discord**. The field is now a chip input. Paste two valid webhook URLs, pressing Enter after each → two chips appear (long URLs truncate with ellipsis; hover shows the full URL).
2. Save. In DevTools → Network, the `POST /api/forwarding` request body has `config.urls` as a 2-element array (no `config.url`).
3. Edit the saved rule → both URL chips reappear.
4. Remove all chips and Save → toast "Discord requires at least one webhook URL." and the save is blocked.
5. Add a non-Discord URL (e.g. `https://example.com/x`) and Save → toast lists it as invalid and the save is blocked.

- [ ] **Step 6: Commit**

```bash
git add code/admin/templates/forwarding.html code/admin/frontend/src/js/forwarding.js code/admin/frontend/src/main.css
git add -A code/admin/frontend/dist
git commit -m "feat(forwarding): multi-URL chip input for Discord webhooks"
```

(If `code/admin/frontend/dist` is gitignored, the second `git add` is a no-op — that's fine.)

---

### Task 6: Full suite green + wrap-up

**Files:** none (verification only).

- [ ] **Step 1: Run the entire Python test suite**

Run: `python -m pytest tests/ -v`
Expected: PASS — all existing tests plus `test_forwarding_urls.py`, `test_forwarding_rules.py`, `test_forwarding_send.py`, and `tests/test_api.py::TestForwardingAPI`.

- [ ] **Step 2: Sanity-check backward compatibility**

Confirm a legacy single-URL rule still works end to end: a rule whose stored `config_json` is `{"url": "https://discord.com/api/webhooks/.../..."}` is parsed by `_parse_rule` into `config["urls"]` (Task 2 test `test_legacy_url_normalized_to_urls` covers this) and routed/sent by Task 3. No migration runs.

- [ ] **Step 3: Final commit (if any uncommitted changes remain)**

```bash
git status
# commit anything outstanding from manual fixes
```

---

## Self-Review

**Spec coverage:**
- Config shape `config.urls` + legacy `url` fallback → Tasks 1, 2, 4 (helpers, parse, save).
- No DB schema change → confirmed; nothing touches `db.py`.
- Cap 10 → Global Constraints + Tasks 2 (parse), 4 (save) with tests.
- Shared identity (single username/avatar) → Task 3 preserves the existing payload block unchanged; Task 5 leaves the username/avatar fields intact.
- Single-job internal-loop fan-out → Task 3.
- delivered/dropped/pending per-URL semantics + one DB event when no URL pending → Task 3 implementation + 3 tests.
- Counting unchanged (1 event per rule+message) → Task 3 records exactly one event.
- `_queue_for_rule` routes on ≥1 valid URL → Task 2.
- Admin 400 on zero/invalid URLs → Task 4.
- Frontend chip input + buildPayload/openEditModal/resetForm/validate/ensureDiscordOptionalFields + CSS → Task 5.
- Backward compat (no migration) → Tasks 2 + 6 step 2.
- Test plan (normalize, save round-trip, parse both shapes, fan-out, partial retry) → Tasks 1–4 tests.
- Out of scope (Telegram/Pushover multi-target, per-URL identity, per-URL counts) → not implemented; Global Constraints forbids touching those paths.

**Placeholder scan:** No TBD/TODO; every code step shows complete code.

**Type consistency:** Helper names (`is_discord_webhook_url`, `coerce_url_list`, `discord_urls_from_config`, `DISCORD_WEBHOOK_RE`, `MAX_DISCORD_WEBHOOK_URLS`) are defined in Task 1 and used verbatim in Tasks 2–4. `_send_discord_webhook(*, rule, attrs, session, job, attempt=0)` defined in Task 3 matches the `_execute_job` call site updated in the same task. `ForwardingJob.delivered_urls` defined and consumed in Task 3.
