# Multiple Discord Webhook URLs per Forwarding Rule

**Date:** 2026-06-26
**Status:** Approved design, ready for implementation plan

## Goal

Let a user attach **multiple Discord webhook URLs** to a single forwarding rule.
When the rule matches a message, Copycord forwards it to **every** URL on the
rule. Today a Discord rule holds exactly one webhook URL (`config.url`).

## Scope

- **Discord provider only.** Telegram and Pushover remain single-target.
- **Shared identity:** the existing single username/avatar fields apply to all
  URLs on the rule. No per-URL identity override.
- **No DB schema change.** Rule config is stored as JSON in
  `message_forwarding.config_json`; we only change the shape of that JSON.

## Config shape

- Canonical: `config.urls: list[str]`.
- Backward compatibility: if `config.urls` is absent but legacy `config.url`
  (string) is present, it is read as `[config.url]`. Old rules keep working with
  no migration. New saves write `config.urls` only (legacy `url` is dropped on
  next save).
- Normalization on save: trim each URL, drop empties, de-duplicate, cap at
  `MAX_DISCORD_URLS = 10`.
- `config.username` / `config.avatar_url` unchanged (shared across all URLs).

## Fan-out approach: single job, internal loop

One queue job per `(rule, message)` exactly as today. Inside
`_send_discord_webhook`, build the payload once and loop the URLs, POSTing to
each. This avoids any change to the rule-keyed dedup and the events table.

Rejected alternative — one job per URL: cleaner per-URL isolation, but the
dedup key `(message_id, rule_id)` and the DB check
`has_forwarding_event(rule_id, source_message_id)` would collide across sibling
jobs, forcing a target discriminator (schema change). Not worth it for a fan-out
of ~2–10 webhooks.

### Partial-retry correctness

Each URL on an attempt ends in one of three states:

- **delivered** — POST returned `< 400`.
- **dropped** — POST returned `>= 400` (permanent client error for that URL);
  logged and abandoned, never retried.
- **pending** — transient failure (429 after inline wait, 5xx/408, network);
  eligible for retry.

Mechanism:

- Add `delivered_urls: set[str]` (default empty) to `ForwardingJob`.
- A retry re-enqueues the **same** `ForwardingJob` instance
  (`_maybe_retry` → `_requeue_later` reuse the object), so `delivered_urls`
  survives across attempts.
- In `_send_discord_webhook`: skip any URL already in `job.delivered_urls`;
  POST the rest; on `< 400` add the URL to `delivered_urls`; on `>= 400` drop
  it; otherwise leave it pending. If **any URL is left pending** at the end of
  the loop, raise `RetryableForwardingError` so the worker requeues the job —
  delivered URLs are skipped next attempt, so no double-sends.
- Record **one** DB forwarding event when **no URL is left pending** (i.e. every
  URL is delivered or dropped). This fires even when some URLs were dropped, so
  the job stops retrying; it does not fire while transient failures remain. This
  keeps the existing `has_forwarding_event` dedup and the retry path consistent.

### Counting

A fan-out counts as **1** "Total Forwarded" per `(rule, message)`, unchanged.
Per-URL counts would require a target column in `forwarding_events` — explicitly
out of scope.

## Component changes

### `code/client/forwarding.py`

- **`ForwardingJob`**: add `delivered_urls: set[str] = field(default_factory=set)`.
- **`_parse_rule`**: compute the normalized URL list as
  `config.get("urls") or ([config["url"]] if config.get("url") else [])`,
  keep only URLs matching `DISCORD_WEBHOOK_RE`, and write the cleaned list back
  to `config["urls"]`. If no valid URL remains, skip the rule (same behavior as
  today's single-URL invalid case).
- **`_queue_for_rule`**: route to the `discord` queue when ≥1 valid URL is
  present (check the normalized list instead of the single `url`).
- **`_send_discord_webhook`**: signature gains `job: ForwardingJob` so per-URL
  progress (`job.delivered_urls`) persists across retries. Build the payload
  once (content, embeds, username, avatar), then loop the normalized URLs:
  skip delivered, POST via the existing `_post_with_discord_429_retry`,
  classify status as today (429 / transient / network → keep URL pending;
  `>= 400` → log and drop that URL; `< 400` → mark delivered). After the loop,
  if any URL is still pending raise `RetryableForwardingError`; otherwise (all
  delivered or dropped) record one DB event and log OK.
  Caller `_execute_job` passes `job=job` instead of unpacking `rule`/`attrs`.
- The top-of-function DB dedup check (`has_forwarding_event`) stays, but the
  event is only recorded once all URLs are delivered. Duplicate upstream
  dispatches in the retry window are already blocked by the in-memory
  `_dedup_seen` / `_dedup_touch` on `(message_id, rule_id)`.

### `code/admin/app.py`

- **`api_save_forwarding`**: when `provider == "discord"`, normalize the config
  into `config.urls` — accept either a `urls` list or a CSV/legacy `url`,
  trim, drop empties, de-dup, cap at 10. Reject with HTTP 400 + a plain-text
  message if the discord rule ends up with **zero** URLs, or if **any** provided
  URL is not a valid Discord webhook (reuse a `DISCORD_WEBHOOK_RE` equivalent).
  This is new validation — discord configs are currently saved unchecked.
- No change to `upsert_message_forwarding_rule` (config is opaque JSON).

### `code/admin/templates/forwarding.html`

- Replace the single `#discord_webhook_url` input in `#provider_discord` with a
  chip input: a hidden `<input id="discord_webhook_urls">` plus a
  `<div class="chip-input-wrap" data-chip-input="discord_webhook_urls">` with a
  `.chip-text-input` (mirrors the keyword/channel chip inputs). Update the help
  text to say each entry is one webhook URL.

### `code/admin/frontend/src/js/forwarding.js`

- **`initChipInputs`** already auto-binds every `.chip-input-wrap`, so the new
  URL chip input works without new wiring.
- **`buildPayloadFromForm`**: set `cfg.urls = this.splitCsv(#discord_webhook_urls.value)`
  instead of `cfg.url`.
- **`openEditModal`**: populate the URL chips from `cfg.urls`, falling back to
  `[cfg.url]` when only the legacy field exists
  (`setChipsFromValue(urlWrap, (cfg.urls || (cfg.url ? [cfg.url] : [])).join(", "))`).
- **`resetForm`**: the global chip clear already removes URL chips; remove the
  obsolete `discord_webhook_url` id from the reset list and add the new field.
- **`validateProviderConfig`**: for `discord`, require ≥1 URL and that each
  matches a Discord webhook regex; show a toast and return false otherwise
  (today discord short-circuits to valid).

### `code/admin/frontend/src/main.css`

- Add styling so long URL chips truncate with ellipsis (`max-width` +
  `text-overflow: ellipsis`) and carry `title`=full URL for hover. The
  `addChip` helper sets `textContent` and `aria-label`; add `title` too (general
  or scoped to the URL wrap).

## Backward compatibility

- Existing rules with `config.url` (string) continue to forward: every read path
  (`_parse_rule`, `_queue_for_rule`, send, edit modal) falls back to `[url]`.
- No data migration. A legacy rule is rewritten to `config.urls` only the next
  time the user saves it.

## Edge cases / limits

- `MAX_DISCORD_URLS = 10` (truncate beyond, server-side).
- Duplicate URLs on one rule collapse to one send.
- Whitespace-only entries dropped.
- All URLs invalid → save returns 400 (admin) and the rule is skipped client-side.
- A single URL returning `>= 400` is logged and dropped; it does not block
  delivery to the other URLs and does not by itself requeue the job.

## Test plan (`tests/`)

- Normalize: legacy `{"url": x}` → `urls == [x]`; CSV/list inputs trimmed,
  de-duped, capped at 10.
- Save round-trip via `/api/forwarding`: posting `config.urls` persists and
  reloads unchanged; zero/invalid URLs → 400.
- `_parse_rule`: accepts both `urls` list and legacy `url`; strips invalid URLs;
  skips rule when none valid.
- Send fan-out: mock `aiohttp` session, assert one POST per URL with the shared
  payload (username/avatar applied to each).
- Partial retry: first attempt delivers to URL A, URL B fails → job requeued
  with `delivered_urls == {A}`; second attempt only POSTs URL B; one DB event
  recorded total.

## Out of scope

- Multi-target for Telegram / Pushover.
- Per-URL username/avatar override.
- Per-URL forwarding counts / events.
