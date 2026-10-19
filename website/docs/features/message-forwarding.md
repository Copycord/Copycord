---
sidebar_position: 3
title: External Forwarding
---

# External Message Forwarding

Beyond server cloning, Copycord can forward messages from source servers to external services in real time. This lets you receive notifications, build integrations, or create redundant archives.

## Supported services

### Discord Webhooks

Forward messages to any Discord channel via a webhook URL. Messages arrive with their original content, embeds, and author information.

A single rule can forward to **multiple Discord webhook URLs** at once: add as many as you like and the matched message is delivered to every one of them. The rule's username and avatar (if set) are shared across all URLs. Existing single-URL rules keep working unchanged.

### Telegram

Send messages to a Telegram chat or channel via the Telegram Bot API. Useful for mobile notifications.

### Pushover

Receive push notifications on your phone when matching messages appear. Great for high-priority alerts.

## Creating rules

Forwarding rules are created through the [web dashboard](/docs/dashboard/forwarding). Each rule specifies:

- **Provider**: where to send (Discord webhooks, Telegram, Pushover)
- **Filters**: which messages to forward

## Filter options

| Filter | Description |
|--------|-------------|
| **Guild scope** | Only forward from a specific source server |
| **Words** | Forward messages containing any of the listed words or phrases, or require all of a second list |
| **Channel IDs** | Only forward from specific channels |
| **User IDs** | Only forward messages from specific users |
| **Message kinds** | Messages with attachments, bot messages, system messages |

See [Message Forwarding](/docs/dashboard/forwarding#filter-options) for every option.

All filters are optional and can be combined. Empty filters match everything.

## How it works

1. The **client** self-bot receives every message from source servers
2. Each message is checked against all active forwarding rules
3. If a message matches a rule's filters, it's queued for delivery
4. Worker threads process the queue and send to external services
5. Failed deliveries are retried with exponential backoff

## Deduplication

Copycord includes built-in deduplication to prevent sending the same message twice.

## Performance

Forwarding uses a worker pool model with concurrent workers for each service type. Messages are processed in order with automatic retry on failure.

When a Discord rule targets multiple webhook URLs, those deliveries run **concurrently** (bounded so they stay within Discord's rate limits), so adding more webhooks barely changes how long the rule takes. If one URL fails, only that URL is retried; the ones that already succeeded are not re-sent.
