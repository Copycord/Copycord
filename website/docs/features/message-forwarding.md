---
sidebar_position: 3
title: External Forwarding
---

# External Message Forwarding

Beyond server cloning, Copycord can forward messages from source servers to external services in real time. This lets you receive notifications, build integrations, or create redundant archives.

## Supported services

### Discord Webhooks

Forward messages to any Discord channel via a webhook URL. Messages arrive with their original content, embeds, and author information.

### Telegram

Send messages to a Telegram chat or channel via the Telegram Bot API. Useful for mobile notifications.

### Pushover

Receive push notifications on your phone when matching messages appear. Great for high-priority alerts.

## Creating rules

Forwarding rules are created through the [web dashboard](/docs/dashboard/forwarding). Each rule specifies:

- **Destination** — Where to send (webhook URL, Telegram, Pushover)
- **Filters** — Which messages to forward

## Filter options

| Filter | Description |
|--------|-------------|
| **Guild** | Only forward from a specific source server |
| **Channel** | Only forward from specific channels |
| **User** | Only forward messages from specific users |
| **Keyword** | Only forward messages containing certain words |

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
