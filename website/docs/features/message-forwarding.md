---
sidebar_position: 3
title: External forwarding
---

# External message forwarding

Forwarding sends messages from your source servers to services outside your clone server as they arrive: Discord webhooks, Telegram and Pushover. This page explains what forwarding is for and how messages are delivered. To set up a rule, see [Message forwarding](/docs/dashboard/forwarding).

## What you can use it for

- Alerts: get a Telegram or Pushover notification when someone mentions a keyword.
- Archives: post every message to a separate Discord server as well.
- Monitoring: follow certain users or topics across several servers.
- Your own tools: send messages to your own service through a Discord webhook.

## Services

| Service | Where messages go |
|---------|-------------------|
| Discord webhook | One or more Discord channels, with the message's content, embeds and author |
| Telegram | A Telegram chat or channel, through a Telegram bot |
| Pushover | Push notifications on your phone |

## How a message is forwarded

1. The client, your user account, receives every message from the source servers.
2. Copycord checks each message against every enabled rule. A rule matches when the message passes all of its filters; a rule with no filters matches everything from its **Guild scope**. The filters are listed in [Filter options](/docs/dashboard/forwarding#filter-options).
3. Each match is queued and delivered in order.
4. A failed delivery is retried, with a longer wait after each attempt.

Copycord does not send the same message to a rule twice.

When a Discord rule has several webhook URLs, the message goes to all of them at the same time, within Discord's rate limits, so more webhooks barely slow the rule down. When one URL fails, only that URL is retried.

Forwarding works without cloning: a mapping with cloning turned off still feeds the rules. See [Forward without cloning](/docs/dashboard/forwarding#forward-without-cloning).
