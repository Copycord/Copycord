---
sidebar_position: 6
title: Message Forwarding
---

# Message Forwarding

Copycord can forward messages from source servers to external services in real time. This is separate from server cloning — forwarding sends messages to webhooks, Telegram, Pushover, and more.

## Supported services

| Service | Type | Description |
|---------|------|-------------|
| **Discord Webhook** | Webhook URL | Forward to any Discord channel via webhook |
| **Telegram** | Bot API | Send messages to a Telegram chat or channel |
| **Pushover** | Push notification | Get push notifications on your phone |

## Creating a forwarding rule

1. Go to the **Forwarding** page in the dashboard
2. Click **Create Rule**
3. Configure the rule:
   - **Service** — Choose the destination (Discord webhook, Telegram, Pushover)
   - **Webhook/API URL** — The destination endpoint
   - **Guild filter** — Which source server to watch (or all)
   - **Channel filter** — Specific channels (or all)
   - **User filter** — Specific users (or all)
   - **Keyword filter** — Only forward messages containing specific words
4. Click **Save**

## Filter options

Each rule supports flexible filtering so you only receive relevant messages:

- **Guild** — Scope the rule to a specific source server
- **Channel** — Only forward from specific channels
- **User** — Only forward messages from specific users
- **Keyword** — Only forward messages containing certain keywords

All filters are optional. Leave them empty to forward everything.

## Validating webhooks

Before saving a rule, you can click **Validate** to test the webhook URL. This sends a test message to confirm the endpoint is reachable and working.

## Using Copycord as a forwarder only

You don't need to clone a full server to use message forwarding. If you only want to forward messages to external services without mirroring a server, you can run Copycord in a forwarder-only setup:

1. Set up Copycord as normal with your **Client Token** and **Server Token**
2. Create a guild mapping with any server as the source and your bot's server as the clone (this is a shell mapping — it just needs to exist)
3. In the mapping settings, disable **Enable Cloning** to turn off all channel/role/message cloning
4. Set up your forwarding rules on the **Forwarding** page

With cloning disabled, Copycord won't create channels, webhooks, or forward messages to the clone server. The client will still monitor the source server and your forwarding rules will fire normally — giving you a lightweight message forwarder without any of the cloning overhead.

## Use cases

- **Notifications** — Get Telegram/Pushover alerts when someone mentions a keyword
- **Archival** — Forward all messages to a separate Discord server for redundancy
- **Monitoring** — Watch for specific users or topics across multiple servers
- **Integration** — Pipe Discord messages into your own tools via webhooks
