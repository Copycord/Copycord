---
sidebar_position: 7
title: Message Forwarding
---

# Message Forwarding

Copycord can forward messages from source servers to external services in real time. This is separate from server cloning: forwarding sends messages to Discord webhooks, Telegram and Pushover.

## Supported services

| Service | Type | Description |
|---------|------|-------------|
| **Discord Webhook** | Webhook URL(s) | Forward to one or more Discord channels via webhook |
| **Telegram** | Bot API | Send messages to a Telegram chat or channel |
| **Pushover** | Push notification | Get push notifications on your phone |

## Creating a forwarding rule

1. Go to the **Forwarding** page in the dashboard
2. Click **New Forward**
3. Configure the rule:
   - **Name**: a label for the rule
   - **Provider**: **Discord**, **Telegram** or **Pushover**
   - The provider's settings: for Discord the webhook URLs and optional **Bot Username (optional)** and **Bot Avatar URL (optional)**; for Telegram the **Bot token** and **Chat ID**; for Pushover the **App token** and **User / group key**
   - **Guild scope**: the source server the rule applies to, or **All guilds**
   - **Match any of these words / phrases** and the other filters below
   - **Rule enabled**
4. Click **Save Rule**

## Filter options

Each rule supports filtering so you only receive relevant messages:

- **Guild scope**: only messages from the selected source server
- **Match any of these words / phrases**: forward messages containing any of them
- **Require all of these words (optional)**: forward only messages containing all of them
- **Case sensitive** and **Search text in embeds**

**More filters** adds:

- **Limit to channel IDs (optional)** and **Limit to user IDs (optional)**
- **Has attachments**, **Bot messages** and **System messages**

All filters are optional. Leave them empty to forward everything.

## Validation

For Telegram and Pushover, **Save Rule** first checks the credentials with the service, and the rule is not saved if the check fails. Discord webhook URLs are checked when the rule is saved.

## Managing rules

Each rule card shows how many messages the rule has forwarded, with buttons to **Edit**, **Duplicate**, **Enable** or **Disable**, and **Delete** (asks for confirmation). **Search rules** filters the list.

## Forwarding to multiple Discord webhooks

A single Discord rule can target several webhook URLs. In the rule's **Webhook URLs** field, paste each webhook URL and press Enter; each one becomes a chip. When the rule matches a message, it's forwarded to **every** URL in the list.

- The rule's **username** and **avatar** apply to all URLs (one shared identity).
- You need at least one valid Discord webhook URL. Invalid URLs are rejected on save, and duplicates are removed automatically.
- Deliveries run concurrently, so adding more webhooks doesn't noticeably slow the rule down.
- A webhook that fails is retried on its own, without re-sending to the URLs that already succeeded.

Rules created with a single webhook URL continue to work and are upgraded to the multi-URL format the next time you save them.

## Using Copycord as a forwarder only

You don't need to clone a full server to use message forwarding. If you only want to forward messages to external services without mirroring a server, you can run Copycord in a forwarder-only setup:

1. Set up Copycord as normal with your **Client Token** and **Server Token**
2. Create a guild mapping with any server as the source and your bot's server as the clone (this is a shell mapping; it just needs to exist)
3. In the mapping settings, disable **Enable Cloning** to turn off all channel/role/message cloning
4. Set up your forwarding rules on the **Forwarding** page

With cloning disabled, Copycord won't create channels, webhooks, or forward messages to the clone server. The client will still monitor the source server and your forwarding rules will fire normally, which gives you a lightweight message forwarder without the cloning overhead.

## Use cases

- **Notifications**: get Telegram or Pushover alerts when someone mentions a keyword
- **Archival**: forward all messages to a separate Discord server for redundancy
- **Monitoring**: watch for specific users or topics across multiple servers
- **Integration**: pipe Discord messages into your own tools via webhooks
