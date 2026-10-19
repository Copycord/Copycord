---
sidebar_position: 1
title: Message cloning
---

# Message cloning

Message cloning copies every new message in a source server to your clone server within seconds. This page explains how a message travels, what is copied, and how edits, deletes and mentions are handled.

## How a message is cloned

1. Someone posts a message in the source server.
2. The client, your user account, sees the message and reads its content, embeds and attachments.
3. The client passes the message to the server bot.
4. The server bot posts it in the matching clone channel through a webhook, with the original author's name and avatar.

A mapping can post messages from user accounts instead of a webhook; see [Token message cloning](/docs/dashboard/guild-mappings#token-message-cloning).

Messages sent before you set up Copycord are copied with the [Backfill channels](/docs/dashboard/backfill) task. Every cloned message is listed on the [Messages](/docs/dashboard/messages) page.

## What is cloned

| Content | How it is cloned |
|---------|------------------|
| Text | The full text |
| Embeds | Cleaned up and posted again |
| Attachments | Downloaded and uploaded again |
| Stickers | Matched to the cloned stickers |
| Replies | Posted as messages, with an optional link to the original (**Tag Replies**) |
| Threads | Created in the clone, with their messages |
| Forum posts | Created in the clone, with their tags and guidelines |
| Forwarded messages | Posted with the forwarded content |
| System messages | Copied, for example join notices and pins |

## Edits and deletes

Each mapping decides what happens when a source message changes. These settings are in the mapping's **Settings**, under **Messages**:

- **Edit Messages**: the clone is edited to match.
- **Resend Edited Messages**: the edited message is posted again as a new message.
- **Delete Messages**: the clone is deleted when the source message is deleted.

Copycord matches each source message to its clone through the message records it stores. A message whose record was [purged](/docs/dashboard/purge-and-sync#purge-messages) is no longer edited or deleted.

## Mentions and identity

- User mentions become a readable `@username`.
- **Disable @everyone** removes `@everyone` and `@here` pings, and **Disable Role Mentions** removes role pings.
- **Anonymize Users** replaces each author's name and avatar with a random name, such as "SwiftFox123", and a random image, to hide who posted in the source server.

These are optional message features of a mapping; see [Change how cloned messages look](/docs/dashboard/guild-mappings#change-how-cloned-messages-look).

## Webhooks

Webhooks let Copycord post each message with the author's name and avatar.

- With **On-Demand Webhooks** on (the default), Copycord creates a channel's webhook when the first message arrives for it.
- With it off, Copycord creates all webhooks after the structure sync.

To post a channel's messages under one fixed name and avatar instead, use [`/channel_webhook`](/docs/commands/webhooks).
