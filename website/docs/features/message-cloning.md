---
sidebar_position: 1
title: Message Cloning
---

# Message Cloning

Copycord's core feature is real-time message cloning — every message sent in a source server appears in your clone server within seconds.

## How messages are cloned

```
Source Server        Client (self-bot)        Server (bot)           Clone Server
     │                    │                       │                      │
     │  New message       │                       │                      │
     ├───────────────────►│                       │                      │
     │                    │  WebSocket forward     │                      │
     │                    ├──────────────────────►│                      │
     │                    │                       │  Webhook post         │
     │                    │                       ├─────────────────────►│
     │                    │                       │                      │  Message appears
```

1. A user posts a message in the source server
2. The **client** (self-bot) detects the message via Discord gateway events
3. The client extracts the content, embeds, attachments, and metadata
4. The message is sent to the **server** bot via WebSocket
5. The server posts the message to the matching clone channel using a **webhook**

The webhook preserves the original author's username and avatar, making messages look natural in the clone.

## What gets cloned

| Content | Supported | Notes |
|---------|-----------|-------|
| Text messages | Yes | Full content preserved |
| Embeds | Yes | Sanitized and re-posted |
| Attachments | Yes | Downloaded and re-uploaded |
| Stickers | Yes | Matched to cloned stickers |
| Replies | Yes | Optional reference tag via `TAG_REPLY_MSG` |
| Threads | Yes | Auto-created, messages forwarded |
| Forum posts | Yes | Tags and guidelines synced |
| Forwarded messages | Yes | References resolved |
| System messages | Yes | Join notifications, pins, etc. |

## Message edits

When a message is edited in the source server, Copycord can:

- **Edit** the cloned message to match (when `EDIT_MESSAGES` is enabled)
- **Resend** the edited message as a new post (when `RESEND_EDITED_MESSAGES` is enabled)

## Message deletes

When a message is deleted in the source, Copycord deletes the corresponding cloned message (when `DELETE_MESSAGES` is enabled).

## Mention handling

Copycord sanitizes mentions in cloned messages:

- `<@user_id>` mentions are converted to readable `@username` format
- `@everyone` and `@here` can be stripped via `DISABLE_EVERYONE_MENTIONS`
- Role mentions can be stripped via `DISABLE_ROLE_MENTIONS`

## User anonymization

When `ANONYMIZE_USERS` is enabled, Copycord replaces the original author's identity with:
- A random username (e.g., "SwiftFox123")
- A random avatar image

This hides the real identity of users in the source server.

## Webhook system

Copycord uses Discord webhooks to post cloned messages. Webhooks allow messages to appear with custom usernames and avatars, making them look like they were posted by the original author.

- By default (`ON_DEMAND_WEBHOOKS: true`), webhooks are created on-demand when a channel receives its first message
- If `ON_DEMAND_WEBHOOKS` is disabled, webhooks are batch-created in the background after structure sync
- A webhook pool is maintained for performance
- Custom webhook identities can be set per-channel via the `/channel_webhook` commands
