---
sidebar_position: 2
title: Guild Mappings
---

# Guild Mappings

A **guild mapping** is the core concept in Copycord — it defines a link between a source server and a clone server. Everything Copycord does revolves around these mappings.

## Creating a mapping

1. Navigate to the **Guilds** page in the dashboard
2. Click **Create Mapping**
3. Select the **Source Server** — the server your user account has access to
4. Select the **Clone Server** — the server where your bot is installed
5. Click **Create**

Once created, Copycord will immediately begin syncing the structure (channels, categories, roles) from the source to the clone.

## Managing mappings

### Pause / Resume

You can temporarily pause a mapping without deleting it. Paused mappings stop all syncing — no messages, edits, deletes, or structure changes will be forwarded.

Click the **toggle** button next to a mapping to pause or resume it.

### Edit settings

Each mapping has its own set of [cloning options](/docs/configuration/cloning-options) that control what gets synced. Click on a mapping to view and edit its settings. Settings are organized into sections:

- **General** — master cloning toggle, message cloning, webhooks
- **Channels** — channel deletion, renaming, repositioning, permissions
- **Messages** — message editing, deletion, resending
- **Roles** — role cloning, deletion, permissions, icons
- **Assets** — emoji, stickers, voice/stage channels
- **Server Identity** — server icon, banner, splash, description

### Message features

Click **Optional Message Features** at the bottom of the mapping settings to customize how cloned messages appear:

- **Tag Replies** — prepend a link when a message is a reply
- **Anonymize Users** — replace usernames with random identities
- **Disable @everyone** — strip @everyone and @here pings
- **Disable Role Mentions** — strip role mention pings
- **Append Timestamp** — show the original message timestamp
- **Append Author** — show the original author's name

A live preview shows how the combined settings affect the cloned message.

### User message sending

By default Copycord posts cloned messages through channel **webhooks**. The **User Message Sending** section lets you instead post them from real **user accounts** (self-bot tokens), so messages appear to come from ordinary members rather than a webhook.

To use it:

1. Open a mapping and expand **User Message Sending**.
2. Add one or more **user tokens**. Each token is validated when you add it — the account must be a member of the **clone** server, or it is rejected.
3. Enable the **Send messages as users** toggle.

When enabled, each new channel message (and each message posted into an existing thread) is sent by a **randomly chosen enabled token** via the Discord API. If a token is invalid, missing permissions, rate-limited, or removed from the clone server, Copycord rotates to another token; if **every** token fails it falls back to the normal webhook send, so no message is lost. Tokens are shown masked and can be individually enabled, disabled, or removed.

:::warning
Using self-bot accounts to send messages is against Discord's Terms of Service and can get those accounts banned. Each account must be in the clone server with permission to post in the target channels.
:::

Some behavior differs from webhook sending:

- **Identity** — the message appears as whichever account sent it; per-message usernames/avatars are not possible.
- **Embeds** — user accounts cannot post rich embeds, so embeds are flattened into text and links.
- **Attachments** — files are re-downloaded from the source and re-uploaded by the sending account.
- **Scope** — only new channel messages and thread posts are sent this way. Forum-thread creation, message edits, deletes, and backfill still use webhooks. (A webhook cannot edit a user-sent message, so edits to those messages are not applied.)

### Delete a mapping

Deleting a mapping removes the link between the source and clone servers. It does **not** delete any channels, roles, or messages that were already created in the clone server.

## Multiple mappings

Copycord supports multiple guild mappings simultaneously. You can:

- Clone several source servers into one clone server
- Clone one source server into multiple clone servers
- Any combination of source → clone pairs

Each mapping operates independently with its own filters and settings.

## How syncing works

When a mapping is active, Copycord continuously:

1. **Watches** the source server for changes (via the client self-bot)
2. **Forwards** new messages to the clone via webhooks
3. **Syncs** structural changes (new channels, renames, deletions)
4. **Updates** roles, emojis, and stickers as they change
5. **Tracks** message edits and deletes

The sync interval for structure checks defaults to every **60 minutes**, but individual changes (new channels, renames) are detected and applied in real time via Discord gateway events.
