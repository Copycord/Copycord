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

When enabled, each cloned message is sent by one of the enabled tokens via the Discord API. If a token is invalid, missing permissions, rate-limited, or removed from the clone server, Copycord rotates to another token; if **Fall back to webhook** is on and **every** token fails, it falls back to the normal webhook send so no message is lost. Tokens are shown masked and can be individually enabled, disabled, or removed. The **⚙ gear** menu can verify every token at once (and offer to delete the ones that fail) or clear all tokens.

:::warning
Using self-bot accounts to send messages is against Discord's Terms of Service and can get those accounts banned. Each account must be in the clone server with permission to post in the target channels.
:::

#### Options

- **Account selection** — how a token is chosen for each message:
  - **Rotate evenly** — spread messages across all enabled tokens (round-robin).
  - **Sticky per author** — pin each source author to one token, so that author's messages always come from the same account.
- **Mirror author nickname** *(Sticky per author only)* — rename the assigned account in the clone server to the host author's display name, so it looks like that member.
- **Mirror author roles** *(Sticky per author only)* — give the assigned account the cloned roles that match the host author's roles.
- **Identity hold** *(Sticky per author only)* — how many minutes an account keeps an author before rotating to a free one. On rotation the previous account's mirrored nickname and roles are reset. `0` never rotates.
- **Fall back to webhook** — if every token fails, send via the normal webhook instead of dropping the message.
- **Show typing indicator** — briefly show "typing…" before each message so it looks more human.
- **Attachments as links** — post the source attachment links instead of re-downloading and re-uploading the files.
- **Send delay** — a random pause (min–max seconds) between messages to the same channel, so they don't arrive in a burst.

Some behavior differs from webhook sending:

- **Identity** — the message appears as whichever account sent it; per-message usernames/avatars are not possible. Use **Sticky per author** with nickname/role mirroring to make an account resemble the original author.
- **Bots, webhooks & rich embeds** — messages authored by bots or webhooks, and any message containing a rich embed, are always sent by the normal webhook (a user account cannot reproduce them), so their author identity and embed are preserved.
- **Embeds** — user accounts cannot post rich embeds, so plain embeds are flattened into text and links.
- **Attachments** — files are re-downloaded from the source and re-uploaded by the sending account, unless **Attachments as links** is on.
- **Stickers** — cloned custom stickers are sent by the token; standard Discord stickers require the account to have Nitro; a custom sticker that isn't cloned into the clone server falls back to the bot posting its image.
- **Threads** — token accounts create both **text threads** and **forum-thread starter posts** (and post the messages inside them), so the thread creator and the "started a thread" system message reflect the sending account. If a token can't create the thread, the bot (text) or webhook (forum) creates it instead.
- **Backfill** — historical backfill also forwards through tokens when enabled, with no artificial delay between token sends.
- **Edits & deletes** — a webhook cannot edit or delete a user-sent message, so edits and deletes to token-sent messages are not applied.

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
