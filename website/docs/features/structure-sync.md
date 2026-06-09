---
sidebar_position: 2
title: Structure Sync
---

# Dynamic Structure Sync

Copycord continuously watches source servers for structural changes and mirrors them to your clone server in real time.

## What gets synced

### Channels

| Event | Action in clone | Configurable |
|-------|----------------|-------------|
| Channel created | New channel created in clone | Always on |
| Channel deleted | Clone channel deleted | Delete Removed Channels |
| Channel renamed | Clone channel renamed | Rename Channels |
| Channel repositioned | Clone channel repositioned | Reposition Channels |
| Topic changed | Clone topic updated | Sync Channel Topic |
| NSFW toggled | Clone NSFW updated | Sync NSFW Flag |
| Slowmode changed | Clone slowmode updated | Sync Slowmode |
| Permissions changed | Clone permissions updated | Mirror Channel Permissions |

### Threads

| Event | Action in clone |
|-------|----------------|
| Thread created | Matching thread created |
| Thread deleted | Clone thread removed (Delete Removed Threads) |
| Thread renamed | Clone thread renamed |
| Thread archived/unarchived | Clone thread updated |

### Roles

| Event | Action in clone | Configurable |
|-------|----------------|-------------|
| Role created | New role created in clone | Clone Roles |
| Role deleted | Clone role deleted | Delete Removed Roles |
| Role renamed | Clone role renamed | Update Role Properties |
| Color changed | Clone role color updated | Update Role Properties |
| Permissions changed | Clone permissions updated | Mirror Role Permissions |
| Hoist toggled | Clone hoist updated | Update Role Properties |
| Position changed | Clone position updated | Rearrange Roles |
| Icon changed | Clone icon updated | Clone Role Icons |

### Emojis and Stickers

| Event | Action in clone | Configurable |
|-------|----------------|-------------|
| Emoji added | New emoji cloned | Clone Emoji |
| Emoji removed | Clone emoji deleted | Clone Emoji |
| Sticker added | New sticker cloned | Clone Stickers |
| Sticker removed | Clone sticker deleted | Clone Stickers |

### Server Identity

| Event | Configurable |
|-------|-------------|
| Server icon changed | Clone Server Icon |
| Server banner changed | Clone Server Banner |
| Splash screen changed | Clone Invite Splash |
| Description changed | Sync Server Description |

## How it works

Copycord uses two methods to keep the clone in sync:

### 1. Real-time gateway events

The client self-bot receives Discord gateway events for every change in the source server. These are processed immediately:

- `GUILD_CHANNEL_CREATE/DELETE/UPDATE`
- `GUILD_ROLE_CREATE/DELETE/UPDATE`
- `GUILD_EMOJIS_UPDATE`
- `GUILD_STICKERS_UPDATE`
- `GUILD_UPDATE`
- `THREAD_CREATE/DELETE/UPDATE`

### 2. Startup sync

When the client starts, it builds and sends a full sitemap for each server. Servers are processed one at a time with a configurable delay between each (default 3 seconds) to avoid rate limits. After startup, all changes are handled by real-time events — no periodic re-syncing is needed.

## Sync architecture

Structure sync is designed to be as fast as possible while respecting Discord's API limits.

### Two-phase channel sync

Channel and category creation runs first without any artificial delays — Discord's built-in rate limit handling (via `Retry-After` headers) is used automatically. Webhook creation is handled separately:

- **`ON_DEMAND_WEBHOOKS: true` (default)** — Webhooks are not created during sync at all. Instead, they are created lazily when the first message arrives for a channel. This makes initial server cloning near-instant for the structure phase.
- **`ON_DEMAND_WEBHOOKS: false`** — Webhooks are batch-created in the background after structure sync completes, with a small delay between each to avoid hitting rate limits.

### Parallel background tasks

Roles, emojis, and stickers sync in parallel as background tasks while structure sync runs. The sync is only reported as complete once all background tasks have finished.

### Cancel and restart

If a new sitemap arrives while a sync is already running for the same clone guild, the running sync is canceled and a new one starts with the latest data. Background tasks (roles, emojis, stickers, webhooks) from the canceled sync continue running independently — they are not interrupted.

### Message buffering

Messages that arrive for channels not yet cloned are automatically queued. Once the sync creates the channel and its webhook (on-demand or batch), buffered messages are replayed in order. No messages are dropped during sync.

### Emoji and sticker limits

When cloning emojis or stickers, if the clone guild hits Discord's limit (e.g., 50 emojis for unboosted servers), the sync stops. The limit is re-evaluated on each sync cycle.

### Rate limiting

Copycord uses discord.py's native rate limit handling for all structure sync operations. When Discord returns a `429 Too Many Requests` response, the library automatically waits the required `Retry-After` duration before retrying.
