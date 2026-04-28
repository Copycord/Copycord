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
| Channel deleted | Clone channel deleted | `DELETE_CHANNELS` |
| Channel renamed | Clone channel renamed | `RENAME_CHANNELS` |
| Channel repositioned | Clone channel repositioned | `REPOSITION_CHANNELS` |
| Topic changed | Clone topic updated | `SYNC_CHANNEL_TOPIC` |
| NSFW toggled | Clone NSFW updated | `SYNC_CHANNEL_NSFW` |
| Slowmode changed | Clone slowmode updated | `SYNC_CHANNEL_SLOWMODE` |
| Permissions changed | Clone permissions updated | `MIRROR_CHANNEL_PERMISSIONS` |

### Threads

| Event | Action in clone |
|-------|----------------|
| Thread created | Matching thread created |
| Thread deleted | Clone thread removed (`DELETE_THREADS`) |
| Thread renamed | Clone thread renamed |
| Thread archived/unarchived | Clone thread updated |

### Roles

| Event | Action in clone | Configurable |
|-------|----------------|-------------|
| Role created | New role created in clone | `CLONE_ROLES` |
| Role deleted | Clone role deleted | `DELETE_ROLES` |
| Role renamed | Clone role renamed | `UPDATE_ROLES` |
| Color changed | Clone role color updated | `UPDATE_ROLES` |
| Permissions changed | Clone permissions updated | `MIRROR_ROLE_PERMISSIONS` |
| Hoist toggled | Clone hoist updated | `UPDATE_ROLES` |
| Position changed | Clone position updated | `REARRANGE_ROLES` |
| Icon changed | Clone icon updated | `CLONE_ROLE_ICONS` |

### Emojis and Stickers

| Event | Action in clone |
|-------|----------------|
| Emoji added | New emoji cloned | `CLONE_EMOJI` |
| Emoji removed | Clone emoji deleted | `CLONE_EMOJI` |
| Sticker added | New sticker cloned | `CLONE_STICKER` |
| Sticker removed | Clone sticker deleted | `CLONE_STICKER` |

### Guild-level properties

| Event | Configurable |
|-------|-------------|
| Server icon changed | `CLONE_GUILD_ICON` |
| Server banner changed | `CLONE_GUILD_BANNER` |
| Splash screen changed | `CLONE_GUILD_SPLASH` |
| Description changed | `SYNC_GUILD_DESCRIPTION` |

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

### 2. Periodic full sync

A comprehensive structure comparison runs periodically (default: every 60 minutes). This catches any changes that might have been missed during downtime or network interruptions.

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

### Rate limiting

Copycord relies on discord.py's native rate limit handling for all structure sync operations. When Discord returns a `429 Too Many Requests` response, the library automatically waits the required `Retry-After` duration before retrying.
