---
sidebar_position: 2
title: Cloning Options
---

# Cloning Options

These settings control what Copycord syncs between the source and clone servers. They can be configured per-mapping through the web dashboard.

## Master controls

| Option | Default | Description |
|--------|---------|-------------|
| `ENABLE_CLONING` | `true` | Master switch — disables all cloning when off |
| `CLONE_MESSAGES` | `true` | Clone messages in real time via webhooks. When disabled, webhook creation is also skipped during sync |

## Message sync

| Option | Default | Description |
|--------|---------|-------------|
| `EDIT_MESSAGES` | `true` | Edit cloned messages when the source message is edited |
| `RESEND_EDITED_MESSAGES` | `true` | Resend edited messages as new messages |
| `DELETE_MESSAGES` | `true` | Delete cloned messages when the source message is deleted |
| `TAG_REPLY_MSG` | `false` | Add a reference tag showing which message is being replied to |

## Channel sync

| Option | Default | Description |
|--------|---------|-------------|
| `DELETE_CHANNELS` | `true` | Delete cloned channels when removed from the source |
| `DELETE_THREADS` | `true` | Delete cloned threads when removed from the source |
| `REPOSITION_CHANNELS` | `true` | Sync channel order/position from source |
| `RENAME_CHANNELS` | `true` | Sync channel name changes |
| `SYNC_CHANNEL_NSFW` | `false` | Sync the NSFW (age-restricted) flag |
| `SYNC_CHANNEL_TOPIC` | `false` | Sync channel topic/description |
| `SYNC_CHANNEL_SLOWMODE` | `false` | Sync slowmode (message cooldown) settings |
| `MIRROR_CHANNEL_PERMISSIONS` | `false` | Mirror channel-level permission overwrites |

## Voice and stage channels

| Option | Default | Description |
|--------|---------|-------------|
| `CLONE_VOICE` | `true` | Clone voice channels |
| `CLONE_VOICE_PROPERTIES` | `false` | Sync voice channel bitrate and user limit |
| `CLONE_STAGE` | `true` | Clone stage channels |
| `CLONE_STAGE_PROPERTIES` | `false` | Sync stage channel properties |

## Role sync

| Option | Default | Description |
|--------|---------|-------------|
| `CLONE_ROLES` | `true` | Clone roles from the source server |
| `UPDATE_ROLES` | `true` | Allow updating role properties after initial creation |
| `DELETE_ROLES` | `true` | Delete cloned roles when removed from the source |
| `MIRROR_ROLE_PERMISSIONS` | `false` | Mirror role permissions from source |
| `REARRANGE_ROLES` | `false` | Sync role ordering/position |
| `CLONE_ROLE_ICONS` | `false` | Clone role icons (requires Server Boost level 2+) |

## Emoji and sticker sync

| Option | Default | Description |
|--------|---------|-------------|
| `CLONE_EMOJI` | `true` | Clone custom emojis |
| `CLONE_STICKER` | `true` | Clone custom stickers |

## Guild-level sync

| Option | Default | Description |
|--------|---------|-------------|
| `CLONE_GUILD_ICON` | `false` | Sync the server icon |
| `CLONE_GUILD_BANNER` | `false` | Sync the server banner |
| `CLONE_GUILD_SPLASH` | `false` | Sync the server invite splash screen |
| `CLONE_GUILD_DISCOVERY_SPLASH` | `false` | Sync the discovery splash screen |
| `SYNC_GUILD_DESCRIPTION` | `false` | Sync the server description |

## Forum channels

| Option | Default | Description |
|--------|---------|-------------|
| `SYNC_FORUM_PROPERTIES` | `false` | Sync forum layout, tags, and posting guidelines |

## Message customization

| Option | Default | Description |
|--------|---------|-------------|
| `ANONYMIZE_USERS` | `false` | Replace user names with random identities (e.g., "SwiftFox123") and random avatars |
| `DISABLE_EVERYONE_MENTIONS` | `false` | Strip @everyone and @here mentions from cloned messages |
| `DISABLE_ROLE_MENTIONS` | `false` | Strip role mentions from cloned messages |

## Database maintenance

| Option | Default | Description |
|--------|---------|-------------|
| `DB_CLEANUP_MSG` | `true` | Automatically clean up message records when messages are deleted |

---

:::tip Recommended starting configuration
Start with the defaults — they cover the most common use case (full channel and message cloning with roles and emojis). Enable additional sync options as needed. Options like `MIRROR_CHANNEL_PERMISSIONS` and `MIRROR_ROLE_PERMISSIONS` add more API calls and may slow syncing on large servers.
:::
