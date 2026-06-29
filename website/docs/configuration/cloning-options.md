---
sidebar_position: 2
title: Cloning Options
---

# Cloning Options

These settings control what Copycord syncs between the source and clone servers. They can be configured per-mapping through the web dashboard.

## General

| Option | Default | Description |
|--------|---------|-------------|
| Enable Cloning | On | Master switch — disables all cloning when off |
| Clone Messages | On | Clone messages in real time via webhooks. When disabled, webhook creation is also skipped during sync |
| On-Demand Webhooks | On | Webhooks are only created when a channel receives its first message instead of during sync. Makes server cloning much faster |

## Messages

| Option | Default | Description |
|--------|---------|-------------|
| Edit Messages | On | Edit cloned messages when the source message is edited |
| Resend Edited Messages | On | Resend edited messages as new messages. Requires Edit Messages to be enabled |
| Delete Messages | On | Delete cloned messages when the source message is deleted |

## Channels

| Option | Default | Description |
|--------|---------|-------------|
| Delete Removed Channels | On | Delete cloned channels when removed from the source |
| Delete Removed Threads | On | Delete cloned threads when removed from the source |
| Reposition Channels | On | Sync channel order/position from source |
| Rename Channels | On | Sync channel name changes |
| Sync NSFW Flag | Off | Sync the NSFW (age-restricted) flag |
| Sync Channel Topic | Off | Sync channel topic/description |
| Sync Slowmode | Off | Sync slowmode (message cooldown) settings |
| Sync Forum Properties | Off | Sync forum layout, tags, and posting guidelines |
| Mirror Channel Permissions | Off | Mirror channel-level permission overwrites. Requires Clone Roles to be enabled |

## Assets

| Option | Default | Description |
|--------|---------|-------------|
| Clone Emoji | On | Clone custom emojis |
| Clone Stickers | On | Clone custom stickers |
| Clone Voice Channels | On | Clone voice channels |
| Sync Voice Properties | Off | Sync voice channel bitrate and user limit |
| Clone Stage Channels | On | Clone stage channels |
| Sync Stage Properties | Off | Sync stage channel properties |

## Roles

| Option | Default | Description |
|--------|---------|-------------|
| Clone Roles | On | Clone roles from the source server |
| Update Role Properties | On | Keep role properties (name, color, permissions) in sync after creation. When off, you can freely edit roles in the clone |
| Delete Removed Roles | On | Delete cloned roles when removed from the source |
| Mirror Role Permissions | Off | Mirror role permissions from source |
| Rearrange Roles | Off | Sync role ordering/position |
| Clone Role Icons | Off | Clone role icons (requires Boost Level 2+) |

## Server Identity

| Option | Default | Description |
|--------|---------|-------------|
| Clone Server Icon | Off | Clone the server icon |
| Clone Server Banner | Off | Clone the server banner |
| Clone Invite Splash | Off | Clone the invite splash image |
| Clone Discovery Splash | Off | Clone the discovery splash image |
| Sync Server Description | Off | Sync the server description |

## Message Customization

These options are configured via the **Optional Message Features** popup in the mapping settings.

| Option | Default | Description |
|--------|---------|-------------|
| Tag Replies | Off | Prepend a link to the original message when cloning a reply |
| Anonymize Users | Off | Replace usernames and avatars with random identities |
| Disable @everyone | Off | Strip @everyone and @here mentions from cloned messages |
| Disable Role Mentions | Off | Strip role mentions from cloned messages |
| Append Timestamp | Off | Append the original message timestamp below the cloned message |
| Append Author | Off | Append the original author's username below the cloned message |

## Database Maintenance

| Option | Default | Description |
|--------|---------|-------------|
| Auto-Clean Message DB | On | Automatically clean up message records older than 7 days |

---

:::tip Recommended starting configuration
Start with the defaults — they cover the most common use case (full channel and message cloning with roles and emojis). Enable additional sync options as needed. Options like Mirror Channel Permissions and Mirror Role Permissions add more API calls and may slow syncing on large servers.
:::
