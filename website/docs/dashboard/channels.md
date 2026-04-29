---
sidebar_position: 4
title: Channels
---

# Channels

The **Channels** page lets you view the channel structure of your cloned servers and customize how channels appear.

## Viewing channels

For each guild mapping, you can see:

- All **categories** and their child channels
- **Text channels**, **voice channels**, **stage channels**, and **forum channels**
- The mapping between source channel IDs and clone channel IDs
- Whether a webhook is active for each channel

## Customizing channel names

You can rename cloned channels to anything you want without affecting the source server:

1. Navigate to the **Channels** page
2. Find the channel you want to rename
3. Enter the new name
4. Click **Save**

The custom name persists even if the source channel is renamed — Copycord will keep your custom name instead of syncing the rename.

:::tip Category names
You can also customize category names the same way. This lets you completely rebrand the clone server's layout.
:::

## Channel types

Copycord supports cloning all Discord channel types:

| Type | Cloned? | Messages? | Notes |
|------|---------|-----------|-------|
| **Text channels** | Yes | Yes | Full message, edit, and delete sync |
| **Voice channels** | Yes | No | Structure only (configurable) |
| **Stage channels** | Yes | No | Structure only (configurable) |
| **Forum channels** | Yes | Yes | Posts, tags, and guidelines synced |
| **Threads** | Yes | Yes | Automatically created when source thread exists |
| **Categories** | Yes | N/A | Organizational structure preserved |

## Channel properties

Depending on your [cloning options](/docs/configuration/cloning-options), Copycord can sync these channel properties:

- **Name** — Channel name (enabled by default)
- **Position** — Channel ordering (enabled by default)
- **Topic** — Channel description/topic
- **NSFW flag** — Age-restricted setting
- **Slowmode** — Message cooldown timer
- **Permissions** — Channel-level permission overwrites
- **Voice properties** — Bitrate and user limit
- **Forum properties** — Layout, tags, and posting guidelines
