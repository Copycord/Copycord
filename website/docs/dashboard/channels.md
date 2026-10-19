---
sidebar_position: 4
title: Channels
---

# Channels

The **Channels** page lets you view the channel structure of your cloned servers and customize how channels appear. Both the server and client bots must be running for the page to load.

To import message history into cloned channels, use the [Backfill channels](/docs/dashboard/backfill) task on the **Tasks** page.

## Viewing channels

Pick a mapping in **Guild mapping**. The page shows the host server's **categories** and their channels as a tree, with each channel's state: cloned, **Not cloned yet**, customized, or **Orphan**.

- **Search channels** filters by name or ID
- **Filter**: **All channels**, **Orphans only** or **Non-orphans only**
- **Sort**: by **Name**, **Category** or **Type**

## Row menu

Each channel and category row has a menu:

- **Customize**: set a custom name (see below)
- **Copy original channel ID** / **Copy clone channel ID** (and the category equivalents)
- **Add to blacklist**: add the channel or category to the mapping's [filters](/docs/dashboard/filters) so it is no longer cloned
- **Delete orphan**: on an orphan, delete it from the clone server

## Customizing channel names

You can rename cloned channels to anything you want without affecting the source server:

1. Open the row menu of the channel and click **Customize**
2. Enter the **Custom channel name**. Leave it empty to use the original name
3. Save

The custom name persists even if the source channel is renamed: Copycord keeps your custom name instead of syncing the rename.

:::tip Category names
You can also customize category names the same way. This lets you completely rebrand the clone server's layout.
:::

## Orphans

An orphan is a channel or category that exists in the clone but is no longer in the host server. Click **Fetch orphans** to scan the clone against the last sitemap. The scan reports what it found; delete orphans one at a time from the row menu or all at once with **Delete all**, which asks for confirmation.

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

- **Name**: channel name (**Rename Channels**)
- **Position**: channel ordering (**Reposition Channels**)
- **Topic**: channel description (**Sync Channel Topic**)
- **NSFW flag**: age-restricted setting (**Sync NSFW Flag**)
- **Slowmode**: message cooldown timer (**Sync Slowmode**)
- **Permissions**: channel-level permission overwrites (**Mirror Channel Permissions**)
- **Voice and stage properties**: bitrate and user limit (**Sync Voice Properties**, **Sync Stage Properties**)
- **Forum properties**: layout, tags, and posting guidelines (**Sync Forum Properties**)
