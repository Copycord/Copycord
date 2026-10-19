---
sidebar_position: 6
title: Assets
---

# Asset Commands

Export emojis and stickers from a server.

:::info
Bulk deleting emojis, stickers, or roles is the [Purge assets](/docs/dashboard/purge-and-sync#purge-assets) task on the dashboard's Tasks page. Pick a
mapping, the kinds of asset, and whether to delete only what Copycord cloned (the default), only what it did not
clone, or everything. A dry run counts without deleting, and the run page shows each delete as it happens.
Roles at or above the Copycord bot's role are left alone, so keep the bot role at the top of the role list.

To bring a clone's roles, emojis and stickers back in line with the host, run the [Sync assets](/docs/dashboard/purge-and-sync#sync-assets) task.
:::

## `/pull_assets`

Export server emojis and/or stickers to a compressed ZIP archive.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `asset` | Yes | What to export: `both`, `emojis`, or `stickers` |
| `guild_id` | No | Server ID to pull from. Leave blank for the host server of this clone |

**Usage:**
```
/pull_assets both
/pull_assets emojis
/pull_assets stickers 159962941502783488
```

The client downloads the assets, so it must be in that server. The archive is saved under `assets/` in the data directory (`/data/assets/` in Docker) and organized by asset type. The reply shows how many emojis and stickers were saved and the archive path.
