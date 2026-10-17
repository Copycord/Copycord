---
sidebar_position: 6
title: Assets
---

# Asset Commands

Manage emojis, stickers, and roles in your clone server.

:::info
Bulk deleting emojis, stickers, or roles is now the **Purge assets** task on the dashboard's Tasks page. Pick a
mapping, the kinds of asset, and whether to delete only what Copycord cloned (the default), only what it did not
clone, or everything. A dry run counts without deleting, and the run page shows each delete as it happens.
Roles at or above the Copycord bot's role are left alone, so keep the bot role at the top of the role list.
:::

## `/pull_assets`

Export server emojis and/or stickers to a compressed ZIP archive.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `asset` | Yes | What to export: `both`, `emojis`, or `stickers` |
| `guild_id` | No | Target server ID (defaults to the current server) |

**Usage:**
```
/pull_assets both
/pull_assets emojis
/pull_assets stickers 159962941502783488
```

The archive is saved to `/data/assets/` and organized by asset type.
