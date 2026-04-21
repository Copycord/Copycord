---
sidebar_position: 6
title: Assets
---

# Asset Commands

Manage emojis, stickers, and roles in your clone server.

## `/purge_assets`

Bulk delete emojis, stickers, or roles from the current guild. This is a **destructive** operation.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `kind` | Yes | What to purge: `emojis`, `stickers`, or `roles` |
| `confirm` | Yes | Must type `confirm` to execute |
| `unmapped_only` | No | Only delete assets **not** tracked in the database |
| `cloned_only` | No | Only delete assets that **were** cloned by Copycord |

:::warning
`unmapped_only` and `cloned_only` are mutually exclusive — you can only use one at a time.
:::

**Usage:**
```
/purge_assets roles confirm
/purge_assets emojis confirm unmapped_only:true
/purge_assets stickers confirm cloned_only:true
```

:::tip
Make sure the Copycord bot role is positioned **at the top** of the role list to be able to delete roles above it.
:::

---

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
