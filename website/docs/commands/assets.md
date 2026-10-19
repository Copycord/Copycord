---
sidebar_position: 6
title: Assets
---

# Asset commands

This command saves a server's emojis and stickers to an archive. To delete emojis, stickers or roles from a clone server, or to bring them in line with the host, use the [Purge assets and Sync assets](/docs/dashboard/purge-and-sync) tasks.

## `/pull_assets`

Saves a server's emojis, stickers or both to a ZIP archive.

```
/pull_assets asset:<both|emojis|stickers> [guild_id:<id>]
```

| Option | Required | Description |
|--------|----------|-------------|
| `asset` | Yes | What to save: `both`, `emojis` or `stickers` |
| `guild_id` | No | The ID of the server to save from. Default: the host server of this clone |

```
/pull_assets asset:both
/pull_assets asset:emojis
/pull_assets asset:stickers guild_id:159962941502783488
```

The client downloads the assets, so your user account must be in the server. The archive is saved under `assets/` in the data directory (`/data/assets/` in Docker), with a folder per asset type. The reply shows how many emojis and stickers were saved and where the archive is.
