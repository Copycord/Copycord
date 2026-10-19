---
sidebar_position: 4
title: Channels
---

# Channels

The **Channels** page shows the channels of each mapping and their clones. Use it to rename channels in the clone, copy channel IDs, block a channel from cloning and delete orphans.

The page loads once both the server bot and the client are running. To import the message history of channels, use the [Backfill channels](/docs/dashboard/backfill) task instead.

## View a mapping's channels

1. In **Guild mapping**, pick the mapping.
2. Optional: narrow the list. Type a name or ID in **Search channels**, pick **All channels**, **Orphans only** or **Non-orphans only** in **Filter**, or pick **Name**, **Category** or **Type** in **Sort**.

The page shows the host server's categories and channels as a tree. Each row shows the channel's state: cloned, **Not cloned yet**, customized or **Orphan**.

Each channel and category row has a menu with these actions:

- **Customize** or **Customize category**: give the clone a custom name. Shown once the channel is cloned.
- **Copy original channel ID** and **Copy clone channel ID**, or **Copy original category ID** and **Copy clone category ID**.
- **Add channel to blacklist** or **Add category to blacklist**: add it to the mapping's [filters](/docs/dashboard/filters), so it is no longer cloned.
- **Delete orphan** or **Delete orphan category**: on an orphan, delete it from the clone server.

## Rename a channel in the clone

1. Open the menu on the channel's row and click **Customize**.
2. In **Custom channel name**, enter the new name. To go back to the original name, leave it empty.
3. Click **Save**.

The custom name stays when the source channel is renamed. To rename a category, use **Customize category** and **Custom category name** the same way. The source server is never changed.

## Delete orphans

An orphan is a channel or category that exists in the clone but no longer exists in the host server. The page checks for orphans when it loads a mapping and marks them **Orphan**.

- To delete one orphan, open the menu on its row, click **Delete orphan** or **Delete orphan category**, and confirm.
- To delete all of them, click **Delete *n* orphans** in the toolbar, then click **Delete all** to confirm.

Which channel types are cloned, and which channel properties are kept in sync, is described in [Structure sync](/docs/features/structure-sync).
