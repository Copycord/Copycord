---
title: Purge and Sync
---

# Purge and Sync

Three task types on the [Tasks](/docs/dashboard/tasks) page clean up or realign what Copycord keeps and what it cloned. Open the **Tasks** page, pick the type in **Task types** and click **Run task**.

## Purge messages

Deletes Copycord's stored message records: the links from each source message to its clone. These records let Copycord mirror edits and deletes, and let a backfill with **Ignore cloned messages** skip messages already cloned. Messages on Discord are not touched.

This task does not need the bots running.

| Option | Description |
|--------|-------------|
| **Older than** | A whole number with a unit: hours, days or weeks. Default: 30 days |
| **All records** | Delete every record instead of only the old ones |
| **Mapping** | Limit the purge to one mapping. Default: **All mappings** |
| **Dry run: count the records, delete nothing** | Count what would be deleted without deleting it |

**Preview count** shows how many records match before you start. **Delete records** asks for confirmation; with **Dry run** ticked the button reads **Start dry run**.

After a purge, edits and deletes of those messages are no longer mirrored, and a backfill with **Ignore cloned messages** may copy them again.

## Purge assets

Deletes emojis, stickers and roles in a mapping's clone server through the server bot, and forgets the mappings of the cloned ones. The host server is never touched.

| Option | Description |
|--------|-------------|
| **Mapping** | The mapping whose clone server to clean |
| Kinds | **Emojis**, **Stickers** and **Roles**; pick at least one |
| **Which assets** | **Only what Copycord cloned** (default), **Only what Copycord did not clone**, or **Everything** |
| **Dry run: count the assets, delete nothing** | Count what would be deleted without deleting it |

The form counts the matching assets for the picked mapping, which needs the bots running. Without a dry run, the deletion asks for confirmation. Deleted assets are gone for good on Discord.

## Sync assets

Brings the clone server's roles, emojis and stickers in line with the host through the server bot: creates what is missing, updates what changed and puts the roles in the host's order. The host server is never touched.

| Option | Description |
|--------|-------------|
| **Mapping** | The mapping to sync |
| Kinds | **Roles**, **Emojis** and **Stickers**; all are picked by default |
| **Delete extras: remove what the clone has and the host does not** | Also delete clone assets that have no match on the host. This asks for confirmation and cannot be undone |

:::caution Role order
Make sure the Copycord bot's role is the highest role in the clone server, or one below your admin role, or it cannot move the roles. Roles that are not cloned from the host end up above the cloned roles.
:::

The run's summary counts the assets created, updated, deleted, skipped and failed, and the roles reordered.

Both asset tasks need the server and client bots running.
