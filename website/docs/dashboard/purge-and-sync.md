---
title: Purge and sync
---

# Purge and sync

Three task types on the [Tasks](/docs/dashboard/tasks) page clean up what Copycord stores and what it cloned: **Purge messages**, **Purge assets** and **Sync assets**.

## Purge messages

**Purge messages** deletes Copycord's stored message records, the links from each source message to its clone. Messages on Discord are not touched, and the bots do not need to be running.

Copycord uses these records to mirror edits and deletes, and a backfill with **Ignore cloned messages** uses them to skip messages already cloned. After a purge, edits and deletes of those messages are no longer mirrored, and a backfill may copy them again.

To purge message records:

1. Open the **Tasks** page, pick **Purge messages** in **Task types** and click **Run task**.
2. In **Older than**, enter a whole number and pick hours, days or weeks. The default is 30 days. To delete every record instead, pick **All records**.
3. Optional: in **Mapping**, pick one mapping. The default is **All mappings**.
4. Optional: click **Preview count** to see how many records match.
5. Optional: tick **Dry run: count the records, delete nothing** to count without deleting.
6. Click **Delete records** and confirm. With **Dry run** ticked, the button reads **Start dry run**.

## Purge assets

**Purge assets** deletes emojis, stickers and roles in a mapping's clone server through the server bot. The host server is never touched. The server bot and the client must be running.

:::warning
Assets deleted without a dry run are gone for good on Discord.
:::

To purge assets:

1. Open the **Tasks** page, pick **Purge assets** in **Task types** and click **Run task**.
2. In **Mapping**, pick the mapping whose clone server to clean.
3. Tick the kinds to delete: **Emojis**, **Stickers** and **Roles**. Pick at least one. Each kind shows how many assets a run would delete.
4. In **Which assets**, pick **Only what Copycord cloned** (the default), **Only what Copycord did not clone** or **Everything**.
5. Optional: click **Preview count** to see how many assets match.
6. Optional: tick **Dry run: count the assets, delete nothing** to count without deleting.
7. Click **Purge assets** and confirm. With **Dry run** ticked, the button reads **Start dry run**.

Roles at or above the Copycord bot's role are not deleted.

## Sync assets

**Sync assets** brings the clone server's roles, emojis and stickers in line with the host through the server bot: it creates what is missing, updates what changed and puts the roles in the host's order. The host server is never touched. The server bot and the client must be running.

Before you sync, make the Copycord bot's role the highest role in the clone server, or the one below your admin role. Otherwise the bot cannot move the roles.

To sync assets:

1. Open the **Tasks** page, pick **Sync assets** in **Task types** and click **Run task**.
2. In **Mapping**, pick the mapping to sync.
3. Optional: untick the kinds to leave alone. **Roles**, **Emojis** and **Stickers** are all picked by default.
4. Optional: tick **Delete extras: remove what the clone has and the host does not** to also delete clone assets with no match on the host. This cannot be undone.
5. Click **Sync assets**. With **Delete extras** ticked, confirm.

Roles that were not cloned from the host end up above the cloned roles. The run's summary counts the assets created, updated, deleted, skipped and failed, and the roles reordered.
