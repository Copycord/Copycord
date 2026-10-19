---
title: Event logs
---

# Event logs

The **Logs** page (**Event Logs**) records what Copycord changed in your clone servers. Use it to check when a channel, role or webhook was created, changed or deleted, and to find errors.

It records these events:

- Structure sync: channels, categories, roles, emojis and stickers created, deleted, renamed or moved
- Permission sync
- Webhooks created
- Server details updated
- Thread operations
- Errors

For the bots' text logs, see [Where logs go](/docs/dashboard/system#where-logs-go).

## Find an event

1. Optional: in **Filter by type**, pick one event type, or **All Events**. The list shows only types that have entries.
2. Optional: type in **Search logs**. The search matches the event details, the channel, category and guild names, the log, guild and channel IDs, and the values in each event's details, such as a clone channel ID or a sync task ID.
3. Scroll the table. Entries load 50 at a time.
4. Click an entry to expand it. It shows the event's details, such as the source and clone IDs, the changed fields and the category.

To reload the list, click **Refresh logs**.

## Delete events

- To delete one entry, click **Delete** on it and confirm.
- To delete every entry, click **Clear all logs** and confirm.

Deleted entries cannot be restored.
