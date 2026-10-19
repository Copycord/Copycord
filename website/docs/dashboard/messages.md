---
title: Messages
---

# Messages

The **Messages** page lists every message Copycord has cloned, from the message records in its database, in one table. Rows load 50 at a time as you scroll.

## Search and filters

- **Search messages**: matches the author name or username. A numeric ID matches the original or cloned message ID, the author ID, or the original or cloned channel ID
- **Filter by mapping**: **All mappings** or one mapping
- **Filter by channel**: **All channels** or one channel
- **Filter by attachments**: **Any attachments**, **With attachments** or **No attachments**
- **Sort messages**: **Ascending** or **Descending** by time

The **Visible columns** menu picks which columns the table shows, such as the author, channel, guild, IDs, attachments and embeds. At least one column stays visible.

## Message preview

Click a row to open its preview: the author and time, the message's IDs and links, and the message itself. **Open original** and **Open clone** open the message in Discord.

The preview reads the message from Discord through the server bot, so the bot needs access to the channel. If the record was purged in the meantime, the preview says it was removed from the database.

Message records are what lets Copycord mirror edits and deletes. To remove old records, run a [Purge messages](/docs/dashboard/purge-and-sync#purge-messages) task.
