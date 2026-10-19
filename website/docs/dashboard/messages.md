---
title: Messages
---

# Messages

The **Messages** page lists every message Copycord has cloned. Use it to find a cloned message and open it in Discord.

## Find a message

1. Optional: in **Search messages**, type an author's name or username. A numeric ID matches the original or cloned message ID, the author ID, or the original or cloned channel ID.
2. Optional: narrow the list with the filters:
   - **Filter by mapping**: **All mappings** or one mapping.
   - **Filter by channel**: **All channels** or one channel.
   - **Filter by attachments**: **Any attachments**, **With attachments** or **No attachments**.
   - **Sort messages**: **Ascending** or **Descending** by time.
3. Scroll the table. Rows load 50 at a time.

To choose the table's columns, such as the author, channel, guild, IDs, attachments and embeds, open the **Visible columns** menu. At least one column stays visible.

## Open a message

1. Click the message's row. The preview opens with the author, the time, the message's IDs and links, and the message itself.
2. Optional: click **Open original** or **Open clone** to open the message in Discord.

The preview reads the message from Discord through the server bot, so the bot needs access to the channel. If the record was purged, the preview says it was removed from the database.

## Remove old records

Copycord keeps a record for each cloned message so that it can mirror edits and deletes. To remove old records, run a [Purge messages](/docs/dashboard/purge-and-sync#purge-messages) task.
