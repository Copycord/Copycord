---
title: Event Logs
---

# Event Logs

The **Logs** page (**Event Logs**) is a structured audit trail of what Copycord changed in your clone servers:

- Structure sync events (channel, category, role, emoji and sticker creation, deletion, rename and moves)
- Permission sync events
- Webhook creation events
- Guild metadata updates
- Thread operations
- Error events

For the bots' text logs, see [Where logs go](/docs/dashboard/system#where-logs-go).

## Features

- **Search logs**: matches the event details, the channel, category and guild names, the log, guild and channel IDs, and the values in each event's metadata, such as a clone channel ID or a sync task ID
- **Filter by type**: **All Events** or one event type. The dropdown only shows event types that have logs
- **Expandable rows**: click an entry to see its details and metadata (source and clone IDs, changed fields, category info)
- **Refresh logs**: reload the current view
- **Delete**: delete a single entry, or **Clear all logs**. Both ask for confirmation and cannot be undone

Logs load 50 at a time; the next 50 load as you scroll to the end of the table.
