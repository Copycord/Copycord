---
sidebar_position: 6
title: History Import (Backfill)
---

# History Import (Backfill)

Copycord's backfill feature lets you import historical messages from source channels — not just new ones sent after setup. You can backfill a single channel or multiple channels at once using the batch backfill.

## Starting a backfill

1. Navigate to the **Channels** page
2. Select the channel(s) you want to backfill
3. Click **Clone** (single channel) or select multiple channels and click **Batch Backfill**
4. Configure the backfill options in the popup dialog
5. Click **Start** — the import will begin processing in the background

### Single channel backfill

Click the clone button on any individual channel card to open the backfill dialog for that specific channel.

### Batch backfill

Select multiple channels by clicking their cards (they'll highlight when selected), then click the batch action button. The batch dialog lets you configure one set of options that apply to all selected channels at once.

## Range options

When starting a backfill, you can choose how far back to import:

| Option | Description |
|--------|-------------|
| **All history** | Import the entire message history of the channel from the very beginning |
| **Since date/time** | Import messages sent after a specific date and time |
| **Between dates** | Import messages within a specific date and time range (from/to) |
| **Last N messages** | Import only the most recent N messages |

The **Since** and **Between** options support both date-only and date-with-time selection. If you only pick a date without a time, it defaults to the start of that day (midnight). If you select a specific time, the backfill will use the exact timestamp.

:::tip Date range for Between mode
When using **Between dates** with date-only values, the "To" date is inclusive — the range extends through the end of that day. When a specific time is selected, the exact timestamp is used as the cutoff.
:::

## Backfill mode (batch only)

The batch backfill dialog includes a **Mode** selector:

| Mode | Description |
|------|-------------|
| **Resume previous** | Resumes any in-progress backfill runs. If no previous run exists for a channel, a fresh run is started automatically |
| **Start new** | Discards any in-progress runs and begins fresh backfills for all selected channels |

## Settings

### Ignore cloned messages

When enabled, the backfill will skip messages that have already been cloned to the target server. Before starting the backfill, Copycord checks its local database for existing cloned message records and filters out any original message IDs that already have a clone. The server only receives the remaining uncloned messages.

This is useful when:
- You need to re-run a backfill but don't want to duplicate messages that were already cloned
- A previous backfill was partially completed and you want to fill in the gaps without duplicates

:::caution DB_CLEANUP_MSG and message records
This feature relies on cloned message records stored in the database. If `DB_CLEANUP_MSG` is enabled in your [mapping settings](/docs/configuration/cloning-options#database-maintenance), old message records are periodically purged — which means previously cloned messages may no longer be detected. Consider disabling `DB_CLEANUP_MSG` for mappings where you plan to use this feature, so that message records are retained.
:::

## How it works

When you start a backfill:

1. The **client** fetches message history from the source channel page by page
2. Messages are sent to the **server** via WebSocket
3. The **server** posts them to the clone channel using webhooks
4. Progress is tracked and displayed in the dashboard

Messages are imported in chronological order (oldest first), preserving the conversation flow.

## Monitoring progress

The dashboard shows:

- **Queue** — Backfill jobs waiting to start
- **In-flight** — Currently running backfill operations
- **Progress** — Messages delivered vs. expected for each job

## Resuming interrupted backfills

If a backfill is interrupted (e.g., bot restart, network issue), you can resume it from where it left off. The dashboard shows resume information for incomplete backfills, including the last checkpoint and number of messages delivered.

For single channel backfills, Copycord will automatically prompt you to resume if an incomplete run is detected. For batch backfills, select **Resume previous** in the Mode selector.

## Limits and considerations

- **Rate limits** — Discord imposes rate limits on message fetching. Copycord automatically handles these with built-in delays
- **Large channels** — Channels with tens of thousands of messages may take a while. The page delay prevents hitting rate limits
- **Attachments** — Media files are re-uploaded to the clone, which adds processing time
- **Order** — Messages appear in chronological order, but Discord may display them slightly differently since they're posted via webhook
