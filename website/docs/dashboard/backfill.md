---
title: History Import (Backfill)
---

# History Import (Backfill)

The **Backfill channels** task imports the message history of source channels into their clones, not just the messages sent after setup. One run can cover one channel, several channels or whole categories.

Both the server and the client bots must be running to start a backfill.

## Starting a backfill

1. Open the **Tasks** page and pick **Backfill channels** in **Task types**
2. Click **Run task**
3. Pick the **Mapping**
4. Under **Channels**, tick the channels or categories to import. Ticking a category picks every channel in it; the search box filters the list by name
5. Choose **How far back?** and the settings below
6. Click **Start**

The run page opens and shows one step per channel with its progress.

## Range options

| Option | Description |
|--------|-------------|
| **All history** | Import the entire message history of the channel from the very beginning |
| **Since date/time** | Import messages sent after a specific date and time |
| **Between dates** | Import messages within a specific date and time range (**From** and **To**) |
| **Last N messages** | Import only the most recent N messages |

The **Since** and **Between** options take a date and an optional time. A date without a time starts at midnight. If you select a specific time, the backfill uses the exact timestamp.

:::tip Date range for Between mode
When using **Between dates** with date-only values, the **To** date is inclusive: the range extends through the end of that day. When a specific time is selected, the exact timestamp is used as the cutoff.
:::

## Settings

### Ignore cloned messages

When enabled, the backfill skips messages that have already been cloned to the target server. Before starting the backfill, Copycord checks its local database for existing cloned message records and filters out any original message IDs that already have a clone. The server only receives the remaining uncloned messages.

This is useful when:
- You need to re-run a backfill but don't want to duplicate messages that were already cloned
- A previous backfill was partially completed and you want to fill in the gaps without duplicates

:::caution Message records
This feature relies on cloned message records stored in the database. Messages whose records were deleted, for example by a [Purge messages](/docs/dashboard/purge-and-sync#purge-messages) run, are no longer detected as cloned.
:::

### Mode

When some of the picked channels have an unfinished backfill, the form shows **Some picked channels have an unfinished backfill** and a mode choice:

| Mode | Description |
|------|-------------|
| **Resume previous** | Continue each unfinished backfill from its last checkpoint. A channel without one starts fresh |
| **Start new** | Discard the unfinished backfills and start fresh for all picked channels |

## How it works

When you start a backfill:

1. The **client** fetches message history from the source channel page by page
2. Messages are sent to the **server** via WebSocket
3. The **server** posts them to the clone channel using webhooks
4. Progress is shown on the run page: each channel step reads how many messages were delivered and pulled

Messages are imported in chronological order (oldest first), preserving the conversation flow. Channels waiting their turn show as queued with their place in the queue.

The run's summary counts the messages delivered and the channel steps done, failed, skipped and cancelled.

## Resuming an unfinished backfill

If a backfill is interrupted (for example by a restart or **Cancel run**), its channels keep a checkpoint. Start a new **Backfill channels** run for the same channels and pick **Resume previous** to continue from where each one stopped.

If the bots stop during a backfill, the run ends as cancelled with **Bots stopped**.

## Limits and considerations

- **Rate limits**: Discord imposes rate limits on message fetching. Copycord handles these with built-in delays
- **Large channels**: Channels with tens of thousands of messages may take a while
- **Attachments**: Media files are re-uploaded to the clone, which adds processing time
- **Order**: Messages appear in chronological order, but Discord may display them slightly differently since they're posted via webhook
