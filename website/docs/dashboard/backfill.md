---
title: History import (backfill)
---

# History import (backfill)

The **Backfill channels** task copies the message history of source channels into their clones, so the clone also has the messages sent before you set up Copycord. One run can cover one channel, several channels or whole categories.

Both the server bot and the client must be running to start a backfill.

## Import channel history

1. Open the **Tasks** page and pick **Backfill channels** in **Task types**.
2. Click **Run task**.
3. In **Mapping**, pick the mapping.
4. Under **Channels**, tick the channels to import. Ticking a category picks every channel in it. To find a channel, type its name in the search box.
5. Under **How far back?**, pick a range. See [Range options](#range-options).
6. Optional: turn on **Ignore cloned messages** to skip messages that are already in the clone.
7. If the form shows **Some picked channels have an unfinished backfill**, pick **Resume previous** or **Start new**. See [Resume an unfinished backfill](#resuming-an-unfinished-backfill).
8. Click **Start**.

The run page opens with one step per channel. Each step shows how many messages were pulled and delivered; channels waiting their turn show their place in the queue. Messages are posted oldest first.

When the run ends, its summary counts the messages delivered and the channel steps done, failed, skipped and cancelled.

## Range options

| Option | What is imported |
|--------|------------------|
| **All history** | Every message, from the start of the channel |
| **Since date/time** | Messages sent after a date and time |
| **Between dates** | Messages between a **From** and a **To** date and time |
| **Last N messages** | The most recent N messages |

**Since date/time** and **Between dates** take a date and an optional time. A date without a time means midnight at the start of that day, except for the **To** date of **Between dates**, which includes the whole day. With a time, the exact time is used.

## Avoid duplicate messages

**Ignore cloned messages** skips every message that already has a clone, so you can run a backfill again, or fill the gaps after a partial one, without posting duplicates. Copycord decides this from its stored message records before the run starts.

Messages whose records were deleted, for example by a [Purge messages](/docs/dashboard/purge-and-sync#purge-messages) run, count as not cloned and are copied again.

## Resuming an unfinished backfill

When a backfill is interrupted, for example by a restart or **Cancel run**, each of its channels keeps a checkpoint. To continue:

1. Start a new **Backfill channels** run for the same channels.
2. When the form shows **Some picked channels have an unfinished backfill**, pick **Resume previous**.
3. Click **Start**.

| Mode | Effect |
|------|--------|
| **Resume previous** | Each unfinished channel continues from its checkpoint. A channel without one starts from the beginning |
| **Start new** | The checkpoints are discarded and every picked channel starts from the beginning |

If the bots stop during a backfill, the run is cancelled with **Bots stopped**.

## How long a backfill takes

- Discord limits how fast messages can be read and posted. Copycord waits as Discord asks, so channels with tens of thousands of messages can take a long time.
- Attachments are downloaded and uploaded again, which adds time.
- Messages are posted in their original order, but each shows the time it was posted to the clone.
