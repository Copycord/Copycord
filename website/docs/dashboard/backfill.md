---
sidebar_position: 5
title: History Import (Backfill)
---

# History Import (Backfill)

Copycord's backfill feature lets you import historical messages from source channels — not just new ones sent after setup.

## Starting a backfill

1. Navigate to the **Dashboard** or **Channels** page
2. Select the channel(s) you want to backfill
3. Click **Start Backfill** (single channel) or **Batch Backfill** (multiple channels)
4. The import will begin processing in the background

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

If a backfill is interrupted (e.g., bot restart), you can resume it from where it left off. The dashboard shows resume information for incomplete backfills.

## Limits and considerations

- **Rate limits** — Discord imposes rate limits on message fetching. Copycord automatically handles these with built-in delays
- **Large channels** — Channels with tens of thousands of messages may take a while. The page delay prevents hitting rate limits
- **Attachments** — Media files are re-uploaded to the clone, which adds processing time
- **Order** — Messages appear in chronological order, but Discord may display them slightly differently since they're posted via webhook
