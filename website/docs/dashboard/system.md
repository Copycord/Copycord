---
sidebar_position: 9
title: System
---

# System

The **System** page provides system-level management tools for your Copycord installation.

## Bot status

View real-time status for both bots:

- **Server bot** — Connection state, latency, uptime
- **Client bot** — Connection state, latency, uptime

## Logs

The system page provides access to application logs:

- **Server logs** — Activity from the Discord bot (message forwarding, structure sync)
- **Client logs** — Activity from the self-bot (message detection, event handling)
- **Scraper logs** — Member scraper activity

Logs stream in real time and can be cleared as needed.

## Event logs

The **Event Logs** page provides a structured audit trail of all Copycord operations:

- Message forwarding events
- Structure sync events (channel/role creation, deletion, rename)
- Backfill progress
- Error events
- System events

You can filter by event type, browse pages, and delete individual or bulk entries.

## Version information

The system page shows:

- Current Copycord version
- Available updates (if a newer release exists on GitHub)
- Release notes for the latest version

Copycord automatically checks for new releases every 30 minutes.

## Database management

- **Backup Now** — Create an immediate database backup
- **View Backups** — See all available backups with download/restore options
- See the [Backups](/docs/dashboard/backups) page for full details.
