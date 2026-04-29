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

Logs stream in real time. Each log viewer has its own **Clear logs** button to wipe that specific log file.

### Log pruning

Copycord automatically prunes log files to prevent them from growing indefinitely. Configure the maximum log file size via the **MAX_LOG_SIZE_MB** setting in Global Configuration (default: 10 MB). Set to 0 to disable pruning. The pruner checks log file sizes every 5 minutes and trims older entries when a file exceeds the limit.

## Event logs

The **Event Logs** page provides a structured audit trail of all Copycord operations:

- Structure sync events (channel/role/emoji/sticker creation, deletion, rename)
- Permission sync events
- Webhook creation events
- Guild metadata updates
- Thread operations
- Error events

### Features

- **Filter by type** — Dropdown only shows event types that have actual logs
- **Expandable rows** — Click any log entry to see detailed metadata (source/clone IDs, sync task ID, category info)
- **Refresh** — Refresh button to reload the current view
- **Delete** — Delete individual entries or clear all logs

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
