---
sidebar_position: 10
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

### Where logs go {#where-logs-go}

Copycord runs the dashboard (admin), the server bot and the client together. Their logs go to three places:

| Where | What you see |
|-------|--------------|
| **Terminal** (`copycord run`), `docker compose logs`, or the service's log | The dashboard's log, plus **warnings and errors** from the server and client. Each line is tagged `[admin]`, `[server]` or `[client]`. |
| **Dashboard log viewers** | The **full** server and client logs, streamed live |
| **Data directory** | `server.out` and `client.out`: the files behind the dashboard log viewers (plus `scraper.out` for the member scraper) |

A terminal line looks like this:

```
2026-10-12 14:03:11 | WARNING | [client] [🔀] No healthy proxies available
```

The `LOG_LEVEL` setting (`INFO` or `DEBUG`) applies to all of them. `server.out` and `client.out` are rotated at about 10 MB, keeping one older file (`server.out.1`, `client.out.1`).

Where the [data directory](/docs/cli#data-dir) is depends on how you run Copycord: `./data` next to your `docker-compose.yml` for Docker, otherwise `~/.copycord` (Windows: `%USERPROFILE%\.copycord`), unless you set `COPYCORD_HOME` or `--data-dir`.

When Copycord runs as a [service](/docs/cli#copycord-service), the terminal output goes to:

- **Linux:** the journal: `journalctl --user -u copycord -f` for a `--user` service, `sudo journalctl -u copycord -f` for the system service
- **macOS:** `copycord.out.log` and `copycord.err.log` in the data directory
- **Windows:** nowhere visible; use the dashboard log viewers or the files in the data directory

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
