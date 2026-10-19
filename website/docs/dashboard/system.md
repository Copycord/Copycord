---
sidebar_position: 10
title: System
---

# System

The **System** page holds the **Database** card, where you manage database backups. The card is collapsed by default; click its header to open it.

## Database

- **Backup now**: create a database backup immediately
- **Download latest**: download the newest backup archive
- **Restore**: upload a `.tar.gz` backup archive and restore it
- The backup list shows each archive's **Name**, **Time** and **Size**, with **Download**, **Restore** and **Delete** actions

Restore and delete ask for confirmation first. See [Backups](/docs/dashboard/backups) for what a backup holds, the daily schedule and restoring from the command line.

## Version information

The sidebar footer shows the running Copycord version, linked to its release notes. When a newer release exists, **New Update Available** appears in the dashboard and links to the release page. See [`copycord update`](/docs/cli) for updating.

## Bot logs

The bots' logs are opened from the **Bots** card on the [Configuration page](/docs/dashboard/overview#log-viewer) with **View server logs** and **View client logs**. Event logs of what Copycord changed in the clone servers are on the [Logs](/docs/dashboard/logs) page.

### Where logs go {#where-logs-go}

Copycord runs the dashboard (admin), the server bot and the client together. Their logs go to three places:

| Where | What you see |
|-------|--------------|
| **Terminal** (`copycord run`), `docker compose logs`, or the service's log | The dashboard's log, plus **warnings and errors** from the server and client. Each line is tagged `[admin]`, `[server]` or `[client]`. |
| **Dashboard log viewers** | The **full** server and client logs, streamed live |
| **Log files** | `server.out` and `client.out`: the files behind the dashboard log viewers (plus `scraper.out` for the member scraper), in `logs` in the Copycord home (`~/.copycord/logs`), or in `/data` in Docker |

A terminal line looks like this:

```
2026-10-12 14:03:11 | WARNING | [client] [🔀] No healthy proxies available
```

The `LOG_LEVEL` setting (`INFO` or `DEBUG`) applies to all of them. `server.out` and `client.out` are rotated at about 10 MB, keeping one older file (`server.out.1`, `client.out.1`).

Where the [data directory](/docs/cli#data-dir) is depends on how you run Copycord: `./data` next to your `docker-compose.yml` for Docker, otherwise `~/.copycord` (Windows: `%USERPROFILE%\.copycord`), unless you set `COPYCORD_HOME` or `--data-dir`.

When Copycord runs as a [service](/docs/cli#copycord-service), the terminal output goes to:

- **Linux:** the journal: `journalctl --user -u copycord -f` for the service of your account (the default), `sudo journalctl -u copycord -f` for a `--system` service
- **macOS:** nowhere visible; the same lines are in the log files in `<home>/logs`
- **Windows:** nowhere visible; the same lines are in `copycord.out` and the other log files in `<home>\logs`, and in the dashboard log viewers

### Log pruning

Copycord automatically prunes log files to prevent them from growing indefinitely. Configure the maximum log file size via the **Max Log Size** setting (`LOG_MAX_SIZE_MB`) in Global Configuration (default: 10 MB). Set to 0 to disable pruning. The pruner checks log file sizes every 5 minutes and trims older entries when a file exceeds the limit.
