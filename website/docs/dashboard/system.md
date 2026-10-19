---
sidebar_position: 10
title: System
---

# System

The **System** page holds the **Database** card, where you make, download, restore and delete database backups. This page also explains where Copycord writes its logs.

## Database card

The card is closed by default. To open it, click its header.

| Control | What it does |
|---------|--------------|
| **Backup now** | Makes a database backup |
| **Download latest** | Downloads the newest backup archive |
| **Restore** (top of the card) | Uploads a `.tar.gz` backup archive and restores it |
| Backup list | Shows each archive's **Name**, **Time** and **Size**, with **Download**, **Restore** and **Delete** |

**Restore** and **Delete** ask you to confirm. For the steps, the daily schedule and restoring from the command line, see [Backups](/docs/dashboard/backups).

## Version information

The sidebar footer shows the running Copycord version, linked to its release notes. When a newer release exists, **New Update Available** appears in the dashboard and links to the release page. To update, see [`copycord update`](/docs/cli).

## Where logs go {#where-logs-go}

Copycord runs three parts together: the dashboard (admin), the server bot and the client. Their logs go to three places.

| Where | What you see |
|-------|--------------|
| The terminal running `copycord run`, `docker compose logs`, or the service's log | The dashboard's log, plus warnings and errors from the server and client. Each line is tagged `[admin]`, `[server]` or `[client]` |
| The dashboard log viewers | The full server and client logs, live. Open them with **View server logs** and **View client logs** on the **Bots** card; see [Log viewer](/docs/dashboard/overview#log-viewer) |
| Log files | `server.out` and `client.out`, the files behind the log viewers, plus `scraper.out` for the member scraper. They are in the `logs` folder of the data directory (`~/.copycord/logs`), or in `/data` in Docker |

A terminal line looks like this:

```
2026-10-12 14:03:11 | WARNING | [client] [🔀] No healthy proxies available
```

The `LOG_LEVEL` setting, `INFO` or `DEBUG`, applies to all of them. `server.out` and `client.out` are rotated at about 10 MB, and one older file is kept (`server.out.1`, `client.out.1`).

The [data directory](/docs/cli#data-dir) is `./data` next to your `docker-compose.yml` in Docker, and `~/.copycord` otherwise (Windows: `%USERPROFILE%\.copycord`). `COPYCORD_HOME` or `--data-dir` changes it.

When Copycord runs as a [service](/docs/cli#copycord-service), the terminal output goes to:

- Linux: the journal. For a service of your account, the default, run `journalctl --user -u copycord -f`. For a `--system` service, run `sudo journalctl -u copycord -f`.
- macOS: nowhere visible. The same lines are in the log files in `<home>/logs`.
- Windows: nowhere visible. The same lines are in `copycord.out` and the other log files in `<home>\logs`, and in the dashboard log viewers.

To see what Copycord changed in your clone servers, use the [Logs](/docs/dashboard/logs) page instead.

## Log size limit

Copycord trims old entries from a log file when the file grows past **Max Log Size** (`LOG_MAX_SIZE_MB`) in **Global Configuration**. The default is 10 MB; `0` turns trimming off. Copycord checks the sizes every 5 minutes.
