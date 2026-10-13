---
sidebar_position: 1
title: Dashboard Overview
---

# Web Dashboard Overview

Copycord's web dashboard is a modern, feature-rich interface for managing all aspects of your server cloning setup. Access it at **http://localhost:8080** (or your configured port).

## Dashboard home

The main dashboard page gives you an at-a-glance view of your entire setup:

- **Bot status** — Connection status and uptime for both the server bot and client bot
- **Guild mappings** — All your source → clone server pairs
- **Quick actions** — Start/stop bots, create new mappings
- **Activity feed** — Recent events and operations

## Navigation

The dashboard sidebar has these pages:

| Page | Purpose |
|------|---------|
| **Configuration** | Bot status, global config, guild mappings, start/stop |
| **Channels** | View and customize cloned channel names, orphan detection, backfill |
| **Guilds** | Browse all servers your self-bot is in, export messages |
| **Forwarding** | Set up message forwarding to Telegram, Pushover, etc. |
| **Scraper** | Member scraper tool for exporting user data |
| **System** | Version info, database backups |
| **Logs** | Event logs  |

## Authentication

The dashboard always asks for a password. The session stays signed in through a secure cookie. Click **Log out** in the sidebar to end it.

To change the password:
- **Docker**: Edit the `PASSWORD` environment variable in `docker-compose.yml`, then `docker compose up -d`
- **Install script**: Run `copycord config set PASSWORD 'your-new-password'`, then `copycord restart`

See [Security defaults](/docs/configuration/security) for who can reach the dashboard.

## Starting and stopping bots

The dashboard provides a **Start** button for both the server and client bots. Both must be running for cloning to work. Status chips show green "running" or red "stopped" with live uptime tracking.

Valid tokens are required to start.

### Add Bot to Server

Once your bot token is validated, an **Add Bot to Server** button appears next to the Start button. Click it to open Discord's bot authorization page with the correct permissions pre-configured — no need to manually build an invite URL.

## Global configuration

The Global Configuration card lets you set core settings:

- **SERVER_TOKEN** / **CLIENT_TOKEN** — Your Discord bot and account tokens
- **COMMAND_USERS** — User IDs allowed to run slash commands
- **LOG_LEVEL** — `INFO` or `DEBUG`
- **AUTO_START** — Automatically start bots when Copycord launches (requires valid tokens)
- **MAX_LOG_SIZE_MB** — Maximum log file size before old entries are pruned (default: 10 MB, set to 0 to disable)

## Log viewer

Click **View server logs** or **View client logs** on the Bots card to open the log viewer. It shows the full `server.out` / `client.out` logs; the terminal only shows their warnings and errors (see [Where logs go](/docs/dashboard/system#where-logs-go)). Features:

- Real-time log streaming
- Search/filter logs
- **Clear logs** button to wipe the selected log file (with confirmation)

## Real-time updates

The dashboard uses WebSocket connections for real-time updates. You'll see live status changes, log streaming, and operation progress without needing to refresh the page.
