---
sidebar_position: 1
title: Dashboard Overview
---

# Web Dashboard Overview

Copycord's web dashboard is where you manage your whole cloning setup. Access it at **http://localhost:8080** (or your configured port).

## Navigation

The dashboard sidebar has these pages:

| Page | Purpose |
|------|---------|
| **Configuration** | Bots, global config, guild mappings, notifications, sync settings and proxies |
| **Channels** | View and customize cloned channel names, find and delete orphans |
| **Messages** | Search every cloned message |
| **Guilds** | Browse the servers your self-bot is in, start a message export |
| **Tasks** | Run and follow long jobs: backfills, exports, purges, asset syncs, avatar downloads and member scrapes |
| **Forwarding** | Set up message forwarding to Telegram, Pushover, etc. |
| **System** | Database backups |
| **Logs** | Event logs |

See [Tasks](/docs/dashboard/tasks), [Messages](/docs/dashboard/messages) and [Event Logs](/docs/dashboard/logs) for the newer pages.

## Authentication

The dashboard always asks for a password. The session stays signed in through a secure cookie. Click **Log out** in the sidebar to end it.

To change the password:
- **Docker**: Edit the `PASSWORD` environment variable in `docker-compose.yml`, then `docker compose up -d`
- **Install script**: Run `copycord config set PASSWORD 'your-new-password'`, then `copycord restart`

See [Security defaults](/docs/configuration/security) for who can reach the dashboard.

## The Configuration page

The Configuration page is the dashboard home. Its cards, top to bottom:

- **Bots**: status and uptime of the server and client, the **Start** / **Stop** button and the log viewers
- **Global Configuration**: tokens and core settings
- **Cloning Configuration**: your [guild mappings](/docs/dashboard/guild-mappings)
- **Notifications**, **Sync Settings** and **Proxy**: see [Advanced configuration](/docs/configuration/advanced)

When a newer release exists, **New Update Available** appears at the top and links to the release page.

## Starting and stopping bots

The **Start** button on the **Bots** card starts the server and client bots; while they run it reads **Stop**. Both must be running for cloning to work. The card shows each bot's status and live uptime.

Valid tokens are required to start. Guild mappings can only be added or edited while the bots are stopped.

### Add Bot to Server

Once your bot token is validated, an **Add Bot to Server** link appears in Global Configuration. Click it to open Discord's bot authorization page with the correct permissions pre-configured, so you don't have to build an invite URL by hand.

## Global configuration

The **Global Configuration** card lets you set core settings. Click **Save** to apply them.

- **SERVER_TOKEN** / **CLIENT_TOKEN**: your Discord bot and account tokens. **Manage backup tokens** holds fallback client tokens
- **Command Users** (`COMMAND_USERS`): user IDs allowed to run slash commands
- **Log Level** (`LOG_LEVEL`): `INFO` or `DEBUG`
- **Auto Start** (`COPYCORD_AUTOSTART`): start the bots automatically when Copycord launches (requires valid tokens)
- **Max Log Size** (`LOG_MAX_SIZE_MB`): maximum log file size before old entries are pruned (default: 10 MB, set to 0 to disable)

## Log viewer

Click **View server logs** or **View client logs** on the **Bots** card to open the log viewer. It shows the full `server.out` / `client.out` logs; the terminal only shows their warnings and errors (see [Where logs go](/docs/dashboard/system#where-logs-go)). Features:

- Real-time log streaming
- **Search logs** to filter the lines
- **Clear logs** to wipe the selected log file (with confirmation)

## Real-time updates

The dashboard uses WebSocket connections for real-time updates. You'll see live status changes, log streaming, and task progress without needing to refresh the page.
