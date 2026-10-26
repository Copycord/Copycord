---
sidebar_position: 1
title: Dashboard overview
---

# Web dashboard overview

The web dashboard is where you set up and run Copycord: start the bots, map servers, run tasks and read the logs. Open it at `http://localhost:8080`, or at the port you configured.

## Pages

| Page | What you do there |
|------|-------------------|
| **Configuration** | Start and stop the bots, set tokens and core settings, manage [guild mappings](/docs/dashboard/guild-mappings), notifications, sync settings and proxies |
| [**Channels**](/docs/dashboard/channels) | View cloned channels, rename them in the clone, delete orphans |
| [**Messages**](/docs/dashboard/messages) | Search every cloned message |
| [**Guilds**](/docs/dashboard/guilds) | Browse the servers your user account is in, start a message export |
| [**Tasks**](/docs/dashboard/tasks) | Run and follow long jobs: backfills, exports, purges, asset syncs, avatar downloads and member scrapes |
| [**Forwarding**](/docs/dashboard/forwarding) | Forward messages to Discord webhooks, Telegram and Pushover |
| [**System**](/docs/dashboard/system) | Database backups |
| [**Logs**](/docs/dashboard/logs) | Event logs of what Copycord changed in your clone servers |

The dashboard updates live: bot status, logs and task progress change without a page refresh.

## Sign in and out

The dashboard always asks for a password. You stay signed in until you click **Log out** in the sidebar.

To change the password, do one of the following:

- Docker: edit the `PASSWORD` environment variable in `docker-compose.yml`, then run `docker compose up -d`.
- Install script: run `copycord config set PASSWORD 'your-new-password'`, then run `copycord restart`.

To control who can reach the dashboard, see [Security defaults](/docs/configuration/security).

## The Configuration page

The **Configuration** page is the dashboard home. Its cards, top to bottom:

- **Bots**: the status and uptime of the server and client bots, the **Start** or **Stop** button and the log viewers.
- **Global Configuration**: tokens and core settings.
- **Cloning Configuration**: your [guild mappings](/docs/dashboard/guild-mappings).
- **Notifications**, **Sync Settings** and **Proxy**: see [Advanced configuration](/docs/configuration/advanced).

When a newer release exists, **New Update Available** appears at the top of the page and links to the release page.

## Start and stop the bots

Cloning works only while both the server bot and the client are running. The bots need valid tokens to start, and you can add or edit guild mappings only while they are stopped.

- To start the bots, click **Start** on the **Bots** card. While they run, the button reads **Stop**, and the card shows each bot's status and uptime. After you click **Stop**, the bots show an amber **stopping** status and the button reads **Stopping...** and cannot be clicked until both bots have really stopped. With many user tokens online, closing their sessions can take a little while.

### Bot settings

The gear button at the top right of the **Bots** card opens **Bot settings**, where you pick how the server bot and the client bot each appear on Discord: **Online**, **Idle**, **Busy** or **Invisible**. These are global settings, not per mapping. A bot that is running switches right away when you click **Save**; a stopped bot uses the new status at its next start. The values are stored in two keys, both `online` by default:

| Key | Values |
| --- | --- |
| `SERVER_STATUS` | `online`, `idle`, `dnd` (Busy), `invisible` |
| `CLIENT_STATUS` | `online`, `idle`, `dnd` (Busy), `invisible` |

## Add the bot to your server

After your bot token is validated, an **Add Bot to Server** link appears in **Global Configuration**.

1. Click **Add Bot to Server**. Discord's authorization page opens with the permissions Copycord needs already selected.
2. Pick your clone server and authorize the bot.

## Global configuration

To change a core setting, edit it in the **Global Configuration** card, then click **Save**.

| Setting | What it does |
|---------|--------------|
| **SERVER_TOKEN** | Your Discord bot token |
| **CLIENT_TOKEN** | Your Discord user account token. **Manage backup tokens** holds fallback client tokens |
| **Command Users** (`COMMAND_USERS`) | User IDs allowed to run slash commands |
| **Log Level** (`LOG_LEVEL`) | `INFO` or `DEBUG` |
| **Auto Start** (`COPYCORD_AUTOSTART`) | Start the bots when Copycord launches. Needs valid tokens |
| **Max Log Size** (`LOG_MAX_SIZE_MB`) | Largest size of a log file before old entries are pruned. Default: 10 MB. `0` turns pruning off |

## Log viewer

To read the full bot logs, click **View server logs** or **View client logs** on the **Bots** card. The terminal shows only their warnings and errors; see [Where logs go](/docs/dashboard/system#where-logs-go).

In the log viewer:

- New lines stream in as they are written.
- **Search logs** filters the lines.
- **Clear logs** empties the selected log file, after a confirmation.
