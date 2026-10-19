---
sidebar_position: 2
title: First Run
description: Log in to the dashboard, save your tokens and create your first guild mapping.
---

# First Run

Set up Copycord in the web dashboard after you install it: save your tokens, create your first clone and start the bots. All setup happens in the dashboard.

## Before you begin

- Copycord is installed and running. See the [Quick Start](/docs/getting-started/quick-start), or [Docker Install](/docs/getting-started/docker-install) for Docker.
- You have a clone server, a bot token and a user token. See [Discord Setup](/docs/getting-started/discord-setup).

## Log in

Open `http://localhost:8080` and log in with the password the installer showed at the end. If you changed `ADMIN_PORT`, use that port.

To print the password again, run `copycord config get PASSWORD --show-secrets`. To choose your own, see [Configuration File (.env)](/docs/getting-started/configuration#change-a-setting).

## Save your tokens and settings

The **Configuration** page opens after you log in. In its **Global Configuration** card:

1. In **Server Token**, paste the bot token.
2. In **Client Token**, paste the user token.
3. Optional: in **Command Users**, add your Discord user ID so you can use the [slash commands](/docs/commands/overview) in your clone server.
4. Click **Save**.

To find your Discord user ID, turn on **Developer Mode** in Discord's **Settings > Advanced**, right-click your username and click **Copy User ID**.

The card also has **Auto Start** (start the bots whenever Copycord starts), **Log Level** and **Max Log Size**. The button next to **Client Token** manages backup client tokens.

## Add the bot to your clone server

If you did not invite the bot during [Discord Setup](/docs/getting-started/discord-setup#invite-the-bot-to-your-clone-server), click **Add Bot to Server** at the bottom of the Global Configuration card and choose your clone server. The button turns on once the server token is saved and checked.

## Create a guild mapping

A guild mapping links a source server to your clone server. Mappings are locked while the bots run, so create it before you start them.

1. In the **Cloning Configuration** card, click **+** (Add new mapping).
2. Optional: enter a **Mapping Name**.
3. In **Host Guild**, enter the ID of the server you want to clone. Your user account must be a member of it.
4. In **Clone Guild**, enter the ID of your clone server.
5. Click **Save Mapping**.

The same window holds the [cloning options](/docs/configuration/cloning-options) for this mapping. You can add as many mappings as you need.

## Start the bots

In the **Bots** card, click **Start**. The button stays off until both tokens are saved and valid. When each bot is connected, its **State** shows "running".

Copycord then reads the source server, creates the matching channels, categories and roles in your clone server, and copies new messages as they are sent. To change the mappings or the Global Configuration card later, stop the bots first.

To follow what happens, open the **Logs** page, or click **View server logs** or **View client logs** in the Bots card. The terminal, or `docker compose logs`, shows only warnings and errors from the bots. See [Where logs go](/docs/dashboard/system#where-logs-go).

## Next steps

- [Backfill](/docs/dashboard/backfill): copy the messages sent before you set up Copycord.
- [Filters](/docs/dashboard/filters): choose which channels to include or exclude.
- [Cloning options](/docs/configuration/cloning-options): choose what gets synced.
- [Forwarding](/docs/dashboard/forwarding): send messages to Telegram or other services.
- [Slash commands](/docs/commands/overview): manage your clone from Discord.
