---
sidebar_position: 2
title: First Run
description: Log in to the dashboard, save your tokens and create your first guild mapping.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# First Run

All of Copycord's setup happens in the web dashboard; there is no setup command. This page picks up after the [Quick Start](/docs/getting-started/quick-start) install and ends with your first server clone.

## 1. Start Copycord

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
copycord run
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell"
copycord run
```

</TabItem>
<TabItem value="docker" label="Docker">

```bash title="Terminal"
docker compose up -d
```

</TabItem>
</Tabs>

The dashboard, the server bot and the client start together. If you installed the [service](/docs/cli#copycord-service), Copycord is already running.

## 2. Log in

Open **http://localhost:8080** and log in with the password the installer showed at the end (`copycord config get PASSWORD --show-secrets` prints it again). If you changed `ADMIN_PORT`, use that port.

:::tip Pick your own password
To use a password you can remember: `copycord config set PASSWORD 'your-new-password'`, then `copycord restart`. See [Configuration File (.env)](/docs/getting-started/configuration).
:::

## 3. Enter your tokens

On the **Configuration** page (the first page after you log in), in the **Global Configuration** card:

1. **Server Token** (`SERVER_TOKEN`): paste the **bot token** from the Discord Developer Portal
2. **Client Token** (`CLIENT_TOKEN`): paste the **user token** from your browser

Click **Save**. [Discord Setup](/docs/getting-started/discord-setup) shows how to get both tokens. The button next to **Client Token** manages backup client tokens.

## 4. Set command users

In the same card, add your Discord user ID to **Command Users**. This lets you use Copycord's slash commands in your clone server. The card also has **Auto Start** (start the bots whenever Copycord starts), **Log Level** and **Max Log Size**.

:::tip How to find your Discord user ID
1. Open Discord Settings, then Advanced, and enable **Developer Mode**
2. Right-click your username anywhere in Discord
3. Click **Copy User ID**
:::

## 5. Add your bot to the clone server

Once your server token is saved and checked, the **Add Bot to Server** button at the bottom of the Global Configuration card turns on. Click it to invite your bot to the clone server with the right permissions.

## 6. Start the bots

In the **Bots** card, click **Start** to launch the server bot and the client. Start stays off until both tokens are saved and valid. The **State** of each turns to "running" when it is connected.

## 7. Create a guild mapping

A **guild mapping** links a source server to your clone server:

1. In the **Cloning Configuration** card, click **+** (Add new mapping)
2. Optionally give it a **Mapping Name**
3. Enter the **Host Guild**: the ID of the server you want to clone (your user account must be in it)
4. Enter the **Clone Guild**: the ID of the empty server where your bot is
5. Click **Save Mapping**

The same window holds the [cloning options](/docs/configuration/cloning-options) for this mapping. While the bots run, the mappings and the Global Configuration card are locked; stop the bots to change them.

## 8. Watch it sync

With the mapping created and both bots running, Copycord:

1. **Scans** the source server structure
2. **Creates** matching channels, categories and roles in the clone
3. **Forwards** new messages in real time

You see the activity on the dashboard's **Logs** page, through **View server logs** and **View client logs** in the Bots card, and in your clone server. The terminal (or `docker compose logs`) only shows warnings and errors from the server and client; see [Logs](/docs/dashboard/system#where-logs-go).

## What's next?

- **[Backfill history](/docs/dashboard/backfill)**: import messages from before you set up Copycord, with the Backfill channels task on the **Tasks** page
- **[Configure filters](/docs/dashboard/filters)**: choose which channels to include or exclude
- **[Cloning options](/docs/configuration/cloning-options)**: fine-tune what gets synced
- **[Forwarding](/docs/dashboard/forwarding)**: send messages to Telegram or other services
- **[Slash commands](/docs/commands/overview)**: manage your clone from within Discord

:::tip Multiple servers
Create as many guild mappings as you need. Copycord clones several source servers at once.
:::
