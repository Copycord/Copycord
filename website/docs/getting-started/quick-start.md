---
sidebar_position: 0
title: Quick Start
description: Install Copycord with one command, open the dashboard and clone your first server.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Quick Start

Install Copycord, open its web dashboard and clone your first Discord server. It takes about ten minutes.

## Before you begin

- A computer running Linux, macOS or Windows.
- A clone server, a bot token and a user token. To get them, follow [Discord Setup](/docs/getting-started/discord-setup).

:::warning
Use an alternate Discord account for the user token. Logging in to Discord with a user token is against Discord's Terms of Service and can get the account suspended.
:::

## Install Copycord

Open a terminal as your normal user and run the installer for your system:

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell (not as Administrator)"
irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

</TabItem>
</Tabs>

The installer ends with a box that shows your dashboard password. Copy it, then open a new terminal so it finds the `copycord` command.

## Start Copycord

1. Start Copycord:

   ```bash title="Terminal"
   copycord run
   ```

   Leave the terminal open. To stop Copycord, press **Ctrl+C**.

2. Open `http://localhost:8080` in your browser and log in with the password from the installer.

   If you lost the password, run `copycord config get PASSWORD --show-secrets` to print it.

## Connect your bot and account

1. On the **Configuration** page, in the **Global Configuration** card, paste the bot token into **Server Token** and the user token into **Client Token**.
2. Click **Save**.
3. If the bot is not in your clone server yet, click **Add Bot to Server** and choose the clone server.

## Clone a server

1. In the **Cloning Configuration** card, click **+**.
2. In **Host Guild**, enter the ID of the server you want to clone. Your user account must be a member of it.
3. In **Clone Guild**, enter the ID of your clone server.
4. Click **Save Mapping**.
5. In the **Bots** card, click **Start**.

   When both bots show the state "running", Copycord builds the channels, categories and roles in your clone server and copies new messages as they are sent.

## Next steps

- [First Run](/docs/getting-started/first-run): command users, auto start and what the dashboard shows.
- [Backfill](/docs/dashboard/backfill): copy the messages sent before you started.
- [Filters](/docs/dashboard/filters): choose which channels to clone.
- [`copycord service`](/docs/cli#copycord-service): keep Copycord running in the background.
- [Docker Install](/docs/getting-started/docker-install): run Copycord in a container instead.

If something does not work, run `copycord doctor`. It checks your install and settings and says what to fix. See [`copycord doctor`](/docs/cli#copycord-doctor), or ask on [Discord](https://discord.gg/ArFdqrJHBj).
