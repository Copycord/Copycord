---
sidebar_position: 0
title: Quick Start
description: Install Copycord with one command, open the dashboard and clone your first server.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Quick Start

Install Copycord with one command, start it, and finish the setup in the web dashboard. It takes about five minutes. By the end you have Copycord running on your machine, logged in to the dashboard at `http://localhost:8080`, with both Discord tokens saved.

## What you need

- **A computer running Linux, macOS or Windows.**
- **A bot token and a user token from Discord.** [Discord Setup](/docs/getting-started/discord-setup) shows how to get both and how to create the clone server.

:::tip
Use an **alternate Discord account** for the user token. Self-bots are against Discord's Terms of Service.
:::

## Try it in one command

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
<TabItem value="docker" label="Docker (advanced)">

The install script above is the recommended way. Docker is for advanced users who already run their apps in containers. See [Docker Install](/docs/getting-started/docker-install).

</TabItem>
</Tabs>

Then run `copycord run`, open **http://localhost:8080** and log in with the password the installer shows at the end. The steps below explain each part.

## Steps

### 1. Install Copycord

Run the command for your system from above. The installer runs numbered steps:

1. **Preparing uv:** installs [uv](https://docs.astral.sh/uv/) if it is missing. uv provides Python 3.14.
2. **Finding the release:** the newest stable [GitHub release](https://github.com/Copycord/Copycord/releases) (with `--beta`, pre-releases too).
3. **Installing Copycord:** downloads the release wheel, checks it against the checksum GitHub lists for it, and installs it as a per-user uv tool.
4. **Finishing up:** puts `copycord` on your `PATH` and writes the [settings file](/docs/getting-started/configuration) `.env` in the Copycord home.

The Copycord home holds everything Copycord stores: `.env`, the `data` folder (`data.db`, `secret.key` and the daily backups) and the `logs` folder.

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

| Copycord home | Settings file |
|---------------|---------------|
| `~/.copycord` | `~/.copycord/.env` |

</TabItem>
<TabItem value="windows" label="Windows">

| Copycord home | Settings file |
|---------------|---------------|
| `%USERPROFILE%\.copycord` | `%USERPROFILE%\.copycord\.env` |

</TabItem>
<TabItem value="docker" label="Docker">

The `./data` folder next to `docker-compose.yml`. Settings go in the compose file instead of `.env`.

</TabItem>
</Tabs>

:::tip Command not found?
Open a new terminal so it picks up the updated `PATH`, then try again.
:::

### 2. Start Copycord

```bash title="Terminal"
copycord run
```

This starts the dashboard, the server bot and the client. Leave the terminal open; **Ctrl+C** stops it.

### 3. Log in to the dashboard

Open **http://localhost:8080** and log in with the password from the installer's final box. The installer makes up a random one on a first install and shows it only once. It is saved as `PASSWORD` in `.env`; to see it again:

```bash title="Terminal"
copycord config get PASSWORD --show-secrets
```

:::tip Pick your own password
To use a password you can remember:

```bash title="Terminal"
copycord config set PASSWORD 'your-new-password'
copycord restart
```

`copycord restart` applies it when Copycord runs as a service. In a terminal, press **Ctrl+C** and run `copycord run` again. See [Configuration file (.env)](/docs/getting-started/configuration).
:::

### 4. Enter your tokens

On the dashboard, paste the **bot token** into **Server Token** (`SERVER_TOKEN`) and the **user token** into **Client Token** (`CLIENT_TOKEN`), then save. Everything else is set up in the dashboard too: there is no setup command. [First Run](/docs/getting-started/first-run) walks through the rest: command users, inviting the bot, starting the bots and creating your first guild mapping.

### 5. Keep it running in the background (optional)

Install Copycord as a service so it runs without an open terminal and starts again on its own. Stop the `copycord run` in your terminal first.

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

```bash title="Terminal"
# Linux: a systemd service for your account, started at boot (no sudo)
copycord service install

# macOS: a launchd agent
copycord service install
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell title="PowerShell"
# a Scheduled Task that starts when you log in
copycord service install
```

</TabItem>
<TabItem value="docker" label="Docker">

`restart: unless-stopped` in the compose file already keeps the container running. `copycord service` is not used with Docker.

</TabItem>
</Tabs>

Check it with `copycord service status`. See [`copycord service`](/docs/cli#copycord-service) for the system service and the details per OS.

## What happens next

- **Updates.** When a new release is out, `copycord run` prints a one-line hint at start (it asks GitHub at most once a day). Run `copycord update` to install it; a running service is restarted for you. `copycord update --channel beta` (or `stable`, `dev`) switches channels, and `copycord config set AUTO_UPDATE true` lets the service update itself on the stable channel, going back to the previous version if the new one does not start. See [`copycord update`](/docs/cli#copycord-update).
- **Settings.** Edit `.env` or run `copycord config set KEY VALUE`, then `copycord restart`. See [Configuration file (.env)](/docs/getting-started/configuration).
- **Your first clone.** Continue with [First Run](/docs/getting-started/first-run), then [backfill history](/docs/dashboard/backfill) and [filters](/docs/dashboard/filters).

## If something does not work

```bash title="Terminal"
copycord doctor
```

`copycord doctor` checks the install (Python, the dashboard files, the packages), your settings, the Copycord home, database and backups, the dashboard port, the service and whether an update is out. Each line starts with `✓` (fine), `!` (warning), `✗` (problem) or `•` (info). `copycord doctor --fix` repairs what it can and asks only before moving the dashboard to a free port; it never changes your password. See [`copycord doctor`](/docs/cli#copycord-doctor).

- `copycord` not found: open a new terminal.
- Port 8080 is taken: set `ADMIN_PORT` in [`.env`](/docs/getting-started/configuration) and restart.
- Installer flags, testing a branch, a custom home and uninstalling: [Install Options](/docs/getting-started/manual-install).
- Coming from v3: [Upgrading from v3](/docs/getting-started/upgrading-from-v3).
- Still stuck: ask on [Discord](https://discord.gg/ArFdqrJHBj) or open an [issue](https://github.com/Copycord/Copycord/issues).
