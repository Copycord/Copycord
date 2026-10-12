_⭐️ Love Copycord? Give us a star and join the conversation in our Discord community!_

# <img width="24px" src="logo/logo.png" alt="Copycord"></img> Copycord

![Version](https://img.shields.io/github/v/release/Copycord/Copycord?logo=git&label=version&color=blue)
![Total Downloads](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fghcr-badge.elias.eu.org%2Fapi%2FCopycord%2FCopycord%2Fcopycord&query=%24.downloadCount&style=flat&logo=docker&label=Total%20Downloads&color=blue)
![Tests](https://github.com/Copycord/Copycord/actions/workflows/ci.yml/badge.svg)

[![Discord](https://img.shields.io/discord/1406152440377638952?color=7289DA&label=Discord&style=for-the-badge&logo=discord)](https://discord.gg/ArFdqrJHBj)

Copycord is the ultimate Discord server mirroring tool. Effortlessly clone multiple servers at once including channels, roles, emojis, and history while keeping everything in perfect sync with real-time message forwarding and structure updates. With powerful filters, custom branding options, DM and export tools, and a sleek web dashboard, Copycord gives you complete control to replicate, manage, and customize servers your way.

<br>

<div align="center">
<a href="https://copycord.github.io/Copycord/">
<img src="https://img.shields.io/badge/📖_Read_the_Docs-copycord.github.io-5865F2?style=for-the-badge&logoColor=white" alt="Documentation" height="40">
</a>
</div>

<br>

> [!TIP]
> **✨ Copycord Features**
>
> <details>
> <summary><b>Multi-Server Cloning</b></summary>
> Instantly mirror categories, channels, and message history from target servers—with the option to include roles, emojis, and stickers, and much more, all fully controlled through the web UI.
> </details>
>
> <details>
> <summary><b>Forward Messages to External Services</b></summary>
> Create custom rules with flexible filters, then forward matching messages instantly in real time to Telegram, Pushover, and more—so you never miss an important message or notification again.
> </details>
>
> <details>
> <summary><b>Live Message Forwarding</b></summary>
> Every new message is forwarded in real time to your clone via webhooks, keeping both servers perfectly in sync including edits and deletes.
> </details>
>
> <details>
> <summary><b>Send Messages as Real Users</b></summary>
> Optionally deliver cloned messages through real user accounts instead of webhooks, so posts appear to come from genuine members—pin each source author to their own account and mirror their nickname and roles so it looks just like them.
> </details>
>
> <details>
> <summary><b>Dynamic Structure Sync</b></summary>
> Copycord constantly watches for changes in the source server (new channels, renames, role updates) and applies them to your clone automatically.
> </details>
>
> <details>
> <summary><b>Advanced Channel Filtering</b></summary>
> Choose exactly which channels to include or exclude for maximum control over your clone's layout.
> </details>
>
> <details>
> <summary><b>Custom Branding</b></summary>
> Rename channels, customize webhook names/icons, and make the clone feel like your own personalized server.
> </details>
>
> <details>
> <summary><b>Smart Message Filtering</b></summary>
> Automatically block or drop unwanted messages based on custom keyword rules.
> </details>
>
> <details>
> <summary><b>Member List Scraper</b></summary>
> Use the member scraper to grab User IDs, Usernames, Avatars, and Bios from any server.
> </details>
>
> <details>
> <summary><b>Deep History Import</b></summary>
> Clone an entire channel's message history, not just the new ones.
> </details>
>
> <details>
> <summary><b>Universal Message Export</b></summary>
> Export all messages from any server into a JSON file with optional filtering, Webhook forwarding, and attachment downloading.
> </details>
>
> <details>
> <summary><b>DM History Export</b></summary>
> Export all DM messages from any user's inbox into a JSON file with optional Webhook forwarding.
> </details>
>
> <details>
> <summary><b>Sleek Web Dashboard</b></summary>
> Manage everything through a modern, easy-to-use web app.
> </details>

## Quick Start

> [!TIP]
> For full setup instructions, configuration options, slash commands, and more, visit the **[Copycord Documentation](https://copycord.github.io/Copycord/)**.

### Prerequisites

- Linux, macOS or Windows (with the install script), or [Docker](https://www.docker.com/products/docker-desktop)
- A Discord Bot Token + a Discord User Token ([how to get them](https://copycord.github.io/Copycord/docs/getting-started/discord-setup))

### Install script

The install script installs Copycord from the latest GitHub release. It installs [uv](https://docs.astral.sh/uv/) if needed, which provides Python; there is nothing else to set up.

Linux and macOS:

```bash
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

Then:

```bash
copycord doctor                   # check the setup
copycord run                      # start Copycord, then open http://localhost:8080
copycord service install --user   # or run it in the background as a service
```

The dashboard password is `copycord`. Change it in `~/.copycord/.env` (`%USERPROFILE%\.copycord\.env` on Windows) or with `copycord config set PASSWORD 'new-pass'`, then run `copycord restart`. Update later with `copycord update`.

Installer options (after `sh -s --`): `--version` picks a release (`4.0.0`) or builds a branch, tag or commit from source, `--repo owner/name` picks a fork, `--beta`, `--home` (default `~/.copycord`, or `COPYCORD_HOME`), `--verify` and `--dry-run`. Finish the setup in the dashboard at http://localhost:8080. Test a branch (a release-looking version installs that release, anything else is a git ref):

```bash
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/v4-dev/install.sh | sh -s -- --version v4-dev
```

See [Install Script](https://copycord.github.io/Copycord/docs/getting-started/manual-install) and the [CLI reference](https://copycord.github.io/Copycord/docs/cli).

### Docker

1. Create a folder with this `docker-compose.yml`:

```yaml
services:
  copycord:
    image: ghcr.io/copycord/copycord:latest # pinning a release tag is recommended
    container_name: copycord
    environment:
      - PASSWORD=copycord # change or comment out to disable login
    ports:
      - '8080:8080'
    volumes:
      - ./data:/data
    restart: unless-stopped
```

2. Run `docker compose up -d` and open **http://localhost:8080**

Copycord runs as a single container with the dashboard, server and client together. See [Docker Install](https://copycord.github.io/Copycord/docs/getting-started/docker-install).

### Upgrading from v3

v4 replaces the three `admin`/`server`/`client` containers with one and removes the `ROLE` variable (v4 refuses to start while it is set). The old launcher in `install-tools/` is replaced by the install script. Your database is kept and backed up automatically on the first v4 start. Follow **[Upgrading from v3](https://copycord.github.io/Copycord/docs/getting-started/upgrading-from-v3)**.

> [!WARNING]
> Copycord uses self-bot functionality, which is against Discord's Terms of Service and could lead to account suspension or termination. While uncommon, we strongly recommend using an alternate account to minimize risk.

## Documentation

Visit the **[Copycord Docs](https://copycord.github.io/Copycord/)** for:

- [Getting Started](https://copycord.github.io/Copycord/docs/getting-started/prerequisites): Prerequisites, Discord setup, installation
- [Upgrading from v3](https://copycord.github.io/Copycord/docs/getting-started/upgrading-from-v3): Moving from three containers or the v3 launcher to v4
- [CLI Reference](https://copycord.github.io/Copycord/docs/cli): `copycord run`, `doctor`, `config`, `service` and `update`
- [Web Dashboard](https://copycord.github.io/Copycord/docs/dashboard/overview): Managing mappings, filters, backfills, forwarding
- [Slash Commands](https://copycord.github.io/Copycord/docs/commands/overview): Full command reference
- [Configuration](https://copycord.github.io/Copycord/docs/configuration/cloning-options): All cloning options explained
- [Features](https://copycord.github.io/Copycord/docs/features/message-cloning): How cloning, syncing, and forwarding work

## Contributing & Support

Feel free to [open an issue](https://github.com/Copycord/Copycord/issues) if you hit any road bumps or want to request new features.

We appreciate all contributions — see the [Contributing Guide](https://copycord.github.io/Copycord/docs/contributing) for development setup instructions.

## Buy me a coffee

If you are enjoying Copycord, consider buying me a coffee!

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/A0A41KPDX4)
