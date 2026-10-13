_⭐️ Love Copycord? Give us a star and join the conversation in our Discord community!_

# <img width="24px" src="assets/logo.png" alt="Copycord"></img> Copycord

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
> Instantly mirror categories, channels, and message history from target servers, with the option to include roles, emojis, and stickers, and much more, all fully controlled through the web UI.
> </details>
>
> <details>
> <summary><b>Forward Messages to External Services</b></summary>
> Create custom rules with flexible filters, then forward matching messages instantly in real time to Telegram, Pushover, and more, so you never miss an important message or notification again.
> </details>
>
> <details>
> <summary><b>Live Message Forwarding</b></summary>
> Every new message is forwarded in real time to your clone via webhooks, keeping both servers perfectly in sync including edits and deletes.
> </details>
>
> <details>
> <summary><b>Send Messages as Real Users</b></summary>
> Optionally deliver cloned messages through real user accounts instead of webhooks, so posts appear to come from genuine members. Pin each source author to their own account and mirror their nickname and roles so it looks just like them.
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

### What you need

- A computer running Linux, macOS or Windows
- A Discord bot token and a Discord user token ([how to get them](https://copycord.github.io/Copycord/docs/getting-started/discord-setup))

### Install

Linux and macOS:

```bash
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

The installer sets up everything Copycord needs. Run again, it stops when that version is already installed; `--force` (`-Force` on Windows) reinstalls.

### Start

```bash
copycord run
```

Open **http://localhost:8080**, log in with the password the installer showed at the end, and add your tokens in the dashboard. That's it.

To keep Copycord running in the background, run `copycord service install`.

> [!IMPORTANT]
> The installer makes up a random dashboard password and shows it once. It is saved as `PASSWORD` in `~/.copycord/.env` (`%USERPROFILE%\.copycord\.env` on Windows); `copycord config get PASSWORD --show-secrets` prints it again. To pick your own, run `copycord config set PASSWORD 'your-new-password'`, then `copycord restart`.
>
> The dashboard is only reachable from this computer. To open it from other devices, set your own `PASSWORD` and `DASHBOARD_HOST=0.0.0.0` in the same file. Copycord will not start with that address while the password is empty or still `copycord`.

The [Quick Start guide](https://copycord.github.io/Copycord/docs/getting-started/quick-start) walks through every step.

### Docker (advanced)

If you already run your apps in containers, see [Docker Install](https://copycord.github.io/Copycord/docs/getting-started/docker-install).

> [!WARNING]
> Copycord uses self-bot functionality, which is against Discord's Terms of Service and could lead to account suspension or termination. We strongly recommend using an alternate account.

## Documentation

The **[Copycord Docs](https://copycord.github.io/Copycord/)** cover:

- [Getting Started](https://copycord.github.io/Copycord/docs/getting-started/quick-start): installing and first run
- [Web Dashboard](https://copycord.github.io/Copycord/docs/dashboard/overview): mappings, filters, history import, forwarding
- [Slash Commands](https://copycord.github.io/Copycord/docs/commands/overview): every command
- [Configuration](https://copycord.github.io/Copycord/docs/configuration/cloning-options): all cloning options
- [Command Line](https://copycord.github.io/Copycord/docs/cli): start, stop, update and settings
- [Upgrading from v3](https://copycord.github.io/Copycord/docs/getting-started/upgrading-from-v3)

## Contributing & Support

[Open an issue](https://github.com/Copycord/Copycord/issues) if you hit a problem or want to request a feature. Contributions are welcome: see the [Contributing Guide](https://copycord.github.io/Copycord/docs/contributing).

## Buy me a coffee

If you are enjoying Copycord, consider buying me a coffee!

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/A0A41KPDX4)
