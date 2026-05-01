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
> For full setup instructions, configuration options, slash commands, and more — visit the **[Copycord Documentation](https://copycord.github.io/Copycord/)**.

### Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop) (recommended) or the manual installer
- A Discord Bot Token + a Discord User Token

### Docker Install

1. Create a folder with this `docker-compose.yml`:

```yaml
services:
  admin:
    image: ghcr.io/copycord/copycord:v3.20.0
    container_name: copycord-admin
    environment:
      - ROLE=admin
      - PASSWORD=copycord # change or comment out to disable login
    ports:
      - '8080:8080'
    volumes:
      - ./data:/data
    restart: unless-stopped

  server:
    image: ghcr.io/copycord/copycord:v3.20.0
    container_name: copycord-server
    environment:
      - ROLE=server
    volumes:
      - ./data:/data
    depends_on:
      - admin
    restart: unless-stopped

  client:
    image: ghcr.io/copycord/copycord:v3.20.0
    container_name: copycord-client
    environment:
      - ROLE=client
    volumes:
      - ./data:/data
    depends_on:
      - admin
    restart: unless-stopped
```

2. Run `docker compose up -d` and open **http://localhost:8080**

> [!WARNING]
> Copycord uses self-bot functionality, which is against Discord's Terms of Service and could lead to account suspension or termination. While uncommon, we strongly recommend using an alternate account to minimize risk.

## Documentation

Visit the **[Copycord Docs](https://copycord.github.io/Copycord/)** for:

- [Getting Started](https://copycord.github.io/Copycord/docs/getting-started/prerequisites) — Prerequisites, Discord setup, installation
- [Web Dashboard](https://copycord.github.io/Copycord/docs/dashboard/overview) — Managing mappings, filters, backfills, forwarding
- [Slash Commands](https://copycord.github.io/Copycord/docs/commands/overview) — Full command reference
- [Configuration](https://copycord.github.io/Copycord/docs/configuration/cloning-options) — All cloning options explained
- [Features](https://copycord.github.io/Copycord/docs/features/message-cloning) — How cloning, syncing, and forwarding work

## Contributing & Support

Feel free to [open an issue](https://github.com/Copycord/Copycord/issues) if you hit any road bumps or want to request new features.

We appreciate all contributions — see the [Contributing Guide](https://copycord.github.io/Copycord/docs/contributing) for development setup instructions.

## Buy me a coffee

If you are enjoying Copycord, consider buying me a coffee!

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/A0A41KPDX4)
