---
sidebar_position: 1
slug: /intro
title: What is Copycord?
---

# What is Copycord?

**Copycord** is an open-source Discord server mirroring tool. It clones entire Discord servers (channels, roles, emojis, stickers and message history) and keeps the clone in sync with real-time message cloning and automatic structure updates.

## How it works

Copycord has three parts. They run together in one app, started with `copycord run` or one Docker container:

| Part | Role | What it does |
|---------|------|-------------|
| **Client** | Source watcher | A self-bot that monitors your source server for new messages, edits, deletes, and structure changes |
| **Server** | Clone manager | A Discord bot that manages the cloned server: it creates channels, posts messages via webhooks, syncs roles |
| **Admin** | Web dashboard | A web app where you configure mappings and filters, run tasks such as backfills and exports, and monitor everything |

```
Source Server ──► Client (self-bot) ──► Server (bot) ──► Clone Server
                                           ▲
                                           │
                                     Admin Dashboard
                                    (localhost:8080)
```

## Key features

- **Multi-server cloning**: Mirror multiple servers simultaneously
- **Real-time message sync**: Messages, edits, and deletes forwarded instantly via webhooks
- **Dynamic structure sync**: New channels, roles, renames, and deletions auto-synced
- **Tasks**: Backfill channel history, purge messages or assets, sync assets, export messages and DMs, download avatars and scrape members from the Tasks page, with progress and a history of every run
- **Message forwarding**: Forward messages to Telegram, Pushover or a Discord webhook
- **Advanced filtering**: Whitelist/exclude channels, block keywords, rewrite text
- **Custom branding**: Rename channels, customize webhook names and avatars
- **Sending as users**: Optionally post cloned messages from user accounts instead of webhooks
- **Client proxies**: Route the client through HTTP or SOCKS5 proxies with rotation and failover
- **Announcement system**: Keyword-triggered notifications with user subscriptions
- **Database backups**: Automatic daily backups, plus `copycord backup` and `copycord restore`
- **Command line**: Install, update, run as a background service and check your setup with the [`copycord` command](/docs/cli)

## Quick start

Install with one command, then finish the setup in the dashboard:

```bash title="Linux / macOS"
curl -fsSL https://raw.githubusercontent.com/Copycord/Copycord/main/install.sh | sh
```

```powershell title="Windows PowerShell"
irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
```

The [Quick Start](/docs/getting-started/quick-start) walks through it step by step, including [Docker](/docs/getting-started/docker-install). Coming from v3? See [Upgrading from v3](/docs/getting-started/upgrading-from-v3).

:::warning Self-Bot Notice
Copycord uses self-bot functionality (logging into Discord with a user token), which is against Discord's Terms of Service and could lead to account suspension. We strongly recommend using an **alternate account** to minimize risk.
:::

## Support

- **Discord**: [Join our community](https://discord.gg/ArFdqrJHBj)
- **Issues**: [Report bugs or request features](https://github.com/Copycord/Copycord/issues)
- **Support the project**: [Buy us a coffee](https://ko-fi.com/A0A41KPDX4)
