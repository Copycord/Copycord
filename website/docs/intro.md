---
sidebar_position: 1
slug: /intro
title: What is Copycord?
---

# What is Copycord?

**Copycord** is the ultimate open-source Discord server mirroring tool. It lets you clone entire Discord servers — channels, roles, emojis, stickers, and full message history — while keeping everything in perfect sync with real-time message forwarding and automatic structure updates.

## How it works

Copycord runs three lightweight services that work together:

| Service | Role | What it does |
|---------|------|-------------|
| **Client** | Source watcher | A self-bot that monitors your source server for new messages, edits, deletes, and structure changes |
| **Server** | Clone manager | A Discord bot that manages the cloned server — creates channels, posts messages via webhooks, syncs roles |
| **Admin** | Web dashboard | A FastAPI web app where you configure mappings, manage filters, run backfills, and monitor everything |

```
Source Server ──► Client (self-bot) ──► Server (bot) ──► Clone Server
                                           ▲
                                           │
                                     Admin Dashboard
                                    (localhost:8080)
```

## Key features

- **Multi-server cloning** — Mirror multiple servers simultaneously
- **Real-time message sync** — Messages, edits, and deletes forwarded instantly via webhooks
- **Dynamic structure sync** — New channels, roles, renames, and deletions auto-synced
- **Deep history import** — Backfill entire channel histories, not just new messages
- **Message forwarding** — Forward messages to Telegram, Pushover, or any webhook
- **Advanced filtering** — Whitelist/exclude channels, block keywords, rewrite text
- **Custom branding** — Rename channels, customize webhook names and avatars
- **Member scraper** — Export user IDs, usernames, avatars, and bios
- **DM export** — Export direct message history to JSON
- **Announcement system** — Keyword-triggered notifications with user subscriptions
- **Database backups** — Automatic daily backups with restore capability
- **Web dashboard** — Modern UI to manage everything from your browser

## Quick start

The fastest way to get running:

1. [Set up your Discord tokens](/docs/getting-started/prerequisites)
2. [Install with Docker](/docs/getting-started/docker-install) (recommended) or [manually](/docs/getting-started/manual-install)
3. [Configure your first clone](/docs/getting-started/first-run)

:::warning Self-Bot Notice
Copycord uses self-bot functionality (logging into Discord with a user token), which is against Discord's Terms of Service and could lead to account suspension. We strongly recommend using an **alternate account** to minimize risk.
:::

## Support

- **Discord**: [Join our community](https://discord.gg/ArFdqrJHBj)
- **Issues**: [Report bugs or request features](https://github.com/Copycord/Copycord/issues)
- **Support the project**: [Buy us a coffee](https://ko-fi.com/A0A41KPDX4)
