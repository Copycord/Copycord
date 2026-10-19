---
sidebar_position: 1
slug: /intro
title: What is Copycord?
---

# What is Copycord?

Copycord is an open-source tool that clones Discord servers. It copies channels, roles, emojis, stickers and message history into a server you own, then keeps the clone in sync as the source server changes.

## How it works

Copycord runs as one app, started with `copycord run` or one Docker container. It has three parts:

| Part | What it does |
|------|--------------|
| **Client** | Logs in with a Discord user account and watches the source server for new messages, edits, deletions and changes to channels and roles. |
| **Server** | A Discord bot in your clone server. It creates channels, posts messages through webhooks and syncs roles. |
| **Dashboard** | A web page at `http://localhost:8080` where you set up clones and filters, run tasks such as backfills and exports, and watch the logs. |

```
Source server ──► Client (user account) ──► Server (bot) ──► Clone server
                                               ▲
                                               │
                                           Dashboard
```

## Features

- **Several servers at once**: clone as many source servers as you need.
- **Real-time messages**: new messages, edits and deletions appear in the clone within seconds.
- **Structure sync**: new, renamed and deleted channels and roles are applied to the clone.
- **Tasks**: copy channel history, purge messages or assets, export messages and DMs, download avatars and list server members, with progress and a history of every run.
- **Forwarding**: send messages to Telegram, Pushover or a Discord webhook.
- **Filters**: include or exclude channels, block keywords and rewrite text.
- **Custom names**: rename channels and set webhook names and avatars.
- **Sending as users**: post cloned messages from user accounts instead of webhooks.
- **Proxies**: route the client through HTTP or SOCKS5 proxies, with rotation and failover.
- **Announcements**: keyword alerts that users can subscribe to.
- **Backups**: a daily database backup, plus `copycord backup` and `copycord restore`.
- **Command line**: install, update, run in the background and check your setup with the [`copycord` command](/docs/cli).

## Get started

To install Copycord and clone your first server, follow the [Quick Start](/docs/getting-started/quick-start). To run it in a container, see [Docker Install](/docs/getting-started/docker-install). If you run Copycord v3, see [Upgrading from v3](/docs/getting-started/upgrading-from-v3).

:::warning Use an alternate account
The client logs in to Discord with a user token. This is against Discord's Terms of Service and can get the account suspended. Use an alternate account, not your main one.
:::

## Support

- **Discord**: [join the community server](https://discord.gg/ArFdqrJHBj)
- **Issues**: [report a bug or request a feature](https://github.com/Copycord/Copycord/issues)
- **Donations**: [support the project on Ko-fi](https://ko-fi.com/A0A41KPDX4)
