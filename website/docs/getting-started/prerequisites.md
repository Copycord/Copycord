---
sidebar_position: 1
title: Prerequisites
---

# Prerequisites

Before installing Copycord, you'll need a few things ready.

## Requirements

### For Docker install (recommended)

- [Docker](https://www.docker.com/products/docker-desktop) and Docker Compose
- A Discord **bot token** (for the server bot)
- A Discord **user token** (for the client self-bot)

### For manual install

- **Python 3.11** or newer
- **Node.js** (LTS) + **npm**
- A Discord **bot token**
- A Discord **user token**

:::tip Which install method should I use?
**Docker** is recommended for most users — it handles all dependencies automatically and is easy to update. Choose the **manual install** if you prefer not to use Docker or want more control over the setup.
:::

## What you'll need from Discord

You need **two tokens** to run Copycord:

| Token | What it is | Where it's used |
|-------|-----------|----------------|
| **Bot Token** | A token from the Discord Developer Portal for a bot application | The **server** component — manages the clone server |
| **User Token** | Your personal Discord account token (or an alt account) | The **client** component — reads messages from the source server |

The next page walks you through obtaining both tokens and setting up your Discord bot.

## System requirements

Copycord is lightweight and runs comfortably on:

- **1 CPU core**
- **512 MB RAM** (1 GB recommended for large servers)
- **1 GB disk space** (more if backfilling history or storing backups)
- A stable internet connection

Copycord runs on **Windows**, **Linux**, and **macOS**.
