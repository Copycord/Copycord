---
sidebar_position: 1
title: Prerequisites
---

# Prerequisites

Before installing Copycord, you'll need a few things ready.

## Requirements

### For the install script

- **Linux**, **macOS** or **Windows**
- `curl` or `wget` on Linux/macOS, **PowerShell** on Windows
- A Discord **bot token** (for the server bot)
- A Discord **user token** (for the client)

You do not need to install Python, Node.js or virtual environments. The install script installs [uv](https://docs.astral.sh/uv/), and uv provides Python 3.12 or newer and every dependency Copycord needs.

### For Docker

- [Docker](https://www.docker.com/products/docker-desktop) and Docker Compose
- A Discord **bot token**
- A Discord **user token**

:::tip Which install method should I use?
Use the **install script** if you want Copycord running directly on your computer or server; it can also install Copycord as a background service. Use **Docker** if you already run your apps in containers. Both run the same Copycord and keep all data in one data directory.
:::

## What you'll need from Discord

You need **two tokens** to run Copycord:

| Token | What it is | Where it's used |
|-------|-----------|----------------|
| **Bot Token** | A token from the Discord Developer Portal for a bot application | The **server** part: manages the clone server |
| **User Token** | Your personal Discord account token (or an alt account) | The **client** part: reads messages from the source server |

The next page walks you through obtaining both tokens and setting up your Discord bot.

## System requirements

Copycord is lightweight and runs comfortably on:

- **1 CPU core**
- **512 MB RAM** (1 GB recommended for large servers)
- **1 GB disk space** (more if backfilling history or storing backups)
- A stable internet connection

Copycord runs on **Windows**, **Linux**, and **macOS**. Natively it supports **Python 3.12 and newer**; the Docker image uses Python 3.14.
