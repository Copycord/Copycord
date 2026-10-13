---
sidebar_position: 7
title: Requirements
description: Supported systems, what the installer brings and the hardware Copycord needs.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Requirements

## Systems

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

- A recent Linux distribution or macOS
- `curl` or `wget`
- A normal user account: the installer refuses to run as root

</TabItem>
<TabItem value="windows" label="Windows">

- Windows 10 or 11
- PowerShell, opened as your normal user (not as Administrator)

</TabItem>
<TabItem value="docker" label="Docker">

- [Docker](https://docs.docker.com/get-docker/) with Docker Compose

</TabItem>
</Tabs>

You do not need to install Python, Node.js or virtual environments. The installer installs [uv](https://docs.astral.sh/uv/), and uv provides Python 3.12 and every package Copycord needs. Only [testing a branch](/docs/getting-started/manual-install#branches) also needs git, and Node.js, which the installer fetches for you when it is missing.

## From Discord

| Token | What it is | Used by |
|-------|------------|---------|
| **Bot token** | A bot application from the Discord Developer Portal | The **server** part, which manages the clone server |
| **User token** | The token of a Discord account, ideally an alt | The **client** part, which reads the source servers |

[Discord Setup](/docs/getting-started/discord-setup) shows how to get both.

## Hardware

Copycord is light and runs well on:

- **1 CPU core**
- **512 MB RAM** (1 GB for large servers)
- **1 GB disk** (more for backfilled history and backups)
- A stable internet connection

:::tip Installer or Docker?
Use the **installer** to run Copycord directly on your computer or server; it can also install Copycord as a background service. Use **Docker** if you already run your apps in containers. Both run the same Copycord.
:::
