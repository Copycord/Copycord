---
sidebar_position: 7
title: Requirements
description: Supported systems, what the installer brings and the hardware Copycord needs.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Requirements

Check that your computer can run Copycord and that you have the Discord tokens it needs.

## Systems

<Tabs groupId="os" queryString>
<TabItem value="unix" label="Linux / macOS" default>

- A recent Linux distribution or macOS
- `curl` or `wget`
- A normal user account: the installer does not run as root

</TabItem>
<TabItem value="windows" label="Windows">

- Windows 10 or 11
- PowerShell, opened as your normal user, not as Administrator

</TabItem>
<TabItem value="docker" label="Docker">

- [Docker](https://docs.docker.com/get-docker/) with Docker Compose

</TabItem>
</Tabs>

The installer brings Python 3.14 and every package Copycord needs. To [test a branch](/docs/getting-started/manual-install#branches), you also need git. Node.js is also used for that; the installer downloads it when it is missing.

## Discord tokens

| Token | What it is | Used by |
|-------|------------|---------|
| **Bot token** | The token of a bot application from the Discord Developer Portal | The server part, which manages the clone server |
| **User token** | The token of a Discord account, ideally an alternate account | The client part, which reads the source servers |

To get both, follow [Discord Setup](/docs/getting-started/discord-setup).

## Hardware

- 1 CPU core
- 512 MB of memory, or 1 GB for large servers
- 1 GB of disk space, plus room for copied history and backups
- A stable internet connection

## Installer or Docker

Use the installer to run Copycord directly on your computer or server; it can also run Copycord as a background service. Use Docker if you already run your apps in containers. Both run the same Copycord.
