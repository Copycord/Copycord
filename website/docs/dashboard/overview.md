---
sidebar_position: 1
title: Dashboard Overview
---

# Web Dashboard Overview

Copycord's web dashboard is a modern, feature-rich interface for managing all aspects of your server cloning setup. Access it at **http://localhost:8080** (or your configured port).

## Dashboard home

The main dashboard page gives you an at-a-glance view of your entire setup:

- **Bot status** — Connection status and uptime for both the server bot and client bot
- **Guild mappings** — All your source → clone server pairs
- **Quick actions** — Start/stop bots, create new mappings
- **Activity feed** — Recent events and operations

## Navigation

The dashboard is organized into these pages:

| Page | Purpose |
|------|---------|
| **Dashboard** | Main control center — status, mappings, start/stop |
| **Guilds** | Create and manage source → clone server mappings |
| **Channels** | View and customize cloned channel names |
| **Filters** | Configure channel/category whitelist and exclusion filters |
| **Forwarding** | Set up message forwarding to Telegram, Pushover, etc. |
| **Scraper** | Member scraper tool for exporting user data |
| **System** | System settings, logs, database backups, configuration |
| **Event Logs** | Browse the activity/audit log |

## Authentication

If you set a `PASSWORD` environment variable (or configured it in `.env`), you'll need to log in before accessing the dashboard. The session persists via a secure cookie.

To change or remove the password:
- **Docker**: Edit the `PASSWORD` environment variable in `docker-compose.yml`
- **Manual**: Edit `PASSWORD` in `code/.env`

## Starting and stopping bots

The dashboard provides **Start** and **Stop** buttons for both the server and client bots. Both must be running for cloning to work:

- **Server** (green indicator) — The Discord bot in your clone server
- **Client** (green indicator) — The self-bot monitoring source servers

## Real-time updates

The dashboard uses WebSocket connections for real-time updates. You'll see live status changes, log streaming, and operation progress without needing to refresh the page.
