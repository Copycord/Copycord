---
sidebar_position: 2
title: Guild Mappings
---

# Guild Mappings

A **guild mapping** is the core concept in Copycord — it defines a link between a source server and a clone server. Everything Copycord does revolves around these mappings.

## Creating a mapping

1. Navigate to the **Guilds** page in the dashboard
2. Click **Create Mapping**
3. Select the **Source Server** — the server your user account has access to
4. Select the **Clone Server** — the server where your bot is installed
5. Click **Create**

Once created, Copycord will immediately begin syncing the structure (channels, categories, roles) from the source to the clone.

## Managing mappings

### Pause / Resume

You can temporarily pause a mapping without deleting it. Paused mappings stop all syncing — no messages, edits, deletes, or structure changes will be forwarded.

Click the **toggle** button next to a mapping to pause or resume it.

### Edit settings

Each mapping has its own set of [cloning options](/docs/configuration/cloning-options) that control what gets synced. Click on a mapping to view and edit its settings.

### Delete a mapping

Deleting a mapping removes the link between the source and clone servers. It does **not** delete any channels, roles, or messages that were already created in the clone server.

## Multiple mappings

Copycord supports multiple guild mappings simultaneously. You can:

- Clone several source servers into one clone server
- Clone one source server into multiple clone servers
- Any combination of source → clone pairs

Each mapping operates independently with its own filters and settings.

## How syncing works

When a mapping is active, Copycord continuously:

1. **Watches** the source server for changes (via the client self-bot)
2. **Forwards** new messages to the clone via webhooks
3. **Syncs** structural changes (new channels, renames, deletions)
4. **Updates** roles, emojis, and stickers as they change
5. **Tracks** message edits and deletes

The sync interval for structure checks defaults to every **60 minutes**, but individual changes (new channels, renames) are detected and applied in real time via Discord gateway events.
