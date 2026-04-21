---
sidebar_position: 4
title: Filters
---

# Filters

Filters give you fine-grained control over which channels and categories Copycord clones. Instead of mirroring everything, you can pick exactly what you want.

## Filter modes

Each mapping supports two filter modes:

### Whitelist mode

Only the channels and categories you explicitly select will be cloned. Everything else is ignored.

**Best for**: When you only want a few specific channels from a large server.

### Exclude mode

Everything is cloned **except** the channels and categories you select.

**Best for**: When you want most of the server but need to skip a few channels.

## Setting up filters

1. Go to the **Filters** page in the dashboard (or navigate from a guild mapping)
2. Select the mapping you want to filter
3. Choose your filter mode (whitelist or exclude)
4. Select the channels and/or categories to include or exclude
5. Click **Save**

## Category filters

When you filter a **category**, all channels within that category are affected. This is a quick way to include or exclude entire sections of a server.

## Channel filters

You can also filter individual **channels** within a category. This gives you more granular control — for example, you could whitelist a category but exclude one specific channel within it.

## Keyword blocking

In addition to channel filters, you can block messages containing specific keywords:

- Messages matching blocked keywords are silently dropped — they won't appear in the clone
- Keywords are case-insensitive
- You can manage blocked keywords via the dashboard or the `/block_add` and `/block_list` [slash commands](/docs/commands/filtering)

## How filters interact with syncing

- **Filtered-out channels** are not created in the clone server
- If a channel is filtered out after it was already cloned, the existing clone channel remains (it won't be deleted automatically)
- New channels created in filtered categories follow the filter rules
- Filters apply to **messages only** — structure changes (renames, deletes) for filtered channels are still tracked internally
