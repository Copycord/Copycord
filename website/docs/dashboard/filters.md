---
sidebar_position: 5
title: Filters
---

# Filters

Filters give you fine-grained control over what Copycord clones for each mapping: which categories and channels, which roles, and which messages.

## Opening the filters

1. Stop the bots, then open the **Configuration** page
2. In **Cloning Configuration**, click **Filters** on the mapping card
3. Edit the lists and save

Each list takes IDs or words as chips. Type a value and press Enter, or paste several IDs at once. The browse buttons next to the category and channel lists open a picker of the host server's categories and channels, searchable by name or ID.

## Whitelist (Allow-list)

Only what you list here is cloned. Leave a list empty to allow everything.

- **Allowed Category IDs**: only these categories (and their channels) are cloned
- **Allowed Channel / Thread IDs**: only these channels are cloned. Thread and forum-post IDs work here too

**Best for**: when you only want a few specific channels from a large server.

## Exclude (Deny-list)

Everything is cloned **except** what you list here.

- **Blocked Category IDs**: channels inside these categories are not cloned
- **Blocked Channel / Thread IDs**: never sync these specific channels or threads

**Best for**: when you want most of the server but need to skip a few channels.

You can combine the two, for example allow a category but block one channel inside it.

## Channel Name Blacklist

**Blocked Channel Name Patterns**: channels whose names match these patterns are not cloned. Patterns can use wildcards, for example `ticket-*`.

## Role Blocks

**Blocked roles** lists roles that are not cloned for this mapping. Click **Fetch roles from Discord** to load the host server's roles and tick the ones to block. You can also block roles with the `/role_block` [slash command](/docs/commands/roles).

## Word Filters

**Blocked Words / Phrases**: any message that contains one of these words or phrases is not cloned for this mapping. You can also manage blocked keywords with the `/block_add` and `/block_list` [slash commands](/docs/commands/filtering).

## User Filters

- **Whitelisted User IDs**: only messages from these users are cloned
- **Blacklisted User IDs**: messages from these users are not cloned

## How filters interact with syncing

- **Filtered-out channels** are not created in the clone server
- If a channel is filtered out after it was already cloned, the existing clone channel remains (it won't be deleted automatically)
- New channels created in filtered categories follow the filter rules
