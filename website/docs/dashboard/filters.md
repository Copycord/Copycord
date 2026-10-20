---
sidebar_position: 5
title: Filters
---

# Filters

Filters choose what Copycord clones for one mapping: which categories and channels, which roles, and which messages.

## Edit a mapping's filters

1. On the **Bots** card, click **Stop**.
2. In **Cloning Configuration**, click **Filters** on the mapping card.
3. Add or remove values in the lists described below. To add a value, type it and press Enter; you can also paste several IDs at once. To pick categories or channels by name, click the browse button next to the list.
4. Click **Save Filters**.

## Allow only some channels

When an allow list has values, only what it lists is cloned. An empty list allows everything.

| List | Effect |
|------|--------|
| **Allowed Category IDs** | Only these categories and their channels are cloned |
| **Allowed Channel / Thread IDs** | Only these channels are cloned. Thread and forum post IDs work too |

Use the allow lists when you want a few channels from a large server.

## Block some channels

Everything is cloned except what the block lists name.

| List | Effect |
|------|--------|
| **Blocked Category IDs** | Channels in these categories are not cloned |
| **Blocked Channel / Thread IDs** | These channels and threads are not cloned |
| **Blocked Channel Name Patterns** | Channels whose names match are not cloned. `*` matches any text and `?` one character, for example `ticket-*`. A pattern without wildcards matches any name that contains it |

You can combine allow and block lists, for example allow a category and block one channel in it.

To block a channel from the **Channels** page instead, use **Add channel to blacklist** in its row menu; see [Channels](/docs/dashboard/channels).

## Block roles

1. Under **Blocked roles**, click **Fetch roles from Discord** to load the host server's roles.
2. Tick the roles that should not be cloned.

## Block messages

| List | Effect |
|------|--------|
| **Blocked Words / Phrases** | Messages that contain any of these words or phrases are not cloned |
| **Whitelisted User IDs** | Only messages from these users are cloned |
| **Blacklisted User IDs** | Messages from these users are not cloned |

Blocked words are set here, per mapping, in **Blocked Words / Phrases**. There is no slash command for them.

## What happens to existing channels

- A channel that is filtered out is not created in the clone server.
- A channel that was cloned before you filtered it out stays in the clone. Copycord does not delete it.
- New channels in a filtered category follow the filter.
