---
sidebar_position: 3
title: Guilds
---

# Guilds

The **Guilds** page lists all Discord servers your self-bot (client) is a member of. Both the **server** and **client** bots must be running for this page to load — if either is stopped, the guild list will be empty.

## Server list

The page displays a card for each guild your self-bot account is in, showing the server name and icon.

Click on any guild card to see two options:

- **View Details** — Opens a popup with guild information (server name, member count, owner ID)
- **Export Messages** — Opens the export modal to extract messages from that server

## Export messages

The export tool extracts messages from a source server with granular filtering options.

### Options

| Option | Description |
|--------|-------------|
| **Channel ID** | Export only a specific channel (leave blank for all channels) |
| **Filter by User ID** | Export only messages from a specific user |
| **Forward to webhook** | Send each exported message to a Discord webhook in real time |
| **Date Range** | Filter by "After" and "Before" dates |
| **Download media** | Save attachments locally (images, videos, audio, other files) |

### Filters

The **Filters** section lets you narrow down which messages are exported:

- Content types: text, embeds, attachments, links, emojis, stickers, mentions
- Attachment types: images, videos, audio, other
- Message types: replies, bot messages, system messages, pinned messages
- Thread types: threads, forum threads, private threads
- Keyword search: include only messages containing a specific word
- Minimum message length and minimum total reactions

### Output

- **JSON file** — Saved to `/data/exports/<guild_id>/<timestamp>/messages.json` with all matched messages
- **Webhook forwarding** — Each message is forwarded to the provided webhook URL with a small delay between sends
- **Media files** — Downloaded attachments are organized by type in `/data/exports/<guild_id>/<timestamp>/media/`

:::info
Only one export per guild can run at a time. Exports run asynchronously — you can close the modal and the export continues in the background.
:::
