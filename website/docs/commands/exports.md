---
sidebar_position: 8
title: Exports
---

# Export Commands

Debug information. DM exports are no longer a slash command: run **Export DMs** from the dashboard's
Tasks page.

## Export DMs (Tasks page)

Exports a user's DM (direct message) history from the client account to a JSON file, a webhook, or both.

| Input | Required | Description |
|-------|----------|-------------|
| User ID | Yes | The Discord user ID to export DMs from |
| Save a JSON file | No | Writes `exports/dm/<user_id>/<time>/messages.json` in the data folder (default: on) |
| Webhook URL | No | Forwards every message to this webhook, oldest first |

The run page shows the messages read, the JSON file's path and the messages forwarded as they go, and
Cancel stops the export.

:::note
- Only one DM export per user can run at a time
- The user token (client) must have an open DM with the target user
- Messages are exported chronologically
:::

---

## `/mapping_debug`

Show a debug view of the current clone's guild mapping. Displays internal mapping data for troubleshooting.

**Usage:**
```
/mapping_debug
```
