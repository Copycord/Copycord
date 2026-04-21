---
sidebar_position: 8
title: Exports
---

# Export Commands

Export DM histories and debug information.

## `/export_dms`

Export a user's DM (direct message) history to a JSON file, with optional real-time webhook forwarding.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `user_id` | Yes | The Discord user ID to export DMs from |
| `webhook_url` | No | Webhook URL to forward messages to in real time |
| `json_file` | No | Save a JSON snapshot (default: `true`) |

**Usage:**
```
/export_dms 123456789012345678
/export_dms 123456789012345678 https://discord.com/api/webhooks/123/abc true
```

:::note
- Only one DM export per user can run at a time
- The user token (client) must have DM history with the target user
- Messages are exported chronologically
:::

---

## `/mapping_debug`

Show a debug view of the current clone's guild mapping. Displays internal mapping data for troubleshooting.

**Usage:**
```
/mapping_debug
```

---

## `/env msg_cleanup`

Set how many days stored messages are kept in the database before automatic cleanup.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `days` | Yes | Number of days to retain messages (minimum: 1) |

**Usage:**
```
/env msg_cleanup days:7
/env msg_cleanup days:30
```

This controls when old message records are automatically pruned from the database.
